use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{
    ResourceContext, ScopeContext, SpanId, TelemetryAttribute, TelemetryError, TelemetryRecordRef,
    TelemetryResult, TelemetrySignal, TraceId,
};

const TRACE_BLOCK_MAGIC: [u8; 4] = *b"STSP";
const TRACE_BLOCK_VERSION: u8 = 1;
const TRACE_PCO_LEVEL: usize = 8;
const TRACE_SIDECAR_ZSTD_LEVEL: i32 = 1;
const DEFAULT_TRACE_IDLE_NANOS: u64 = 30_000_000_000;
const DEFAULT_LATE_TRACE_NANOS: u64 = 15 * 60 * 1_000_000_000;

/// Final OpenTelemetry span status.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanStatus {
    /// Status message.
    pub message: Arc<str>,
    /// OTLP status enum value, including unknown future values.
    pub code: i32,
}

/// One nested span event. It does not consume a shard-stream offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Event name.
    pub name: Arc<str>,
    /// Exact typed event attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Attributes dropped before export.
    pub dropped_attributes_count: u32,
}

/// One nested link to another span. It does not consume a shard-stream offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanLink {
    /// Linked trace ID.
    pub trace_id: TraceId,
    /// Linked span ID.
    pub span_id: SpanId,
    /// W3C trace state.
    pub trace_state: Arc<str>,
    /// Exact typed link attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// OTLP link flags, including unknown future bits.
    pub flags: u32,
}

/// One durable OTLP span. Exactly one instance consumes one logical offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableSpan {
    /// Physical shard-stream owner stripe.
    pub stream_shard_id: ShardId,
    /// Durable signal-aware address.
    pub record_ref: TelemetryRecordRef,
    /// Authenticated tenant.
    pub tenant: Arc<str>,
    /// Exact resource context.
    pub resource: Arc<ResourceContext>,
    /// Exact instrumentation scope context.
    pub scope: Arc<ScopeContext>,
    /// Trace ID.
    pub trace_id: TraceId,
    /// Span ID.
    pub span_id: SpanId,
    /// Parent span ID, absent for a root.
    pub parent_span_id: Option<SpanId>,
    /// W3C trace state.
    pub trace_state: Arc<str>,
    /// OTLP span flags, including unknown future bits.
    pub flags: u32,
    /// Span operation name.
    pub name: Arc<str>,
    /// OTLP span kind enum value, including unknown future values.
    pub kind: i32,
    /// Start timestamp in Unix nanoseconds.
    pub start_time_unix_nanos: u64,
    /// Exact nonnegative duration in nanoseconds.
    pub duration_nanos: u64,
    /// Exact typed span attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// Nested span events.
    pub events: Arc<Vec<SpanEvent>>,
    /// Events dropped before export.
    pub dropped_events_count: u32,
    /// Nested links.
    pub links: Arc<Vec<SpanLink>>,
    /// Links dropped before export.
    pub dropped_links_count: u32,
    /// Final status. `None` is distinct from an explicitly unset status.
    pub status: Option<SpanStatus>,
}

impl DurableSpan {
    /// Returns the exact end timestamp after checked duration reconstruction.
    #[must_use]
    pub fn end_time_unix_nanos(&self) -> Option<u64> {
        self.start_time_unix_nanos.checked_add(self.duration_nanos)
    }

    fn estimated_head_bytes(&self) -> usize {
        rmp_serde::to_vec(self).map_or(usize::MAX, |value| value.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SpanSidecar {
    tenant: Arc<str>,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    trace_state: Arc<str>,
    flags: u32,
    name: Arc<str>,
    kind: i32,
    attributes: Arc<Vec<TelemetryAttribute>>,
    dropped_attributes_count: u32,
    events: Arc<Vec<SpanEvent>>,
    dropped_events_count: u32,
    links: Arc<Vec<SpanLink>>,
    dropped_links_count: u32,
    status: Option<SpanStatus>,
}

impl From<&DurableSpan> for SpanSidecar {
    fn from(span: &DurableSpan) -> Self {
        Self {
            tenant: Arc::clone(&span.tenant),
            resource: Arc::clone(&span.resource),
            scope: Arc::clone(&span.scope),
            trace_state: Arc::clone(&span.trace_state),
            flags: span.flags,
            name: Arc::clone(&span.name),
            kind: span.kind,
            attributes: Arc::clone(&span.attributes),
            dropped_attributes_count: span.dropped_attributes_count,
            events: Arc::clone(&span.events),
            dropped_events_count: span.dropped_events_count,
            links: Arc::clone(&span.links),
            dropped_links_count: span.dropped_links_count,
            status: span.status.clone(),
        }
    }
}

/// Encodes one partition's spans into the signal-native columnar trace block.
///
/// Records are sorted by trace ID, start time, and durable offset. Offsets
/// remain the durable identity and are reconstructed exactly during decode.
pub fn encode_trace_block(records: &[DurableSpan]) -> TelemetryResult<Vec<u8>> {
    let Some(first) = records.first() else {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block must contain at least one span",
        ));
    };
    let partition = first.record_ref.topic_partition;
    let stream_shard_id = first.stream_shard_id;
    if records.iter().any(|record| {
        record.record_ref.signal != TelemetrySignal::Traces
            || record.record_ref.topic_partition != partition
            || record.stream_shard_id != stream_shard_id
    }) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block records do not share a trace partition and owner",
        ));
    }
    let mut sorted = records.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|record| {
        (
            record.trace_id,
            record.start_time_unix_nanos,
            record.record_ref.offset,
        )
    });
    let offsets = sorted
        .iter()
        .map(|record| record.record_ref.offset.get())
        .collect::<Vec<_>>();
    let starts = sorted
        .iter()
        .map(|record| record.start_time_unix_nanos)
        .collect::<Vec<_>>();
    let durations = sorted
        .iter()
        .map(|record| record.duration_nanos)
        .collect::<Vec<_>>();
    let id_lane = encode_span_ids(&sorted)?;
    let sidecars = sorted
        .iter()
        .map(|record| SpanSidecar::from(*record))
        .collect::<Vec<_>>();
    let sidecar_bytes = rmp_serde::to_vec_named(&sidecars)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed_sidecars = zstd::bulk::compress(&sidecar_bytes, TRACE_SIDECAR_ZSTD_LEVEL)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&TRACE_BLOCK_MAGIC);
    encoded.push(TRACE_BLOCK_VERSION);
    encoded.extend_from_slice(&[0; 3]);
    encoded.extend_from_slice(&stream_shard_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.topic_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.partition_id.get().to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(sorted.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    for section in [
        compress_u64(&offsets)?,
        compress_u64(&starts)?,
        compress_u64(&durations)?,
        id_lane,
        compressed_sidecars,
    ] {
        append_section(&mut encoded, &section)?;
    }
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

/// Decodes and verifies a signal-native trace block.
pub fn decode_trace_block(encoded: &[u8]) -> TelemetryResult<Vec<DurableSpan>> {
    const FIXED_HEADER: usize = 36;
    if encoded.len() < FIXED_HEADER + 32 || encoded[..4] != TRACE_BLOCK_MAGIC {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing trace block header",
        ));
    }
    if encoded[4] != TRACE_BLOCK_VERSION || encoded[5..8] != [0, 0, 0] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "unsupported trace block version or flags",
        ));
    }
    let payload_end = encoded.len() - 32;
    if blake3::hash(&encoded[..payload_end]).as_bytes() != &encoded[payload_end..] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block checksum mismatch",
        ));
    }
    let stream_shard_id = ShardId::new(u32::from_le_bytes(
        encoded[8..12].try_into().expect("fixed range"),
    ));
    let topic_partition = TopicPartition::new(
        TopicId::new(u128::from_le_bytes(
            encoded[12..28].try_into().expect("fixed range"),
        )),
        LogicalPartitionId::new(u32::from_le_bytes(
            encoded[28..32].try_into().expect("fixed range"),
        )),
    );
    let count = u32::from_le_bytes(encoded[32..36].try_into().expect("fixed range")) as usize;
    if count == 0 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace block has no spans",
        ));
    }
    let mut cursor = FIXED_HEADER;
    let offsets = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let starts = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let durations = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let ids = decode_span_ids(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let compressed_sidecars = read_section(encoded, &mut cursor, payload_end)?;
    if cursor != payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing trace block sections",
        ));
    }
    let sidecar_bytes =
        zstd::bulk::decompress(compressed_sidecars, encoded.len().saturating_mul(64)).map_err(
            |_| TelemetryError::InvalidBlockEncoding("invalid trace sidecar compression"),
        )?;
    let sidecars: Vec<SpanSidecar> = rmp_serde::from_slice(&sidecar_bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace sidecars"))?;
    if sidecars.len() != count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace sidecar count mismatch",
        ));
    }
    Ok(offsets
        .into_iter()
        .zip(starts)
        .zip(durations)
        .zip(ids)
        .zip(sidecars)
        .map(
            |((((offset, start_time_unix_nanos), duration_nanos), ids), sidecar)| DurableSpan {
                stream_shard_id,
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Traces,
                    topic_partition,
                    LogicalOffset::new(offset),
                ),
                tenant: sidecar.tenant,
                resource: sidecar.resource,
                scope: sidecar.scope,
                trace_id: ids.0,
                span_id: ids.1,
                parent_span_id: ids.2,
                trace_state: sidecar.trace_state,
                flags: sidecar.flags,
                name: sidecar.name,
                kind: sidecar.kind,
                start_time_unix_nanos,
                duration_nanos,
                attributes: sidecar.attributes,
                dropped_attributes_count: sidecar.dropped_attributes_count,
                events: sidecar.events,
                dropped_events_count: sidecar.dropped_events_count,
                links: sidecar.links,
                dropped_links_count: sidecar.dropped_links_count,
                status: sidecar.status,
            },
        )
        .collect())
}

fn encode_span_ids(records: &[&DurableSpan]) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::with_capacity(records.len().saturating_mul(18));
    let mut previous_trace = [0; 16];
    let mut previous_span = [0; 8];
    let mut previous_parent = [0; 8];
    for record in records {
        let trace = record.trace_id.as_bytes();
        let prefix = trace
            .iter()
            .zip(previous_trace)
            .take_while(|(left, right)| **left == *right)
            .count();
        encoded.push(u8::try_from(prefix).expect("trace prefix is at most 16"));
        encoded.extend_from_slice(&trace[prefix..]);
        encode_xor_id(record.span_id.as_bytes(), &previous_span, &mut encoded);
        match record.parent_span_id {
            Some(parent) => {
                encoded.push(1);
                encode_xor_id(parent.as_bytes(), &previous_parent, &mut encoded);
                previous_parent = *parent.as_bytes();
            }
            None => encoded.push(0),
        }
        previous_trace = *trace;
        previous_span = *record.span_id.as_bytes();
    }
    Ok(encoded)
}

fn encode_xor_id(current: &[u8; 8], previous: &[u8; 8], encoded: &mut Vec<u8>) {
    let xor = u64::from_be_bytes(*current) ^ u64::from_be_bytes(*previous);
    let bytes = xor.to_be_bytes();
    let leading = bytes.iter().take_while(|byte| **byte == 0).count();
    encoded.push(u8::try_from(leading).expect("leading byte count is at most 8"));
    encoded.extend_from_slice(&bytes[leading..]);
}

type DecodedSpanIds = (TraceId, SpanId, Option<SpanId>);

fn decode_span_ids(encoded: &[u8], count: usize) -> TelemetryResult<Vec<DecodedSpanIds>> {
    let mut cursor = 0;
    let mut previous_trace = [0; 16];
    let mut previous_span = [0; 8];
    let mut previous_parent = [0; 8];
    let mut decoded = Vec::with_capacity(count);
    for _ in 0..count {
        let prefix = read_byte(encoded, &mut cursor)? as usize;
        if prefix > 16 || encoded.len().saturating_sub(cursor) < 16 - prefix {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid trace ID prefix lane",
            ));
        }
        let mut trace = previous_trace;
        trace[prefix..].copy_from_slice(&encoded[cursor..cursor + 16 - prefix]);
        cursor += 16 - prefix;
        let span = decode_xor_id(encoded, &mut cursor, previous_span)?;
        let parent = match read_byte(encoded, &mut cursor)? {
            0 => None,
            1 => {
                let parent = decode_xor_id(encoded, &mut cursor, previous_parent)?;
                previous_parent = parent;
                Some(SpanId::from_bytes(parent)?)
            }
            _ => {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid parent span marker",
                ));
            }
        };
        let trace_id = TraceId::from_bytes(trace)?;
        let span_id = SpanId::from_bytes(span)?;
        decoded.push((trace_id, span_id, parent));
        previous_trace = trace;
        previous_span = span;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing span ID lane bytes",
        ));
    }
    Ok(decoded)
}

fn decode_xor_id(
    encoded: &[u8],
    cursor: &mut usize,
    previous: [u8; 8],
) -> TelemetryResult<[u8; 8]> {
    let leading = read_byte(encoded, cursor)? as usize;
    if leading > 8 || encoded.len().saturating_sub(*cursor) < 8 - leading {
        return Err(TelemetryError::InvalidBlockEncoding("invalid XOR ID lane"));
    }
    let mut xor_bytes = [0; 8];
    xor_bytes[leading..].copy_from_slice(&encoded[*cursor..*cursor + 8 - leading]);
    *cursor += 8 - leading;
    let value = u64::from_be_bytes(previous) ^ u64::from_be_bytes(xor_bytes);
    Ok(value.to_be_bytes())
}

fn compress_u64(values: &[u64]) -> TelemetryResult<Vec<u8>> {
    simple_compress(
        values,
        &ChunkConfig::default().with_compression_level(TRACE_PCO_LEVEL),
    )
    .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
}

fn decompress_u64(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    let mut values = vec![0; count];
    let progress = simple_decompress_into(encoded, &mut values)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace Pco lane"))?;
    if progress.n_processed != count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace Pco lane count mismatch",
        ));
    }
    Ok(values)
}

fn append_section(encoded: &mut Vec<u8>, section: &[u8]) -> TelemetryResult<()> {
    encoded.extend_from_slice(
        &u32::try_from(section.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(section);
    Ok(())
}

fn read_section<'a>(
    encoded: &'a [u8],
    cursor: &mut usize,
    payload_end: usize,
) -> TelemetryResult<&'a [u8]> {
    if payload_end.saturating_sub(*cursor) < 4 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated trace block section length",
        ));
    }
    let len = u32::from_le_bytes(
        encoded[*cursor..*cursor + 4]
            .try_into()
            .expect("fixed range"),
    ) as usize;
    *cursor += 4;
    let end = cursor
        .checked_add(len)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "trace section length overflow",
        ))?;
    if end > payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated trace block section",
        ));
    }
    let section = &encoded[*cursor..end];
    *cursor = end;
    Ok(section)
}

fn read_byte(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u8> {
    let value = *encoded
        .get(*cursor)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "truncated span ID lane",
        ))?;
    *cursor += 1;
    Ok(value)
}

/// Immutable directory entry for one trace's block fragments and summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSummary {
    /// Trace ID.
    pub trace_id: TraceId,
    /// Tenant.
    pub tenant: Arc<str>,
    /// Earliest span start.
    pub start_time_unix_nanos: u64,
    /// Latest span end.
    pub end_time_unix_nanos: u64,
    /// Maximum observed span duration.
    pub max_duration_nanos: u64,
    /// Number of current winning spans.
    pub span_count: u32,
    /// Number of spans with error status.
    pub error_count: u32,
    /// Root span name when known.
    pub root_name: Option<Arc<str>>,
    /// Immutable block IDs containing current or late fragments.
    pub block_fragments: Arc<Vec<u64>>,
}

/// Trace-by-ID query and bounded search constraints.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceQuery {
    /// Required tenant.
    pub tenant: Arc<str>,
    /// Exact trace ID for direct lookup.
    pub trace_id: Option<TraceId>,
    /// Inclusive lower start-time bound.
    pub start_time_unix_nanos: Option<u64>,
    /// Exclusive upper start-time bound.
    pub end_time_unix_nanos: Option<u64>,
    /// Minimum span or trace duration.
    pub min_duration_nanos: Option<u64>,
    /// Maximum number of trace summaries returned.
    pub limit: usize,
}

/// In-memory view of the immutable trace directory.
#[derive(Debug, Default, Clone)]
pub struct TraceDirectory {
    entries: BTreeMap<(Arc<str>, TraceId), TraceSummary>,
}

impl TraceDirectory {
    /// Inserts or replaces one summary after immutable block publication.
    pub fn publish(&mut self, summary: TraceSummary) {
        self.entries
            .insert((Arc::clone(&summary.tenant), summary.trace_id), summary);
    }

    /// Executes direct-ID or bounded summary search without span materialization.
    #[must_use]
    pub fn query(&self, query: &TraceQuery) -> Vec<TraceSummary> {
        let limit = query.limit.max(1);
        self.entries
            .values()
            .filter(|entry| entry.tenant == query.tenant)
            .filter(|entry| query.trace_id.is_none_or(|value| value == entry.trace_id))
            .filter(|entry| {
                query
                    .start_time_unix_nanos
                    .is_none_or(|value| entry.end_time_unix_nanos >= value)
            })
            .filter(|entry| {
                query
                    .end_time_unix_nanos
                    .is_none_or(|value| entry.start_time_unix_nanos < value)
            })
            .filter(|entry| {
                query
                    .min_duration_nanos
                    .is_none_or(|value| entry.max_duration_nanos >= value)
            })
            .take(limit)
            .cloned()
            .collect()
    }
}

#[derive(Debug)]
struct HotTrace {
    spans: BTreeMap<SpanId, DurableSpan>,
    bytes: usize,
    last_append_nanos: u64,
    first_sealed_nanos: Option<u64>,
    conflicts: u64,
    retries: u64,
}

/// Result of applying one span to a stripe-local trace head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceApplyOutcome {
    /// A new span identity was inserted.
    Inserted,
    /// A byte-identical retransmission was ignored.
    Duplicate,
    /// A conflicting version replaced an older durable offset.
    Replaced,
    /// An older conflicting version was ignored.
    Obsolete,
}

/// Bounded, single-writer trace state owned by one physical stripe.
#[derive(Debug)]
pub struct TraceStripe {
    head_budget_bytes: usize,
    head_bytes: usize,
    idle_nanos: u64,
    late_grace_nanos: u64,
    traces: HashMap<(Arc<str>, TraceId), HotTrace>,
    directory: TraceDirectory,
    next_block_id: u64,
}

impl TraceStripe {
    /// Creates a stripe-local trace head with production idle and late windows.
    pub fn new(head_budget_bytes: usize) -> TelemetryResult<Self> {
        if head_budget_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "trace head budget must be nonzero",
            ));
        }
        Ok(Self {
            head_budget_bytes,
            head_bytes: 0,
            idle_nanos: DEFAULT_TRACE_IDLE_NANOS,
            late_grace_nanos: DEFAULT_LATE_TRACE_NANOS,
            traces: HashMap::new(),
            directory: TraceDirectory::default(),
            next_block_id: 1,
        })
    }

    /// Applies one durable span with deterministic retry/conflict semantics.
    pub fn apply(
        &mut self,
        span: DurableSpan,
        append_time_nanos: u64,
    ) -> TelemetryResult<TraceApplyOutcome> {
        if span.record_ref.signal != TelemetrySignal::Traces {
            return Err(TelemetryError::InvalidBlockEncoding(
                "non-trace record applied to trace stripe",
            ));
        }
        let key = (Arc::clone(&span.tenant), span.trace_id);
        let estimated = span.estimated_head_bytes();
        if estimated > self.head_budget_bytes {
            return Err(TelemetryError::RecordTooLarge);
        }
        if self.head_bytes.saturating_add(estimated) > self.head_budget_bytes {
            self.seal_idle(append_time_nanos)?;
        }
        if self.head_bytes.saturating_add(estimated) > self.head_budget_bytes {
            return Err(TelemetryError::InvalidConfig(
                "trace head memory budget exhausted",
            ));
        }
        let trace = self.traces.entry(key).or_insert_with(|| HotTrace {
            spans: BTreeMap::new(),
            bytes: 0,
            last_append_nanos: append_time_nanos,
            first_sealed_nanos: None,
            conflicts: 0,
            retries: 0,
        });
        trace.last_append_nanos = append_time_nanos;
        match trace.spans.get(&span.span_id) {
            Some(existing) if existing == &span => {
                trace.retries = trace.retries.saturating_add(1);
                Ok(TraceApplyOutcome::Duplicate)
            }
            Some(existing) if existing.record_ref.offset >= span.record_ref.offset => {
                trace.conflicts = trace.conflicts.saturating_add(1);
                Ok(TraceApplyOutcome::Obsolete)
            }
            Some(existing) => {
                let previous = existing.estimated_head_bytes();
                trace.conflicts = trace.conflicts.saturating_add(1);
                trace.bytes = trace
                    .bytes
                    .saturating_sub(previous)
                    .saturating_add(estimated);
                self.head_bytes = self
                    .head_bytes
                    .saturating_sub(previous)
                    .saturating_add(estimated);
                trace.spans.insert(span.span_id, span);
                Ok(TraceApplyOutcome::Replaced)
            }
            None => {
                trace.bytes = trace.bytes.saturating_add(estimated);
                self.head_bytes = self.head_bytes.saturating_add(estimated);
                trace.spans.insert(span.span_id, span);
                Ok(TraceApplyOutcome::Inserted)
            }
        }
    }

    /// Seals traces idle for 30 seconds and returns immutable trace blocks.
    pub fn seal_idle(&mut self, now_nanos: u64) -> TelemetryResult<Vec<Vec<u8>>> {
        let ready = self
            .traces
            .iter()
            .filter_map(|(key, trace)| {
                (now_nanos.saturating_sub(trace.last_append_nanos) >= self.idle_nanos)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        let mut blocks = Vec::with_capacity(ready.len());
        for key in ready {
            let mut trace = self.traces.remove(&key).expect("selected trace exists");
            self.head_bytes = self.head_bytes.saturating_sub(trace.bytes);
            trace.first_sealed_nanos = Some(now_nanos);
            let spans = trace.spans.into_values().collect::<Vec<_>>();
            let block_id = self.next_block_id;
            self.next_block_id = self.next_block_id.saturating_add(1);
            let block = encode_trace_block(&spans)?;
            self.directory.publish(summarize_trace(&spans, block_id)?);
            blocks.push(block);
        }
        Ok(blocks)
    }

    /// Returns the immutable summary directory.
    #[must_use]
    pub const fn directory(&self) -> &TraceDirectory {
        &self.directory
    }

    /// Queries current hot spans after trace/time pushdown.
    #[must_use]
    pub fn query(&self, query: &TraceQuery) -> Vec<DurableSpan> {
        let limit = query.limit.max(1);
        let mut spans = self
            .traces
            .iter()
            .filter(|((tenant, trace_id), _)| {
                tenant.as_ref() == query.tenant.as_ref()
                    && query
                        .trace_id
                        .is_none_or(|requested| requested == *trace_id)
            })
            .flat_map(|(_, trace)| trace.spans.values())
            .filter(|span| {
                query
                    .start_time_unix_nanos
                    .is_none_or(|start| span.end_time_unix_nanos().unwrap_or(u64::MAX) >= start)
            })
            .filter(|span| {
                query
                    .end_time_unix_nanos
                    .is_none_or(|end| span.start_time_unix_nanos < end)
            })
            .filter(|span| {
                query
                    .min_duration_nanos
                    .is_none_or(|duration| span.duration_nanos >= duration)
            })
            .cloned()
            .collect::<Vec<_>>();
        spans.sort_unstable_by_key(|span| {
            (
                span.trace_id,
                span.start_time_unix_nanos,
                span.record_ref.offset,
            )
        });
        spans.truncate(limit);
        spans
    }

    /// Returns current stripe-local head bytes.
    #[must_use]
    pub const fn head_bytes(&self) -> usize {
        self.head_bytes
    }

    /// Returns the configured late-fragment compaction window.
    #[must_use]
    pub const fn late_grace_nanos(&self) -> u64 {
        self.late_grace_nanos
    }
}

fn summarize_trace(spans: &[DurableSpan], block_id: u64) -> TelemetryResult<TraceSummary> {
    let first = spans.first().ok_or(TelemetryError::InvalidBlockEncoding(
        "cannot summarize an empty trace",
    ))?;
    let mut start = u64::MAX;
    let mut end = 0;
    let mut max_duration = 0;
    let mut errors = 0u32;
    let mut root_name = None;
    for span in spans {
        start = start.min(span.start_time_unix_nanos);
        end = end.max(span.end_time_unix_nanos().unwrap_or(u64::MAX));
        max_duration = max_duration.max(span.duration_nanos);
        if span.status.as_ref().is_some_and(|status| status.code == 2) {
            errors = errors.saturating_add(1);
        }
        if span.parent_span_id.is_none() && root_name.is_none() {
            root_name = Some(Arc::clone(&span.name));
        }
    }
    Ok(TraceSummary {
        trace_id: first.trace_id,
        tenant: Arc::clone(&first.tenant),
        start_time_unix_nanos: start,
        end_time_unix_nanos: end,
        max_duration_nanos: max_duration,
        span_count: u32::try_from(spans.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        error_count: errors,
        root_name,
        block_fragments: Arc::new(vec![block_id]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TRACES_TOPIC_ID;

    fn span(offset: u64, trace_byte: u8, span_byte: u8) -> DurableSpan {
        DurableSpan {
            stream_shard_id: ShardId::new(3),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Traces,
                TopicPartition::new(TRACES_TOPIC_ID, LogicalPartitionId::new(9)),
                LogicalOffset::new(offset),
            ),
            tenant: Arc::from("tenant-a"),
            resource: Arc::new(ResourceContext::default()),
            scope: Arc::new(ScopeContext::default()),
            trace_id: TraceId::from_bytes([trace_byte; 16]).unwrap(),
            span_id: SpanId::from_bytes([span_byte; 8]).unwrap(),
            parent_span_id: None,
            trace_state: Arc::from(""),
            flags: 1,
            name: Arc::from("GET /checkout"),
            kind: 2,
            start_time_unix_nanos: 1_000 + offset,
            duration_nanos: 50,
            attributes: Arc::new(vec![TelemetryAttribute::new(
                "http.status_code",
                crate::TelemetryValue::Integer(200),
            )]),
            dropped_attributes_count: 0,
            events: Arc::new(Vec::new()),
            dropped_events_count: 0,
            links: Arc::new(Vec::new()),
            dropped_links_count: 0,
            status: Some(SpanStatus {
                message: Arc::from("ok"),
                code: 1,
            }),
        }
    }

    #[test]
    fn trace_block_round_trips_after_sorting() {
        let records = vec![span(8, 2, 3), span(2, 1, 2), span(1, 1, 1)];
        let encoded = encode_trace_block(&records).unwrap();
        let decoded = decode_trace_block(&encoded).unwrap();
        assert_eq!(decoded[0], records[2]);
        assert_eq!(decoded[1], records[1]);
        assert_eq!(decoded[2], records[0]);
    }

    #[test]
    fn duplicate_and_conflicting_spans_follow_durable_offset() {
        let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
        let original = span(1, 1, 1);
        assert_eq!(
            stripe.apply(original.clone(), 0).unwrap(),
            TraceApplyOutcome::Inserted
        );
        assert_eq!(
            stripe.apply(original.clone(), 1).unwrap(),
            TraceApplyOutcome::Duplicate
        );
        let mut newer = original.clone();
        newer.record_ref.offset = LogicalOffset::new(2);
        newer.name = Arc::from("changed");
        assert_eq!(stripe.apply(newer, 2).unwrap(), TraceApplyOutcome::Replaced);
        let mut older = original;
        older.name = Arc::from("obsolete");
        assert_eq!(stripe.apply(older, 3).unwrap(), TraceApplyOutcome::Obsolete);
    }

    #[test]
    fn idle_trace_seals_and_becomes_directly_queryable() {
        let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
        stripe.apply(span(1, 1, 1), 10).unwrap();
        let blocks = stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();
        assert_eq!(blocks.len(), 1);
        let summaries = stripe.directory().query(&TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
            limit: 10,
            ..TraceQuery::default()
        });
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].span_count, 1);
    }
}
