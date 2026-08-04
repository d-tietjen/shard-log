use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{
    CorrelationBlockFilter, ResourceContext, ScopeContext, SignalTierPayload, SpanId,
    TelemetryAttribute, TelemetryError, TelemetryRecordRef, TelemetryResult, TelemetrySignal,
    TraceId,
};

const TRACE_BLOCK_MAGIC: [u8; 4] = *b"STSP";
const TRACE_BLOCK_VERSION: u8 = 1;
const TRACE_PCO_LEVEL: usize = 8;
const TRACE_SIDECAR_ZSTD_LEVEL: i32 = 1;
const TARGET_TRACE_BLOCK_SOURCE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_TRACE_IDLE_NANOS: u64 = 30_000_000_000;
const DEFAULT_LATE_TRACE_NANOS: u64 = 15 * 60 * 1_000_000_000;

/// Final OpenTelemetry span status.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanStatus {
    /// Status message.
    pub message: Arc<str>,
    /// OTLP status enum value, including unknown future values.
    pub code: i32,
}

/// One nested span event. It does not consume a shard-stream offset.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Returns the cross-signal identity of this span's resource context.
    #[must_use]
    pub fn resource_id(&self) -> crate::ResourceContextId {
        self.resource.id()
    }

    /// Returns the cross-signal identity of this span's instrumentation scope.
    #[must_use]
    pub fn scope_id(&self) -> crate::ScopeContextId {
        self.scope.id()
    }

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
struct PackedSpanSidecar {
    tenant_id: u32,
    resource_id: u32,
    scope_id: u32,
    trace_state_id: u32,
    flags: u32,
    name_id: u32,
    kind: i32,
    attributes_id: u32,
    dropped_attributes_count: u32,
    events_id: u32,
    dropped_events_count: u32,
    links_id: u32,
    dropped_links_count: u32,
    status_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TraceBlockSidecars {
    tenants: Vec<Arc<str>>,
    resources: Vec<Arc<ResourceContext>>,
    scopes: Vec<Arc<ScopeContext>>,
    trace_states: Vec<Arc<str>>,
    names: Vec<Arc<str>>,
    attribute_sets: Vec<Arc<Vec<TelemetryAttribute>>>,
    event_sets: Vec<Arc<Vec<SpanEvent>>>,
    link_sets: Vec<Arc<Vec<SpanLink>>>,
    statuses: Vec<Option<SpanStatus>>,
    spans: Vec<PackedSpanSidecar>,
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
    let sidecars = encode_trace_sidecars(&sorted)?;
    let sidecar_bytes = rmp_serde::to_vec(&sidecars)
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
    let sidecar_bytes = zstd::bulk::decompress(compressed_sidecars, 256 * 1024 * 1024)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace sidecar compression"))?;
    let sidecars: TraceBlockSidecars = rmp_serde::from_slice(&sidecar_bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid trace sidecars"))?;
    if sidecars.spans.len() != count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trace sidecar count mismatch",
        ));
    }
    offsets
        .into_iter()
        .zip(starts)
        .zip(durations)
        .zip(ids)
        .zip(sidecars.spans)
        .map(
            |((((offset, start_time_unix_nanos), duration_nanos), ids), sidecar)| {
                Ok(DurableSpan {
                    stream_shard_id,
                    record_ref: TelemetryRecordRef::for_signal(
                        TelemetrySignal::Traces,
                        topic_partition,
                        LogicalOffset::new(offset),
                    ),
                    tenant: resolve_sidecar(&sidecars.tenants, sidecar.tenant_id, "tenant")?,
                    resource: resolve_sidecar(
                        &sidecars.resources,
                        sidecar.resource_id,
                        "resource",
                    )?,
                    scope: resolve_sidecar(&sidecars.scopes, sidecar.scope_id, "scope")?,
                    trace_id: ids.0,
                    span_id: ids.1,
                    parent_span_id: ids.2,
                    trace_state: resolve_sidecar(
                        &sidecars.trace_states,
                        sidecar.trace_state_id,
                        "trace state",
                    )?,
                    flags: sidecar.flags,
                    name: resolve_sidecar(&sidecars.names, sidecar.name_id, "name")?,
                    kind: sidecar.kind,
                    start_time_unix_nanos,
                    duration_nanos,
                    attributes: resolve_sidecar(
                        &sidecars.attribute_sets,
                        sidecar.attributes_id,
                        "attributes",
                    )?,
                    dropped_attributes_count: sidecar.dropped_attributes_count,
                    events: resolve_sidecar(&sidecars.event_sets, sidecar.events_id, "events")?,
                    dropped_events_count: sidecar.dropped_events_count,
                    links: resolve_sidecar(&sidecars.link_sets, sidecar.links_id, "links")?,
                    dropped_links_count: sidecar.dropped_links_count,
                    status: resolve_sidecar(&sidecars.statuses, sidecar.status_id, "status")?,
                })
            },
        )
        .collect::<TelemetryResult<Vec<_>>>()
}

fn encode_trace_sidecars(records: &[&DurableSpan]) -> TelemetryResult<TraceBlockSidecars> {
    let mut tenants = SidecarInterner::new(records.len());
    let mut resources = SidecarInterner::new(records.len());
    let mut scopes = SidecarInterner::new(records.len());
    let mut trace_states = SidecarInterner::new(records.len());
    let mut names = SidecarInterner::new(records.len());
    let mut attribute_sets = SidecarInterner::new(records.len());
    let mut event_sets = SidecarInterner::new(records.len());
    let mut link_sets = SidecarInterner::new(records.len());
    let mut statuses = SidecarInterner::new(records.len());
    let mut spans = Vec::with_capacity(records.len());
    for span in records {
        spans.push(PackedSpanSidecar {
            tenant_id: tenants.intern(&span.tenant)?,
            resource_id: resources.intern(&span.resource)?,
            scope_id: scopes.intern(&span.scope)?,
            trace_state_id: trace_states.intern(&span.trace_state)?,
            flags: span.flags,
            name_id: names.intern(&span.name)?,
            kind: span.kind,
            attributes_id: attribute_sets.intern(&span.attributes)?,
            dropped_attributes_count: span.dropped_attributes_count,
            events_id: event_sets.intern(&span.events)?,
            dropped_events_count: span.dropped_events_count,
            links_id: link_sets.intern(&span.links)?,
            dropped_links_count: span.dropped_links_count,
            status_id: statuses.intern(&span.status)?,
        });
    }
    Ok(TraceBlockSidecars {
        tenants: tenants.into_values(),
        resources: resources.into_values(),
        scopes: scopes.into_values(),
        trace_states: trace_states.into_values(),
        names: names.into_values(),
        attribute_sets: attribute_sets.into_values(),
        event_sets: event_sets.into_values(),
        link_sets: link_sets.into_values(),
        statuses: statuses.into_values(),
        spans,
    })
}

struct SidecarInterner<T> {
    values: Vec<T>,
    ids: Option<HashMap<T, u32>>,
    capacity: usize,
}

impl<T: Clone + Eq + Hash> SidecarInterner<T> {
    fn new(capacity: usize) -> Self {
        Self {
            values: Vec::new(),
            ids: None,
            capacity,
        }
    }

    fn intern(&mut self, value: &T) -> TelemetryResult<u32> {
        if let Some(ids) = &self.ids {
            if let Some(index) = ids.get(value) {
                return Ok(*index);
            }
        } else {
            if let Some(index) = self.values.iter().position(|candidate| candidate == value) {
                return u32::try_from(index).map_err(|_| TelemetryError::RecordTooLarge);
            }
            if self.values.len() == 16 {
                self.ids = Some(
                    self.values
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(index, value)| {
                            Ok((
                                value,
                                u32::try_from(index).map_err(|_| TelemetryError::RecordTooLarge)?,
                            ))
                        })
                        .collect::<TelemetryResult<HashMap<_, _>>>()?,
                );
                self.ids
                    .as_mut()
                    .expect("interner map was installed")
                    .reserve(self.capacity.min(4_096).saturating_sub(16));
            }
        }
        let index = u32::try_from(self.values.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let value = value.clone();
        self.values.push(value.clone());
        if let Some(ids) = &mut self.ids {
            ids.insert(value, index);
        }
        Ok(index)
    }

    fn into_values(self) -> Vec<T> {
        self.values
    }
}

fn resolve_sidecar<T: Clone>(values: &[T], id: u32, lane: &'static str) -> TelemetryResult<T> {
    values
        .get(id as usize)
        .cloned()
        .ok_or(TelemetryError::InvalidBlockEncoding(match lane {
            "tenant" => "trace tenant sidecar ID is out of range",
            "resource" => "trace resource sidecar ID is out of range",
            "scope" => "trace scope sidecar ID is out of range",
            "trace state" => "trace state sidecar ID is out of range",
            "name" => "trace name sidecar ID is out of range",
            "attributes" => "trace attribute sidecar ID is out of range",
            "events" => "trace event sidecar ID is out of range",
            "links" => "trace link sidecar ID is out of range",
            "status" => "trace status sidecar ID is out of range",
            _ => "trace sidecar ID is out of range",
        }))
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
    /// Publishes one immutable fragment and merges it into the trace summary.
    ///
    /// Late fragments must extend the directory entry rather than replacing
    /// the earlier block list; otherwise a direct trace lookup can lose the
    /// only directory reference to spans that were already sealed.
    pub fn publish(&mut self, summary: TraceSummary) {
        let key = (Arc::clone(&summary.tenant), summary.trace_id);
        let Some(current) = self.entries.get_mut(&key) else {
            self.entries.insert(key, summary);
            return;
        };
        current.start_time_unix_nanos = current
            .start_time_unix_nanos
            .min(summary.start_time_unix_nanos);
        current.end_time_unix_nanos = current.end_time_unix_nanos.max(summary.end_time_unix_nanos);
        current.max_duration_nanos = current.max_duration_nanos.max(summary.max_duration_nanos);
        current.span_count = current.span_count.saturating_add(summary.span_count);
        current.error_count = current.error_count.saturating_add(summary.error_count);
        if current.root_name.is_none() {
            current.root_name = summary.root_name;
        }
        let mut fragments = current.block_fragments.as_ref().clone();
        fragments.extend(summary.block_fragments.iter().copied());
        fragments.sort_unstable();
        fragments.dedup();
        current.block_fragments = Arc::new(fragments);
    }

    fn publish_current(&mut self, mut summary: TraceSummary) {
        let key = (Arc::clone(&summary.tenant), summary.trace_id);
        if let Some(current) = self.entries.get(&key) {
            let mut fragments = current.block_fragments.as_ref().clone();
            fragments.extend(summary.block_fragments.iter().copied());
            fragments.sort_unstable();
            fragments.dedup();
            summary.block_fragments = Arc::new(fragments);
        }
        self.entries.insert(key, summary);
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

#[derive(Debug)]
struct RecentlySealedTrace {
    spans: BTreeMap<SpanId, DurableSpan>,
    bytes: usize,
    first_sealed_nanos: u64,
    last_sealed_nanos: u64,
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
    recently_sealed: HashMap<(Arc<str>, TraceId), RecentlySealedTrace>,
    recent_order: VecDeque<(u64, (Arc<str>, TraceId))>,
    recently_sealed_bytes: usize,
    sealed_blocks: HashMap<u64, Arc<[u8]>>,
    pending_blocks: Vec<SignalTierPayload>,
    directory: TraceDirectory,
    next_block_id: u64,
}

struct PreparedTrace {
    spans: Vec<DurableSpan>,
    summary_spans: Vec<DurableSpan>,
    replaces_summary: bool,
    source_bytes: usize,
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
            recently_sealed: HashMap::new(),
            recent_order: VecDeque::new(),
            recently_sealed_bytes: 0,
            sealed_blocks: HashMap::new(),
            pending_blocks: Vec::new(),
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
        self.expire_recent(append_time_nanos);
        let hot_has_span = self
            .traces
            .get(&key)
            .is_some_and(|trace| trace.spans.contains_key(&span.span_id));
        let mut replaces_recent = false;
        if !hot_has_span
            && let Some(recent) = self.recently_sealed.get_mut(&key)
            && let Some(existing) = recent.spans.get(&span.span_id)
        {
            if same_span_payload(existing, &span) {
                recent.retries = recent.retries.saturating_add(1);
                return Ok(TraceApplyOutcome::Duplicate);
            }
            recent.conflicts = recent.conflicts.saturating_add(1);
            if existing.record_ref.offset >= span.record_ref.offset {
                return Ok(TraceApplyOutcome::Obsolete);
            }
            let previous = existing.estimated_head_bytes();
            recent.spans.remove(&span.span_id);
            recent.bytes = recent.bytes.saturating_sub(previous);
            self.recently_sealed_bytes = self.recently_sealed_bytes.saturating_sub(previous);
            replaces_recent = true;
        }
        if self.resident_state_bytes().saturating_add(estimated) > self.head_budget_bytes {
            self.seal_idle(append_time_nanos)?;
        }
        self.evict_recent_to_fit(estimated);
        if self.resident_state_bytes().saturating_add(estimated) > self.head_budget_bytes {
            return Err(TelemetryError::InvalidConfig(
                "trace head memory budget exhausted",
            ));
        }
        let first_sealed_nanos = self
            .recently_sealed
            .get(&key)
            .map(|recent| recent.first_sealed_nanos);
        let trace = self.traces.entry(key).or_insert_with(|| HotTrace {
            spans: BTreeMap::new(),
            bytes: 0,
            last_append_nanos: append_time_nanos,
            first_sealed_nanos,
            conflicts: 0,
            retries: 0,
        });
        trace.last_append_nanos = append_time_nanos;
        match trace.spans.get(&span.span_id) {
            Some(existing) if same_span_payload(existing, &span) => {
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
                Ok(if replaces_recent {
                    TraceApplyOutcome::Replaced
                } else {
                    TraceApplyOutcome::Inserted
                })
            }
        }
    }

    fn resident_state_bytes(&self) -> usize {
        self.head_bytes.saturating_add(self.recently_sealed_bytes)
    }

    fn expire_recent(&mut self, now_nanos: u64) {
        while self.recent_order.front().is_some_and(|(sealed_at, _)| {
            now_nanos.saturating_sub(*sealed_at) > self.late_grace_nanos
        }) {
            let (sealed_at, key) = self.recent_order.pop_front().expect("front was checked");
            if self
                .recently_sealed
                .get(&key)
                .is_some_and(|recent| recent.last_sealed_nanos == sealed_at)
                && let Some(recent) = self.recently_sealed.remove(&key)
            {
                self.recently_sealed_bytes =
                    self.recently_sealed_bytes.saturating_sub(recent.bytes);
            }
        }
    }

    fn evict_recent_to_fit(&mut self, additional_bytes: usize) {
        while self.resident_state_bytes().saturating_add(additional_bytes) > self.head_budget_bytes
        {
            let Some((sealed_at, key)) = self.recent_order.pop_front() else {
                break;
            };
            if self
                .recently_sealed
                .get(&key)
                .is_some_and(|recent| recent.last_sealed_nanos == sealed_at)
                && let Some(recent) = self.recently_sealed.remove(&key)
            {
                self.recently_sealed_bytes =
                    self.recently_sealed_bytes.saturating_sub(recent.bytes);
            }
        }
    }

    fn remember_recent(
        &mut self,
        key: &(Arc<str>, TraceId),
        spans: &[DurableSpan],
        first_sealed_nanos: u64,
        now_nanos: u64,
    ) {
        let recent =
            self.recently_sealed
                .entry(key.clone())
                .or_insert_with(|| RecentlySealedTrace {
                    spans: BTreeMap::new(),
                    bytes: 0,
                    first_sealed_nanos,
                    last_sealed_nanos: now_nanos,
                    conflicts: 0,
                    retries: 0,
                });
        let previous_bytes = recent.bytes;
        recent.first_sealed_nanos = recent.first_sealed_nanos.min(first_sealed_nanos);
        recent.last_sealed_nanos = now_nanos;
        for span in spans {
            let replace = recent
                .spans
                .get(&span.span_id)
                .is_none_or(|existing| existing.record_ref.offset < span.record_ref.offset);
            if replace {
                if let Some(existing) = recent.spans.insert(span.span_id, span.clone()) {
                    recent.bytes = recent.bytes.saturating_sub(existing.estimated_head_bytes());
                }
                recent.bytes = recent.bytes.saturating_add(span.estimated_head_bytes());
            }
        }
        self.recently_sealed_bytes = self
            .recently_sealed_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(recent.bytes);
        self.recent_order.push_back((now_nanos, key.clone()));
        self.evict_recent_to_fit(0);
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
        self.seal_keys(ready, now_nanos)
    }

    /// Seals every hot fragment belonging to one logical trace partition.
    pub(crate) fn seal_partition(
        &mut self,
        partition: TopicPartition,
        now_nanos: u64,
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        let ready = self
            .traces
            .iter()
            .filter_map(|(key, trace)| {
                trace
                    .spans
                    .values()
                    .next()
                    .is_some_and(|span| span.record_ref.topic_partition == partition)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        self.seal_keys(ready, now_nanos)
    }

    fn seal_keys(
        &mut self,
        mut ready: Vec<(Arc<str>, TraceId)>,
        now_nanos: u64,
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        ready.sort_unstable();
        let mut by_partition = BTreeMap::<TopicPartition, Vec<PreparedTrace>>::new();
        for key in ready {
            let trace = self.traces.remove(&key).expect("selected trace exists");
            self.head_bytes = self.head_bytes.saturating_sub(trace.bytes);
            let continued_recent_trace = trace.first_sealed_nanos.is_some();
            let first_sealed_nanos = trace.first_sealed_nanos.unwrap_or(now_nanos);
            let spans = trace.spans.into_values().collect::<Vec<_>>();
            self.remember_recent(&key, &spans, first_sealed_nanos, now_nanos);
            let summary_spans = if continued_recent_trace {
                self.recently_sealed
                    .get(&key)
                    .expect("continued trace was remembered")
                    .spans
                    .values()
                    .cloned()
                    .collect()
            } else {
                spans.clone()
            };
            let partition = spans[0].record_ref.topic_partition;
            by_partition
                .entry(partition)
                .or_default()
                .push(PreparedTrace {
                    spans,
                    summary_spans,
                    replaces_summary: continued_recent_trace,
                    source_bytes: trace.bytes,
                });
        }
        let mut blocks = Vec::new();
        for traces in by_partition.into_values() {
            let mut group = Vec::new();
            let mut group_bytes = 0usize;
            for trace in traces {
                if !group.is_empty()
                    && group_bytes.saturating_add(trace.source_bytes)
                        > TARGET_TRACE_BLOCK_SOURCE_BYTES
                {
                    blocks.push(self.seal_prepared_block(std::mem::take(&mut group))?);
                    group_bytes = 0;
                }
                group_bytes = group_bytes.saturating_add(trace.source_bytes);
                group.push(trace);
            }
            if !group.is_empty() {
                blocks.push(self.seal_prepared_block(group)?);
            }
        }
        Ok(blocks)
    }

    fn seal_prepared_block(&mut self, traces: Vec<PreparedTrace>) -> TelemetryResult<Vec<u8>> {
        let block_id = self.next_block_id;
        self.next_block_id = self.next_block_id.saturating_add(1);
        let span_count = traces.iter().map(|trace| trace.spans.len()).sum();
        let mut spans = Vec::with_capacity(span_count);
        let mut summaries = Vec::with_capacity(traces.len());
        for trace in traces {
            spans.extend(trace.spans);
            summaries.push((trace.summary_spans, trace.replaces_summary));
        }
        let block = encode_trace_block(&spans)?;
        for (summary_spans, replaces_summary) in summaries {
            let summary = summarize_trace(&summary_spans, block_id)?;
            if replaces_summary {
                self.directory.publish_current(summary);
            } else {
                self.directory.publish(summary);
            }
        }
        let payload = Arc::<[u8]>::from(block.clone());
        let first_offset = spans
            .iter()
            .map(|span| span.record_ref.offset.get())
            .min()
            .expect("sealed trace block is nonempty");
        let last_offset = spans
            .iter()
            .map(|span| span.record_ref.offset.get())
            .max()
            .expect("sealed trace block is nonempty");
        let min_timestamp_unix_nanos = spans
            .iter()
            .map(|span| span.start_time_unix_nanos)
            .min()
            .expect("sealed trace block is nonempty");
        let max_timestamp_unix_nanos = spans
            .iter()
            .map(|span| span.end_time_unix_nanos().unwrap_or(u64::MAX))
            .max()
            .expect("sealed trace block is nonempty");
        let min_signal_identity = spans
            .iter()
            .map(|span| u128::from_be_bytes(*span.trace_id.as_bytes()))
            .min()
            .expect("sealed trace block is nonempty");
        let max_signal_identity = spans
            .iter()
            .map(|span| u128::from_be_bytes(*span.trace_id.as_bytes()))
            .max()
            .expect("sealed trace block is nonempty");
        self.pending_blocks.push(SignalTierPayload {
            resident_id: block_id,
            topic_partition: spans[0].record_ref.topic_partition,
            min_signal_identity,
            max_signal_identity,
            first_offset,
            last_offset,
            record_count: u32::try_from(spans.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            payload: Arc::clone(&payload),
            correlation_filter: CorrelationBlockFilter::for_spans(&spans),
        });
        self.sealed_blocks.insert(block_id, payload);
        Ok(block)
    }

    pub(crate) fn pending_partition(&self, partition: TopicPartition) -> Vec<SignalTierPayload> {
        self.pending_blocks
            .iter()
            .filter(|payload| payload.topic_partition == partition)
            .cloned()
            .collect()
    }

    pub(crate) fn release_published_blocks(&mut self, resident_ids: &[u64]) {
        self.pending_blocks
            .retain(|payload| !resident_ids.contains(&payload.resident_id));
        self.sealed_blocks
            .retain(|block_id, _| !resident_ids.contains(block_id));
    }

    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.sealed_blocks
            .values()
            .map(|payload| u64::try_from(payload.len()).unwrap_or(u64::MAX))
            .sum()
    }

    /// Returns the immutable summary directory.
    #[must_use]
    pub const fn directory(&self) -> &TraceDirectory {
        &self.directory
    }

    /// Queries current hot spans after trace/time pushdown.
    pub fn query(&self, query: &TraceQuery) -> TelemetryResult<Vec<DurableSpan>> {
        let limit = query.limit.max(1);
        if let Some(trace_id) = query.trace_id {
            return self.query_exact_trace(query, trace_id, limit);
        }
        let mut winners = BTreeMap::<(Arc<str>, TraceId, SpanId), DurableSpan>::new();
        for payload in self.sealed_blocks.values() {
            for span in decode_trace_block(payload)? {
                if trace_query_matches(query, &span) {
                    retain_newest_span(&mut winners, span);
                }
            }
        }
        for span in self
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
        {
            retain_newest_span(&mut winners, span);
        }
        let mut spans = winners.into_values().collect::<Vec<_>>();
        spans.sort_unstable_by_key(|span| {
            (
                span.trace_id,
                span.start_time_unix_nanos,
                span.record_ref.offset,
            )
        });
        spans.truncate(limit);
        Ok(spans)
    }

    fn query_exact_trace(
        &self,
        query: &TraceQuery,
        trace_id: TraceId,
        limit: usize,
    ) -> TelemetryResult<Vec<DurableSpan>> {
        let key = (Arc::clone(&query.tenant), trace_id);
        let mut winners = BTreeMap::<SpanId, DurableSpan>::new();
        if let Some(summary) = self.directory.entries.get(&key) {
            for block_id in summary.block_fragments.iter() {
                let Some(payload) = self.sealed_blocks.get(block_id) else {
                    continue;
                };
                for span in decode_trace_block(payload)? {
                    if trace_query_matches(query, &span) {
                        retain_exact_trace_span(&mut winners, span);
                    }
                }
            }
        }
        if let Some(trace) = self.traces.get(&key) {
            for span in trace
                .spans
                .values()
                .filter(|span| trace_query_matches(query, span))
            {
                retain_exact_trace_span(&mut winners, span.clone());
            }
        }
        let mut spans = winners.into_values().collect::<Vec<_>>();
        spans.sort_unstable_by_key(|span| (span.start_time_unix_nanos, span.record_ref.offset));
        spans.truncate(limit);
        Ok(spans)
    }

    /// Returns current mutable and late-fragment state bytes.
    #[must_use]
    pub fn head_bytes(&self) -> usize {
        self.resident_state_bytes()
    }

    /// Returns the configured late-fragment compaction window.
    #[must_use]
    pub const fn late_grace_nanos(&self) -> u64 {
        self.late_grace_nanos
    }
}

pub(crate) fn trace_query_matches(query: &TraceQuery, span: &DurableSpan) -> bool {
    span.tenant == query.tenant
        && query.trace_id.is_none_or(|value| value == span.trace_id)
        && query
            .start_time_unix_nanos
            .is_none_or(|start| span.end_time_unix_nanos().unwrap_or(u64::MAX) >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| span.start_time_unix_nanos < end)
        && query
            .min_duration_nanos
            .is_none_or(|duration| span.duration_nanos >= duration)
}

fn same_span_payload(left: &DurableSpan, right: &DurableSpan) -> bool {
    let mut normalized = right.clone();
    normalized.record_ref.offset = left.record_ref.offset;
    left == &normalized
}

fn retain_newest_span(
    winners: &mut BTreeMap<(Arc<str>, TraceId, SpanId), DurableSpan>,
    span: DurableSpan,
) {
    let key = (Arc::clone(&span.tenant), span.trace_id, span.span_id);
    if winners
        .get(&key)
        .is_none_or(|existing| existing.record_ref.offset < span.record_ref.offset)
    {
        winners.insert(key, span);
    }
}

fn retain_exact_trace_span(winners: &mut BTreeMap<SpanId, DurableSpan>, span: DurableSpan) {
    if winners
        .get(&span.span_id)
        .is_none_or(|existing| existing.record_ref.offset < span.record_ref.offset)
    {
        winners.insert(span.span_id, span);
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
    fn ready_traces_share_a_bounded_columnar_block() {
        let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
        stripe.apply(span(1, 1, 1), 10).unwrap();
        stripe.apply(span(2, 2, 2), 10).unwrap();
        let blocks = stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(decode_trace_block(&blocks[0]).unwrap().len(), 2);
        assert_eq!(stripe.pending_blocks.len(), 1);
        assert_eq!(
            stripe.pending_blocks[0].min_signal_identity,
            u128::from_be_bytes([1; 16])
        );
        assert_eq!(
            stripe.pending_blocks[0].max_signal_identity,
            u128::from_be_bytes([2; 16])
        );
        for trace_byte in [1, 2] {
            let summaries = stripe.directory().query(&TraceQuery {
                tenant: Arc::from("tenant-a"),
                trace_id: Some(TraceId::from_bytes([trace_byte; 16]).unwrap()),
                limit: 1,
                ..TraceQuery::default()
            });
            assert_eq!(summaries[0].block_fragments.as_ref(), &[1]);
        }
    }

    #[test]
    fn trace_sidecar_interner_promotes_without_losing_high_cardinality_values() {
        let records = (1..=40)
            .map(|ordinal| {
                let mut record = span(ordinal, ordinal as u8, ordinal as u8);
                record.name = Arc::from(format!("operation-{ordinal}"));
                record.attributes = Arc::new(vec![TelemetryAttribute::new(
                    "request.id",
                    crate::TelemetryValue::String(Arc::from(format!("request-{ordinal}"))),
                )]);
                record
            })
            .collect::<Vec<_>>();
        let encoded = encode_trace_block(&records).unwrap();
        let decoded = decode_trace_block(&encoded).unwrap();
        assert_eq!(decoded, records);
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
    fn retries_and_conflicts_remain_deterministic_after_sealing() {
        let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
        let original = span(1, 1, 1);
        stripe.apply(original.clone(), 10).unwrap();
        stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();

        let mut retry = original.clone();
        retry.record_ref.offset = LogicalOffset::new(2);
        assert_eq!(
            stripe
                .apply(retry, 10 + DEFAULT_TRACE_IDLE_NANOS + 1)
                .unwrap(),
            TraceApplyOutcome::Duplicate
        );

        let mut replacement = original.clone();
        replacement.record_ref.offset = LogicalOffset::new(3);
        replacement.name = Arc::from("changed");
        let replacement_time = 10 + DEFAULT_TRACE_IDLE_NANOS + 2;
        assert_eq!(
            stripe.apply(replacement, replacement_time).unwrap(),
            TraceApplyOutcome::Replaced
        );
        stripe
            .seal_idle(replacement_time + DEFAULT_TRACE_IDLE_NANOS)
            .unwrap();

        let mut obsolete = original;
        obsolete.record_ref.offset = LogicalOffset::new(2);
        obsolete.name = Arc::from("obsolete");
        assert_eq!(
            stripe
                .apply(obsolete, replacement_time + DEFAULT_TRACE_IDLE_NANOS + 1)
                .unwrap(),
            TraceApplyOutcome::Obsolete
        );

        let query = TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
            limit: 10,
            ..TraceQuery::default()
        };
        let summary = stripe.directory().query(&query);
        assert_eq!(summary[0].span_count, 1);
        assert_eq!(summary[0].block_fragments.as_ref(), &[1, 2]);
        let spans = stripe.query(&query).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name.as_ref(), "changed");
    }

    #[test]
    fn idle_trace_seals_and_becomes_directly_queryable() {
        let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
        stripe.apply(span(1, 1, 1), 10).unwrap();
        let blocks = stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();
        assert_eq!(blocks.len(), 1);
        let query = TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
            limit: 10,
            ..TraceQuery::default()
        };
        let summaries = stripe.directory().query(&query);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].span_count, 1);
        assert_eq!(stripe.query(&query).unwrap().len(), 1);
    }

    #[test]
    fn late_fragment_extends_the_trace_directory() {
        let mut stripe = TraceStripe::new(1024 * 1024).unwrap();
        stripe.apply(span(1, 1, 1), 10).unwrap();
        stripe.seal_idle(10 + DEFAULT_TRACE_IDLE_NANOS).unwrap();

        let late_append = 10 + DEFAULT_TRACE_IDLE_NANOS + 1;
        stripe.apply(span(2, 1, 2), late_append).unwrap();
        stripe
            .seal_idle(late_append + DEFAULT_TRACE_IDLE_NANOS)
            .unwrap();

        let query = TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(TraceId::from_bytes([1; 16]).unwrap()),
            limit: 10,
            ..TraceQuery::default()
        };
        let summaries = stripe.directory().query(&query);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].span_count, 2);
        assert_eq!(summaries[0].block_fragments.as_ref(), &[1, 2]);
        assert_eq!(stripe.query(&query).unwrap().len(), 2);
    }

    #[test]
    fn late_retry_state_stays_within_the_trace_head_budget() {
        let one_span_bytes = span(1, 1, 1).estimated_head_bytes();
        let budget = one_span_bytes.saturating_mul(2);
        let mut stripe = TraceStripe::new(budget).unwrap();
        let mut now = 1u64;
        for trace_byte in 1..=16 {
            stripe
                .apply(span(u64::from(trace_byte), trace_byte, 1), now)
                .unwrap();
            now = now.saturating_add(DEFAULT_TRACE_IDLE_NANOS);
            stripe.seal_idle(now).unwrap();
            now = now.saturating_add(1);
            assert!(stripe.head_bytes() <= budget);
        }
        assert!(stripe.recently_sealed.len() <= 2);
    }
}
