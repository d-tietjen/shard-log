use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{
    ResourceContext, ScopeContext, SeriesFingerprint, SignalTierPayload, SpanId,
    TelemetryAttribute, TelemetryError, TelemetryRecordRef, TelemetryResult, TelemetrySignal,
    TraceId,
};

const METRIC_CHUNK_MAGIC: [u8; 4] = *b"STMP";
const METRIC_CHUNK_VERSION: u8 = 1;
const METRIC_PCO_LEVEL: usize = 8;
const METRIC_SIDECAR_ZSTD_LEVEL: i32 = 1;
const DEFAULT_OUT_OF_ORDER_NANOS: u64 = 10 * 60 * 1_000_000_000;
const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_CHUNK_POINTS: usize = 4_096;
const DEFAULT_CHUNK_NANOS: u64 = 2 * 60 * 60 * 1_000_000_000;

/// Exact scalar number used by gauges, sums, and exemplars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumberValue {
    /// Signed integer sample.
    Integer(i64),
    /// Exact IEEE-754 double bits.
    DoubleBits(u64),
}

/// Exact integer or floating-point histogram count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistogramCount {
    /// Integer count used by OTLP and integer Prometheus native histograms.
    Integer(u64),
    /// Exact IEEE-754 count bits used by Prometheus float histograms.
    DoubleBits(u64),
}

impl NumberValue {
    /// Creates a bit-exact floating-point sample.
    #[must_use]
    pub const fn from_f64(value: f64) -> Self {
        Self::DoubleBits(value.to_bits())
    }
}

/// One metric exemplar nested under a point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricExemplar {
    /// Filtered attributes in wire order.
    pub filtered_attributes: Arc<Vec<TelemetryAttribute>>,
    /// Exemplar timestamp.
    pub timestamp_unix_nanos: u64,
    /// Exact scalar value.
    pub value: NumberValue,
    /// Optional binary span ID.
    pub span_id: Option<SpanId>,
    /// Optional binary trace ID.
    pub trace_id: Option<TraceId>,
}

/// Explicit histogram point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplicitHistogramValue {
    /// Number of observations.
    pub count: HistogramCount,
    /// Exact optional sum bits.
    pub sum_bits: Option<u64>,
    /// Bucket counts.
    pub bucket_counts: Arc<Vec<HistogramCount>>,
    /// Exact explicit-bound bits.
    pub explicit_bounds_bits: Arc<Vec<u64>>,
    /// Exact optional minimum bits.
    pub min_bits: Option<u64>,
    /// Exact optional maximum bits.
    pub max_bits: Option<u64>,
    /// Prometheus reset hint; zero for OTLP explicit histograms.
    pub reset_hint: i32,
}

/// Positive or negative exponential-histogram buckets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExponentialHistogramBuckets {
    /// Sparse bucket spans in wire order.
    pub spans: Arc<Vec<HistogramBucketSpan>>,
    /// Bucket counts corresponding to the concatenated spans.
    pub bucket_counts: Arc<Vec<HistogramCount>>,
}

/// One sparse native-histogram bucket span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramBucketSpan {
    /// Gap from the prior span, or starting bucket for the first span.
    pub offset: i32,
    /// Number of consecutive buckets.
    pub length: u32,
}

/// Exponential histogram point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExponentialHistogramValue {
    /// Number of observations.
    pub count: HistogramCount,
    /// Exact optional sum bits.
    pub sum_bits: Option<u64>,
    /// Base-2 scale.
    pub scale: i32,
    /// Count of exact zero values.
    pub zero_count: HistogramCount,
    /// Positive buckets.
    pub positive: Option<ExponentialHistogramBuckets>,
    /// Negative buckets.
    pub negative: Option<ExponentialHistogramBuckets>,
    /// Exact optional minimum bits.
    pub min_bits: Option<u64>,
    /// Exact optional maximum bits.
    pub max_bits: Option<u64>,
    /// Exact zero-threshold bits.
    pub zero_threshold_bits: u64,
    /// Prometheus reset hint; zero for OTLP exponential histograms.
    pub reset_hint: i32,
}

/// One legacy summary quantile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryQuantileValue {
    /// Exact quantile bits.
    pub quantile_bits: u64,
    /// Exact value bits.
    pub value_bits: u64,
}

/// Legacy summary point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryValue {
    /// Number of observations.
    pub count: u64,
    /// Exact sum bits.
    pub sum_bits: u64,
    /// Quantile values in wire order.
    pub quantiles: Arc<Vec<SummaryQuantileValue>>,
}

/// Signal-native metric point payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricValue {
    /// Gauge sample.
    Gauge(NumberValue),
    /// Sum sample. Temporality and monotonicity are part of series identity.
    Sum(NumberValue),
    /// Explicit histogram sample.
    ExplicitHistogram(ExplicitHistogramValue),
    /// Exponential histogram sample.
    ExponentialHistogram(ExponentialHistogramValue),
    /// Legacy summary sample.
    Summary(SummaryValue),
}

/// Metric instrument identity fields that are common to a series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricKind {
    /// Gauge.
    Gauge,
    /// Sum with raw OTLP temporality and monotonicity.
    Sum {
        /// OTLP aggregation temporality enum value.
        temporality: i32,
        /// Whether the sum is monotonic.
        monotonic: bool,
    },
    /// Explicit histogram with raw OTLP temporality.
    ExplicitHistogram {
        /// OTLP aggregation temporality enum value.
        temporality: i32,
    },
    /// Exponential histogram with raw OTLP temporality.
    ExponentialHistogram {
        /// OTLP aggregation temporality enum value.
        temporality: i32,
    },
    /// Legacy cumulative summary.
    Summary,
}

/// Canonical identity of one metric series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricIdentity {
    /// Authenticated tenant.
    pub tenant: Arc<str>,
    /// Resource context.
    pub resource: Arc<ResourceContext>,
    /// Instrumentation scope context.
    pub scope: Arc<ScopeContext>,
    /// Metric name.
    pub name: Arc<str>,
    /// Metric unit.
    pub unit: Arc<str>,
    /// Metric kind, temporality, and monotonicity.
    pub kind: MetricKind,
    /// Exact point attributes. Their canonical sorted representation defines identity.
    pub point_attributes: Arc<Vec<TelemetryAttribute>>,
}

impl MetricIdentity {
    /// Computes the process-independent canonical series fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        let mut canonical = Vec::new();
        append_bytes(&mut canonical, self.tenant.as_bytes());
        self.resource.append_identity(&mut canonical);
        self.scope.append_identity(&mut canonical);
        append_bytes(&mut canonical, self.name.as_bytes());
        append_bytes(&mut canonical, self.unit.as_bytes());
        let kind = rmp_serde::to_vec(&self.kind).expect("in-memory metric kind serializes");
        append_bytes(&mut canonical, &kind);
        let mut attributes = self
            .point_attributes
            .iter()
            .map(|attribute| {
                let mut bytes = Vec::new();
                attribute.append_canonical(&mut bytes);
                bytes
            })
            .collect::<Vec<_>>();
        attributes.sort_unstable();
        for attribute in attributes {
            append_bytes(&mut canonical, &attribute);
        }
        SeriesFingerprint::from_canonical(&canonical)
    }
}

/// One durable raw metric point. Exactly one point consumes one logical offset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableMetricPoint {
    /// Physical shard-stream owner stripe.
    pub stream_shard_id: ShardId,
    /// Durable signal-aware address.
    pub record_ref: TelemetryRecordRef,
    /// Canonical series identity.
    pub identity: Arc<MetricIdentity>,
    /// Description metadata, deliberately excluded from series identity.
    pub description: Arc<str>,
    /// Non-identifying OTLP metric metadata.
    pub metadata: Arc<Vec<TelemetryAttribute>>,
    /// Optional start timestamp.
    pub start_time_unix_nanos: u64,
    /// Required sample timestamp.
    pub timestamp_unix_nanos: u64,
    /// Point flags, including stale-marker flags and unknown future bits.
    pub flags: u32,
    /// Exact raw point payload.
    pub value: MetricValue,
    /// Nested exemplars.
    pub exemplars: Arc<Vec<MetricExemplar>>,
}

impl DurableMetricPoint {
    /// Returns this point's canonical series fingerprint.
    #[must_use]
    pub fn series_fingerprint(&self) -> SeriesFingerprint {
        self.identity.fingerprint()
    }

    fn estimated_head_bytes(&self) -> usize {
        rmp_serde::to_vec(self).map_or(usize::MAX, |value| value.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetricPointSidecar {
    description: Arc<str>,
    metadata: Arc<Vec<TelemetryAttribute>>,
    start_time_unix_nanos: u64,
    flags: u32,
    exemplars: Arc<Vec<MetricExemplar>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetricChunkSidecars {
    identity: Arc<MetricIdentity>,
    points: Vec<MetricPointSidecar>,
}

/// Encodes one series' sorted points into a signal-native metric chunk.
pub fn encode_metric_chunk(points: &[DurableMetricPoint]) -> TelemetryResult<Vec<u8>> {
    let Some(first) = points.first() else {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk must contain at least one point",
        ));
    };
    let fingerprint = first.series_fingerprint();
    let partition = first.record_ref.topic_partition;
    let stream_shard_id = first.stream_shard_id;
    if points.iter().any(|point| {
        point.record_ref.signal != TelemetrySignal::Metrics
            || point.record_ref.topic_partition != partition
            || point.stream_shard_id != stream_shard_id
            || point.series_fingerprint() != fingerprint
    }) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk points do not share a series, partition, and owner",
        ));
    }
    let mut sorted = points.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
    let offsets = sorted
        .iter()
        .map(|point| point.record_ref.offset.get())
        .collect::<Vec<_>>();
    let timestamps = sorted
        .iter()
        .map(|point| point.timestamp_unix_nanos)
        .collect::<Vec<_>>();
    let values = encode_metric_values(&sorted)?;
    let sidecars = MetricChunkSidecars {
        identity: Arc::clone(&first.identity),
        points: sorted
            .iter()
            .map(|point| MetricPointSidecar {
                description: Arc::clone(&point.description),
                metadata: Arc::clone(&point.metadata),
                start_time_unix_nanos: point.start_time_unix_nanos,
                flags: point.flags,
                exemplars: Arc::clone(&point.exemplars),
            })
            .collect(),
    };
    let sidecar_bytes = rmp_serde::to_vec_named(&sidecars)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed_sidecars = zstd::bulk::compress(&sidecar_bytes, METRIC_SIDECAR_ZSTD_LEVEL)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&METRIC_CHUNK_MAGIC);
    encoded.push(METRIC_CHUNK_VERSION);
    encoded.extend_from_slice(&[0; 3]);
    encoded.extend_from_slice(&stream_shard_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.topic_id.get().to_le_bytes());
    encoded.extend_from_slice(&partition.partition_id.get().to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(sorted.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&fingerprint.get().to_le_bytes());
    for section in [
        compress_u64(&offsets)?,
        encode_timestamp_delta_of_delta(&timestamps)?,
        values,
        compressed_sidecars,
    ] {
        append_section(&mut encoded, &section)?;
    }
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

/// Decodes and verifies one signal-native metric chunk.
pub fn decode_metric_chunk(encoded: &[u8]) -> TelemetryResult<Vec<DurableMetricPoint>> {
    const FIXED_HEADER: usize = 52;
    if encoded.len() < FIXED_HEADER + 32 || encoded[..4] != METRIC_CHUNK_MAGIC {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing metric chunk header",
        ));
    }
    if encoded[4] != METRIC_CHUNK_VERSION || encoded[5..8] != [0, 0, 0] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "unsupported metric chunk version or flags",
        ));
    }
    let payload_end = encoded.len() - 32;
    if blake3::hash(&encoded[..payload_end]).as_bytes() != &encoded[payload_end..] {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk checksum mismatch",
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
    let expected_fingerprint = SeriesFingerprint::from_raw(u128::from_le_bytes(
        encoded[36..52].try_into().expect("fixed range"),
    ));
    if count == 0 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric chunk has no points",
        ));
    }
    let mut cursor = FIXED_HEADER;
    let offsets = decompress_u64(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let timestamps =
        decode_timestamp_delta_of_delta(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let values = decode_metric_values(read_section(encoded, &mut cursor, payload_end)?, count)?;
    let compressed_sidecars = read_section(encoded, &mut cursor, payload_end)?;
    if cursor != payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing metric chunk sections",
        ));
    }
    let sidecar_bytes = zstd::bulk::decompress(compressed_sidecars, 64 * 1024 * 1024)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric sidecar compression"))?;
    let sidecars: MetricChunkSidecars = rmp_serde::from_slice(&sidecar_bytes)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric sidecars"))?;
    if sidecars.points.len() != count || sidecars.identity.fingerprint() != expected_fingerprint {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric sidecar identity or count mismatch",
        ));
    }
    Ok(offsets
        .into_iter()
        .zip(timestamps)
        .zip(values)
        .zip(sidecars.points)
        .map(
            |(((offset, timestamp_unix_nanos), value), sidecar)| DurableMetricPoint {
                stream_shard_id,
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    topic_partition,
                    LogicalOffset::new(offset),
                ),
                identity: Arc::clone(&sidecars.identity),
                description: sidecar.description,
                metadata: sidecar.metadata,
                start_time_unix_nanos: sidecar.start_time_unix_nanos,
                timestamp_unix_nanos,
                flags: sidecar.flags,
                value,
                exemplars: sidecar.exemplars,
            },
        )
        .collect())
}

fn encode_metric_values(points: &[&DurableMetricPoint]) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::new();
    let mut previous_float = 0u64;
    for point in points {
        match &point.value {
            MetricValue::Gauge(value) => {
                encoded.push(0);
                encode_number(*value, &mut previous_float, &mut encoded);
            }
            MetricValue::Sum(value) => {
                encoded.push(1);
                encode_number(*value, &mut previous_float, &mut encoded);
            }
            MetricValue::ExplicitHistogram(value) => {
                encoded.push(2);
                append_messagepack(&mut encoded, value)?;
            }
            MetricValue::ExponentialHistogram(value) => {
                encoded.push(3);
                append_messagepack(&mut encoded, value)?;
            }
            MetricValue::Summary(value) => {
                encoded.push(4);
                append_messagepack(&mut encoded, value)?;
            }
        }
    }
    Ok(encoded)
}

fn decode_metric_values(encoded: &[u8], count: usize) -> TelemetryResult<Vec<MetricValue>> {
    let mut cursor = 0;
    let mut previous_float = 0u64;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(match read_byte(encoded, &mut cursor)? {
            0 => MetricValue::Gauge(decode_number(encoded, &mut cursor, &mut previous_float)?),
            1 => MetricValue::Sum(decode_number(encoded, &mut cursor, &mut previous_float)?),
            2 => MetricValue::ExplicitHistogram(read_messagepack(encoded, &mut cursor)?),
            3 => MetricValue::ExponentialHistogram(read_messagepack(encoded, &mut cursor)?),
            4 => MetricValue::Summary(read_messagepack(encoded, &mut cursor)?),
            _ => {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid metric value tag",
                ));
            }
        });
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing metric value bytes",
        ));
    }
    Ok(values)
}

fn encode_number(value: NumberValue, previous_float: &mut u64, encoded: &mut Vec<u8>) {
    match value {
        NumberValue::Integer(value) => {
            encoded.push(0);
            write_varint(zigzag_i64(value), encoded);
        }
        NumberValue::DoubleBits(bits) => {
            encoded.push(1);
            write_varint(bits ^ *previous_float, encoded);
            *previous_float = bits;
        }
    }
}

fn decode_number(
    encoded: &[u8],
    cursor: &mut usize,
    previous_float: &mut u64,
) -> TelemetryResult<NumberValue> {
    match read_byte(encoded, cursor)? {
        0 => Ok(NumberValue::Integer(unzigzag_i64(read_varint(
            encoded, cursor,
        )?))),
        1 => {
            let bits = read_varint(encoded, cursor)? ^ *previous_float;
            *previous_float = bits;
            Ok(NumberValue::DoubleBits(bits))
        }
        _ => Err(TelemetryError::InvalidBlockEncoding(
            "invalid metric number tag",
        )),
    }
}

fn append_messagepack<T: Serialize>(encoded: &mut Vec<u8>, value: &T) -> TelemetryResult<()> {
    let bytes = rmp_serde::to_vec(value)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    append_section(encoded, &bytes)
}

fn read_messagepack<T: for<'de> Deserialize<'de>>(
    encoded: &[u8],
    cursor: &mut usize,
) -> TelemetryResult<T> {
    let section = read_section(encoded, cursor, encoded.len())?;
    rmp_serde::from_slice(section)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric value payload"))
}

fn encode_timestamp_delta_of_delta(timestamps: &[u64]) -> TelemetryResult<Vec<u8>> {
    let mut encoded = Vec::new();
    let Some(&first) = timestamps.first() else {
        return Ok(encoded);
    };
    encoded.extend_from_slice(&first.to_le_bytes());
    if timestamps.len() == 1 {
        return Ok(encoded);
    }
    let mut previous_delta =
        timestamps[1]
            .checked_sub(first)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamps are not sorted",
            ))?;
    write_varint(previous_delta, &mut encoded);
    let mut previous = timestamps[1];
    for &timestamp in &timestamps[2..] {
        let delta = timestamp
            .checked_sub(previous)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamps are not sorted",
            ))?;
        let delta_of_delta = i128::from(delta) - i128::from(previous_delta);
        write_varint128(zigzag_i128(delta_of_delta), &mut encoded);
        previous = timestamp;
        previous_delta = delta;
    }
    Ok(encoded)
}

fn decode_timestamp_delta_of_delta(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    if count == 0 || encoded.len() < 8 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid metric timestamp lane",
        ));
    }
    let mut cursor = 8;
    let first = u64::from_le_bytes(encoded[..8].try_into().expect("fixed range"));
    let mut timestamps = Vec::with_capacity(count);
    timestamps.push(first);
    if count == 1 {
        if cursor != encoded.len() {
            return Err(TelemetryError::InvalidBlockEncoding(
                "trailing metric timestamp bytes",
            ));
        }
        return Ok(timestamps);
    }
    let mut previous_delta = read_varint(encoded, &mut cursor)?;
    let mut previous =
        first
            .checked_add(previous_delta)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamp overflow",
            ))?;
    timestamps.push(previous);
    for _ in 2..count {
        let delta_of_delta = unzigzag_i128(read_varint128(encoded, &mut cursor)?);
        let delta = i128::from(previous_delta)
            .checked_add(delta_of_delta)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamp delta overflow",
            ))?;
        previous = previous
            .checked_add(delta)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "metric timestamp overflow",
            ))?;
        timestamps.push(previous);
        previous_delta = delta;
    }
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding(
            "trailing metric timestamp bytes",
        ));
    }
    Ok(timestamps)
}

fn compress_u64(values: &[u64]) -> TelemetryResult<Vec<u8>> {
    simple_compress(
        values,
        &ChunkConfig::default().with_compression_level(METRIC_PCO_LEVEL),
    )
    .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
}

fn decompress_u64(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    let mut values = vec![0; count];
    let progress = simple_decompress_into(encoded, &mut values)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric Pco lane"))?;
    if progress.n_processed != count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric Pco lane count mismatch",
        ));
    }
    Ok(values)
}

fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    output.extend_from_slice(bytes);
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
            "truncated metric section length",
        ));
    }
    let len = u32::from_le_bytes(
        encoded[*cursor..*cursor + 4]
            .try_into()
            .expect("fixed range"),
    ) as usize;
    *cursor += 4;
    let end = (*cursor)
        .checked_add(len)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "metric section length overflow",
        ))?;
    if end > payload_end {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated metric section",
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
            "truncated metric value",
        ))?;
    *cursor += 1;
    Ok(value)
}

fn write_varint(mut value: u64, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

fn read_varint(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = read_byte(encoded, cursor)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(TelemetryError::InvalidBlockEncoding(
        "metric varint overflow",
    ))
}

fn write_varint128(mut value: u128, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

fn read_varint128(encoded: &[u8], cursor: &mut usize) -> TelemetryResult<u128> {
    let mut value = 0u128;
    for shift in (0..128).step_by(7) {
        let byte = read_byte(encoded, cursor)?;
        value |= u128::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(TelemetryError::InvalidBlockEncoding(
        "metric wide varint overflow",
    ))
}

const fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

const fn unzigzag_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

const fn zigzag_i128(value: i128) -> u128 {
    ((value << 1) ^ (value >> 127)) as u128
}

const fn unzigzag_i128(value: u128) -> i128 {
    ((value >> 1) as i128) ^ -((value & 1) as i128)
}

/// Ingestion semantics used for same-timestamp sample conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricIngestProtocol {
    /// OTLP overlaps use highest durable offset with diagnostics.
    Otlp,
    /// Prometheus Remote Write rejects conflicting same-timestamp values.
    RemoteWrite,
}

impl MetricIngestProtocol {
    pub(crate) const fn to_wire(self) -> u8 {
        match self {
            Self::Otlp => 1,
            Self::RemoteWrite => 2,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> TelemetryResult<Self> {
        match value {
            1 => Ok(Self::Otlp),
            2 => Ok(Self::RemoteWrite),
            _ => Err(TelemetryError::InvalidTelemetryEnvelope(
                "unknown metric ingestion protocol",
            )),
        }
    }
}

/// Result of applying one raw metric point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricApplyOutcome {
    /// New point inserted.
    Inserted,
    /// Byte-identical retry ignored.
    Duplicate,
    /// OTLP conflict replaced by a higher durable offset.
    Replaced,
    /// Older OTLP conflict ignored.
    Obsolete,
    /// Accepted into the out-of-order delta region.
    OutOfOrder,
}

/// Checkpointed delta-to-cumulative state for PromQL views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesAccumulatorCheckpoint {
    /// Canonical series fingerprint.
    pub series: SeriesFingerprint,
    /// Logical partition that owns the complete series.
    pub topic_partition: TopicPartition,
    /// Reset generation.
    pub reset_generation: u64,
    /// Latest raw timestamp.
    pub latest_timestamp_unix_nanos: u64,
    /// Exact cumulative value when the series is numeric.
    pub cumulative: Option<NumberValue>,
}

#[derive(Debug)]
struct SeriesHead {
    topic_partition: TopicPartition,
    identity: Arc<MetricIdentity>,
    points: BTreeMap<(u64, LogicalOffset), DurableMetricPoint>,
    latest_timestamp: u64,
    bytes: usize,
    reset_generation: u64,
    cumulative: Option<NumberValue>,
    conflicts: u64,
}

/// Bounded single-writer metric head and immutable raw chunk collection.
#[derive(Debug)]
pub struct MetricStripe {
    head_budget_bytes: usize,
    head_bytes: usize,
    out_of_order_nanos: u64,
    chunk_bytes: usize,
    chunk_points: usize,
    chunk_nanos: u64,
    series: HashMap<SeriesFingerprint, SeriesHead>,
    chunks: HashMap<SeriesFingerprint, Vec<SealedMetricChunk>>,
    pending_chunks: Vec<SignalTierPayload>,
    next_chunk_id: u64,
    recovered_accumulators: HashMap<SeriesFingerprint, SeriesAccumulatorCheckpoint>,
    name_index: HashMap<Arc<str>, HashSet<SeriesFingerprint>>,
    label_index: HashMap<(Arc<str>, Arc<str>), HashSet<SeriesFingerprint>>,
}

#[derive(Debug)]
struct SealedMetricChunk {
    resident_id: u64,
    min_timestamp_unix_nanos: u64,
    max_timestamp_unix_nanos: u64,
    payload: Arc<[u8]>,
}

impl MetricStripe {
    /// Creates a bounded metric stripe using production chunk and OOO limits.
    pub fn new(head_budget_bytes: usize) -> TelemetryResult<Self> {
        if head_budget_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "metric head budget must be nonzero",
            ));
        }
        Ok(Self {
            head_budget_bytes,
            head_bytes: 0,
            out_of_order_nanos: DEFAULT_OUT_OF_ORDER_NANOS,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            chunk_points: DEFAULT_CHUNK_POINTS,
            chunk_nanos: DEFAULT_CHUNK_NANOS,
            series: HashMap::new(),
            chunks: HashMap::new(),
            pending_chunks: Vec::new(),
            next_chunk_id: 1,
            recovered_accumulators: HashMap::new(),
            name_index: HashMap::new(),
            label_index: HashMap::new(),
        })
    }

    /// Applies one raw metric point under OTLP or Remote Write conflict rules.
    pub fn apply(
        &mut self,
        point: DurableMetricPoint,
        protocol: MetricIngestProtocol,
    ) -> TelemetryResult<MetricApplyOutcome> {
        if point.record_ref.signal != TelemetrySignal::Metrics {
            return Err(TelemetryError::InvalidBlockEncoding(
                "non-metric record applied to metric stripe",
            ));
        }
        let fingerprint = point.series_fingerprint();
        if self.series.get(&fingerprint).is_some_and(|head| {
            head.identity.as_ref() != point.identity.as_ref()
                || head.topic_partition != point.record_ref.topic_partition
        }) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "series fingerprint collision",
            ));
        }
        if !self.series.contains_key(&fingerprint) {
            self.name_index
                .entry(Arc::clone(&point.identity.name))
                .or_default()
                .insert(fingerprint);
            for (name, value) in prometheus_string_labels(&point.identity) {
                self.label_index
                    .entry((name, value))
                    .or_default()
                    .insert(fingerprint);
            }
        }
        let estimated = point.estimated_head_bytes();
        if estimated > self.head_budget_bytes {
            return Err(TelemetryError::RecordTooLarge);
        }
        if self.head_bytes.saturating_add(estimated) > self.head_budget_bytes {
            self.seal_largest_head()?;
        }
        if self.head_bytes.saturating_add(estimated) > self.head_budget_bytes {
            return Err(TelemetryError::InvalidConfig(
                "metric head memory budget exhausted",
            ));
        }
        let recovered = self.recovered_accumulators.get(&fingerprint).cloned();
        if recovered.as_ref().is_some_and(|checkpoint| {
            checkpoint.topic_partition != point.record_ref.topic_partition
        }) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "recovered metric accumulator belongs to another partition",
            ));
        }
        self.recovered_accumulators.remove(&fingerprint);
        let sealed_same_timestamp =
            self.sealed_points_at(fingerprint, point.timestamp_unix_nanos)?;
        let (outcome, should_seal) = {
            let head = self
                .series
                .entry(fingerprint)
                .or_insert_with(|| SeriesHead {
                    topic_partition: point.record_ref.topic_partition,
                    identity: Arc::clone(&point.identity),
                    points: BTreeMap::new(),
                    latest_timestamp: recovered
                        .as_ref()
                        .map_or(point.timestamp_unix_nanos, |value| {
                            value.latest_timestamp_unix_nanos
                        }),
                    bytes: 0,
                    reset_generation: recovered.as_ref().map_or(0, |value| value.reset_generation),
                    cumulative: recovered.as_ref().and_then(|value| value.cumulative),
                    conflicts: 0,
                });
            if point
                .timestamp_unix_nanos
                .saturating_add(self.out_of_order_nanos)
                < head.latest_timestamp
            {
                return Err(TelemetryError::InvalidMetricSample(
                    "metric point exceeds the 10 minute out-of-order window".into(),
                ));
            }
            let mut same_timestamp = head
                .points
                .range(
                    (point.timestamp_unix_nanos, LogicalOffset::new(0))
                        ..=(point.timestamp_unix_nanos, LogicalOffset::new(u64::MAX)),
                )
                .map(|(key, value)| (*key, value.clone()))
                .collect::<Vec<_>>();
            same_timestamp.extend(sealed_same_timestamp.into_iter().map(|existing| {
                (
                    (existing.timestamp_unix_nanos, existing.record_ref.offset),
                    existing,
                )
            }));
            if same_timestamp.iter().any(|(_, existing)| {
                existing.value == point.value
                    && existing.flags == point.flags
                    && existing.exemplars == point.exemplars
                    && existing.start_time_unix_nanos == point.start_time_unix_nanos
            }) {
                return Ok(MetricApplyOutcome::Duplicate);
            }
            if let Some((_, winner)) = same_timestamp
                .iter()
                .max_by_key(|(_, existing)| existing.record_ref.offset)
            {
                if protocol == MetricIngestProtocol::RemoteWrite {
                    return Err(TelemetryError::MetricSampleConflict {
                        series: fingerprint.get(),
                        timestamp_unix_nanos: point.timestamp_unix_nanos,
                    });
                }
                head.conflicts = head.conflicts.saturating_add(1);
                if winner.record_ref.offset >= point.record_ref.offset {
                    return Ok(MetricApplyOutcome::Obsolete);
                }
                for (key, _) in &same_timestamp {
                    if let Some(removed) = head.points.remove(key) {
                        let bytes = removed.estimated_head_bytes();
                        head.bytes = head.bytes.saturating_sub(bytes);
                        self.head_bytes = self.head_bytes.saturating_sub(bytes);
                    }
                }
            }
            let out_of_order = point.timestamp_unix_nanos < head.latest_timestamp;
            let outcome = if same_timestamp.is_empty() {
                if out_of_order {
                    MetricApplyOutcome::OutOfOrder
                } else {
                    MetricApplyOutcome::Inserted
                }
            } else {
                MetricApplyOutcome::Replaced
            };
            head.latest_timestamp = head.latest_timestamp.max(point.timestamp_unix_nanos);
            update_accumulator(head, &point);
            head.bytes = head.bytes.saturating_add(estimated);
            self.head_bytes = self.head_bytes.saturating_add(estimated);
            head.points
                .insert((point.timestamp_unix_nanos, point.record_ref.offset), point);
            let should_seal = head.points.len() >= self.chunk_points
                || head.bytes >= self.chunk_bytes
                || head
                    .points
                    .first_key_value()
                    .is_some_and(|((timestamp, _), _)| {
                        head.latest_timestamp.saturating_sub(*timestamp) >= self.chunk_nanos
                    });
            (outcome, should_seal)
        };
        if should_seal {
            self.seal_series(fingerprint)?;
        }
        Ok(outcome)
    }

    /// Serializes exact accumulator checkpoints for restart recovery.
    pub fn accumulator_checkpoints(&self) -> TelemetryResult<Vec<u8>> {
        let mut checkpoints = self
            .recovered_accumulators
            .values()
            .cloned()
            .collect::<Vec<_>>();
        checkpoints.extend(
            self.series
                .iter()
                .map(|(series, head)| SeriesAccumulatorCheckpoint {
                    series: *series,
                    topic_partition: head.topic_partition,
                    reset_generation: head.reset_generation,
                    latest_timestamp_unix_nanos: head.latest_timestamp,
                    cumulative: head.cumulative,
                }),
        );
        checkpoints.sort_unstable_by_key(|checkpoint| checkpoint.series);
        rmp_serde::to_vec_named(&checkpoints)
            .map_err(|error| TelemetryError::StorageIo(error.to_string()))
    }

    pub(crate) fn accumulator_checkpoints_for_partition(
        &self,
        partition: TopicPartition,
    ) -> TelemetryResult<Vec<u8>> {
        let mut checkpoints = self
            .recovered_accumulators
            .values()
            .filter(|checkpoint| checkpoint.topic_partition == partition)
            .cloned()
            .collect::<Vec<_>>();
        checkpoints.extend(self.series.iter().filter_map(|(series, head)| {
            (head.topic_partition == partition).then_some(SeriesAccumulatorCheckpoint {
                series: *series,
                topic_partition: head.topic_partition,
                reset_generation: head.reset_generation,
                latest_timestamp_unix_nanos: head.latest_timestamp,
                cumulative: head.cumulative,
            })
        }));
        checkpoints.sort_unstable_by_key(|checkpoint| checkpoint.series);
        rmp_serde::to_vec_named(&checkpoints)
            .map_err(|error| TelemetryError::StorageIo(error.to_string()))
    }

    /// Restores accumulator generations before accepting new points.
    pub fn restore_accumulator_checkpoints(&mut self, encoded: &[u8]) -> TelemetryResult<()> {
        let checkpoints: Vec<SeriesAccumulatorCheckpoint> = rmp_serde::from_slice(encoded)
            .map_err(|error| TelemetryError::StorageIo(error.to_string()))?;
        for checkpoint in checkpoints {
            if checkpoint.topic_partition.topic_id != TelemetrySignal::Metrics.topic_id() {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "metric accumulator checkpoint uses the wrong signal topic",
                ));
            }
            if let Some(head) = self.series.get_mut(&checkpoint.series) {
                if head.topic_partition != checkpoint.topic_partition {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "metric accumulator checkpoint changed partitions",
                    ));
                }
                head.reset_generation = checkpoint.reset_generation;
                head.latest_timestamp = checkpoint.latest_timestamp_unix_nanos;
                head.cumulative = checkpoint.cumulative;
            } else if self
                .recovered_accumulators
                .insert(checkpoint.series, checkpoint)
                .is_some()
            {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "duplicate metric accumulator checkpoint",
                ));
            }
        }
        Ok(())
    }

    /// Returns immutable raw chunks for one series.
    #[must_use]
    pub fn chunks(&self, series: SeriesFingerprint) -> Vec<Arc<[u8]>> {
        self.chunks
            .get(&series)
            .into_iter()
            .flatten()
            .map(|chunk| Arc::clone(&chunk.payload))
            .collect()
    }

    /// Returns current head memory accounting.
    #[must_use]
    pub const fn head_bytes(&self) -> usize {
        self.head_bytes
    }

    /// Queries exact raw hot and immutable-chunk points with index pushdown.
    pub fn query(&self, query: &MetricQuery) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let limit = query.limit.max(1);
        let mut winners = BTreeMap::<(SeriesFingerprint, u64), DurableMetricPoint>::new();
        let candidates = self.query_candidates(query);
        for (series, head) in &self.series {
            if candidates
                .as_ref()
                .is_some_and(|values| !values.contains(series))
                || query.series.is_some_and(|requested| requested != *series)
                || head.identity.tenant != query.tenant
                || query
                    .name
                    .as_ref()
                    .is_some_and(|name| name.as_ref() != head.identity.name.as_ref())
            {
                continue;
            }
            for point in head
                .points
                .values()
                .filter(|point| metric_query_matches(query, point))
            {
                retain_metric_winner(&mut winners, point.clone());
            }
        }
        for (series, chunks) in &self.chunks {
            if candidates
                .as_ref()
                .is_some_and(|values| !values.contains(series))
                || query.series.is_some_and(|requested| requested != *series)
            {
                continue;
            }
            for chunk in chunks {
                for point in decode_metric_chunk(&chunk.payload)?
                    .into_iter()
                    .filter(|point| metric_query_matches(query, point))
                {
                    retain_metric_winner(&mut winners, point);
                }
            }
        }
        let mut points = winners.into_values().collect::<Vec<_>>();
        points.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
        points.truncate(limit);
        Ok(points)
    }

    fn sealed_points_at(
        &self,
        series: SeriesFingerprint,
        timestamp_unix_nanos: u64,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let mut points = Vec::new();
        for chunk in self
            .chunks
            .get(&series)
            .into_iter()
            .flatten()
            .filter(|chunk| {
                chunk.min_timestamp_unix_nanos <= timestamp_unix_nanos
                    && timestamp_unix_nanos <= chunk.max_timestamp_unix_nanos
            })
        {
            points.extend(
                decode_metric_chunk(&chunk.payload)?
                    .into_iter()
                    .filter(|point| point.timestamp_unix_nanos == timestamp_unix_nanos),
            );
        }
        Ok(points)
    }

    fn query_candidates(&self, query: &MetricQuery) -> Option<HashSet<SeriesFingerprint>> {
        let mut candidate = query
            .name
            .as_ref()
            .map(|name| self.name_index.get(name).cloned().unwrap_or_default());
        for (name, value) in query.exact_labels.iter() {
            let posting = self
                .label_index
                .get(&(Arc::clone(name), Arc::clone(value)))
                .cloned()
                .unwrap_or_default();
            candidate = Some(match candidate {
                Some(existing) => existing.intersection(&posting).copied().collect(),
                None => posting,
            });
        }
        candidate
    }

    fn seal_largest_head(&mut self) -> TelemetryResult<()> {
        let Some(series) = self
            .series
            .iter()
            .max_by_key(|(_, head)| head.bytes)
            .map(|(series, _)| *series)
        else {
            return Ok(());
        };
        self.seal_series(series)
    }

    /// Seals every mutable series head belonging to one logical metric partition.
    pub(crate) fn seal_partition(&mut self, partition: TopicPartition) -> TelemetryResult<()> {
        let series = self
            .series
            .iter()
            .filter_map(|(series, head)| (head.topic_partition == partition).then_some(*series))
            .collect::<Vec<_>>();
        for series in series {
            self.seal_series(series)?;
        }
        Ok(())
    }

    fn seal_series(&mut self, series: SeriesFingerprint) -> TelemetryResult<()> {
        let Some(head) = self.series.get_mut(&series) else {
            return Ok(());
        };
        if head.points.is_empty() {
            return Ok(());
        }
        let points = std::mem::take(&mut head.points)
            .into_values()
            .collect::<Vec<_>>();
        self.head_bytes = self.head_bytes.saturating_sub(head.bytes);
        head.bytes = 0;
        let encoded = encode_metric_chunk(&points)?;
        let resident_id = self.next_chunk_id;
        self.next_chunk_id = self.next_chunk_id.saturating_add(1);
        let payload = Arc::<[u8]>::from(encoded);
        let first_offset = points
            .iter()
            .map(|point| point.record_ref.offset.get())
            .min()
            .expect("sealed metric chunk is nonempty");
        let last_offset = points
            .iter()
            .map(|point| point.record_ref.offset.get())
            .max()
            .expect("sealed metric chunk is nonempty");
        let min_timestamp_unix_nanos = points
            .iter()
            .map(|point| point.timestamp_unix_nanos)
            .min()
            .expect("sealed metric chunk is nonempty");
        let max_timestamp_unix_nanos = points
            .iter()
            .map(|point| point.timestamp_unix_nanos)
            .max()
            .expect("sealed metric chunk is nonempty");
        self.pending_chunks.push(SignalTierPayload {
            resident_id,
            topic_partition: head.topic_partition,
            signal_identity: series.get(),
            first_offset,
            last_offset,
            record_count: u32::try_from(points.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            payload: Arc::clone(&payload),
        });
        self.chunks
            .entry(series)
            .or_default()
            .push(SealedMetricChunk {
                resident_id,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                payload,
            });
        Ok(())
    }

    pub(crate) fn pending_partition(&self, partition: TopicPartition) -> Vec<SignalTierPayload> {
        self.pending_chunks
            .iter()
            .filter(|payload| payload.topic_partition == partition)
            .cloned()
            .collect()
    }

    pub(crate) fn release_published_chunks(&mut self, resident_ids: &[u64]) {
        self.pending_chunks
            .retain(|payload| !resident_ids.contains(&payload.resident_id));
        self.chunks.retain(|_, chunks| {
            chunks.retain(|chunk| !resident_ids.contains(&chunk.resident_id));
            !chunks.is_empty()
        });
    }

    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.chunks
            .values()
            .flatten()
            .map(|chunk| u64::try_from(chunk.payload.len()).unwrap_or(u64::MAX))
            .sum()
    }
}

fn update_accumulator(head: &mut SeriesHead, point: &DurableMetricPoint) {
    let MetricKind::Sum {
        temporality,
        monotonic: _,
    } = point.identity.kind
    else {
        return;
    };
    let MetricValue::Sum(value) = point.value else {
        return;
    };
    if temporality == 1 {
        head.cumulative = match (head.cumulative, value) {
            (Some(NumberValue::Integer(left)), NumberValue::Integer(right)) => {
                left.checked_add(right).map(NumberValue::Integer)
            }
            (Some(NumberValue::DoubleBits(left)), NumberValue::DoubleBits(right)) => Some(
                NumberValue::from_f64(f64::from_bits(left) + f64::from_bits(right)),
            ),
            (None, value) => Some(value),
            _ => {
                head.reset_generation = head.reset_generation.saturating_add(1);
                Some(value)
            }
        };
    } else {
        head.cumulative = Some(value);
    }
}

fn retain_metric_winner(
    winners: &mut BTreeMap<(SeriesFingerprint, u64), DurableMetricPoint>,
    point: DurableMetricPoint,
) {
    let key = (point.series_fingerprint(), point.timestamp_unix_nanos);
    if winners
        .get(&key)
        .is_none_or(|winner| winner.record_ref.offset < point.record_ref.offset)
    {
        winners.insert(key, point);
    }
}

/// Native metric selector used by PromQL storage scans and direct APIs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricQuery {
    /// Required tenant.
    pub tenant: Arc<str>,
    /// Optional exact series fingerprint.
    pub series: Option<SeriesFingerprint>,
    /// Optional exact metric name.
    pub name: Option<Arc<str>>,
    /// Exact Prometheus labels pushed into the stripe-local inverted index.
    pub exact_labels: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Inclusive start time.
    pub start_time_unix_nanos: Option<u64>,
    /// Inclusive end time.
    pub end_time_unix_nanos: Option<u64>,
    /// Maximum raw points to return.
    pub limit: usize,
}

pub(crate) fn metric_query_matches(query: &MetricQuery, point: &DurableMetricPoint) -> bool {
    point.identity.tenant == query.tenant
        && query
            .name
            .as_ref()
            .is_none_or(|name| name.as_ref() == point.identity.name.as_ref())
        && query
            .start_time_unix_nanos
            .is_none_or(|start| point.timestamp_unix_nanos >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| point.timestamp_unix_nanos <= end)
        && query.exact_labels.iter().all(|(name, value)| {
            prometheus_string_labels(&point.identity).iter().any(
                |(observed_name, observed_value)| {
                    observed_name.as_ref() == name.as_ref()
                        && observed_value.as_ref() == value.as_ref()
                },
            )
        })
}

/// Returns Prometheus-visible string labels for one canonical series.
#[must_use]
pub fn prometheus_string_labels(identity: &MetricIdentity) -> Vec<(Arc<str>, Arc<str>)> {
    let mut labels = BTreeMap::<Arc<str>, Arc<str>>::new();
    for attribute in identity.resource.attributes.iter() {
        if let Some(crate::TelemetryValue::String(value)) = &attribute.value {
            labels.insert(Arc::clone(&attribute.key), Arc::clone(value));
        }
    }
    for attribute in identity.point_attributes.iter() {
        if let Some(crate::TelemetryValue::String(value)) = &attribute.value {
            labels.insert(Arc::clone(&attribute.key), Arc::clone(value));
        }
    }
    labels.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{METRICS_TOPIC_ID, TelemetryValue};

    fn point(offset: u64, timestamp: u64, value: NumberValue) -> DurableMetricPoint {
        let identity = Arc::new(MetricIdentity {
            tenant: Arc::from("tenant-a"),
            resource: Arc::new(ResourceContext::default()),
            scope: Arc::new(ScopeContext::default()),
            name: Arc::from("http.server.duration"),
            unit: Arc::from("ms"),
            kind: MetricKind::Gauge,
            point_attributes: Arc::new(vec![TelemetryAttribute::new(
                "service",
                TelemetryValue::String(Arc::from("api")),
            )]),
        });
        DurableMetricPoint {
            stream_shard_id: ShardId::new(2),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Metrics,
                TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(11)),
                LogicalOffset::new(offset),
            ),
            identity,
            description: Arc::from("request latency"),
            metadata: Arc::new(Vec::new()),
            start_time_unix_nanos: 0,
            timestamp_unix_nanos: timestamp,
            flags: 0,
            value: MetricValue::Gauge(value),
            exemplars: Arc::new(Vec::new()),
        }
    }

    #[test]
    fn series_fingerprint_is_attribute_order_independent() {
        let first = point(1, 1, NumberValue::Integer(1));
        let mut second = first.clone();
        let mut attributes = vec![
            TelemetryAttribute::new("z", TelemetryValue::Integer(1)),
            TelemetryAttribute::new("a", TelemetryValue::Integer(2)),
        ];
        second.identity = Arc::new(MetricIdentity {
            point_attributes: Arc::new(attributes.clone()),
            ..first.identity.as_ref().clone()
        });
        attributes.reverse();
        let third = MetricIdentity {
            point_attributes: Arc::new(attributes),
            ..second.identity.as_ref().clone()
        };
        assert_eq!(second.series_fingerprint(), third.fingerprint());
    }

    #[test]
    fn metric_chunk_round_trips_nan_payloads_and_out_of_order_input() {
        let points = vec![
            point(2, 200, NumberValue::DoubleBits(0x7ff8_0000_0000_0042)),
            point(1, 100, NumberValue::DoubleBits((-0.0f64).to_bits())),
        ];
        let encoded = encode_metric_chunk(&points).unwrap();
        let decoded = decode_metric_chunk(&encoded).unwrap();
        assert_eq!(decoded[0], points[1]);
        assert_eq!(decoded[1], points[0]);
    }

    #[test]
    fn remote_write_rejects_conflicts_while_otlp_uses_offset() {
        let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
        let first = point(1, 100, NumberValue::Integer(1));
        stripe
            .apply(first.clone(), MetricIngestProtocol::RemoteWrite)
            .unwrap();
        let conflicting = point(2, 100, NumberValue::Integer(2));
        assert!(matches!(
            stripe.apply(conflicting.clone(), MetricIngestProtocol::RemoteWrite),
            Err(TelemetryError::MetricSampleConflict { .. })
        ));
        assert_eq!(
            stripe
                .apply(conflicting, MetricIngestProtocol::Otlp)
                .unwrap(),
            MetricApplyOutcome::Replaced
        );
    }

    #[test]
    fn sealed_metric_conflicts_preserve_remote_write_and_offset_semantics() {
        let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
        stripe.chunk_points = 1;
        let first = point(1, 100, NumberValue::Integer(1));
        assert_eq!(
            stripe
                .apply(first.clone(), MetricIngestProtocol::RemoteWrite)
                .unwrap(),
            MetricApplyOutcome::Inserted
        );
        assert!(stripe.series[&first.series_fingerprint()].points.is_empty());

        let mut duplicate = first.clone();
        duplicate.record_ref.offset = LogicalOffset::new(2);
        assert_eq!(
            stripe
                .apply(duplicate, MetricIngestProtocol::RemoteWrite)
                .unwrap(),
            MetricApplyOutcome::Duplicate
        );

        let conflict = point(2, 100, NumberValue::Integer(2));
        assert!(matches!(
            stripe.apply(conflict.clone(), MetricIngestProtocol::RemoteWrite),
            Err(TelemetryError::MetricSampleConflict { .. })
        ));
        let mut obsolete = conflict.clone();
        obsolete.record_ref.offset = LogicalOffset::new(0);
        assert_eq!(
            stripe.apply(obsolete, MetricIngestProtocol::Otlp).unwrap(),
            MetricApplyOutcome::Obsolete
        );
        assert_eq!(
            stripe
                .apply(conflict.clone(), MetricIngestProtocol::Otlp)
                .unwrap(),
            MetricApplyOutcome::Replaced
        );

        let queried = stripe
            .query(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                series: Some(conflict.series_fingerprint()),
                limit: usize::MAX,
                ..MetricQuery::default()
            })
            .unwrap();
        assert_eq!(queried, vec![conflict]);
    }

    #[test]
    fn timestamp_delta_of_delta_round_trips_wide_changes() {
        let timestamps = [1, 2, 1_000_000_000_000, 1_000_000_000_001];
        let encoded = encode_timestamp_delta_of_delta(&timestamps).unwrap();
        assert_eq!(
            decode_timestamp_delta_of_delta(&encoded, timestamps.len()).unwrap(),
            timestamps
        );
    }

    #[test]
    fn delta_accumulator_restarts_before_accepting_the_next_point() {
        let mut first = point(1, 100, NumberValue::Integer(5));
        first.identity = Arc::new(MetricIdentity {
            kind: MetricKind::Sum {
                temporality: 1,
                monotonic: true,
            },
            ..first.identity.as_ref().clone()
        });
        first.value = MetricValue::Sum(NumberValue::Integer(5));
        let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
        stripe
            .apply(first.clone(), MetricIngestProtocol::Otlp)
            .unwrap();
        let checkpoint = stripe.accumulator_checkpoints().unwrap();

        let mut recovered = MetricStripe::new(1024 * 1024).unwrap();
        recovered
            .restore_accumulator_checkpoints(&checkpoint)
            .unwrap();
        let mut second = first;
        second.record_ref.offset = LogicalOffset::new(2);
        second.timestamp_unix_nanos = 200;
        second.value = MetricValue::Sum(NumberValue::Integer(7));
        recovered.apply(second, MetricIngestProtocol::Otlp).unwrap();
        let checkpoints: Vec<SeriesAccumulatorCheckpoint> =
            rmp_serde::from_slice(&recovered.accumulator_checkpoints().unwrap()).unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].cumulative, Some(NumberValue::Integer(12)));
        assert_eq!(checkpoints[0].reset_generation, 0);
    }
}
