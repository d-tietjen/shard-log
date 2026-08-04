use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use pco::ChunkConfig;
use pco::standalone::{simple_compress, simple_decompress_into};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use crate::{
    CorrelationBlockFilter, ResourceContext, ScopeContext, SeriesFingerprint, SignalTierPayload,
    SpanId, TelemetryAttribute, TelemetryError, TelemetryRecordRef, TelemetryResult,
    TelemetrySignal, TraceId,
};

const METRIC_CHUNK_MAGIC: [u8; 4] = *b"STMP";
const METRIC_CHUNK_VERSION: u8 = 1;
const METRIC_PCO_LEVEL: usize = 8;
const METRIC_SIDECAR_ZSTD_LEVEL: i32 = 1;
const DEFAULT_OUT_OF_ORDER_NANOS: u64 = 10 * 60 * 1_000_000_000;
const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_CHUNK_POINTS: usize = 4_096;
const DEFAULT_CHUNK_NANOS: u64 = 2 * 60 * 60 * 1_000_000_000;
const SERIES_ID_CACHE_ENTRIES: usize = 1_024;

/// Exact scalar number used by gauges, sums, and exemplars.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Returns the cross-signal identity of this series' resource context.
    #[must_use]
    pub fn resource_id(&self) -> crate::ResourceContextId {
        self.resource.id()
    }

    /// Returns the cross-signal identity of this series' instrumentation scope.
    #[must_use]
    pub fn scope_id(&self) -> crate::ScopeContextId {
        self.scope.id()
    }

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
    description_id: u32,
    metadata_id: u32,
    start_time_unix_nanos: u64,
    flags: u32,
    exemplars_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetricChunkSidecars {
    identity: Arc<MetricIdentity>,
    descriptions: Vec<Arc<str>>,
    metadata_sets: Vec<Arc<Vec<TelemetryAttribute>>>,
    exemplar_sets: Vec<Arc<Vec<MetricExemplar>>>,
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
            || point.identity.as_ref() != first.identity.as_ref()
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
    let sidecars = encode_metric_sidecars(&sorted)?;
    let sidecar_bytes = rmp_serde::to_vec(&sidecars)
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
    offsets
        .into_iter()
        .zip(timestamps)
        .zip(values)
        .zip(sidecars.points)
        .map(|(((offset, timestamp_unix_nanos), value), sidecar)| {
            Ok(DurableMetricPoint {
                stream_shard_id,
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    topic_partition,
                    LogicalOffset::new(offset),
                ),
                identity: Arc::clone(&sidecars.identity),
                description: resolve_metric_sidecar(
                    &sidecars.descriptions,
                    sidecar.description_id,
                    "description",
                )?,
                metadata: resolve_metric_sidecar(
                    &sidecars.metadata_sets,
                    sidecar.metadata_id,
                    "metadata",
                )?,
                start_time_unix_nanos: sidecar.start_time_unix_nanos,
                timestamp_unix_nanos,
                flags: sidecar.flags,
                value,
                exemplars: resolve_metric_sidecar(
                    &sidecars.exemplar_sets,
                    sidecar.exemplars_id,
                    "exemplars",
                )?,
            })
        })
        .collect::<TelemetryResult<Vec<_>>>()
}

fn encode_metric_sidecars(points: &[&DurableMetricPoint]) -> TelemetryResult<MetricChunkSidecars> {
    let mut descriptions = MetricSidecarInterner::new(points.len());
    let mut metadata_sets = MetricSidecarInterner::new(points.len());
    let mut exemplar_sets = MetricSidecarInterner::new(points.len());
    let mut packed = Vec::with_capacity(points.len());
    for point in points {
        let description_id = descriptions.intern(&point.description)?;
        let metadata_id = metadata_sets.intern(&point.metadata)?;
        let exemplars_id = exemplar_sets.intern(&point.exemplars)?;
        packed.push(MetricPointSidecar {
            description_id,
            metadata_id,
            start_time_unix_nanos: point.start_time_unix_nanos,
            flags: point.flags,
            exemplars_id,
        });
    }
    Ok(MetricChunkSidecars {
        identity: Arc::clone(&points[0].identity),
        descriptions: descriptions.into_values(),
        metadata_sets: metadata_sets.into_values(),
        exemplar_sets: exemplar_sets.into_values(),
        points: packed,
    })
}

struct MetricSidecarInterner<T> {
    values: Vec<T>,
    ids: Option<HashMap<T, u32>>,
    capacity: usize,
}

impl<T: Clone + Eq + Hash> MetricSidecarInterner<T> {
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

fn resolve_metric_sidecar<T: Clone>(
    values: &[T],
    id: u32,
    lane: &'static str,
) -> TelemetryResult<T> {
    values
        .get(id as usize)
        .cloned()
        .ok_or(TelemetryError::InvalidBlockEncoding(match lane {
            "description" => "metric description sidecar ID is out of range",
            "metadata" => "metric metadata sidecar ID is out of range",
            "exemplars" => "metric exemplar sidecar ID is out of range",
            _ => "metric sidecar ID is out of range",
        }))
}

fn encode_metric_values(points: &[&DurableMetricPoint]) -> TelemetryResult<Vec<u8>> {
    if let Some(values) = collect_number_lane(points, true, false) {
        return encode_integer_value_lane(1, &values);
    }
    if let Some(values) = collect_double_lane(points, true) {
        return encode_double_value_lane(2, &values);
    }
    if let Some(values) = collect_number_lane(points, false, true) {
        return encode_integer_value_lane(3, &values);
    }
    if let Some(values) = collect_double_lane(points, false) {
        return encode_double_value_lane(4, &values);
    }
    if points
        .iter()
        .all(|point| matches!(point.value, MetricValue::ExplicitHistogram(_)))
    {
        let values = points
            .iter()
            .map(|point| match &point.value {
                MetricValue::ExplicitHistogram(value) => value.clone(),
                _ => unreachable!("value kind was checked"),
            })
            .collect::<Vec<_>>();
        return encode_zstd_value_lane(5, &values);
    }
    if points
        .iter()
        .all(|point| matches!(point.value, MetricValue::ExponentialHistogram(_)))
    {
        let values = points
            .iter()
            .map(|point| match &point.value {
                MetricValue::ExponentialHistogram(value) => value.clone(),
                _ => unreachable!("value kind was checked"),
            })
            .collect::<Vec<_>>();
        return encode_zstd_value_lane(6, &values);
    }
    if points
        .iter()
        .all(|point| matches!(point.value, MetricValue::Summary(_)))
    {
        let values = points
            .iter()
            .map(|point| match &point.value {
                MetricValue::Summary(value) => value.clone(),
                _ => unreachable!("value kind was checked"),
            })
            .collect::<Vec<_>>();
        return encode_zstd_value_lane(7, &values);
    }

    let mut encoded = vec![0];
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
    let (&codec, payload) = encoded
        .split_first()
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "metric value lane is empty",
        ))?;
    match codec {
        1 => {
            return decode_integer_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::Integer)
                    .map(MetricValue::Gauge)
                    .collect()
            });
        }
        2 => {
            return decode_double_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::DoubleBits)
                    .map(MetricValue::Gauge)
                    .collect()
            });
        }
        3 => {
            return decode_integer_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::Integer)
                    .map(MetricValue::Sum)
                    .collect()
            });
        }
        4 => {
            return decode_double_value_lane(payload, count).map(|values| {
                values
                    .into_iter()
                    .map(NumberValue::DoubleBits)
                    .map(MetricValue::Sum)
                    .collect()
            });
        }
        5 => {
            return decode_zstd_value_lane::<ExplicitHistogramValue>(payload, count).map(
                |values| {
                    values
                        .into_iter()
                        .map(MetricValue::ExplicitHistogram)
                        .collect()
                },
            );
        }
        6 => {
            return decode_zstd_value_lane::<ExponentialHistogramValue>(payload, count).map(
                |values| {
                    values
                        .into_iter()
                        .map(MetricValue::ExponentialHistogram)
                        .collect()
                },
            );
        }
        7 => {
            return decode_zstd_value_lane::<SummaryValue>(payload, count)
                .map(|values| values.into_iter().map(MetricValue::Summary).collect());
        }
        0 => {}
        _ => {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid metric value-lane codec",
            ));
        }
    }
    let encoded = payload;
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

fn collect_number_lane(points: &[&DurableMetricPoint], gauge: bool, sum: bool) -> Option<Vec<i64>> {
    points
        .iter()
        .map(|point| match point.value {
            MetricValue::Gauge(NumberValue::Integer(value)) if gauge => Some(value),
            MetricValue::Sum(NumberValue::Integer(value)) if sum => Some(value),
            _ => None,
        })
        .collect()
}

fn collect_double_lane(points: &[&DurableMetricPoint], gauge: bool) -> Option<Vec<u64>> {
    points
        .iter()
        .map(|point| match point.value {
            MetricValue::Gauge(NumberValue::DoubleBits(value)) if gauge => Some(value),
            MetricValue::Sum(NumberValue::DoubleBits(value)) if !gauge => Some(value),
            _ => None,
        })
        .collect()
}

fn encode_integer_value_lane(codec: u8, values: &[i64]) -> TelemetryResult<Vec<u8>> {
    let compressed = simple_compress(
        values,
        &ChunkConfig::default().with_compression_level(METRIC_PCO_LEVEL),
    )
    .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let mut encoded = Vec::with_capacity(1 + compressed.len());
    encoded.push(codec);
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn decode_integer_value_lane(encoded: &[u8], count: usize) -> TelemetryResult<Vec<i64>> {
    let mut values = vec![0; count];
    let progress = simple_decompress_into(encoded, &mut values)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric integer Pco lane"))?;
    if progress.n_processed != count || !progress.finished {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric integer Pco lane count mismatch",
        ));
    }
    Ok(values)
}

fn encode_double_value_lane(codec: u8, values: &[u64]) -> TelemetryResult<Vec<u8>> {
    let Some((&first, rest)) = values.split_first() else {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric double lane is empty",
        ));
    };
    let mut writer = MetricBitWriter::new();
    let mut previous = first;
    for &value in rest {
        let xor = value ^ previous;
        if xor == 0 {
            writer.write_bit(false);
        } else {
            writer.write_bit(true);
            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;
            let significant = 64 - leading - trailing;
            writer.write_bits(u64::from(leading), 6);
            writer.write_bits(u64::from(trailing), 6);
            writer.write_bits(xor >> trailing, significant);
        }
        previous = value;
    }
    let bits = writer.finish();
    let mut encoded = Vec::with_capacity(9 + bits.len());
    encoded.push(codec);
    encoded.extend_from_slice(&first.to_le_bytes());
    encoded.extend_from_slice(&bits);
    Ok(encoded)
}

fn decode_double_value_lane(encoded: &[u8], count: usize) -> TelemetryResult<Vec<u64>> {
    if count == 0 || encoded.len() < 8 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid metric double lane",
        ));
    }
    let first = u64::from_le_bytes(encoded[..8].try_into().expect("fixed range"));
    let mut values = Vec::with_capacity(count);
    values.push(first);
    let mut previous = first;
    let mut reader = MetricBitReader::new(&encoded[8..]);
    for _ in 1..count {
        let value = if !reader.read_bit()? {
            previous
        } else {
            let leading = reader.read_bits(6)? as u8;
            let trailing = reader.read_bits(6)? as u8;
            if leading.saturating_add(trailing) >= 64 {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "invalid metric double XOR window",
                ));
            }
            let significant = 64 - leading - trailing;
            previous ^ (reader.read_bits(significant)? << trailing)
        };
        values.push(value);
        previous = value;
    }
    reader.finish()?;
    Ok(values)
}

fn encode_zstd_value_lane<T: Serialize>(codec: u8, values: &[T]) -> TelemetryResult<Vec<u8>> {
    let raw = rmp_serde::to_vec(values)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let compressed = zstd::bulk::compress(&raw, METRIC_SIDECAR_ZSTD_LEVEL)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let mut encoded = Vec::with_capacity(5 + compressed.len());
    encoded.push(codec);
    encoded.extend_from_slice(
        &u32::try_from(raw.len())
            .map_err(|_| TelemetryError::RecordTooLarge)?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn decode_zstd_value_lane<T: for<'de> Deserialize<'de>>(
    encoded: &[u8],
    count: usize,
) -> TelemetryResult<Vec<T>> {
    if encoded.len() < 4 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "truncated compressed metric value lane",
        ));
    }
    let raw_len = u32::from_le_bytes(encoded[..4].try_into().expect("fixed range")) as usize;
    if raw_len > 64 * 1024 * 1024 {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric value lane exceeds safety limit",
        ));
    }
    let raw = zstd::bulk::decompress(&encoded[4..], raw_len)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric value compression"))?;
    let values: Vec<T> = rmp_serde::from_slice(&raw)
        .map_err(|_| TelemetryError::InvalidBlockEncoding("invalid metric value lane"))?;
    if values.len() != count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "metric value lane count mismatch",
        ));
    }
    Ok(values)
}

struct MetricBitWriter {
    bytes: Vec<u8>,
    used: u8,
}

impl MetricBitWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            used: 8,
        }
    }

    fn write_bit(&mut self, value: bool) {
        if self.used == 8 {
            self.bytes.push(0);
            self.used = 0;
        }
        if value {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 1 << (7 - self.used);
        }
        self.used += 1;
    }

    fn write_bits(&mut self, value: u64, bits: u8) {
        for shift in (0..bits).rev() {
            self.write_bit(value & (1u64 << shift) != 0);
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct MetricBitReader<'a> {
    bytes: &'a [u8],
    bit: usize,
}

impl<'a> MetricBitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, bit: 0 }
    }

    fn read_bit(&mut self) -> TelemetryResult<bool> {
        if self.bit >= self.bytes.len().saturating_mul(8) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "truncated metric double lane",
            ));
        }
        let value = self.bytes[self.bit / 8] & (1 << (7 - self.bit % 8)) != 0;
        self.bit += 1;
        Ok(value)
    }

    fn read_bits(&mut self, bits: u8) -> TelemetryResult<u64> {
        let mut value = 0;
        for _ in 0..bits {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Ok(value)
    }

    fn finish(mut self) -> TelemetryResult<()> {
        let remaining = self.bytes.len().saturating_mul(8).saturating_sub(self.bit);
        if remaining >= 8 {
            return Err(TelemetryError::InvalidBlockEncoding(
                "trailing metric double-lane bytes",
            ));
        }
        while self.bit < self.bytes.len().saturating_mul(8) {
            if self.read_bit()? {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "nonzero metric double-lane padding",
                ));
            }
        }
        Ok(())
    }
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
    identity_fingerprints: Vec<Option<CachedSeriesIdentity>>,
}

#[derive(Debug)]
struct CachedSeriesIdentity {
    hash: u64,
    identity: Arc<MetricIdentity>,
    fingerprint: SeriesFingerprint,
}

#[derive(Debug)]
struct SealedMetricChunk {
    resident_id: u64,
    min_timestamp_unix_nanos: u64,
    max_timestamp_unix_nanos: u64,
    payload: Arc<[u8]>,
}

enum ExactMetricSource<'a> {
    Head(&'a SeriesHead),
    Chunk(&'a SealedMetricChunk),
}

impl ExactMetricSource<'_> {
    fn min_timestamp_unix_nanos(&self) -> u64 {
        match self {
            Self::Head(head) => head
                .points
                .first_key_value()
                .map_or(u64::MAX, |((timestamp, _), _)| *timestamp),
            Self::Chunk(chunk) => chunk.min_timestamp_unix_nanos,
        }
    }

    fn max_timestamp_unix_nanos(&self) -> u64 {
        match self {
            Self::Head(head) => head
                .points
                .last_key_value()
                .map_or(0, |((timestamp, _), _)| *timestamp),
            Self::Chunk(chunk) => chunk.max_timestamp_unix_nanos,
        }
    }
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
            identity_fingerprints: std::iter::repeat_with(|| None)
                .take(SERIES_ID_CACHE_ENTRIES)
                .collect(),
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
        let fingerprint = self.series_fingerprint(&point.identity);
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

    fn series_fingerprint(&mut self, identity: &Arc<MetricIdentity>) -> SeriesFingerprint {
        let mut hasher = DefaultHasher::new();
        identity.hash(&mut hasher);
        let hash = hasher.finish();
        let slot = hash as usize & (SERIES_ID_CACHE_ENTRIES - 1);
        if let Some(cached) = &self.identity_fingerprints[slot]
            && cached.hash == hash
            && (Arc::ptr_eq(&cached.identity, identity)
                || cached.identity.as_ref() == identity.as_ref())
        {
            return cached.fingerprint;
        }
        let fingerprint = identity.fingerprint();
        self.identity_fingerprints[slot] = Some(CachedSeriesIdentity {
            hash,
            identity: Arc::clone(identity),
            fingerprint,
        });
        fingerprint
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
        rmp_serde::to_vec(&checkpoints)
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
        rmp_serde::to_vec(&checkpoints)
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
        if let Some(series) = query.series {
            return self.query_exact_series(query, series, limit);
        }
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
                retain_metric_winner(&mut winners, *series, point.clone());
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
                    retain_metric_winner(&mut winners, *series, point);
                }
            }
        }
        let mut points = winners.into_values().collect::<Vec<_>>();
        points.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
        points.truncate(limit);
        Ok(points)
    }

    fn query_exact_series(
        &self,
        query: &MetricQuery,
        series: SeriesFingerprint,
        limit: usize,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let Some(head) = self.series.get(&series) else {
            return Ok(Vec::new());
        };
        if !metric_identity_matches(query, &head.identity) {
            return Ok(Vec::new());
        }
        let mut sources = Vec::with_capacity(
            usize::from(!head.points.is_empty()) + self.chunks.get(&series).map_or(0, Vec::len),
        );
        if !head.points.is_empty() {
            sources.push(ExactMetricSource::Head(head));
        }
        sources.extend(
            self.chunks
                .get(&series)
                .into_iter()
                .flatten()
                .map(ExactMetricSource::Chunk),
        );
        sources.sort_unstable_by_key(ExactMetricSource::min_timestamp_unix_nanos);

        let mut winners = BTreeMap::<u64, DurableMetricPoint>::new();
        for source in sources {
            let min_timestamp = source.min_timestamp_unix_nanos();
            let max_timestamp = source.max_timestamp_unix_nanos();
            if query
                .start_time_unix_nanos
                .is_some_and(|start| max_timestamp < start)
                || query
                    .end_time_unix_nanos
                    .is_some_and(|end| min_timestamp > end)
            {
                continue;
            }
            if winners.len() >= limit
                && winners
                    .keys()
                    .nth(limit - 1)
                    .is_some_and(|cutoff| min_timestamp > *cutoff)
            {
                break;
            }
            match source {
                ExactMetricSource::Head(head) => {
                    for point in head
                        .points
                        .values()
                        .filter(|point| metric_point_time_matches(query, point))
                    {
                        retain_exact_metric_winner(&mut winners, point.clone());
                    }
                }
                ExactMetricSource::Chunk(chunk) => {
                    for point in decode_metric_chunk(&chunk.payload)?
                        .into_iter()
                        .filter(|point| metric_point_time_matches(query, point))
                    {
                        retain_exact_metric_winner(&mut winners, point);
                    }
                }
            }
        }
        Ok(winners.into_values().take(limit).collect())
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
            min_signal_identity: series.get(),
            max_signal_identity: series.get(),
            first_offset,
            last_offset,
            record_count: u32::try_from(points.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            payload: Arc::clone(&payload),
            correlation_filter: CorrelationBlockFilter::for_metrics(&points),
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
    series: SeriesFingerprint,
    point: DurableMetricPoint,
) {
    let key = (series, point.timestamp_unix_nanos);
    if winners
        .get(&key)
        .is_none_or(|winner| winner.record_ref.offset < point.record_ref.offset)
    {
        winners.insert(key, point);
    }
}

fn retain_exact_metric_winner(
    winners: &mut BTreeMap<u64, DurableMetricPoint>,
    point: DurableMetricPoint,
) {
    let key = point.timestamp_unix_nanos;
    if winners
        .get(&key)
        .is_none_or(|winner| winner.record_ref.offset < point.record_ref.offset)
    {
        winners.insert(key, point);
    }
}

fn metric_identity_matches(query: &MetricQuery, identity: &MetricIdentity) -> bool {
    identity.tenant == query.tenant
        && query
            .name
            .as_ref()
            .is_none_or(|name| name.as_ref() == identity.name.as_ref())
        && query.exact_labels.iter().all(|(name, value)| {
            prometheus_string_labels(identity)
                .iter()
                .any(|(observed_name, observed_value)| {
                    observed_name.as_ref() == name.as_ref()
                        && observed_value.as_ref() == value.as_ref()
                })
        })
}

fn metric_point_time_matches(query: &MetricQuery, point: &DurableMetricPoint) -> bool {
    query
        .start_time_unix_nanos
        .is_none_or(|start| point.timestamp_unix_nanos >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| point.timestamp_unix_nanos <= end)
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
    metric_identity_matches(query, &point.identity) && metric_point_time_matches(query, point)
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
    fn metric_sidecar_interner_promotes_and_gorilla_lane_stays_bit_exact() {
        let points = (1..=40)
            .map(|ordinal| {
                let mut point = point(
                    ordinal,
                    ordinal * 100,
                    NumberValue::DoubleBits(
                        f64::from_bits(0x3ff0_0000_0000_0000 + ordinal).to_bits(),
                    ),
                );
                point.description = Arc::from(format!("description-{ordinal}"));
                point.metadata = Arc::new(vec![TelemetryAttribute::new(
                    "metadata.id",
                    TelemetryValue::Integer(ordinal as i64),
                )]);
                point
            })
            .collect::<Vec<_>>();
        let encoded = encode_metric_chunk(&points).unwrap();
        let decoded = decode_metric_chunk(&encoded).unwrap();
        assert_eq!(decoded, points);
    }

    #[test]
    fn homogeneous_integer_and_histogram_value_lanes_round_trip() {
        let integers = vec![
            point(1, 100, NumberValue::Integer(i64::MIN)),
            point(2, 200, NumberValue::Integer(i64::MAX)),
        ];
        assert_eq!(
            decode_metric_chunk(&encode_metric_chunk(&integers).unwrap()).unwrap(),
            integers
        );

        let histogram = ExplicitHistogramValue {
            count: HistogramCount::Integer(3),
            sum_bits: Some(6.0f64.to_bits()),
            bucket_counts: Arc::new(vec![HistogramCount::Integer(1), HistogramCount::Integer(2)]),
            explicit_bounds_bits: Arc::new(vec![1.0f64.to_bits()]),
            min_bits: Some(1.0f64.to_bits()),
            max_bits: Some(3.0f64.to_bits()),
            reset_hint: 0,
        };
        let mut histograms = integers;
        for point in &mut histograms {
            point.identity = Arc::new(MetricIdentity {
                kind: MetricKind::ExplicitHistogram { temporality: 2 },
                ..point.identity.as_ref().clone()
            });
            point.value = MetricValue::ExplicitHistogram(histogram.clone());
        }
        assert_eq!(
            decode_metric_chunk(&encode_metric_chunk(&histograms).unwrap()).unwrap(),
            histograms
        );
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
    fn exact_series_query_limits_early_and_keeps_late_conflict_winners() {
        let mut stripe = MetricStripe::new(1024 * 1024).unwrap();
        stripe.chunk_points = 2;
        let first = point(1, 100, NumberValue::Integer(1));
        let series = first.series_fingerprint();
        for value in [
            first,
            point(2, 200, NumberValue::Integer(2)),
            point(3, 300, NumberValue::Integer(3)),
            point(4, 400, NumberValue::Integer(4)),
        ] {
            stripe.apply(value, MetricIngestProtocol::Otlp).unwrap();
        }
        let conflict = point(5, 100, NumberValue::Integer(9));
        assert_eq!(
            stripe
                .apply(conflict.clone(), MetricIngestProtocol::Otlp)
                .unwrap(),
            MetricApplyOutcome::Replaced
        );

        let first_result = stripe
            .query(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                series: Some(series),
                limit: 1,
                ..MetricQuery::default()
            })
            .unwrap();
        assert_eq!(first_result, vec![conflict]);

        let ranged = stripe
            .query(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                series: Some(series),
                start_time_unix_nanos: Some(201),
                limit: 1,
                ..MetricQuery::default()
            })
            .unwrap();
        assert_eq!(ranged[0].timestamp_unix_nanos, 300);
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
