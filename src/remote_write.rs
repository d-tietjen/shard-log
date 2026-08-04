use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use prost::Message;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

use crate::prometheus_protocol::{v1, v2};
use crate::{
    DurableMetricPoint, ExplicitHistogramValue, ExponentialHistogramBuckets,
    ExponentialHistogramValue, HistogramBucketSpan, HistogramCount, METRICS_TOPIC_ID,
    MetricExemplar, MetricIdentity, MetricKind, MetricValue, NativePartitionAppend,
    NativeTelemetryBatch, NumberValue, OtlpMetricEvent, ResourceContext, ScopeContext, SpanId,
    TelemetryAttribute, TelemetryError, TelemetryRecordRef, TelemetryResult, TelemetryRouter,
    TelemetrySignal, TelemetryValue, TraceId, prepare_metric_envelope_with_protocol,
};

/// Exact Prometheus stale-NaN payload.
pub const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
/// Internal point flag indicating the Prometheus stale marker.
pub const METRIC_FLAG_STALE: u32 = 1;

/// Remote Write protobuf schema selected through the Content-Type parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteWriteVersion {
    /// Deprecated `prometheus.WriteRequest` schema.
    V1,
    /// Experimental `io.prometheus.write.v2.Request` schema.
    V2,
}

/// Counts reported in the mandatory Remote Write 2.0 response headers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemoteWriteStats {
    /// Scalar samples accepted.
    pub samples: u64,
    /// Native histogram samples accepted.
    pub histograms: u64,
    /// Exemplars accepted.
    pub exemplars: u64,
}

/// A completely decoded and validated Remote Write request.
#[derive(Debug)]
pub struct DecodedRemoteWrite {
    /// Schema selected from Content-Type.
    pub version: RemoteWriteVersion,
    /// Validated metric events, before deterministic partition assignment.
    pub events: Vec<OtlpMetricEvent>,
    /// Protocol element counts.
    pub stats: RemoteWriteStats,
}

impl DecodedRemoteWrite {
    /// Routes every series and creates one durable partition envelope.
    pub fn into_native_batch(
        self,
        router: &TelemetryRouter,
    ) -> TelemetryResult<NativeTelemetryBatch> {
        let mut partitions = BTreeMap::<TopicPartition, Vec<OtlpMetricEvent>>::new();
        for event in self.events {
            let partition = router.metric(event.tenant(), event.series_fingerprint());
            partitions.entry(partition).or_default().push(event);
        }
        let mut appends = Vec::with_capacity(partitions.len());
        for (topic_partition, events) in partitions {
            appends.push(NativePartitionAppend {
                topic_partition,
                envelope: prepare_metric_envelope_with_protocol(
                    topic_partition,
                    events,
                    crate::MetricIngestProtocol::RemoteWrite,
                )?,
            });
        }
        Ok(NativeTelemetryBatch {
            partitions: appends,
        })
    }
}

/// Lossless decoder for Remote Write v1 and v2 protobuf messages.
#[derive(Debug, Default, Clone, Copy)]
pub struct RemoteWriteDecoder;

impl RemoteWriteDecoder {
    /// Selects a schema using the required Content-Type negotiation rules.
    pub fn version_from_content_type(content_type: &str) -> TelemetryResult<RemoteWriteVersion> {
        let mut parts = content_type.split(';');
        let media_type = parts.next().unwrap_or_default().trim();
        if !media_type.eq_ignore_ascii_case("application/x-protobuf") {
            return Err(TelemetryError::InvalidMetricSample(
                "Remote Write Content-Type must be application/x-protobuf".into(),
            ));
        }
        let mut proto = None;
        for parameter in parts {
            let Some((name, value)) = parameter.trim().split_once('=') else {
                return Err(TelemetryError::InvalidMetricSample(
                    "malformed Remote Write Content-Type parameter".into(),
                ));
            };
            if name.trim().eq_ignore_ascii_case("proto") {
                proto = Some(value.trim().trim_matches('"'));
            }
        }
        match proto {
            None | Some("prometheus.WriteRequest") => Ok(RemoteWriteVersion::V1),
            Some("io.prometheus.write.v2.Request") => Ok(RemoteWriteVersion::V2),
            Some(_) => Err(TelemetryError::InvalidMetricSample(
                "unsupported Remote Write protobuf message".into(),
            )),
        }
    }

    /// Decodes a Snappy-decompressed request body without partially returning data.
    pub fn decode(
        &self,
        tenant: &str,
        version: RemoteWriteVersion,
        protobuf: &[u8],
    ) -> TelemetryResult<DecodedRemoteWrite> {
        if tenant.is_empty() {
            return Err(TelemetryError::InvalidMetricSample(
                "Remote Write tenant must not be empty".into(),
            ));
        }
        match version {
            RemoteWriteVersion::V1 => self.decode_v1(tenant, protobuf),
            RemoteWriteVersion::V2 => self.decode_v2(tenant, protobuf),
        }
    }

    fn decode_v1(&self, tenant: &str, protobuf: &[u8]) -> TelemetryResult<DecodedRemoteWrite> {
        let request = v1::WriteRequest::decode(protobuf)
            .map_err(|error| TelemetryError::InvalidMetricSample(error.to_string()))?;
        let metadata = request
            .metadata
            .into_iter()
            .map(|metadata| (metadata.metric_family_name.clone(), metadata))
            .collect::<HashMap<_, _>>();
        let mut events = Vec::new();
        let mut stats = RemoteWriteStats::default();
        for series in request.timeseries {
            let labels = validate_v1_labels(series.labels)?;
            let name = metric_name(&labels)?.to_owned();
            let metadata = metadata_for_name(&metadata, &name);
            let metric_type = metadata.map_or(v1::MetricType::Unknown as i32, |value| value.r#type);
            let unit = metadata.map_or_else(String::new, |value| value.unit.clone());
            let description = metadata.map_or_else(String::new, |value| value.help.clone());
            let exemplars = series
                .exemplars
                .into_iter()
                .map(decode_v1_exemplar)
                .collect::<TelemetryResult<Vec<_>>>()?;
            stats.exemplars = stats.exemplars.saturating_add(exemplars.len() as u64);
            let mut raw_points = Vec::with_capacity(series.samples.len() + series.histograms.len());
            for sample in series.samples {
                raw_points.push(RawPoint {
                    timestamp_unix_nanos: millis_to_nanos(sample.timestamp)?,
                    start_time_unix_nanos: 0,
                    value: scalar_value(metric_type, sample.value.to_bits()),
                });
                stats.samples = stats.samples.saturating_add(1);
            }
            for histogram in series.histograms {
                raw_points.push(decode_v1_histogram(histogram)?);
                stats.histograms = stats.histograms.saturating_add(1);
            }
            append_series_events(
                tenant,
                &name,
                &unit,
                &description,
                metric_type,
                labels,
                raw_points,
                exemplars,
                &mut events,
            )?;
        }
        Ok(DecodedRemoteWrite {
            version: RemoteWriteVersion::V1,
            events,
            stats,
        })
    }

    fn decode_v2(&self, tenant: &str, protobuf: &[u8]) -> TelemetryResult<DecodedRemoteWrite> {
        let request = v2::Request::decode(protobuf)
            .map_err(|error| TelemetryError::InvalidMetricSample(error.to_string()))?;
        validate_symbols(&request.symbols)?;
        let mut events = Vec::new();
        let mut stats = RemoteWriteStats::default();
        for series in request.timeseries {
            let labels = decode_symbol_labels(&request.symbols, &series.labels_refs, true)?;
            let name = metric_name(&labels)?.to_owned();
            let metric_type = series
                .metadata
                .as_ref()
                .map_or(v2::MetricType::Unspecified as i32, |metadata| {
                    metadata.r#type
                });
            let unit = symbol(
                &request.symbols,
                series
                    .metadata
                    .as_ref()
                    .map_or(0, |metadata| metadata.unit_ref),
            )?
            .to_owned();
            let description = symbol(
                &request.symbols,
                series
                    .metadata
                    .as_ref()
                    .map_or(0, |metadata| metadata.help_ref),
            )?
            .to_owned();
            let exemplars = series
                .exemplars
                .into_iter()
                .map(|exemplar| decode_v2_exemplar(&request.symbols, exemplar))
                .collect::<TelemetryResult<Vec<_>>>()?;
            stats.exemplars = stats.exemplars.saturating_add(exemplars.len() as u64);
            let mut raw_points = Vec::with_capacity(series.samples.len() + series.histograms.len());
            for sample in series.samples {
                raw_points.push(RawPoint {
                    timestamp_unix_nanos: millis_to_nanos(sample.timestamp)?,
                    start_time_unix_nanos: optional_millis_to_nanos(sample.start_timestamp)?,
                    value: scalar_value(metric_type, sample.value.to_bits()),
                });
                stats.samples = stats.samples.saturating_add(1);
            }
            for histogram in series.histograms {
                raw_points.push(decode_v2_histogram(histogram)?);
                stats.histograms = stats.histograms.saturating_add(1);
            }
            append_series_events(
                tenant,
                &name,
                &unit,
                &description,
                metric_type,
                labels,
                raw_points,
                exemplars,
                &mut events,
            )?;
        }
        Ok(DecodedRemoteWrite {
            version: RemoteWriteVersion::V2,
            events,
            stats,
        })
    }
}

#[derive(Debug)]
struct RawPoint {
    timestamp_unix_nanos: u64,
    start_time_unix_nanos: u64,
    value: MetricValue,
}

#[allow(clippy::too_many_arguments)]
fn append_series_events(
    tenant: &str,
    name: &str,
    unit: &str,
    description: &str,
    metric_type: i32,
    labels: Vec<(String, String)>,
    mut raw_points: Vec<RawPoint>,
    exemplars: Vec<MetricExemplar>,
    events: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    if raw_points.is_empty() {
        if exemplars.is_empty() {
            return Ok(());
        }
        return Err(TelemetryError::InvalidMetricSample(
            "Remote Write exemplar-only series cannot be attached to a metric point".into(),
        ));
    }
    raw_points.sort_unstable_by_key(|point| point.timestamp_unix_nanos);
    let mut assigned = vec![Vec::new(); raw_points.len()];
    for exemplar in exemplars {
        let index = raw_points
            .binary_search_by_key(&exemplar.timestamp_unix_nanos, |point| {
                point.timestamp_unix_nanos
            })
            .unwrap_or_else(|index| index.min(raw_points.len() - 1));
        assigned[index].push(exemplar);
    }
    let point_attributes = Arc::new(
        labels
            .into_iter()
            .filter(|(key, _)| key != "__name__")
            .map(|(key, value)| TelemetryAttribute::new(key, TelemetryValue::String(value.into())))
            .collect(),
    );
    let kind = metric_kind(metric_type, &raw_points);
    let identity = Arc::new(MetricIdentity {
        tenant: Arc::from(tenant),
        resource: Arc::new(ResourceContext::default()),
        scope: Arc::new(ScopeContext::default()),
        name: Arc::from(name),
        unit: Arc::from(unit),
        kind,
        point_attributes,
    });
    for (ordinal, (raw, exemplars)) in raw_points.into_iter().zip(assigned).enumerate() {
        let flags = match raw.value {
            MetricValue::Gauge(NumberValue::DoubleBits(PROMETHEUS_STALE_NAN_BITS))
            | MetricValue::Sum(NumberValue::DoubleBits(PROMETHEUS_STALE_NAN_BITS)) => {
                METRIC_FLAG_STALE
            }
            _ => 0,
        };
        let point = DurableMetricPoint {
            stream_shard_id: ShardId::new(0),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Metrics,
                TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                LogicalOffset::new(ordinal as u64),
            ),
            identity: Arc::clone(&identity),
            description: Arc::from(description),
            metadata: Arc::new(Vec::new()),
            start_time_unix_nanos: raw.start_time_unix_nanos,
            timestamp_unix_nanos: raw.timestamp_unix_nanos,
            flags,
            value: raw.value,
            exemplars: Arc::new(exemplars),
        };
        events.push(OtlpMetricEvent::from_durable(point)?);
    }
    Ok(())
}

fn metric_kind(metric_type: i32, points: &[RawPoint]) -> MetricKind {
    if points
        .iter()
        .any(|point| matches!(point.value, MetricValue::ExplicitHistogram(_)))
    {
        return MetricKind::ExplicitHistogram { temporality: 2 };
    }
    if points
        .iter()
        .any(|point| matches!(point.value, MetricValue::ExponentialHistogram(_)))
    {
        return MetricKind::ExponentialHistogram { temporality: 2 };
    }
    if metric_type == v1::MetricType::Counter as i32 {
        MetricKind::Sum {
            temporality: 2,
            monotonic: true,
        }
    } else {
        MetricKind::Gauge
    }
}

fn scalar_value(metric_type: i32, bits: u64) -> MetricValue {
    if metric_type == v1::MetricType::Counter as i32 {
        MetricValue::Sum(NumberValue::DoubleBits(bits))
    } else {
        MetricValue::Gauge(NumberValue::DoubleBits(bits))
    }
}

fn validate_v1_labels(labels: Vec<v1::Label>) -> TelemetryResult<Vec<(String, String)>> {
    let mut seen = HashSet::with_capacity(labels.len());
    let mut decoded = Vec::with_capacity(labels.len());
    for label in labels {
        if label.name.is_empty() || !seen.insert(label.name.clone()) {
            return Err(TelemetryError::InvalidMetricSample(
                "Remote Write v1 labels require unique non-empty names".into(),
            ));
        }
        decoded.push((label.name, label.value));
    }
    decoded.sort_unstable();
    Ok(decoded)
}

fn validate_symbols(symbols: &[String]) -> TelemetryResult<()> {
    if symbols.first().is_none_or(|symbol| !symbol.is_empty()) {
        return Err(TelemetryError::InvalidMetricSample(
            "Remote Write v2 symbol zero must be the empty string".into(),
        ));
    }
    let mut seen = HashSet::with_capacity(symbols.len());
    if symbols.iter().any(|symbol| !seen.insert(symbol)) {
        return Err(TelemetryError::InvalidMetricSample(
            "Remote Write v2 symbols must be deduplicated".into(),
        ));
    }
    Ok(())
}

fn decode_symbol_labels(
    symbols: &[String],
    refs: &[u32],
    require_sorted: bool,
) -> TelemetryResult<Vec<(String, String)>> {
    if !refs.len().is_multiple_of(2) {
        return Err(TelemetryError::InvalidMetricSample(
            "Remote Write v2 label references must be name/value pairs".into(),
        ));
    }
    let mut labels = Vec::with_capacity(refs.len() / 2);
    let mut seen = HashSet::with_capacity(refs.len() / 2);
    for pair in refs.chunks_exact(2) {
        let name = symbol(symbols, pair[0])?;
        let value = symbol(symbols, pair[1])?;
        if name.is_empty() || value.is_empty() || !seen.insert(name) {
            return Err(TelemetryError::InvalidMetricSample(
                "Remote Write v2 labels require unique non-empty names and values".into(),
            ));
        }
        labels.push((name.to_owned(), value.to_owned()));
    }
    if require_sorted && labels.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(TelemetryError::InvalidMetricSample(
            "Remote Write v2 labels must be lexicographically sorted".into(),
        ));
    }
    Ok(labels)
}

fn symbol(symbols: &[String], reference: u32) -> TelemetryResult<&str> {
    symbols
        .get(reference as usize)
        .map(String::as_str)
        .ok_or_else(|| {
            TelemetryError::InvalidMetricSample(
                "Remote Write v2 symbol reference is out of bounds".into(),
            )
        })
}

fn metric_name(labels: &[(String, String)]) -> TelemetryResult<&str> {
    labels
        .iter()
        .find_map(|(name, value)| (name == "__name__").then_some(value.as_str()))
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            TelemetryError::InvalidMetricSample(
                "Remote Write series requires a non-empty __name__ label".into(),
            )
        })
}

fn metadata_for_name<'a>(
    metadata: &'a HashMap<String, v1::MetricMetadata>,
    name: &str,
) -> Option<&'a v1::MetricMetadata> {
    metadata.get(name).or_else(|| {
        name.strip_suffix("_total")
            .and_then(|family| metadata.get(family))
    })
}

fn decode_v1_exemplar(exemplar: v1::Exemplar) -> TelemetryResult<MetricExemplar> {
    let labels = validate_v1_labels(exemplar.labels)?;
    decode_exemplar(labels, exemplar.value.to_bits(), exemplar.timestamp)
}

fn decode_v2_exemplar(
    symbols: &[String],
    exemplar: v2::Exemplar,
) -> TelemetryResult<MetricExemplar> {
    let labels = decode_symbol_labels(symbols, &exemplar.labels_refs, true)?;
    decode_exemplar(labels, exemplar.value.to_bits(), exemplar.timestamp)
}

fn decode_exemplar(
    labels: Vec<(String, String)>,
    value_bits: u64,
    timestamp_ms: i64,
) -> TelemetryResult<MetricExemplar> {
    let trace_id = labels
        .iter()
        .find_map(|(key, value)| (key == "trace_id").then_some(value))
        .map(|value| decode_hex::<16>(value).and_then(TraceId::from_bytes))
        .transpose()?;
    let span_id = labels
        .iter()
        .find_map(|(key, value)| (key == "span_id").then_some(value))
        .map(|value| decode_hex::<8>(value).and_then(SpanId::from_bytes))
        .transpose()?;
    Ok(MetricExemplar {
        filtered_attributes: Arc::new(
            labels
                .into_iter()
                .map(|(key, value)| {
                    TelemetryAttribute::new(key, TelemetryValue::String(value.into()))
                })
                .collect(),
        ),
        timestamp_unix_nanos: millis_to_nanos(timestamp_ms)?,
        value: NumberValue::DoubleBits(value_bits),
        span_id,
        trace_id,
    })
}

fn decode_hex<const N: usize>(value: &str) -> TelemetryResult<[u8; N]> {
    if value.len() != N * 2 {
        return Err(TelemetryError::InvalidMetricSample(
            "Prometheus exemplar trace/span ID has invalid length".into(),
        ));
    }
    let mut decoded = [0_u8; N];
    for (index, byte) in decoded.iter_mut().enumerate() {
        let pair = &value.as_bytes()[index * 2..index * 2 + 2];
        *byte = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> TelemetryResult<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(TelemetryError::InvalidMetricSample(
            "Prometheus exemplar trace/span ID is not hexadecimal".into(),
        )),
    }
}

fn decode_v1_histogram(histogram: v1::Histogram) -> TelemetryResult<RawPoint> {
    let count = histogram.count.ok_or_else(|| {
        TelemetryError::InvalidMetricSample("Prometheus histogram is missing count".into())
    })?;
    let zero_count = histogram.zero_count.ok_or_else(|| {
        TelemetryError::InvalidMetricSample("Prometheus histogram is missing zero count".into())
    })?;
    decode_histogram(
        histogram.schema,
        count_v1(count),
        histogram.sum.to_bits(),
        histogram.zero_threshold.to_bits(),
        zero_count_v1(zero_count),
        spans_v1(histogram.negative_spans),
        histogram.negative_deltas,
        histogram.negative_counts,
        spans_v1(histogram.positive_spans),
        histogram.positive_deltas,
        histogram.positive_counts,
        histogram.reset_hint,
        histogram.timestamp,
        histogram.start_timestamp,
        histogram.custom_values,
    )
}

fn decode_v2_histogram(histogram: v2::Histogram) -> TelemetryResult<RawPoint> {
    let count = histogram.count.ok_or_else(|| {
        TelemetryError::InvalidMetricSample("Prometheus histogram is missing count".into())
    })?;
    let zero_count = histogram.zero_count.ok_or_else(|| {
        TelemetryError::InvalidMetricSample("Prometheus histogram is missing zero count".into())
    })?;
    decode_histogram(
        histogram.schema,
        count_v2(count),
        histogram.sum.to_bits(),
        histogram.zero_threshold.to_bits(),
        zero_count_v2(zero_count),
        spans_v1(histogram.negative_spans),
        histogram.negative_deltas,
        histogram.negative_counts,
        spans_v1(histogram.positive_spans),
        histogram.positive_deltas,
        histogram.positive_counts,
        histogram.reset_hint,
        histogram.timestamp,
        histogram.start_timestamp,
        histogram.custom_values,
    )
}

#[allow(clippy::too_many_arguments)]
fn decode_histogram(
    schema: i32,
    count: HistogramCount,
    sum_bits: u64,
    zero_threshold_bits: u64,
    zero_count: HistogramCount,
    negative_spans: Vec<HistogramBucketSpan>,
    negative_deltas: Vec<i64>,
    negative_counts: Vec<f64>,
    positive_spans: Vec<HistogramBucketSpan>,
    positive_deltas: Vec<i64>,
    positive_counts: Vec<f64>,
    reset_hint: i32,
    timestamp_ms: i64,
    start_timestamp_ms: i64,
    custom_values: Vec<f64>,
) -> TelemetryResult<RawPoint> {
    let negative = decode_buckets(negative_spans, negative_deltas, negative_counts)?;
    let positive = decode_buckets(positive_spans, positive_deltas, positive_counts)?;
    let value = if schema == -53 {
        if negative.is_some() || zero_count != HistogramCount::Integer(0) {
            return Err(TelemetryError::InvalidMetricSample(
                "custom Prometheus histogram cannot use negative or zero buckets".into(),
            ));
        }
        let bounds = custom_values.iter().map(|value| value.to_bits()).collect();
        let bucket_counts = expand_custom_buckets(positive, custom_values.len() + 1)?;
        MetricValue::ExplicitHistogram(ExplicitHistogramValue {
            count,
            sum_bits: Some(sum_bits),
            bucket_counts: Arc::new(bucket_counts),
            explicit_bounds_bits: Arc::new(bounds),
            min_bits: None,
            max_bits: None,
            reset_hint,
        })
    } else {
        if !(-4..=8).contains(&schema) {
            return Err(TelemetryError::InvalidMetricSample(format!(
                "unsupported Prometheus native histogram schema {schema}"
            )));
        }
        MetricValue::ExponentialHistogram(ExponentialHistogramValue {
            count,
            sum_bits: Some(sum_bits),
            scale: schema,
            zero_count,
            positive,
            negative,
            min_bits: None,
            max_bits: None,
            zero_threshold_bits,
            reset_hint,
        })
    };
    Ok(RawPoint {
        timestamp_unix_nanos: millis_to_nanos(timestamp_ms)?,
        start_time_unix_nanos: optional_millis_to_nanos(start_timestamp_ms)?,
        value,
    })
}

fn decode_buckets(
    spans: Vec<HistogramBucketSpan>,
    deltas: Vec<i64>,
    counts: Vec<f64>,
) -> TelemetryResult<Option<ExponentialHistogramBuckets>> {
    let expected = spans.iter().try_fold(0_usize, |total, span| {
        total.checked_add(span.length as usize).ok_or_else(|| {
            TelemetryError::InvalidMetricSample("histogram span length overflow".into())
        })
    })?;
    if expected == 0 {
        if !deltas.is_empty() || !counts.is_empty() {
            return Err(TelemetryError::InvalidMetricSample(
                "histogram bucket values have no spans".into(),
            ));
        }
        return Ok(None);
    }
    if !deltas.is_empty() && !counts.is_empty() {
        return Err(TelemetryError::InvalidMetricSample(
            "histogram cannot mix integer deltas and float counts".into(),
        ));
    }
    let bucket_counts = if !deltas.is_empty() {
        if deltas.len() != expected {
            return Err(TelemetryError::InvalidMetricSample(
                "histogram integer bucket count does not match spans".into(),
            ));
        }
        let mut previous = 0_i128;
        let mut decoded = Vec::with_capacity(expected);
        for delta in deltas {
            previous = previous.checked_add(i128::from(delta)).ok_or_else(|| {
                TelemetryError::InvalidMetricSample("histogram bucket delta overflow".into())
            })?;
            let count = u64::try_from(previous).map_err(|_| {
                TelemetryError::InvalidMetricSample("histogram bucket count is negative".into())
            })?;
            decoded.push(HistogramCount::Integer(count));
        }
        decoded
    } else {
        if counts.len() != expected {
            return Err(TelemetryError::InvalidMetricSample(
                "histogram float bucket count does not match spans".into(),
            ));
        }
        counts
            .into_iter()
            .map(|value| HistogramCount::DoubleBits(value.to_bits()))
            .collect()
    };
    Ok(Some(ExponentialHistogramBuckets {
        spans: Arc::new(spans),
        bucket_counts: Arc::new(bucket_counts),
    }))
}

fn expand_custom_buckets(
    buckets: Option<ExponentialHistogramBuckets>,
    output_len: usize,
) -> TelemetryResult<Vec<HistogramCount>> {
    let Some(buckets) = buckets else {
        return Ok(vec![HistogramCount::Integer(0); output_len]);
    };
    let fill = if buckets
        .bucket_counts
        .iter()
        .any(|count| matches!(count, HistogramCount::DoubleBits(_)))
    {
        HistogramCount::DoubleBits(0.0_f64.to_bits())
    } else {
        HistogramCount::Integer(0)
    };
    let mut output = vec![fill; output_len];
    let mut bucket_index = 0_usize;
    let mut prior_end = 0_i64;
    for (span_index, span) in buckets.spans.iter().enumerate() {
        let start = if span_index == 0 {
            i64::from(span.offset)
        } else {
            prior_end.saturating_add(i64::from(span.offset))
        };
        if start < 0 {
            return Err(TelemetryError::InvalidMetricSample(
                "custom histogram span starts before bucket zero".into(),
            ));
        }
        let end = start.checked_add(i64::from(span.length)).ok_or_else(|| {
            TelemetryError::InvalidMetricSample("custom histogram span overflow".into())
        })?;
        if end as usize > output.len() {
            return Err(TelemetryError::InvalidMetricSample(
                "custom histogram span exceeds custom bounds".into(),
            ));
        }
        for destination in &mut output[start as usize..end as usize] {
            *destination = buckets.bucket_counts[bucket_index];
            bucket_index += 1;
        }
        prior_end = end;
    }
    Ok(output)
}

fn spans_v1(spans: Vec<v1::BucketSpan>) -> Vec<HistogramBucketSpan> {
    spans
        .into_iter()
        .map(|span| HistogramBucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn count_v1(count: v1::histogram::Count) -> HistogramCount {
    match count {
        v1::histogram::Count::Int(value) => HistogramCount::Integer(value),
        v1::histogram::Count::Float(value) => HistogramCount::DoubleBits(value.to_bits()),
    }
}

fn zero_count_v1(count: v1::histogram::ZeroCount) -> HistogramCount {
    match count {
        v1::histogram::ZeroCount::Int(value) => HistogramCount::Integer(value),
        v1::histogram::ZeroCount::Float(value) => HistogramCount::DoubleBits(value.to_bits()),
    }
}

fn count_v2(count: v2::histogram::Count) -> HistogramCount {
    match count {
        v2::histogram::Count::Int(value) => HistogramCount::Integer(value),
        v2::histogram::Count::Float(value) => HistogramCount::DoubleBits(value.to_bits()),
    }
}

fn zero_count_v2(count: v2::histogram::ZeroCount) -> HistogramCount {
    match count {
        v2::histogram::ZeroCount::Int(value) => HistogramCount::Integer(value),
        v2::histogram::ZeroCount::Float(value) => HistogramCount::DoubleBits(value.to_bits()),
    }
}

fn millis_to_nanos(timestamp_ms: i64) -> TelemetryResult<u64> {
    let timestamp_ms = u64::try_from(timestamp_ms).map_err(|_| {
        TelemetryError::InvalidMetricSample(
            "negative Prometheus timestamps are outside the storage epoch".into(),
        )
    })?;
    timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        TelemetryError::InvalidMetricSample("Prometheus timestamp overflows nanoseconds".into())
    })
}

fn optional_millis_to_nanos(timestamp_ms: i64) -> TelemetryResult<u64> {
    if timestamp_ms == 0 {
        Ok(0)
    } else {
        millis_to_nanos(timestamp_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_negotiation_is_deterministic() {
        assert_eq!(
            RemoteWriteDecoder::version_from_content_type("application/x-protobuf").unwrap(),
            RemoteWriteVersion::V1
        );
        assert_eq!(
            RemoteWriteDecoder::version_from_content_type(
                "application/x-protobuf;proto=io.prometheus.write.v2.Request"
            )
            .unwrap(),
            RemoteWriteVersion::V2
        );
        assert!(
            RemoteWriteDecoder::version_from_content_type(
                "application/x-protobuf;proto=unknown.Request"
            )
            .is_err()
        );
    }

    #[test]
    fn v1_preserves_stale_nan_and_exemplar_ids() {
        let request = v1::WriteRequest {
            timeseries: vec![v1::TimeSeries {
                labels: vec![
                    v1::Label {
                        name: "__name__".into(),
                        value: "requests_total".into(),
                    },
                    v1::Label {
                        name: "service".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![v1::Sample {
                    value: f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
                    timestamp: 10,
                }],
                exemplars: vec![v1::Exemplar {
                    labels: vec![
                        v1::Label {
                            name: "trace_id".into(),
                            value: "01010101010101010101010101010101".into(),
                        },
                        v1::Label {
                            name: "span_id".into(),
                            value: "0202020202020202".into(),
                        },
                    ],
                    value: 3.0,
                    timestamp: 10,
                }],
                histograms: Vec::new(),
            }],
            metadata: vec![v1::MetricMetadata {
                r#type: v1::MetricType::Counter as i32,
                metric_family_name: "requests".into(),
                help: "requests".into(),
                unit: String::new(),
            }],
        };
        let decoded = RemoteWriteDecoder
            .decode("tenant-a", RemoteWriteVersion::V1, &request.encode_to_vec())
            .unwrap();
        assert_eq!(decoded.stats.samples, 1);
        assert_eq!(decoded.stats.exemplars, 1);
        let point = decoded.events[0].clone().into_durable(
            ShardId::new(0),
            TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(1)),
            LogicalOffset::new(1),
        );
        assert_eq!(point.flags & METRIC_FLAG_STALE, METRIC_FLAG_STALE);
        assert_eq!(
            point.exemplars[0].trace_id,
            Some(TraceId::from_bytes([1; 16]).unwrap())
        );
        assert_eq!(
            point.exemplars[0].span_id,
            Some(SpanId::from_bytes([2; 8]).unwrap())
        );
    }

    #[test]
    fn v2_float_histogram_counts_round_trip_without_integer_coercion() {
        let request = v2::Request {
            symbols: vec!["".into(), "__name__".into(), "latency".into()],
            timeseries: vec![v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: Vec::new(),
                histograms: vec![v2::Histogram {
                    count: Some(v2::histogram::Count::Float(3.5)),
                    sum: 9.0,
                    schema: 1,
                    zero_threshold: 0.001,
                    zero_count: Some(v2::histogram::ZeroCount::Float(0.5)),
                    negative_spans: Vec::new(),
                    negative_deltas: Vec::new(),
                    negative_counts: Vec::new(),
                    positive_spans: vec![v1::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_deltas: Vec::new(),
                    positive_counts: vec![1.25, 1.75],
                    reset_hint: v2::histogram::ResetHint::Gauge as i32,
                    timestamp: 100,
                    custom_values: Vec::new(),
                    start_timestamp: 50,
                }],
                exemplars: Vec::new(),
                metadata: Some(v2::Metadata {
                    r#type: v2::MetricType::GaugeHistogram as i32,
                    help_ref: 0,
                    unit_ref: 0,
                }),
            }],
        };
        let decoded = RemoteWriteDecoder
            .decode("tenant-a", RemoteWriteVersion::V2, &request.encode_to_vec())
            .unwrap();
        let point = decoded.events[0].clone().into_durable(
            ShardId::new(0),
            TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(1)),
            LogicalOffset::new(1),
        );
        let MetricValue::ExponentialHistogram(histogram) = point.value else {
            panic!("expected native histogram");
        };
        assert_eq!(
            histogram.count,
            HistogramCount::DoubleBits(3.5f64.to_bits())
        );
        assert_eq!(
            histogram.positive.unwrap().bucket_counts.as_slice(),
            &[
                HistogramCount::DoubleBits(1.25f64.to_bits()),
                HistogramCount::DoubleBits(1.75f64.to_bits())
            ]
        );
    }
}
