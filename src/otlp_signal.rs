use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use opentelemetry_proto::tonic::{
    collector::{metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::KeyValue,
    metrics::v1::{
        Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric, NumberDataPoint,
        SummaryDataPoint, exemplar, metric, number_data_point,
    },
    trace::v1::Span,
};
use prost::Message;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

use crate::otlp::{
    decode_resource_context, decode_scope_context, decode_telemetry_attributes, optional_span_id,
    optional_trace_id,
};
use crate::{
    DurableMetricPoint, DurableSpan, ExplicitHistogramValue, ExponentialHistogramBuckets,
    ExponentialHistogramValue, HistogramBucketSpan, HistogramCount, METRICS_TOPIC_ID,
    MetricExemplar, MetricIdentity, MetricKind, MetricValue, NumberValue, ResourceContext,
    ScopeContext, SpanEvent, SpanId, SpanLink, SpanStatus, SummaryQuantileValue, SummaryValue,
    TRACES_TOPIC_ID, TelemetryError, TelemetryRecordRef, TelemetryResult, TelemetryRouter,
    TelemetrySignal, TraceId,
};

/// One fully validated OTLP span awaiting its durable offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpSpanEvent {
    span: DurableSpan,
}

impl OtlpSpanEvent {
    /// Returns the validated trace ID used for routing.
    #[must_use]
    pub const fn trace_id(&self) -> TraceId {
        self.span.trace_id
    }

    /// Returns the authenticated tenant.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.span.tenant
    }

    /// Assigns the shard-stream owner and logical offset.
    #[must_use]
    pub fn into_durable(
        mut self,
        stream_shard_id: ShardId,
        topic_partition: TopicPartition,
        offset: LogicalOffset,
    ) -> DurableSpan {
        self.span.stream_shard_id = stream_shard_id;
        self.span.record_ref =
            TelemetryRecordRef::for_signal(TelemetrySignal::Traces, topic_partition, offset);
        self.span
    }
}

/// One fully validated OTLP metric point awaiting its durable offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpMetricEvent {
    point: DurableMetricPoint,
}

impl OtlpMetricEvent {
    /// Wraps an already validated transport-independent metric point before routing.
    pub fn from_durable(point: DurableMetricPoint) -> TelemetryResult<Self> {
        if point.record_ref.signal != TelemetrySignal::Metrics
            || point.identity.tenant.is_empty()
            || point.identity.name.is_empty()
        {
            return Err(TelemetryError::InvalidMetricSample(
                "metric event requires the metrics signal, tenant, and name".into(),
            ));
        }
        Ok(Self { point })
    }

    /// Returns the canonical series fingerprint used for routing.
    #[must_use]
    pub fn series_fingerprint(&self) -> crate::SeriesFingerprint {
        self.point.series_fingerprint()
    }

    /// Returns the authenticated tenant.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.point.identity.tenant
    }

    /// Assigns the shard-stream owner and logical offset.
    #[must_use]
    pub fn into_durable(
        mut self,
        stream_shard_id: ShardId,
        topic_partition: TopicPartition,
        offset: LogicalOffset,
    ) -> DurableMetricPoint {
        self.point.stream_shard_id = stream_shard_id;
        self.point.record_ref =
            TelemetryRecordRef::for_signal(TelemetrySignal::Metrics, topic_partition, offset);
        self.point
    }
}

/// Transport-independent OTLP trace and metric decoder.
///
/// Each complete request is decoded and validated before any event is returned,
/// allowing callers to partition and append without partial-request mutation.
#[derive(Debug, Default, Clone, Copy)]
pub struct OtlpTelemetryDecoder;

impl OtlpTelemetryDecoder {
    /// Decodes one protobuf `ExportTraceServiceRequest` exactly.
    pub fn decode_traces(
        &self,
        tenant: &str,
        payload: &[u8],
    ) -> TelemetryResult<Vec<OtlpSpanEvent>> {
        if tenant.is_empty() {
            return Err(TelemetryError::InvalidOtlpPayload(
                "tenant must not be empty".into(),
            ));
        }
        let request = ExportTraceServiceRequest::decode(payload)
            .map_err(|error| TelemetryError::InvalidOtlpPayload(error.to_string()))?;
        let mut events = Vec::new();
        for resource_spans in &request.resource_spans {
            validate_attributes(
                resource_spans
                    .resource
                    .as_ref()
                    .map_or(&[], |resource| resource.attributes.as_slice()),
            )?;
            let resource = Arc::new(decode_resource_context(
                resource_spans.resource.as_ref(),
                &resource_spans.schema_url,
            ));
            for scope_spans in &resource_spans.scope_spans {
                validate_attributes(
                    scope_spans
                        .scope
                        .as_ref()
                        .map_or(&[], |scope| scope.attributes.as_slice()),
                )?;
                let scope = Arc::new(decode_scope_context(
                    scope_spans.scope.as_ref(),
                    &scope_spans.schema_url,
                ));
                for span in &scope_spans.spans {
                    events.push(OtlpSpanEvent {
                        span: decode_span(tenant, Arc::clone(&resource), Arc::clone(&scope), span)?,
                    });
                }
            }
        }
        Ok(events)
    }

    /// Decodes one protobuf `ExportMetricsServiceRequest` exactly.
    pub fn decode_metrics(
        &self,
        tenant: &str,
        payload: &[u8],
    ) -> TelemetryResult<Vec<OtlpMetricEvent>> {
        if tenant.is_empty() {
            return Err(TelemetryError::InvalidOtlpPayload(
                "tenant must not be empty".into(),
            ));
        }
        let request = ExportMetricsServiceRequest::decode(payload)
            .map_err(|error| TelemetryError::InvalidOtlpPayload(error.to_string()))?;
        let mut events = Vec::new();
        for resource_metrics in &request.resource_metrics {
            validate_attributes(
                resource_metrics
                    .resource
                    .as_ref()
                    .map_or(&[], |resource| resource.attributes.as_slice()),
            )?;
            let resource = Arc::new(decode_resource_context(
                resource_metrics.resource.as_ref(),
                &resource_metrics.schema_url,
            ));
            for scope_metrics in &resource_metrics.scope_metrics {
                validate_attributes(
                    scope_metrics
                        .scope
                        .as_ref()
                        .map_or(&[], |scope| scope.attributes.as_slice()),
                )?;
                let scope = Arc::new(decode_scope_context(
                    scope_metrics.scope.as_ref(),
                    &scope_metrics.schema_url,
                ));
                for metric in &scope_metrics.metrics {
                    decode_metric(
                        tenant,
                        Arc::clone(&resource),
                        Arc::clone(&scope),
                        metric,
                        &mut events,
                    )?;
                }
            }
        }
        Ok(events)
    }

    /// Groups validated spans by deterministic trace partition.
    #[must_use]
    pub fn partition_traces(
        &self,
        router: &TelemetryRouter,
        events: Vec<OtlpSpanEvent>,
    ) -> BTreeMap<TopicPartition, Vec<OtlpSpanEvent>> {
        let mut partitions = BTreeMap::new();
        for event in events {
            let partition = router.trace(event.tenant(), event.trace_id());
            partitions
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(event);
        }
        partitions
    }

    /// Groups validated metric points by deterministic series partition.
    #[must_use]
    pub fn partition_metrics(
        &self,
        router: &TelemetryRouter,
        events: Vec<OtlpMetricEvent>,
    ) -> BTreeMap<TopicPartition, Vec<OtlpMetricEvent>> {
        let mut partitions = BTreeMap::new();
        for event in events {
            let partition = router.metric(event.tenant(), event.series_fingerprint());
            partitions
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(event);
        }
        partitions
    }
}

fn decode_span(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    span: &Span,
) -> TelemetryResult<DurableSpan> {
    validate_attributes(&span.attributes)?;
    if span.end_time_unix_nano < span.start_time_unix_nano {
        return Err(TelemetryError::InvalidOtlpPayload(
            "span end time precedes start time".into(),
        ));
    }
    let trace_id = TraceId::from_slice(&span.trace_id)?;
    let span_id = SpanId::from_slice(&span.span_id)?;
    let parent_span_id = optional_span_id(&span.parent_span_id)?;
    let mut events = Vec::with_capacity(span.events.len());
    for event in &span.events {
        validate_attributes(&event.attributes)?;
        events.push(SpanEvent {
            timestamp_unix_nanos: event.time_unix_nano,
            name: Arc::from(event.name.as_str()),
            attributes: Arc::new(decode_telemetry_attributes(&event.attributes)),
            dropped_attributes_count: event.dropped_attributes_count,
        });
    }
    let mut links = Vec::with_capacity(span.links.len());
    for link in &span.links {
        validate_attributes(&link.attributes)?;
        links.push(SpanLink {
            trace_id: TraceId::from_slice(&link.trace_id)?,
            span_id: SpanId::from_slice(&link.span_id)?,
            trace_state: Arc::from(link.trace_state.as_str()),
            attributes: Arc::new(decode_telemetry_attributes(&link.attributes)),
            dropped_attributes_count: link.dropped_attributes_count,
            flags: link.flags,
        });
    }
    Ok(DurableSpan {
        stream_shard_id: ShardId::new(0),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Traces,
            TopicPartition::new(TRACES_TOPIC_ID, LogicalPartitionId::new(0)),
            LogicalOffset::new(0),
        ),
        tenant: Arc::from(tenant),
        resource,
        scope,
        trace_id,
        span_id,
        parent_span_id,
        trace_state: Arc::from(span.trace_state.as_str()),
        flags: span.flags,
        name: Arc::from(span.name.as_str()),
        kind: span.kind,
        start_time_unix_nanos: span.start_time_unix_nano,
        duration_nanos: span.end_time_unix_nano - span.start_time_unix_nano,
        attributes: Arc::new(decode_telemetry_attributes(&span.attributes)),
        dropped_attributes_count: span.dropped_attributes_count,
        events: Arc::new(events),
        dropped_events_count: span.dropped_events_count,
        links: Arc::new(links),
        dropped_links_count: span.dropped_links_count,
        status: span.status.as_ref().map(|status| SpanStatus {
            message: Arc::from(status.message.as_str()),
            code: status.code,
        }),
    })
}

fn decode_metric(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    metric: &Metric,
    output: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    validate_attributes(&metric.metadata)?;
    let metadata = Arc::new(decode_telemetry_attributes(&metric.metadata));
    let Some(data) = &metric.data else {
        return Err(TelemetryError::InvalidOtlpPayload(
            "metric has no data variant".into(),
        ));
    };
    match data {
        metric::Data::Gauge(gauge) => {
            for point in &gauge.data_points {
                push_number_point(
                    tenant,
                    Arc::clone(&resource),
                    Arc::clone(&scope),
                    metric,
                    Arc::clone(&metadata),
                    MetricKind::Gauge,
                    point,
                    false,
                    output,
                )?;
            }
        }
        metric::Data::Sum(sum) => {
            let kind = MetricKind::Sum {
                temporality: sum.aggregation_temporality,
                monotonic: sum.is_monotonic,
            };
            for point in &sum.data_points {
                push_number_point(
                    tenant,
                    Arc::clone(&resource),
                    Arc::clone(&scope),
                    metric,
                    Arc::clone(&metadata),
                    kind.clone(),
                    point,
                    true,
                    output,
                )?;
            }
        }
        metric::Data::Histogram(histogram) => {
            let kind = MetricKind::ExplicitHistogram {
                temporality: histogram.aggregation_temporality,
            };
            for point in &histogram.data_points {
                push_histogram_point(
                    tenant,
                    Arc::clone(&resource),
                    Arc::clone(&scope),
                    metric,
                    Arc::clone(&metadata),
                    kind.clone(),
                    point,
                    output,
                )?;
            }
        }
        metric::Data::ExponentialHistogram(histogram) => {
            let kind = MetricKind::ExponentialHistogram {
                temporality: histogram.aggregation_temporality,
            };
            for point in &histogram.data_points {
                push_exponential_histogram_point(
                    tenant,
                    Arc::clone(&resource),
                    Arc::clone(&scope),
                    metric,
                    Arc::clone(&metadata),
                    kind.clone(),
                    point,
                    output,
                )?;
            }
        }
        metric::Data::Summary(summary) => {
            for point in &summary.data_points {
                push_summary_point(
                    tenant,
                    Arc::clone(&resource),
                    Arc::clone(&scope),
                    metric,
                    Arc::clone(&metadata),
                    point,
                    output,
                )?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_number_point(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    metric: &Metric,
    metadata: Arc<Vec<crate::TelemetryAttribute>>,
    kind: MetricKind,
    point: &NumberDataPoint,
    sum: bool,
    output: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    validate_attributes(&point.attributes)?;
    let number = match point.value {
        Some(number_data_point::Value::AsDouble(value)) => NumberValue::from_f64(value),
        Some(number_data_point::Value::AsInt(value)) => NumberValue::Integer(value),
        None => {
            return Err(TelemetryError::InvalidOtlpPayload(
                "number data point has no value".into(),
            ));
        }
    };
    let value = if sum {
        MetricValue::Sum(number)
    } else {
        MetricValue::Gauge(number)
    };
    push_point(
        tenant,
        resource,
        scope,
        metric,
        metadata,
        kind,
        &point.attributes,
        point.start_time_unix_nano,
        point.time_unix_nano,
        point.flags,
        value,
        &point.exemplars,
        output,
    )
}

#[allow(clippy::too_many_arguments)]
fn push_histogram_point(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    metric: &Metric,
    metadata: Arc<Vec<crate::TelemetryAttribute>>,
    kind: MetricKind,
    point: &HistogramDataPoint,
    output: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    validate_attributes(&point.attributes)?;
    if (!point.bucket_counts.is_empty()
        && point.bucket_counts.len() != point.explicit_bounds.len().saturating_add(1))
        || (point.bucket_counts.is_empty() && !point.explicit_bounds.is_empty())
        || point
            .explicit_bounds
            .windows(2)
            .any(|values| values[0].total_cmp(&values[1]).is_ge())
    {
        return Err(TelemetryError::InvalidOtlpPayload(
            "invalid explicit histogram buckets".into(),
        ));
    }
    push_point(
        tenant,
        resource,
        scope,
        metric,
        metadata,
        kind,
        &point.attributes,
        point.start_time_unix_nano,
        point.time_unix_nano,
        point.flags,
        MetricValue::ExplicitHistogram(ExplicitHistogramValue {
            count: HistogramCount::Integer(point.count),
            sum_bits: point.sum.map(f64::to_bits),
            bucket_counts: Arc::new(
                point
                    .bucket_counts
                    .iter()
                    .copied()
                    .map(HistogramCount::Integer)
                    .collect(),
            ),
            explicit_bounds_bits: Arc::new(
                point
                    .explicit_bounds
                    .iter()
                    .map(|value| value.to_bits())
                    .collect(),
            ),
            min_bits: point.min.map(f64::to_bits),
            max_bits: point.max.map(f64::to_bits),
            reset_hint: 0,
        }),
        &point.exemplars,
        output,
    )
}

#[allow(clippy::too_many_arguments)]
fn push_exponential_histogram_point(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    metric: &Metric,
    metadata: Arc<Vec<crate::TelemetryAttribute>>,
    kind: MetricKind,
    point: &ExponentialHistogramDataPoint,
    output: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    validate_attributes(&point.attributes)?;
    let buckets = |buckets: &opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets| {
        ExponentialHistogramBuckets {
            spans: Arc::new(vec![HistogramBucketSpan {
                offset: buckets.offset,
                length: u32::try_from(buckets.bucket_counts.len()).unwrap_or(u32::MAX),
            }]),
            bucket_counts: Arc::new(
                buckets
                    .bucket_counts
                    .iter()
                    .copied()
                    .map(HistogramCount::Integer)
                    .collect(),
            ),
        }
    };
    push_point(
        tenant,
        resource,
        scope,
        metric,
        metadata,
        kind,
        &point.attributes,
        point.start_time_unix_nano,
        point.time_unix_nano,
        point.flags,
        MetricValue::ExponentialHistogram(ExponentialHistogramValue {
            count: HistogramCount::Integer(point.count),
            sum_bits: point.sum.map(f64::to_bits),
            scale: point.scale,
            zero_count: HistogramCount::Integer(point.zero_count),
            positive: point.positive.as_ref().map(buckets),
            negative: point.negative.as_ref().map(buckets),
            min_bits: point.min.map(f64::to_bits),
            max_bits: point.max.map(f64::to_bits),
            zero_threshold_bits: point.zero_threshold.to_bits(),
            reset_hint: 0,
        }),
        &point.exemplars,
        output,
    )
}

#[allow(clippy::too_many_arguments)]
fn push_summary_point(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    metric: &Metric,
    metadata: Arc<Vec<crate::TelemetryAttribute>>,
    point: &SummaryDataPoint,
    output: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    validate_attributes(&point.attributes)?;
    if point
        .quantile_values
        .windows(2)
        .any(|values| values[0].quantile.total_cmp(&values[1].quantile).is_ge())
    {
        return Err(TelemetryError::InvalidOtlpPayload(
            "summary quantiles are not strictly increasing".into(),
        ));
    }
    push_point(
        tenant,
        resource,
        scope,
        metric,
        metadata,
        MetricKind::Summary,
        &point.attributes,
        point.start_time_unix_nano,
        point.time_unix_nano,
        point.flags,
        MetricValue::Summary(SummaryValue {
            count: point.count,
            sum_bits: point.sum.to_bits(),
            quantiles: Arc::new(
                point
                    .quantile_values
                    .iter()
                    .map(|value| SummaryQuantileValue {
                        quantile_bits: value.quantile.to_bits(),
                        value_bits: value.value.to_bits(),
                    })
                    .collect(),
            ),
        }),
        &[],
        output,
    )
}

#[allow(clippy::too_many_arguments)]
fn push_point(
    tenant: &str,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    metric: &Metric,
    metadata: Arc<Vec<crate::TelemetryAttribute>>,
    kind: MetricKind,
    attributes: &[KeyValue],
    start_time_unix_nanos: u64,
    timestamp_unix_nanos: u64,
    flags: u32,
    value: MetricValue,
    exemplars: &[Exemplar],
    output: &mut Vec<OtlpMetricEvent>,
) -> TelemetryResult<()> {
    if timestamp_unix_nanos == 0 {
        return Err(TelemetryError::InvalidOtlpPayload(
            "metric point timestamp must be nonzero".into(),
        ));
    }
    let point_attributes = Arc::new(decode_telemetry_attributes(attributes));
    let identity = Arc::new(MetricIdentity {
        tenant: Arc::from(tenant),
        resource,
        scope,
        name: Arc::from(metric.name.as_str()),
        unit: Arc::from(metric.unit.as_str()),
        kind,
        point_attributes,
    });
    output.push(OtlpMetricEvent {
        point: DurableMetricPoint {
            stream_shard_id: ShardId::new(0),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Metrics,
                TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
            ),
            identity,
            description: Arc::from(metric.description.as_str()),
            metadata,
            start_time_unix_nanos,
            timestamp_unix_nanos,
            flags,
            value,
            exemplars: Arc::new(decode_exemplars(exemplars)?),
        },
    });
    Ok(())
}

fn decode_exemplars(exemplars: &[Exemplar]) -> TelemetryResult<Vec<MetricExemplar>> {
    exemplars
        .iter()
        .map(|value| {
            validate_attributes(&value.filtered_attributes)?;
            let number = match value.value {
                Some(exemplar::Value::AsDouble(value)) => NumberValue::from_f64(value),
                Some(exemplar::Value::AsInt(value)) => NumberValue::Integer(value),
                None => {
                    return Err(TelemetryError::InvalidOtlpPayload(
                        "metric exemplar has no value".into(),
                    ));
                }
            };
            Ok(MetricExemplar {
                filtered_attributes: Arc::new(decode_telemetry_attributes(
                    &value.filtered_attributes,
                )),
                timestamp_unix_nanos: value.time_unix_nano,
                value: number,
                span_id: optional_span_id(&value.span_id)?,
                trace_id: optional_trace_id(&value.trace_id)?,
            })
        })
        .collect()
}

fn validate_attributes(attributes: &[KeyValue]) -> TelemetryResult<()> {
    let mut keys = HashSet::with_capacity(attributes.len());
    for attribute in attributes {
        if attribute.key.is_empty() && attribute.key_strindex == 0 {
            return Err(TelemetryError::InvalidOtlpPayload(
                "attribute key must not be empty".into(),
            ));
        }
        if !attribute.key.is_empty() && !keys.insert(attribute.key.as_str()) {
            return Err(TelemetryError::InvalidOtlpPayload(
                "attribute keys must be unique".into(),
            ));
        }
        if let Some(value) = &attribute.value {
            validate_any_value(value)?;
        }
    }
    Ok(())
}

fn validate_any_value(
    value: &opentelemetry_proto::tonic::common::v1::AnyValue,
) -> TelemetryResult<()> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match &value.value {
        Some(Value::ArrayValue(values)) => {
            for value in &values.values {
                validate_any_value(value)?;
            }
        }
        Some(Value::KvlistValue(values)) => validate_attributes(&values.values)?,
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        collector::{
            metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest,
        },
        metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
            number_data_point,
        },
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };

    use super::*;

    #[test]
    fn complete_trace_request_decodes_before_partitioning() {
        let span = Span {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 10,
            end_time_unix_nano: 30,
            name: "root".into(),
            ..Span::default()
        };
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![span],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let decoded = OtlpTelemetryDecoder
            .decode_traces("tenant-a", &request.encode_to_vec())
            .unwrap();
        assert_eq!(decoded.len(), 1);
        let router = TelemetryRouter::new(std::num::NonZeroU16::new(256).unwrap());
        let partitions = OtlpTelemetryDecoder.partition_traces(&router, decoded);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions.keys().next().unwrap().topic_id, TRACES_TOPIC_ID);
    }

    #[test]
    fn metric_nan_payload_is_preserved() {
        let bits = 0x7ff8_0000_0000_0042;
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "temperature".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 10,
                                value: Some(number_data_point::Value::AsDouble(f64::from_bits(
                                    bits,
                                ))),
                                ..NumberDataPoint::default()
                            }],
                        })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        };
        let decoded = OtlpTelemetryDecoder
            .decode_metrics("tenant-a", &request.encode_to_vec())
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(matches!(
            decoded[0].point.value,
            MetricValue::Gauge(NumberValue::DoubleBits(value)) if value == bits
        ));
    }

    #[test]
    fn invalid_span_rejects_the_complete_request() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 15],
                        span_id: vec![2; 8],
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        assert!(
            OtlpTelemetryDecoder
                .decode_traces("tenant-a", &request.encode_to_vec())
                .is_err()
        );
    }
}
