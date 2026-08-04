//! Clean-room Tempo query wire messages and OTLP reconstruction.
//!
//! These field layouts are implemented from Tempo's published API contract.
//! No Tempo source code or fixtures are included.

use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};
use prost::Message;

use crate::{DurableSpan, TelemetryAttribute, TelemetryValue, TraceqlTrace};

/// Tempo v2 trace-by-ID response.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct TraceByIdResponse {
    /// Reconstructed trace.
    #[prost(message, optional, tag = "1")]
    pub(crate) trace: Option<TempoTrace>,
    /// Query inspection metrics.
    #[prost(message, optional, tag = "2")]
    pub(crate) metrics: Option<TraceByIdMetrics>,
}

/// Tempo trace payload containing OTLP resource-span batches.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct TempoTrace {
    /// OTLP resource batches.
    #[prost(message, repeated, tag = "1")]
    pub(crate) batches: Vec<ResourceSpans>,
}

/// Trace-by-ID query metrics.
#[derive(Clone, Copy, PartialEq, Message)]
pub(crate) struct TraceByIdMetrics {
    /// Number of bytes inspected by the query.
    #[prost(uint64, tag = "1")]
    pub(crate) inspected_bytes: u64,
}

pub(crate) fn trace_by_id_response(trace: &TraceqlTrace) -> TraceByIdResponse {
    TraceByIdResponse {
        trace: Some(TempoTrace {
            batches: trace.spans.iter().map(resource_spans).collect(),
        }),
        metrics: Some(TraceByIdMetrics { inspected_bytes: 0 }),
    }
}

fn resource_spans(span: &DurableSpan) -> ResourceSpans {
    ResourceSpans {
        resource: Some(Resource {
            attributes: attributes(&span.resource.attributes),
            dropped_attributes_count: span.resource.dropped_attributes_count,
            entity_refs: span
                .resource
                .entity_refs
                .iter()
                .map(|entity| EntityRef {
                    schema_url: entity.schema_url.to_string(),
                    r#type: entity.entity_type.to_string(),
                    id_keys: entity.id_keys.iter().map(ToString::to_string).collect(),
                    description_keys: entity
                        .description_keys
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                })
                .collect(),
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope {
                name: span.scope.name.to_string(),
                version: span.scope.version.to_string(),
                attributes: attributes(&span.scope.attributes),
                dropped_attributes_count: span.scope.dropped_attributes_count,
            }),
            spans: vec![otlp_span(span)],
            schema_url: span.scope.schema_url.to_string(),
        }],
        schema_url: span.resource.schema_url.to_string(),
    }
}

fn otlp_span(record: &DurableSpan) -> Span {
    Span {
        trace_id: record.trace_id.as_bytes().to_vec(),
        span_id: record.span_id.as_bytes().to_vec(),
        trace_state: record.trace_state.to_string(),
        parent_span_id: record
            .parent_span_id
            .map_or_else(Vec::new, |value| value.as_bytes().to_vec()),
        flags: record.flags,
        name: record.name.to_string(),
        kind: record.kind,
        start_time_unix_nano: record.start_time_unix_nanos,
        end_time_unix_nano: record.end_time_unix_nanos().unwrap_or(u64::MAX),
        attributes: attributes(&record.attributes),
        dropped_attributes_count: record.dropped_attributes_count,
        events: record
            .events
            .iter()
            .map(|event| span::Event {
                time_unix_nano: event.timestamp_unix_nanos,
                name: event.name.to_string(),
                attributes: attributes(&event.attributes),
                dropped_attributes_count: event.dropped_attributes_count,
            })
            .collect(),
        dropped_events_count: record.dropped_events_count,
        links: record
            .links
            .iter()
            .map(|link| span::Link {
                trace_id: link.trace_id.as_bytes().to_vec(),
                span_id: link.span_id.as_bytes().to_vec(),
                trace_state: link.trace_state.to_string(),
                attributes: attributes(&link.attributes),
                dropped_attributes_count: link.dropped_attributes_count,
                flags: link.flags,
            })
            .collect(),
        dropped_links_count: record.dropped_links_count,
        status: record.status.as_ref().map(|status| Status {
            message: status.message.to_string(),
            code: status.code,
        }),
    }
}

fn attributes(values: &[TelemetryAttribute]) -> Vec<KeyValue> {
    values
        .iter()
        .map(|attribute| KeyValue {
            key: attribute.key.to_string(),
            value: attribute.value.as_ref().map(any_value),
            key_strindex: attribute.key_strindex,
        })
        .collect()
}

fn any_value(value: &TelemetryValue) -> AnyValue {
    let value = match value {
        TelemetryValue::Empty => None,
        TelemetryValue::String(value) => Some(any_value::Value::StringValue(value.to_string())),
        TelemetryValue::Boolean(value) => Some(any_value::Value::BoolValue(*value)),
        TelemetryValue::Integer(value) => Some(any_value::Value::IntValue(*value)),
        TelemetryValue::DoubleBits(bits) => {
            Some(any_value::Value::DoubleValue(f64::from_bits(*bits)))
        }
        TelemetryValue::Bytes(value) => Some(any_value::Value::BytesValue(value.to_vec())),
        TelemetryValue::Array(values) => Some(any_value::Value::ArrayValue(ArrayValue {
            values: values.iter().map(any_value).collect(),
        })),
        TelemetryValue::Map(values) => Some(any_value::Value::KvlistValue(KeyValueList {
            values: attributes(values),
        })),
        TelemetryValue::StringTableIndex(value) => {
            Some(any_value::Value::StringValueStrindex(*value))
        }
    };
    AnyValue { value }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

    use super::*;
    use crate::{
        ResourceContext, ScopeContext, SpanId, TRACES_TOPIC_ID, TelemetryRecordRef,
        TelemetrySignal, TraceId,
    };

    #[test]
    fn trace_response_reconstructs_exact_otlp_identifiers_and_times() {
        let trace_id = TraceId::from_bytes([1; 16]).unwrap();
        let span_id = SpanId::from_bytes([2; 8]).unwrap();
        let span = DurableSpan {
            stream_shard_id: ShardId::new(1),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Traces,
                TopicPartition::new(TRACES_TOPIC_ID, LogicalPartitionId::new(2)),
                LogicalOffset::new(3),
            ),
            tenant: Arc::from("tenant-a"),
            resource: Arc::new(ResourceContext::default()),
            scope: Arc::new(ScopeContext::default()),
            trace_id,
            span_id,
            parent_span_id: None,
            trace_state: Arc::from(""),
            flags: 1,
            name: Arc::from("root"),
            kind: 1,
            start_time_unix_nanos: 10,
            duration_nanos: 5,
            attributes: Arc::new(Vec::new()),
            dropped_attributes_count: 0,
            events: Arc::new(Vec::new()),
            dropped_events_count: 0,
            links: Arc::new(Vec::new()),
            dropped_links_count: 0,
            status: None,
        };
        let response = trace_by_id_response(&TraceqlTrace {
            trace_id,
            spans: vec![span],
            start_time_unix_nanos: 10,
            end_time_unix_nanos: 15,
            root_name: Some(Arc::from("root")),
            root_service_name: None,
            error_count: 0,
        });
        let span = &response.trace.unwrap().batches[0].scope_spans[0].spans[0];
        assert_eq!(span.trace_id, [1; 16]);
        assert_eq!(span.span_id, [2; 8]);
        assert_eq!(span.end_time_unix_nano, 15);
    }
}
