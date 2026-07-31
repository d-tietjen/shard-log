use std::sync::Arc;

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;
use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::{CompressionCohortId, DurableLogRecord, LogDbError, LogDbResult, MetadataField};

/// One normalized OpenTelemetry log event before shard-stream assigns its offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpLogEvent {
    /// Event timestamp in Unix nanoseconds. Uses observed time when event time is absent.
    pub timestamp_unix_nanos: u64,
    /// Preserved human-readable or structured log body.
    pub message: Arc<str>,
    /// Resource, instrumentation scope, record, and promoted OTLP metadata.
    pub fields: Arc<Vec<MetadataField>>,
    /// Stable compression cohort derived from service and instrumentation scope identity.
    pub compression_cohort: CompressionCohortId,
}

impl OtlpLogEvent {
    /// Associates this normalized event with its owning shard-stream log offset.
    #[must_use]
    pub fn into_durable(
        self,
        stream_shard_id: ShardId,
        topic_partition: TopicPartition,
        offset: LogicalOffset,
    ) -> DurableLogRecord {
        DurableLogRecord {
            stream_shard_id,
            record_ref: crate::RecordRef::new(topic_partition, offset),
            timestamp_unix_nanos: self.timestamp_unix_nanos,
            message: self.message,
            fields: self.fields,
            compression_cohort: self.compression_cohort,
        }
    }
}

/// Decoder for binary OTLP `ExportLogsServiceRequest` payloads.
///
/// OTLP/HTTP uses the same protobuf request message as OTLP/gRPC. The decoder
/// is transport-independent: the HTTP or gRPC server removes framing and hands
/// the protobuf message bytes to [`Self::decode`].
#[derive(Debug, Default, Clone, Copy)]
pub struct OtlpLogDecoder;

impl OtlpLogDecoder {
    /// Decodes one binary OTLP Logs export request into normalized log events.
    pub fn decode(&self, payload: &[u8]) -> LogDbResult<Vec<OtlpLogEvent>> {
        let request = ExportLogsServiceRequest::decode(payload)
            .map_err(|error| LogDbError::InvalidOtlpPayload(error.to_string()))?;
        Ok(request
            .resource_logs
            .iter()
            .flat_map(decode_resource_logs)
            .collect())
    }

    /// Decodes an OTLP export and assigns its events contiguous shard-stream offsets.
    ///
    /// Call this only after shard-stream has reserved the range beginning at
    /// `first_offset`. The caller must also ensure the reserved record count
    /// equals the returned vector length before acknowledging the append.
    pub fn decode_durable(
        &self,
        stream_shard_id: ShardId,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        payload: &[u8],
    ) -> LogDbResult<Vec<DurableLogRecord>> {
        self.decode(payload)?
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let relative_offset = u64::try_from(index)
                    .map_err(|_| LogDbError::OffsetExhausted(topic_partition))?;
                let offset = first_offset
                    .get()
                    .checked_add(relative_offset)
                    .map(LogicalOffset::new)
                    .ok_or(LogDbError::OffsetExhausted(topic_partition))?;
                Ok(event.into_durable(stream_shard_id, topic_partition, offset))
            })
            .collect()
    }
}

fn decode_resource_logs(resource_logs: &ResourceLogs) -> Vec<OtlpLogEvent> {
    let resource_fields = resource_logs
        .resource
        .as_ref()
        .map_or_else(Vec::new, resource_fields);
    resource_logs
        .scope_logs
        .iter()
        .flat_map(|scope_logs| decode_scope_logs(scope_logs, &resource_fields))
        .collect()
}

fn decode_scope_logs(
    scope_logs: &ScopeLogs,
    resource_fields: &[MetadataField],
) -> Vec<OtlpLogEvent> {
    let scope_fields = scope_fields(scope_logs.scope.as_ref());
    let compression_cohort = compression_cohort(resource_fields, scope_logs.scope.as_ref());
    scope_logs
        .log_records
        .iter()
        .map(|record| {
            let mut fields = Vec::with_capacity(
                resource_fields.len() + scope_fields.len() + record.attributes.len() + 8,
            );
            fields.extend_from_slice(resource_fields);
            fields.extend_from_slice(&scope_fields);
            fields.extend(record_fields(record));
            OtlpLogEvent {
                timestamp_unix_nanos: record.time_unix_nano.max(record.observed_time_unix_nano),
                message: body_text(record.body.as_ref()),
                fields: Arc::new(fields),
                compression_cohort,
            }
        })
        .collect()
}

fn resource_fields(resource: &Resource) -> Vec<MetadataField> {
    let mut fields = attributes("resource.", &resource.attributes);
    for attribute in &resource.attributes {
        if attribute.key == "service.name" {
            fields.push(MetadataField::new(
                "service.name",
                any_value_text(attribute.value.as_ref()),
            ));
        }
    }
    fields
}

fn scope_fields(scope: Option<&InstrumentationScope>) -> Vec<MetadataField> {
    let Some(scope) = scope else {
        return Vec::new();
    };
    let mut fields = Vec::with_capacity(scope.attributes.len() + 2);
    if !scope.name.is_empty() {
        fields.push(MetadataField::new("otel.scope.name", scope.name.clone()));
    }
    if !scope.version.is_empty() {
        fields.push(MetadataField::new(
            "otel.scope.version",
            scope.version.clone(),
        ));
    }
    fields.extend(attributes("scope.", &scope.attributes));
    fields
}

fn record_fields(record: &LogRecord) -> Vec<MetadataField> {
    let mut fields = Vec::with_capacity(record.attributes.len() + 6);
    if record.severity_number != 0 {
        fields.push(MetadataField::new(
            "otel.severity_number",
            record.severity_number.to_string(),
        ));
    }
    if !record.severity_text.is_empty() {
        fields.push(MetadataField::new(
            "otel.severity_text",
            record.severity_text.clone(),
        ));
    }
    if !record.trace_id.is_empty() {
        fields.push(MetadataField::new("otel.trace_id", hex(&record.trace_id)));
    }
    if !record.span_id.is_empty() {
        fields.push(MetadataField::new("otel.span_id", hex(&record.span_id)));
    }
    if !record.event_name.is_empty() {
        fields.push(MetadataField::new(
            "otel.event_name",
            record.event_name.clone(),
        ));
    }
    fields.extend(attributes("attr.", &record.attributes));
    fields
}

fn attributes(prefix: &str, attributes: &[KeyValue]) -> Vec<MetadataField> {
    attributes
        .iter()
        .filter(|attribute| !attribute.key.is_empty())
        .map(|attribute| {
            MetadataField::new(
                format!("{prefix}{}", attribute.key),
                any_value_text(attribute.value.as_ref()),
            )
        })
        .collect()
}

fn body_text(body: Option<&AnyValue>) -> Arc<str> {
    Arc::from(any_value_text(body))
}

fn any_value_text(value: Option<&AnyValue>) -> String {
    let Some(value) = value.and_then(|value| value.value.as_ref()) else {
        return "null".into();
    };
    match value {
        Value::StringValue(value) => value.clone(),
        Value::BoolValue(value) => value.to_string(),
        Value::IntValue(value) => value.to_string(),
        Value::DoubleValue(value) => value.to_string(),
        Value::BytesValue(value) => hex(value),
        Value::ArrayValue(values) => {
            let values = values
                .values
                .iter()
                .map(|value| any_value_text(Some(value)))
                .collect::<Vec<_>>();
            format!("[{}]", values.join(","))
        }
        Value::KvlistValue(values) => {
            let values = values
                .values
                .iter()
                .map(|value| format!("{}={}", value.key, any_value_text(value.value.as_ref())))
                .collect::<Vec<_>>();
            format!("{{{}}}", values.join(","))
        }
        // This development-only variant is reserved for the Profiling signal.
        // The OTLP schema directs non-Profiling receivers to treat it as empty.
        Value::StringValueStrindex(_) => String::new(),
    }
}

fn compression_cohort(
    resource_fields: &[MetadataField],
    scope: Option<&InstrumentationScope>,
) -> CompressionCohortId {
    let service = resource_fields
        .iter()
        .find(|field| field.key.as_ref() == "service.name")
        .map_or("", |field| field.value.as_ref());
    let scope_name = scope.map_or("", |scope| scope.name.as_str());
    let scope_version = scope.map_or("", |scope| scope.version.as_str());
    CompressionCohortId::new(fnv1a64([
        service.as_bytes(),
        scope_name.as_bytes(),
        scope_version.as_bytes(),
    ]))
}

fn fnv1a64(parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for part in parts {
        for byte in part.as_ref() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message;
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use super::*;

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.into())),
            }),
            key_strindex: 0,
        }
    }

    #[test]
    fn decodes_resource_scope_and_record_metadata() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_attribute("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "checkout-api".into(),
                        version: "1.0.0".into(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 11,
                        observed_time_unix_nano: 12,
                        severity_number: 17,
                        severity_text: "ERROR".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("payment declined".into())),
                        }),
                        attributes: vec![string_attribute("http.status_code", "402")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![1, 2],
                        span_id: vec![3],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let payload = request.encode_to_vec();

        let events = OtlpLogDecoder.decode(&payload).expect("payload decodes");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.timestamp_unix_nanos, 12);
        assert_eq!(event.message.as_ref(), "payment declined");
        assert!(
            event
                .fields
                .contains(&MetadataField::new("service.name", "checkout"))
        );
        assert!(
            event
                .fields
                .contains(&MetadataField::new("resource.service.name", "checkout"))
        );
        assert!(
            event
                .fields
                .contains(&MetadataField::new("otel.scope.name", "checkout-api"))
        );
        assert!(
            event
                .fields
                .contains(&MetadataField::new("attr.http.status_code", "402"))
        );
        assert!(
            event
                .fields
                .contains(&MetadataField::new("otel.trace_id", "0102"))
        );

        let durable = event.clone().into_durable(
            ShardId::new(7),
            TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(2)),
            LogicalOffset::new(3),
        );
        assert_eq!(durable.record_ref.offset, LogicalOffset::new(3));

        let durable = OtlpLogDecoder
            .decode_durable(
                ShardId::new(7),
                TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(2)),
                LogicalOffset::new(3),
                &payload,
            )
            .expect("durable records decode");
        assert_eq!(durable.len(), 1);
        assert_eq!(durable[0].record_ref.offset, LogicalOffset::new(3));
    }

    #[test]
    fn rejects_non_protobuf_payloads() {
        let error = OtlpLogDecoder
            .decode(b"not a protobuf request")
            .expect_err("invalid input");
        assert!(matches!(error, LogDbError::InvalidOtlpPayload(_)));
    }
}
