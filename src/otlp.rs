use std::sync::Arc;

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;
use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::{
    CompressionCohortId, DurableLog, MetadataField, ResourceContext, ScopeContext, SpanId,
    TelemetryAttribute, TelemetryEntityRef, TelemetryError, TelemetryRecordRef, TelemetryResult,
    TelemetrySignal, TelemetryValue, TraceId,
};

/// One normalized OpenTelemetry log event before shard-stream assigns its offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpLogEvent {
    /// Original event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Original observed timestamp in Unix nanoseconds.
    pub observed_timestamp_unix_nanos: u64,
    /// Exact typed body.
    pub body: Option<TelemetryValue>,
    /// Preserved human-readable or structured log body.
    pub message: Arc<str>,
    /// Resource, instrumentation scope, record, and promoted OTLP metadata.
    pub fields: Arc<Vec<MetadataField>>,
    /// Exact typed record attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Exact resource context.
    pub resource: Arc<ResourceContext>,
    /// Exact scope context.
    pub scope: Arc<ScopeContext>,
    /// OTLP severity enum value.
    pub severity_number: i32,
    /// Exact severity text.
    pub severity_text: Arc<str>,
    /// Dropped record-attribute count.
    pub dropped_attributes_count: u32,
    /// OTLP flags.
    pub flags: u32,
    /// Binary trace ID when present.
    pub trace_id: Option<TraceId>,
    /// Binary span ID when present.
    pub span_id: Option<SpanId>,
    /// Exact event name.
    pub event_name: Arc<str>,
    /// Stable compression cohort derived from service and instrumentation scope identity.
    pub compression_cohort: CompressionCohortId,
}

impl Default for OtlpLogEvent {
    fn default() -> Self {
        Self {
            timestamp_unix_nanos: 0,
            observed_timestamp_unix_nanos: 0,
            body: None,
            message: Arc::from(""),
            fields: Arc::new(Vec::new()),
            attributes: Arc::new(Vec::new()),
            resource: Arc::new(ResourceContext::default()),
            scope: Arc::new(ScopeContext::default()),
            severity_number: 0,
            severity_text: Arc::from(""),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: None,
            span_id: None,
            event_name: Arc::from(""),
            compression_cohort: CompressionCohortId::new(0),
        }
    }
}

impl OtlpLogEvent {
    /// Associates this normalized event with its owning shard-stream log offset.
    #[must_use]
    pub fn into_durable(
        self,
        stream_shard_id: ShardId,
        topic_partition: TopicPartition,
        offset: LogicalOffset,
    ) -> DurableLog {
        DurableLog {
            stream_shard_id,
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Logs,
                topic_partition,
                offset,
            ),
            timestamp_unix_nanos: self.timestamp_unix_nanos,
            observed_timestamp_unix_nanos: self.observed_timestamp_unix_nanos,
            body: self.body,
            message: self.message,
            fields: self.fields,
            attributes: self.attributes,
            resource: self.resource,
            scope: self.scope,
            severity_number: self.severity_number,
            severity_text: self.severity_text,
            dropped_attributes_count: self.dropped_attributes_count,
            flags: self.flags,
            trace_id: self.trace_id,
            span_id: self.span_id,
            event_name: self.event_name,
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
    pub fn decode(&self, payload: &[u8]) -> TelemetryResult<Vec<OtlpLogEvent>> {
        let request = ExportLogsServiceRequest::decode(payload)
            .map_err(|error| TelemetryError::InvalidOtlpPayload(error.to_string()))?;
        let mut events = Vec::new();
        for resource_logs in &request.resource_logs {
            events.extend(decode_resource_logs(resource_logs)?);
        }
        Ok(events)
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
    ) -> TelemetryResult<Vec<DurableLog>> {
        self.decode(payload)?
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let relative_offset = u64::try_from(index)
                    .map_err(|_| TelemetryError::OffsetExhausted(topic_partition))?;
                let offset = first_offset
                    .get()
                    .checked_add(relative_offset)
                    .map(LogicalOffset::new)
                    .ok_or(TelemetryError::OffsetExhausted(topic_partition))?;
                Ok(event.into_durable(stream_shard_id, topic_partition, offset))
            })
            .collect()
    }
}

fn decode_resource_logs(resource_logs: &ResourceLogs) -> TelemetryResult<Vec<OtlpLogEvent>> {
    let resource_fields = resource_logs
        .resource
        .as_ref()
        .map_or_else(Vec::new, resource_fields);
    let resource = Arc::new(resource_context(resource_logs));
    let mut events = Vec::new();
    for scope_logs in &resource_logs.scope_logs {
        events.extend(decode_scope_logs(
            scope_logs,
            &resource_fields,
            Arc::clone(&resource),
        )?);
    }
    Ok(events)
}

fn decode_scope_logs(
    scope_logs: &ScopeLogs,
    resource_fields: &[MetadataField],
    resource: Arc<ResourceContext>,
) -> TelemetryResult<Vec<OtlpLogEvent>> {
    let scope_fields = scope_fields(scope_logs.scope.as_ref());
    let scope = Arc::new(scope_context(scope_logs));
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
            let trace_id = optional_trace_id(&record.trace_id)?;
            let span_id = optional_span_id(&record.span_id)?;
            Ok(OtlpLogEvent {
                timestamp_unix_nanos: record.time_unix_nano,
                observed_timestamp_unix_nanos: record.observed_time_unix_nano,
                body: record.body.as_ref().map(decode_telemetry_value),
                message: body_text(record.body.as_ref()),
                fields: Arc::new(fields),
                attributes: Arc::new(decode_telemetry_attributes(&record.attributes)),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                severity_number: record.severity_number,
                severity_text: Arc::from(record.severity_text.as_str()),
                dropped_attributes_count: record.dropped_attributes_count,
                flags: record.flags,
                trace_id,
                span_id,
                event_name: Arc::from(record.event_name.as_str()),
                compression_cohort,
            })
        })
        .collect()
}

fn resource_context(resource_logs: &ResourceLogs) -> ResourceContext {
    decode_resource_context(resource_logs.resource.as_ref(), &resource_logs.schema_url)
}

pub(crate) fn decode_resource_context(
    resource: Option<&Resource>,
    schema_url: &str,
) -> ResourceContext {
    ResourceContext {
        attributes: Arc::new(resource.map_or_else(Vec::new, |value| {
            decode_telemetry_attributes(&value.attributes)
        })),
        dropped_attributes_count: resource.map_or(0, |value| value.dropped_attributes_count),
        schema_url: Arc::from(schema_url),
        entity_refs: Arc::new(resource.map_or_else(Vec::new, |value| {
            value
                .entity_refs
                .iter()
                .map(|entity| TelemetryEntityRef {
                    schema_url: Arc::from(entity.schema_url.as_str()),
                    entity_type: Arc::from(entity.r#type.as_str()),
                    id_keys: Arc::new(
                        entity
                            .id_keys
                            .iter()
                            .map(|value| Arc::from(value.as_str()))
                            .collect(),
                    ),
                    description_keys: Arc::new(
                        entity
                            .description_keys
                            .iter()
                            .map(|value| Arc::from(value.as_str()))
                            .collect(),
                    ),
                })
                .collect()
        })),
    }
}

fn scope_context(scope_logs: &ScopeLogs) -> ScopeContext {
    decode_scope_context(scope_logs.scope.as_ref(), &scope_logs.schema_url)
}

pub(crate) fn decode_scope_context(
    scope: Option<&InstrumentationScope>,
    schema_url: &str,
) -> ScopeContext {
    ScopeContext {
        name: Arc::from(scope.map_or("", |value| value.name.as_str())),
        version: Arc::from(scope.map_or("", |value| value.version.as_str())),
        attributes: Arc::new(scope.map_or_else(Vec::new, |value| {
            decode_telemetry_attributes(&value.attributes)
        })),
        dropped_attributes_count: scope.map_or(0, |value| value.dropped_attributes_count),
        schema_url: Arc::from(schema_url),
    }
}

pub(crate) fn decode_telemetry_attributes(attributes: &[KeyValue]) -> Vec<TelemetryAttribute> {
    attributes
        .iter()
        .map(|attribute| TelemetryAttribute {
            key: Arc::from(attribute.key.as_str()),
            key_strindex: attribute.key_strindex,
            value: attribute.value.as_ref().map(decode_telemetry_value),
        })
        .collect()
}

pub(crate) fn decode_telemetry_value(value: &AnyValue) -> TelemetryValue {
    match value.value.as_ref() {
        None => TelemetryValue::Empty,
        Some(Value::StringValue(value)) => TelemetryValue::String(Arc::from(value.as_str())),
        Some(Value::BoolValue(value)) => TelemetryValue::Boolean(*value),
        Some(Value::IntValue(value)) => TelemetryValue::Integer(*value),
        Some(Value::DoubleValue(value)) => TelemetryValue::DoubleBits(value.to_bits()),
        Some(Value::BytesValue(value)) => TelemetryValue::Bytes(Arc::from(value.as_slice())),
        Some(Value::ArrayValue(values)) => TelemetryValue::Array(Arc::new(
            values.values.iter().map(decode_telemetry_value).collect(),
        )),
        Some(Value::KvlistValue(values)) => {
            TelemetryValue::Map(Arc::new(decode_telemetry_attributes(&values.values)))
        }
        Some(Value::StringValueStrindex(value)) => TelemetryValue::StringTableIndex(*value),
    }
}

pub(crate) fn optional_trace_id(bytes: &[u8]) -> TelemetryResult<Option<TraceId>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        TraceId::from_slice(bytes).map(Some)
    }
}

pub(crate) fn optional_span_id(bytes: &[u8]) -> TelemetryResult<Option<SpanId>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        SpanId::from_slice(bytes).map(Some)
    }
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
                        trace_id: vec![1; 16],
                        span_id: vec![3; 8],
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
        assert_eq!(event.timestamp_unix_nanos, 11);
        assert_eq!(event.observed_timestamp_unix_nanos, 12);
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
        assert!(event.fields.contains(&MetadataField::new(
            "otel.trace_id",
            "01010101010101010101010101010101"
        )));
        assert_eq!(event.trace_id, Some(TraceId::from_bytes([1; 16]).unwrap()));
        assert_eq!(event.span_id, Some(SpanId::from_bytes([3; 8]).unwrap()));
        assert_eq!(
            event.body,
            Some(TelemetryValue::String(Arc::from("payment declined")))
        );

        let packed = crate::ingest_pack::prepare_ingest_pack(&events).expect("typed log packs");
        let recovered = crate::ingest_pack::decode_ingest_pack(&packed.payload)
            .expect("typed log pack recovers");
        assert_eq!(recovered, events);

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
        assert!(matches!(error, TelemetryError::InvalidOtlpPayload(_)));
    }
}
