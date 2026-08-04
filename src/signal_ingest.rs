use std::sync::Arc;

use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::ingest_pack::prepare_ingest_pack;
use crate::{
    CompressionCohortId, LokiEntry, MetadataField, MetricIngestProtocol, OtlpLogEvent,
    OtlpMetricEvent, OtlpSpanEvent, ResourceContext, ScopeContext, TelemetryAttribute,
    TelemetryEnvelope, TelemetryError, TelemetryResult, TelemetrySignal, TelemetryValue,
    encode_metric_chunk, encode_trace_block,
};

const LABEL_PREFIX: &str = "resource.loki.label.";
const METADATA_PREFIX: &str = "attr.loki.metadata.";
const TENANT_FIELD: &str = "resource.loki.tenant";

/// Builds one durable STEL log envelope from already validated events.
pub fn prepare_log_envelope(
    tenant: &str,
    events: &[OtlpLogEvent],
) -> TelemetryResult<TelemetryEnvelope> {
    let prepared = prepare_ingest_pack(events)?;
    TelemetryEnvelope::new(
        TelemetrySignal::Logs,
        tenant,
        u32::try_from(events.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        Arc::<[u8]>::from([]),
        Arc::<[u8]>::from(prepared.payload),
    )
}

/// Converts Loki entries into the single typed log representation and STEL envelope.
pub fn prepare_loki_log_envelope(
    tenant: &str,
    entries: Vec<LokiEntry>,
) -> TelemetryResult<TelemetryEnvelope> {
    if tenant.is_empty() {
        return Err(TelemetryError::InvalidNativePayload(
            "Loki tenant must not be empty".into(),
        ));
    }
    let mut events = Vec::with_capacity(entries.len());
    for entry in entries {
        let timestamp_unix_nanos = u64::try_from(entry.timestamp_unix_nanos).map_err(|_| {
            TelemetryError::InvalidNativePayload(
                "negative Loki timestamps are outside the storage epoch".into(),
            )
        })?;
        let mut fields = Vec::with_capacity(
            1 + entry
                .labels
                .len()
                .saturating_add(entry.structured_metadata.len()),
        );
        fields.push(MetadataField::new(TENANT_FIELD, tenant));
        let mut resource_attributes = Vec::with_capacity(entry.labels.len());
        let mut cohort = blake3::Hasher::new();
        for (key, value) in entry.labels {
            cohort.update(&(key.len() as u64).to_le_bytes());
            cohort.update(key.as_bytes());
            cohort.update(&(value.len() as u64).to_le_bytes());
            cohort.update(value.as_bytes());
            fields.push(MetadataField::new(
                format!("{LABEL_PREFIX}{key}"),
                value.clone(),
            ));
            resource_attributes.push(TelemetryAttribute::new(
                key,
                TelemetryValue::String(value.into()),
            ));
        }
        let mut attributes = Vec::with_capacity(entry.structured_metadata.len());
        for (key, value) in entry.structured_metadata {
            fields.push(MetadataField::new(
                format!("{METADATA_PREFIX}{key}"),
                value.clone(),
            ));
            attributes.push(TelemetryAttribute::new(
                key,
                TelemetryValue::String(value.into()),
            ));
        }
        let cohort_bytes = cohort.finalize();
        let compression_cohort = CompressionCohortId::new(u64::from_le_bytes(
            cohort_bytes.as_bytes()[..8]
                .try_into()
                .expect("BLAKE3 output contains eight bytes"),
        ));
        let message: Arc<str> = entry.line.into();
        events.push(OtlpLogEvent {
            timestamp_unix_nanos,
            observed_timestamp_unix_nanos: 0,
            body: Some(TelemetryValue::String(Arc::clone(&message))),
            message,
            fields: Arc::new(fields),
            attributes: Arc::new(attributes),
            resource: Arc::new(ResourceContext {
                attributes: Arc::new(resource_attributes),
                ..ResourceContext::default()
            }),
            scope: Arc::new(ScopeContext::default()),
            compression_cohort,
            ..OtlpLogEvent::default()
        });
    }
    prepare_log_envelope(tenant, &events)
}

/// Builds one durable STEL trace envelope for a single routed partition.
pub fn prepare_trace_envelope(
    topic_partition: TopicPartition,
    events: Vec<OtlpSpanEvent>,
) -> TelemetryResult<TelemetryEnvelope> {
    if topic_partition.topic_id != TelemetrySignal::Traces.topic_id() {
        return Err(TelemetryError::InvalidOtlpPayload(
            "trace envelope uses the wrong topic".into(),
        ));
    }
    let tenant = events
        .first()
        .map(|event| Arc::<str>::from(event.tenant()))
        .ok_or(TelemetryError::InvalidOtlpPayload(
            "trace envelope must contain at least one span".into(),
        ))?;
    if events.iter().any(|event| event.tenant() != tenant.as_ref()) {
        return Err(TelemetryError::InvalidOtlpPayload(
            "trace partition batch crosses tenants".into(),
        ));
    }
    let records = events
        .into_iter()
        .enumerate()
        .map(|(ordinal, event)| {
            Ok(event.into_durable(
                ShardId::new(0),
                topic_partition,
                LogicalOffset::new(
                    u64::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?,
                ),
            ))
        })
        .collect::<TelemetryResult<Vec<_>>>()?;
    let payload = encode_trace_block(&records)?;
    TelemetryEnvelope::new(
        TelemetrySignal::Traces,
        tenant,
        u32::try_from(records.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        topic_partition.partition_id.get().to_le_bytes().as_slice(),
        Arc::<[u8]>::from(payload),
    )
}

/// Builds one durable STEL metric envelope for a single routed partition.
pub fn prepare_metric_envelope(
    topic_partition: TopicPartition,
    events: Vec<OtlpMetricEvent>,
) -> TelemetryResult<TelemetryEnvelope> {
    prepare_metric_envelope_with_protocol(topic_partition, events, MetricIngestProtocol::Otlp)
}

/// Builds one durable STEL metric envelope with explicit conflict semantics.
pub fn prepare_metric_envelope_with_protocol(
    topic_partition: TopicPartition,
    events: Vec<OtlpMetricEvent>,
    protocol: MetricIngestProtocol,
) -> TelemetryResult<TelemetryEnvelope> {
    if topic_partition.topic_id != TelemetrySignal::Metrics.topic_id() {
        return Err(TelemetryError::InvalidOtlpPayload(
            "metric envelope uses the wrong topic".into(),
        ));
    }
    let tenant = events
        .first()
        .map(|event| Arc::<str>::from(event.tenant()))
        .ok_or(TelemetryError::InvalidOtlpPayload(
            "metric envelope must contain at least one point".into(),
        ))?;
    if events.iter().any(|event| event.tenant() != tenant.as_ref()) {
        return Err(TelemetryError::InvalidOtlpPayload(
            "metric partition batch crosses tenants".into(),
        ));
    }
    let records = events
        .into_iter()
        .enumerate()
        .map(|(ordinal, event)| {
            Ok(event.into_durable(
                ShardId::new(0),
                topic_partition,
                LogicalOffset::new(
                    u64::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?,
                ),
            ))
        })
        .collect::<TelemetryResult<Vec<_>>>()?;
    let payload = encode_metric_chunk(&records)?;
    let mut routing_metadata = [0_u8; 5];
    routing_metadata[..4].copy_from_slice(&topic_partition.partition_id.get().to_le_bytes());
    routing_metadata[4] = protocol.to_wire();
    TelemetryEnvelope::new(
        TelemetrySignal::Metrics,
        tenant,
        u32::try_from(records.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        routing_metadata.as_slice(),
        Arc::<[u8]>::from(payload),
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    use crate::{OtlpTelemetryDecoder, TelemetryRouter, decode_trace_block};

    use super::*;

    #[test]
    fn trace_partition_envelope_is_self_verifying_and_counted() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 16],
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
        let decoder = OtlpTelemetryDecoder;
        let events = decoder
            .decode_traces("tenant-a", &request.encode_to_vec())
            .unwrap();
        let router = TelemetryRouter::new(NonZeroU16::new(256).unwrap());
        let mut partitioned = decoder.partition_traces(&router, events);
        let (partition, events) = partitioned.pop_first().unwrap();
        let envelope = prepare_trace_envelope(partition, events).unwrap();
        let encoded = envelope.encode().unwrap();
        let decoded = TelemetryEnvelope::decode(&encoded).unwrap();
        assert_eq!(decoded.item_count, 1);
        assert_eq!(decode_trace_block(&decoded.payload).unwrap().len(), 1);
    }
}
