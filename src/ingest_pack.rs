use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ops::Range;

use bytes::Bytes;
use shard_stream_core::LogicalOffset;

use crate::{
    CompressionCohortId, DecodedStructuralRecord, EmbeddedFrameIndex, OtlpLogEvent,
    StructuralLogMetadataRef, StructuralRecordView, TelemetryError, TelemetryResult,
    decode_embedded_frame_index, decode_structural_block, decode_structural_records,
    encode_indexed_structural_records, structural::decode_embedded_frame_index_section,
};

const INGEST_PACK_MAGIC: &[u8; 4] = b"SLW1";
const INGEST_PACK_HEADER_BYTES: usize = 12;
const INGEST_GROUP_HEADER_BYTES: usize = 40;
const TRANSIENT_PACK_MAGIC: &[u8; 4] = b"SLT1";
const TRANSIENT_PACK_HEADER_BYTES: usize = 8;
const TRANSIENT_GROUP_HEADER_BYTES: usize = 20;
const INGEST_PACK_ZSTD_LEVEL: i32 = 1;
const MAX_INGEST_STRUCTURAL_BYTES: usize = 64 * 1024 * 1024;

thread_local! {
    static INGEST_COMPRESSOR: RefCell<zstd::bulk::Compressor<'static>> =
        RefCell::new(zstd::bulk::Compressor::new(INGEST_PACK_ZSTD_LEVEL)
            .expect("ingest zstd level is valid"));
    static INGEST_DECOMPRESSOR: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new(zstd::bulk::Decompressor::new()
            .expect("ingest zstd decompressor initializes"));
}

struct IngestRecordView<'a> {
    ordinal: u32,
    event: &'a OtlpLogEvent,
}

impl StructuralRecordView for IngestRecordView<'_> {
    fn structural_offset(&self) -> LogicalOffset {
        LogicalOffset::new(u64::from(self.ordinal))
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.event.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.event.message
    }

    fn structural_field_count(&self) -> usize {
        self.event.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.event
            .fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }

    fn structural_log_metadata(&self) -> Option<StructuralLogMetadataRef<'_>> {
        let event = self.event;
        let present = event.observed_timestamp_unix_nanos != 0
            || event.body.is_some()
            || !event.attributes.is_empty()
            || event.resource.as_ref() != &crate::ResourceContext::default()
            || event.scope.as_ref() != &crate::ScopeContext::default()
            || event.severity_number != 0
            || !event.severity_text.is_empty()
            || event.dropped_attributes_count != 0
            || event.flags != 0
            || event.trace_id.is_some()
            || event.span_id.is_some()
            || !event.event_name.is_empty();
        present.then_some(StructuralLogMetadataRef {
            observed_timestamp_unix_nanos: event.observed_timestamp_unix_nanos,
            body: event.body.as_ref(),
            attributes: &event.attributes,
            resource: &event.resource,
            scope: &event.scope,
            severity_number: event.severity_number,
            severity_text: &event.severity_text,
            dropped_attributes_count: event.dropped_attributes_count,
            flags: event.flags,
            trace_id: event.trace_id,
            span_id: event.span_id,
            event_name: &event.event_name,
        })
    }
}

pub(crate) struct PreparedIngestPack {
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedIngestFrame {
    pub(crate) frame_id: u64,
    pub(crate) cohort: CompressionCohortId,
    pub(crate) record_count: u32,
    pub(crate) structural_bytes: usize,
    pub(crate) min_timestamp_unix_nanos: u64,
    pub(crate) max_timestamp_unix_nanos: u64,
    pub(crate) compressed: Bytes,
    pub(crate) index: EmbeddedFrameIndex,
}

#[cfg(test)]
pub(crate) fn encode_ingest_pack(events: &[OtlpLogEvent]) -> TelemetryResult<Vec<u8>> {
    prepare_ingest_pack(events).map(|prepared| prepared.payload)
}

pub(crate) fn prepare_ingest_pack(events: &[OtlpLogEvent]) -> TelemetryResult<PreparedIngestPack> {
    let record_count = u32::try_from(events.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    let mut cohorts = BTreeMap::<CompressionCohortId, Vec<IngestRecordView<'_>>>::new();
    for (ordinal, event) in events.iter().enumerate() {
        cohorts
            .entry(event.compression_cohort)
            .or_default()
            .push(IngestRecordView {
                ordinal: u32::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?,
                event,
            });
    }
    prepare_grouped_ingest_pack(record_count, cohorts)
}

fn prepare_grouped_ingest_pack<R: StructuralRecordView>(
    record_count: u32,
    cohorts: BTreeMap<CompressionCohortId, Vec<R>>,
) -> TelemetryResult<PreparedIngestPack> {
    let group_count = u16::try_from(cohorts.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(INGEST_PACK_MAGIC);
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(&group_count.to_le_bytes());
    encoded.extend_from_slice(&0_u16.to_le_bytes());
    for (cohort, records) in cohorts {
        let indexed = encode_indexed_structural_records(&records)?;
        let structural = indexed.structural;
        if structural.len() > MAX_INGEST_STRUCTURAL_BYTES {
            return Err(TelemetryError::RecordTooLarge);
        }
        let compressed = INGEST_COMPRESSOR.with_borrow_mut(|compressor| {
            compressor
                .compress(&structural)
                .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
        })?;
        encoded.extend_from_slice(&cohort.get().to_le_bytes());
        encoded.extend_from_slice(
            &u32::try_from(records.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(
            &u32::try_from(structural.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(
            &u32::try_from(compressed.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&payload_checksum(&compressed).to_le_bytes());
        let min_timestamp_unix_nanos = records
            .iter()
            .map(StructuralRecordView::structural_timestamp_unix_nanos)
            .min()
            .expect("ingest cohort groups are nonempty");
        let max_timestamp_unix_nanos = records
            .iter()
            .map(StructuralRecordView::structural_timestamp_unix_nanos)
            .max()
            .expect("ingest cohort groups are nonempty");
        encoded.extend_from_slice(&min_timestamp_unix_nanos.to_le_bytes());
        encoded.extend_from_slice(&max_timestamp_unix_nanos.to_le_bytes());
        encoded.extend_from_slice(&compressed);
    }
    if encoded.len() > crate::native_protocol::MAX_NATIVE_FRAME_BYTES {
        return Err(TelemetryError::RecordTooLarge);
    }
    Ok(PreparedIngestPack { payload: encoded })
}

pub(crate) fn validate_ingest_pack(
    payload: &[u8],
    expected_record_count: u32,
) -> TelemetryResult<()> {
    let mut cursor = IngestPackCursor::new(payload, expected_record_count)?;
    for _ in 0..cursor.group_count {
        let group = cursor.next_group()?;
        validate_group(&group)?;
    }
    cursor.finish()
}

pub(crate) fn decode_ingest_pack(payload: &[u8]) -> TelemetryResult<Vec<OtlpLogEvent>> {
    let expected_record_count = payload
        .get(4..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(TelemetryError::InvalidBlockEncoding(
            "compressed ingest pack header is truncated",
        ))?;
    let mut cursor = IngestPackCursor::new(payload, expected_record_count)?;
    let mut events = Vec::with_capacity(expected_record_count as usize);
    for _ in 0..cursor.group_count {
        let group = cursor.next_group()?;
        validate_group(&group)?;
        let structural = INGEST_DECOMPRESSOR.with_borrow_mut(|decompressor| {
            decompressor
                .decompress(group.compressed, group.structural_bytes)
                .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
        })?;
        if structural.len() != group.structural_bytes {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest structural length mismatch",
            ));
        }
        let records = decode_structural_block(&structural)?;
        if records.len() != group.record_count {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest group record count mismatch",
            ));
        }
        for record in records {
            let ordinal =
                u32::try_from(record.offset.get()).map_err(|_| TelemetryError::RecordTooLarge)?;
            if ordinal >= expected_record_count {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "compressed ingest ordinal is out of range",
                ));
            }
            events.push((
                ordinal,
                OtlpLogEvent {
                    timestamp_unix_nanos: record.timestamp_unix_nanos,
                    observed_timestamp_unix_nanos: record.observed_timestamp_unix_nanos,
                    body: record.body,
                    message: record.message,
                    fields: record.fields,
                    attributes: record.attributes,
                    resource: record.resource,
                    scope: record.scope,
                    severity_number: record.severity_number,
                    severity_text: record.severity_text,
                    dropped_attributes_count: record.dropped_attributes_count,
                    flags: record.flags,
                    trace_id: record.trace_id,
                    span_id: record.span_id,
                    event_name: record.event_name,
                    compression_cohort: group.cohort,
                },
            ));
        }
    }
    cursor.finish()?;
    events.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if events.len() != expected_record_count as usize
        || events
            .iter()
            .enumerate()
            .any(|(expected, (ordinal, _))| *ordinal as usize != expected)
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "compressed ingest ordinals are not contiguous",
        ));
    }
    Ok(events.into_iter().map(|(_, event)| event).collect())
}

pub(crate) fn decode_indexed_ingest_frames(
    payload: Bytes,
    transient_context: Option<&[u8]>,
    expected_record_count: u32,
) -> TelemetryResult<Vec<IndexedIngestFrame>> {
    let mut cursor = IngestPackCursor::new(&payload, expected_record_count)?;
    let mut transient = transient_context
        .filter(|context| context.starts_with(TRANSIENT_PACK_MAGIC))
        .map(|context| TransientPackCursor::new(context, cursor.group_count))
        .transpose()?;
    let mut frames = Vec::with_capacity(cursor.group_count);
    for _ in 0..cursor.group_count {
        let group = cursor.next_group()?;
        validate_group(&group)?;
        let index = if let Some(transient) = transient.as_mut() {
            let live = transient.next_group()?;
            if live.cohort != group.cohort
                || live.record_count != group.record_count
                || live.structural_bytes != group.structural_bytes
            {
                return Err(TelemetryError::InvalidBlockEncoding(
                    "transient ingest group disagrees with durable group",
                ));
            }
            decode_embedded_frame_index_section(live.embedded_index, group.record_count)?
        } else {
            let structural = decompress_group(&group)?;
            decode_embedded_frame_index(&structural)?
        };
        if index.record_count() as usize != group.record_count {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest frame index record count mismatch",
            ));
        }
        frames.push(IndexedIngestFrame {
            frame_id: 0,
            cohort: group.cohort,
            record_count: index.record_count(),
            structural_bytes: group.structural_bytes,
            min_timestamp_unix_nanos: group.min_timestamp_unix_nanos,
            max_timestamp_unix_nanos: group.max_timestamp_unix_nanos,
            compressed: payload.slice(group.compressed_range),
            index,
        });
    }
    cursor.finish()?;
    if let Some(transient) = transient {
        transient.finish()?;
    }
    Ok(frames)
}

pub(crate) fn decode_indexed_ingest_records(
    frame: &IndexedIngestFrame,
    record_ordinals: &[u32],
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    let structural = INGEST_DECOMPRESSOR.with_borrow_mut(|decompressor| {
        decompressor
            .decompress(&frame.compressed, frame.structural_bytes)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    })?;
    if structural.len() != frame.structural_bytes {
        return Err(TelemetryError::InvalidBlockEncoding(
            "compressed ingest structural length mismatch",
        ));
    }
    decode_structural_records(&structural, record_ordinals)
}

struct IngestGroup<'a> {
    cohort: CompressionCohortId,
    record_count: usize,
    structural_bytes: usize,
    checksum: u32,
    min_timestamp_unix_nanos: u64,
    max_timestamp_unix_nanos: u64,
    compressed: &'a [u8],
    compressed_range: Range<usize>,
}

struct IngestPackCursor<'a> {
    payload: &'a [u8],
    cursor: usize,
    group_count: usize,
    expected_record_count: u32,
    observed_record_count: u64,
}

impl<'a> IngestPackCursor<'a> {
    fn new(payload: &'a [u8], expected_record_count: u32) -> TelemetryResult<Self> {
        if payload.len() < INGEST_PACK_HEADER_BYTES
            || payload.get(..4) != Some(INGEST_PACK_MAGIC)
            || payload[10..12] != [0; 2]
        {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid compressed ingest pack header",
            ));
        }
        let declared_record_count =
            u32::from_le_bytes(payload[4..8].try_into().expect("fixed range"));
        if declared_record_count != expected_record_count {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest record count mismatch",
            ));
        }
        Ok(Self {
            payload,
            cursor: INGEST_PACK_HEADER_BYTES,
            group_count: usize::from(u16::from_le_bytes(
                payload[8..10].try_into().expect("fixed range"),
            )),
            expected_record_count,
            observed_record_count: 0,
        })
    }

    fn next_group(&mut self) -> TelemetryResult<IngestGroup<'a>> {
        let header_end = self
            .cursor
            .checked_add(INGEST_GROUP_HEADER_BYTES)
            .filter(|end| *end <= self.payload.len())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "compressed ingest group header is truncated",
            ))?;
        let header = &self.payload[self.cursor..header_end];
        let record_count = u32::from_le_bytes(header[8..12].try_into().expect("fixed range"));
        let structural_bytes =
            u32::from_le_bytes(header[12..16].try_into().expect("fixed range")) as usize;
        let compressed_bytes =
            u32::from_le_bytes(header[16..20].try_into().expect("fixed range")) as usize;
        let payload_end = header_end
            .checked_add(compressed_bytes)
            .filter(|end| *end <= self.payload.len())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "compressed ingest group payload is truncated",
            ))?;
        self.cursor = payload_end;
        self.observed_record_count = self
            .observed_record_count
            .checked_add(u64::from(record_count))
            .ok_or(TelemetryError::RecordTooLarge)?;
        Ok(IngestGroup {
            cohort: CompressionCohortId::new(u64::from_le_bytes(
                header[0..8].try_into().expect("fixed range"),
            )),
            record_count: record_count as usize,
            structural_bytes,
            checksum: u32::from_le_bytes(header[20..24].try_into().expect("fixed range")),
            min_timestamp_unix_nanos: u64::from_le_bytes(
                header[24..32].try_into().expect("fixed range"),
            ),
            max_timestamp_unix_nanos: u64::from_le_bytes(
                header[32..40].try_into().expect("fixed range"),
            ),
            compressed: &self.payload[header_end..payload_end],
            compressed_range: header_end..payload_end,
        })
    }

    fn finish(&self) -> TelemetryResult<()> {
        if self.cursor != self.payload.len()
            || self.observed_record_count != u64::from(self.expected_record_count)
            || (self.expected_record_count > 0 && self.group_count == 0)
        {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest pack totals are invalid",
            ));
        }
        Ok(())
    }
}

fn validate_group(group: &IngestGroup<'_>) -> TelemetryResult<()> {
    if group.record_count == 0
        || group.structural_bytes == 0
        || group.structural_bytes > MAX_INGEST_STRUCTURAL_BYTES
        || group.min_timestamp_unix_nanos > group.max_timestamp_unix_nanos
        || payload_checksum(group.compressed) != group.checksum
    {
        return Err(TelemetryError::InvalidBlockEncoding(
            "invalid compressed ingest group",
        ));
    }
    Ok(())
}

fn decompress_group(group: &IngestGroup<'_>) -> TelemetryResult<Vec<u8>> {
    let structural = INGEST_DECOMPRESSOR.with_borrow_mut(|decompressor| {
        decompressor
            .decompress(group.compressed, group.structural_bytes)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    })?;
    if structural.len() != group.structural_bytes {
        return Err(TelemetryError::InvalidBlockEncoding(
            "compressed ingest structural length mismatch",
        ));
    }
    Ok(structural)
}

struct TransientGroup<'a> {
    cohort: CompressionCohortId,
    record_count: usize,
    structural_bytes: usize,
    embedded_index: &'a [u8],
}

struct TransientPackCursor<'a> {
    payload: &'a [u8],
    cursor: usize,
    group_count: usize,
    observed_groups: usize,
}

impl<'a> TransientPackCursor<'a> {
    fn new(payload: &'a [u8], expected_group_count: usize) -> TelemetryResult<Self> {
        if payload.len() < TRANSIENT_PACK_HEADER_BYTES
            || payload.get(..4) != Some(TRANSIENT_PACK_MAGIC)
            || payload[6..8] != [0; 2]
        {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid transient ingest pack header",
            ));
        }
        let group_count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if group_count != expected_group_count {
            return Err(TelemetryError::InvalidBlockEncoding(
                "transient ingest group count mismatch",
            ));
        }
        Ok(Self {
            payload,
            cursor: TRANSIENT_PACK_HEADER_BYTES,
            group_count,
            observed_groups: 0,
        })
    }

    fn next_group(&mut self) -> TelemetryResult<TransientGroup<'a>> {
        let header_end = self
            .cursor
            .checked_add(TRANSIENT_GROUP_HEADER_BYTES)
            .filter(|end| *end <= self.payload.len())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "transient ingest group header is truncated",
            ))?;
        let header = &self.payload[self.cursor..header_end];
        let structural_bytes =
            u32::from_le_bytes(header[12..16].try_into().expect("fixed range")) as usize;
        let embedded_index_bytes =
            u32::from_le_bytes(header[16..20].try_into().expect("fixed range")) as usize;
        let payload_end = header_end
            .checked_add(embedded_index_bytes)
            .filter(|end| *end <= self.payload.len())
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "transient ingest index payload is truncated",
            ))?;
        self.cursor = payload_end;
        self.observed_groups += 1;
        Ok(TransientGroup {
            cohort: CompressionCohortId::new(u64::from_le_bytes(
                header[0..8].try_into().expect("fixed range"),
            )),
            record_count: u32::from_le_bytes(header[8..12].try_into().expect("fixed range"))
                as usize,
            structural_bytes,
            embedded_index: &self.payload[header_end..payload_end],
        })
    }

    fn finish(&self) -> TelemetryResult<()> {
        if self.cursor != self.payload.len() || self.observed_groups != self.group_count {
            return Err(TelemetryError::InvalidBlockEncoding(
                "transient ingest pack totals are invalid",
            ));
        }
        Ok(())
    }
}

fn payload_checksum(payload: &[u8]) -> u32 {
    u32::from_le_bytes(
        blake3::hash(payload).as_bytes()[..4]
            .try_into()
            .expect("fixed checksum"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::MetadataField;

    #[test]
    fn compressed_ingest_pack_preserves_order_cohorts_and_exact_records() {
        let events = (0..32)
            .map(|ordinal| OtlpLogEvent {
                timestamp_unix_nanos: 100 + ordinal,
                message: Arc::from(format!("request id={ordinal} completed")),
                fields: Arc::new(vec![MetadataField::new(
                    "service",
                    if ordinal % 2 == 0 { "api" } else { "worker" },
                )]),
                compression_cohort: CompressionCohortId::new(ordinal % 3),
                ..OtlpLogEvent::default()
            })
            .collect::<Vec<_>>();
        let encoded = encode_ingest_pack(&events).expect("pack encodes");
        validate_ingest_pack(&encoded, events.len() as u32).expect("pack validates");
        let decoded = decode_ingest_pack(&encoded).expect("pack decodes");
        assert_eq!(decoded, events);
        assert!(encoded.len() < 16 * 1024);
    }

    #[test]
    fn compressor_index_is_reused_live_and_recovered_from_durable_frames() {
        let events = (0..64)
            .map(|ordinal| OtlpLogEvent {
                timestamp_unix_nanos: 10_000 + ordinal,
                message: Arc::from(format!("request id={ordinal} completed successfully")),
                fields: Arc::new(vec![
                    MetadataField::new("service", if ordinal % 2 == 0 { "api" } else { "worker" }),
                    MetadataField::new("trace", format!("trace-{ordinal}")),
                ]),
                compression_cohort: CompressionCohortId::new(ordinal % 3),
                ..OtlpLogEvent::default()
            })
            .collect::<Vec<_>>();
        let prepared = prepare_ingest_pack(&events).expect("indexed pack prepares");
        let live = decode_indexed_ingest_frames(
            Bytes::from(prepared.payload.clone()),
            None,
            events.len() as u32,
        )
        .expect("live indexes install");
        let recovered =
            decode_indexed_ingest_frames(Bytes::from(prepared.payload), None, events.len() as u32)
                .expect("durable indexes recover");
        assert_eq!(live.len(), 3);
        assert_eq!(live.len(), recovered.len());
        for (live, recovered) in live.iter().zip(&recovered) {
            assert_eq!(live.index, recovered.index);
            let candidates = live.index.term_candidate_ordinals("completed");
            let decoded = decode_indexed_ingest_records(live, &candidates)
                .expect("static term candidates selectively decode");
            assert_eq!(decoded.len(), live.record_count as usize);
            let api_candidates = live.index.field_candidate_ordinals("service", "api");
            let api = decode_indexed_ingest_records(live, &api_candidates)
                .expect("field candidates selectively decode");
            assert!(api.iter().all(|record| record.fields.iter().any(|field| {
                field.key.as_ref() == "service" && field.value.as_ref() == "api"
            })));
        }
    }

    #[test]
    fn compressed_ingest_pack_rejects_corruption_and_wrong_counts() {
        let events = vec![OtlpLogEvent {
            timestamp_unix_nanos: 1,
            message: Arc::from("hello"),
            fields: Arc::new(Vec::new()),
            compression_cohort: CompressionCohortId::new(7),
            ..OtlpLogEvent::default()
        }];
        let mut encoded = encode_ingest_pack(&events).expect("pack encodes");
        assert!(validate_ingest_pack(&encoded, 2).is_err());
        let last = encoded.len() - 1;
        encoded[last] ^= 1;
        assert!(validate_ingest_pack(&encoded, 1).is_err());
        assert!(decode_ingest_pack(&encoded).is_err());
    }
}
