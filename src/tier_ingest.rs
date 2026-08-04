use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

use bytes::Bytes;
use shard_stream_core::LogicalOffset;

use crate::structural::decode_embedded_frame_index_section;
use crate::{
    CompressionCohortId, EmbeddedFrameIndex, TelemetryError, TelemetryResult, TierBlockEntry,
};

const QUERY_INDEX_MAGIC: &[u8; 8] = b"SLTQIX1\0";
const QUERY_INDEX_HEADER_BYTES: usize = QUERY_INDEX_MAGIC.len() + 8;
const QUERY_INDEX_LEVEL: i32 = 1;
const MAX_QUERY_INDEX_BYTES: usize = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct TierIngestFrameSource {
    pub(crate) frame_id: u64,
    pub(crate) cohort: CompressionCohortId,
    pub(crate) record_count: u32,
    pub(crate) structural_bytes: usize,
    pub(crate) min_timestamp_unix_nanos: u64,
    pub(crate) max_timestamp_unix_nanos: u64,
    pub(crate) compressed: Bytes,
    pub(crate) index: EmbeddedFrameIndex,
}

#[derive(Debug, Clone)]
pub(crate) struct TierIngestAppendSource {
    pub(crate) first_offset: LogicalOffset,
    pub(crate) last_offset: LogicalOffset,
    pub(crate) record_count: u32,
    pub(crate) frames: Vec<TierIngestFrameSource>,
}

#[derive(Debug)]
pub(crate) struct DecodedTierIngestFrame {
    pub(crate) frame_id: u64,
    pub(crate) cohort: CompressionCohortId,
    pub(crate) record_count: u32,
    pub(crate) structural_bytes: usize,
    pub(crate) min_timestamp_unix_nanos: u64,
    pub(crate) max_timestamp_unix_nanos: u64,
    pub(crate) payload_offset: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) payload_checksum: String,
    pub(crate) index: EmbeddedFrameIndex,
}

#[derive(Debug)]
pub(crate) struct DecodedTierIngestAppend {
    pub(crate) first_offset: LogicalOffset,
    pub(crate) last_offset: LogicalOffset,
    pub(crate) record_count: u32,
    pub(crate) frames: Vec<DecodedTierIngestFrame>,
}

pub(crate) fn write_tier_ingest_group(
    appends: &[TierIngestAppendSource],
    payload_path: &Path,
    query_index_path: &Path,
) -> TelemetryResult<Vec<TierBlockEntry>> {
    if appends.is_empty() || appends.iter().any(|append| append.frames.is_empty()) {
        return Err(TelemetryError::ObjectStore(
            "an ingest tier group requires nonempty appends and frames".into(),
        ));
    }
    create_parent(payload_path)?;
    create_parent(query_index_path)?;
    let mut payload = open_truncated(payload_path, "create ingest payload pack")?;
    let mut raw_index = Vec::new();
    append_u32(
        &mut raw_index,
        u32::try_from(appends.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
    );
    let mut blocks = Vec::new();
    let mut payload_offset = 0u64;
    let mut previous_frame_id = None;
    for append in appends {
        if append.first_offset > append.last_offset || append.record_count == 0 {
            return Err(TelemetryError::CorruptTier(
                "ingest tier append bounds are invalid".into(),
            ));
        }
        append_u64(&mut raw_index, append.first_offset.get());
        append_u64(&mut raw_index, append.last_offset.get());
        append_u32(&mut raw_index, append.record_count);
        append_u32(
            &mut raw_index,
            u32::try_from(append.frames.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        );
        for frame in &append.frames {
            if previous_frame_id.is_some_and(|previous| previous >= frame.frame_id)
                || frame.record_count == 0
                || frame.compressed.is_empty()
                || frame.min_timestamp_unix_nanos > frame.max_timestamp_unix_nanos
            {
                return Err(TelemetryError::CorruptTier(
                    "ingest tier frame metadata is invalid".into(),
                ));
            }
            previous_frame_id = Some(frame.frame_id);
            let payload_bytes = u64::try_from(frame.compressed.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?;
            let payload_checksum = checksum_bytes(&frame.compressed);
            payload
                .write_all(&frame.compressed)
                .map_err(|error| storage_io("write ingest payload pack", error))?;
            let index = frame.index.encoded_bytes()?;
            append_u64(&mut raw_index, frame.frame_id);
            append_u64(&mut raw_index, frame.cohort.get());
            append_u32(&mut raw_index, frame.record_count);
            append_u64(
                &mut raw_index,
                u64::try_from(frame.structural_bytes)
                    .map_err(|_| TelemetryError::RecordTooLarge)?,
            );
            append_u64(&mut raw_index, frame.min_timestamp_unix_nanos);
            append_u64(&mut raw_index, frame.max_timestamp_unix_nanos);
            append_u64(&mut raw_index, payload_offset);
            append_u64(&mut raw_index, payload_bytes);
            append_bytes(&mut raw_index, &index)?;
            blocks.push(TierBlockEntry {
                block_id: frame.frame_id,
                source_compression_cohort: frame.cohort.get(),
                placement_id: frame.cohort.get(),
                dictionary_id: None,
                compression_codec: "zstd".into(),
                compression_level: 1,
                first_offset: append.first_offset.get(),
                last_offset: append.last_offset.get(),
                record_count: frame.record_count,
                source_bytes: u64::try_from(frame.structural_bytes)
                    .map_err(|_| TelemetryError::RecordTooLarge)?,
                structural_bytes: u64::try_from(frame.structural_bytes)
                    .map_err(|_| TelemetryError::RecordTooLarge)?,
                stored_bytes: payload_bytes,
                min_timestamp_unix_nanos: frame.min_timestamp_unix_nanos,
                max_timestamp_unix_nanos: frame.max_timestamp_unix_nanos,
                compression_temperature: 0,
                compression_shape_hash: frame.cohort.get(),
                compression_temperature_variance_q8: 0,
                max_compression_temperature_deviation: 0,
                payload_offset,
                payload_bytes,
                payload_checksum,
            });
            payload_offset = payload_offset
                .checked_add(payload_bytes)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
    }
    if raw_index.len() > MAX_QUERY_INDEX_BYTES {
        return Err(TelemetryError::RecordTooLarge);
    }
    payload
        .sync_all()
        .map_err(|error| storage_io("sync ingest payload pack", error))?;
    let compressed_index = zstd::bulk::compress(&raw_index, QUERY_INDEX_LEVEL)
        .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
    let mut index_file = open_truncated(query_index_path, "create ingest query index")?;
    index_file
        .write_all(QUERY_INDEX_MAGIC)
        .and_then(|()| index_file.write_all(&(raw_index.len() as u64).to_le_bytes()))
        .and_then(|()| index_file.write_all(&compressed_index))
        .and_then(|()| index_file.sync_all())
        .map_err(|error| storage_io("write ingest query index", error))?;
    sync_parent(payload_path)?;
    if payload_path.parent() != query_index_path.parent() {
        sync_parent(query_index_path)?;
    }
    Ok(blocks)
}

pub(crate) fn decode_tier_ingest_group(
    encoded: &[u8],
    blocks: &[TierBlockEntry],
) -> TelemetryResult<Vec<DecodedTierIngestAppend>> {
    if encoded.len() < QUERY_INDEX_HEADER_BYTES
        || encoded.get(..QUERY_INDEX_MAGIC.len()) != Some(QUERY_INDEX_MAGIC)
    {
        return Err(TelemetryError::CorruptTier(
            "ingest query-index header is invalid".into(),
        ));
    }
    let uncompressed_bytes = usize::try_from(u64::from_le_bytes(
        encoded[QUERY_INDEX_MAGIC.len()..QUERY_INDEX_HEADER_BYTES]
            .try_into()
            .expect("fixed query-index length"),
    ))
    .map_err(|_| TelemetryError::RecordTooLarge)?;
    if uncompressed_bytes == 0 || uncompressed_bytes > MAX_QUERY_INDEX_BYTES {
        return Err(TelemetryError::CorruptTier(
            "ingest query-index decoded length is invalid".into(),
        ));
    }
    let raw = zstd::bulk::decompress(&encoded[QUERY_INDEX_HEADER_BYTES..], uncompressed_bytes)
        .map_err(|error| {
            TelemetryError::CorruptTier(format!("decompress ingest query index: {error}"))
        })?;
    if raw.len() != uncompressed_bytes {
        return Err(TelemetryError::CorruptTier(
            "ingest query-index decoded length changed".into(),
        ));
    }
    let mut cursor = 0usize;
    let append_count = read_u32(&raw, &mut cursor)? as usize;
    ensure_count(append_count, raw.len().saturating_sub(cursor))?;
    let mut appends = Vec::with_capacity(append_count);
    let mut block_index = 0usize;
    for _ in 0..append_count {
        let first_offset = LogicalOffset::new(read_u64(&raw, &mut cursor)?);
        let last_offset = LogicalOffset::new(read_u64(&raw, &mut cursor)?);
        let record_count = read_u32(&raw, &mut cursor)?;
        let frame_count = read_u32(&raw, &mut cursor)? as usize;
        if first_offset > last_offset || record_count == 0 {
            return Err(TelemetryError::CorruptTier(
                "ingest query-index append metadata is invalid".into(),
            ));
        }
        ensure_count(frame_count, blocks.len().saturating_sub(block_index))?;
        let mut frames = Vec::with_capacity(frame_count);
        for _ in 0..frame_count {
            let frame_id = read_u64(&raw, &mut cursor)?;
            let cohort = CompressionCohortId::new(read_u64(&raw, &mut cursor)?);
            let frame_record_count = read_u32(&raw, &mut cursor)?;
            let structural_bytes = usize::try_from(read_u64(&raw, &mut cursor)?)
                .map_err(|_| TelemetryError::RecordTooLarge)?;
            let min_timestamp_unix_nanos = read_u64(&raw, &mut cursor)?;
            let max_timestamp_unix_nanos = read_u64(&raw, &mut cursor)?;
            let payload_offset = read_u64(&raw, &mut cursor)?;
            let payload_bytes = read_u64(&raw, &mut cursor)?;
            let index_bytes = read_bytes(&raw, &mut cursor)?;
            let index = decode_embedded_frame_index_section(
                index_bytes,
                usize::try_from(frame_record_count).map_err(|_| TelemetryError::RecordTooLarge)?,
            )?;
            let block = blocks.get(block_index).ok_or_else(|| {
                TelemetryError::CorruptTier(
                    "ingest query index has more frames than manifest".into(),
                )
            })?;
            if block.block_id != frame_id
                || block.source_compression_cohort != cohort.get()
                || block.first_offset != first_offset.get()
                || block.last_offset != last_offset.get()
                || block.record_count != frame_record_count
                || block.structural_bytes != structural_bytes as u64
                || block.min_timestamp_unix_nanos != min_timestamp_unix_nanos
                || block.max_timestamp_unix_nanos != max_timestamp_unix_nanos
                || block.payload_offset != payload_offset
                || block.payload_bytes != payload_bytes
            {
                return Err(TelemetryError::CorruptTier(
                    "ingest query index disagrees with group manifest".into(),
                ));
            }
            frames.push(DecodedTierIngestFrame {
                frame_id,
                cohort,
                record_count: frame_record_count,
                structural_bytes,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                payload_offset,
                payload_bytes,
                payload_checksum: block.payload_checksum.clone(),
                index,
            });
            block_index += 1;
        }
        if frames.is_empty() {
            return Err(TelemetryError::CorruptTier(
                "ingest query-index append has no frames".into(),
            ));
        }
        appends.push(DecodedTierIngestAppend {
            first_offset,
            last_offset,
            record_count,
            frames,
        });
    }
    if cursor != raw.len() || block_index != blocks.len() {
        return Err(TelemetryError::CorruptTier(
            "ingest query index has trailing or missing frames".into(),
        ));
    }
    Ok(appends)
}

fn create_parent(path: &Path) -> TelemetryResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create ingest tier spool", error))?;
    }
    Ok(())
}

fn open_truncated(path: &Path, operation: &str) -> TelemetryResult<File> {
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|error| storage_io(operation, error))
}

fn sync_parent(path: &Path) -> TelemetryResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_io("sync ingest tier spool directory", error))
}

fn append_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn append_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn append_bytes(output: &mut Vec<u8>, value: &[u8]) -> TelemetryResult<()> {
    append_u32(
        output,
        u32::try_from(value.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
    );
    output.extend_from_slice(value);
    Ok(())
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> TelemetryResult<u32> {
    Ok(u32::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> TelemetryResult<u64> {
    Ok(u64::from_le_bytes(read_array(bytes, cursor)?))
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> TelemetryResult<&'a [u8]> {
    let length = read_u32(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(length)
        .ok_or(TelemetryError::RecordTooLarge)?;
    let value = bytes.get(*cursor..end).ok_or_else(|| {
        TelemetryError::CorruptTier("ingest query-index field is truncated".into())
    })?;
    *cursor = end;
    Ok(value)
}

fn read_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> TelemetryResult<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or(TelemetryError::RecordTooLarge)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| TelemetryError::CorruptTier("ingest query index is truncated".into()))?
        .try_into()
        .expect("fixed read length");
    *cursor = end;
    Ok(value)
}

fn ensure_count(count: usize, remaining: usize) -> TelemetryResult<()> {
    if count <= remaining {
        Ok(())
    } else {
        Err(TelemetryError::CorruptTier(
            "ingest query-index count exceeds remaining bytes".into(),
        ))
    }
}

fn checksum_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn storage_io(operation: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::StorageIo(format!("{operation}: {error}"))
}
