use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use crate::{CompressionCohortId, LogDbError, LogDbResult, LokiEntry, MetadataField, OtlpLogEvent};

/// Fixed number of bytes in every native protocol frame header.
pub const NATIVE_FRAME_HEADER_BYTES: usize = 32;
/// Production maximum for one native request or response payload.
pub const MAX_NATIVE_FRAME_BYTES: usize = 16 * 1024 * 1024;

const FRAME_MAGIC: [u8; 4] = *b"SLNP";
const FRAME_VERSION: u8 = 1;
const FRAME_FLAG_RESPONSE: u8 = 1;
const BATCH_MAGIC: [u8; 4] = *b"SLB1";
const QUERY_MAGIC: [u8; 4] = *b"SLQ1";
const APPEND_ACK_MAGIC: [u8; 4] = *b"SLA1";
const BATCH_HEADER_BYTES: usize = 16;
const QUERY_HEADER_BYTES: usize = 32;
const MAX_TENANT_BYTES: usize = 1_024;
const MAX_STREAMS: usize = 65_535;
const MAX_LABELS_PER_STREAM: usize = 256;
const MAX_METADATA_PER_ENTRY: usize = 256;
const MAX_QUERY_TERMS: usize = 256;
const MAX_QUERY_LIMIT: u32 = 1_000_000;
const LINEAR_NATIVE_KEY_LIMIT: usize = 16;
const LABEL_PREFIX: &str = "resource.loki.label.";
const METADATA_PREFIX: &str = "attr.loki.metadata.";
const TENANT_FIELD: &str = "resource.loki.tenant";

/// Native operation carried by a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NativeOpcode {
    /// Append one grouped log batch.
    Append = 1,
    /// Execute an indexed exact-label and term query.
    Query = 2,
    /// Verify the connection and echo the request payload.
    Ping = 3,
    /// Authenticates a connection before any tenant operation is accepted.
    Authenticate = 4,
}

impl NativeOpcode {
    fn from_byte(value: u8) -> Result<Self, NativeProtocolError> {
        match value {
            1 => Ok(Self::Append),
            2 => Ok(Self::Query),
            3 => Ok(Self::Ping),
            4 => Ok(Self::Authenticate),
            _ => Err(NativeProtocolError::new(format!(
                "unsupported native opcode {value}"
            ))),
        }
    }
}

/// Status returned in a native response frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NativeStatus {
    /// The operation completed successfully.
    Ok = 0,
    /// The request frame or payload was invalid.
    BadRequest = 1,
    /// The operation failed after validation.
    Internal = 2,
    /// The requested version or operation is unsupported.
    Unsupported = 3,
    /// The connection did not supply the configured production credential.
    Unauthorized = 4,
    /// The service is draining or temporarily unavailable.
    Unavailable = 5,
    /// A bounded admission or rate limit rejected the request.
    TooManyRequests = 6,
    /// Query execution exceeded the configured response deadline.
    Timeout = 7,
}

impl NativeStatus {
    fn from_byte(value: u8) -> Result<Self, NativeProtocolError> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::BadRequest),
            2 => Ok(Self::Internal),
            3 => Ok(Self::Unsupported),
            4 => Ok(Self::Unauthorized),
            5 => Ok(Self::Unavailable),
            6 => Ok(Self::TooManyRequests),
            7 => Ok(Self::Timeout),
            _ => Err(NativeProtocolError::new(format!(
                "unsupported native status {value}"
            ))),
        }
    }
}

/// Validated fixed header for one multiplexed native frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeFrameHeader {
    /// Operation associated with the frame.
    pub opcode: NativeOpcode,
    /// Caller-selected ID copied into the response.
    pub request_id: u128,
    /// Response status; requests must use [`NativeStatus::Ok`].
    pub status: NativeStatus,
    /// Whether this frame is a server response.
    pub is_response: bool,
    /// Number of payload bytes following the header.
    pub payload_len: u32,
    payload_checksum: u32,
}

impl NativeFrameHeader {
    /// Creates a request header for `payload`.
    pub fn request(
        opcode: NativeOpcode,
        request_id: u128,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        Self::new(opcode, request_id, NativeStatus::Ok, false, payload)
    }

    /// Creates a response header for `payload`.
    pub fn response(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        Self::new(opcode, request_id, status, true, payload)
    }

    fn new(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        is_response: bool,
        payload: &[u8],
    ) -> Result<Self, NativeProtocolError> {
        if payload.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(format!(
                "native frame payload is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
                payload.len()
            )));
        }
        Ok(Self {
            opcode,
            request_id,
            status,
            is_response,
            payload_len: u32::try_from(payload.len())
                .map_err(|_| NativeProtocolError::new("native frame payload exceeds u32"))?,
            payload_checksum: payload_checksum(payload),
        })
    }

    /// Encodes this header into its fixed-width wire representation.
    #[must_use]
    pub fn encode(self) -> [u8; NATIVE_FRAME_HEADER_BYTES] {
        let mut bytes = [0; NATIVE_FRAME_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&FRAME_MAGIC);
        bytes[4] = FRAME_VERSION;
        bytes[5] = self.opcode as u8;
        bytes[6] = u8::from(self.is_response) * FRAME_FLAG_RESPONSE;
        bytes[7] = self.status as u8;
        bytes[8..24].copy_from_slice(&self.request_id.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.payload_checksum.to_le_bytes());
        bytes
    }

    /// Decodes and validates a fixed-width wire header.
    pub fn decode(bytes: &[u8; NATIVE_FRAME_HEADER_BYTES]) -> Result<Self, NativeProtocolError> {
        if bytes[0..4] != FRAME_MAGIC {
            return Err(NativeProtocolError::new("invalid native frame magic"));
        }
        if bytes[4] != FRAME_VERSION {
            return Err(NativeProtocolError::new(format!(
                "unsupported native frame version {}",
                bytes[4]
            )));
        }
        if bytes[6] & !FRAME_FLAG_RESPONSE != 0 {
            return Err(NativeProtocolError::new(
                "native frame contains unknown flags",
            ));
        }
        let payload_len = u32::from_le_bytes(bytes[24..28].try_into().expect("fixed range"));
        if payload_len as usize > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(format!(
                "native frame payload is {payload_len} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}"
            )));
        }
        Ok(Self {
            opcode: NativeOpcode::from_byte(bytes[5])?,
            request_id: u128::from_le_bytes(bytes[8..24].try_into().expect("fixed range")),
            status: NativeStatus::from_byte(bytes[7])?,
            is_response: bytes[6] == FRAME_FLAG_RESPONSE,
            payload_len,
            payload_checksum: u32::from_le_bytes(bytes[28..32].try_into().expect("fixed range")),
        })
    }

    /// Verifies that `payload` has the declared length and BLAKE3 checksum.
    pub fn verify_payload(self, payload: &[u8]) -> Result<(), NativeProtocolError> {
        if payload.len() != self.payload_len as usize {
            return Err(NativeProtocolError::new(
                "native frame payload length disagrees with its header",
            ));
        }
        if payload_checksum(payload) != self.payload_checksum {
            return Err(NativeProtocolError::new(
                "native frame payload checksum mismatch",
            ));
        }
        Ok(())
    }
}

/// One complete native wire frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFrame {
    /// Validated frame header.
    pub header: NativeFrameHeader,
    /// Operation-specific payload.
    pub payload: Vec<u8>,
}

impl NativeFrame {
    /// Creates a request frame.
    pub fn request(
        opcode: NativeOpcode,
        request_id: u128,
        payload: Vec<u8>,
    ) -> Result<Self, NativeProtocolError> {
        Ok(Self {
            header: NativeFrameHeader::request(opcode, request_id, &payload)?,
            payload,
        })
    }

    /// Creates a response frame.
    pub fn response(
        opcode: NativeOpcode,
        request_id: u128,
        status: NativeStatus,
        payload: Vec<u8>,
    ) -> Result<Self, NativeProtocolError> {
        Ok(Self {
            header: NativeFrameHeader::response(opcode, request_id, status, &payload)?,
            payload,
        })
    }

    /// Encodes the complete frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(NATIVE_FRAME_HEADER_BYTES + self.payload.len());
        encoded.extend_from_slice(&self.header.encode());
        encoded.extend_from_slice(&self.payload);
        encoded
    }
}

/// Decoded stream-grouped native append or query-result batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeLogBatch {
    /// Tenant whose records are carried by the batch.
    pub tenant: String,
    /// Flattened records; stream labels are repeated only after decoding.
    pub entries: Vec<LokiEntry>,
}

/// Durable coordinates returned after a native append.
///
/// The native server configuration determines whether the response waits for
/// exact-query visibility or only the authoritative compressed WAL commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAppendAck {
    /// Logical tenant partition selected for the batch.
    pub partition_id: u32,
    /// First durable offset reserved for the batch.
    pub first_offset: u64,
    /// Last durable offset reserved for the batch.
    pub last_offset: u64,
}

impl NativeAppendAck {
    /// Encodes a fixed 24-byte append acknowledgement payload.
    #[must_use]
    pub fn encode(self) -> [u8; 24] {
        let mut encoded = [0; 24];
        encoded[0..4].copy_from_slice(&APPEND_ACK_MAGIC);
        encoded[4..8].copy_from_slice(&self.partition_id.to_le_bytes());
        encoded[8..16].copy_from_slice(&self.first_offset.to_le_bytes());
        encoded[16..24].copy_from_slice(&self.last_offset.to_le_bytes());
        encoded
    }

    /// Decodes a fixed append acknowledgement payload.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        if payload.len() != 24 || payload[0..4] != APPEND_ACK_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native append acknowledgement",
            ));
        }
        Ok(Self {
            partition_id: u32::from_le_bytes(payload[4..8].try_into().expect("fixed range")),
            first_offset: u64::from_le_bytes(payload[8..16].try_into().expect("fixed range")),
            last_offset: u64::from_le_bytes(payload[16..24].try_into().expect("fixed range")),
        })
    }
}

/// Lightweight information read from a native batch header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeBatchInfo<'a> {
    /// UTF-8 tenant encoded by the batch.
    pub tenant: &'a str,
    /// Number of records declared by the batch.
    pub record_count: u32,
}

#[derive(Debug)]
struct NativeFieldView<'a> {
    key: String,
    raw_key: &'a str,
    value: &'a str,
}

#[derive(Debug)]
struct NativeStreamView<'a> {
    labels: Vec<NativeFieldView<'a>>,
    compression_cohort: CompressionCohortId,
}

#[derive(Debug)]
struct NativeMetadataView<'a> {
    key_id: usize,
    value: &'a str,
}

#[derive(Debug)]
struct NativeRecordView<'a> {
    stream_id: usize,
    timestamp_unix_nanos: u64,
    message: &'a str,
    metadata: std::ops::Range<usize>,
}

#[derive(Debug)]
struct NativeMetadataKey<'a> {
    raw: &'a str,
    normalized: String,
}

#[derive(Debug, Default)]
struct NativeMetadataKeys<'a> {
    entries: Vec<NativeMetadataKey<'a>>,
    ids: Option<HashMap<&'a str, usize>>,
}

impl<'a> NativeMetadataKeys<'a> {
    fn intern(&mut self, key: &'a str) -> usize {
        let existing = if let Some(ids) = &self.ids {
            ids.get(key).copied()
        } else {
            self.entries.iter().position(|entry| entry.raw == key)
        };
        if let Some(id) = existing {
            return id;
        }
        if self.entries.len() == LINEAR_NATIVE_KEY_LIMIT {
            self.ids = Some(
                self.entries
                    .iter()
                    .enumerate()
                    .map(|(id, entry)| (entry.raw, id))
                    .collect(),
            );
        }
        let id = self.entries.len();
        self.entries.push(NativeMetadataKey {
            raw: key,
            normalized: prefixed_key(METADATA_PREFIX, key),
        });
        if let Some(ids) = &mut self.ids {
            ids.insert(key, id);
        }
        id
    }
}

/// Fully validated borrowed view over one grouped native append batch.
///
/// The view owns only bounded record descriptors and normalized field-key
/// dictionaries. Message bodies and field values remain slices of the caller's
/// native frame.
#[derive(Debug)]
pub(crate) struct NativeBatchView<'a> {
    tenant: &'a str,
    streams: Vec<NativeStreamView<'a>>,
    records: Vec<NativeRecordView<'a>>,
    metadata: Vec<NativeMetadataView<'a>>,
    metadata_keys: NativeMetadataKeys<'a>,
}

impl NativeBatchView<'_> {
    pub(crate) fn tenant(&self) -> &str {
        self.tenant
    }

    pub(crate) fn record_count(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn record_cohort(&self, index: usize) -> Option<CompressionCohortId> {
        let record = self.records.get(index)?;
        self.streams
            .get(record.stream_id)
            .map(|stream| stream.compression_cohort)
    }

    pub(crate) fn record_timestamp(&self, index: usize) -> Option<u64> {
        self.records
            .get(index)
            .map(|record| record.timestamp_unix_nanos)
    }

    pub(crate) fn record_message(&self, index: usize) -> Option<&str> {
        self.records.get(index).map(|record| record.message)
    }

    pub(crate) fn record_field_count(&self, index: usize) -> Option<usize> {
        let record = self.records.get(index)?;
        let labels = self.streams.get(record.stream_id)?.labels.len();
        Some(1 + labels + record.metadata.len())
    }

    pub(crate) fn record_field(&self, record_index: usize, index: usize) -> Option<(&str, &str)> {
        let record = self.records.get(record_index)?;
        if index == 0 {
            return Some((TENANT_FIELD, self.tenant));
        }
        let stream = self.streams.get(record.stream_id)?;
        if let Some(label) = stream.labels.get(index - 1) {
            return Some((&label.key, label.value));
        }
        let metadata_index = record
            .metadata
            .start
            .checked_add(index.checked_sub(1 + stream.labels.len())?)?;
        if metadata_index >= record.metadata.end {
            return None;
        }
        let metadata = self.metadata.get(metadata_index)?;
        let key = self.metadata_keys.entries.get(metadata.key_id)?;
        Some((&key.normalized, metadata.value))
    }

    #[inline(always)]
    pub(crate) fn try_for_each_record_field<F>(
        &self,
        record_index: usize,
        mut visitor: F,
    ) -> LogDbResult<()>
    where
        F: FnMut(&str, &str) -> LogDbResult<()>,
    {
        let record = self
            .records
            .get(record_index)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "validated native record index is out of range",
            ))?;
        let stream = self
            .streams
            .get(record.stream_id)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "validated native stream index is out of range",
            ))?;
        visitor(TENANT_FIELD, self.tenant)?;
        for label in &stream.labels {
            visitor(&label.key, label.value)?;
        }
        for metadata in
            self.metadata
                .get(record.metadata.clone())
                .ok_or(LogDbError::InvalidBlockEncoding(
                    "validated native metadata range is out of bounds",
                ))?
        {
            let key = self.metadata_keys.entries.get(metadata.key_id).ok_or(
                LogDbError::InvalidBlockEncoding("validated native metadata key is out of range"),
            )?;
            visitor(&key.normalized, metadata.value)?;
        }
        Ok(())
    }
}

fn prefixed_key(prefix: &str, key: &str) -> String {
    let mut normalized = String::with_capacity(prefix.len() + key.len());
    normalized.push_str(prefix);
    normalized.push_str(key);
    normalized
}

/// Validates a grouped native append while retaining message and value slices
/// directly into `payload`.
pub(crate) fn decode_native_log_batch_view(
    payload: &[u8],
) -> Result<NativeBatchView<'_>, NativeProtocolError> {
    let info = inspect_native_log_batch(payload)?;
    let record_count =
        usize::try_from(info.record_count).expect("u32 record count fits this platform");
    if record_count > payload.len() / 16 {
        return Err(NativeProtocolError::new(
            "native batch record count exceeds its bounded payload",
        ));
    }
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let mut cursor = Cursor::at(payload, BATCH_HEADER_BYTES + info.tenant.len());
    let mut streams = Vec::with_capacity(stream_count);
    let mut records = Vec::with_capacity(record_count);
    let mut metadata = Vec::new();
    let mut metadata_keys = NativeMetadataKeys::default();
    for stream_id in 0..stream_count {
        let label_count = usize::from(cursor.u16("label count")?);
        if label_count > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        if cursor.u16("stream reserved bytes")? != 0 {
            return Err(NativeProtocolError::new(
                "native stream reserved bytes must be zero",
            ));
        }
        let entry_count = cursor.u32("entry count")? as usize;
        records
            .len()
            .checked_add(entry_count)
            .filter(|count| *count <= record_count)
            .ok_or_else(|| {
                NativeProtocolError::new("native stream counts exceed declared record count")
            })?;
        let mut labels = Vec::<NativeFieldView<'_>>::with_capacity(label_count);
        let mut large_label_keys = (label_count > LINEAR_NATIVE_KEY_LIMIT).then(BTreeSet::new);
        let mut cohort_hash = 0xcbf2_9ce4_8422_2325_u64;
        for _ in 0..label_count {
            let key = cursor.string16("label key")?;
            let value = cursor.string16("label value")?;
            let duplicate = if let Some(keys) = &mut large_label_keys {
                !keys.insert(key)
            } else {
                labels.iter().any(|field| field.raw_key == key)
            };
            if key.is_empty() || duplicate {
                return Err(NativeProtocolError::new(
                    "native stream contains an empty or duplicate label",
                ));
            }
            update_cohort_hash(&mut cohort_hash, key, value);
            labels.push(NativeFieldView {
                key: prefixed_key(LABEL_PREFIX, key),
                raw_key: key,
                value,
            });
        }
        streams.push(NativeStreamView {
            labels,
            compression_cohort: CompressionCohortId::new(cohort_hash),
        });
        for _ in 0..entry_count {
            let timestamp_unix_nanos = cursor.u64("timestamp")?;
            if timestamp_unix_nanos > i64::MAX as u64 {
                return Err(NativeProtocolError::new(
                    "native log timestamp exceeds the signed i64 range",
                ));
            }
            let line_len = cursor.u32("line length")? as usize;
            let metadata_count = usize::from(cursor.u16("metadata count")?);
            if metadata_count > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            if cursor.u16("entry reserved bytes")? != 0 {
                return Err(NativeProtocolError::new(
                    "native entry reserved bytes must be zero",
                ));
            }
            let message = cursor.string(line_len, "log line")?;
            let metadata_start = metadata.len();
            let mut large_metadata_keys =
                (metadata_count > LINEAR_NATIVE_KEY_LIMIT).then(BTreeSet::new);
            for _ in 0..metadata_count {
                let key = cursor.string16("metadata key")?;
                let value = cursor.string16("metadata value")?;
                let key_id = metadata_keys.intern(key);
                let duplicate = if let Some(keys) = &mut large_metadata_keys {
                    !keys.insert(key)
                } else {
                    metadata[metadata_start..]
                        .iter()
                        .any(|field: &NativeMetadataView<'_>| field.key_id == key_id)
                };
                if key.is_empty() || duplicate {
                    return Err(NativeProtocolError::new(
                        "native entry contains empty or duplicate metadata",
                    ));
                }
                metadata.push(NativeMetadataView { key_id, value });
            }
            records.push(NativeRecordView {
                stream_id,
                timestamp_unix_nanos,
                message,
                metadata: metadata_start..metadata.len(),
            });
        }
    }
    if records.len() != record_count {
        return Err(NativeProtocolError::new(format!(
            "native batch decoded {} records, expected {}",
            records.len(),
            info.record_count
        )));
    }
    cursor.finish()?;
    Ok(NativeBatchView {
        tenant: info.tenant,
        streams,
        records,
        metadata,
        metadata_keys,
    })
}

/// Returns whether a payload declares the native grouped-batch format.
#[must_use]
pub fn is_native_log_batch(payload: &[u8]) -> bool {
    payload.starts_with(&BATCH_MAGIC)
}

/// Reads the bounded batch header without decoding its records.
pub fn inspect_native_log_batch(
    payload: &[u8],
) -> Result<NativeBatchInfo<'_>, NativeProtocolError> {
    if payload.len() < BATCH_HEADER_BYTES {
        return Err(NativeProtocolError::new(
            "native log batch is shorter than its header",
        ));
    }
    if payload[0..4] != BATCH_MAGIC {
        return Err(NativeProtocolError::new("invalid native log batch magic"));
    }
    if payload[12..16] != [0; 4] {
        return Err(NativeProtocolError::new(
            "native log batch reserved bytes must be zero",
        ));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    if tenant_len > MAX_TENANT_BYTES {
        return Err(NativeProtocolError::new(
            "native log batch tenant exceeds its limit",
        ));
    }
    let end = BATCH_HEADER_BYTES
        .checked_add(tenant_len)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| NativeProtocolError::new("native log batch tenant is truncated"))?;
    let tenant = std::str::from_utf8(&payload[BATCH_HEADER_BYTES..end])
        .map_err(|_| NativeProtocolError::new("native log batch tenant is not UTF-8"))?;
    if tenant.is_empty() {
        return Err(NativeProtocolError::new(
            "native log batch tenant must not be empty",
        ));
    }
    Ok(NativeBatchInfo {
        tenant,
        record_count: u32::from_le_bytes(payload[8..12].try_into().expect("fixed range")),
    })
}

/// Encodes records with labels stored once per stream.
pub fn encode_native_log_batch(
    tenant: &str,
    entries: Vec<LokiEntry>,
) -> Result<Vec<u8>, NativeProtocolError> {
    validate_tenant(tenant)?;
    let record_count = u32::try_from(entries.len())
        .map_err(|_| NativeProtocolError::new("native batch contains more than u32 records"))?;
    let mut streams = BTreeMap::<BTreeMap<String, String>, Vec<LokiEntry>>::new();
    for entry in entries {
        if entry.timestamp_unix_nanos < 0 {
            return Err(NativeProtocolError::new(
                "negative native log timestamps are unsupported",
            ));
        }
        streams.entry(entry.labels.clone()).or_default().push(entry);
    }
    if streams.len() > MAX_STREAMS {
        return Err(NativeProtocolError::new(
            "native batch contains too many streams",
        ));
    }

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&BATCH_MAGIC);
    put_u16(&mut encoded, tenant.len(), "tenant")?;
    put_u16(&mut encoded, streams.len(), "stream count")?;
    encoded.extend_from_slice(&record_count.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(tenant.as_bytes());
    for (labels, entries) in streams {
        if labels.len() > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        put_u16(&mut encoded, labels.len(), "label count")?;
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        let entry_count = u32::try_from(entries.len())
            .map_err(|_| NativeProtocolError::new("native stream contains too many entries"))?;
        encoded.extend_from_slice(&entry_count.to_le_bytes());
        for (key, value) in &labels {
            put_string16(&mut encoded, key, "label key")?;
            put_string16(&mut encoded, value, "label value")?;
        }
        for entry in entries {
            encoded.extend_from_slice(&(entry.timestamp_unix_nanos as u64).to_le_bytes());
            put_u32(&mut encoded, entry.line.len(), "log line")?;
            if entry.structured_metadata.len() > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            put_u16(
                &mut encoded,
                entry.structured_metadata.len(),
                "metadata count",
            )?;
            encoded.extend_from_slice(&0_u16.to_le_bytes());
            encoded.extend_from_slice(entry.line.as_bytes());
            for (key, value) in entry.structured_metadata {
                put_string16(&mut encoded, &key, "metadata key")?;
                put_string16(&mut encoded, &value, "metadata value")?;
            }
        }
    }
    if encoded.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeProtocolError::new(format!(
            "native batch is {} bytes, exceeding {MAX_NATIVE_FRAME_BYTES}",
            encoded.len()
        )));
    }
    Ok(encoded)
}

/// Decodes and fully validates a grouped native log batch.
pub fn decode_native_log_batch(payload: &[u8]) -> Result<NativeLogBatch, NativeProtocolError> {
    let info = inspect_native_log_batch(payload)?;
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let mut cursor = Cursor::at(payload, BATCH_HEADER_BYTES + info.tenant.len());
    let mut entries = Vec::with_capacity(info.record_count as usize);
    for _ in 0..stream_count {
        let label_count = usize::from(cursor.u16("label count")?);
        if label_count > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        if cursor.u16("stream reserved bytes")? != 0 {
            return Err(NativeProtocolError::new(
                "native stream reserved bytes must be zero",
            ));
        }
        let entry_count = cursor.u32("entry count")? as usize;
        let mut labels = BTreeMap::new();
        for _ in 0..label_count {
            let key = cursor.string16("label key")?.to_owned();
            let value = cursor.string16("label value")?.to_owned();
            if key.is_empty() || labels.insert(key, value).is_some() {
                return Err(NativeProtocolError::new(
                    "native stream contains an empty or duplicate label",
                ));
            }
        }
        entries
            .len()
            .checked_add(entry_count)
            .filter(|count| *count <= info.record_count as usize)
            .ok_or_else(|| {
                NativeProtocolError::new("native stream counts exceed declared record count")
            })?;
        for _ in 0..entry_count {
            let timestamp = cursor.u64("timestamp")?;
            let line_len = cursor.u32("line length")? as usize;
            let metadata_count = usize::from(cursor.u16("metadata count")?);
            if metadata_count > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            if cursor.u16("entry reserved bytes")? != 0 {
                return Err(NativeProtocolError::new(
                    "native entry reserved bytes must be zero",
                ));
            }
            let line = cursor.string(line_len, "log line")?.to_owned();
            let mut structured_metadata = BTreeMap::new();
            for _ in 0..metadata_count {
                let key = cursor.string16("metadata key")?.to_owned();
                let value = cursor.string16("metadata value")?.to_owned();
                if key.is_empty() || structured_metadata.insert(key, value).is_some() {
                    return Err(NativeProtocolError::new(
                        "native entry contains empty or duplicate metadata",
                    ));
                }
            }
            let timestamp_unix_nanos = i64::try_from(timestamp).map_err(|_| {
                NativeProtocolError::new("native log timestamp exceeds the signed i64 range")
            })?;
            entries.push(LokiEntry {
                timestamp_unix_nanos,
                labels: labels.clone(),
                line,
                structured_metadata,
            });
        }
    }
    if entries.len() != info.record_count as usize {
        return Err(NativeProtocolError::new(format!(
            "native batch decoded {} records, expected {}",
            entries.len(),
            info.record_count
        )));
    }
    cursor.finish()?;
    Ok(NativeLogBatch {
        tenant: info.tenant.to_owned(),
        entries,
    })
}

/// Decodes a grouped native batch directly into normalized indexing events.
///
/// Stream labels are materialized once and cloned as shared `Arc` fields for
/// each event, avoiding compatibility-layer maps on the durable ingest path.
pub fn decode_native_log_events(
    payload: &[u8],
) -> Result<(String, Vec<OtlpLogEvent>), NativeProtocolError> {
    let info = inspect_native_log_batch(payload)?;
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let tenant = info.tenant.to_owned();
    let tenant_field = MetadataField::new(TENANT_FIELD, tenant.clone());
    let mut cursor = Cursor::at(payload, BATCH_HEADER_BYTES + info.tenant.len());
    let mut events = Vec::with_capacity(info.record_count as usize);
    for _ in 0..stream_count {
        let label_count = usize::from(cursor.u16("label count")?);
        if label_count > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        if cursor.u16("stream reserved bytes")? != 0 {
            return Err(NativeProtocolError::new(
                "native stream reserved bytes must be zero",
            ));
        }
        let entry_count = cursor.u32("entry count")? as usize;
        events
            .len()
            .checked_add(entry_count)
            .filter(|count| *count <= info.record_count as usize)
            .ok_or_else(|| {
                NativeProtocolError::new("native stream counts exceed declared record count")
            })?;
        let mut label_keys = BTreeSet::new();
        let mut label_fields = Vec::with_capacity(label_count + 1);
        label_fields.push(tenant_field.clone());
        let mut cohort_hash = 0xcbf2_9ce4_8422_2325_u64;
        for _ in 0..label_count {
            let key = cursor.string16("label key")?;
            let value = cursor.string16("label value")?;
            if key.is_empty() || !label_keys.insert(key) {
                return Err(NativeProtocolError::new(
                    "native stream contains an empty or duplicate label",
                ));
            }
            update_cohort_hash(&mut cohort_hash, key, value);
            label_fields.push(MetadataField::new(
                format!("{LABEL_PREFIX}{key}"),
                Arc::<str>::from(value),
            ));
        }
        let compression_cohort = CompressionCohortId::new(cohort_hash);
        let label_fields = Arc::new(label_fields);
        let mut previous_line = None::<Arc<str>>;
        let mut previous_single_metadata = None::<(Arc<str>, Arc<str>, Arc<Vec<MetadataField>>)>;
        for _ in 0..entry_count {
            let timestamp_unix_nanos = cursor.u64("timestamp")?;
            if timestamp_unix_nanos > i64::MAX as u64 {
                return Err(NativeProtocolError::new(
                    "native log timestamp exceeds the signed i64 range",
                ));
            }
            let line_len = cursor.u32("line length")? as usize;
            let metadata_count = usize::from(cursor.u16("metadata count")?);
            if metadata_count > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            if cursor.u16("entry reserved bytes")? != 0 {
                return Err(NativeProtocolError::new(
                    "native entry reserved bytes must be zero",
                ));
            }
            let observed_line = cursor.string(line_len, "log line")?;
            let line = match &previous_line {
                Some(previous) if previous.as_ref() == observed_line => Arc::clone(previous),
                _ => {
                    let line = Arc::<str>::from(observed_line);
                    previous_line = Some(Arc::clone(&line));
                    line
                }
            };
            let fields = match metadata_count {
                0 => Arc::clone(&label_fields),
                1 => {
                    let key = cursor.string16("metadata key")?;
                    let value = cursor.string16("metadata value")?;
                    if key.is_empty() {
                        return Err(NativeProtocolError::new(
                            "native entry contains empty metadata",
                        ));
                    }
                    match &previous_single_metadata {
                        Some((previous_key, previous_value, fields))
                            if previous_key.as_ref() == key && previous_value.as_ref() == value =>
                        {
                            Arc::clone(fields)
                        }
                        _ => {
                            let key = Arc::<str>::from(key);
                            let value = Arc::<str>::from(value);
                            let mut fields = Vec::with_capacity(label_fields.len() + 1);
                            fields.extend_from_slice(&label_fields);
                            fields.push(MetadataField::new(
                                format!("{METADATA_PREFIX}{key}"),
                                Arc::clone(&value),
                            ));
                            let fields = Arc::new(fields);
                            previous_single_metadata = Some((key, value, Arc::clone(&fields)));
                            fields
                        }
                    }
                }
                _ => {
                    let mut fields = Vec::with_capacity(label_fields.len() + metadata_count);
                    fields.extend_from_slice(&label_fields);
                    let mut metadata_keys = BTreeSet::new();
                    for _ in 0..metadata_count {
                        let key = cursor.string16("metadata key")?;
                        let value = cursor.string16("metadata value")?;
                        if key.is_empty() || !metadata_keys.insert(key) {
                            return Err(NativeProtocolError::new(
                                "native entry contains empty or duplicate metadata",
                            ));
                        }
                        fields.push(MetadataField::new(
                            format!("{METADATA_PREFIX}{key}"),
                            Arc::<str>::from(value),
                        ));
                    }
                    Arc::new(fields)
                }
            };
            events.push(OtlpLogEvent {
                timestamp_unix_nanos,
                message: line,
                fields,
                compression_cohort,
            });
        }
    }
    if events.len() != info.record_count as usize {
        return Err(NativeProtocolError::new(format!(
            "native batch decoded {} records, expected {}",
            events.len(),
            info.record_count
        )));
    }
    cursor.finish()?;
    Ok((tenant, events))
}

/// Fully validates a grouped native batch without materializing its records.
///
/// Compatibility sinks use this before reserving offsets. The production
/// native service instead validates while building a borrowed structural view.
pub fn validate_native_log_batch(payload: &[u8]) -> Result<(), NativeProtocolError> {
    let info = inspect_native_log_batch(payload)?;
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let mut cursor = Cursor::at(payload, BATCH_HEADER_BYTES + info.tenant.len());
    let mut decoded_count = 0usize;
    for _ in 0..stream_count {
        let label_count = usize::from(cursor.u16("label count")?);
        if label_count > MAX_LABELS_PER_STREAM {
            return Err(NativeProtocolError::new(
                "native stream contains too many labels",
            ));
        }
        if cursor.u16("stream reserved bytes")? != 0 {
            return Err(NativeProtocolError::new(
                "native stream reserved bytes must be zero",
            ));
        }
        let entry_count = cursor.u32("entry count")? as usize;
        decoded_count = decoded_count
            .checked_add(entry_count)
            .filter(|count| *count <= info.record_count as usize)
            .ok_or_else(|| {
                NativeProtocolError::new("native stream counts exceed declared record count")
            })?;
        let mut label_keys = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            let key = cursor.string16("label key")?;
            let _value = cursor.string16("label value")?;
            if key.is_empty() || label_keys.contains(&key) {
                return Err(NativeProtocolError::new(
                    "native stream contains an empty or duplicate label",
                ));
            }
            label_keys.push(key);
        }
        for _ in 0..entry_count {
            let timestamp_unix_nanos = cursor.u64("timestamp")?;
            if timestamp_unix_nanos > i64::MAX as u64 {
                return Err(NativeProtocolError::new(
                    "native log timestamp exceeds the signed i64 range",
                ));
            }
            let line_len = cursor.u32("line length")? as usize;
            let metadata_count = usize::from(cursor.u16("metadata count")?);
            if metadata_count > MAX_METADATA_PER_ENTRY {
                return Err(NativeProtocolError::new(
                    "native entry contains too much structured metadata",
                ));
            }
            if cursor.u16("entry reserved bytes")? != 0 {
                return Err(NativeProtocolError::new(
                    "native entry reserved bytes must be zero",
                ));
            }
            let _line = cursor.string(line_len, "log line")?;
            match metadata_count {
                0 => {}
                1 => {
                    let key = cursor.string16("metadata key")?;
                    let _value = cursor.string16("metadata value")?;
                    if key.is_empty() {
                        return Err(NativeProtocolError::new(
                            "native entry contains empty metadata",
                        ));
                    }
                }
                _ => {
                    let mut metadata_keys = Vec::with_capacity(metadata_count);
                    for _ in 0..metadata_count {
                        let key = cursor.string16("metadata key")?;
                        let _value = cursor.string16("metadata value")?;
                        if key.is_empty() || metadata_keys.contains(&key) {
                            return Err(NativeProtocolError::new(
                                "native entry contains empty or duplicate metadata",
                            ));
                        }
                        metadata_keys.push(key);
                    }
                }
            }
        }
    }
    if decoded_count != info.record_count as usize {
        return Err(NativeProtocolError::new(format!(
            "native batch decoded {decoded_count} records, expected {}",
            info.record_count
        )));
    }
    cursor.finish()
}

/// Sort direction for an indexed native query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NativeQueryDirection {
    /// Lowest timestamps first.
    #[default]
    OldestFirst,
    /// Highest timestamps first.
    NewestFirst,
}

/// Bounded native indexed-query request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeQuery {
    /// Tenant to search.
    pub tenant: String,
    /// Exact stream labels combined with AND semantics.
    pub labels: BTreeMap<String, String>,
    /// Case-insensitive exact message tokens combined with AND semantics.
    pub terms: Vec<String>,
    /// Inclusive lower timestamp bound, or no lower bound.
    pub start_timestamp_unix_nanos: Option<u64>,
    /// Exclusive upper timestamp bound, or no upper bound.
    pub end_timestamp_unix_nanos: Option<u64>,
    /// Maximum records to return.
    pub limit: u32,
    /// Timestamp result order.
    pub direction: NativeQueryDirection,
}

/// Encodes an indexed native query.
pub fn encode_native_query(query: &NativeQuery) -> Result<Vec<u8>, NativeProtocolError> {
    validate_query(query)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&QUERY_MAGIC);
    put_u16(&mut encoded, query.tenant.len(), "query tenant")?;
    put_u16(&mut encoded, query.labels.len(), "query label count")?;
    put_u16(&mut encoded, query.terms.len(), "query term count")?;
    encoded.push(match query.direction {
        NativeQueryDirection::OldestFirst => 0,
        NativeQueryDirection::NewestFirst => 1,
    });
    encoded.push(0);
    encoded.extend_from_slice(&query.limit.to_le_bytes());
    encoded.extend_from_slice(
        &query
            .start_timestamp_unix_nanos
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    encoded.extend_from_slice(
        &query
            .end_timestamp_unix_nanos
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    encoded.extend_from_slice(query.tenant.as_bytes());
    for (key, value) in &query.labels {
        put_string16(&mut encoded, key, "query label key")?;
        put_string16(&mut encoded, value, "query label value")?;
    }
    for term in &query.terms {
        put_string16(&mut encoded, term, "query term")?;
    }
    Ok(encoded)
}

/// Decodes and validates an indexed native query.
pub fn decode_native_query(payload: &[u8]) -> Result<NativeQuery, NativeProtocolError> {
    if payload.len() < QUERY_HEADER_BYTES || payload[0..4] != QUERY_MAGIC {
        return Err(NativeProtocolError::new("invalid native query header"));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    let label_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let term_count = usize::from(u16::from_le_bytes(
        payload[8..10].try_into().expect("fixed range"),
    ));
    let direction = match payload[10] {
        0 => NativeQueryDirection::OldestFirst,
        1 => NativeQueryDirection::NewestFirst,
        value => {
            return Err(NativeProtocolError::new(format!(
                "unsupported native query direction {value}"
            )));
        }
    };
    if payload[11] != 0 {
        return Err(NativeProtocolError::new(
            "native query reserved byte must be zero",
        ));
    }
    let limit = u32::from_le_bytes(payload[12..16].try_into().expect("fixed range"));
    let start = u64::from_le_bytes(payload[16..24].try_into().expect("fixed range"));
    let end = u64::from_le_bytes(payload[24..32].try_into().expect("fixed range"));
    let mut cursor = Cursor::at(payload, QUERY_HEADER_BYTES);
    let tenant = cursor.string(tenant_len, "query tenant")?.to_owned();
    let mut labels = BTreeMap::new();
    for _ in 0..label_count {
        let key = cursor.string16("query label key")?.to_owned();
        let value = cursor.string16("query label value")?.to_owned();
        if key.is_empty() || labels.insert(key, value).is_some() {
            return Err(NativeProtocolError::new(
                "native query contains an empty or duplicate label",
            ));
        }
    }
    let mut terms = Vec::with_capacity(term_count);
    for _ in 0..term_count {
        let term = cursor.string16("query term")?.to_owned();
        if term.is_empty() {
            return Err(NativeProtocolError::new(
                "native query terms must not be empty",
            ));
        }
        terms.push(term);
    }
    cursor.finish()?;
    let query = NativeQuery {
        tenant,
        labels,
        terms,
        start_timestamp_unix_nanos: (start != u64::MAX).then_some(start),
        end_timestamp_unix_nanos: (end != u64::MAX).then_some(end),
        limit,
        direction,
    };
    validate_query(&query)?;
    Ok(query)
}

/// Protocol validation error suitable for a native bad-request response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeProtocolError {
    message: String,
}

impl NativeProtocolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for NativeProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NativeProtocolError {}

fn validate_tenant(tenant: &str) -> Result<(), NativeProtocolError> {
    if tenant.is_empty() {
        return Err(NativeProtocolError::new("native tenant must not be empty"));
    }
    if tenant.len() > MAX_TENANT_BYTES || tenant.len() > usize::from(u16::MAX) {
        return Err(NativeProtocolError::new(
            "native tenant exceeds its length limit",
        ));
    }
    Ok(())
}

fn validate_query(query: &NativeQuery) -> Result<(), NativeProtocolError> {
    validate_tenant(&query.tenant)?;
    if query.labels.len() > MAX_LABELS_PER_STREAM {
        return Err(NativeProtocolError::new(
            "native query contains too many labels",
        ));
    }
    if query.terms.len() > MAX_QUERY_TERMS {
        return Err(NativeProtocolError::new(
            "native query contains too many terms",
        ));
    }
    if query.limit == 0 || query.limit > MAX_QUERY_LIMIT {
        return Err(NativeProtocolError::new(format!(
            "native query limit must be in 1..={MAX_QUERY_LIMIT}"
        )));
    }
    if let (Some(start), Some(end)) = (
        query.start_timestamp_unix_nanos,
        query.end_timestamp_unix_nanos,
    ) && start >= end
    {
        return Err(NativeProtocolError::new(
            "native query timestamp range must be nonempty",
        ));
    }
    for (key, value) in &query.labels {
        if key.is_empty() {
            return Err(NativeProtocolError::new(
                "native query label keys must not be empty",
            ));
        }
        validate_string16(key, "query label key")?;
        validate_string16(value, "query label value")?;
    }
    for term in &query.terms {
        if term.is_empty() {
            return Err(NativeProtocolError::new(
                "native query terms must not be empty",
            ));
        }
        validate_string16(term, "query term")?;
    }
    Ok(())
}

fn update_cohort_hash(hash: &mut u64, key: &str, value: &str) {
    for byte in key
        .bytes()
        .chain(std::iter::once(0xff))
        .chain(value.bytes())
    {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    *hash ^= 0xfe;
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

fn payload_checksum(payload: &[u8]) -> u32 {
    u32::from_le_bytes(
        blake3::hash(payload).as_bytes()[0..4]
            .try_into()
            .expect("fixed range"),
    )
}

fn put_u16(
    encoded: &mut Vec<u8>,
    value: usize,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    let value = u16::try_from(value)
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u16")))?;
    encoded.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_u32(
    encoded: &mut Vec<u8>,
    value: usize,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    let value = u32::try_from(value)
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u32")))?;
    encoded.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn validate_string16(value: &str, field: &'static str) -> Result<(), NativeProtocolError> {
    u16::try_from(value.len())
        .map(|_| ())
        .map_err(|_| NativeProtocolError::new(format!("{field} exceeds u16")))
}

fn put_string16(
    encoded: &mut Vec<u8>,
    value: &str,
    field: &'static str,
) -> Result<(), NativeProtocolError> {
    put_u16(encoded, value.len(), field)?;
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn at(bytes: &'a [u8], offset: usize) -> Self {
        Self { bytes, offset }
    }

    fn bytes(&mut self, len: usize, field: &'static str) -> Result<&'a [u8], NativeProtocolError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| NativeProtocolError::new(format!("native {field} is truncated")))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, NativeProtocolError> {
        Ok(u16::from_le_bytes(
            self.bytes(2, field)?.try_into().expect("fixed range"),
        ))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, NativeProtocolError> {
        Ok(u32::from_le_bytes(
            self.bytes(4, field)?.try_into().expect("fixed range"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, NativeProtocolError> {
        Ok(u64::from_le_bytes(
            self.bytes(8, field)?.try_into().expect("fixed range"),
        ))
    }

    fn string(&mut self, len: usize, field: &'static str) -> Result<&'a str, NativeProtocolError> {
        std::str::from_utf8(self.bytes(len, field)?)
            .map_err(|_| NativeProtocolError::new(format!("native {field} is not UTF-8")))
    }

    fn string16(&mut self, field: &'static str) -> Result<&'a str, NativeProtocolError> {
        let len = usize::from(self.u16(field)?);
        self.string(len, field)
    }

    fn finish(self) -> Result<(), NativeProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(NativeProtocolError::new(
                "native payload contains trailing bytes",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<LokiEntry> {
        vec![
            LokiEntry {
                timestamp_unix_nanos: 10,
                labels: BTreeMap::from([
                    ("app".to_owned(), "api".to_owned()),
                    ("region".to_owned(), "東京".to_owned()),
                ]),
                line: "request café".to_owned(),
                structured_metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
            },
            LokiEntry {
                timestamp_unix_nanos: 11,
                labels: BTreeMap::from([
                    ("app".to_owned(), "api".to_owned()),
                    ("region".to_owned(), "東京".to_owned()),
                ]),
                line: "request complete".to_owned(),
                structured_metadata: BTreeMap::new(),
            },
        ]
    }

    #[test]
    fn grouped_batch_round_trips_byte_exact_text_and_metadata() {
        let expected = entries();
        let encoded = encode_native_log_batch("tenant-a", expected.clone()).expect("batch encodes");
        let info = inspect_native_log_batch(&encoded).expect("header");
        assert_eq!(info.tenant, "tenant-a");
        assert_eq!(info.record_count, 2);
        validate_native_log_batch(&encoded).expect("batch validates without materializing");
        let decoded = decode_native_log_batch(&encoded).expect("batch decodes");
        assert_eq!(decoded.tenant, "tenant-a");
        assert_eq!(decoded.entries, expected);
        let (tenant, events) = decode_native_log_events(&encoded).expect("direct events");
        assert_eq!(tenant, "tenant-a");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message.as_ref(), "request café");
        assert!(events[0].fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.label.region" && field.value.as_ref() == "東京"
        }));
        let borrowed = decode_native_log_batch_view(&encoded).expect("borrowed view");
        assert_eq!(borrowed.tenant(), "tenant-a");
        assert_eq!(borrowed.record_count(), 2);
        assert_eq!(borrowed.record_timestamp(0), Some(10));
        assert_eq!(borrowed.record_message(0), Some("request café"));
        assert_eq!(borrowed.record_field_count(0), Some(4));
        assert_eq!(
            borrowed.record_field(0, 0),
            Some(("resource.loki.tenant", "tenant-a"))
        );
        assert_eq!(
            borrowed.record_field(0, 2),
            Some(("resource.loki.label.region", "東京"))
        );
        assert_eq!(
            borrowed.record_field(0, 3),
            Some(("attr.loki.metadata.trace_id", "abc"))
        );
        assert_eq!(borrowed.record_field(0, 4), None);
    }

    #[test]
    fn batch_rejects_truncation_count_mismatch_and_trailing_bytes() {
        let encoded = encode_native_log_batch("tenant-a", entries()).expect("batch");
        for end in 0..encoded.len() {
            assert!(decode_native_log_batch(&encoded[..end]).is_err(), "{end}");
            assert!(
                decode_native_log_batch_view(&encoded[..end]).is_err(),
                "{end}"
            );
            assert!(validate_native_log_batch(&encoded[..end]).is_err(), "{end}");
        }
        let mut count = encoded.clone();
        count[8..12].copy_from_slice(&3_u32.to_le_bytes());
        assert!(decode_native_log_batch(&count).is_err());
        assert!(decode_native_log_batch_view(&count).is_err());
        assert!(validate_native_log_batch(&count).is_err());
        let mut reserved = encoded.clone();
        reserved[12] = 1;
        assert!(decode_native_log_batch(&reserved).is_err());
        assert!(decode_native_log_batch_view(&reserved).is_err());
        assert!(validate_native_log_batch(&reserved).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_native_log_batch(&trailing).is_err());
        assert!(decode_native_log_batch_view(&trailing).is_err());
        assert!(validate_native_log_batch(&trailing).is_err());
    }

    #[test]
    fn frame_header_checks_magic_version_length_and_payload_checksum() {
        let frame = NativeFrame::request(NativeOpcode::Append, 42, vec![1, 2, 3]).expect("frame");
        let encoded = frame.encode();
        let header: [u8; NATIVE_FRAME_HEADER_BYTES] = encoded[..NATIVE_FRAME_HEADER_BYTES]
            .try_into()
            .expect("header");
        let decoded = NativeFrameHeader::decode(&header).expect("decode");
        assert_eq!(decoded.request_id, 42);
        decoded
            .verify_payload(&encoded[NATIVE_FRAME_HEADER_BYTES..])
            .expect("checksum");
        assert!(decoded.verify_payload(&[1, 2, 4]).is_err());

        let mut invalid = header;
        invalid[4] = 2;
        assert!(NativeFrameHeader::decode(&invalid).is_err());
    }

    #[test]
    fn indexed_query_round_trips_all_bounds_and_constraints() {
        let query = NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: vec!["timeout".to_owned(), "error".to_owned()],
            start_timestamp_unix_nanos: Some(10),
            end_timestamp_unix_nanos: Some(20),
            limit: 100,
            direction: NativeQueryDirection::NewestFirst,
        };
        assert_eq!(
            decode_native_query(&encode_native_query(&query).expect("encode")).expect("decode"),
            query
        );
    }
}
