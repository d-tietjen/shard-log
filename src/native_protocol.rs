use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use shard_stream_core::{LogicalPartitionId, TopicId, TopicPartition};

use crate::{LokiEntry, TelemetryEnvelope};

/// Fixed number of bytes in every native protocol frame header.
pub const NATIVE_FRAME_HEADER_BYTES: usize = 32;
/// Production maximum for one native request or response payload.
pub const MAX_NATIVE_FRAME_BYTES: usize = 64 * 1024 * 1024;

const FRAME_MAGIC: [u8; 4] = *b"STNP";
const FRAME_VERSION: u8 = 2;
const FRAME_FLAG_RESPONSE: u8 = 1;
const LOG_QUERY_RESULT_MAGIC: [u8; 4] = *b"STR2";
const TELEMETRY_BATCH_MAGIC: [u8; 4] = *b"STB2";
const TELEMETRY_ACK_MAGIC: [u8; 4] = *b"STM2";
const QUERY_MAGIC: [u8; 4] = *b"STQ2";
const LOG_QUERY_RESULT_HEADER_BYTES: usize = 16;
const QUERY_HEADER_BYTES: usize = 32;
const MAX_TENANT_BYTES: usize = 1_024;
const MAX_STREAMS: usize = 65_535;
const MAX_LABELS_PER_STREAM: usize = 256;
const MAX_METADATA_PER_ENTRY: usize = 256;
const MAX_QUERY_TERMS: usize = 256;
const MAX_QUERY_LIMIT: u32 = 1_000_000;

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

/// Decoded stream-grouped native log query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeLogQueryResult {
    /// Tenant whose records are returned by the query.
    pub tenant: String,
    /// Flattened records; stream labels are encoded once per stream on the wire.
    pub entries: Vec<LokiEntry>,
}

/// One routed STEL envelope in a signal-aware native append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePartitionAppend {
    /// Exact logical topic and partition selected before append.
    pub topic_partition: TopicPartition,
    /// Checksummed signal envelope for that partition.
    pub envelope: TelemetryEnvelope,
}

/// Native protocol v2 append containing one envelope per resulting partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTelemetryBatch {
    /// Routed partition appends. Duplicate partitions are rejected.
    pub partitions: Vec<NativePartitionAppend>,
}

impl NativeTelemetryBatch {
    /// Encodes the bounded signal-aware native v2 payload.
    pub fn encode(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.is_empty() || self.partitions.len() > 256 {
            return Err(NativeProtocolError::new(
                "native telemetry batch requires 1..=256 partitions",
            ));
        }
        let mut seen = BTreeSet::new();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&TELEMETRY_BATCH_MAGIC);
        encoded.extend_from_slice(
            &u16::try_from(self.partitions.len())
                .expect("partition count was bounded")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[0; 2]);
        for partition in &self.partitions {
            if !seen.insert(partition.topic_partition) {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            if partition.topic_partition.topic_id != partition.envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            let envelope = partition
                .envelope
                .encode()
                .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            encoded.extend_from_slice(&partition.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.topic_partition.partition_id.get().to_le_bytes());
            encoded.extend_from_slice(
                &u32::try_from(envelope.len())
                    .map_err(|_| NativeProtocolError::new("STEL envelope exceeds u32"))?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(&envelope);
        }
        if encoded.len() > MAX_NATIVE_FRAME_BYTES {
            return Err(NativeProtocolError::new(
                "native telemetry batch exceeds the frame limit",
            ));
        }
        Ok(encoded)
    }

    /// Decodes and verifies every STEL envelope before returning any partition.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_BATCH_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry batch header",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count == 0 || count > 256 || payload[6..8] != [0, 0] {
            return Err(NativeProtocolError::new(
                "invalid native telemetry partition count or flags",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut partitions = Vec::with_capacity(count);
        let mut seen = BTreeSet::new();
        for _ in 0..count {
            let topic_partition = TopicPartition::new(
                TopicId::new(cursor.u128("telemetry topic ID")?),
                LogicalPartitionId::new(cursor.u32("telemetry partition ID")?),
            );
            let envelope_len = cursor.u32("telemetry envelope length")? as usize;
            let envelope =
                TelemetryEnvelope::decode(cursor.bytes(envelope_len, "telemetry envelope")?)
                    .map_err(|error| NativeProtocolError::new(error.to_string()))?;
            if topic_partition.topic_id != envelope.signal.topic_id() {
                return Err(NativeProtocolError::new(
                    "native telemetry partition topic disagrees with its signal",
                ));
            }
            if !seen.insert(topic_partition) {
                return Err(NativeProtocolError::new(
                    "native telemetry batch contains a duplicate partition",
                ));
            }
            partitions.push(NativePartitionAppend {
                topic_partition,
                envelope,
            });
        }
        cursor.finish()?;
        Ok(Self { partitions })
    }
}

/// Returns true when a native append payload uses the signal-aware v2 batch format.
#[must_use]
pub fn is_native_telemetry_batch(payload: &[u8]) -> bool {
    payload.starts_with(&TELEMETRY_BATCH_MAGIC)
}

/// Per-partition acknowledgement returned by native protocol v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativePartitionAck {
    /// Appended topic and partition.
    pub topic_partition: TopicPartition,
    /// First assigned durable offset.
    pub first_offset: u64,
    /// Last assigned durable offset.
    pub last_offset: u64,
}

/// Atomic native v2 response containing one acknowledgement per partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTelemetryAppendAck {
    /// Partition acknowledgements in request order.
    pub partitions: Vec<NativePartitionAck>,
}

impl NativeTelemetryAppendAck {
    /// Encodes a native v2 multi-partition acknowledgement.
    pub fn encode(&self) -> Result<Vec<u8>, NativeProtocolError> {
        if self.partitions.len() > 256 {
            return Err(NativeProtocolError::new(
                "native acknowledgement exceeds 256 partitions",
            ));
        }
        let mut encoded = Vec::with_capacity(8 + self.partitions.len() * 36);
        encoded.extend_from_slice(&TELEMETRY_ACK_MAGIC);
        encoded.extend_from_slice(
            &u16::try_from(self.partitions.len())
                .expect("ack partition count was bounded")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[0; 2]);
        for partition in &self.partitions {
            encoded.extend_from_slice(&partition.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.topic_partition.partition_id.get().to_le_bytes());
            encoded.extend_from_slice(&partition.first_offset.to_le_bytes());
            encoded.extend_from_slice(&partition.last_offset.to_le_bytes());
        }
        Ok(encoded)
    }

    /// Decodes a native v2 multi-partition acknowledgement.
    pub fn decode(payload: &[u8]) -> Result<Self, NativeProtocolError> {
        if payload.len() < 8 || payload[..4] != TELEMETRY_ACK_MAGIC {
            return Err(NativeProtocolError::new(
                "invalid native telemetry acknowledgement",
            ));
        }
        let count = usize::from(u16::from_le_bytes(
            payload[4..6].try_into().expect("fixed range"),
        ));
        if count > 256 || payload[6..8] != [0, 0] || payload.len() != 8 + count * 36 {
            return Err(NativeProtocolError::new(
                "invalid native telemetry acknowledgement length",
            ));
        }
        let mut cursor = Cursor::at(payload, 8);
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(NativePartitionAck {
                topic_partition: TopicPartition::new(
                    TopicId::new(cursor.u128("ack topic ID")?),
                    LogicalPartitionId::new(cursor.u32("ack partition ID")?),
                ),
                first_offset: cursor.u64("ack first offset")?,
                last_offset: cursor.u64("ack last offset")?,
            });
        }
        cursor.finish()?;
        Ok(Self { partitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NativeLogQueryResultInfo<'a> {
    tenant: &'a str,
    record_count: u32,
}

fn inspect_native_log_query_result(
    payload: &[u8],
) -> Result<NativeLogQueryResultInfo<'_>, NativeProtocolError> {
    if payload.len() < LOG_QUERY_RESULT_HEADER_BYTES {
        return Err(NativeProtocolError::new(
            "native log query result is shorter than its header",
        ));
    }
    if payload[0..4] != LOG_QUERY_RESULT_MAGIC {
        return Err(NativeProtocolError::new(
            "invalid native log query result magic",
        ));
    }
    if payload[12..16] != [0; 4] {
        return Err(NativeProtocolError::new(
            "native log query result reserved bytes must be zero",
        ));
    }
    let tenant_len = usize::from(u16::from_le_bytes(
        payload[4..6].try_into().expect("fixed range"),
    ));
    if tenant_len > MAX_TENANT_BYTES {
        return Err(NativeProtocolError::new(
            "native log query result tenant exceeds its limit",
        ));
    }
    let end = LOG_QUERY_RESULT_HEADER_BYTES
        .checked_add(tenant_len)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| NativeProtocolError::new("native log query result tenant is truncated"))?;
    let tenant = std::str::from_utf8(&payload[LOG_QUERY_RESULT_HEADER_BYTES..end])
        .map_err(|_| NativeProtocolError::new("native log query result tenant is not UTF-8"))?;
    if tenant.is_empty() {
        return Err(NativeProtocolError::new(
            "native log query result tenant must not be empty",
        ));
    }
    Ok(NativeLogQueryResultInfo {
        tenant,
        record_count: u32::from_le_bytes(payload[8..12].try_into().expect("fixed range")),
    })
}

/// Encodes records with labels stored once per stream.
pub fn encode_native_log_query_result(
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
    encoded.extend_from_slice(&LOG_QUERY_RESULT_MAGIC);
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
pub fn decode_native_log_query_result(
    payload: &[u8],
) -> Result<NativeLogQueryResult, NativeProtocolError> {
    let info = inspect_native_log_query_result(payload)?;
    let stream_count = usize::from(u16::from_le_bytes(
        payload[6..8].try_into().expect("fixed range"),
    ));
    let mut cursor = Cursor::at(payload, LOG_QUERY_RESULT_HEADER_BYTES + info.tenant.len());
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
    Ok(NativeLogQueryResult {
        tenant: info.tenant.to_owned(),
        entries,
    })
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

    fn u128(&mut self, field: &'static str) -> Result<u128, NativeProtocolError> {
        Ok(u128::from_le_bytes(
            self.bytes(16, field)?.try_into().expect("fixed range"),
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

    #[test]
    fn signal_aware_batch_and_partition_ack_round_trip() {
        let topic_partition =
            TopicPartition::new(crate::TRACES_TOPIC_ID, LogicalPartitionId::new(7));
        let batch = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition,
                envelope: TelemetryEnvelope::new(
                    crate::TelemetrySignal::Traces,
                    "tenant-a",
                    2,
                    &b"route"[..],
                    &b"payload"[..],
                )
                .unwrap(),
            }],
        };
        assert_eq!(
            NativeTelemetryBatch::decode(&batch.encode().unwrap()).unwrap(),
            batch
        );

        let acknowledgement = NativeTelemetryAppendAck {
            partitions: vec![NativePartitionAck {
                topic_partition,
                first_offset: 10,
                last_offset: 11,
            }],
        };
        assert_eq!(
            NativeTelemetryAppendAck::decode(&acknowledgement.encode().unwrap()).unwrap(),
            acknowledgement
        );
    }

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
    fn log_query_result_round_trips_byte_exact_text_and_metadata() {
        let expected = entries();
        let encoded = encode_native_log_query_result("tenant-a", expected.clone())
            .expect("query result encodes");
        let decoded = decode_native_log_query_result(&encoded).expect("query result decodes");
        assert_eq!(decoded.tenant, "tenant-a");
        assert_eq!(decoded.entries, expected);
    }

    #[test]
    fn log_query_result_rejects_truncation_count_mismatch_and_trailing_bytes() {
        let encoded = encode_native_log_query_result("tenant-a", entries()).expect("query result");
        for end in 0..encoded.len() {
            assert!(
                decode_native_log_query_result(&encoded[..end]).is_err(),
                "{end}"
            );
        }
        let mut count = encoded.clone();
        count[8..12].copy_from_slice(&3_u32.to_le_bytes());
        assert!(decode_native_log_query_result(&count).is_err());
        let mut reserved = encoded.clone();
        reserved[12] = 1;
        assert!(decode_native_log_query_result(&reserved).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_native_log_query_result(&trailing).is_err());
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
        invalid[4] = 3;
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
