use std::fmt;

use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::DictionaryId;

/// Result returned by ShardTelemetry operations.
pub type TelemetryResult<T> = Result<T, TelemetryError>;

/// Error returned by ShardTelemetry operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryError {
    /// A required size limit was configured as zero.
    InvalidConfig(&'static str),
    /// A signal-native bounded configuration is invalid.
    InvalidConfiguration(String),
    /// A checksummed STEL envelope is malformed.
    InvalidTelemetryEnvelope(&'static str),
    /// A durable STEL envelope exceeds the protocol safety limit.
    TelemetryEnvelopeTooLarge,
    /// An OTLP trace ID is not exactly 16 nonzero bytes.
    InvalidTraceId,
    /// An OTLP span ID is not exactly 8 nonzero bytes.
    InvalidSpanId,
    /// A metric point violates signal or temporal invariants.
    InvalidMetricSample(String),
    /// Remote Write supplied a different value at an existing timestamp.
    MetricSampleConflict {
        /// Canonical series fingerprint.
        series: u128,
        /// Conflicting sample timestamp.
        timestamp_unix_nanos: u64,
    },
    /// The same shard-stream shard was configured more than once.
    DuplicateStripe(ShardId),
    /// A record was routed to a stripe that is not configured.
    UnknownStripe(ShardId),
    /// A record was sent to a stripe other than its shard-stream owner.
    WrongStripe {
        /// Physical stripe that received the record.
        expected: ShardId,
        /// Physical shard declared by the record.
        observed: ShardId,
    },
    /// A partition append reused or regressed its lane-assigned offset.
    OffsetOutOfOrder {
        /// Partition whose append order was violated.
        partition: TopicPartition,
        /// Required next offset.
        expected: LogicalOffset,
        /// Observed record offset.
        observed: LogicalOffset,
    },
    /// The next offset cannot be represented.
    OffsetExhausted(TopicPartition),
    /// The record was already visible in the hot index.
    DuplicateRecord {
        /// Duplicate record partition.
        partition: TopicPartition,
        /// Duplicate record offset.
        offset: LogicalOffset,
    },
    /// A replay reused an offset with different log content.
    ConflictingRecord {
        /// Conflicting record partition.
        partition: TopicPartition,
        /// Conflicting record offset.
        offset: LogicalOffset,
    },
    /// A requested sealed block does not exist.
    UnknownBlock(u64),
    /// A dictionary payload was empty.
    EmptyDictionary,
    /// A dictionary cannot fit into the configured cache.
    DictionaryTooLarge {
        /// Bytes in the candidate dictionary.
        bytes: usize,
        /// Configured LRU capacity in bytes.
        capacity: usize,
    },
    /// A dictionary ID was reused with different immutable bytes.
    DictionaryIdConflict(DictionaryId),
    /// The immutable dictionary catalog could not be accessed.
    DictionaryCatalogUnavailable,
    /// The real-time dictionary worker is no longer available.
    DictionaryTrainerUnavailable,
    /// The real-time dictionary worker could not be started or trained.
    DictionaryTrainingFailed(String),
    /// A published dictionary required by a block is no longer in its snapshot.
    MissingDictionary(DictionaryId),
    /// A record's encoded block representation cannot fit in its on-disk format.
    RecordTooLarge,
    /// A sealed structural block could not be decoded.
    InvalidBlockEncoding(&'static str),
    /// The selected compression backend rejected a block.
    CompressionFailed(String),
    /// An OTLP protobuf payload could not be decoded as an ExportLogs request.
    InvalidOtlpPayload(String),
    /// A native grouped log payload failed bounded decoding or validation.
    InvalidNativePayload(String),
    /// A lookup contains an invalid regular expression or other predicate.
    InvalidQuery(String),
    /// A stripe-owned query worker is unavailable.
    QueryWorkerUnavailable(String),
    /// A local durable-storage operation failed.
    StorageIo(String),
    /// A committed sink journal frame failed validation.
    CorruptSinkJournal(String),
    /// The bounded sink journal cannot accept another transaction.
    SinkJournalFull {
        /// Bytes that would be occupied by the transaction.
        bytes: u64,
        /// Configured journal capacity.
        capacity: u64,
    },
    /// An object-store operation failed.
    ObjectStore(String),
    /// Tier metadata or immutable object contents failed validation.
    CorruptTier(String),
    /// A conditional catalog publication observed a different current root.
    StaleCatalog {
        /// Object version expected by the publishing writer.
        expected: Option<String>,
        /// Object version observed by the object store.
        observed: Option<String>,
    },
    /// A sealed block no longer has the local payload needed for offload.
    MissingStagedPayload(u64),
}

impl fmt::Display for TelemetryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid configuration: {message}"),
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid telemetry configuration: {message}")
            }
            Self::InvalidTelemetryEnvelope(message) => {
                write!(formatter, "invalid STEL telemetry envelope: {message}")
            }
            Self::TelemetryEnvelopeTooLarge => {
                formatter.write_str("STEL telemetry envelope exceeds the 64 MiB safety limit")
            }
            Self::InvalidTraceId => formatter.write_str("trace ID must be 16 nonzero bytes"),
            Self::InvalidSpanId => formatter.write_str("span ID must be 8 nonzero bytes"),
            Self::InvalidMetricSample(message) => {
                write!(formatter, "invalid metric sample: {message}")
            }
            Self::MetricSampleConflict {
                series,
                timestamp_unix_nanos,
            } => write!(
                formatter,
                "conflicting Remote Write sample for series {series:032x} at {timestamp_unix_nanos}"
            ),
            Self::DuplicateStripe(shard_id) => {
                write!(formatter, "duplicate stripe for shard {shard_id}")
            }
            Self::UnknownStripe(shard_id) => {
                write!(formatter, "unknown stripe for shard {shard_id}")
            }
            Self::WrongStripe { expected, observed } => write!(
                formatter,
                "record for shard {observed} was sent to stripe {expected}"
            ),
            Self::OffsetOutOfOrder {
                partition,
                expected,
                observed,
            } => write!(
                formatter,
                "partition {partition:?} expected offset {expected}, observed {observed}"
            ),
            Self::OffsetExhausted(partition) => {
                write!(
                    formatter,
                    "partition {partition:?} has exhausted its logical offsets"
                )
            }
            Self::DuplicateRecord { partition, offset } => {
                write!(formatter, "duplicate record {partition:?}@{offset}")
            }
            Self::ConflictingRecord { partition, offset } => {
                write!(
                    formatter,
                    "conflicting replay for record {partition:?}@{offset}"
                )
            }
            Self::UnknownBlock(block_id) => write!(formatter, "unknown block {block_id}"),
            Self::EmptyDictionary => formatter.write_str("compression dictionary cannot be empty"),
            Self::DictionaryTooLarge { bytes, capacity } => write!(
                formatter,
                "compression dictionary is {bytes} bytes, exceeding cache capacity {capacity}"
            ),
            Self::DictionaryIdConflict(dictionary_id) => write!(
                formatter,
                "dictionary ID {} was reused with different immutable bytes",
                dictionary_id.get()
            ),
            Self::DictionaryCatalogUnavailable => {
                formatter.write_str("immutable dictionary catalog is unavailable")
            }
            Self::DictionaryTrainerUnavailable => {
                formatter.write_str("real-time dictionary trainer is unavailable")
            }
            Self::DictionaryTrainingFailed(message) => {
                write!(formatter, "real-time dictionary training failed: {message}")
            }
            Self::MissingDictionary(dictionary_id) => write!(
                formatter,
                "dictionary {} is missing from the stripe snapshot",
                dictionary_id.get()
            ),
            Self::RecordTooLarge => {
                formatter.write_str("log record cannot fit in the block encoding")
            }
            Self::InvalidBlockEncoding(message) => {
                write!(formatter, "invalid structural block encoding: {message}")
            }
            Self::CompressionFailed(message) => {
                write!(formatter, "block compression failed: {message}")
            }
            Self::InvalidOtlpPayload(message) => {
                write!(formatter, "invalid OTLP Logs protobuf payload: {message}")
            }
            Self::InvalidNativePayload(message) => {
                write!(formatter, "invalid native log payload: {message}")
            }
            Self::InvalidQuery(message) => {
                write!(formatter, "invalid log query: {message}")
            }
            Self::QueryWorkerUnavailable(message) => {
                write!(formatter, "query worker is unavailable: {message}")
            }
            Self::StorageIo(message) => {
                write!(formatter, "durable storage operation failed: {message}")
            }
            Self::CorruptSinkJournal(message) => {
                write!(formatter, "sink journal is corrupt: {message}")
            }
            Self::SinkJournalFull { bytes, capacity } => write!(
                formatter,
                "sink journal requires {bytes} bytes, exceeding capacity {capacity}"
            ),
            Self::ObjectStore(message) => {
                write!(formatter, "object-store operation failed: {message}")
            }
            Self::CorruptTier(message) => {
                write!(formatter, "corrupt tier metadata or object: {message}")
            }
            Self::StaleCatalog { expected, observed } => write!(
                formatter,
                "conditional catalog publication failed: expected object version {expected:?}, observed {observed:?}"
            ),
            Self::MissingStagedPayload(block_id) => {
                write!(formatter, "sealed block {block_id} has no staged payload")
            }
        }
    }
}

impl std::error::Error for TelemetryError {}
