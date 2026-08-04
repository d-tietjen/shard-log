use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use shard_stream_core::{ShardId, TopicPartition};

use crate::{
    BlockCatalog, BlockDescriptor, BlockId, CompressionCodec, CorrelationBlockFilter,
    TelemetryError, TelemetryResult, TelemetrySignal,
};

const TIER_FORMAT_VERSION: u8 = 1;
const CHECKSUM_ALGORITHM: &str = "blake3";
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const POINTER_READ_LIMIT: u64 = 64 * 1024;
const CACHE_HEADER_MAGIC: &[u8; 8] = b"SLCACHE1";
const SIGNAL_INDEX_MAGIC: &[u8; 4] = b"STSI";
const SIGNAL_INDEX_HEADER_BYTES: usize = 16;
const CACHE_HEADER_BYTES: usize = 8 + 8 + 32;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Metadata returned for one object-store object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// Exact object length in bytes.
    pub bytes: u64,
    /// Opaque object-store version used for conditional publication.
    ///
    /// This is deliberately not treated as a content checksum. For example,
    /// an S3 multipart ETag is a version token but is not a BLAKE3 digest.
    pub version_token: String,
    /// Lowercase BLAKE3 digest calculated over the complete object contents.
    pub content_digest: String,
}

/// Minimal object-store contract needed by the ShardTelemetry tier.
///
/// Immutable data uses put-if-absent operations. Only the small `CURRENT`
/// pointer is mutable, and it is replaced with an object-version
/// compare-and-swap.
pub trait TelemetryObjectStore: Send + Sync {
    /// Creates an immutable object from bytes, or verifies an identical retry.
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata>;

    /// Creates an immutable object from a local file without buffering it all.
    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata>;

    /// Reads an entire object subject to a caller-provided allocation limit.
    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>>;

    /// Reads exactly one byte range from an object.
    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>>;

    /// Returns object metadata, or `None` when the key is absent.
    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>>;

    /// Conditionally replaces a small mutable object.
    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata>;
}

/// Cloneable type-erased object-store handle used by production stripe owners.
#[derive(Clone)]
pub struct SharedTelemetryObjectStore(Arc<dyn TelemetryObjectStore>);

impl SharedTelemetryObjectStore {
    /// Wraps an object-store adapter for use by independently owned stripes.
    #[must_use]
    pub fn new(store: Arc<dyn TelemetryObjectStore>) -> Self {
        Self(store)
    }
}

impl std::fmt::Debug for SharedTelemetryObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SharedTelemetryObjectStore(..)")
    }
}

impl From<LocalObjectStore> for SharedTelemetryObjectStore {
    fn from(store: LocalObjectStore) -> Self {
        Self(Arc::new(store))
    }
}

impl TelemetryObjectStore for SharedTelemetryObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        self.0.put_bytes_if_absent(key, bytes)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        self.0.put_file_if_absent(key, source)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        self.0.get(key, max_bytes)
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        self.0.get_range(key, range)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        self.0.head(key)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        self.0.compare_and_swap(key, expected_version, bytes)
    }
}

/// Filesystem implementation of [`TelemetryObjectStore`] used for local operation
/// and deterministic testing of S3-style immutable publication.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Opens or creates a local object-store root.
    pub fn open(root: impl AsRef<Path>) -> TelemetryResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .map_err(|error| storage_io("create local object-store root", error))?;
        Ok(Self { root })
    }

    /// Returns the backing filesystem root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn object_path(&self, key: &str) -> TelemetryResult<PathBuf> {
        validate_object_key(key)?;
        Ok(self.root.join(key))
    }

    fn update_lock(&self) -> TelemetryResult<File> {
        let path = self.root.join(".shard-telemetry-object-store.lock");
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|error| storage_io("open object-store update lock", error))?;
        FileExt::lock_exclusive(&lock)
            .map_err(|error| storage_io("lock object-store update lock", error))?;
        Ok(lock)
    }
}

impl TelemetryObjectStore for LocalObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let expected = metadata_for_bytes(bytes);
        if let Some(observed) = metadata_for_path_if_present(&path)? {
            unlock_file(&lock)?;
            if observed == expected {
                return Ok(observed);
            }
            return Err(TelemetryError::ObjectStore(format!(
                "immutable object key {key} already contains different bytes"
            )));
        }
        write_bytes_atomically(&path, bytes)?;
        unlock_file(&lock)?;
        Ok(expected)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let source_metadata = source
            .metadata()
            .map_err(|error| storage_io("inspect immutable object source", error))?;
        if !source_metadata.is_file() || source_metadata.len() == 0 {
            return Err(TelemetryError::ObjectStore(
                "immutable object source must be a nonempty regular file".into(),
            ));
        }
        let lock = self.update_lock()?;
        let expected = hash_file(source)?;
        if let Some(observed) = metadata_for_path_if_present(&path)? {
            unlock_file(&lock)?;
            if observed == expected {
                return Ok(observed);
            }
            return Err(TelemetryError::ObjectStore(format!(
                "immutable object key {key} already contains different bytes"
            )));
        }
        let copied = copy_file_atomically(source, &path)?;
        unlock_file(&lock)?;
        if copied != expected {
            return Err(TelemetryError::CorruptTier(
                "object source changed while it was copied".into(),
            ));
        }
        Ok(copied)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        let path = self.object_path(key)?;
        let metadata = path
            .metadata()
            .map_err(|error| object_io(key, "inspect", error))?;
        if metadata.len() > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "object {key} is {} bytes, exceeding read limit {max_bytes}",
                metadata.len()
            )));
        }
        fs::read(path).map_err(|error| object_io(key, "read", error))
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end {
            return Err(TelemetryError::ObjectStore(
                "object byte range starts after its end".into(),
            ));
        }
        let path = self.object_path(key)?;
        let mut file = File::open(path).map_err(|error| object_io(key, "open", error))?;
        let object_bytes = file
            .metadata()
            .map_err(|error| object_io(key, "inspect", error))?
            .len();
        if range.end > object_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "object range {}..{} exceeds {key} length {object_bytes}",
                range.start, range.end
            )));
        }
        let bytes = usize::try_from(range.end - range.start).map_err(|_| {
            TelemetryError::ObjectStore("object byte range cannot fit in memory".into())
        })?;
        file.seek(SeekFrom::Start(range.start))
            .map_err(|error| object_io(key, "seek", error))?;
        let mut output = vec![0; bytes];
        file.read_exact(&mut output)
            .map_err(|error| object_io(key, "read range", error))?;
        Ok(output)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        let path = self.object_path(key)?;
        metadata_for_path_if_present(&path)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let observed = metadata_for_path_if_present(&path)?;
        if observed
            .as_ref()
            .map(|metadata| metadata.version_token.as_str())
            != expected_version
        {
            unlock_file(&lock)?;
            return Err(TelemetryError::StaleCatalog {
                expected: expected_version.map(str::to_owned),
                observed: observed.map(|metadata| metadata.version_token),
            });
        }
        write_bytes_atomically(&path, bytes)?;
        unlock_file(&lock)?;
        Ok(metadata_for_bytes(bytes))
    }
}

/// Kind of immutable artifact attached to one block group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierArtifactKind {
    /// Concatenated compressed block payloads.
    PayloadPack,
    /// Independent persistent query-index segment for the group.
    QueryIndex,
    /// Immutable compression dictionary payload.
    Dictionary,
    /// Immutable placement-to-dictionary assignment catalog.
    DictionaryCatalog,
}

impl TierArtifactKind {
    fn key_name(self) -> &'static str {
        match self {
            Self::PayloadPack => "payload",
            Self::QueryIndex => "query-index",
            Self::Dictionary => "dictionary",
            Self::DictionaryCatalog => "dictionary-catalog",
        }
    }
}

/// Local source file to publish as one immutable group artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierArtifactSource {
    /// Artifact role.
    pub kind: TierArtifactKind,
    /// Stable, path-free artifact name.
    pub name: String,
    /// Local sealed file to upload.
    pub path: PathBuf,
}

/// Immutable object metadata for one published group artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierArtifact {
    /// Artifact role.
    pub kind: TierArtifactKind,
    /// Stable, path-free artifact name.
    pub name: String,
    /// Immutable object-store key.
    pub object_key: String,
    /// Exact object length.
    pub bytes: u64,
    /// Checksum algorithm, currently BLAKE3.
    pub checksum_algorithm: String,
    /// Lowercase BLAKE3 checksum.
    pub checksum: String,
}

/// Durable block metadata and payload extent inside a block-group pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierBlockEntry {
    /// Stripe-local block identifier.
    pub block_id: u64,
    /// Source compression cohort.
    pub source_compression_cohort: u64,
    /// Final compression placement.
    pub placement_id: u64,
    /// Optional immutable dictionary identifier as a decimal `u128`.
    pub dictionary_id: Option<String>,
    /// Compression codec name.
    pub compression_codec: String,
    /// Compression level.
    pub compression_level: i32,
    /// Lowest durable logical offset in the block.
    pub first_offset: u64,
    /// Highest durable logical offset in the block.
    pub last_offset: u64,
    /// Number of records in the block.
    pub record_count: u32,
    /// Raw source bytes represented by the block.
    pub source_bytes: u64,
    /// Structural bytes before byte compression.
    pub structural_bytes: u64,
    /// Stored compressed bytes.
    pub stored_bytes: u64,
    /// Lowest event timestamp in the block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp in the block.
    pub max_timestamp_unix_nanos: u64,
    /// Block compression temperature.
    pub compression_temperature: u16,
    /// Representative template shape.
    pub compression_shape_hash: u64,
    /// Internal temperature variance in Q8.
    pub compression_temperature_variance_q8: u16,
    /// Maximum record-to-block temperature deviation.
    pub max_compression_temperature_deviation: u8,
    /// Byte offset in the payload-pack artifact.
    pub payload_offset: u64,
    /// Byte length in the payload-pack artifact.
    pub payload_bytes: u64,
    /// Lowercase BLAKE3 checksum of this block's compressed bytes.
    pub payload_checksum: String,
    /// Lowest trace ID or metric-series fingerprint represented by this block.
    pub min_signal_identity: Option<u128>,
    /// Highest trace ID or metric-series fingerprint represented by this block.
    pub max_signal_identity: Option<u128>,
    /// Compact shared-identity filter for cold cross-signal lookup.
    pub correlation_filter: Option<CorrelationBlockFilter>,
}

/// Durable sink watermark covered by an immutable object-tier group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierCheckpoint {
    /// First placement sequence not represented by this or an older group.
    pub next_placement_sequence: u64,
    /// First logical offset not represented by this or an older group.
    pub next_offset: u64,
}

impl TierCheckpoint {
    fn covers(self, other: Self) -> bool {
        self.next_placement_sequence >= other.next_placement_sequence
            && self.next_offset >= other.next_offset
    }
}

impl TierBlockEntry {
    fn from_descriptor(
        descriptor: &BlockDescriptor,
        payload_offset: u64,
        payload_checksum: String,
    ) -> Self {
        Self {
            block_id: descriptor.block_id.get(),
            source_compression_cohort: descriptor.source_compression_cohort.get(),
            placement_id: descriptor.placement_id.get(),
            dictionary_id: descriptor
                .dictionary_id
                .map(|dictionary_id| dictionary_id.get().to_string()),
            compression_codec: match descriptor.compression_codec {
                CompressionCodec::Zstd => "zstd".into(),
            },
            compression_level: descriptor.compression_level,
            first_offset: descriptor.first_offset.get(),
            last_offset: descriptor.last_offset.get(),
            record_count: descriptor.record_count,
            source_bytes: descriptor.source_bytes,
            structural_bytes: descriptor.structural_bytes,
            stored_bytes: descriptor.stored_bytes,
            min_timestamp_unix_nanos: descriptor.min_timestamp_unix_nanos,
            max_timestamp_unix_nanos: descriptor.max_timestamp_unix_nanos,
            compression_temperature: descriptor.compression_temperature,
            compression_shape_hash: descriptor.compression_shape_hash,
            compression_temperature_variance_q8: descriptor.compression_temperature_variance_q8,
            max_compression_temperature_deviation: descriptor.max_compression_temperature_deviation,
            payload_offset,
            payload_bytes: descriptor.stored_bytes,
            payload_checksum,
            min_signal_identity: None,
            max_signal_identity: None,
            correlation_filter: None,
        }
    }

    /// Creates catalog metadata for one signal-native trace block or metric chunk.
    #[allow(clippy::too_many_arguments)]
    pub fn for_signal_payload(
        signal: TelemetrySignal,
        block_id: u64,
        min_signal_identity: u128,
        max_signal_identity: u128,
        first_offset: u64,
        last_offset: u64,
        record_count: u32,
        min_timestamp_unix_nanos: u64,
        max_timestamp_unix_nanos: u64,
        payload_offset: u64,
        payload_bytes: u64,
        payload_checksum: String,
        correlation_filter: CorrelationBlockFilter,
    ) -> TelemetryResult<Self> {
        if !matches!(signal, TelemetrySignal::Traces | TelemetrySignal::Metrics) {
            return Err(TelemetryError::InvalidConfig(
                "signal-native tier payload must be a trace block or metric chunk",
            ));
        }
        Ok(Self {
            block_id,
            source_compression_cohort: 0,
            placement_id: 0,
            dictionary_id: None,
            compression_codec: match signal {
                TelemetrySignal::Traces => "trace-native".into(),
                TelemetrySignal::Metrics => "metric-native".into(),
                TelemetrySignal::Logs => unreachable!("validated above"),
            },
            compression_level: 0,
            first_offset,
            last_offset,
            record_count,
            source_bytes: payload_bytes,
            structural_bytes: payload_bytes,
            stored_bytes: payload_bytes,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            compression_temperature: 0,
            compression_shape_hash: 0,
            compression_temperature_variance_q8: 0,
            max_compression_temperature_deviation: 0,
            payload_offset,
            payload_bytes,
            payload_checksum,
            min_signal_identity: Some(min_signal_identity),
            max_signal_identity: Some(max_signal_identity),
            correlation_filter: Some(correlation_filter),
        })
    }

    fn validate(&self, payload_bytes: u64) -> TelemetryResult<()> {
        if self.first_offset > self.last_offset
            || self.min_timestamp_unix_nanos > self.max_timestamp_unix_nanos
            || self.record_count == 0
            || self.stored_bytes == 0
            || self.payload_bytes != self.stored_bytes
            || !matches!(
                self.compression_codec.as_str(),
                "zstd" | "trace-native" | "metric-native"
            )
            || self.min_signal_identity.is_some() != self.max_signal_identity.is_some()
            || self
                .min_signal_identity
                .zip(self.max_signal_identity)
                .is_some_and(|(minimum, maximum)| minimum > maximum)
            || (self.compression_codec == "zstd") != self.min_signal_identity.is_none()
            || (self.compression_codec == "zstd") != self.correlation_filter.is_none()
            || !valid_checksum(&self.payload_checksum)
            || self
                .payload_offset
                .checked_add(self.payload_bytes)
                .is_none_or(|end| end > payload_bytes)
            || self
                .dictionary_id
                .as_ref()
                .is_some_and(|value| value.parse::<u128>().is_err())
        {
            return Err(TelemetryError::CorruptTier(
                "group contains invalid block metadata".into(),
            ));
        }
        Ok(())
    }
}

/// Complete local input needed to publish one block group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierGroupSource {
    /// Monotonic sequence within one physical-shard partition namespace.
    pub group_sequence: u64,
    /// Durable sink watermark covered after this complete group is published.
    pub checkpoint: TierCheckpoint,
    /// Block payload extents in pack order.
    pub blocks: Vec<TierBlockEntry>,
    /// Sealed local artifacts, including exactly one payload and query index.
    pub artifacts: Vec<TierArtifactSource>,
}

/// One already encoded trace block or metric chunk awaiting immutable publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalTierPayload {
    /// Stripe-local resident identifier used only to retire memory after publication.
    pub resident_id: u64,
    /// Signal partition whose catalog owns this payload.
    pub topic_partition: TopicPartition,
    /// Lowest trace ID or canonical metric-series fingerprint in the payload.
    pub min_signal_identity: u128,
    /// Highest trace ID or canonical metric-series fingerprint in the payload.
    pub max_signal_identity: u128,
    /// First durable offset represented by the payload.
    pub first_offset: u64,
    /// Last durable offset represented by the payload.
    pub last_offset: u64,
    /// Number of spans or points represented by the payload.
    pub record_count: u32,
    /// Lowest signal timestamp represented by the payload.
    pub min_timestamp_unix_nanos: u64,
    /// Highest signal timestamp represented by the payload.
    pub max_timestamp_unix_nanos: u64,
    /// Complete self-verifying signal-native bytes.
    pub payload: Arc<[u8]>,
    /// Compact shared-identity filter computed before the mutable head is released.
    pub correlation_filter: CorrelationBlockFilter,
}

/// Stages one complete trace or metric object-tier group.
#[allow(clippy::too_many_arguments)]
pub fn stage_signal_group(
    spool_directory: impl AsRef<Path>,
    file_stem: &str,
    signal: TelemetrySignal,
    group_sequence: u64,
    first_block_id: u64,
    checkpoint: TierCheckpoint,
    payloads: &[SignalTierPayload],
    recovery_state: &[u8],
) -> TelemetryResult<TierGroupSource> {
    if payloads.is_empty() || !matches!(signal, TelemetrySignal::Traces | TelemetrySignal::Metrics)
    {
        return Err(TelemetryError::ObjectStore(
            "signal group requires trace or metric payloads".into(),
        ));
    }
    validate_artifact_name(file_stem)?;
    let directory = spool_directory.as_ref();
    fs::create_dir_all(directory).map_err(|error| storage_io("create signal tier spool", error))?;
    let payload_path = directory.join(format!("{file_stem}.payload"));
    let index_path = directory.join(format!("{file_stem}.index"));
    let mut payload_pack = Vec::new();
    let mut blocks = Vec::with_capacity(payloads.len());
    let topic_partition = payloads[0].topic_partition;
    for (ordinal, payload) in payloads.iter().enumerate() {
        if payload.resident_id == 0
            || payload.topic_partition != topic_partition
            || payload.topic_partition.topic_id != signal.topic_id()
            || payload.payload.is_empty()
            || payload.record_count == 0
            || payload.first_offset > payload.last_offset
            || payload.max_timestamp_unix_nanos < payload.min_timestamp_unix_nanos
            || payload.last_offset >= checkpoint.next_offset
        {
            return Err(TelemetryError::ObjectStore(
                "signal tier payload has invalid bounds".into(),
            ));
        }
        let payload_offset =
            u64::try_from(payload_pack.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let payload_bytes =
            u64::try_from(payload.payload.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        payload_pack.extend_from_slice(&payload.payload);
        blocks.push(TierBlockEntry::for_signal_payload(
            signal,
            first_block_id
                .checked_add(u64::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?)
                .ok_or(TelemetryError::RecordTooLarge)?,
            payload.min_signal_identity,
            payload.max_signal_identity,
            payload.first_offset,
            payload.last_offset,
            payload.record_count,
            payload.min_timestamp_unix_nanos,
            payload.max_timestamp_unix_nanos,
            payload_offset,
            payload_bytes,
            checksum_bytes(&payload.payload),
            payload.correlation_filter.clone(),
        )?);
    }
    let index = encode_signal_index(signal, recovery_state)?;
    write_bytes_atomically(&payload_path, &payload_pack)?;
    write_bytes_atomically(&index_path, &index)?;
    Ok(TierGroupSource {
        group_sequence,
        checkpoint,
        blocks,
        artifacts: vec![
            TierArtifactSource {
                kind: TierArtifactKind::PayloadPack,
                name: "signal.payload".into(),
                path: payload_path,
            },
            TierArtifactSource {
                kind: TierArtifactKind::QueryIndex,
                name: "signal.index".into(),
                path: index_path,
            },
        ],
    })
}

fn encode_signal_index(signal: TelemetrySignal, recovery_state: &[u8]) -> TelemetryResult<Vec<u8>> {
    if !matches!(signal, TelemetrySignal::Traces | TelemetrySignal::Metrics) {
        return Err(TelemetryError::ObjectStore(
            "signal index requires traces or metrics".into(),
        ));
    }
    let recovery_bytes =
        u64::try_from(recovery_state.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    let mut encoded = Vec::with_capacity(
        SIGNAL_INDEX_HEADER_BYTES
            .saturating_add(recovery_state.len())
            .saturating_add(32),
    );
    encoded.extend_from_slice(SIGNAL_INDEX_MAGIC);
    encoded.push(signal as u8);
    encoded.extend_from_slice(&[0; 3]);
    encoded.extend_from_slice(&recovery_bytes.to_le_bytes());
    encoded.extend_from_slice(recovery_state);
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

/// Decodes the signal-local recovery snapshot from the current pre-release index format.
pub fn decode_signal_recovery_state(
    encoded: &[u8],
    expected_signal: TelemetrySignal,
) -> TelemetryResult<Vec<u8>> {
    if encoded.len() < SIGNAL_INDEX_HEADER_BYTES + 32
        || &encoded[..4] != SIGNAL_INDEX_MAGIC
        || encoded[4] != expected_signal as u8
        || encoded[5..8] != [0; 3]
    {
        return Err(TelemetryError::CorruptTier(
            "invalid signal recovery index header".into(),
        ));
    }
    let payload_end = encoded.len() - 32;
    if blake3::hash(&encoded[..payload_end]).as_bytes() != &encoded[payload_end..] {
        return Err(TelemetryError::CorruptTier(
            "signal recovery index checksum failed".into(),
        ));
    }
    let recovery_bytes = usize::try_from(u64::from_le_bytes(
        encoded[8..16]
            .try_into()
            .expect("fixed signal index length"),
    ))
    .map_err(|_| TelemetryError::CorruptTier("signal recovery index is too large".into()))?;
    if SIGNAL_INDEX_HEADER_BYTES
        .checked_add(recovery_bytes)
        .is_none_or(|end| end != payload_end)
    {
        return Err(TelemetryError::CorruptTier(
            "signal recovery index length mismatch".into(),
        ));
    }
    Ok(encoded[SIGNAL_INDEX_HEADER_BYTES..payload_end].to_vec())
}

/// Immutable manifest for one independently queryable block group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierGroupManifest {
    /// Storage-format version.
    pub format_version: u8,
    /// Monotonic group sequence.
    pub group_sequence: u64,
    /// Durable sink watermark covered by this complete group.
    pub checkpoint: TierCheckpoint,
    /// Owning physical shard.
    pub shard_id: u32,
    /// Logical topic as a decimal `u128`.
    pub topic_id: String,
    /// Logical partition.
    pub partition_id: u32,
    /// Ordered compressed blocks.
    pub blocks: Vec<TierBlockEntry>,
    /// Immutable payload, query-index, and dictionary artifacts.
    pub artifacts: Vec<TierArtifact>,
}

impl TierGroupManifest {
    /// Returns the artifact with the requested role.
    #[must_use]
    pub fn artifact(&self, kind: TierArtifactKind) -> Option<&TierArtifact> {
        self.artifacts.iter().find(|artifact| artifact.kind == kind)
    }

    fn validate(
        &self,
        shard_id: ShardId,
        partition: TopicPartition,
        max_blocks_per_group: usize,
        max_group_payload_bytes: u64,
    ) -> TelemetryResult<()> {
        if self.format_version != TIER_FORMAT_VERSION
            || self.shard_id != shard_id.get()
            || self.topic_id != partition.topic_id.get().to_string()
            || self.partition_id != partition.partition_id.get()
            || self.blocks.is_empty()
            || self.blocks.len() > max_blocks_per_group
            || self.artifacts.is_empty()
            || self
                .blocks
                .iter()
                .any(|block| block.last_offset >= self.checkpoint.next_offset)
        {
            return Err(TelemetryError::CorruptTier(
                "group manifest identity or cardinality is invalid".into(),
            ));
        }
        let payloads = self
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == TierArtifactKind::PayloadPack)
            .count();
        let indexes = self
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == TierArtifactKind::QueryIndex)
            .count();
        if payloads != 1 || indexes != 1 {
            return Err(TelemetryError::CorruptTier(
                "group requires exactly one payload and one query-index artifact".into(),
            ));
        }
        for artifact in &self.artifacts {
            validate_artifact(artifact)?;
        }
        if self.artifacts.iter().enumerate().any(|(index, artifact)| {
            self.artifacts[index + 1..]
                .iter()
                .any(|other| artifact.kind == other.kind && artifact.name == other.name)
        }) {
            return Err(TelemetryError::CorruptTier(
                "group contains duplicate artifact names".into(),
            ));
        }
        let payload_bytes = self
            .artifact(TierArtifactKind::PayloadPack)
            .expect("payload cardinality was checked")
            .bytes;
        if payload_bytes > max_group_payload_bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "group payload is {payload_bytes} bytes, exceeding limit {max_group_payload_bytes}"
            )));
        }
        let mut previous_block = None;
        let mut previous_payload_end = 0;
        for block in &self.blocks {
            block.validate(payload_bytes)?;
            if previous_block.is_some_and(|previous| previous >= block.block_id)
                || block.payload_offset < previous_payload_end
            {
                return Err(TelemetryError::CorruptTier(
                    "group blocks are not strictly ordered".into(),
                ));
            }
            previous_block = Some(block.block_id);
            previous_payload_end = block.payload_offset + block.payload_bytes;
        }
        Ok(())
    }
}

/// Bounded catalog entry pointing to one immutable group manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogGroupEntry {
    /// Group sequence.
    pub group_sequence: u64,
    /// Durable sink watermark covered by this group.
    pub checkpoint: TierCheckpoint,
    /// Immutable group-manifest object key.
    pub manifest_key: String,
    /// Group-manifest length.
    pub manifest_bytes: u64,
    /// Group-manifest BLAKE3 checksum.
    pub manifest_checksum: String,
    /// First logical offset covered by any block.
    pub first_offset: u64,
    /// Last logical offset covered by any block.
    pub last_offset: u64,
    /// Lowest event timestamp in any block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp in any block.
    pub max_timestamp_unix_nanos: u64,
    /// Number of blocks in the group.
    pub block_count: u32,
    /// Total compressed payload bytes represented by the group.
    pub payload_bytes: u64,
    /// Lowest optional trace ID or series fingerprint represented by the group.
    pub min_signal_identity: Option<u128>,
    /// Highest optional trace ID or series fingerprint represented by the group.
    pub max_signal_identity: Option<u128>,
}

impl CatalogGroupEntry {
    fn from_manifest(
        manifest: &TierGroupManifest,
        manifest_key: String,
        metadata: &ObjectMetadata,
    ) -> TelemetryResult<Self> {
        let first_offset = manifest
            .blocks
            .iter()
            .map(|block| block.first_offset)
            .min()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no block bounds".into()))?;
        let last_offset = manifest
            .blocks
            .iter()
            .map(|block| block.last_offset)
            .max()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no block bounds".into()))?;
        let min_timestamp_unix_nanos = manifest
            .blocks
            .iter()
            .map(|block| block.min_timestamp_unix_nanos)
            .min()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no time bounds".into()))?;
        let max_timestamp_unix_nanos = manifest
            .blocks
            .iter()
            .map(|block| block.max_timestamp_unix_nanos)
            .max()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no time bounds".into()))?;
        let block_count = u32::try_from(manifest.blocks.len())
            .map_err(|_| TelemetryError::CorruptTier("group has too many blocks".into()))?;
        let payload_bytes = manifest
            .blocks
            .iter()
            .try_fold(0u64, |total, block| total.checked_add(block.payload_bytes))
            .ok_or_else(|| TelemetryError::CorruptTier("group payload bytes overflow".into()))?;
        let min_signal_identity = manifest
            .blocks
            .iter()
            .filter_map(|block| block.min_signal_identity)
            .min();
        let max_signal_identity = manifest
            .blocks
            .iter()
            .filter_map(|block| block.max_signal_identity)
            .max();
        Ok(Self {
            group_sequence: manifest.group_sequence,
            checkpoint: manifest.checkpoint,
            manifest_key,
            manifest_bytes: metadata.bytes,
            manifest_checksum: metadata.content_digest.clone(),
            first_offset,
            last_offset,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            block_count,
            payload_bytes,
            min_signal_identity,
            max_signal_identity,
        })
    }

    fn validate(&self) -> TelemetryResult<()> {
        validate_object_key(&self.manifest_key)?;
        if self.manifest_bytes == 0
            || !valid_checksum(&self.manifest_checksum)
            || self.first_offset > self.last_offset
            || self.min_timestamp_unix_nanos > self.max_timestamp_unix_nanos
            || self.block_count == 0
            || self.payload_bytes == 0
            || self.last_offset >= self.checkpoint.next_offset
            || self.min_signal_identity.is_some() != self.max_signal_identity.is_some()
            || self
                .min_signal_identity
                .zip(self.max_signal_identity)
                .is_some_and(|(min, max)| min > max)
        {
            return Err(TelemetryError::CorruptTier(
                "catalog contains an invalid group entry".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable bounded page of group catalog entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPage {
    /// Storage-format version.
    pub format_version: u8,
    /// Page sequence in this partition namespace.
    pub page_sequence: u64,
    /// Owning physical shard.
    pub shard_id: u32,
    /// Logical topic as a decimal `u128`.
    pub topic_id: String,
    /// Logical partition.
    pub partition_id: u32,
    /// Ordered group entries.
    pub groups: Vec<CatalogGroupEntry>,
}

impl CatalogPage {
    fn validate(
        &self,
        shard_id: ShardId,
        partition: TopicPartition,
        groups_per_page: usize,
    ) -> TelemetryResult<()> {
        if self.format_version != TIER_FORMAT_VERSION
            || self.shard_id != shard_id.get()
            || self.topic_id != partition.topic_id.get().to_string()
            || self.partition_id != partition.partition_id.get()
            || self.groups.is_empty()
            || self.groups.len() > groups_per_page
        {
            return Err(TelemetryError::CorruptTier(
                "catalog page identity or cardinality is invalid".into(),
            ));
        }
        let mut previous = None;
        let mut previous_checkpoint = None;
        for group in &self.groups {
            group.validate()?;
            if previous.is_some_and(|sequence| sequence >= group.group_sequence)
                || previous_checkpoint
                    .is_some_and(|checkpoint: TierCheckpoint| !group.checkpoint.covers(checkpoint))
            {
                return Err(TelemetryError::CorruptTier(
                    "catalog group sequences or checkpoints are not increasing".into(),
                ));
            }
            previous = Some(group.group_sequence);
            previous_checkpoint = Some(group.checkpoint);
        }
        Ok(())
    }
}

/// Root-level coarse bounds and immutable pointer for one catalog page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPageRef {
    /// Page sequence.
    pub page_sequence: u64,
    /// Immutable page object key.
    pub page_key: String,
    /// Page object length.
    pub page_bytes: u64,
    /// Page object BLAKE3 checksum.
    pub page_checksum: String,
    /// First group sequence in the page.
    pub first_group_sequence: u64,
    /// Last group sequence in the page.
    pub last_group_sequence: u64,
    /// Latest durable sink watermark covered by the page.
    pub last_checkpoint: TierCheckpoint,
    /// Number of group entries.
    pub group_count: u32,
    /// Lowest logical offset covered by the page.
    pub first_offset: u64,
    /// Highest logical offset covered by the page.
    pub last_offset: u64,
    /// Lowest event timestamp covered by the page.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp covered by the page.
    pub max_timestamp_unix_nanos: u64,
    /// Lowest optional trace ID or series fingerprint covered by the page.
    pub min_signal_identity: Option<u128>,
    /// Highest optional trace ID or series fingerprint covered by the page.
    pub max_signal_identity: Option<u128>,
}

impl CatalogPageRef {
    fn from_page(
        page: &CatalogPage,
        page_key: String,
        metadata: &ObjectMetadata,
    ) -> TelemetryResult<Self> {
        let first = page
            .groups
            .first()
            .ok_or_else(|| TelemetryError::CorruptTier("catalog page is empty".into()))?;
        let last = page
            .groups
            .last()
            .ok_or_else(|| TelemetryError::CorruptTier("catalog page is empty".into()))?;
        Ok(Self {
            page_sequence: page.page_sequence,
            page_key,
            page_bytes: metadata.bytes,
            page_checksum: metadata.content_digest.clone(),
            first_group_sequence: first.group_sequence,
            last_group_sequence: last.group_sequence,
            last_checkpoint: last.checkpoint,
            group_count: u32::try_from(page.groups.len())
                .map_err(|_| TelemetryError::CorruptTier("catalog page is too large".into()))?,
            first_offset: page
                .groups
                .iter()
                .map(|group| group.first_offset)
                .min()
                .expect("page is nonempty"),
            last_offset: page
                .groups
                .iter()
                .map(|group| group.last_offset)
                .max()
                .expect("page is nonempty"),
            min_timestamp_unix_nanos: page
                .groups
                .iter()
                .map(|group| group.min_timestamp_unix_nanos)
                .min()
                .expect("page is nonempty"),
            max_timestamp_unix_nanos: page
                .groups
                .iter()
                .map(|group| group.max_timestamp_unix_nanos)
                .max()
                .expect("page is nonempty"),
            min_signal_identity: page
                .groups
                .iter()
                .filter_map(|group| group.min_signal_identity)
                .min(),
            max_signal_identity: page
                .groups
                .iter()
                .filter_map(|group| group.max_signal_identity)
                .max(),
        })
    }

    fn validate(&self) -> TelemetryResult<()> {
        validate_object_key(&self.page_key)?;
        if self.page_bytes == 0
            || !valid_checksum(&self.page_checksum)
            || self.first_group_sequence > self.last_group_sequence
            || self.group_count == 0
            || self.first_offset > self.last_offset
            || self.min_timestamp_unix_nanos > self.max_timestamp_unix_nanos
            || self.last_offset >= self.last_checkpoint.next_offset
            || self.min_signal_identity.is_some() != self.max_signal_identity.is_some()
            || self
                .min_signal_identity
                .zip(self.max_signal_identity)
                .is_some_and(|(min, max)| min > max)
        {
            return Err(TelemetryError::CorruptTier(
                "catalog root contains an invalid page reference".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable catalog root for one physical-shard logical-partition pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogRoot {
    /// Storage-format version.
    pub format_version: u8,
    /// Monotonically increasing publication generation.
    pub generation: u64,
    /// Owning physical shard.
    pub shard_id: u32,
    /// Logical topic as a decimal `u128`.
    pub topic_id: String,
    /// Logical partition.
    pub partition_id: u32,
    /// Latest durable sink watermark selected by this root.
    pub latest_checkpoint: Option<TierCheckpoint>,
    /// First block identifier not used by any published group.
    pub next_block_id: u64,
    /// Ordered immutable catalog pages.
    pub pages: Vec<CatalogPageRef>,
}

impl CatalogRoot {
    fn empty(shard_id: ShardId, partition: TopicPartition) -> Self {
        Self {
            format_version: TIER_FORMAT_VERSION,
            generation: 0,
            shard_id: shard_id.get(),
            topic_id: partition.topic_id.get().to_string(),
            partition_id: partition.partition_id.get(),
            latest_checkpoint: None,
            next_block_id: 0,
            pages: Vec::new(),
        }
    }

    fn validate(&self, shard_id: ShardId, partition: TopicPartition) -> TelemetryResult<()> {
        if self.format_version != TIER_FORMAT_VERSION
            || self.shard_id != shard_id.get()
            || self.topic_id != partition.topic_id.get().to_string()
            || self.partition_id != partition.partition_id.get()
        {
            return Err(TelemetryError::CorruptTier(
                "catalog root belongs to a different namespace".into(),
            ));
        }
        let mut previous_page = None;
        let mut previous_group = None;
        let mut previous_checkpoint = None;
        for page in &self.pages {
            page.validate()?;
            if previous_page.is_some_and(|sequence| sequence >= page.page_sequence)
                || previous_group.is_some_and(|sequence| sequence >= page.first_group_sequence)
                || previous_checkpoint.is_some_and(|checkpoint: TierCheckpoint| {
                    !page.last_checkpoint.covers(checkpoint)
                })
            {
                return Err(TelemetryError::CorruptTier(
                    "catalog root pages or checkpoints are not strictly increasing".into(),
                ));
            }
            previous_page = Some(page.page_sequence);
            previous_group = Some(page.last_group_sequence);
            previous_checkpoint = Some(page.last_checkpoint);
        }
        if self.latest_checkpoint != previous_checkpoint {
            return Err(TelemetryError::CorruptTier(
                "catalog root latest checkpoint disagrees with its final page".into(),
            ));
        }
        Ok(())
    }
}

/// Small mutable pointer atomically selecting the authoritative catalog root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPointer {
    /// Storage-format version.
    pub format_version: u8,
    /// Selected root generation.
    pub generation: u64,
    /// Immutable root object key.
    pub root_key: String,
    /// Root object length.
    pub root_bytes: u64,
    /// Root object BLAKE3 checksum.
    pub root_checksum: String,
}

impl CatalogPointer {
    fn validate(&self) -> TelemetryResult<()> {
        validate_object_key(&self.root_key)?;
        if self.format_version != TIER_FORMAT_VERSION
            || self.root_bytes == 0
            || !valid_checksum(&self.root_checksum)
        {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Coarse inclusive bounds used to prune catalog pages and groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TierQueryRange {
    /// Optional first logical offset.
    pub first_offset: Option<u64>,
    /// Optional last logical offset.
    pub last_offset: Option<u64>,
    /// Optional lowest event timestamp.
    pub min_timestamp_unix_nanos: Option<u64>,
    /// Optional highest event timestamp.
    pub max_timestamp_unix_nanos: Option<u64>,
    /// Optional exact trace ID or metric-series fingerprint.
    pub signal_identity: Option<u128>,
}

impl TierQueryRange {
    fn validate(self) -> TelemetryResult<()> {
        if self
            .first_offset
            .zip(self.last_offset)
            .is_some_and(|(first, last)| first > last)
            || self
                .min_timestamp_unix_nanos
                .zip(self.max_timestamp_unix_nanos)
                .is_some_and(|(first, last)| first > last)
        {
            return Err(TelemetryError::InvalidQuery(
                "tier query range starts after its end".into(),
            ));
        }
        Ok(())
    }

    fn overlaps(
        self,
        first_offset: u64,
        last_offset: u64,
        min_timestamp: u64,
        max_timestamp: u64,
        min_signal_identity: Option<u128>,
        max_signal_identity: Option<u128>,
    ) -> bool {
        self.first_offset
            .is_none_or(|query_first| last_offset >= query_first)
            && self
                .last_offset
                .is_none_or(|query_last| first_offset <= query_last)
            && self
                .min_timestamp_unix_nanos
                .is_none_or(|query_min| max_timestamp >= query_min)
            && self
                .max_timestamp_unix_nanos
                .is_none_or(|query_max| min_timestamp <= query_max)
            && self.signal_identity.is_none_or(|identity| {
                min_signal_identity
                    .zip(max_signal_identity)
                    .is_some_and(|(min, max)| identity >= min && identity <= max)
            })
    }
}

/// Bounded object-tier catalog configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectTierConfig {
    /// Preferred compressed payload bytes per block group.
    pub target_group_payload_bytes: u64,
    /// Hard limit for one block-group payload object.
    pub max_group_payload_bytes: u64,
    /// Hard limit for independently compressed blocks in one group.
    pub max_blocks_per_group: usize,
    /// Maximum group entries per immutable catalog page.
    pub groups_per_page: usize,
    /// Maximum bytes read for any root, page, or group manifest.
    pub max_control_object_bytes: u64,
}

impl Default for ObjectTierConfig {
    fn default() -> Self {
        Self {
            target_group_payload_bytes: 1024 * 1024 * 1024,
            max_group_payload_bytes: 2 * 1024 * 1024 * 1024,
            max_blocks_per_group: 4_096,
            groups_per_page: 1_024,
            max_control_object_bytes: 64 * 1024 * 1024,
        }
    }
}

impl ObjectTierConfig {
    fn validate(self) -> TelemetryResult<()> {
        if self.target_group_payload_bytes == 0
            || self.max_group_payload_bytes < self.target_group_payload_bytes
        {
            return Err(TelemetryError::InvalidConfig(
                "object tier group payload limits are invalid",
            ));
        }
        if self.max_blocks_per_group == 0 {
            return Err(TelemetryError::InvalidConfig(
                "object tier max_blocks_per_group must be nonzero",
            ));
        }
        if self.groups_per_page == 0 {
            return Err(TelemetryError::InvalidConfig(
                "object tier groups_per_page must be nonzero",
            ));
        }
        if self.max_control_object_bytes < POINTER_READ_LIMIT {
            return Err(TelemetryError::InvalidConfig(
                "object tier control-object limit must be at least 64 KiB",
            ));
        }
        Ok(())
    }
}

/// Partition-scoped immutable object tier with a conditionally published root.
#[derive(Debug)]
pub struct TelemetryObjectTier<S> {
    store: S,
    shard_id: ShardId,
    partition: TopicPartition,
    namespace: String,
    config: ObjectTierConfig,
    root: CatalogRoot,
    current_version: Option<String>,
}

impl<S: TelemetryObjectStore> TelemetryObjectTier<S> {
    /// Opens the current partition catalog without listing object storage.
    ///
    /// Startup validates only `CURRENT` and its root. Catalog pages, group
    /// manifests, and payloads are checked lazily as queries touch them.
    pub fn open(
        store: S,
        shard_id: ShardId,
        partition: TopicPartition,
        config: ObjectTierConfig,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        let namespace = catalog_namespace(shard_id, partition);
        let current_key = format!("{namespace}/CURRENT");
        let Some(current_metadata) = store.head(&current_key)? else {
            return Ok(Self {
                store,
                shard_id,
                partition,
                namespace,
                config,
                root: CatalogRoot::empty(shard_id, partition),
                current_version: None,
            });
        };
        if current_metadata.bytes > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let pointer_bytes = store.get(&current_key, POINTER_READ_LIMIT)?;
        verify_bytes_metadata(&pointer_bytes, &current_metadata, "catalog CURRENT")?;
        let pointer: CatalogPointer = decode_json(&pointer_bytes, "catalog CURRENT")?;
        pointer.validate()?;
        let root_bytes = store.get(&pointer.root_key, config.max_control_object_bytes)?;
        verify_expected_object(
            &root_bytes,
            pointer.root_bytes,
            &pointer.root_checksum,
            "catalog root",
        )?;
        let root: CatalogRoot = decode_json(&root_bytes, "catalog root")?;
        root.validate(shard_id, partition)?;
        if root.generation != pointer.generation {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT and root generations disagree".into(),
            ));
        }
        Ok(Self {
            store,
            shard_id,
            partition,
            namespace,
            config,
            root,
            current_version: Some(current_metadata.version_token),
        })
    }

    /// Returns the currently selected immutable root.
    #[must_use]
    pub fn root(&self) -> &CatalogRoot {
        &self.root
    }

    /// Returns the object-store adapter.
    #[must_use]
    pub fn object_store(&self) -> &S {
        &self.store
    }

    /// Publishes a complete immutable group and conditionally advances `CURRENT`.
    ///
    /// Artifact, manifest, page, and root writes are idempotent. A competing
    /// writer can only cause the final compare-and-swap to fail.
    pub fn publish_group(&mut self, source: TierGroupSource) -> TelemetryResult<TierGroupManifest> {
        validate_source(&source)?;
        let mut artifacts = Vec::with_capacity(source.artifacts.len());
        for artifact_source in &source.artifacts {
            let source_metadata = hash_file(&artifact_source.path)?;
            let object_key = format!(
                "{}/groups/{:020}/{}-{}-{}",
                self.namespace,
                source.group_sequence,
                artifact_source.kind.key_name(),
                artifact_source.name,
                source_metadata.content_digest
            );
            let stored = self
                .store
                .put_file_if_absent(&object_key, &artifact_source.path)?;
            if stored.bytes != source_metadata.bytes
                || stored.content_digest != source_metadata.content_digest
            {
                return Err(TelemetryError::CorruptTier(format!(
                    "object store changed artifact {}",
                    artifact_source.name
                )));
            }
            artifacts.push(TierArtifact {
                kind: artifact_source.kind,
                name: artifact_source.name.clone(),
                object_key,
                bytes: stored.bytes,
                checksum_algorithm: CHECKSUM_ALGORITHM.into(),
                checksum: stored.content_digest,
            });
        }
        let manifest = TierGroupManifest {
            format_version: TIER_FORMAT_VERSION,
            group_sequence: source.group_sequence,
            checkpoint: source.checkpoint,
            shard_id: self.shard_id.get(),
            topic_id: self.partition.topic_id.get().to_string(),
            partition_id: self.partition.partition_id.get(),
            blocks: source.blocks,
            artifacts,
        };
        manifest.validate(
            self.shard_id,
            self.partition,
            self.config.max_blocks_per_group,
            self.config.max_group_payload_bytes,
        )?;
        let manifest_bytes = encode_json(&manifest, "group manifest")?;
        ensure_control_size(
            manifest_bytes.len(),
            self.config.max_control_object_bytes,
            "group manifest",
        )?;
        let manifest_checksum = checksum_bytes(&manifest_bytes);
        let manifest_key = format!(
            "{}/groups/{:020}/manifest-{}.json",
            self.namespace, manifest.group_sequence, manifest_checksum
        );
        let manifest_metadata = self
            .store
            .put_bytes_if_absent(&manifest_key, &manifest_bytes)?;
        let entry = CatalogGroupEntry::from_manifest(&manifest, manifest_key, &manifest_metadata)?;

        if let Some(last_page_ref) = self.root.pages.last() {
            let last_page = self.load_page(last_page_ref)?;
            let last_group = last_page
                .groups
                .last()
                .expect("validated catalog pages are nonempty");
            if source.group_sequence == last_group.group_sequence {
                if *last_group != entry {
                    return Err(TelemetryError::CorruptTier(
                        "group sequence was retried with different contents".into(),
                    ));
                }
                return Ok(manifest);
            }
            if source.group_sequence < last_group.group_sequence {
                return Err(TelemetryError::ObjectStore(
                    "group sequences must be published in increasing order".into(),
                ));
            }
            if !source.checkpoint.covers(last_group.checkpoint) {
                return Err(TelemetryError::ObjectStore(
                    "group checkpoints must advance monotonically".into(),
                ));
            }
        }

        let (page, replace_last) = match self.root.pages.last() {
            Some(last_ref) => {
                let mut last = self.load_page(last_ref)?;
                if last.groups.len() < self.config.groups_per_page {
                    last.groups.push(entry);
                    (last, true)
                } else {
                    (
                        CatalogPage {
                            format_version: TIER_FORMAT_VERSION,
                            page_sequence: last.page_sequence.checked_add(1).ok_or_else(|| {
                                TelemetryError::ObjectStore(
                                    "catalog page sequence exhausted".into(),
                                )
                            })?,
                            shard_id: self.shard_id.get(),
                            topic_id: self.partition.topic_id.get().to_string(),
                            partition_id: self.partition.partition_id.get(),
                            groups: vec![entry],
                        },
                        false,
                    )
                }
            }
            None => (
                CatalogPage {
                    format_version: TIER_FORMAT_VERSION,
                    page_sequence: 0,
                    shard_id: self.shard_id.get(),
                    topic_id: self.partition.topic_id.get().to_string(),
                    partition_id: self.partition.partition_id.get(),
                    groups: vec![entry],
                },
                false,
            ),
        };
        page.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
        let page_bytes = encode_json(&page, "catalog page")?;
        ensure_control_size(
            page_bytes.len(),
            self.config.max_control_object_bytes,
            "catalog page",
        )?;
        let page_checksum = checksum_bytes(&page_bytes);
        let page_key = format!(
            "{}/pages/page-{:020}-{}.json",
            self.namespace, page.page_sequence, page_checksum
        );
        let page_metadata = self.store.put_bytes_if_absent(&page_key, &page_bytes)?;
        let page_ref = CatalogPageRef::from_page(&page, page_key, &page_metadata)?;

        let mut next_root = self.root.clone();
        next_root.generation = next_root
            .generation
            .checked_add(1)
            .ok_or_else(|| TelemetryError::ObjectStore("catalog generation exhausted".into()))?;
        next_root.latest_checkpoint = Some(source.checkpoint);
        let first_block_id = manifest
            .blocks
            .first()
            .expect("validated group has blocks")
            .block_id;
        if first_block_id < self.root.next_block_id {
            return Err(TelemetryError::ObjectStore(
                "group block identifiers overlap an older group".into(),
            ));
        }
        next_root.next_block_id = manifest
            .blocks
            .last()
            .expect("validated group has blocks")
            .block_id
            .checked_add(1)
            .ok_or_else(|| TelemetryError::ObjectStore("block identifier exhausted".into()))?;
        if replace_last {
            *next_root
                .pages
                .last_mut()
                .expect("a replaced page has an existing reference") = page_ref;
        } else {
            next_root.pages.push(page_ref);
        }
        next_root.validate(self.shard_id, self.partition)?;
        let root_bytes = encode_json(&next_root, "catalog root")?;
        ensure_control_size(
            root_bytes.len(),
            self.config.max_control_object_bytes,
            "catalog root",
        )?;
        let root_checksum = checksum_bytes(&root_bytes);
        let root_key = format!(
            "{}/roots/root-{:020}-{}.json",
            self.namespace, next_root.generation, root_checksum
        );
        let root_metadata = self.store.put_bytes_if_absent(&root_key, &root_bytes)?;
        let pointer = CatalogPointer {
            format_version: TIER_FORMAT_VERSION,
            generation: next_root.generation,
            root_key,
            root_bytes: root_metadata.bytes,
            root_checksum: root_metadata.content_digest,
        };
        let pointer_bytes = encode_json(&pointer, "catalog CURRENT")?;
        if u64::try_from(pointer_bytes.len()).unwrap_or(u64::MAX) > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let current_key = format!("{}/CURRENT", self.namespace);
        let current_metadata = self.store.compare_and_swap(
            &current_key,
            self.current_version.as_deref(),
            &pointer_bytes,
        )?;
        self.root = next_root;
        self.current_version = Some(current_metadata.version_token);
        Ok(manifest)
    }

    /// Returns group entries whose coarse bounds overlap the query.
    ///
    /// Only overlapping catalog pages are loaded. This never lists objects.
    pub fn candidate_groups(
        &self,
        range: TierQueryRange,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        let mut groups = Vec::new();
        self.for_each_candidate_group(range, |group| {
            groups.push(group.clone());
            Ok(true)
        })?;
        Ok(groups)
    }

    /// Returns overlapping groups while serving immutable catalog pages from
    /// the integrity-checked SSD cache when they fit its read bound.
    pub fn candidate_groups_cached(
        &self,
        range: TierQueryRange,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        let mut groups = Vec::new();
        self.for_each_candidate_group_with(
            range,
            |reference| self.load_page_cached(reference, cache),
            |group| {
                groups.push(group.clone());
                Ok(true)
            },
        )?;
        Ok(groups)
    }

    /// Loads only the final catalog page and returns its newest group entry.
    pub fn latest_group(&self) -> TelemetryResult<Option<CatalogGroupEntry>> {
        let Some(reference) = self.root.pages.last() else {
            return Ok(None);
        };
        Ok(self.load_page(reference)?.groups.last().cloned())
    }

    /// Loads the newest group through the bounded catalog-page cache.
    pub fn latest_group_cached(
        &self,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Option<CatalogGroupEntry>> {
        let Some(reference) = self.root.pages.last() else {
            return Ok(None);
        };
        Ok(self
            .load_page_cached(reference, cache)?
            .groups
            .last()
            .cloned())
    }

    /// Visits overlapping groups one at a time without materializing a
    /// corpus-sized candidate vector.
    ///
    /// Returning `false` from `visit` stops traversal successfully. Catalog
    /// pages are loaded and verified lazily, and object storage is never
    /// listed.
    pub fn for_each_candidate_group(
        &self,
        range: TierQueryRange,
        visit: impl FnMut(&CatalogGroupEntry) -> TelemetryResult<bool>,
    ) -> TelemetryResult<()> {
        self.for_each_candidate_group_with(range, |reference| self.load_page(reference), visit)
    }

    fn for_each_candidate_group_with(
        &self,
        range: TierQueryRange,
        mut load_page: impl FnMut(&CatalogPageRef) -> TelemetryResult<CatalogPage>,
        mut visit: impl FnMut(&CatalogGroupEntry) -> TelemetryResult<bool>,
    ) -> TelemetryResult<()> {
        range.validate()?;
        for page_ref in &self.root.pages {
            if !range.overlaps(
                page_ref.first_offset,
                page_ref.last_offset,
                page_ref.min_timestamp_unix_nanos,
                page_ref.max_timestamp_unix_nanos,
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
            ) {
                continue;
            }
            let page = load_page(page_ref)?;
            for group in &page.groups {
                if range.overlaps(
                    group.first_offset,
                    group.last_offset,
                    group.min_timestamp_unix_nanos,
                    group.max_timestamp_unix_nanos,
                    group.min_signal_identity,
                    group.max_signal_identity,
                ) && !visit(group)?
                {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Loads and validates one group manifest selected from a catalog page.
    pub fn load_group(&self, entry: &CatalogGroupEntry) -> TelemetryResult<TierGroupManifest> {
        entry.validate()?;
        let bytes = self
            .store
            .get(&entry.manifest_key, self.config.max_control_object_bytes)?;
        self.decode_group(entry, &bytes)
    }

    /// Loads and validates a group manifest through the immutable SSD cache.
    pub fn load_group_cached(
        &self,
        entry: &CatalogGroupEntry,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<TierGroupManifest> {
        entry.validate()?;
        if entry.manifest_bytes > cache.max_read_bytes() {
            return self.load_group(entry);
        }
        let metadata = ObjectMetadata {
            bytes: entry.manifest_bytes,
            version_token: entry.manifest_checksum.clone(),
            content_digest: entry.manifest_checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &entry.manifest_key,
            &metadata,
            0..entry.manifest_bytes,
        )?;
        self.decode_group(entry, &bytes)
    }

    /// Reads and verifies a complete immutable artifact on demand.
    pub fn read_artifact(
        &self,
        artifact: &TierArtifact,
        max_bytes: u64,
    ) -> TelemetryResult<Vec<u8>> {
        validate_artifact(artifact)?;
        if artifact.bytes > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "artifact {} exceeds read limit {max_bytes}",
                artifact.name
            )));
        }
        let bytes = self.store.get(&artifact.object_key, max_bytes)?;
        verify_expected_object(&bytes, artifact.bytes, &artifact.checksum, "group artifact")?;
        Ok(bytes)
    }

    /// Reads a complete immutable artifact through the bounded SSD cache.
    pub fn read_artifact_cached(
        &self,
        artifact: &TierArtifact,
        max_bytes: u64,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Vec<u8>> {
        validate_artifact(artifact)?;
        if artifact.bytes > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "artifact {} exceeds read limit {max_bytes}",
                artifact.name
            )));
        }
        if artifact.bytes > cache.max_read_bytes() {
            return self.read_artifact(artifact, max_bytes);
        }
        let metadata = ObjectMetadata {
            bytes: artifact.bytes,
            version_token: artifact.checksum.clone(),
            content_digest: artifact.checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &artifact.object_key,
            &metadata,
            0..artifact.bytes,
        )?;
        verify_expected_object(&bytes, artifact.bytes, &artifact.checksum, "group artifact")?;
        Ok(bytes)
    }

    fn load_page(&self, reference: &CatalogPageRef) -> TelemetryResult<CatalogPage> {
        reference.validate()?;
        let bytes = self
            .store
            .get(&reference.page_key, self.config.max_control_object_bytes)?;
        self.decode_page(reference, &bytes)
    }

    fn load_page_cached(
        &self,
        reference: &CatalogPageRef,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<CatalogPage> {
        reference.validate()?;
        if reference.page_bytes > cache.max_read_bytes() {
            return self.load_page(reference);
        }
        let metadata = ObjectMetadata {
            bytes: reference.page_bytes,
            version_token: reference.page_checksum.clone(),
            content_digest: reference.page_checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &reference.page_key,
            &metadata,
            0..reference.page_bytes,
        )?;
        self.decode_page(reference, &bytes)
    }

    fn decode_page(
        &self,
        reference: &CatalogPageRef,
        bytes: &[u8],
    ) -> TelemetryResult<CatalogPage> {
        verify_expected_object(
            bytes,
            reference.page_bytes,
            &reference.page_checksum,
            "catalog page",
        )?;
        let page: CatalogPage = decode_json(bytes, "catalog page")?;
        page.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
        if page.page_sequence != reference.page_sequence {
            return Err(TelemetryError::CorruptTier(
                "catalog root and page sequences disagree".into(),
            ));
        }
        Ok(page)
    }

    fn decode_group(
        &self,
        entry: &CatalogGroupEntry,
        bytes: &[u8],
    ) -> TelemetryResult<TierGroupManifest> {
        verify_expected_object(
            bytes,
            entry.manifest_bytes,
            &entry.manifest_checksum,
            "group manifest",
        )?;
        let manifest: TierGroupManifest = decode_json(bytes, "group manifest")?;
        manifest.validate(
            self.shard_id,
            self.partition,
            self.config.max_blocks_per_group,
            self.config.max_group_payload_bytes,
        )?;
        if manifest.group_sequence != entry.group_sequence {
            return Err(TelemetryError::CorruptTier(
                "catalog group and manifest sequences disagree".into(),
            ));
        }
        Ok(manifest)
    }
}

/// Configuration for one byte-bounded SSD object-range cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsdCacheConfig {
    /// Maximum cache bytes, including per-chunk integrity headers.
    pub max_bytes: u64,
    /// Object range chunk size.
    pub chunk_bytes: u64,
    /// Maximum bytes returned by one cache read.
    pub max_read_bytes: u64,
}

impl Default for SsdCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024 * 1024,
            chunk_bytes: 4 * 1024 * 1024,
            max_read_bytes: 64 * 1024 * 1024,
        }
    }
}

impl SsdCacheConfig {
    fn validate(self) -> TelemetryResult<()> {
        if self.max_bytes == 0 || self.chunk_bytes == 0 || self.max_read_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "SSD cache byte limits must be nonzero",
            ));
        }
        if self.chunk_bytes > self.max_read_bytes {
            return Err(TelemetryError::InvalidConfig(
                "SSD cache chunks cannot exceed the read limit",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CacheEntry {
    path: PathBuf,
    bytes: u64,
    stamp: u64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<String, CacheEntry>,
    used_bytes: u64,
    clock: u64,
    hits: u64,
    misses: u64,
    source_bytes: u64,
}

/// Runtime diagnostics for one recoverable SSD object cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SsdCacheStats {
    /// Immutable chunks currently retained on SSD.
    pub entries: usize,
    /// Framed bytes currently retained on SSD.
    pub used_bytes: u64,
    /// Successful integrity-checked SSD chunk reads since open.
    pub hits: u64,
    /// Chunks fetched from object storage since open.
    pub misses: u64,
    /// Object-store payload bytes fetched by cache misses since open.
    pub source_bytes: u64,
}

/// Recoverable, integrity-checked SSD cache for immutable object ranges.
///
/// Deployments normally use separate instances and budgets for control/index
/// objects and payload data so a scan cannot evict all query metadata.
#[derive(Debug)]
pub struct SsdObjectCache {
    root: PathBuf,
    config: SsdCacheConfig,
    state: Mutex<CacheState>,
}

impl SsdObjectCache {
    /// Opens an SSD cache and reconstructs its bounded local directory.
    pub fn open(root: impl AsRef<Path>, config: SsdCacheConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| storage_io("create SSD cache", error))?;
        let mut state = CacheState::default();
        let entries = fs::read_dir(&root).map_err(|error| storage_io("scan SSD cache", error))?;
        for entry in entries {
            let entry = entry.map_err(|error| storage_io("read SSD cache entry", error))?;
            let path = entry.path();
            let Some(name) = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.ends_with(".chunk") && name.len() == 70)
            else {
                continue;
            };
            let metadata = entry
                .metadata()
                .map_err(|error| storage_io("inspect SSD cache entry", error))?;
            if !metadata.is_file() {
                continue;
            }
            let bytes = metadata.len();
            state.used_bytes = state.used_bytes.saturating_add(bytes);
            state.entries.insert(
                name[..64].to_owned(),
                CacheEntry {
                    path,
                    bytes,
                    stamp: 0,
                },
            );
        }
        let cache = Self {
            root,
            config,
            state: Mutex::new(state),
        };
        cache.evict_to_budget()?;
        Ok(cache)
    }

    /// Returns currently occupied cache bytes.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.state.lock().map(|state| state.used_bytes).unwrap_or(0)
    }

    /// Returns bounded cache occupancy and read-amplification counters.
    #[must_use]
    pub fn stats(&self) -> SsdCacheStats {
        self.state.lock().map_or_else(
            |_| SsdCacheStats::default(),
            |state| SsdCacheStats {
                entries: state.entries.len(),
                used_bytes: state.used_bytes,
                hits: state.hits,
                misses: state.misses,
                source_bytes: state.source_bytes,
            },
        )
    }

    /// Returns the largest object extent accepted by one cache operation.
    #[must_use]
    pub const fn max_read_bytes(&self) -> u64 {
        self.config.max_read_bytes
    }

    /// Reads an object range, filling and reusing fixed immutable SSD chunks.
    pub fn read_range<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end || range.end - range.start > self.config.max_read_bytes {
            return Err(TelemetryError::ObjectStore(
                "SSD cache read range is invalid or exceeds its limit".into(),
            ));
        }
        let metadata = store.head(object_key)?.ok_or_else(|| {
            TelemetryError::ObjectStore(format!("object {object_key} does not exist"))
        })?;
        self.read_range_with_metadata(store, object_key, &metadata, range)
    }

    /// Reads an object range using immutable metadata already held in a
    /// manifest, avoiding a remote HEAD request on the query path.
    pub fn read_range_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end || range.end - range.start > self.config.max_read_bytes {
            return Err(TelemetryError::ObjectStore(
                "SSD cache read range is invalid or exceeds its limit".into(),
            ));
        }
        if range.end > metadata.bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "SSD cache range exceeds object {object_key}"
            )));
        }
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
            TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
        })?;
        let mut output = Vec::with_capacity(output_bytes);
        let first_chunk = range.start / self.config.chunk_bytes;
        let last_chunk = (range.end - 1) / self.config.chunk_bytes;
        for chunk_index in first_chunk..=last_chunk {
            let chunk_start = chunk_index
                .checked_mul(self.config.chunk_bytes)
                .ok_or_else(|| TelemetryError::ObjectStore("cache chunk offset overflow".into()))?;
            let chunk_end = chunk_start
                .saturating_add(self.config.chunk_bytes)
                .min(metadata.bytes);
            let chunk = self.load_or_fetch_chunk(
                store,
                object_key,
                metadata,
                chunk_index,
                chunk_start..chunk_end,
            )?;
            let copy_start = range.start.max(chunk_start) - chunk_start;
            let copy_end = range.end.min(chunk_end) - chunk_start;
            let copy_start = usize::try_from(copy_start)
                .map_err(|_| TelemetryError::ObjectStore("cache slice offset overflow".into()))?;
            let copy_end = usize::try_from(copy_end)
                .map_err(|_| TelemetryError::ObjectStore("cache slice offset overflow".into()))?;
            output.extend_from_slice(&chunk[copy_start..copy_end]);
        }
        Ok(output)
    }

    /// Reads sorted, non-overlapping immutable ranges while retaining the
    /// most recently loaded cache chunk for the complete batch.
    ///
    /// Payload packs place block and frame extents in ascending order. A
    /// batched query therefore reads or fetches each shared SSD chunk once
    /// instead of reopening that chunk for every selected extent.
    pub fn read_ranges_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        ranges: &[Range<u64>],
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        if ranges.windows(2).any(|pair| pair[0].end > pair[1].start) {
            return Err(TelemetryError::ObjectStore(
                "batched SSD cache ranges must be sorted and non-overlapping".into(),
            ));
        }
        let mut outputs = Vec::with_capacity(ranges.len());
        let mut loaded = None::<(u64, u64, Vec<u8>)>;
        for range in ranges {
            if range.start > range.end
                || range.end > metadata.bytes
                || range.end - range.start > self.config.max_read_bytes
            {
                return Err(TelemetryError::ObjectStore(
                    "batched SSD cache range is invalid or exceeds its limit".into(),
                ));
            }
            let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
                TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
            })?;
            let mut output = Vec::with_capacity(output_bytes);
            if !range.is_empty() {
                let first_chunk = range.start / self.config.chunk_bytes;
                let last_chunk = (range.end - 1) / self.config.chunk_bytes;
                for chunk_index in first_chunk..=last_chunk {
                    let chunk_start = chunk_index
                        .checked_mul(self.config.chunk_bytes)
                        .ok_or_else(|| {
                            TelemetryError::ObjectStore("cache chunk offset overflow".into())
                        })?;
                    let chunk_end = chunk_start
                        .saturating_add(self.config.chunk_bytes)
                        .min(metadata.bytes);
                    if loaded
                        .as_ref()
                        .is_none_or(|(index, _, _)| *index != chunk_index)
                    {
                        loaded = Some((
                            chunk_index,
                            chunk_start,
                            self.load_or_fetch_chunk(
                                store,
                                object_key,
                                metadata,
                                chunk_index,
                                chunk_start..chunk_end,
                            )?,
                        ));
                    }
                    let (_, loaded_start, chunk) =
                        loaded.as_ref().expect("requested cache chunk was loaded");
                    let copy_start = usize::try_from(range.start.max(chunk_start) - *loaded_start)
                        .map_err(|_| {
                            TelemetryError::ObjectStore("cache slice offset overflow".into())
                        })?;
                    let copy_end = usize::try_from(range.end.min(chunk_end) - *loaded_start)
                        .map_err(|_| {
                            TelemetryError::ObjectStore("cache slice offset overflow".into())
                        })?;
                    output.extend_from_slice(&chunk[copy_start..copy_end]);
                }
            }
            outputs.push(output);
        }
        Ok(outputs)
    }

    fn load_or_fetch_chunk<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        chunk_index: u64,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        let cache_key = checksum_bytes(
            format!("{object_key}\0{}\0{chunk_index}", metadata.version_token).as_bytes(),
        );
        if let Some(path) = self.cache_hit(&cache_key)? {
            match read_cache_chunk(&path) {
                Ok(bytes)
                    if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                        == range.end - range.start =>
                {
                    self.record_cache_hit()?;
                    return Ok(bytes);
                }
                Ok(_) | Err(_) => self.remove_entry(&cache_key)?,
            }
        }
        let bytes = store.get_range(object_key, range)?;
        self.record_cache_miss(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
        self.install_chunk(&cache_key, &bytes)?;
        Ok(bytes)
    }

    fn record_cache_hit(&self) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.hits = state.hits.saturating_add(1);
        Ok(())
    }

    fn record_cache_miss(&self, bytes: u64) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.misses = state.misses.saturating_add(1);
        state.source_bytes = state.source_bytes.saturating_add(bytes);
        Ok(())
    }

    fn cache_hit(&self, cache_key: &str) -> TelemetryResult<Option<PathBuf>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        Ok(state.entries.get_mut(cache_key).map(|entry| {
            entry.stamp = stamp;
            entry.path.clone()
        }))
    }

    fn install_chunk(&self, cache_key: &str, bytes: &[u8]) -> TelemetryResult<()> {
        let framed_bytes = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(CACHE_HEADER_BYTES as u64);
        if framed_bytes > self.config.max_bytes {
            return Ok(());
        }
        let path = self.root.join(format!("{cache_key}.chunk"));
        write_cache_chunk_atomically(&path, bytes)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state.entries.insert(
            cache_key.to_owned(),
            CacheEntry {
                path,
                bytes: framed_bytes,
                stamp,
            },
        ) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.bytes);
        }
        state.used_bytes = state.used_bytes.saturating_add(framed_bytes);
        evict_locked(&mut state, self.config.max_bytes)
    }

    fn remove_entry(&self, cache_key: &str) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        if let Some(entry) = state.entries.remove(cache_key) {
            state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
            remove_cache_file(&entry.path)?;
        }
        Ok(())
    }

    fn evict_to_budget(&self) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        evict_locked(&mut state, self.config.max_bytes)
    }
}

/// Writes selected staged block payloads as one immutable concatenated pack.
///
/// The returned entries contain exact byte extents and per-block checksums.
pub fn write_staged_payload_pack(
    catalog: &BlockCatalog,
    block_ids: &[BlockId],
    destination: impl AsRef<Path>,
) -> TelemetryResult<Vec<TierBlockEntry>> {
    if block_ids.is_empty() {
        return Err(TelemetryError::ObjectStore(
            "a payload pack requires at least one block".into(),
        ));
    }
    if block_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(TelemetryError::ObjectStore(
            "payload-pack block IDs must be strictly increasing".into(),
        ));
    }
    let first_descriptor = catalog
        .get(block_ids[0])
        .ok_or_else(|| TelemetryError::UnknownBlock(block_ids[0].get()))?;
    let destination = destination.as_ref();
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create payload-pack directory", error))?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(|error| storage_io("create staged payload pack", error))?;
    let mut offset = 0u64;
    let mut entries = Vec::with_capacity(block_ids.len());
    for &block_id in block_ids {
        let descriptor = catalog
            .get(block_id)
            .ok_or_else(|| TelemetryError::UnknownBlock(block_id.get()))?;
        if descriptor.stream_shard_id != first_descriptor.stream_shard_id
            || descriptor.topic_partition != first_descriptor.topic_partition
        {
            return Err(TelemetryError::ObjectStore(
                "one payload pack cannot cross a shard or logical partition".into(),
            ));
        }
        let payload = catalog
            .staged_payload(block_id)
            .ok_or_else(|| TelemetryError::MissingStagedPayload(block_id.get()))?;
        if u64::try_from(payload.len()).unwrap_or(u64::MAX) != descriptor.stored_bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "staged block {} length differs from its descriptor",
                block_id.get()
            )));
        }
        file.write_all(&payload)
            .map_err(|error| storage_io("write staged payload pack", error))?;
        entries.push(TierBlockEntry::from_descriptor(
            descriptor,
            offset,
            checksum_bytes(&payload),
        ));
        offset = offset
            .checked_add(descriptor.stored_bytes)
            .ok_or_else(|| TelemetryError::ObjectStore("payload-pack length overflow".into()))?;
    }
    file.sync_all()
        .map_err(|error| storage_io("sync staged payload pack", error))?;
    sync_parent(destination)?;
    Ok(entries)
}

/// Marks all local blocks in a published group as durable payload ranges.
pub fn mark_group_offloaded(
    catalog: &mut BlockCatalog,
    manifest: &TierGroupManifest,
) -> TelemetryResult<()> {
    let payload = manifest
        .artifact(TierArtifactKind::PayloadPack)
        .ok_or_else(|| TelemetryError::CorruptTier("group has no payload artifact".into()))?;

    // Validate the complete transition before mutating any block. A corrupt
    // or stale manifest must not leave a partially offloaded local catalog.
    for block in &manifest.blocks {
        let descriptor = catalog
            .get(BlockId::new(block.block_id))
            .ok_or(TelemetryError::UnknownBlock(block.block_id))?;
        let range_end = block
            .payload_offset
            .checked_add(descriptor.stored_bytes)
            .ok_or_else(|| TelemetryError::CorruptTier("payload range overflow".into()))?;
        if range_end > payload.bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "block {} exceeds payload artifact length",
                block.block_id
            )));
        }
        if descriptor.stream_shard_id.get() != manifest.shard_id
            || descriptor.topic_partition.topic_id.get().to_string() != manifest.topic_id
            || descriptor.topic_partition.partition_id.get() != manifest.partition_id
        {
            return Err(TelemetryError::CorruptTier(format!(
                "block {} belongs to another catalog namespace",
                block.block_id
            )));
        }
    }

    for block in &manifest.blocks {
        catalog.mark_offloaded_range(
            BlockId::new(block.block_id),
            payload.object_key.clone(),
            block.payload_offset,
        )?;
    }
    Ok(())
}

fn validate_source(source: &TierGroupSource) -> TelemetryResult<()> {
    if source.blocks.is_empty() || source.artifacts.is_empty() {
        return Err(TelemetryError::ObjectStore(
            "a tier group requires blocks and artifacts".into(),
        ));
    }
    if source
        .blocks
        .windows(2)
        .any(|pair| pair[0].block_id >= pair[1].block_id)
    {
        return Err(TelemetryError::ObjectStore(
            "tier group blocks must be strictly increasing".into(),
        ));
    }
    for artifact in &source.artifacts {
        validate_artifact_name(&artifact.name)?;
    }
    if source
        .artifacts
        .iter()
        .enumerate()
        .any(|(index, artifact)| {
            source.artifacts[index + 1..]
                .iter()
                .any(|other| artifact.kind == other.kind && artifact.name == other.name)
        })
    {
        return Err(TelemetryError::ObjectStore(
            "tier group artifact names must be unique per role".into(),
        ));
    }
    Ok(())
}

fn validate_artifact(artifact: &TierArtifact) -> TelemetryResult<()> {
    validate_artifact_name(&artifact.name)?;
    validate_object_key(&artifact.object_key)?;
    if artifact.bytes == 0
        || artifact.checksum_algorithm != CHECKSUM_ALGORITHM
        || !valid_checksum(&artifact.checksum)
    {
        return Err(TelemetryError::CorruptTier(
            "group artifact metadata is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_artifact_name(name: &str) -> TelemetryResult<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(TelemetryError::ObjectStore(
            "artifact names must be 1..=128 path-free ASCII characters".into(),
        ));
    }
    Ok(())
}

fn validate_object_key(key: &str) -> TelemetryResult<()> {
    let path = Path::new(key);
    if key.is_empty()
        || key.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(TelemetryError::ObjectStore(format!(
            "unsafe object key {key:?}"
        )));
    }
    Ok(())
}

fn catalog_namespace(shard_id: ShardId, partition: TopicPartition) -> String {
    format!(
        "catalog/shard-{}/topic-{:032x}/partition-{}",
        shard_id.get(),
        partition.topic_id.get(),
        partition.partition_id.get()
    )
}

fn encode_json<T: Serialize>(value: &T, context: &str) -> TelemetryResult<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| TelemetryError::CorruptTier(format!("{context} encoding failed: {error}")))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], context: &str) -> TelemetryResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| TelemetryError::CorruptTier(format!("{context} decoding failed: {error}")))
}

fn ensure_control_size(bytes: usize, limit: u64, context: &str) -> TelemetryResult<()> {
    if u64::try_from(bytes).unwrap_or(u64::MAX) > limit {
        return Err(TelemetryError::ObjectStore(format!(
            "{context} exceeds configured control-object limit {limit}"
        )));
    }
    Ok(())
}

fn verify_bytes_metadata(
    bytes: &[u8],
    metadata: &ObjectMetadata,
    context: &str,
) -> TelemetryResult<()> {
    verify_expected_object(bytes, metadata.bytes, &metadata.content_digest, context)
}

fn verify_expected_object(
    bytes: &[u8],
    expected_bytes: u64,
    expected_checksum: &str,
    context: &str,
) -> TelemetryResult<()> {
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_bytes
        || checksum_bytes(bytes) != expected_checksum
    {
        return Err(TelemetryError::CorruptTier(format!(
            "{context} failed length or BLAKE3 verification"
        )));
    }
    Ok(())
}

fn valid_checksum(checksum: &str) -> bool {
    checksum.len() == 64
        && checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn metadata_for_bytes(bytes: &[u8]) -> ObjectMetadata {
    let content_digest = checksum_bytes(bytes);
    ObjectMetadata {
        bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        version_token: content_digest.clone(),
        content_digest,
    }
}

fn checksum_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn hash_file(path: &Path) -> TelemetryResult<ObjectMetadata> {
    let mut file =
        File::open(path).map_err(|error| storage_io("open file for BLAKE3 hashing", error))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage_io("read file for BLAKE3 hashing", error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| TelemetryError::StorageIo("file length overflow".into()))?;
    }
    Ok(ObjectMetadata {
        bytes,
        version_token: hasher.finalize().to_hex().to_string(),
        content_digest: hasher.finalize().to_hex().to_string(),
    })
}

fn metadata_for_path_if_present(path: &Path) -> TelemetryResult<Option<ObjectMetadata>> {
    match path.metadata() {
        Ok(metadata) if metadata.is_file() => hash_file(path).map(Some),
        Ok(_) => Err(TelemetryError::ObjectStore(format!(
            "object path {} is not a regular file",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_io("inspect local object", error)),
    }
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create object parent directory", error))?;
    }
    let temporary = temporary_path(path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("create temporary object", error))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|error| storage_io("write temporary object", error))?;
        file.sync_all()
            .map_err(|error| storage_io("sync temporary object", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| storage_io("publish temporary object", error))?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn copy_file_atomically(source: &Path, destination: &Path) -> TelemetryResult<ObjectMetadata> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create object parent directory", error))?;
    }
    let temporary = temporary_path(destination);
    let mut input = File::open(source).map_err(|error| storage_io("open object source", error))?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("create temporary object", error))?;
    let result = (|| {
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0; COPY_BUFFER_BYTES];
        let mut bytes = 0u64;
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|error| storage_io("read object source", error))?;
            if read == 0 {
                break;
            }
            output
                .write_all(&buffer[..read])
                .map_err(|error| storage_io("write temporary object", error))?;
            hasher.update(&buffer[..read]);
            bytes = bytes
                .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
                .ok_or_else(|| TelemetryError::StorageIo("object length overflow".into()))?;
        }
        output
            .sync_all()
            .map_err(|error| storage_io("sync temporary object", error))?;
        fs::rename(&temporary, destination)
            .map_err(|error| storage_io("publish temporary object", error))?;
        sync_parent(destination)?;
        Ok(ObjectMetadata {
            bytes,
            version_token: hasher.finalize().to_hex().to_string(),
            content_digest: hasher.finalize().to_hex().to_string(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("tmp-{}-{sequence}", std::process::id()))
}

fn sync_parent(path: &Path) -> TelemetryResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_io("sync parent directory", error))
}

fn unlock_file(file: &File) -> TelemetryResult<()> {
    FileExt::unlock(file).map_err(|error| storage_io("unlock object-store update lock", error))
}

fn storage_io(context: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::StorageIo(format!("{context}: {error}"))
}

fn object_io(key: &str, operation: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::ObjectStore(format!("{operation} object {key}: {error}"))
}

fn write_cache_chunk_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    let mut framed = Vec::with_capacity(bytes.len().saturating_add(CACHE_HEADER_BYTES));
    framed.extend_from_slice(CACHE_HEADER_MAGIC);
    framed.extend_from_slice(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    framed.extend_from_slice(blake3::hash(bytes).as_bytes());
    framed.extend_from_slice(bytes);
    write_bytes_atomically(path, &framed)
}

fn read_cache_chunk(path: &Path) -> TelemetryResult<Vec<u8>> {
    let framed = fs::read(path).map_err(|error| storage_io("read SSD cache chunk", error))?;
    if framed.len() < CACHE_HEADER_BYTES || &framed[..8] != CACHE_HEADER_MAGIC {
        return Err(TelemetryError::CorruptTier(
            "SSD cache chunk header is invalid".into(),
        ));
    }
    let bytes = u64::from_le_bytes(
        framed[8..16]
            .try_into()
            .map_err(|_| TelemetryError::CorruptTier("SSD cache length is invalid".into()))?,
    );
    let payload = &framed[CACHE_HEADER_BYTES..];
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) != bytes
        || blake3::hash(payload).as_bytes() != &framed[16..48]
    {
        return Err(TelemetryError::CorruptTier(
            "SSD cache chunk failed integrity verification".into(),
        ));
    }
    Ok(payload.to_vec())
}

fn evict_locked(state: &mut CacheState, max_bytes: u64) -> TelemetryResult<()> {
    while state.used_bytes > max_bytes {
        let Some((key, _)) = state
            .entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, entry)| (key.clone(), entry.stamp))
        else {
            state.used_bytes = 0;
            break;
        };
        let entry = state
            .entries
            .remove(&key)
            .expect("selected cache entry still exists");
        state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
        remove_cache_file(&entry.path)?;
    }
    Ok(())
}

fn remove_cache_file(path: &Path) -> TelemetryResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_io("evict SSD cache chunk", error)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};

    use super::*;
    use crate::{
        CompressionCohortId, CompressionPlacementId, CompressionTemperature, DictionaryId,
    };

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new(name: &str) -> Self {
            let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "shard-telemetry-{name}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("test directory is created");
            Self { path }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn partition() -> TopicPartition {
        TopicPartition::new(TopicId::new(91), LogicalPartitionId::new(3))
    }

    fn tier_config(groups_per_page: usize) -> ObjectTierConfig {
        ObjectTierConfig {
            groups_per_page,
            ..ObjectTierConfig::default()
        }
    }

    fn write_test_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("test artifact is written");
    }

    fn group_source(
        directory: &Path,
        sequence: u64,
        first_offset: u64,
        timestamp: u64,
    ) -> TierGroupSource {
        let payload = vec![u8::try_from(sequence).unwrap_or(u8::MAX); 32];
        let payload_path = directory.join(format!("group-{sequence}.payload"));
        let index_path = directory.join(format!("group-{sequence}.query-index"));
        write_test_file(&payload_path, &payload);
        write_test_file(&index_path, format!("query-index-{sequence}").as_bytes());
        TierGroupSource {
            group_sequence: sequence,
            checkpoint: TierCheckpoint {
                next_placement_sequence: sequence + 1,
                next_offset: first_offset + 10,
            },
            blocks: vec![TierBlockEntry {
                block_id: sequence,
                source_compression_cohort: 7,
                placement_id: 11,
                dictionary_id: None,
                compression_codec: "zstd".into(),
                compression_level: 1,
                first_offset,
                last_offset: first_offset + 9,
                record_count: 10,
                source_bytes: 320,
                structural_bytes: 128,
                stored_bytes: 32,
                min_timestamp_unix_nanos: timestamp,
                max_timestamp_unix_nanos: timestamp + 99,
                compression_temperature: 17,
                compression_shape_hash: 19,
                compression_temperature_variance_q8: 2,
                max_compression_temperature_deviation: 1,
                payload_offset: 0,
                payload_bytes: 32,
                payload_checksum: checksum_bytes(&payload),
                min_signal_identity: None,
                max_signal_identity: None,
                correlation_filter: None,
            }],
            artifacts: vec![
                TierArtifactSource {
                    kind: TierArtifactKind::PayloadPack,
                    name: "blocks.pack".into(),
                    path: payload_path,
                },
                TierArtifactSource {
                    kind: TierArtifactKind::QueryIndex,
                    name: "query.slogqix".into(),
                    path: index_path,
                },
            ],
        }
    }

    #[test]
    fn local_object_store_is_immutable_conditional_and_key_safe() {
        let directory = TestDirectory::new("local-object-store");
        let store = LocalObjectStore::open(&directory.path).expect("store opens");
        let first = store
            .put_bytes_if_absent("objects/one", b"first")
            .expect("immutable object is created");
        assert_eq!(
            store
                .put_bytes_if_absent("objects/one", b"first")
                .expect("identical retry succeeds"),
            first
        );
        assert!(matches!(
            store.put_bytes_if_absent("objects/one", b"different"),
            Err(TelemetryError::ObjectStore(_))
        ));
        assert!(matches!(
            store.put_bytes_if_absent("../escape", b"bad"),
            Err(TelemetryError::ObjectStore(_))
        ));

        let current = store
            .compare_and_swap("catalog/CURRENT", None, b"one")
            .expect("missing pointer is created");
        assert!(matches!(
            store.compare_and_swap("catalog/CURRENT", None, b"two"),
            Err(TelemetryError::StaleCatalog { .. })
        ));
        let next = store
            .compare_and_swap("catalog/CURRENT", Some(&current.version_token), b"two")
            .expect("matching pointer is replaced");
        assert_ne!(current.version_token, next.version_token);
        assert_eq!(current.content_digest, checksum_bytes(b"one"));
        assert_eq!(next.content_digest, checksum_bytes(b"two"));
    }

    #[test]
    fn publication_pages_prune_restart_and_reject_stale_writers() {
        let directory = TestDirectory::new("tier-publication");
        let artifact_directory = directory.path.join("sources");
        fs::create_dir_all(&artifact_directory).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("empty tier opens");
        let source0 = group_source(&artifact_directory, 0, 0, 1_000);
        let manifest0 = tier
            .publish_group(source0.clone())
            .expect("first group publishes");
        assert_eq!(tier.root().generation, 1);
        assert_eq!(tier.root().pages.len(), 1);
        assert_eq!(
            tier.publish_group(source0)
                .expect("identical last-group retry succeeds"),
            manifest0
        );
        assert_eq!(tier.root().generation, 1);

        tier.publish_group(group_source(&artifact_directory, 1, 10, 2_000))
            .expect("second group publishes");
        assert_eq!(tier.root().pages.len(), 1);
        tier.publish_group(group_source(&artifact_directory, 2, 20, 3_000))
            .expect("page rollover publishes");
        assert_eq!(tier.root().pages.len(), 2);
        assert_eq!(tier.root().generation, 3);

        let candidates = tier
            .candidate_groups(TierQueryRange {
                first_offset: Some(25),
                last_offset: Some(25),
                ..TierQueryRange::default()
            })
            .expect("offset pruning succeeds");
        assert_eq!(
            candidates
                .iter()
                .map(|entry| entry.group_sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        let loaded = tier
            .load_group(&candidates[0])
            .expect("candidate group loads");
        assert_eq!(loaded.group_sequence, 2);
        assert_eq!(
            tier.read_artifact(
                loaded
                    .artifact(TierArtifactKind::QueryIndex)
                    .expect("query index exists"),
                1024,
            )
            .expect("query index verifies"),
            b"query-index-2"
        );

        let reopened =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("published tier recovers");
        assert_eq!(reopened.root(), tier.root());

        let mut first_writer =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("first writer opens");
        let mut stale_writer =
            TelemetryObjectTier::open(store, ShardId::new(4), partition(), tier_config(2))
                .expect("stale writer opens");
        first_writer
            .publish_group(group_source(&artifact_directory, 3, 30, 4_000))
            .expect("first writer advances catalog");
        assert!(matches!(
            stale_writer.publish_group(group_source(&artifact_directory, 4, 40, 5_000)),
            Err(TelemetryError::StaleCatalog { .. })
        ));
    }

    #[test]
    fn startup_is_shallow_and_touched_pages_are_verified() {
        let directory = TestDirectory::new("lazy-verification");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(2), partition(), tier_config(2))
                .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 100))
            .expect("group publishes");
        let page_key = tier.root().pages[0].page_key.clone();
        write_test_file(&store.root().join(page_key), b"corrupt");

        let reopened =
            TelemetryObjectTier::open(store, ShardId::new(2), partition(), tier_config(2))
                .expect("startup does not scan every page or payload");
        assert!(matches!(
            reopened.candidate_groups(TierQueryRange::default()),
            Err(TelemetryError::CorruptTier(_))
        ));
    }

    fn block_descriptor(offset: u64) -> BlockDescriptor {
        BlockDescriptor {
            block_id: BlockId::new(0),
            stream_shard_id: ShardId::new(8),
            topic_partition: partition(),
            source_compression_cohort: CompressionCohortId::new(4),
            placement_id: CompressionPlacementId::new(5),
            dictionary_id: Some(DictionaryId::new(6)),
            compression_codec: CompressionCodec::Zstd,
            compression_level: 1,
            first_offset: LogicalOffset::new(offset),
            last_offset: LogicalOffset::new(offset),
            record_count: 1,
            source_bytes: 20,
            structural_bytes: 12,
            stored_bytes: 4,
            min_timestamp_unix_nanos: offset,
            max_timestamp_unix_nanos: offset,
            compression_temperature: CompressionTemperature::new(7).get(),
            compression_shape_hash: 8,
            compression_temperature_variance_q8: 0,
            max_compression_temperature_deviation: 0,
            object_key: None,
            object_offset: None,
        }
    }

    #[test]
    fn staged_blocks_become_exact_object_ranges() {
        let directory = TestDirectory::new("payload-pack");
        let mut catalog = BlockCatalog::default();
        let first = catalog.seal(block_descriptor(0), Arc::from(&b"abcd"[..]));
        let second = catalog.seal(block_descriptor(1), Arc::from(&b"efgh"[..]));
        let pack_path = directory.path.join("blocks.pack");
        let entries =
            write_staged_payload_pack(&catalog, &[first.block_id, second.block_id], &pack_path)
                .expect("payload pack is written");
        assert_eq!(fs::read(&pack_path).expect("pack reads"), b"abcdefgh");
        assert_eq!(entries[0].payload_offset, 0);
        assert_eq!(entries[1].payload_offset, 4);
        assert_eq!(entries[0].payload_checksum, checksum_bytes(b"abcd"));
        assert_eq!(entries[1].payload_checksum, checksum_bytes(b"efgh"));

        let query_path = directory.path.join("query.index");
        write_test_file(&query_path, b"index");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store, ShardId::new(8), partition(), tier_config(2))
                .expect("tier opens");
        let manifest = tier
            .publish_group(TierGroupSource {
                group_sequence: 0,
                checkpoint: TierCheckpoint {
                    next_placement_sequence: 1,
                    next_offset: 2,
                },
                blocks: entries,
                artifacts: vec![
                    TierArtifactSource {
                        kind: TierArtifactKind::PayloadPack,
                        name: "blocks.pack".into(),
                        path: pack_path,
                    },
                    TierArtifactSource {
                        kind: TierArtifactKind::QueryIndex,
                        name: "query.index".into(),
                        path: query_path,
                    },
                ],
            })
            .expect("group publishes");
        mark_group_offloaded(&mut catalog, &manifest)
            .expect("catalog is advanced to object ranges");
        assert!(catalog.staged_payload(first.block_id).is_none());
        assert!(catalog.staged_payload(second.block_id).is_none());
        assert_eq!(
            catalog
                .get(first.block_id)
                .expect("first block exists")
                .object_offset,
            Some(0)
        );
        assert_eq!(
            catalog
                .get(second.block_id)
                .expect("second block exists")
                .object_offset,
            Some(4)
        );
    }

    #[test]
    fn invalid_group_offload_is_atomic() {
        let directory = TestDirectory::new("atomic-offload");
        let mut catalog = BlockCatalog::default();
        let first = catalog.seal(block_descriptor(0), Arc::from(&b"abcd"[..]));
        let second = catalog.seal(block_descriptor(1), Arc::from(&b"efgh"[..]));
        let pack_path = directory.path.join("blocks.pack");
        let entries =
            write_staged_payload_pack(&catalog, &[first.block_id, second.block_id], &pack_path)
                .expect("payload pack is written");
        let query_path = directory.path.join("query.index");
        write_test_file(&query_path, b"index");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store, ShardId::new(8), partition(), tier_config(2))
                .expect("tier opens");
        let mut manifest = tier
            .publish_group(TierGroupSource {
                group_sequence: 0,
                checkpoint: TierCheckpoint {
                    next_placement_sequence: 1,
                    next_offset: 2,
                },
                blocks: entries,
                artifacts: vec![
                    TierArtifactSource {
                        kind: TierArtifactKind::PayloadPack,
                        name: "blocks.pack".into(),
                        path: pack_path,
                    },
                    TierArtifactSource {
                        kind: TierArtifactKind::QueryIndex,
                        name: "query.index".into(),
                        path: query_path,
                    },
                ],
            })
            .expect("group publishes");
        manifest.blocks[1].block_id = 999;

        assert!(matches!(
            mark_group_offloaded(&mut catalog, &manifest),
            Err(TelemetryError::UnknownBlock(999))
        ));
        for block_id in [first.block_id, second.block_id] {
            assert!(catalog.staged_payload(block_id).is_some());
            assert!(
                catalog
                    .get(block_id)
                    .expect("block remains")
                    .object_key
                    .is_none()
            );
        }
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: LocalObjectStore,
        range_reads: AtomicUsize,
    }

    impl CountingStore {
        fn new(inner: LocalObjectStore) -> Self {
            Self {
                inner,
                range_reads: AtomicUsize::new(0),
            }
        }
    }

    impl TelemetryObjectStore for CountingStore {
        fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
            self.inner.put_bytes_if_absent(key, bytes)
        }

        fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
            self.inner.put_file_if_absent(key, source)
        }

        fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
            self.inner.get(key, max_bytes)
        }

        fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
            self.range_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_range(key, range)
        }

        fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
            self.inner.head(key)
        }

        fn compare_and_swap(
            &self,
            key: &str,
            expected_version: Option<&str>,
            bytes: &[u8],
        ) -> TelemetryResult<ObjectMetadata> {
            self.inner.compare_and_swap(key, expected_version, bytes)
        }
    }

    #[test]
    fn ssd_cache_reuses_ranges_and_stays_byte_bounded() {
        let directory = TestDirectory::new("ssd-cache");
        let local =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let store = CountingStore::new(local);
        store
            .put_bytes_if_absent("payload/object", b"abcdefghijklmnop")
            .expect("payload object is written");
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 2 * (CACHE_HEADER_BYTES as u64 + 4),
                chunk_bytes: 4,
                max_read_bytes: 16,
            },
        )
        .expect("cache opens");

        assert_eq!(
            cache
                .read_range(&store, "payload/object", 4..12)
                .expect("first range reads"),
            b"efghijkl"
        );
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 2);
        assert_eq!(
            cache
                .read_range(&store, "payload/object", 4..12)
                .expect("second range reads from SSD"),
            b"efghijkl"
        );
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 2);

        cache
            .read_range(&store, "payload/object", 12..16)
            .expect("third chunk is admitted and evicts the oldest");
        assert!(cache.used_bytes() <= 2 * (CACHE_HEADER_BYTES as u64 + 4));
        cache
            .read_range(&store, "payload/object", 4..8)
            .expect("evicted chunk can be fetched again");
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn batched_ssd_ranges_load_a_shared_chunk_once() {
        let directory = TestDirectory::new("ssd-cache-batch");
        let local =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let store = CountingStore::new(local);
        let metadata = store
            .put_bytes_if_absent("payload/object", b"abcdefghijklmnop")
            .expect("payload object is written");
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 4 * (CACHE_HEADER_BYTES as u64 + 4),
                chunk_bytes: 4,
                max_read_bytes: 16,
            },
        )
        .expect("cache opens");
        let ranges = [4..6, 6..8];

        assert_eq!(
            cache
                .read_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
                .expect("batched ranges read"),
            [b"ef".to_vec(), b"gh".to_vec()]
        );
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().source_bytes, 4);
        cache
            .read_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
            .expect("batched ranges are served from SSD");
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn cached_catalog_reads_do_not_revisit_object_storage() {
        let directory = TestDirectory::new("catalog-cache");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let local =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier = TelemetryObjectTier::open(
            CountingStore::new(local),
            ShardId::new(5),
            partition(),
            tier_config(16),
        )
        .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
            .expect("group publishes");
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 32 * (CACHE_HEADER_BYTES as u64 + 1_024),
                chunk_bytes: 1_024,
                max_read_bytes: 64 * 1_024,
            },
        )
        .expect("cache opens");

        let groups = tier
            .candidate_groups_cached(TierQueryRange::default(), &cache)
            .expect("catalog page loads through cache");
        let manifest = tier
            .load_group_cached(&groups[0], &cache)
            .expect("manifest loads through cache");
        let artifact = manifest
            .artifact(TierArtifactKind::QueryIndex)
            .expect("query index exists");
        assert_eq!(
            tier.read_artifact_cached(artifact, 1_024, &cache)
                .expect("artifact loads through cache"),
            b"query-index-0"
        );
        let reads_after_first_query = tier.object_store().range_reads.load(Ordering::Relaxed);
        assert!(reads_after_first_query >= 3);

        let groups = tier
            .candidate_groups_cached(TierQueryRange::default(), &cache)
            .expect("catalog page is cached");
        let manifest = tier
            .load_group_cached(&groups[0], &cache)
            .expect("manifest is cached");
        tier.read_artifact_cached(
            manifest
                .artifact(TierArtifactKind::QueryIndex)
                .expect("query index exists"),
            1_024,
            &cache,
        )
        .expect("artifact is cached");
        assert_eq!(
            tier.object_store().range_reads.load(Ordering::Relaxed),
            reads_after_first_query
        );
    }
}
