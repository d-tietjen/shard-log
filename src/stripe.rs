use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};
use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::ingest_pack::{
    IndexedIngestFrame, decode_indexed_ingest_frames, decode_indexed_ingest_records,
};
use crate::{
    BlockCatalog, BlockDescriptor, BlockId, CompressionBlockCollator, CompressionBlockScore,
    CompressionCodec, CompressionCohortId, CompressionLocalityConfig, CompressionLocalityRecord,
    CompressionLocalityStats, CompressionPlacement, CompressionPlacementId, CompressionTemperature,
    DictionaryCache, DictionaryCatalog, DictionaryCatalogSnapshot, DictionaryId, DictionaryInsert,
    DurableLogRecord, LogDbError, LogDbResult, LogMatch, LogQuery, MessageFingerprint,
    OtlpLogDecoder, OtlpLogEvent, QueryOrder, RealtimeDictionaryObserver,
    RealtimeDictionaryTrainer, RecordRef, fingerprint_message, scan_message_terms,
    structural::{encode_structural_block, row_source_bytes},
};

const MAX_REBALANCE_PASSES: u8 = 3;
const MESSAGE_TERM_CACHE_ENTRIES: usize = 1_024;
const FIELD_CACHE_ENTRIES: usize = 1_024;

type PartitionTermIds = HashMap<Arc<str>, usize>;
type PartitionFieldIds = HashMap<Arc<str>, HashMap<Arc<str>, usize>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrdinalRun {
    first: u32,
    last: u32,
}

#[derive(Debug, Default)]
struct HotPostingList {
    runs: Vec<OrdinalRun>,
    cardinality: usize,
}

impl HotPostingList {
    fn push(&mut self, ordinal: u32) {
        self.push_range(ordinal, ordinal);
    }

    fn push_range(&mut self, first: u32, final_ordinal: u32) {
        debug_assert!(first <= final_ordinal);
        let added = (final_ordinal - first) as usize + 1;
        if let Some(last_run) = self.runs.last_mut()
            && last_run.last.checked_add(1) == Some(first)
        {
            last_run.last = final_ordinal;
            self.cardinality += added;
            return;
        }
        debug_assert!(
            self.runs
                .last()
                .is_none_or(|last_run| last_run.last < first),
            "hot postings must be appended in ordinal order"
        );
        self.runs.push(OrdinalRun {
            first,
            last: final_ordinal,
        });
        self.cardinality += added;
    }

    fn is_empty_in(&self, start: u32, end: u32) -> bool {
        self.runs
            .binary_search_by(|run| {
                if run.last < start {
                    std::cmp::Ordering::Less
                } else if run.first >= end {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_err()
    }

    fn collect_in(
        &self,
        start: u32,
        end: u32,
        order: QueryOrder,
        limit: Option<usize>,
    ) -> Vec<u32> {
        let take = limit.unwrap_or(usize::MAX);
        let mut ordinals = Vec::new();
        match order {
            QueryOrder::OldestFirst => {
                for run in &self.runs {
                    if run.last < start {
                        continue;
                    }
                    if run.first >= end || ordinals.len() == take {
                        break;
                    }
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    ordinals.extend((first..=last).take(take - ordinals.len()));
                }
            }
            QueryOrder::NewestFirst => {
                for run in self.runs.iter().rev() {
                    if run.first >= end {
                        continue;
                    }
                    if run.last < start || ordinals.len() == take {
                        break;
                    }
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    ordinals.extend((first..=last).rev().take(take - ordinals.len()));
                }
            }
        }
        ordinals
    }
}

/// Resource limits for one shard-aligned log stripe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeConfig {
    /// Approximate uncompressed byte threshold at which an active block seals.
    pub target_block_bytes: u64,
    /// Byte capacity of the stripe-local compression-dictionary LRU.
    pub dictionary_cache_bytes: usize,
    /// Zstandard level used by this stripe's owner-local encoder context.
    pub compression_level: i32,
    /// Fixed-capacity algorithmic compression-locality routing settings.
    pub compression_locality: CompressionLocalityConfig,
}

impl Default for StripeConfig {
    fn default() -> Self {
        Self {
            target_block_bytes: 8 * 1024 * 1024,
            dictionary_cache_bytes: 16 * 1024 * 1024,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig::default(),
        }
    }
}

impl StripeConfig {
    fn validate(&self) -> LogDbResult<()> {
        if self.target_block_bytes == 0 {
            return Err(LogDbError::InvalidConfig(
                "target_block_bytes must be nonzero",
            ));
        }
        if self.dictionary_cache_bytes == 0 {
            return Err(LogDbError::InvalidConfig(
                "dictionary_cache_bytes must be nonzero",
            ));
        }
        if !zstd::compression_level_range().contains(&self.compression_level) {
            return Err(LogDbError::InvalidConfig(
                "compression_level is outside zstd's supported range",
            ));
        }
        self.compression_locality
            .validate()
            .map_err(LogDbError::InvalidConfig)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ActiveBlockKey {
    topic_partition: TopicPartition,
    source_compression_cohort: CompressionCohortId,
    placement_id: CompressionPlacementId,
    dictionary_id: Option<DictionaryId>,
}

#[derive(Debug)]
struct DictionarySelection {
    dictionary_id: Option<DictionaryId>,
    payload: Option<Arc<[u8]>>,
}

#[derive(Debug, Clone)]
struct IndexedRecord {
    record: DurableLogRecord,
    tentative_placement: CompressionPlacement,
    temperature: CompressionTemperature,
    final_placement: Option<CompressionPlacement>,
}

#[derive(Debug)]
struct CachedMessageTerms {
    topic_partition: TopicPartition,
    message: Arc<str>,
    term_ids: Arc<[usize]>,
}

#[derive(Debug)]
struct CachedFields {
    topic_partition: TopicPartition,
    fields: Arc<Vec<crate::MetadataField>>,
    field_ids: Arc<[usize]>,
}

#[derive(Debug, Default)]
struct PartitionIndex {
    records: Vec<IndexedRecord>,
    term_ids: PartitionTermIds,
    term_postings: Vec<HotPostingList>,
    field_ids: PartitionFieldIds,
    field_postings: Vec<HotPostingList>,
    indexed_through: Option<LogicalOffset>,
}

#[derive(Debug)]
struct IndexedFrameAppend {
    first_offset: LogicalOffset,
    last_offset: LogicalOffset,
    record_count: u32,
    frames: Vec<IndexedIngestFrame>,
}

#[derive(Debug, Default)]
struct IndexedFramePartition {
    appends: Vec<IndexedFrameAppend>,
    indexed_through: Option<LogicalOffset>,
}

#[derive(Clone, Copy)]
struct IndexedFrameQuery<'a> {
    query: &'a LogQuery,
    append: &'a IndexedFrameAppend,
    frame: &'a IndexedIngestFrame,
}

impl PartitionIndex {
    fn record(&self, offset: LogicalOffset) -> Option<&IndexedRecord> {
        self.records
            .binary_search_by_key(&offset, |record| record.record.record_ref.offset)
            .ok()
            .and_then(|index| self.records.get(index))
    }

    fn record_mut(&mut self, offset: LogicalOffset) -> Option<&mut IndexedRecord> {
        self.records
            .binary_search_by_key(&offset, |record| record.record.record_ref.offset)
            .ok()
            .and_then(|index| self.records.get_mut(index))
    }

    fn last_offset(&self) -> Option<LogicalOffset> {
        self.records
            .last()
            .map(|record| record.record.record_ref.offset)
    }
}

#[derive(Debug, Clone)]
struct PendingRecord {
    record: DurableLogRecord,
    source_bytes: u64,
    fingerprint: MessageFingerprint,
}

impl PendingRecord {
    fn locality(&self) -> CompressionLocalityRecord {
        CompressionLocalityRecord {
            fingerprint: self.fingerprint,
            source_bytes: self.source_bytes,
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveBlock {
    first_offset: LogicalOffset,
    last_offset: LogicalOffset,
    record_count: u32,
    source_bytes: u64,
    min_timestamp_unix_nanos: u64,
    max_timestamp_unix_nanos: u64,
    dictionary_payload: Option<Arc<[u8]>>,
    rebalance_passes: u8,
    records: Vec<PendingRecord>,
}

impl ActiveBlock {
    fn new(record: PendingRecord, dictionary_payload: Option<Arc<[u8]>>) -> Self {
        let durable = &record.record;
        Self {
            first_offset: durable.record_ref.offset,
            last_offset: durable.record_ref.offset,
            record_count: 1,
            source_bytes: record.source_bytes,
            min_timestamp_unix_nanos: durable.timestamp_unix_nanos,
            max_timestamp_unix_nanos: durable.timestamp_unix_nanos,
            dictionary_payload,
            rebalance_passes: 0,
            records: vec![record],
        }
    }

    fn from_records(
        mut records: Vec<PendingRecord>,
        dictionary_payload: Option<Arc<[u8]>>,
        rebalance_passes: u8,
    ) -> Self {
        if records.windows(2).any(|adjacent| {
            adjacent[0].record.record_ref.offset > adjacent[1].record.record_ref.offset
        }) {
            records.sort_unstable_by_key(|record| record.record.record_ref.offset);
        }
        let mut records = records.into_iter();
        let first = records
            .next()
            .expect("a collation assignment always contains records");
        let mut active = Self::new(first, dictionary_payload);
        active.rebalance_passes = rebalance_passes;
        for record in records {
            active.append(record);
        }
        active
    }

    fn append(&mut self, record: PendingRecord) {
        self.first_offset = self.first_offset.min(record.record.record_ref.offset);
        self.last_offset = self.last_offset.max(record.record.record_ref.offset);
        self.record_count = self
            .record_count
            .checked_add(1)
            .expect("a bounded active block cannot contain more than u32 records");
        self.source_bytes = self.source_bytes.saturating_add(record.source_bytes);
        self.min_timestamp_unix_nanos = self
            .min_timestamp_unix_nanos
            .min(record.record.timestamp_unix_nanos);
        self.max_timestamp_unix_nanos = self
            .max_timestamp_unix_nanos
            .max(record.record.timestamp_unix_nanos);
        self.records.push(record);
    }

    fn append_block(&mut self, other: Self) {
        self.first_offset = self.first_offset.min(other.first_offset);
        self.last_offset = self.last_offset.max(other.last_offset);
        self.record_count = self
            .record_count
            .checked_add(other.record_count)
            .expect("a bounded active block cannot contain more than u32 records");
        self.source_bytes = self.source_bytes.saturating_add(other.source_bytes);
        self.min_timestamp_unix_nanos = self
            .min_timestamp_unix_nanos
            .min(other.min_timestamp_unix_nanos);
        self.max_timestamp_unix_nanos = self
            .max_timestamp_unix_nanos
            .max(other.max_timestamp_unix_nanos);
        self.rebalance_passes = self.rebalance_passes.max(other.rebalance_passes);
        let total_records = self.records.len().saturating_add(other.records.len());
        let mut left = std::mem::take(&mut self.records).into_iter().peekable();
        let mut right = other.records.into_iter().peekable();
        let mut merged = Vec::with_capacity(total_records);
        while let (Some(left_record), Some(right_record)) = (left.peek(), right.peek()) {
            if left_record.record.record_ref.offset <= right_record.record.record_ref.offset {
                merged.push(left.next().expect("left record was present"));
            } else {
                merged.push(right.next().expect("right record was present"));
            }
        }
        merged.extend(left);
        merged.extend(right);
        self.records = merged;
    }
}

/// Reusable compression context owned exclusively by one log stripe.
///
/// Dictionary changes occur only when a block seals. The context is never
/// shared with another shard, so the ingest path avoids both locks and cache
/// line contention from a global compressor pool.
struct StripeCompressor {
    zstd_level: i32,
    active_dictionary: Option<DictionaryId>,
    zstd: zstd::bulk::Compressor<'static>,
}

impl std::fmt::Debug for StripeCompressor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StripeCompressor")
            .field("zstd_level", &self.zstd_level)
            .field("active_dictionary", &self.active_dictionary)
            .finish_non_exhaustive()
    }
}

impl StripeCompressor {
    fn new(zstd_level: i32) -> LogDbResult<Self> {
        Ok(Self {
            zstd_level,
            active_dictionary: None,
            zstd: zstd::bulk::Compressor::new(zstd_level)
                .map_err(|error| LogDbError::CompressionFailed(error.to_string()))?,
        })
    }

    fn compress(
        &mut self,
        source: &[u8],
        dictionary_id: Option<DictionaryId>,
        dictionary_payload: Option<&[u8]>,
    ) -> LogDbResult<Vec<u8>> {
        if self.active_dictionary != dictionary_id {
            let dictionary = match (dictionary_id, dictionary_payload) {
                (Some(_), Some(payload)) => payload,
                (Some(dictionary_id), None) => {
                    return Err(LogDbError::MissingDictionary(dictionary_id));
                }
                (None, _) => &[],
            };
            self.zstd
                .set_dictionary(self.zstd_level, dictionary)
                .map_err(|error| LogDbError::CompressionFailed(error.to_string()))?;
            self.active_dictionary = dictionary_id;
        }
        self.zstd
            .compress(source)
            .map_err(|error| LogDbError::CompressionFailed(error.to_string()))
    }
}

/// Result of publishing one durable append into the hot index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReceipt {
    /// Record made visible to term and metadata queries.
    pub record_ref: RecordRef,
    /// Indexed watermark after the publication.
    pub indexed_through: LogicalOffset,
    /// Per-record temperature used for block scoring.
    pub compression_temperature: CompressionTemperature,
    /// Tentative collection lane; final placement is a block decision.
    pub tentative_compression_placement: CompressionPlacement,
    /// Blocks sealed as a result of this append and bounded redistribution.
    pub sealed_blocks: Vec<BlockDescriptor>,
}

struct AppliedRecord {
    receipt: IndexReceipt,
    ordinal: u32,
    term_ids: Option<Arc<[usize]>>,
    field_ids: Option<Arc<[usize]>>,
}

/// Integration point called by the shard-stream worker after durable append.
///
/// This deliberately uses a post-durability callback. The append log remains
/// authoritative; index loss after a process failure can be repaired by
/// replaying records from the last sealed index-block watermark.
pub trait ShardStreamDurableSink {
    /// Publishes a durable log event into the shard-local hot index.
    fn on_durable_append(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt>;
}

/// A lock-free-by-ownership hot index for one shard-stream physical shard.
///
/// The type intentionally exposes mutation only through `&mut self`. The
/// shard-stream worker that owns the corresponding physical shard is therefore
/// the sole writer; readers consume immutable snapshots at a higher query
/// layer or are scheduled on that same stripe. This avoids a global concurrent
/// map on the ingestion path.
#[derive(Debug)]
pub struct LogStripe {
    stream_shard_id: ShardId,
    config: StripeConfig,
    partitions: HashMap<TopicPartition, PartitionIndex>,
    indexed_frame_partitions: HashMap<TopicPartition, IndexedFramePartition>,
    message_term_cache: Vec<Option<CachedMessageTerms>>,
    field_cache: Vec<Option<CachedFields>>,
    active_blocks: HashMap<ActiveBlockKey, ActiveBlock>,
    catalog: BlockCatalog,
    placement_dictionaries: HashMap<CompressionPlacementId, DictionaryId>,
    dictionary_cache: DictionaryCache,
    dictionary_catalog: Option<Arc<DictionaryCatalog>>,
    dictionary_snapshot: Option<Arc<DictionaryCatalogSnapshot>>,
    dictionary_generation: u64,
    realtime_dictionary: Option<RealtimeDictionaryObserver>,
    block_collator: CompressionBlockCollator,
    compressor: StripeCompressor,
}

impl LogStripe {
    /// Creates a stripe owned by one physical shard-stream shard.
    pub fn new(stream_shard_id: ShardId, config: StripeConfig) -> LogDbResult<Self> {
        config.validate()?;
        let compression_level = config.compression_level;
        let block_collator = CompressionBlockCollator::new(
            config.compression_locality.clone(),
            config.target_block_bytes,
        )?;
        Ok(Self {
            stream_shard_id,
            dictionary_cache: DictionaryCache::new(config.dictionary_cache_bytes)?,
            config,
            partitions: HashMap::new(),
            indexed_frame_partitions: HashMap::new(),
            message_term_cache: std::iter::repeat_with(|| None)
                .take(MESSAGE_TERM_CACHE_ENTRIES)
                .collect(),
            field_cache: std::iter::repeat_with(|| None)
                .take(FIELD_CACHE_ENTRIES)
                .collect(),
            active_blocks: HashMap::new(),
            catalog: BlockCatalog::default(),
            placement_dictionaries: HashMap::new(),
            dictionary_catalog: None,
            dictionary_snapshot: None,
            dictionary_generation: 0,
            realtime_dictionary: None,
            block_collator,
            compressor: StripeCompressor::new(compression_level)?,
        })
    }

    /// Creates a stripe that receives immutable dictionary publications from a
    /// shared control-plane catalog.
    pub fn with_dictionary_catalog(
        stream_shard_id: ShardId,
        config: StripeConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> LogDbResult<Self> {
        let mut stripe = Self::new(stream_shard_id, config)?;
        stripe.dictionary_catalog = Some(dictionary_catalog);
        stripe.refresh_dictionary_catalog()?;
        Ok(stripe)
    }

    /// Creates a stripe that contributes sealed blocks to a bounded real-time
    /// dictionary learner and adopts accepted immutable generations.
    pub fn with_realtime_dictionary(
        stream_shard_id: ShardId,
        config: StripeConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> LogDbResult<Self> {
        let mut stripe = Self::with_dictionary_catalog(stream_shard_id, config, trainer.catalog())?;
        stripe.realtime_dictionary = Some(trainer.observer());
        Ok(stripe)
    }

    /// Attaches a non-blocking real-time dictionary observer.
    ///
    /// The observer must publish into the same catalog configured for this
    /// stripe. A full learner queue drops only the observation, never the block.
    pub fn attach_realtime_dictionary(&mut self, observer: RealtimeDictionaryObserver) {
        self.realtime_dictionary = Some(observer);
    }

    /// Returns the shard-stream physical shard that owns this stripe.
    #[must_use]
    pub const fn stream_shard_id(&self) -> ShardId {
        self.stream_shard_id
    }

    /// Returns the visible indexed watermark for a partition.
    #[must_use]
    pub fn indexed_through(&self, topic_partition: TopicPartition) -> Option<LogicalOffset> {
        let record_watermark = self
            .partitions
            .get(&topic_partition)
            .and_then(|partition| partition.indexed_through);
        let frame_watermark = self
            .indexed_frame_partitions
            .get(&topic_partition)
            .and_then(|partition| partition.indexed_through);
        record_watermark.max(frame_watermark)
    }

    /// Returns the local catalog of sealed data blocks.
    #[must_use]
    pub const fn catalog(&self) -> &BlockCatalog {
        &self.catalog
    }

    /// Returns the local catalog of sealed data blocks for offload bookkeeping.
    pub fn catalog_mut(&mut self) -> &mut BlockCatalog {
        &mut self.catalog
    }

    /// Returns the stripe-local cache of immutable compression dictionaries.
    #[must_use]
    pub const fn dictionary_cache(&self) -> &DictionaryCache {
        &self.dictionary_cache
    }

    /// Returns the stripe-local cache of immutable compression dictionaries.
    pub fn dictionary_cache_mut(&mut self) -> &mut DictionaryCache {
        &mut self.dictionary_cache
    }

    /// Returns the last immutable catalog generation observed by this stripe.
    #[must_use]
    pub const fn dictionary_generation(&self) -> u64 {
        self.dictionary_generation
    }

    /// Returns cumulative diagnostics from this stripe's block collator.
    #[must_use]
    pub fn compression_collation_stats(&self) -> CompressionLocalityStats {
        self.block_collator.stats()
    }

    /// Returns the final block placement once the record's block has sealed.
    #[must_use]
    pub fn final_compression_placement(
        &self,
        record_ref: RecordRef,
    ) -> Option<CompressionPlacement> {
        self.partitions
            .get(&record_ref.topic_partition)
            .and_then(|partition| partition.record(record_ref.offset))
            .and_then(|record| record.final_placement)
    }

    /// Adopts control-plane state at an append boundary.
    pub fn begin_append_batch(&mut self) -> LogDbResult<bool> {
        self.refresh_dictionary_catalog()
    }

    /// Adopts the latest immutable dictionary snapshot at a batch boundary.
    ///
    /// This is deliberately explicit: individual records only inspect the
    /// stripe-owned assignment map and LRU. The durable sink invokes it once
    /// before each append batch, while embedded callers can choose their own
    /// safe batch boundary.
    pub fn refresh_dictionary_catalog(&mut self) -> LogDbResult<bool> {
        let Some(dictionary_catalog) = &self.dictionary_catalog else {
            return Ok(false);
        };
        let snapshot = dictionary_catalog.snapshot()?;
        if snapshot.generation() == self.dictionary_generation {
            return Ok(false);
        }

        self.placement_dictionaries.clear();
        for (placement_id, dictionary_id) in snapshot.assignments() {
            self.placement_dictionaries
                .insert(placement_id, dictionary_id);
        }
        self.dictionary_generation = snapshot.generation();
        self.dictionary_snapshot = Some(snapshot);
        Ok(true)
    }

    /// Installs an immutable dictionary for future blocks in a placement.
    ///
    /// Existing active blocks retain their previous dictionary identifier, so a
    /// dictionary rotation never makes already accepted log records ambiguous.
    pub fn install_dictionary(
        &mut self,
        placement_id: CompressionPlacementId,
        dictionary_id: DictionaryId,
        payload: Arc<[u8]>,
    ) -> LogDbResult<DictionaryInsert> {
        if let Some(dictionary_catalog) = &self.dictionary_catalog {
            dictionary_catalog.publish(placement_id, dictionary_id, Arc::clone(&payload))?;
            self.refresh_dictionary_catalog()?;
        } else {
            self.placement_dictionaries
                .insert(placement_id, dictionary_id);
        }
        let insert = self.dictionary_cache.insert(dictionary_id, payload)?;
        Ok(insert)
    }

    /// Applies a record only after the corresponding shard-stream append is durable.
    ///
    /// Index postings are written before the visible watermark advances. A
    /// query constrained to [`Self::indexed_through`] consequently cannot see
    /// an incomplete posting update.
    pub fn apply_durable(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt> {
        if record.stream_shard_id != self.stream_shard_id {
            return Err(LogDbError::WrongStripe {
                expected: self.stream_shard_id,
                observed: record.stream_shard_id,
            });
        }
        if self
            .partitions
            .get(&record.record_ref.topic_partition)
            .and_then(|partition| partition.record(record.record_ref.offset))
            .is_some()
        {
            return Err(LogDbError::DuplicateRecord {
                partition: record.record_ref.topic_partition,
                offset: record.record_ref.offset,
            });
        }
        self.apply_durable_new(record)
    }

    fn apply_durable_new(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt> {
        self.apply_durable_new_inner(record, true)
            .map(|applied| applied.receipt)
    }

    fn apply_durable_new_inner(
        &mut self,
        record: DurableLogRecord,
        index_record: bool,
    ) -> LogDbResult<AppliedRecord> {
        self.validate_offset(&record)?;

        let record_source_bytes = row_source_bytes(&record)?;
        let fingerprint = if self.block_collator.is_enabled() {
            fingerprint_message(&record.message, &record.fields)
        } else {
            MessageFingerprint {
                shape_hash: 0,
                locality_signature: 0,
            }
        };
        let compression_temperature = CompressionTemperature::new(fingerprint.locality_signature);
        let tentative_compression_placement = self
            .block_collator
            .tentative_placement(record.compression_cohort, fingerprint);
        let dictionary = self.resolve_dictionary(tentative_compression_placement.placement_id)?;
        let active_key = ActiveBlockKey {
            topic_partition: record.record_ref.topic_partition,
            source_compression_cohort: record.compression_cohort,
            placement_id: tentative_compression_placement.placement_id,
            dictionary_id: dictionary.dictionary_id,
        };
        let reference = record.record_ref;
        let pending = PendingRecord {
            record: record.clone(),
            source_bytes: record_source_bytes,
            fingerprint,
        };
        let record_ordinal = {
            let partition = self
                .partitions
                .entry(reference.topic_partition)
                .or_default();
            let record_ordinal =
                u32::try_from(partition.records.len()).map_err(|_| LogDbError::RecordTooLarge)?;
            partition.records.push(IndexedRecord {
                record: record.clone(),
                tentative_placement: tentative_compression_placement,
                temperature: compression_temperature,
                final_placement: None,
            });
            record_ordinal
        };

        let next_source_bytes = self.active_blocks.get(&active_key).map_or_else(
            || record_source_bytes,
            |active| active.source_bytes.saturating_add(record_source_bytes),
        );
        let sealed_result = if next_source_bytes >= self.config.target_block_bytes {
            let active = match self.active_blocks.remove(&active_key) {
                Some(mut active) => {
                    active.append(pending);
                    active
                }
                None => ActiveBlock::new(pending, dictionary.payload),
            };
            self.rebalance_block(active_key, active, false)
        } else {
            match self.active_blocks.get_mut(&active_key) {
                Some(active) => {
                    active.append(pending);
                }
                None => {
                    self.active_blocks
                        .insert(active_key, ActiveBlock::new(pending, dictionary.payload));
                }
            }
            Ok(Vec::new())
        };
        let sealed_blocks = match sealed_result {
            Ok(sealed_blocks) => sealed_blocks,
            Err(error) => {
                let removed = self
                    .partitions
                    .get_mut(&reference.topic_partition)
                    .and_then(|partition| partition.records.pop());
                debug_assert!(
                    removed.is_some_and(|removed| removed.record.record_ref == reference)
                );
                return Err(error);
            }
        };

        let (term_ids, field_ids) = if index_record {
            let term_ids = self.index_terms(&record, record_ordinal);
            let field_ids = self.index_fields(&record, record_ordinal);
            // This assignment is deliberately last: it is the publication
            // barrier for readers sharing this stripe's ordering domain.
            self.partitions
                .get_mut(&reference.topic_partition)
                .expect("record partition was inserted")
                .indexed_through = Some(reference.offset);
            (Some(term_ids), Some(field_ids))
        } else {
            (None, None)
        };

        Ok(AppliedRecord {
            receipt: IndexReceipt {
                record_ref: reference,
                indexed_through: reference.offset,
                compression_temperature,
                tentative_compression_placement,
                sealed_blocks,
            },
            ordinal: record_ordinal,
            term_ids,
            field_ids,
        })
    }

    /// Publishes OTLP events after shard-stream has made their append durable.
    ///
    /// The live ingestion path should decode the export once before appending,
    /// set shard-stream's `record_count` to `events.len()`, then pass the same
    /// events here after it receives `first_offset` in the append response.
    pub fn apply_otlp_events(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: impl IntoIterator<Item = OtlpLogEvent>,
    ) -> LogDbResult<Vec<IndexReceipt>> {
        self.begin_append_batch()?;
        let events = events.into_iter().collect::<Vec<_>>();
        validate_batch_offset_range(topic_partition, first_offset, events.len())?;
        if self.can_index_as_homogeneous_range(topic_partition, first_offset, &events) {
            self.apply_homogeneous_events(topic_partition, first_offset, events)
        } else {
            events
                .into_iter()
                .enumerate()
                .map(|(index, event)| {
                    let offset = batch_offset(topic_partition, first_offset, index)?;
                    self.apply_durable_idempotent(event.into_durable(
                        self.stream_shard_id,
                        topic_partition,
                        offset,
                    ))
                })
                .collect()
        }
    }

    /// Publishes one durable compressed ingest pack without rebuilding a
    /// second per-record posting index.
    ///
    /// The authoritative compressed cohort frames remain resident and the
    /// compressor-derived indexes select candidates. Exact bodies and fields
    /// are reconstructed only for candidate records during lookup.
    pub(crate) fn apply_indexed_ingest_pack(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
        transient_context: Option<&[u8]>,
    ) -> LogDbResult<()> {
        if record_count == 0 {
            return Err(LogDbError::InvalidConfig(
                "compressed ingest append must contain records",
            ));
        }
        let last_offset = batch_offset(
            topic_partition,
            first_offset,
            usize::try_from(record_count - 1)
                .map_err(|_| LogDbError::OffsetExhausted(topic_partition))?,
        )?;
        if let Some(previous) = self.indexed_through(topic_partition)
            && first_offset <= previous
        {
            let expected = previous
                .get()
                .checked_add(1)
                .map(LogicalOffset::new)
                .ok_or(LogDbError::OffsetExhausted(topic_partition))?;
            return Err(LogDbError::OffsetOutOfOrder {
                partition: topic_partition,
                expected,
                observed: first_offset,
            });
        }
        let frames = decode_indexed_ingest_frames(payload, transient_context, record_count)?;
        let partition = self
            .indexed_frame_partitions
            .entry(topic_partition)
            .or_default();
        partition.appends.push(IndexedFrameAppend {
            first_offset,
            last_offset,
            record_count,
            frames,
        });
        // Publication barrier: readers never observe a watermark before all
        // frame metadata and embedded index views are installed.
        partition.indexed_through = Some(last_offset);
        Ok(())
    }

    /// Decodes and publishes one OTLP `ExportLogsServiceRequest`.
    ///
    /// This convenience method is suited to replay and tests. A live OTLP
    /// receiver should instead decode before the shard-stream append, use the
    /// decoded event count for reservation, then call [`Self::apply_otlp_events`]
    /// after the durable append response.
    pub fn apply_otlp_export(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        payload: &[u8],
    ) -> LogDbResult<Vec<IndexReceipt>> {
        self.apply_otlp_events(
            topic_partition,
            first_offset,
            OtlpLogDecoder.decode(payload)?,
        )
    }

    /// Seals every active block and returns their immutable descriptors.
    pub fn seal_active_blocks(&mut self) -> LogDbResult<Vec<BlockDescriptor>> {
        let active_blocks = std::mem::take(&mut self.active_blocks);
        let mut sealed = Vec::new();
        for (key, active) in active_blocks {
            sealed.extend(self.rebalance_block(key, active, true)?);
        }
        Ok(sealed)
    }

    /// Marks a sealed block as durably written to the object tier.
    pub fn mark_block_offloaded(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
    ) -> LogDbResult<()> {
        self.catalog.mark_offloaded(block_id, object_key)
    }

    /// Marks a sealed block as a byte range inside a durable object-tier pack.
    pub fn mark_block_offloaded_range(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
        object_offset: u64,
    ) -> LogDbResult<()> {
        self.catalog
            .mark_offloaded_range(block_id, object_key, object_offset)
    }

    /// Performs one exact Boolean lookup over normalized log records.
    ///
    /// This hot-index implementation is intentionally partition-local. A
    /// coordinator may fan out across selected time or tenant partitions, but
    /// that expensive choice is never implicit on a stripe.
    #[must_use]
    pub fn query(&self, query: &LogQuery) -> Vec<LogMatch> {
        self.query_checked(query).unwrap_or_default()
    }

    pub(crate) fn query_checked(&self, query: &LogQuery) -> LogDbResult<Vec<LogMatch>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        let mut matches = self.query_hot_matches(query);
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            matches.extend(self.query_indexed_frames(query, partition)?);
        }
        matches.sort_unstable_by(|left, right| query.compare(&left.record, &right.record));
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }
        Ok(matches)
    }

    pub(crate) fn query_partitions_checked(
        &self,
        queries: &[LogQuery],
    ) -> LogDbResult<Vec<LogMatch>> {
        let Some(ordering_query) = queries.first() else {
            return Ok(Vec::new());
        };
        let Some(limit) = ordering_query.limit else {
            return queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked(query)?);
                Ok(matches)
            });
        };
        if ordering_query.sort != crate::QuerySort::Timestamp
            || !queries
                .iter()
                .all(|query| same_query_across_partition(ordering_query, query))
        {
            return queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked(query)?);
                Ok(matches)
            });
        }

        let mut matches = Vec::new();
        let mut frames = Vec::new();
        for query in queries {
            if query.limit == Some(0) || query.has_invalid_range() {
                continue;
            }
            matches.extend(self.query_hot_matches(query));
            let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
                continue;
            };
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                frames.extend(
                    append
                        .frames
                        .iter()
                        .filter(|frame| frame_matches_query_bounds(query, frame))
                        .map(|frame| IndexedFrameQuery {
                            query,
                            append,
                            frame,
                        }),
                );
            }
        }
        match ordering_query.order {
            QueryOrder::NewestFirst => frames.sort_unstable_by(|left, right| {
                right
                    .frame
                    .max_timestamp_unix_nanos
                    .cmp(&left.frame.max_timestamp_unix_nanos)
            }),
            QueryOrder::OldestFirst => frames.sort_unstable_by(|left, right| {
                left.frame
                    .min_timestamp_unix_nanos
                    .cmp(&right.frame.min_timestamp_unix_nanos)
            }),
        }
        sort_and_limit_matches(&mut matches, ordering_query, limit);
        for pending in frames {
            if matches.len() == limit {
                let boundary = matches
                    .last()
                    .expect("a full result page has a boundary")
                    .record
                    .timestamp_unix_nanos;
                let cannot_improve = match ordering_query.order {
                    QueryOrder::NewestFirst => pending.frame.max_timestamp_unix_nanos < boundary,
                    QueryOrder::OldestFirst => pending.frame.min_timestamp_unix_nanos > boundary,
                };
                if cannot_improve {
                    break;
                }
            }
            matches.extend(self.query_indexed_frame(
                pending.query,
                pending.append,
                pending.frame,
            )?);
            sort_and_limit_matches(&mut matches, ordering_query, limit);
        }
        Ok(matches)
    }

    fn query_hot_matches(&self, query: &LogQuery) -> Vec<LogMatch> {
        self.partitions
            .get(&query.topic_partition)
            .map(|partition| {
                self.query_ordinals(query, partition)
                    .into_iter()
                    .filter_map(|ordinal| {
                        partition
                            .records
                            .get(ordinal as usize)
                            .map(|record| LogMatch {
                                record: record.record.clone(),
                            })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns matching durable record references without cloning record data.
    ///
    /// Posting lists are offset ordered. The query starts with the shortest
    /// list and intersects each remaining list with a linear merge, making
    /// constraint order irrelevant to the asymptotic cost.
    #[must_use]
    pub fn query_refs(&self, query: &LogQuery) -> Vec<RecordRef> {
        self.query(query)
            .into_iter()
            .map(|matched| matched.record.record_ref)
            .collect()
    }

    fn query_indexed_frames(
        &self,
        query: &LogQuery,
        partition: &IndexedFramePartition,
    ) -> LogDbResult<Vec<LogMatch>> {
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Ok(Vec::new());
        }
        let mut matches = Vec::new();
        for append in &partition.appends {
            if !append_matches_query_bounds(query, append) {
                continue;
            }
            for frame in &append.frames {
                if !frame_matches_query_bounds(query, frame) {
                    continue;
                }
                matches.extend(self.query_indexed_frame(query, append, frame)?);
            }
        }
        Ok(matches)
    }

    fn query_indexed_frame(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
    ) -> LogDbResult<Vec<LogMatch>> {
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Ok(Vec::new());
        }
        let mut candidates = None::<Vec<u32>>;
        for term in &constraints.terms {
            intersect_frame_candidates(&mut candidates, frame.index.term_candidate_ordinals(term));
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Ok(Vec::new());
            }
        }
        for (key, value) in &constraints.fields {
            intersect_frame_candidates(
                &mut candidates,
                frame.index.field_candidate_ordinals(key, value),
            );
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Ok(Vec::new());
            }
        }
        let candidates = candidates.unwrap_or_else(|| (0..frame.record_count).collect::<Vec<_>>());
        let mut matches = Vec::new();
        for decoded in decode_indexed_ingest_records(frame, &candidates)? {
            let relative_offset = decoded.offset.get();
            if relative_offset >= u64::from(append.record_count) {
                return Err(LogDbError::InvalidBlockEncoding(
                    "compressed ingest record ordinal is out of range",
                ));
            }
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(relative_offset)
                .map(LogicalOffset::new)
                .ok_or(LogDbError::OffsetExhausted(query.topic_partition))?;
            let record = DurableLogRecord {
                stream_shard_id: self.stream_shard_id,
                record_ref: RecordRef::new(query.topic_partition, absolute_offset),
                timestamp_unix_nanos: decoded.timestamp_unix_nanos,
                message: decoded.message,
                fields: decoded.fields,
                compression_cohort: frame.cohort,
            };
            if query.matches(&record) {
                matches.push(LogMatch { record });
            }
        }
        Ok(matches)
    }

    fn query_ordinals(&self, query: &LogQuery, partition: &PartitionIndex) -> Vec<u32> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Vec::new();
        }
        let record_range =
            ordinal_record_window(&partition.records, query.start_offset, query.end_offset);
        let posting_start =
            u32::try_from(record_range.start).expect("record ordinal was bounded by ingest");
        let posting_end =
            u32::try_from(record_range.end).expect("record ordinal was bounded by ingest");
        let mut posting_lists = Vec::<&HotPostingList>::with_capacity(
            constraints
                .terms
                .len()
                .saturating_add(constraints.fields.len()),
        );
        for term in constraints.terms {
            let normalized = normalize_term(term);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Vec::new();
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Vec::new();
            };
            if postings.is_empty_in(posting_start, posting_end) {
                return Vec::new();
            }
            posting_lists.push(postings);
        }
        for (key, value) in constraints.fields {
            let Some(field_id) = partition
                .field_ids
                .get(key)
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            let Some(postings) = partition.field_postings.get(*field_id) else {
                return Vec::new();
            };
            if postings.is_empty_in(posting_start, posting_end) {
                return Vec::new();
            }
            posting_lists.push(postings);
        }

        let needs_record_filter = query.needs_record_filter();
        let mut ordinals = if posting_lists.is_empty() {
            if !needs_record_filter {
                return collect_ordered_range(record_range, query.order, query.limit);
            }
            record_range
                .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
                .collect::<Vec<_>>()
        } else {
            posting_lists.sort_unstable_by_key(|postings| postings.cardinality);
            if posting_lists.len() == 1 && !needs_record_filter {
                return posting_lists[0].collect_in(
                    posting_start,
                    posting_end,
                    query.order,
                    query.limit,
                );
            }
            let mut ordinals = posting_lists[0].collect_in(
                posting_start,
                posting_end,
                QueryOrder::OldestFirst,
                None,
            );
            for postings in &posting_lists[1..] {
                intersect_ordinal_runs(&mut ordinals, &postings.runs, posting_start, posting_end);
                if ordinals.is_empty() {
                    break;
                }
            }
            ordinals
        };
        if needs_record_filter {
            ordinals.retain(|ordinal| {
                partition
                    .records
                    .get(*ordinal as usize)
                    .is_some_and(|record| query.matches_index_candidate(&record.record))
            });
        }
        if query.sort == crate::QuerySort::Timestamp {
            ordinals.sort_unstable_by(|left, right| {
                let left = partition
                    .records
                    .get(*left as usize)
                    .expect("indexed reference has a visible record");
                let right = partition
                    .records
                    .get(*right as usize)
                    .expect("indexed reference has a visible record");
                query.compare(&left.record, &right.record)
            });
        } else if query.order == QueryOrder::NewestFirst {
            ordinals.reverse();
        }
        if let Some(limit) = query.limit {
            ordinals.truncate(limit);
        }
        ordinals
    }

    fn validate_offset(&self, record: &DurableLogRecord) -> LogDbResult<()> {
        let Some(previous) = self
            .partitions
            .get(&record.record_ref.topic_partition)
            .and_then(PartitionIndex::last_offset)
        else {
            return Ok(());
        };
        let expected = previous
            .get()
            .checked_add(1)
            .map(LogicalOffset::new)
            .ok_or(LogDbError::OffsetExhausted(
                record.record_ref.topic_partition,
            ))?;
        if record.record_ref.offset <= previous {
            return Err(LogDbError::OffsetOutOfOrder {
                partition: record.record_ref.topic_partition,
                expected,
                observed: record.record_ref.offset,
            });
        }
        Ok(())
    }

    fn apply_durable_idempotent(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt> {
        let topic_partition = record.record_ref.topic_partition;
        let offset = record.record_ref.offset;
        let existing = self.partitions.get(&topic_partition).and_then(|partition| {
            let last = partition.records.last()?;
            match last.record.record_ref.offset.cmp(&offset) {
                std::cmp::Ordering::Less => None,
                std::cmp::Ordering::Equal => Some(last),
                std::cmp::Ordering::Greater => partition.record(offset),
            }
        });
        if let Some(existing) = existing {
            if existing.record != record {
                return Err(LogDbError::ConflictingRecord {
                    partition: topic_partition,
                    offset,
                });
            }
            return Ok(IndexReceipt {
                record_ref: record.record_ref,
                indexed_through: self.indexed_through(topic_partition).unwrap_or(offset),
                compression_temperature: existing.temperature,
                tentative_compression_placement: existing.tentative_placement,
                sealed_blocks: Vec::new(),
            });
        }
        self.apply_durable_new(record)
    }

    fn can_index_as_homogeneous_range(
        &self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: &[OtlpLogEvent],
    ) -> bool {
        if events.len() < 2 {
            return false;
        }
        let first = events
            .first()
            .expect("the homogeneous range minimum length was checked");
        if self
            .partitions
            .get(&topic_partition)
            .and_then(PartitionIndex::last_offset)
            .is_some_and(|last_offset| first_offset <= last_offset)
        {
            return false;
        }
        events[1..].iter().all(|event| {
            same_message(&first.message, &event.message)
                && same_fields(&first.fields, &event.fields)
        })
    }

    fn apply_homogeneous_events(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: Vec<OtlpLogEvent>,
    ) -> LogDbResult<Vec<IndexReceipt>> {
        let mut events = events.into_iter().enumerate();
        let (first_index, first_event) = events
            .next()
            .expect("homogeneous event ranges contain at least two records");
        debug_assert_eq!(first_index, 0);
        let first_applied = self.apply_durable_new_inner(
            first_event.into_durable(self.stream_shard_id, topic_partition, first_offset),
            true,
        )?;
        let first_ordinal = first_applied.ordinal;
        let term_ids = first_applied
            .term_ids
            .expect("the first homogeneous record was indexed");
        let field_ids = first_applied
            .field_ids
            .expect("the first homogeneous record was indexed");
        let mut receipts = Vec::with_capacity(events.size_hint().0.saturating_add(1));
        receipts.push(first_applied.receipt);
        let mut last_deferred = None;

        for (index, event) in events {
            let offset = batch_offset(topic_partition, first_offset, index)?;
            match self.apply_durable_new_inner(
                event.into_durable(self.stream_shard_id, topic_partition, offset),
                false,
            ) {
                Ok(applied) => {
                    debug_assert_eq!(
                        applied.ordinal,
                        first_ordinal
                            .checked_add(u32::try_from(index).expect("batch offset was bounded"))
                            .expect("record ordinal was bounded")
                    );
                    last_deferred = Some((applied.ordinal, offset));
                    receipts.push(applied.receipt);
                }
                Err(error) => {
                    if let Some((last_ordinal, last_offset)) = last_deferred {
                        self.publish_homogeneous_posting_range(
                            topic_partition,
                            first_ordinal + 1,
                            last_ordinal,
                            last_offset,
                            &term_ids,
                            &field_ids,
                        );
                    }
                    return Err(error);
                }
            }
        }

        let (last_ordinal, last_offset) =
            last_deferred.expect("homogeneous event ranges contain a deferred record");
        self.publish_homogeneous_posting_range(
            topic_partition,
            first_ordinal + 1,
            last_ordinal,
            last_offset,
            &term_ids,
            &field_ids,
        );
        Ok(receipts)
    }

    fn publish_homogeneous_posting_range(
        &mut self,
        topic_partition: TopicPartition,
        first_ordinal: u32,
        last_ordinal: u32,
        last_offset: LogicalOffset,
        term_ids: &[usize],
        field_ids: &[usize],
    ) {
        debug_assert!(first_ordinal <= last_ordinal);
        let partition = self
            .partitions
            .get_mut(&topic_partition)
            .expect("homogeneous records were inserted");
        for term_id in term_ids {
            partition
                .term_postings
                .get_mut(*term_id)
                .expect("interned term has a posting slot")
                .push_range(first_ordinal, last_ordinal);
        }
        for field_id in field_ids {
            partition
                .field_postings
                .get_mut(*field_id)
                .expect("interned field has a posting slot")
                .push_range(first_ordinal, last_ordinal);
        }
        // This assignment is the publication barrier for the deferred range.
        partition.indexed_through = Some(last_offset);
    }

    fn index_terms(&mut self, record: &DurableLogRecord, record_ordinal: u32) -> Arc<[usize]> {
        let topic_partition = record.record_ref.topic_partition;
        let cache_slot = message_term_cache_slot(topic_partition, record.message.as_bytes());
        let term_ids = if let Some(cached) = &self.message_term_cache[cache_slot]
            && cached.topic_partition == topic_partition
            && same_message(&cached.message, &record.message)
        {
            Arc::clone(&cached.term_ids)
        } else {
            let mut message_term_ids = Vec::new();
            {
                let partition = self
                    .partitions
                    .get_mut(&topic_partition)
                    .expect("record partition was inserted");
                scan_message_terms(&record.message, |term| {
                    let normalized = normalize_term(term);
                    let term_id = match partition.term_ids.get(normalized.as_ref()).copied() {
                        Some(term_id) => term_id,
                        None => {
                            let term_id = partition.term_postings.len();
                            partition
                                .term_ids
                                .insert(Arc::from(normalized.as_ref()), term_id);
                            partition.term_postings.push(HotPostingList::default());
                            term_id
                        }
                    };
                    if !message_term_ids.contains(&term_id) {
                        message_term_ids.push(term_id);
                    }
                });
            }
            let term_ids = Arc::<[usize]>::from(message_term_ids);
            self.message_term_cache[cache_slot] = Some(CachedMessageTerms {
                topic_partition,
                message: Arc::clone(&record.message),
                term_ids: Arc::clone(&term_ids),
            });
            term_ids
        };
        let term_postings = &mut self
            .partitions
            .get_mut(&topic_partition)
            .expect("record partition was inserted")
            .term_postings;
        for term_id in term_ids.iter().copied() {
            term_postings
                .get_mut(term_id)
                .expect("interned term has a posting slot")
                .push(record_ordinal);
        }
        term_ids
    }

    fn index_fields(&mut self, record: &DurableLogRecord, record_ordinal: u32) -> Arc<[usize]> {
        let topic_partition = record.record_ref.topic_partition;
        let cache_slot = field_cache_slot(topic_partition, &record.fields);
        let field_ids = if let Some(cached) = &self.field_cache[cache_slot]
            && cached.topic_partition == topic_partition
            && same_fields(&cached.fields, &record.fields)
        {
            Arc::clone(&cached.field_ids)
        } else {
            let mut record_field_ids = Vec::with_capacity(record.fields.len());
            let partition = self
                .partitions
                .get_mut(&topic_partition)
                .expect("record partition was inserted");
            for (index, field) in record.fields.iter().enumerate() {
                if record.fields[..index]
                    .iter()
                    .any(|existing| existing.key == field.key && existing.value == field.value)
                {
                    continue;
                }
                let field_id = match partition
                    .field_ids
                    .get(field.key.as_ref())
                    .and_then(|values| values.get(field.value.as_ref()))
                    .copied()
                {
                    Some(field_id) => field_id,
                    None => {
                        let field_id = partition.field_postings.len();
                        partition
                            .field_ids
                            .entry(Arc::clone(&field.key))
                            .or_default()
                            .insert(Arc::clone(&field.value), field_id);
                        partition.field_postings.push(HotPostingList::default());
                        field_id
                    }
                };
                record_field_ids.push(field_id);
            }
            let field_ids = Arc::<[usize]>::from(record_field_ids);
            self.field_cache[cache_slot] = Some(CachedFields {
                topic_partition,
                fields: Arc::clone(&record.fields),
                field_ids: Arc::clone(&field_ids),
            });
            field_ids
        };
        let field_postings = &mut self
            .partitions
            .get_mut(&topic_partition)
            .expect("record partition was inserted")
            .field_postings;
        for field_id in field_ids.iter().copied() {
            field_postings
                .get_mut(field_id)
                .expect("interned field has a posting slot")
                .push(record_ordinal);
        }
        field_ids
    }

    fn resolve_dictionary(
        &mut self,
        placement_id: CompressionPlacementId,
    ) -> LogDbResult<DictionarySelection> {
        let Some(dictionary_id) = self.placement_dictionaries.get(&placement_id).copied() else {
            return Ok(DictionarySelection {
                dictionary_id: None,
                payload: None,
            });
        };
        if let Some(payload) = self.dictionary_cache.get(dictionary_id) {
            return Ok(DictionarySelection {
                dictionary_id: Some(dictionary_id),
                payload: Some(payload),
            });
        }

        let payload = self
            .dictionary_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.dictionary(dictionary_id))
            .ok_or(LogDbError::MissingDictionary(dictionary_id))?;
        self.dictionary_cache
            .insert(dictionary_id, Arc::clone(&payload))?;
        Ok(DictionarySelection {
            dictionary_id: Some(dictionary_id),
            payload: Some(payload),
        })
    }

    fn rebalance_block(
        &mut self,
        initial_key: ActiveBlockKey,
        initial_block: ActiveBlock,
        force_seal: bool,
    ) -> LogDbResult<Vec<BlockDescriptor>> {
        if !self.block_collator.is_enabled() {
            let temperature = CompressionTemperature::new(0);
            let placement =
                CompressionPlacement::base(initial_key.source_compression_cohort, temperature);
            let score = CompressionBlockScore {
                temperature,
                shape_hash: 0,
                internal_variance_q8: 0,
                max_deviation: 0,
                source_bytes: initial_block.source_bytes,
                record_count: initial_block.records.len(),
            };
            return self
                .stage_active_block(initial_key, &initial_block, placement, score)
                .map(|descriptor| vec![descriptor]);
        }

        let mut work = vec![(initial_key, initial_block)];
        let mut sealed = Vec::new();
        while let Some((home_key, active)) = work.pop() {
            let home_dictionary_payload = active.dictionary_payload.clone();
            let next_pass = active.rebalance_passes.saturating_add(1);
            let locality_records = active
                .records
                .iter()
                .map(PendingRecord::locality)
                .collect::<Vec<_>>();
            let assignments = self.block_collator.collate(
                home_key.source_compression_cohort,
                home_key.placement_id,
                &locality_records,
            );
            let assignment_count = assignments.len();
            let mut records = active.records.into_iter().map(Some).collect::<Vec<_>>();

            for assignment in assignments {
                let placement = assignment.placement;
                let score = assignment.score;
                let group_records = assignment
                    .record_indices()
                    .map(|index| {
                        records[index]
                            .take()
                            .expect("collation membership contains each record once")
                    })
                    .collect::<Vec<_>>();
                let (target_key, dictionary_payload) =
                    if placement.placement_id == home_key.placement_id {
                        (home_key, home_dictionary_payload.clone())
                    } else {
                        let dictionary = self.resolve_dictionary(placement.placement_id)?;
                        (
                            ActiveBlockKey {
                                topic_partition: home_key.topic_partition,
                                source_compression_cohort: home_key.source_compression_cohort,
                                placement_id: placement.placement_id,
                                dictionary_id: dictionary.dictionary_id,
                            },
                            dictionary.payload,
                        )
                    };
                let mut group =
                    ActiveBlock::from_records(group_records, dictionary_payload, next_pass);

                if force_seal {
                    sealed.push(self.stage_active_block(target_key, &group, placement, score)?);
                    continue;
                }

                let merged_existing = if let Some(existing) = self.active_blocks.remove(&target_key)
                {
                    let mut existing = existing;
                    existing.append_block(group);
                    group = existing;
                    true
                } else {
                    false
                };
                if group.source_bytes < self.config.target_block_bytes {
                    self.active_blocks.insert(target_key, group);
                    continue;
                }

                let stable_home_block = !merged_existing
                    && assignment_count == 1
                    && target_key.placement_id == home_key.placement_id
                    && score.internal_variance_q8
                        <= self.config.compression_locality.split_variance_q8;
                if stable_home_block || group.rebalance_passes >= MAX_REBALANCE_PASSES {
                    sealed.push(self.stage_active_block(target_key, &group, placement, score)?);
                } else {
                    work.push((target_key, group));
                }
            }
            debug_assert!(records.iter().all(Option::is_none));
        }
        Ok(sealed)
    }

    fn stage_active_block(
        &mut self,
        key: ActiveBlockKey,
        active: &ActiveBlock,
        placement: CompressionPlacement,
        score: CompressionBlockScore,
    ) -> LogDbResult<BlockDescriptor> {
        let durable_records = active
            .records
            .iter()
            .map(|pending| pending.record.clone())
            .collect::<Vec<_>>();
        let structural = encode_structural_block(&durable_records)?;
        let structural_bytes = u64::try_from(structural.len()).unwrap_or(u64::MAX);
        let compressed = self.compressor.compress(
            &structural,
            key.dictionary_id,
            active.dictionary_payload.as_deref(),
        )?;
        let stored_bytes = u64::try_from(compressed.len()).unwrap_or(u64::MAX);
        if let Some(observer) = &self.realtime_dictionary {
            let _ = observer.observe_structural_block(key.placement_id, structural);
        }
        for pending in &active.records {
            if let Some(record) = self
                .partitions
                .get_mut(&pending.record.record_ref.topic_partition)
                .and_then(|partition| partition.record_mut(pending.record.record_ref.offset))
            {
                record.final_placement = Some(placement);
            }
        }
        Ok(self.catalog.seal(
            BlockDescriptor {
                block_id: BlockId::new(0),
                stream_shard_id: self.stream_shard_id,
                topic_partition: key.topic_partition,
                source_compression_cohort: key.source_compression_cohort,
                placement_id: key.placement_id,
                dictionary_id: key.dictionary_id,
                compression_codec: CompressionCodec::Zstd,
                compression_level: self.config.compression_level,
                first_offset: active.first_offset,
                last_offset: active.last_offset,
                record_count: active.record_count,
                source_bytes: active.source_bytes,
                structural_bytes,
                stored_bytes,
                min_timestamp_unix_nanos: active.min_timestamp_unix_nanos,
                max_timestamp_unix_nanos: active.max_timestamp_unix_nanos,
                compression_temperature: score.temperature.get(),
                compression_shape_hash: score.shape_hash,
                compression_temperature_variance_q8: score.internal_variance_q8,
                max_compression_temperature_deviation: score.max_deviation,
                object_key: None,
                object_offset: None,
            },
            Arc::from(compressed),
        ))
    }
}

impl ShardStreamDurableSink for LogStripe {
    fn on_durable_append(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt> {
        self.apply_durable(record)
    }
}

/// Container for independently owned shard-logdb stripes.
///
/// A deployment should hand each [`LogStripe`] to the matching shard-stream
/// worker and invoke [`Self::apply_durable`] in that worker. This container is
/// useful for single-process tests and embedded deployments; it never creates a
/// shared global hot index.
#[derive(Debug)]
pub struct ShardLogDb {
    stripes: HashMap<ShardId, LogStripe>,
}

impl ShardLogDb {
    /// Creates one log stripe for each supplied shard-stream shard.
    pub fn new(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
    ) -> LogDbResult<Self> {
        Self::new_with_optional_dictionary_catalog(shard_ids, config, None)
    }

    /// Creates one stripe per physical shard, all observing one immutable
    /// dictionary publication catalog at explicit batch boundaries.
    pub fn with_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> LogDbResult<Self> {
        Self::new_with_optional_dictionary_catalog(shard_ids, config, Some(dictionary_catalog))
    }

    /// Creates one stripe per shard and attaches all of them to one bounded
    /// real-time dictionary trainer.
    pub fn with_realtime_dictionary(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> LogDbResult<Self> {
        let mut database = Self::with_dictionary_catalog(shard_ids, config, trainer.catalog())?;
        for stripe in database.stripes.values_mut() {
            stripe.attach_realtime_dictionary(trainer.observer());
        }
        Ok(database)
    }

    fn new_with_optional_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
        dictionary_catalog: Option<Arc<DictionaryCatalog>>,
    ) -> LogDbResult<Self> {
        let mut stripes = HashMap::new();
        for shard_id in shard_ids {
            if stripes.contains_key(&shard_id) {
                return Err(LogDbError::DuplicateStripe(shard_id));
            }
            let stripe = match &dictionary_catalog {
                Some(dictionary_catalog) => LogStripe::with_dictionary_catalog(
                    shard_id,
                    config.clone(),
                    Arc::clone(dictionary_catalog),
                )?,
                None => LogStripe::new(shard_id, config.clone())?,
            };
            stripes.insert(shard_id, stripe);
        }
        if stripes.is_empty() {
            return Err(LogDbError::InvalidConfig("at least one stripe is required"));
        }
        Ok(Self { stripes })
    }

    /// Returns the stripe owned by a shard-stream worker.
    #[must_use]
    pub fn stripe(&self, stream_shard_id: ShardId) -> Option<&LogStripe> {
        self.stripes.get(&stream_shard_id)
    }

    /// Returns the stripe owned by a shard-stream worker.
    pub fn stripe_mut(&mut self, stream_shard_id: ShardId) -> Option<&mut LogStripe> {
        self.stripes.get_mut(&stream_shard_id)
    }

    /// Routes an already durable shard-stream record to its matching log stripe.
    pub fn apply_durable(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt> {
        self.stripes
            .get_mut(&record.stream_shard_id)
            .ok_or(LogDbError::UnknownStripe(record.stream_shard_id))?
            .apply_durable(record)
    }

    /// Runs a partition-local query through one stripe.
    pub fn query(&self, stream_shard_id: ShardId, query: &LogQuery) -> LogDbResult<Vec<LogMatch>> {
        let stripe = self
            .stripes
            .get(&stream_shard_id)
            .ok_or(LogDbError::UnknownStripe(stream_shard_id))?;
        Ok(stripe.query(query))
    }

    /// Fans a partition-local query across every physical stripe and merges
    /// the bounded results in the query's deterministic order.
    #[must_use]
    pub fn query_all(&self, query: &LogQuery) -> Vec<LogMatch> {
        self.merge_queries(self.stripes.values(), query)
    }

    /// Fans a partition-local query across selected physical stripes and
    /// returns an error if any requested stripe is unknown.
    pub fn query_stripes(
        &self,
        stream_shard_ids: impl IntoIterator<Item = ShardId>,
        query: &LogQuery,
    ) -> LogDbResult<Vec<LogMatch>> {
        let mut seen = HashSet::new();
        let mut stripes = Vec::new();
        for stream_shard_id in stream_shard_ids {
            if seen.insert(stream_shard_id) {
                stripes.push(
                    self.stripes
                        .get(&stream_shard_id)
                        .ok_or(LogDbError::UnknownStripe(stream_shard_id))?,
                );
            }
        }
        Ok(self.merge_queries(stripes, query))
    }

    fn merge_queries<'a>(
        &self,
        stripes: impl IntoIterator<Item = &'a LogStripe>,
        query: &LogQuery,
    ) -> Vec<LogMatch> {
        let mut matches = stripes
            .into_iter()
            .flat_map(|stripe| stripe.query(query))
            .collect::<Vec<_>>();
        matches.sort_unstable_by(|left, right| {
            query.compare(&left.record, &right.record).then_with(|| {
                left.record
                    .stream_shard_id
                    .cmp(&right.record.stream_shard_id)
            })
        });
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }
        matches
    }
}

impl ShardStreamDurableSink for ShardLogDb {
    fn on_durable_append(&mut self, record: DurableLogRecord) -> LogDbResult<IndexReceipt> {
        self.apply_durable(record)
    }
}

fn normalize_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

fn validate_batch_offset_range(
    topic_partition: TopicPartition,
    first_offset: LogicalOffset,
    record_count: usize,
) -> LogDbResult<()> {
    let Some(last_index) = record_count.checked_sub(1) else {
        return Ok(());
    };
    batch_offset(topic_partition, first_offset, last_index).map(|_| ())
}

fn batch_offset(
    topic_partition: TopicPartition,
    first_offset: LogicalOffset,
    index: usize,
) -> LogDbResult<LogicalOffset> {
    let relative_offset =
        u64::try_from(index).map_err(|_| LogDbError::OffsetExhausted(topic_partition))?;
    first_offset
        .get()
        .checked_add(relative_offset)
        .map(LogicalOffset::new)
        .ok_or(LogDbError::OffsetExhausted(topic_partition))
}

#[inline]
fn same_message(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && (std::ptr::eq(left.as_ptr(), right.as_ptr()) || left.as_bytes() == right.as_bytes())
}

#[inline]
fn message_term_cache_slot(topic_partition: TopicPartition, message: &[u8]) -> usize {
    let mut hash = message.len() as u64
        ^ u64::from(topic_partition.partition_id.get()).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    if message.len() >= 16 {
        let first = u64::from_le_bytes(
            message[..8]
                .try_into()
                .expect("eight-byte prefix is present"),
        );
        let last = u64::from_le_bytes(
            message[message.len() - 8..]
                .try_into()
                .expect("eight-byte suffix is present"),
        );
        hash ^= first.rotate_left(17) ^ last.rotate_left(41);
    } else {
        for byte in message {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash as usize & (MESSAGE_TERM_CACHE_ENTRIES - 1)
}

#[inline]
fn field_cache_slot(
    topic_partition: TopicPartition,
    fields: &Arc<Vec<crate::MetadataField>>,
) -> usize {
    let pointer = Arc::as_ptr(fields) as usize;
    let partition = topic_partition.partition_id.get() as usize;
    (pointer.rotate_left(17) ^ partition.wrapping_mul(0x9e37_79b9)) & (FIELD_CACHE_ENTRIES - 1)
}

#[inline]
fn same_fields(
    left: &Arc<Vec<crate::MetadataField>>,
    right: &Arc<Vec<crate::MetadataField>>,
) -> bool {
    Arc::ptr_eq(left, right) || left.as_slice() == right.as_slice()
}

fn ordinal_record_window(
    records: &[IndexedRecord],
    start: Option<LogicalOffset>,
    end: Option<LogicalOffset>,
) -> std::ops::Range<usize> {
    let start_index = start.map_or(0, |start| {
        records.partition_point(|record| record.record.record_ref.offset < start)
    });
    let end_index = end.map_or(records.len(), |end| {
        records.partition_point(|record| record.record.record_ref.offset < end)
    });
    start_index.min(end_index)..end_index
}

fn collect_ordered_range(
    ordinals: std::ops::Range<usize>,
    order: QueryOrder,
    limit: Option<usize>,
) -> Vec<u32> {
    let take = limit.unwrap_or(ordinals.len()).min(ordinals.len());
    match order {
        QueryOrder::OldestFirst => ordinals
            .take(take)
            .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
            .collect(),
        QueryOrder::NewestFirst => ordinals
            .rev()
            .take(take)
            .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
            .collect(),
    }
}

fn same_query_across_partition(left: &LogQuery, right: &LogQuery) -> bool {
    let mut normalized = left.clone();
    normalized.topic_partition = right.topic_partition;
    normalized == *right
}

fn append_matches_query_bounds(query: &LogQuery, append: &IndexedFrameAppend) -> bool {
    query.end_offset.is_none_or(|end| end > append.first_offset)
        && query
            .start_offset
            .is_none_or(|start| start <= append.last_offset)
}

fn frame_matches_query_bounds(query: &LogQuery, frame: &IndexedIngestFrame) -> bool {
    query
        .end_timestamp_unix_nanos
        .is_none_or(|end| end > frame.min_timestamp_unix_nanos)
        && query
            .start_timestamp_unix_nanos
            .is_none_or(|start| start <= frame.max_timestamp_unix_nanos)
}

fn sort_and_limit_matches(matches: &mut Vec<LogMatch>, query: &LogQuery, limit: usize) {
    matches.sort_unstable_by(|left, right| query.compare(&left.record, &right.record));
    matches.truncate(limit);
}

fn intersect_frame_candidates(current: &mut Option<Vec<u32>>, mut incoming: Vec<u32>) {
    let Some(existing) = current.as_mut() else {
        *current = Some(incoming);
        return;
    };
    if existing.len() > incoming.len() {
        std::mem::swap(existing, &mut incoming);
    }
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    let mut write_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => existing_index += 1,
            std::cmp::Ordering::Greater => incoming_index += 1,
            std::cmp::Ordering::Equal => {
                existing[write_index] = existing[existing_index];
                write_index += 1;
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    existing.truncate(write_index);
}

fn intersect_ordinal_runs(candidates: &mut Vec<u32>, runs: &[OrdinalRun], start: u32, end: u32) {
    let mut candidate_index = 0usize;
    let mut run_index = runs.partition_point(|run| run.last < start);
    let mut write_index = 0usize;
    while candidate_index < candidates.len() && run_index < runs.len() {
        let candidate = candidates[candidate_index];
        let run = runs[run_index];
        if candidate >= end || run.first >= end {
            break;
        }
        if candidate < run.first {
            candidate_index += 1;
        } else if candidate > run.last {
            run_index += 1;
        } else {
            candidates[write_index] = candidate;
            write_index += 1;
            candidate_index += 1;
        }
    }
    candidates.truncate(write_index);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message;
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use super::*;
    use crate::{LocalityGranularity, MetadataField, ingest_pack::prepare_ingest_pack};

    fn partition() -> TopicPartition {
        TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3))
    }

    fn record(offset: u64, message: &str) -> DurableLogRecord {
        record_on(ShardId::new(7), offset, message)
    }

    fn record_on(stream_shard_id: ShardId, offset: u64, message: &str) -> DurableLogRecord {
        DurableLogRecord::new(
            stream_shard_id,
            partition(),
            LogicalOffset::new(offset),
            offset * 10,
            message,
            CompressionCohortId::new(4),
        )
    }

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
    fn rebalanced_sub_blocks_merge_in_logical_offset_order() {
        let pending = |offset| {
            let record = record(offset, &format!("request id={offset} completed"));
            PendingRecord {
                source_bytes: row_source_bytes(&record).expect("record size fits"),
                fingerprint: fingerprint_message(&record.message, &record.fields),
                record,
            }
        };
        let mut even = ActiveBlock::from_records(vec![pending(4), pending(0), pending(2)], None, 1);
        let odd = ActiveBlock::from_records(vec![pending(5), pending(1), pending(3)], None, 1);
        even.append_block(odd);

        assert_eq!(
            even.records
                .iter()
                .map(|pending| pending.record.record_ref.offset)
                .collect::<Vec<_>>(),
            (0..6).map(LogicalOffset::new).collect::<Vec<_>>()
        );
        let records = even
            .records
            .iter()
            .map(|pending| pending.record.clone())
            .collect::<Vec<_>>();
        encode_structural_block(&records).expect("merged block offsets encode");
    }

    #[test]
    fn durable_records_become_visible_to_term_and_metadata_queries() {
        let mut database =
            ShardLogDb::new([ShardId::new(7)], StripeConfig::default()).expect("database opens");
        database
            .apply_durable(record(0, "ERROR cannot connect").with_field("service", "api"))
            .expect("first append indexes");
        database
            .apply_durable(record(1, "error timeout").with_field("service", "worker"))
            .expect("second append indexes");

        let matches = database
            .query(
                ShardId::new(7),
                &LogQuery::new(partition())
                    .with_term("error")
                    .with_field("service", "api"),
            )
            .expect("query succeeds");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].record.record_ref.offset, LogicalOffset::new(0));
        assert_eq!(
            database
                .stripe(ShardId::new(7))
                .expect("stripe exists")
                .indexed_through(partition()),
            Some(LogicalOffset::new(1))
        );
    }

    #[test]
    fn compressed_frame_queries_preserve_interleaved_offsets_and_full_exactness() {
        let events = (0..12)
            .map(|ordinal| OtlpLogEvent {
                timestamp_unix_nanos: 1_000 + ordinal,
                message: Arc::from(if ordinal % 2 == 0 {
                    format!("ERROR request id={ordinal} failed")
                } else {
                    format!("INFO request id={ordinal} completed")
                }),
                fields: Arc::new(vec![
                    MetadataField::new("service", if ordinal % 2 == 0 { "api" } else { "worker" }),
                    MetadataField::new("trace", format!("trace-{ordinal}")),
                ]),
                compression_cohort: CompressionCohortId::new(ordinal % 3),
            })
            .collect::<Vec<_>>();
        let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
        let payload = Bytes::from(prepared.payload);
        let first_offset = LogicalOffset::new(50);
        let mut live =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("live stripe opens");
        live.apply_indexed_ingest_pack(
            partition(),
            first_offset,
            events.len() as u32,
            payload.clone(),
            Some(&prepared.transient_context),
        )
        .expect("live frame indexes install");
        let mut recovered = LogStripe::new(ShardId::new(7), StripeConfig::default())
            .expect("recovered stripe opens");
        recovered
            .apply_indexed_ingest_pack(
                partition(),
                first_offset,
                events.len() as u32,
                payload,
                None,
            )
            .expect("durable frame indexes recover");

        let queries = [
            LogQuery::new(partition())
                .with_term("error")
                .with_field("service", "api"),
            LogQuery::new(partition())
                .with_term("7")
                .with_field("trace", "trace-7"),
            LogQuery::new(partition())
                .with_offset_range(LogicalOffset::new(53), LogicalOffset::new(58))
                .sort_by_timestamp()
                .newest_first()
                .with_limit(3),
            LogQuery::new(partition()).with_field("service", "missing"),
        ];
        let expected = [
            vec![50, 52, 54, 56, 58, 60],
            vec![57],
            vec![57, 56, 55],
            vec![],
        ];
        for (query, expected) in queries.iter().zip(expected) {
            let live_offsets = live
                .query_checked(query)
                .expect("live query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>();
            let recovered_offsets = recovered
                .query_checked(query)
                .expect("recovered query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>();
            assert_eq!(live_offsets, expected);
            assert_eq!(recovered_offsets, expected);
        }
        assert_eq!(
            live.indexed_through(partition()),
            Some(LogicalOffset::new(61))
        );
        assert!(!live.partitions.contains_key(&partition()));
    }

    #[test]
    fn compressed_frame_partition_fanout_applies_one_global_timestamp_limit() {
        let other_partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(4));
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for (topic_partition, timestamp_groups) in [
            (partition(), [[100, 101], [300, 301]]),
            (other_partition, [[200, 201], [400, 401]]),
        ] {
            for (batch, timestamps) in timestamp_groups.into_iter().enumerate() {
                let events = timestamps.map(|timestamp| OtlpLogEvent {
                    timestamp_unix_nanos: timestamp,
                    message: Arc::from(format!("ERROR request {timestamp} failed")),
                    fields: Arc::new(vec![MetadataField::new("service", "api")]),
                    compression_cohort: CompressionCohortId::new(1),
                });
                let prepared = prepare_ingest_pack(&events).expect("pack prepares");
                stripe
                    .apply_indexed_ingest_pack(
                        topic_partition,
                        LogicalOffset::new((batch * 2) as u64),
                        events.len() as u32,
                        Bytes::from(prepared.payload),
                        Some(&prepared.transient_context),
                    )
                    .expect("frame append indexes");
            }
        }
        let queries = [partition(), other_partition].map(|topic_partition| {
            LogQuery::new(topic_partition)
                .with_term("error")
                .sort_by_timestamp()
                .newest_first()
                .with_limit(3)
        });
        assert_eq!(
            stripe
                .query_partitions_checked(&queries)
                .expect("newest fanout query succeeds")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![401, 400, 301]
        );
        let queries = [partition(), other_partition].map(|topic_partition| {
            LogQuery::new(topic_partition)
                .with_term("error")
                .sort_by_timestamp()
                .with_limit(3)
        });
        assert_eq!(
            stripe
                .query_partitions_checked(&queries)
                .expect("oldest fanout query succeeds")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![100, 101, 200]
        );
    }

    #[test]
    fn homogeneous_event_batches_publish_one_posting_range_per_value() {
        let message: Arc<str> = Arc::from("repeated request completed");
        let fields = Arc::new(vec![crate::MetadataField::new("service", "api")]);
        let event = OtlpLogEvent {
            timestamp_unix_nanos: 42,
            message,
            fields,
            compression_cohort: CompressionCohortId::new(4),
        };
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        let receipts = stripe
            .apply_otlp_events(partition(), LogicalOffset::new(0), vec![event; 1_024])
            .expect("homogeneous event range indexes");

        assert_eq!(receipts.len(), 1_024);
        assert_eq!(
            stripe.indexed_through(partition()),
            Some(LogicalOffset::new(1_023))
        );
        let indexed = stripe
            .partitions
            .get(&partition())
            .expect("partition was indexed");
        let repeated_id = indexed.term_ids["repeated"];
        assert_eq!(
            indexed.term_postings[repeated_id].runs,
            vec![OrdinalRun {
                first: 0,
                last: 1_023
            }]
        );
        let service_id = indexed.field_ids["service"]["api"];
        assert_eq!(
            indexed.field_postings[service_id].runs,
            vec![OrdinalRun {
                first: 0,
                last: 1_023
            }]
        );
        assert_eq!(
            stripe
                .query_refs(
                    &LogQuery::new(partition())
                        .with_term("repeated")
                        .with_field("service", "api")
                )
                .len(),
            1_024
        );
    }

    #[test]
    fn heterogeneous_event_batches_retain_exact_sparse_postings() {
        let fields = Arc::new(vec![crate::MetadataField::new("service", "api")]);
        let event = |message: &'static str| OtlpLogEvent {
            timestamp_unix_nanos: 42,
            message: Arc::from(message),
            fields: Arc::clone(&fields),
            compression_cohort: CompressionCohortId::new(4),
        };
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_otlp_events(
                partition(),
                LogicalOffset::new(0),
                [
                    event("repeated request completed"),
                    event("different request failed"),
                    event("repeated request completed"),
                ],
            )
            .expect("heterogeneous events index");

        let indexed = stripe
            .partitions
            .get(&partition())
            .expect("partition was indexed");
        let repeated_id = indexed.term_ids["repeated"];
        assert_eq!(
            indexed.term_postings[repeated_id].runs,
            vec![
                OrdinalRun { first: 0, last: 0 },
                OrdinalRun { first: 2, last: 2 }
            ]
        );
        assert_eq!(
            stripe
                .query_refs(&LogQuery::new(partition()).with_term("repeated"))
                .into_iter()
                .map(|record_ref| record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn database_fanout_merges_selected_stripes_without_duplicate_results() {
        let mut database =
            ShardLogDb::new([ShardId::new(7), ShardId::new(8)], StripeConfig::default())
                .expect("database opens");
        for offset in 0..10 {
            let shard = if offset < 5 {
                ShardId::new(7)
            } else {
                ShardId::new(8)
            };
            database
                .apply_durable(
                    record_on(shard, offset, &format!("request {offset} completed"))
                        .with_field("service", "api"),
                )
                .expect("record indexes");
        }
        let query = LogQuery::new(partition())
            .with_predicate(crate::LogPredicate::field_exists("service"))
            .newest_first()
            .with_limit(3);
        assert_eq!(
            database
                .query_all(&query)
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![9, 8, 7]
        );
        assert_eq!(
            database
                .query_stripes([ShardId::new(8), ShardId::new(8), ShardId::new(7)], &query,)
                .expect("selected query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![9, 8, 7]
        );
        assert!(matches!(
            database.query_stripes([ShardId::new(99)], &query),
            Err(LogDbError::UnknownStripe(shard)) if shard == ShardId::new(99)
        ));
    }

    #[test]
    fn query_intersection_is_order_independent_and_offset_sorted() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for offset in 0..1_000u64 {
            let mut message = format!("common request_id={offset}");
            if offset % 10 == 0 {
                message.push_str(" medium");
            }
            if offset % 100 == 0 {
                message.push_str(" rare");
            }
            stripe
                .apply_durable(
                    record(offset, &message)
                        .with_field("service", if offset % 20 == 0 { "api" } else { "worker" }),
                )
                .expect("record indexes");
        }

        let common_first = stripe.query_refs(
            &LogQuery::new(partition())
                .with_term("common")
                .with_term("medium")
                .with_term("rare")
                .with_field("service", "api"),
        );
        let rare_first = stripe.query_refs(
            &LogQuery::new(partition())
                .with_field("service", "api")
                .with_term("rare")
                .with_term("medium")
                .with_term("common"),
        );

        assert_eq!(common_first, rare_first);
        assert_eq!(
            common_first
                .iter()
                .map(|reference| reference.offset.get())
                .collect::<Vec<_>>(),
            (0..1_000).step_by(100).collect::<Vec<_>>()
        );
    }

    #[test]
    fn query_ranges_order_and_limit_bound_materialized_results() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for offset in 0..100u64 {
            stripe
                .apply_durable(record(offset, "common event"))
                .expect("record indexes");
        }

        let matches = stripe.query(
            &LogQuery::new(partition())
                .with_term("common")
                .with_offset_range(LogicalOffset::new(20), LogicalOffset::new(80))
                .with_timestamp_range(300, 700)
                .newest_first()
                .with_limit(3),
        );
        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![69, 68, 67]
        );

        assert!(
            stripe
                .query(
                    &LogQuery::new(partition())
                        .with_offset_range(LogicalOffset::new(5), LogicalOffset::new(5))
                )
                .is_empty()
        );
        assert!(
            stripe
                .query(&LogQuery::new(partition()).with_limit(0))
                .is_empty()
        );
    }

    #[test]
    fn sorted_posting_intersection_handles_disjoint_and_overlapping_ranges() {
        let mut candidates = vec![1, 3, 4, 8, 10];
        let runs = [
            OrdinalRun { first: 0, last: 0 },
            OrdinalRun { first: 3, last: 5 },
            OrdinalRun {
                first: 10,
                last: 10,
            },
            OrdinalRun {
                first: 12,
                last: 12,
            },
        ];
        intersect_ordinal_runs(&mut candidates, &runs, 0, u32::MAX);
        assert_eq!(candidates, [3, 4, 10]);

        intersect_ordinal_runs(
            &mut candidates,
            &[OrdinalRun {
                first: 20,
                last: 20,
            }],
            0,
            u32::MAX,
        );
        assert!(candidates.is_empty());
    }

    #[test]
    fn skewed_posting_intersection_preserves_sparse_matches() {
        let mut candidates = vec![0, 1_000, 50_000, 99_999];
        let runs = [OrdinalRun {
            first: 0,
            last: 99_999,
        }];
        intersect_ordinal_runs(&mut candidates, &runs, 0, 100_000);
        assert_eq!(candidates, [0, 1_000, 50_000, 99_999]);

        let mut candidates = vec![0, 1_001, 50_000, 99_998];
        let runs = (0..100_000)
            .step_by(1_000)
            .map(|ordinal| OrdinalRun {
                first: ordinal,
                last: ordinal,
            })
            .collect::<Vec<_>>();
        intersect_ordinal_runs(&mut candidates, &runs, 0, 100_000);
        assert_eq!(candidates, [0, 50_000]);
    }

    #[test]
    fn lane_global_offset_gaps_are_accepted_but_regressions_are_rejected() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_durable(record(5, "first"))
            .expect("first append");
        stripe
            .apply_durable(record(7, "lane gap"))
            .expect("offsets occupied by sibling lane partitions may be skipped");
        let error = stripe
            .apply_durable(record(6, "regressed"))
            .expect_err("offset regression is rejected");
        assert_eq!(
            error,
            LogDbError::OffsetOutOfOrder {
                partition: partition(),
                expected: LogicalOffset::new(8),
                observed: LogicalOffset::new(6),
            }
        );
        assert_eq!(
            stripe.indexed_through(partition()),
            Some(LogicalOffset::new(7))
        );
    }

    #[test]
    fn disabled_locality_seals_without_collator_work() {
        let config = StripeConfig {
            target_block_bytes: 1,
            ..StripeConfig::default()
        };
        let mut stripe = LogStripe::new(ShardId::new(7), config).expect("stripe opens");
        let receipt = stripe
            .apply_durable(record(0, "repeated message"))
            .expect("record indexes");
        assert_eq!(receipt.sealed_blocks.len(), 1);
        assert_eq!(
            receipt.sealed_blocks[0].placement_id,
            CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4))
        );
        assert_eq!(receipt.sealed_blocks[0].compression_temperature, 0);
        assert_eq!(
            receipt.sealed_blocks[0].compression_temperature_variance_q8,
            0
        );
        let stats = stripe.compression_collation_stats();
        assert_eq!(stats.observations, 0);
        assert_eq!(stats.blocks_scored, 0);
    }

    #[test]
    fn sealing_records_dictionary_identity_and_object_location() {
        let config = StripeConfig {
            target_block_bytes: 1,
            dictionary_cache_bytes: 8,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig {
                enabled: false,
                ..CompressionLocalityConfig::default()
            },
        };
        let mut stripe = LogStripe::new(ShardId::new(7), config).expect("stripe opens");
        stripe
            .install_dictionary(
                CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4)),
                DictionaryId::new(11),
                Arc::from(&b"dict"[..]),
            )
            .expect("dictionary installs");
        let receipt = stripe
            .apply_durable(record(0, "message"))
            .expect("record indexes");
        let block = receipt
            .sealed_blocks
            .into_iter()
            .next()
            .expect("small target seals block");
        assert_eq!(block.dictionary_id, Some(DictionaryId::new(11)));
        assert_eq!(block.compression_codec, CompressionCodec::Zstd);
        let compressed = stripe
            .catalog()
            .staged_payload(block.block_id)
            .expect("sealed payload remains staged until offload");
        assert_eq!(
            u64::try_from(compressed.len()).expect("payload length fits"),
            block.stored_bytes
        );
        let decoded = zstd::bulk::Decompressor::with_dictionary(&b"dict"[..])
            .expect("decoder opens")
            .decompress(
                &compressed,
                usize::try_from(block.structural_bytes).expect("structural size fits"),
            )
            .expect("payload decompresses");
        assert_eq!(
            u64::try_from(decoded.len()).expect("structural size fits"),
            block.structural_bytes
        );
        assert_eq!(
            block.source_bytes,
            row_source_bytes(&record(0, "message")).expect("source accounting succeeds")
        );
        let records = crate::structural::decode_structural_block(&decoded)
            .expect("structural payload decodes");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, LogicalOffset::new(0));
        assert_eq!(records[0].timestamp_unix_nanos, 0);
        assert_eq!(records[0].message.as_ref(), "message");
        assert!(records[0].fields.is_empty());
        stripe
            .mark_block_offloaded(block.block_id, "objects/7/00000000.log")
            .expect("block is known");
        assert!(stripe.catalog().staged_payload(block.block_id).is_none());
        assert_eq!(
            stripe
                .catalog()
                .get(block.block_id)
                .expect("block exists")
                .object_key
                .as_deref(),
            Some("objects/7/00000000.log")
        );
    }

    #[test]
    fn locality_placement_preserves_utf8_records_queries_and_block_diagnostics() {
        let locality = CompressionLocalityConfig {
            enabled: true,
            min_split_records: 2,
            min_split_bytes: 1,
            min_admission_bytes: 1,
            ..CompressionLocalityConfig::default()
        };
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: 150,
                dictionary_cache_bytes: 128,
                compression_level: 1,
                compression_locality: locality,
            },
        )
        .expect("stripe opens");
        let source = CompressionCohortId::new(4);
        let records = [
            DurableLogRecord::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(0),
                10,
                "Échec request 123 at 東京",
                source,
            )
            .with_field("service", "paiements"),
            DurableLogRecord::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(1),
                20,
                "Échec request 456 at 東京",
                source,
            )
            .with_field("service", "paiements"),
            DurableLogRecord::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(2),
                30,
                "Échec request 890 at 東京",
                source,
            )
            .with_field("service", "paiements"),
            DurableLogRecord::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(3),
                40,
                "Échec request 042 at 東京",
                source,
            )
            .with_field("service", "paiements"),
        ];

        let first = stripe
            .apply_durable(records[0].clone())
            .expect("first record indexes");
        let second = stripe
            .apply_durable(records[1].clone())
            .expect("second record indexes");
        assert_eq!(
            first.tentative_compression_placement.granularity,
            LocalityGranularity::Base
        );
        assert_eq!(
            second.tentative_compression_placement.granularity,
            LocalityGranularity::Base
        );
        let third = stripe
            .apply_durable(records[2].clone())
            .expect("third record indexes");
        let fourth = stripe
            .apply_durable(records[3].clone())
            .expect("fourth record indexes");
        assert_eq!(
            third.tentative_compression_placement.granularity,
            LocalityGranularity::Collated
        );
        assert_eq!(
            fourth.tentative_compression_placement.granularity,
            LocalityGranularity::Collated
        );

        let matches = stripe.query(
            &LogQuery::new(partition())
                .with_term("東京")
                .with_term("échec")
                .with_field("service", "paiements"),
        );
        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.record.record_ref.offset)
                .collect::<Vec<_>>(),
            (0..4).map(LogicalOffset::new).collect::<Vec<_>>()
        );

        stripe
            .seal_active_blocks()
            .expect("remaining active blocks seal");
        let mut reconstructed = Vec::new();
        for block in stripe.catalog().iter() {
            assert_eq!(block.source_compression_cohort, source);
            assert!(block.record_count > 0);
            assert!(block.max_compression_temperature_deviation <= 20);
            let compressed = stripe
                .catalog()
                .staged_payload(block.block_id)
                .expect("payload is staged");
            let structural = zstd::bulk::decompress(
                &compressed,
                usize::try_from(block.structural_bytes).expect("structural bytes fit"),
            )
            .expect("payload decompresses");
            reconstructed.extend(
                crate::structural::decode_structural_block(&structural)
                    .expect("structural records decode"),
            );
        }
        reconstructed.sort_unstable_by_key(|record| record.offset);
        assert_eq!(reconstructed.len(), records.len());
        for (decoded, original) in reconstructed.iter().zip(records) {
            assert!(
                stripe
                    .final_compression_placement(original.record_ref)
                    .is_some()
            );
            assert_eq!(decoded.offset, original.record_ref.offset);
            assert_eq!(decoded.timestamp_unix_nanos, original.timestamp_unix_nanos);
            assert_eq!(decoded.message, original.message);
            assert_eq!(decoded.fields.as_ref(), original.fields.as_ref());
        }
    }

    #[test]
    fn mixed_blocks_filter_deviations_and_refill_compression_shards() {
        let candidates = [
            "alpha scheduler accepted static work",
            "database replica checkpoint completed",
            "network listener rejected malformed frame",
            "payment gateway authorized transaction",
            "kernel allocator reclaimed cold pages",
            "telemetry exporter flushed pending spans",
        ];
        let mut selected = (candidates[0], candidates[1], 0u8);
        for left in candidates {
            for right in candidates {
                let distance =
                    CompressionTemperature::new(fingerprint_message(left, &[]).locality_signature)
                        .distance(CompressionTemperature::new(
                            fingerprint_message(right, &[]).locality_signature,
                        ));
                if distance > selected.2 {
                    selected = (left, right, distance);
                }
            }
        }
        assert!(selected.2 >= 2, "test messages need separated temperatures");

        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: 400,
                dictionary_cache_bytes: 1024,
                compression_level: 1,
                compression_locality: CompressionLocalityConfig {
                    enabled: true,
                    min_split_records: 2,
                    min_split_bytes: 1,
                    split_variance_q8: 1,
                    max_shard_variance_q8: u16::MAX,
                    max_assignment_distance: selected.2.saturating_sub(1),
                    min_admission_bytes: 1,
                    ..CompressionLocalityConfig::default()
                },
            },
        )
        .expect("stripe opens");
        let source = CompressionCohortId::new(44);
        let messages = (0..8)
            .map(|index| {
                if index % 2 == 0 {
                    selected.0
                } else {
                    selected.1
                }
            })
            .chain((0..8).map(|_| selected.0))
            .chain((0..8).map(|_| selected.1))
            .collect::<Vec<_>>();
        for (index, message) in messages.iter().enumerate() {
            stripe
                .apply_durable(DurableLogRecord::new(
                    ShardId::new(7),
                    partition(),
                    LogicalOffset::new(u64::try_from(index).expect("offset fits")),
                    u64::try_from(index).expect("timestamp fits"),
                    *message,
                    source,
                ))
                .expect("record indexes");
        }
        stripe
            .seal_active_blocks()
            .expect("remaining compression shards seal");

        let placements = stripe
            .catalog()
            .iter()
            .map(|block| block.placement_id)
            .collect::<HashSet<_>>();
        assert!(placements.len() >= 2);
        assert!(stripe.compression_collation_stats().blocks_split > 0);
        assert!(stripe.compression_collation_stats().records_reassigned > 0);

        let mut reconstructed = Vec::new();
        for block in stripe.catalog().iter() {
            let compressed = stripe
                .catalog()
                .staged_payload(block.block_id)
                .expect("payload staged");
            let structural = zstd::bulk::decompress(
                &compressed,
                usize::try_from(block.structural_bytes).expect("size fits"),
            )
            .expect("block decompresses");
            reconstructed.extend(
                crate::structural::decode_structural_block(&structural)
                    .expect("block reconstructs"),
            );
        }
        reconstructed.sort_unstable_by_key(|record| record.offset);
        assert_eq!(reconstructed.len(), messages.len());
        assert_eq!(
            reconstructed
                .iter()
                .map(|record| record.message.as_ref())
                .collect::<Vec<_>>(),
            messages
        );
    }

    #[test]
    fn dictionary_cache_refreshes_lru_before_eviction() {
        let mut cache = DictionaryCache::new(4).expect("cache opens");
        cache
            .insert(DictionaryId::new(1), Arc::from(&b"aa"[..]))
            .expect("first dictionary");
        cache
            .insert(DictionaryId::new(2), Arc::from(&b"bb"[..]))
            .expect("second dictionary");
        let _ = cache
            .get(DictionaryId::new(1))
            .expect("first dictionary cached");
        let insert = cache
            .insert(DictionaryId::new(3), Arc::from(&b"cc"[..]))
            .expect("third dictionary");
        assert_eq!(insert.evicted, vec![DictionaryId::new(2)]);
        assert!(cache.contains(DictionaryId::new(1)));
        assert!(cache.contains(DictionaryId::new(3)));
    }

    #[test]
    fn catalog_shares_immutable_bytes_but_each_stripe_owns_its_lru_and_compressor() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let dictionary_id = DictionaryId::new(42);
        catalog
            .publish(
                CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4)),
                dictionary_id,
                Arc::from(&b"repeated clickhouse exception service context"[..]),
            )
            .expect("dictionary publishes");
        let config = StripeConfig {
            target_block_bytes: 1,
            dictionary_cache_bytes: 128,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig::default(),
        };
        let mut first = LogStripe::with_dictionary_catalog(
            ShardId::new(7),
            config.clone(),
            Arc::clone(&catalog),
        )
        .expect("first stripe opens");
        let mut second =
            LogStripe::with_dictionary_catalog(ShardId::new(8), config, Arc::clone(&catalog))
                .expect("second stripe opens");

        first
            .apply_durable(record_on(ShardId::new(7), 0, "repeated exception"))
            .expect("first stripe indexes");
        second
            .apply_durable(record_on(ShardId::new(8), 0, "repeated exception"))
            .expect("second stripe indexes");

        let first_payload = first
            .dictionary_cache_mut()
            .get(dictionary_id)
            .expect("first stripe caches dictionary");
        let second_payload = second
            .dictionary_cache_mut()
            .get(dictionary_id)
            .expect("second stripe caches dictionary");
        assert!(Arc::ptr_eq(&first_payload, &second_payload));
        assert_eq!(first.dictionary_generation(), 1);
        assert_eq!(second.dictionary_generation(), 1);
        assert_eq!(first.catalog().len(), 1);
        assert_eq!(second.catalog().len(), 1);
    }

    #[test]
    fn dictionary_rotation_only_affects_new_active_blocks_after_refresh() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let cohort = CompressionCohortId::new(4);
        let placement_id = CompressionPlacementId::from_source_cohort(cohort);
        let first_dictionary = DictionaryId::new(101);
        let second_dictionary = DictionaryId::new(102);
        catalog
            .publish(
                placement_id,
                first_dictionary,
                Arc::from(&b"first dictionary"[..]),
            )
            .expect("first dictionary publishes");
        let mut stripe = LogStripe::with_dictionary_catalog(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                dictionary_cache_bytes: 128,
                compression_level: 1,
                compression_locality: CompressionLocalityConfig::default(),
            },
            Arc::clone(&catalog),
        )
        .expect("stripe opens");
        stripe
            .apply_durable(record(0, "first message"))
            .expect("first record indexes");

        catalog
            .publish(
                placement_id,
                second_dictionary,
                Arc::from(&b"second dictionary"[..]),
            )
            .expect("second dictionary publishes");
        assert!(
            stripe
                .refresh_dictionary_catalog()
                .expect("stripe refreshes catalog")
        );
        stripe
            .apply_durable(record(1, "second message"))
            .expect("second record indexes");

        let mut dictionary_ids = stripe
            .seal_active_blocks()
            .expect("active blocks seal")
            .into_iter()
            .map(|block| block.dictionary_id.expect("dictionary selected"))
            .collect::<Vec<_>>();
        dictionary_ids.sort_unstable();
        assert_eq!(dictionary_ids, vec![first_dictionary, second_dictionary]);
        assert_eq!(stripe.dictionary_generation(), 2);
    }

    #[test]
    fn realtime_dictionary_publications_are_adopted_by_future_blocks() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let trainer = RealtimeDictionaryTrainer::start(
            crate::RealtimeDictionaryConfig {
                max_block_sample_bytes: 1024,
                training_sample_bytes: 8 * 1024,
                dictionary_bytes: 1024,
                holdout_blocks: 8,
                queue_blocks: 64,
                max_placements: 4,
                min_net_savings_bytes: 1,
                min_net_savings_bps: 1,
                retrain_after_bytes: u64::MAX,
            },
            1,
            Arc::clone(&catalog),
        )
        .expect("trainer starts");
        let placement_id = CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4));
        let observer = trainer.observer();
        for index in 0..16u64 {
            let mut state = 0x4d59_5df4_d0f3_3173u64;
            let mut sample = Vec::with_capacity(1024);
            for _ in 0..512 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                sample.push(state as u8);
            }
            sample.extend_from_slice(format!(" unique suffix {index:020}").as_bytes());
            while sample.len() < 1024 {
                sample.push(index.wrapping_mul(31).wrapping_add(sample.len() as u64) as u8);
            }
            assert!(observer.observe_structural_block(placement_id, sample));
        }
        trainer.flush().expect("trainer flushes");
        assert_eq!(trainer.stats().dictionaries_published, 1);

        let mut stripe = LogStripe::with_realtime_dictionary(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: 1,
                dictionary_cache_bytes: 4096,
                compression_level: 1,
                compression_locality: CompressionLocalityConfig::default(),
            },
            &trainer,
        )
        .expect("stripe opens");
        let block = stripe
            .apply_durable(record(0, "future block uses the learned dictionary"))
            .expect("record indexes")
            .sealed_blocks
            .into_iter()
            .next()
            .expect("block seals");
        assert!(block.dictionary_id.is_some());
        let payload = stripe
            .catalog()
            .staged_payload(block.block_id)
            .expect("payload staged");
        let dictionary = catalog
            .snapshot()
            .expect("catalog snapshot")
            .dictionary(block.dictionary_id.expect("dictionary id"))
            .expect("dictionary payload");
        let structural = zstd::bulk::Decompressor::with_dictionary(&dictionary)
            .expect("decompressor opens")
            .decompress(
                &payload,
                usize::try_from(block.structural_bytes).expect("size fits"),
            )
            .expect("block decompresses");
        let decoded =
            crate::structural::decode_structural_block(&structural).expect("block reconstructs");
        assert_eq!(
            decoded[0].message.as_ref(),
            "future block uses the learned dictionary"
        );
    }

    #[test]
    fn otlp_export_is_decoded_and_published_on_the_owning_stripe() {
        let export = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_attribute("service.name", "billing")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 9,
                        observed_time_unix_nano: 0,
                        severity_number: 17,
                        severity_text: "ERROR".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("card declined".into())),
                        }),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: Vec::new(),
                        span_id: Vec::new(),
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        let events = OtlpLogDecoder
            .decode(&export.encode_to_vec())
            .expect("OTLP export decodes before append");
        let receipts = stripe
            .apply_otlp_events(partition(), LogicalOffset::new(0), events)
            .expect("OTLP events index after append");
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            stripe.query(
                &LogQuery::new(partition())
                    .with_term("declined")
                    .with_field("service.name", "billing")
            ),
            vec![LogMatch {
                record: stripe
                    .partitions
                    .get(&partition())
                    .and_then(|partition| partition.record(LogicalOffset::new(0)))
                    .expect("record retained")
                    .record
                    .clone(),
            }]
        );
    }
}
