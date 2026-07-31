use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};

use crate::{LogDbError, LogDbResult, LogQuery, QueryOrder, StructuralRecordView, analyze_message};

const QUERY_INDEX_MAGIC: &[u8; 8] = b"SLOGQIX2";
const COMPRESSED_QUERY_INDEX_MAGIC: &[u8; 8] = b"SLOGQIZ2";
const DELTA_POSTING: u8 = 0;
const RUN_POSTING: u8 = 1;
const MESSAGE_TERM_CACHE_ENTRIES: usize = 1_024;
const TERM_CACHE_ENTRIES: usize = 4_096;
const MESSAGE_TRIGRAM_FILTER_BITS: usize = 65_536;
const MESSAGE_TRIGRAM_FILTER_WORDS: usize = MESSAGE_TRIGRAM_FILTER_BITS / u64::BITS as usize;
const MESSAGE_TRIGRAM_FILTER_BYTES: usize = MESSAGE_TRIGRAM_FILTER_WORDS * size_of::<u64>();

struct CachedMessageTerms<'a> {
    message: &'a str,
    term_ids: Vec<usize>,
}

#[derive(Clone, Copy)]
struct CachedTerm {
    term_id: usize,
}

/// Lossless block-level rejection filter for literal message predicates.
///
/// Every bit represents a deterministic hash of one UTF-8 byte trigram after
/// Unicode lowercase normalization. A missing bit proves that the literal is
/// absent. A present bit remains only a candidate because hashes can collide.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageTrigramFilter {
    words: Box<[u64]>,
}

impl MessageTrigramFilter {
    fn new() -> Self {
        Self {
            words: vec![0; MESSAGE_TRIGRAM_FILTER_WORDS].into_boxed_slice(),
        }
    }

    fn from_bytes(encoded: &[u8]) -> LogDbResult<Self> {
        if encoded.len() != MESSAGE_TRIGRAM_FILTER_BYTES {
            return Err(LogDbError::InvalidBlockEncoding(
                "invalid message trigram filter length",
            ));
        }
        let mut filter = Self::new();
        for (word, bytes) in filter.words.iter_mut().zip(encoded.chunks_exact(8)) {
            *word =
                u64::from_le_bytes(bytes.try_into().map_err(|_| {
                    LogDbError::InvalidBlockEncoding("invalid trigram filter word")
                })?);
        }
        Ok(filter)
    }

    fn insert_message(&mut self, message: &str) {
        visit_normalized_trigrams(message, |slot| {
            self.words[slot / u64::BITS as usize] |= 1u64 << (slot % u64::BITS as usize);
        });
    }

    fn might_contain_all(&self, slots: &[usize]) -> bool {
        slots.iter().all(|slot| {
            self.words[*slot / u64::BITS as usize] & (1u64 << (*slot % u64::BITS as usize)) != 0
        })
    }
}

fn visit_normalized_trigrams(message: &str, mut observe: impl FnMut(usize)) {
    if message.is_ascii() {
        for trigram in message.as_bytes().windows(3) {
            observe(message_trigram_slot([
                trigram[0].to_ascii_lowercase(),
                trigram[1].to_ascii_lowercase(),
                trigram[2].to_ascii_lowercase(),
            ]));
        }
    } else {
        for trigram in message.to_lowercase().as_bytes().windows(3) {
            observe(message_trigram_slot([trigram[0], trigram[1], trigram[2]]));
        }
    }
}

#[inline]
fn message_trigram_slot(trigram: [u8; 3]) -> usize {
    let mut hash =
        u32::from(trigram[0]) | (u32::from(trigram[1]) << 8) | (u32::from(trigram[2]) << 16);
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x7feb_352d);
    hash ^= hash >> 15;
    hash = hash.wrapping_mul(0x846c_a68b);
    hash ^= hash >> 16;
    hash as usize & (MESSAGE_TRIGRAM_FILTER_BITS - 1)
}

fn required_message_trigram_slots(literals: &[&str]) -> Vec<usize> {
    let mut slots = Vec::new();
    for literal in literals {
        visit_normalized_trigrams(literal, |slot| slots.push(slot));
    }
    slots.sort_unstable();
    slots.dedup();
    slots
}

/// Offset and timestamp bounds for one independently compressed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBlockMetadata {
    /// Stable ordinal used by the pack manifest.
    pub block_ordinal: u32,
    /// Logical partition represented by the block.
    pub topic_partition: TopicPartition,
    /// Lowest durable offset in the block.
    pub first_offset: LogicalOffset,
    /// Highest durable offset in the block.
    pub last_offset: LogicalOffset,
    /// Lowest event timestamp in the block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp in the block.
    pub max_timestamp_unix_nanos: u64,
    /// Number of records in the block.
    pub record_count: u32,
}

/// Exact location of a candidate record inside a sealed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryHit {
    /// Manifest block ordinal.
    pub block_ordinal: u32,
    /// Zero-based record ordinal inside the decoded structural block.
    pub record_ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockPosting {
    block_ordinal: u32,
    record_ordinals: PostingList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrdinalRun {
    start: u32,
    length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PostingList {
    Ordinals(Vec<u32>),
    Runs {
        runs: Vec<OrdinalRun>,
        cardinality: usize,
    },
}

impl PostingList {
    fn from_ordinals(ordinals: Vec<u32>) -> LogDbResult<Self> {
        if ordinals.is_empty() {
            return Err(LogDbError::InvalidBlockEncoding("empty query posting"));
        }
        if ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(LogDbError::InvalidBlockEncoding(
                "query posting is not ordered",
            ));
        }
        let runs = posting_runs(&ordinals);
        if runs.len().saturating_mul(2) < ordinals.len() {
            Ok(Self::Runs {
                runs,
                cardinality: ordinals.len(),
            })
        } else {
            Ok(Self::Ordinals(ordinals))
        }
    }

    const fn cardinality(&self) -> usize {
        match self {
            Self::Ordinals(ordinals) => ordinals.len(),
            Self::Runs { cardinality, .. } => *cardinality,
        }
    }

    fn storage_bytes(&self) -> usize {
        match self {
            Self::Ordinals(ordinals) => ordinals.capacity() * size_of::<u32>(),
            Self::Runs { runs, .. } => runs.capacity() * size_of::<OrdinalRun>(),
        }
    }

    fn to_vec(&self) -> Vec<u32> {
        match self {
            Self::Ordinals(ordinals) => ordinals.clone(),
            Self::Runs { runs, cardinality } => {
                let mut ordinals = Vec::with_capacity(*cardinality);
                for run in runs {
                    ordinals.extend(run.start..run.start + run.length);
                }
                ordinals
            }
        }
    }

    fn take_ordered(&self, newest_first: bool, limit: usize) -> Vec<u32> {
        match self {
            Self::Ordinals(ordinals) => {
                let take = limit.min(ordinals.len());
                if newest_first {
                    ordinals[ordinals.len() - take..]
                        .iter()
                        .rev()
                        .copied()
                        .collect()
                } else {
                    ordinals[..take].to_vec()
                }
            }
            Self::Runs { runs, .. } => {
                let mut ordinals = Vec::with_capacity(limit.min(self.cardinality()));
                if newest_first {
                    for run in runs.iter().rev() {
                        for ordinal in (run.start..run.start + run.length).rev() {
                            ordinals.push(ordinal);
                            if ordinals.len() == limit {
                                return ordinals;
                            }
                        }
                    }
                } else {
                    for run in runs {
                        for ordinal in run.start..run.start + run.length {
                            ordinals.push(ordinal);
                            if ordinals.len() == limit {
                                return ordinals;
                            }
                        }
                    }
                }
                ordinals
            }
        }
    }

    fn contains(&self, ordinal: u32) -> bool {
        match self {
            Self::Ordinals(ordinals) => ordinals.binary_search(&ordinal).is_ok(),
            Self::Runs { runs, .. } => {
                let position = runs.partition_point(|run| run.start <= ordinal);
                position > 0
                    && ordinal
                        < runs[position - 1]
                            .start
                            .saturating_add(runs[position - 1].length)
            }
        }
    }
}

type PartitionTermBlockPostings = HashMap<TopicPartition, HashMap<Arc<str>, Vec<BlockPosting>>>;
type PartitionFieldBlockPostings =
    HashMap<TopicPartition, HashMap<Arc<str>, HashMap<Arc<str>, Vec<BlockPosting>>>>;

/// Exact term and metadata postings for one structural block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockQueryIndex {
    record_count: u32,
    message_trigrams: MessageTrigramFilter,
    term_postings: HashMap<Arc<str>, PostingList>,
    field_postings: HashMap<Arc<str>, HashMap<Arc<str>, PostingList>>,
}

impl BlockQueryIndex {
    /// Builds exact record-ordinal postings from normalized structural records.
    pub fn build<R: StructuralRecordView>(records: &[R]) -> LogDbResult<Self> {
        let record_count = u32::try_from(records.len()).map_err(|_| LogDbError::RecordTooLarge)?;
        let mut term_ids = HashMap::<Arc<str>, usize>::new();
        let mut term_entries = Vec::<(Arc<str>, Vec<u32>)>::new();
        let mut message_cache = std::iter::repeat_with(|| None)
            .take(MESSAGE_TERM_CACHE_ENTRIES)
            .collect::<Vec<Option<CachedMessageTerms<'_>>>>();
        let mut term_cache = vec![None::<CachedTerm>; TERM_CACHE_ENTRIES];
        let mut message_trigrams = MessageTrigramFilter::new();
        let mut field_postings = HashMap::<Arc<str>, HashMap<Arc<str>, Vec<u32>>>::new();
        for (record_ordinal, record) in records.iter().enumerate() {
            let record_ordinal =
                u32::try_from(record_ordinal).map_err(|_| LogDbError::RecordTooLarge)?;
            let message = record.structural_message();
            let cache_slot = message_term_cache_slot(message.as_bytes());
            if let Some(cached) = &message_cache[cache_slot]
                && same_message(cached.message, message)
            {
                for &term_id in &cached.term_ids {
                    let postings = &mut term_entries[term_id].1;
                    if postings.last().copied() != Some(record_ordinal) {
                        postings.push(record_ordinal);
                    }
                }
            } else {
                message_trigrams.insert_message(message);
                let mut message_term_ids = message_cache[cache_slot]
                    .take()
                    .map(|cached| cached.term_ids)
                    .unwrap_or_default();
                message_term_ids.clear();
                let _ = analyze_message(message, &[], |term| {
                    let cache_slot = term_cache_slot(term.as_bytes());
                    let term_id = if let Some(cached) = term_cache[cache_slot]
                        && term_matches_cached(&term_entries[cached.term_id].0, term)
                    {
                        cached.term_id
                    } else {
                        let normalized = normalize_term(term);
                        let term_id = match term_ids.get(normalized.as_ref()).copied() {
                            Some(term_id) => term_id,
                            None => {
                                let term = Arc::<str>::from(normalized.as_ref());
                                let term_id = term_entries.len();
                                term_ids.insert(Arc::clone(&term), term_id);
                                term_entries.push((term, Vec::new()));
                                term_id
                            }
                        };
                        term_cache[cache_slot] = Some(CachedTerm { term_id });
                        term_id
                    };
                    message_term_ids.push(term_id);
                });
                for &term_id in &message_term_ids {
                    let postings = &mut term_entries[term_id].1;
                    if postings.last().copied() != Some(record_ordinal) {
                        postings.push(record_ordinal);
                    }
                }
                message_cache[cache_slot] = Some(CachedMessageTerms {
                    message,
                    term_ids: message_term_ids,
                });
            }

            for field_index in 0..record.structural_field_count() {
                let (key, value) = record.structural_field(field_index).ok_or(
                    LogDbError::InvalidBlockEncoding("record field count changed while indexing"),
                )?;
                let values = match field_postings.get_mut(key) {
                    Some(values) => values,
                    None => {
                        field_postings.insert(Arc::from(key), HashMap::new());
                        field_postings.get_mut(key).expect("field key was inserted")
                    }
                };
                let postings = match values.get_mut(value) {
                    Some(postings) => postings,
                    None => {
                        values.insert(Arc::from(value), Vec::new());
                        values.get_mut(value).expect("field value was inserted")
                    }
                };
                if postings.last().copied() != Some(record_ordinal) {
                    postings.push(record_ordinal);
                }
            }
        }
        let term_postings = term_entries
            .into_iter()
            .map(|(term, posting)| Ok((term, PostingList::from_ordinals(posting)?)))
            .collect::<LogDbResult<HashMap<_, _>>>()?;
        let field_postings = field_postings
            .into_iter()
            .map(|(key, values)| {
                let values = values
                    .into_iter()
                    .map(|(value, posting)| Ok((value, PostingList::from_ordinals(posting)?)))
                    .collect::<LogDbResult<HashMap<_, _>>>()?;
                Ok((key, values))
            })
            .collect::<LogDbResult<HashMap<_, _>>>()?;
        Ok(Self {
            record_count,
            message_trigrams,
            term_postings,
            field_postings,
        })
    }

    /// Number of indexed records.
    #[must_use]
    pub const fn record_count(&self) -> u32 {
        self.record_count
    }
}

#[inline]
fn same_message(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && (std::ptr::eq(left.as_ptr(), right.as_ptr()) || left.as_bytes() == right.as_bytes())
}

fn message_term_cache_slot(message: &[u8]) -> usize {
    let mut hash = message.len() as u64 ^ 0x9e37_79b9_7f4a_7c15;
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
    (hash as usize) & (MESSAGE_TERM_CACHE_ENTRIES - 1)
}

fn term_cache_slot(term: &[u8]) -> usize {
    let mut hash = term.len() as u64 ^ 0x517c_c1b7_2722_0a95;
    if let Some(first) = term.first() {
        hash ^= u64::from(*first) << 8;
    }
    if let Some(last) = term.last() {
        hash ^= u64::from(*last) << 24;
    }
    if term.len() >= 8 {
        hash ^= u64::from_le_bytes(term[..8].try_into().expect("eight-byte prefix is present"));
    } else {
        for byte in term {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash ^= hash >> 32;
    (hash as usize) & (TERM_CACHE_ENTRIES - 1)
}

fn term_matches_cached(indexed: &str, observed: &str) -> bool {
    if observed.is_ascii() {
        indexed.eq_ignore_ascii_case(observed)
    } else {
        normalize_term(observed).as_ref() == indexed
    }
}

/// Immutable query directory for a set of sealed structural blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentQueryIndex {
    blocks: Vec<QueryBlockMetadata>,
    message_trigram_words: Box<[u64]>,
    message_trigram_union: MessageTrigramFilter,
    message_trigram_intersection: MessageTrigramFilter,
    partition_blocks: HashMap<TopicPartition, Vec<u32>>,
    term_postings: PartitionTermBlockPostings,
    field_postings: PartitionFieldBlockPostings,
}

impl PersistentQueryIndex {
    /// Builds one immutable directory from block-local exact postings.
    pub fn from_blocks(
        mut blocks: Vec<(QueryBlockMetadata, BlockQueryIndex)>,
    ) -> LogDbResult<Self> {
        blocks.sort_unstable_by_key(|(metadata, _)| metadata.block_ordinal);
        if blocks
            .windows(2)
            .any(|pair| pair[0].0.block_ordinal == pair[1].0.block_ordinal)
        {
            return Err(LogDbError::InvalidBlockEncoding(
                "duplicate query block ordinal",
            ));
        }
        let mut metadata_entries = Vec::with_capacity(blocks.len());
        let mut message_trigram_words =
            Vec::with_capacity(blocks.len().saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS));
        let mut partition_blocks = HashMap::<TopicPartition, Vec<u32>>::new();
        let mut term_postings =
            HashMap::<TopicPartition, HashMap<Arc<str>, Vec<BlockPosting>>>::new();
        let mut field_postings = HashMap::<
            TopicPartition,
            HashMap<Arc<str>, HashMap<Arc<str>, Vec<BlockPosting>>>,
        >::new();
        for (metadata, index) in blocks {
            if metadata.record_count != index.record_count
                || metadata.first_offset > metadata.last_offset
                || metadata.min_timestamp_unix_nanos > metadata.max_timestamp_unix_nanos
            {
                return Err(LogDbError::InvalidBlockEncoding(
                    "invalid query block metadata",
                ));
            }
            partition_blocks
                .entry(metadata.topic_partition)
                .or_default()
                .push(metadata.block_ordinal);
            let partition_terms = term_postings.entry(metadata.topic_partition).or_default();
            for (term, record_ordinals) in index.term_postings {
                partition_terms.entry(term).or_default().push(BlockPosting {
                    block_ordinal: metadata.block_ordinal,
                    record_ordinals,
                });
            }
            let partition_fields = field_postings.entry(metadata.topic_partition).or_default();
            for (key, values) in index.field_postings {
                let indexed_values = partition_fields.entry(key).or_default();
                for (value, record_ordinals) in values {
                    indexed_values.entry(value).or_default().push(BlockPosting {
                        block_ordinal: metadata.block_ordinal,
                        record_ordinals,
                    });
                }
            }
            message_trigram_words.extend_from_slice(&index.message_trigrams.words);
            metadata_entries.push(metadata);
        }
        let (message_trigram_union, message_trigram_intersection) =
            aggregate_message_trigrams(&message_trigram_words);
        Ok(Self {
            blocks: metadata_entries,
            message_trigram_words: message_trigram_words.into_boxed_slice(),
            message_trigram_union,
            message_trigram_intersection,
            partition_blocks,
            term_postings,
            field_postings,
        })
    }

    /// Returns the immutable block directory.
    #[must_use]
    pub fn blocks(&self) -> &[QueryBlockMetadata] {
        &self.blocks
    }

    /// Heap bytes occupied by record-ordinal arrays and run tables.
    ///
    /// This excludes hash-table buckets and interned term/field strings.
    #[must_use]
    pub fn posting_storage_bytes(&self) -> usize {
        self.term_postings
            .values()
            .flat_map(HashMap::values)
            .flatten()
            .chain(
                self.field_postings
                    .values()
                    .flat_map(HashMap::values)
                    .flat_map(HashMap::values)
                    .flatten(),
            )
            .map(|posting| posting.record_ordinals.storage_bytes())
            .sum()
    }

    /// Heap bytes occupied by block-level message trigram rejection filters.
    #[must_use]
    pub fn message_trigram_filter_bytes(&self) -> usize {
        self.message_trigram_words
            .len()
            .saturating_mul(size_of::<u64>())
    }

    /// Logical number of record ordinals represented by all postings.
    #[must_use]
    pub fn posting_cardinality(&self) -> usize {
        self.term_postings
            .values()
            .flat_map(HashMap::values)
            .flatten()
            .chain(
                self.field_postings
                    .values()
                    .flat_map(HashMap::values)
                    .flat_map(HashMap::values)
                    .flatten(),
            )
            .map(|posting| posting.record_ordinals.cardinality())
            .sum()
    }

    /// Plans safe term/field candidates before payload reads and decoding.
    ///
    /// Posting-only queries may return exact hits. Queries containing residual
    /// predicates return a superset. Always pass decoded records through
    /// [`LogQuery::select`] before returning them to a caller. Offset and
    /// timestamp ranges prune whole blocks here, while boundary records are
    /// checked after selective decoding. A limit is applied during planning
    /// only when the complete query can be answered safely by postings.
    #[must_use]
    pub fn candidate_hits(&self, query: &LogQuery) -> Vec<QueryHit> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let required = query.required_index_constraints();
        if required.impossible {
            return Vec::new();
        }
        let Some(required_message_slots) =
            self.prepare_required_message_trigrams(&required.message_literals)
        else {
            return Vec::new();
        };
        let mut constraints = Vec::<&[BlockPosting]>::with_capacity(
            required.terms.len().saturating_add(required.fields.len()),
        );
        let partition_terms = self.term_postings.get(&query.topic_partition);
        for term in required.terms {
            let normalized = normalize_term(term);
            let Some(postings) = partition_terms.and_then(|terms| terms.get(normalized.as_ref()))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }
        let partition_fields = self.field_postings.get(&query.topic_partition);
        for (key, value) in required.fields {
            let Some(postings) = partition_fields
                .and_then(|keys| keys.get(key))
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }

        let mut candidate_blocks = if constraints.is_empty() {
            self.partition_blocks
                .get(&query.topic_partition)
                .cloned()
                .unwrap_or_default()
        } else {
            constraints.sort_unstable_by_key(|postings| postings.len());
            let mut candidates = constraints[0]
                .iter()
                .map(|posting| posting.block_ordinal)
                .collect::<Vec<_>>();
            for postings in &constraints[1..] {
                intersect_block_ordinals(&mut candidates, postings);
                if candidates.is_empty() {
                    return Vec::new();
                }
            }
            candidates
        };
        candidate_blocks.retain(|ordinal| {
            self.block(*ordinal)
                .is_some_and(|metadata| block_overlaps(metadata, query))
                && self.message_might_match(*ordinal, &required_message_slots)
        });
        if query.order == QueryOrder::NewestFirst {
            candidate_blocks.reverse();
        }

        let safe_limit = query
            .can_apply_index_limit()
            .then_some(query.limit)
            .flatten()
            .unwrap_or(usize::MAX);
        let mut hits = Vec::new();
        for block_ordinal in candidate_blocks {
            let Some(metadata) = self.block(block_ordinal) else {
                continue;
            };
            let mut record_constraints = Vec::<&PostingList>::with_capacity(constraints.len());
            for constraint in &constraints {
                let Ok(position) = constraint
                    .binary_search_by_key(&block_ordinal, |posting| posting.block_ordinal)
                else {
                    record_constraints.clear();
                    break;
                };
                record_constraints.push(&constraint[position].record_ordinals);
            }
            let remaining = safe_limit.saturating_sub(hits.len());
            let newest_first = query.order == QueryOrder::NewestFirst;
            let record_ordinals = if record_constraints.is_empty() {
                if constraints.is_empty() {
                    if newest_first {
                        (0..metadata.record_count).rev().take(remaining).collect()
                    } else {
                        (0..metadata.record_count).take(remaining).collect()
                    }
                } else {
                    continue;
                }
            } else if record_constraints.len() == 1 {
                record_constraints[0].take_ordered(newest_first, remaining)
            } else {
                record_constraints.sort_unstable_by_key(|postings| postings.cardinality());
                if remaining.saturating_mul(8) < record_constraints[0].cardinality() {
                    intersect_postings_limited(&record_constraints, newest_first, remaining)
                } else {
                    let mut candidates = record_constraints[0].to_vec();
                    for postings in &record_constraints[1..] {
                        intersect_posting(&mut candidates, postings);
                        if candidates.is_empty() {
                            break;
                        }
                    }
                    if newest_first {
                        candidates.reverse();
                    }
                    candidates.truncate(remaining);
                    candidates
                }
            };
            hits.extend(
                record_ordinals
                    .into_iter()
                    .take(remaining)
                    .map(|record_ordinal| QueryHit {
                        block_ordinal,
                        record_ordinal,
                    }),
            );
            if hits.len() >= safe_limit {
                break;
            }
        }
        hits
    }

    /// Returns candidate block ordinals in deterministic offset order.
    ///
    /// This is the bounded entry point for sealed queries that require
    /// post-decode filtering. Call [`Self::candidate_hits_in_block`] for one
    /// returned block at a time, decode and filter those records, and stop once
    /// an offset-ordered page is complete.
    #[must_use]
    pub fn candidate_blocks(&self, query: &LogQuery) -> Vec<u32> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let required = query.required_index_constraints();
        if required.impossible {
            return Vec::new();
        }
        let Some(required_message_slots) =
            self.prepare_required_message_trigrams(&required.message_literals)
        else {
            return Vec::new();
        };
        let mut constraints = Vec::<&[BlockPosting]>::with_capacity(
            required.terms.len().saturating_add(required.fields.len()),
        );
        let partition_terms = self.term_postings.get(&query.topic_partition);
        for term in required.terms {
            let normalized = normalize_term(term);
            let Some(postings) = partition_terms.and_then(|terms| terms.get(normalized.as_ref()))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }
        let partition_fields = self.field_postings.get(&query.topic_partition);
        for (key, value) in required.fields {
            let Some(postings) = partition_fields
                .and_then(|keys| keys.get(key))
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            constraints.push(postings);
        }

        let mut blocks = if constraints.is_empty() {
            self.partition_blocks
                .get(&query.topic_partition)
                .cloned()
                .unwrap_or_default()
        } else {
            constraints.sort_unstable_by_key(|postings| postings.len());
            let mut blocks = constraints[0]
                .iter()
                .map(|posting| posting.block_ordinal)
                .collect::<Vec<_>>();
            for postings in &constraints[1..] {
                intersect_block_ordinals(&mut blocks, postings);
                if blocks.is_empty() {
                    return blocks;
                }
            }
            blocks
        };
        blocks.retain(|ordinal| {
            self.block(*ordinal)
                .is_some_and(|metadata| block_overlaps(metadata, query))
                && self.message_might_match(*ordinal, &required_message_slots)
        });
        blocks.sort_unstable_by_key(|ordinal| {
            self.block(*ordinal).map(|metadata| metadata.first_offset)
        });
        if query.order == QueryOrder::NewestFirst {
            blocks.reverse();
        }
        blocks
    }

    /// Plans the unbounded candidate records inside one selected block.
    ///
    /// This method deliberately ignores the query's global limit. The caller
    /// must decode these candidates, apply [`LogQuery::matches`], and enforce
    /// the limit only after residual filtering.
    #[must_use]
    pub fn candidate_hits_in_block(&self, query: &LogQuery, block_ordinal: u32) -> Vec<QueryHit> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let Some(metadata) = self.block(block_ordinal) else {
            return Vec::new();
        };
        if metadata.topic_partition != query.topic_partition || !block_overlaps(metadata, query) {
            return Vec::new();
        }
        let required = query.required_index_constraints();
        if required.impossible {
            return Vec::new();
        }
        let Some(required_message_slots) =
            self.prepare_required_message_trigrams(&required.message_literals)
        else {
            return Vec::new();
        };
        if !self.message_might_match(block_ordinal, &required_message_slots) {
            return Vec::new();
        }
        let mut constraints = Vec::<&PostingList>::with_capacity(
            required.terms.len().saturating_add(required.fields.len()),
        );
        let partition_terms = self.term_postings.get(&query.topic_partition);
        for term in required.terms {
            let normalized = normalize_term(term);
            let Some(posting) = partition_terms
                .and_then(|terms| terms.get(normalized.as_ref()))
                .and_then(|postings| posting_for_block(postings, block_ordinal))
            else {
                return Vec::new();
            };
            constraints.push(posting);
        }
        let partition_fields = self.field_postings.get(&query.topic_partition);
        for (key, value) in required.fields {
            let Some(posting) = partition_fields
                .and_then(|keys| keys.get(key))
                .and_then(|values| values.get(value))
                .and_then(|postings| posting_for_block(postings, block_ordinal))
            else {
                return Vec::new();
            };
            constraints.push(posting);
        }

        let newest_first = query.order == QueryOrder::NewestFirst;
        let mut record_ordinals = if constraints.is_empty() {
            (0..metadata.record_count).collect::<Vec<_>>()
        } else {
            constraints.sort_unstable_by_key(|posting| posting.cardinality());
            let mut record_ordinals = constraints[0].to_vec();
            for posting in &constraints[1..] {
                intersect_posting(&mut record_ordinals, posting);
                if record_ordinals.is_empty() {
                    break;
                }
            }
            record_ordinals
        };
        if newest_first {
            record_ordinals.reverse();
        }
        record_ordinals
            .into_iter()
            .map(|record_ordinal| QueryHit {
                block_ordinal,
                record_ordinal,
            })
            .collect()
    }

    /// Encodes the complete immutable directory with hybrid delta/run postings.
    pub fn encode(&self) -> LogDbResult<Vec<u8>> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(QUERY_INDEX_MAGIC);
        write_varint(
            u64::try_from(self.blocks.len()).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut encoded,
        );
        for (block_index, metadata) in self.blocks.iter().enumerate() {
            write_varint(u64::from(metadata.block_ordinal), &mut encoded);
            encoded.extend_from_slice(&metadata.topic_partition.topic_id.get().to_le_bytes());
            encoded.extend_from_slice(&metadata.topic_partition.partition_id.get().to_le_bytes());
            write_varint(metadata.first_offset.get(), &mut encoded);
            write_varint(metadata.last_offset.get(), &mut encoded);
            write_varint(metadata.min_timestamp_unix_nanos, &mut encoded);
            write_varint(metadata.max_timestamp_unix_nanos, &mut encoded);
            write_varint(u64::from(metadata.record_count), &mut encoded);
            let trigram_start = block_index.saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS);
            for word in &self.message_trigram_words
                [trigram_start..trigram_start + MESSAGE_TRIGRAM_FILTER_WORDS]
            {
                encoded.extend_from_slice(&word.to_le_bytes());
            }

            let mut terms = self
                .term_postings
                .get(&metadata.topic_partition)
                .into_iter()
                .flat_map(HashMap::iter)
                .filter_map(|(term, postings)| {
                    posting_for_block(postings, metadata.block_ordinal)
                        .map(|posting| (term.as_ref(), posting))
                })
                .collect::<Vec<_>>();
            terms.sort_unstable_by(|left, right| left.0.cmp(right.0));
            write_varint(
                u64::try_from(terms.len()).map_err(|_| LogDbError::RecordTooLarge)?,
                &mut encoded,
            );
            for (term, posting) in terms {
                append_bytes(term.as_bytes(), &mut encoded)?;
                encode_posting(posting, &mut encoded)?;
            }

            let mut fields = self
                .field_postings
                .get(&metadata.topic_partition)
                .into_iter()
                .flat_map(HashMap::iter)
                .flat_map(|(key, values)| {
                    values.iter().filter_map(move |(value, postings)| {
                        posting_for_block(postings, metadata.block_ordinal)
                            .map(|posting| (key.as_ref(), value.as_ref(), posting))
                    })
                })
                .collect::<Vec<_>>();
            fields.sort_unstable_by(|left, right| {
                left.0.cmp(right.0).then_with(|| left.1.cmp(right.1))
            });
            write_varint(
                u64::try_from(fields.len()).map_err(|_| LogDbError::RecordTooLarge)?,
                &mut encoded,
            );
            for (key, value, posting) in fields {
                append_bytes(key.as_bytes(), &mut encoded)?;
                append_bytes(value.as_bytes(), &mut encoded)?;
                encode_posting(posting, &mut encoded)?;
            }
        }
        Ok(encoded)
    }

    /// Decodes and validates one immutable query directory.
    pub fn decode(encoded: &[u8]) -> LogDbResult<Self> {
        if encoded.get(..QUERY_INDEX_MAGIC.len()) != Some(QUERY_INDEX_MAGIC) {
            return Err(LogDbError::InvalidBlockEncoding(
                "missing query index magic",
            ));
        }
        let mut cursor = QUERY_INDEX_MAGIC.len();
        let block_count = read_usize(encoded, &mut cursor)?;
        ensure_count(block_count, encoded.len().saturating_sub(cursor))?;
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            let block_ordinal = read_u32(encoded, &mut cursor)?;
            let topic_end = cursor
                .checked_add(16)
                .ok_or(LogDbError::InvalidBlockEncoding("query topic overflow"))?;
            let topic_id = TopicId::new(u128::from_le_bytes(
                encoded
                    .get(cursor..topic_end)
                    .ok_or(LogDbError::InvalidBlockEncoding("truncated query topic"))?
                    .try_into()
                    .map_err(|_| LogDbError::InvalidBlockEncoding("invalid query topic"))?,
            ));
            cursor = topic_end;
            let partition_end = cursor
                .checked_add(4)
                .ok_or(LogDbError::InvalidBlockEncoding("query partition overflow"))?;
            let partition_id = LogicalPartitionId::new(u32::from_le_bytes(
                encoded
                    .get(cursor..partition_end)
                    .ok_or(LogDbError::InvalidBlockEncoding(
                        "truncated query partition",
                    ))?
                    .try_into()
                    .map_err(|_| LogDbError::InvalidBlockEncoding("invalid query partition"))?,
            ));
            cursor = partition_end;
            let first_offset = LogicalOffset::new(read_varint(encoded, &mut cursor)?);
            let last_offset = LogicalOffset::new(read_varint(encoded, &mut cursor)?);
            let min_timestamp_unix_nanos = read_varint(encoded, &mut cursor)?;
            let max_timestamp_unix_nanos = read_varint(encoded, &mut cursor)?;
            let record_count = read_u32(encoded, &mut cursor)?;
            let trigram_end = cursor.checked_add(MESSAGE_TRIGRAM_FILTER_BYTES).ok_or(
                LogDbError::InvalidBlockEncoding("message trigram filter overflow"),
            )?;
            let message_trigrams =
                MessageTrigramFilter::from_bytes(encoded.get(cursor..trigram_end).ok_or(
                    LogDbError::InvalidBlockEncoding("truncated message trigram filter"),
                )?)?;
            cursor = trigram_end;
            let term_count = read_usize(encoded, &mut cursor)?;
            ensure_count(term_count, encoded.len().saturating_sub(cursor))?;
            let mut term_postings = HashMap::with_capacity(term_count);
            for _ in 0..term_count {
                let term = decode_text(read_bytes(encoded, &mut cursor)?)?;
                let posting = decode_posting(encoded, &mut cursor, record_count)?;
                if term_postings.insert(term, posting).is_some() {
                    return Err(LogDbError::InvalidBlockEncoding("duplicate indexed term"));
                }
            }
            let field_count = read_usize(encoded, &mut cursor)?;
            ensure_count(field_count, encoded.len().saturating_sub(cursor))?;
            let mut field_postings = HashMap::<Arc<str>, HashMap<Arc<str>, PostingList>>::new();
            for _ in 0..field_count {
                let key = decode_text(read_bytes(encoded, &mut cursor)?)?;
                let value = decode_text(read_bytes(encoded, &mut cursor)?)?;
                let posting = decode_posting(encoded, &mut cursor, record_count)?;
                if field_postings
                    .entry(key)
                    .or_default()
                    .insert(value, posting)
                    .is_some()
                {
                    return Err(LogDbError::InvalidBlockEncoding("duplicate indexed field"));
                }
            }
            blocks.push((
                QueryBlockMetadata {
                    block_ordinal,
                    topic_partition: TopicPartition::new(topic_id, partition_id),
                    first_offset,
                    last_offset,
                    min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos,
                    record_count,
                },
                BlockQueryIndex {
                    record_count,
                    message_trigrams,
                    term_postings,
                    field_postings,
                },
            ));
        }
        if cursor != encoded.len() {
            return Err(LogDbError::InvalidBlockEncoding(
                "trailing query index bytes",
            ));
        }
        Self::from_blocks(blocks)
    }

    /// Encodes and wraps the query directory in one zstd frame.
    pub fn encode_compressed(&self, level: i32) -> LogDbResult<Vec<u8>> {
        let uncompressed = self.encode()?;
        let compressed = zstd::bulk::compress(&uncompressed, level)
            .map_err(|error| LogDbError::CompressionFailed(error.to_string()))?;
        let mut encoded =
            Vec::with_capacity(COMPRESSED_QUERY_INDEX_MAGIC.len() + 8 + compressed.len());
        encoded.extend_from_slice(COMPRESSED_QUERY_INDEX_MAGIC);
        encoded.extend_from_slice(
            &u64::try_from(uncompressed.len())
                .map_err(|_| LogDbError::RecordTooLarge)?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&compressed);
        Ok(encoded)
    }

    /// Decodes a zstd-wrapped immutable query directory.
    pub fn decode_compressed(encoded: &[u8]) -> LogDbResult<Self> {
        if encoded.get(..COMPRESSED_QUERY_INDEX_MAGIC.len()) != Some(COMPRESSED_QUERY_INDEX_MAGIC) {
            return Err(LogDbError::InvalidBlockEncoding(
                "missing compressed query index magic",
            ));
        }
        let length_end = COMPRESSED_QUERY_INDEX_MAGIC.len() + 8;
        let uncompressed_len = usize::try_from(u64::from_le_bytes(
            encoded
                .get(COMPRESSED_QUERY_INDEX_MAGIC.len()..length_end)
                .ok_or(LogDbError::InvalidBlockEncoding(
                    "truncated query index length",
                ))?
                .try_into()
                .map_err(|_| LogDbError::InvalidBlockEncoding("invalid query index length"))?,
        ))
        .map_err(|_| LogDbError::InvalidBlockEncoding("query index length does not fit usize"))?;
        let uncompressed = zstd::bulk::decompress(
            encoded
                .get(length_end..)
                .ok_or(LogDbError::InvalidBlockEncoding(
                    "truncated compressed query index",
                ))?,
            uncompressed_len,
        )
        .map_err(|_| LogDbError::InvalidBlockEncoding("invalid compressed query index"))?;
        Self::decode(&uncompressed)
    }

    fn block(&self, block_ordinal: u32) -> Option<&QueryBlockMetadata> {
        self.blocks
            .binary_search_by_key(&block_ordinal, |metadata| metadata.block_ordinal)
            .ok()
            .and_then(|index| self.blocks.get(index))
    }

    fn message_might_match(&self, block_ordinal: u32, required_slots: &[usize]) -> bool {
        let Ok(index) = self
            .blocks
            .binary_search_by_key(&block_ordinal, |metadata| metadata.block_ordinal)
        else {
            return false;
        };
        let start = index.saturating_mul(MESSAGE_TRIGRAM_FILTER_WORDS);
        required_slots.iter().all(|slot| {
            self.message_trigram_words[start + *slot / u64::BITS as usize]
                & (1u64 << (*slot % u64::BITS as usize))
                != 0
        })
    }

    fn prepare_required_message_trigrams(&self, literals: &[&str]) -> Option<Vec<usize>> {
        let mut slots = required_message_trigram_slots(literals);
        if !self.message_trigram_union.might_contain_all(&slots) {
            return None;
        }
        slots.retain(|slot| {
            !self
                .message_trigram_intersection
                .might_contain_all(&[*slot])
        });
        Some(slots)
    }
}

fn aggregate_message_trigrams(words: &[u64]) -> (MessageTrigramFilter, MessageTrigramFilter) {
    let mut union = MessageTrigramFilter::new();
    let mut intersection = MessageTrigramFilter {
        words: vec![u64::MAX; MESSAGE_TRIGRAM_FILTER_WORDS].into_boxed_slice(),
    };
    if words.is_empty() {
        intersection.words.fill(0);
        return (union, intersection);
    }
    for filter in words.chunks_exact(MESSAGE_TRIGRAM_FILTER_WORDS) {
        for ((union, intersection), observed) in union
            .words
            .iter_mut()
            .zip(intersection.words.iter_mut())
            .zip(filter.iter())
        {
            *union |= *observed;
            *intersection &= *observed;
        }
    }
    (union, intersection)
}

fn block_overlaps(metadata: &QueryBlockMetadata, query: &LogQuery) -> bool {
    query
        .start_offset
        .is_none_or(|start| metadata.last_offset >= start)
        && query
            .end_offset
            .is_none_or(|end| metadata.first_offset < end)
        && query
            .start_timestamp_unix_nanos
            .is_none_or(|start| metadata.max_timestamp_unix_nanos >= start)
        && query
            .end_timestamp_unix_nanos
            .is_none_or(|end| metadata.min_timestamp_unix_nanos < end)
}

fn posting_for_block(postings: &[BlockPosting], block_ordinal: u32) -> Option<&PostingList> {
    postings
        .binary_search_by_key(&block_ordinal, |posting| posting.block_ordinal)
        .ok()
        .map(|index| &postings[index].record_ordinals)
}

fn intersect_block_ordinals(candidates: &mut Vec<u32>, postings: &[BlockPosting]) {
    let mut candidate_index = 0usize;
    let mut posting_index = 0usize;
    let mut write_index = 0usize;
    while candidate_index < candidates.len() && posting_index < postings.len() {
        match candidates[candidate_index].cmp(&postings[posting_index].block_ordinal) {
            std::cmp::Ordering::Less => candidate_index += 1,
            std::cmp::Ordering::Greater => posting_index += 1,
            std::cmp::Ordering::Equal => {
                candidates[write_index] = candidates[candidate_index];
                write_index += 1;
                candidate_index += 1;
                posting_index += 1;
            }
        }
    }
    candidates.truncate(write_index);
}

fn intersect_u32(candidates: &mut Vec<u32>, postings: &[u32]) {
    if candidates.len().saturating_mul(8) < postings.len() {
        let mut posting_index = 0usize;
        let mut write_index = 0usize;
        for candidate_index in 0..candidates.len() {
            let candidate = candidates[candidate_index];
            posting_index +=
                postings[posting_index..].partition_point(|posting| *posting < candidate);
            let Some(posting) = postings.get(posting_index) else {
                break;
            };
            if *posting == candidate {
                candidates[write_index] = candidate;
                write_index += 1;
                posting_index += 1;
            }
        }
        candidates.truncate(write_index);
        return;
    }
    let mut candidate_index = 0usize;
    let mut posting_index = 0usize;
    let mut write_index = 0usize;
    while candidate_index < candidates.len() && posting_index < postings.len() {
        match candidates[candidate_index].cmp(&postings[posting_index]) {
            std::cmp::Ordering::Less => candidate_index += 1,
            std::cmp::Ordering::Greater => posting_index += 1,
            std::cmp::Ordering::Equal => {
                candidates[write_index] = candidates[candidate_index];
                write_index += 1;
                candidate_index += 1;
                posting_index += 1;
            }
        }
    }
    candidates.truncate(write_index);
}

fn intersect_posting(candidates: &mut Vec<u32>, posting: &PostingList) {
    match posting {
        PostingList::Ordinals(postings) => intersect_u32(candidates, postings),
        PostingList::Runs { runs, .. } => {
            let mut candidate_index = 0usize;
            let mut run_index = 0usize;
            let mut write_index = 0usize;
            while candidate_index < candidates.len() && run_index < runs.len() {
                let candidate = candidates[candidate_index];
                let run = runs[run_index];
                let run_end = run.start + run.length;
                if candidate < run.start {
                    candidate_index += 1;
                } else if candidate >= run_end {
                    run_index += 1;
                } else {
                    candidates[write_index] = candidate;
                    write_index += 1;
                    candidate_index += 1;
                }
            }
            candidates.truncate(write_index);
        }
    }
}

fn intersect_postings_limited(
    postings: &[&PostingList],
    newest_first: bool,
    limit: usize,
) -> Vec<u32> {
    let mut matches = Vec::with_capacity(limit);
    let mut observe = |ordinal| {
        if postings[1..]
            .iter()
            .all(|posting| posting.contains(ordinal))
        {
            matches.push(ordinal);
        }
        matches.len() == limit
    };
    match postings[0] {
        PostingList::Ordinals(ordinals) => {
            if newest_first {
                for ordinal in ordinals.iter().rev().copied() {
                    if observe(ordinal) {
                        break;
                    }
                }
            } else {
                for ordinal in ordinals.iter().copied() {
                    if observe(ordinal) {
                        break;
                    }
                }
            }
        }
        PostingList::Runs { runs, .. } => {
            if newest_first {
                'outer_newest: for run in runs.iter().rev() {
                    for ordinal in (run.start..run.start + run.length).rev() {
                        if observe(ordinal) {
                            break 'outer_newest;
                        }
                    }
                }
            } else {
                'outer_oldest: for run in runs {
                    for ordinal in run.start..run.start + run.length {
                        if observe(ordinal) {
                            break 'outer_oldest;
                        }
                    }
                }
            }
        }
    }
    matches
}

fn posting_runs(posting: &[u32]) -> Vec<OrdinalRun> {
    let mut runs = Vec::<OrdinalRun>::new();
    for ordinal in posting.iter().copied() {
        if let Some(run) = runs.last_mut()
            && run.start.saturating_add(run.length) == ordinal
        {
            run.length = run.length.saturating_add(1);
        } else {
            runs.push(OrdinalRun {
                start: ordinal,
                length: 1,
            });
        }
    }
    runs
}

fn encode_posting(posting: &PostingList, encoded: &mut Vec<u8>) -> LogDbResult<()> {
    if let PostingList::Runs { runs, .. } = posting {
        return encode_run_posting(runs, encoded);
    }
    let PostingList::Ordinals(posting) = posting else {
        unreachable!("run postings return above");
    };
    let mut delta = Vec::new();
    delta.push(DELTA_POSTING);
    write_varint(
        u64::try_from(posting.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        &mut delta,
    );
    let mut previous = 0u32;
    for (index, ordinal) in posting.iter().copied().enumerate() {
        if index > 0 && ordinal <= previous {
            return Err(LogDbError::InvalidBlockEncoding(
                "query posting is not ordered",
            ));
        }
        write_varint(u64::from(ordinal - previous), &mut delta);
        previous = ordinal;
    }
    encoded.extend_from_slice(&delta);
    Ok(())
}

fn encode_run_posting(runs: &[OrdinalRun], encoded: &mut Vec<u8>) -> LogDbResult<()> {
    if runs.is_empty() {
        return Err(LogDbError::InvalidBlockEncoding("empty query posting"));
    }
    encoded.push(RUN_POSTING);
    write_varint(
        u64::try_from(runs.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        encoded,
    );
    let mut previous_end = 0u32;
    for run in runs {
        write_varint(u64::from(run.start - previous_end), encoded);
        write_varint(u64::from(run.length), encoded);
        previous_end = run.start.saturating_add(run.length);
    }
    Ok(())
}

fn decode_posting(
    encoded: &[u8],
    cursor: &mut usize,
    record_count: u32,
) -> LogDbResult<PostingList> {
    match read_byte(encoded, cursor)? {
        DELTA_POSTING => {
            let count = read_usize(encoded, cursor)?;
            ensure_count(count, encoded.len().saturating_sub(*cursor))?;
            let mut posting = Vec::with_capacity(count);
            let mut previous = 0u32;
            for index in 0..count {
                let delta = read_u32(encoded, cursor)?;
                let ordinal =
                    previous
                        .checked_add(delta)
                        .ok_or(LogDbError::InvalidBlockEncoding(
                            "query posting delta overflow",
                        ))?;
                if ordinal >= record_count || (index > 0 && ordinal <= previous) {
                    return Err(LogDbError::InvalidBlockEncoding(
                        "invalid query posting ordinal",
                    ));
                }
                posting.push(ordinal);
                previous = ordinal;
            }
            if posting.is_empty() {
                return Err(LogDbError::InvalidBlockEncoding("empty query posting"));
            }
            Ok(PostingList::Ordinals(posting))
        }
        RUN_POSTING => {
            let run_count = read_usize(encoded, cursor)?;
            ensure_count(run_count, encoded.len().saturating_sub(*cursor))?;
            let mut runs = Vec::with_capacity(run_count);
            let mut cardinality = 0usize;
            let mut previous_end = 0u32;
            for _ in 0..run_count {
                let start = previous_end.checked_add(read_u32(encoded, cursor)?).ok_or(
                    LogDbError::InvalidBlockEncoding("query posting run overflow"),
                )?;
                let length = read_u32(encoded, cursor)?;
                let end = start
                    .checked_add(length)
                    .ok_or(LogDbError::InvalidBlockEncoding(
                        "query posting run overflow",
                    ))?;
                if length == 0 || end > record_count {
                    return Err(LogDbError::InvalidBlockEncoding(
                        "invalid query posting run",
                    ));
                }
                cardinality = cardinality
                    .checked_add(usize::try_from(length).map_err(|_| {
                        LogDbError::InvalidBlockEncoding(
                            "query posting cardinality does not fit usize",
                        )
                    })?)
                    .ok_or(LogDbError::InvalidBlockEncoding(
                        "query posting cardinality overflow",
                    ))?;
                runs.push(OrdinalRun { start, length });
                previous_end = end;
            }
            if runs.is_empty() {
                return Err(LogDbError::InvalidBlockEncoding("empty query posting"));
            }
            Ok(PostingList::Runs { runs, cardinality })
        }
        _ => Err(LogDbError::InvalidBlockEncoding(
            "unknown query posting encoding",
        )),
    }
}

fn normalize_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

fn append_bytes(bytes: &[u8], encoded: &mut Vec<u8>) -> LogDbResult<()> {
    write_varint(
        u64::try_from(bytes.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        encoded,
    );
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn write_varint(mut value: u64, encoded: &mut Vec<u8>) {
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

fn read_varint(encoded: &[u8], cursor: &mut usize) -> LogDbResult<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_byte(encoded, cursor)?;
        let payload = u64::from(byte & 0x7f);
        if shift > 63 || (shift == 63 && payload > 1) {
            return Err(LogDbError::InvalidBlockEncoding(
                "query index varint overflow",
            ));
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift = shift.saturating_add(7);
        if shift > 63 {
            return Err(LogDbError::InvalidBlockEncoding(
                "query index varint too long",
            ));
        }
    }
}

fn read_byte(encoded: &[u8], cursor: &mut usize) -> LogDbResult<u8> {
    let byte = *encoded
        .get(*cursor)
        .ok_or(LogDbError::InvalidBlockEncoding("truncated query index"))?;
    *cursor = cursor
        .checked_add(1)
        .ok_or(LogDbError::InvalidBlockEncoding(
            "query index cursor overflow",
        ))?;
    Ok(byte)
}

fn read_u32(encoded: &[u8], cursor: &mut usize) -> LogDbResult<u32> {
    u32::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| LogDbError::InvalidBlockEncoding("query index value does not fit u32"))
}

fn read_usize(encoded: &[u8], cursor: &mut usize) -> LogDbResult<usize> {
    usize::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| LogDbError::InvalidBlockEncoding("query index value does not fit usize"))
}

fn read_bytes<'a>(encoded: &'a [u8], cursor: &mut usize) -> LogDbResult<&'a [u8]> {
    let length = read_usize(encoded, cursor)?;
    let end = cursor
        .checked_add(length)
        .ok_or(LogDbError::InvalidBlockEncoding(
            "query index byte length overflow",
        ))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(LogDbError::InvalidBlockEncoding(
            "truncated query index bytes",
        ))?;
    *cursor = end;
    Ok(bytes)
}

fn decode_text(bytes: &[u8]) -> LogDbResult<Arc<str>> {
    std::str::from_utf8(bytes)
        .map(Arc::<str>::from)
        .map_err(|_| LogDbError::InvalidBlockEncoding("query index text is not UTF-8"))
}

fn ensure_count(count: usize, remaining: usize) -> LogDbResult<()> {
    if count <= remaining {
        Ok(())
    } else {
        Err(LogDbError::InvalidBlockEncoding(
            "query index count exceeds remaining bytes",
        ))
    }
}

#[cfg(test)]
mod tests {
    use shard_stream_core::{ShardId, TopicPartition};

    use super::*;
    use crate::{
        CaseSensitivity, CompressionCohortId, DurableLogRecord, LogPredicate, LogStripe,
        MetadataField, NumericComparison, StripeConfig, TextMatchKind, TextMatcher,
        decode_structural_block, encode_structural_records,
    };

    fn partition() -> TopicPartition {
        TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3))
    }

    fn records(start: u64, count: u64) -> Vec<DurableLogRecord> {
        (start..start + count)
            .map(|offset| {
                let mut message = format!("common request_id={offset}");
                if offset % 10 == 0 {
                    message.push_str(" medium");
                }
                if offset % 100 == 0 {
                    message.push_str(" rare");
                }
                DurableLogRecord::new(
                    ShardId::new(1),
                    partition(),
                    LogicalOffset::new(offset),
                    offset * 10,
                    message,
                    CompressionCohortId::new(1),
                )
                .with_field("service", if offset % 2 == 0 { "api" } else { "worker" })
            })
            .collect()
    }

    fn compatibility_records(count: u64) -> Vec<DurableLogRecord> {
        (0..count)
            .map(|offset| {
                let message = match offset % 4 {
                    0 => format!("INFO request {offset} completed"),
                    1 => format!("ERROR request {offset} cannot access storage"),
                    2 => format!("WARN request {offset} timed out after 250ms"),
                    _ => format!("DEBUG heartbeat node-{offset}"),
                };
                DurableLogRecord::new(
                    ShardId::new(1),
                    partition(),
                    LogicalOffset::new(offset),
                    (offset * 37 % count) * 100 + offset,
                    message,
                    CompressionCohortId::new(1),
                )
                .with_field(
                    "service",
                    match offset % 3 {
                        0 => "api",
                        1 => "worker",
                        _ => "storage",
                    },
                )
                .with_field("env", if offset % 5 == 0 { "dev" } else { "prod" })
                .with_field(
                    "status",
                    if offset % 4 == 1 {
                        "503"
                    } else if offset % 4 == 2 {
                        "429"
                    } else {
                        "200"
                    },
                )
            })
            .collect()
    }

    fn compatibility_index(
        records: &[DurableLogRecord],
        block_records: usize,
    ) -> PersistentQueryIndex {
        PersistentQueryIndex::from_blocks(
            records
                .chunks(block_records)
                .enumerate()
                .map(|(ordinal, records)| {
                    indexed_block(u32::try_from(ordinal).expect("ordinal fits"), records)
                })
                .collect(),
        )
        .expect("compatibility index builds")
    }

    fn cold_matches(
        index: &PersistentQueryIndex,
        records: &[DurableLogRecord],
        block_records: usize,
        query: &LogQuery,
    ) -> Vec<DurableLogRecord> {
        let candidates = index.candidate_hits(query).into_iter().map(|hit| {
            let index = usize::try_from(hit.block_ordinal).expect("block fits") * block_records
                + usize::try_from(hit.record_ordinal).expect("record fits");
            records[index].clone()
        });
        query.select(candidates)
    }

    fn indexed_block(
        block_ordinal: u32,
        records: &[DurableLogRecord],
    ) -> (QueryBlockMetadata, BlockQueryIndex) {
        let (min_timestamp_unix_nanos, max_timestamp_unix_nanos) =
            records
                .iter()
                .fold((u64::MAX, 0), |(minimum, maximum), record| {
                    (
                        minimum.min(record.timestamp_unix_nanos),
                        maximum.max(record.timestamp_unix_nanos),
                    )
                });
        (
            QueryBlockMetadata {
                block_ordinal,
                topic_partition: partition(),
                first_offset: records.first().expect("records exist").record_ref.offset,
                last_offset: records.last().expect("records exist").record_ref.offset,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                record_count: u32::try_from(records.len()).expect("record count fits"),
            },
            BlockQueryIndex::build(records).expect("index builds"),
        )
    }

    #[test]
    fn persistent_index_round_trips_and_preserves_exact_and_results() {
        let first = records(0, 1_000);
        let second = records(1_000, 1_000);
        let index = PersistentQueryIndex::from_blocks(vec![
            indexed_block(0, &first),
            indexed_block(1, &second),
        ])
        .expect("directory builds");
        let encoded = index.encode().expect("index encodes");
        assert_eq!(
            PersistentQueryIndex::decode(&encoded).expect("index decodes"),
            index
        );
        let compressed = index.encode_compressed(1).expect("index compresses");
        let decoded =
            PersistentQueryIndex::decode_compressed(&compressed).expect("index decompresses");
        let hits = decoded.candidate_hits(
            &LogQuery::new(partition())
                .with_term("COMMON")
                .with_term("rare")
                .with_field("service", "api"),
        );
        assert_eq!(
            hits,
            (0..20)
                .map(|index| QueryHit {
                    block_ordinal: u32::from(index >= 10),
                    record_ordinal: u32::try_from((index % 10) * 100).expect("ordinal fits"),
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn newest_limit_and_block_ranges_prune_candidates() {
        let first = records(0, 100);
        let second = records(100, 100);
        let index = PersistentQueryIndex::from_blocks(vec![
            indexed_block(4, &first),
            indexed_block(9, &second),
        ])
        .expect("directory builds");
        assert_eq!(
            index.candidate_hits(
                &LogQuery::new(partition())
                    .with_term("common")
                    .newest_first()
                    .with_limit(3)
            ),
            vec![
                QueryHit {
                    block_ordinal: 9,
                    record_ordinal: 99,
                },
                QueryHit {
                    block_ordinal: 9,
                    record_ordinal: 98,
                },
                QueryHit {
                    block_ordinal: 9,
                    record_ordinal: 97,
                },
            ]
        );
        let ranged = index.candidate_hits(
            &LogQuery::new(partition())
                .with_offset_range(LogicalOffset::new(100), LogicalOffset::new(200)),
        );
        assert_eq!(ranged.len(), 100);
        assert!(ranged.iter().all(|hit| hit.block_ordinal == 9));
    }

    #[test]
    fn hybrid_posting_prefers_runs_for_dense_ordinals() {
        let posting = (0..10_000).collect::<Vec<_>>();
        let posting = PostingList::from_ordinals(posting.clone()).expect("posting builds");
        let mut encoded = Vec::new();
        encode_posting(&posting, &mut encoded).expect("posting encodes");
        assert_eq!(encoded[0], RUN_POSTING);
        let mut cursor = 0;
        assert_eq!(
            decode_posting(&encoded, &mut cursor, 10_000)
                .expect("posting decodes")
                .to_vec(),
            (0..10_000).collect::<Vec<_>>()
        );
        assert_eq!(cursor, encoded.len());
    }

    #[test]
    fn duplicate_metadata_fields_are_indexed_once() {
        let record = DurableLogRecord {
            fields: vec![
                MetadataField::new("service", "api"),
                MetadataField::new("service", "api"),
            ]
            .into(),
            ..records(0, 1).pop().expect("record exists")
        };
        let index = BlockQueryIndex::build(&[record]).expect("index builds");
        assert_eq!(index.field_postings["service"]["api"].to_vec(), vec![0]);
    }

    #[test]
    fn dense_postings_remain_run_encoded_in_resident_index() {
        let records = records(0, 10_000);
        let index = PersistentQueryIndex::from_blocks(vec![indexed_block(0, &records)])
            .expect("directory builds");
        let common = &index.term_postings[&partition()]["common"][0].record_ordinals;
        assert_eq!(common.cardinality(), records.len());
        assert!(common.storage_bytes() < common.cardinality().saturating_mul(size_of::<u32>()) / 2);
        assert_eq!(
            index
                .candidate_hits(
                    &LogQuery::new(partition())
                        .with_term("common")
                        .newest_first()
                        .with_limit(100)
                )
                .len(),
            100
        );
        let limited_intersection = index.candidate_hits(
            &LogQuery::new(partition())
                .with_term("common")
                .with_field("service", "api")
                .newest_first()
                .with_limit(3),
        );
        assert_eq!(
            limited_intersection
                .iter()
                .map(|hit| hit.record_ordinal)
                .collect::<Vec<_>>(),
            vec![9_998, 9_996, 9_994]
        );
    }

    #[test]
    fn hot_and_sealed_lookups_share_full_boolean_compatibility() {
        let records = compatibility_records(60);
        let index = compatibility_index(&records, 10);
        let decoded_blocks = records
            .chunks(10)
            .map(|records| {
                decode_structural_block(
                    &encode_structural_records(records).expect("structural block encodes"),
                )
                .expect("structural block decodes")
            })
            .collect::<Vec<_>>();
        let mut stripe =
            LogStripe::new(ShardId::new(1), StripeConfig::default()).expect("stripe opens");
        for record in records.iter().cloned() {
            stripe.apply_durable(record).expect("record indexes");
        }

        let message_regex =
            LogPredicate::message_regex("cannot|timed out", CaseSensitivity::Insensitive)
                .expect("regex compiles");
        let service_regex =
            LogPredicate::field_regex("service", "^(api|storage)$", CaseSensitivity::Sensitive)
                .expect("regex compiles");
        let queries = vec![
            LogQuery::new(partition())
                .where_predicate(LogPredicate::and(vec![
                    LogPredicate::or(vec![
                        LogPredicate::term("error"),
                        LogPredicate::field_numeric(
                            "status",
                            NumericComparison::GreaterThanOrEqual,
                            500,
                        ),
                    ]),
                    LogPredicate::field_exists("service"),
                    LogPredicate::negate(LogPredicate::field_equals("env", "dev")),
                ]))
                .newest_first()
                .with_limit(11),
            LogQuery::new(partition())
                .where_predicate(LogPredicate::and(vec![
                    message_regex,
                    service_regex,
                    LogPredicate::field_in("env", ["prod", "staging"]),
                ]))
                .with_offset_range(LogicalOffset::new(5), LogicalOffset::new(55)),
            LogQuery::new(partition())
                .where_predicate(LogPredicate::or(vec![
                    LogPredicate::message_contains("heartbeat"),
                    LogPredicate::field_numeric("status", NumericComparison::GreaterThan, 400),
                ]))
                .with_timestamp_range(500, 5_500)
                .sort_by_timestamp()
                .newest_first()
                .with_limit(17),
            LogQuery::new(partition()).where_predicate(LogPredicate::or(vec![
                LogPredicate::message(TextMatcher::new(
                    "INFO request 12 completed",
                    TextMatchKind::Exact,
                    CaseSensitivity::Sensitive,
                )),
                LogPredicate::message(TextMatcher::new(
                    "debug heartbeat",
                    TextMatchKind::Prefix,
                    CaseSensitivity::Insensitive,
                )),
                LogPredicate::message(TextMatcher::new(
                    "250ms",
                    TextMatchKind::Suffix,
                    CaseSensitivity::Sensitive,
                )),
            ])),
            LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                LogPredicate::field(
                    "service",
                    TextMatcher::new("TOR", TextMatchKind::Contains, CaseSensitivity::Insensitive),
                ),
                LogPredicate::field_numeric("status", NumericComparison::Equal, 429),
                LogPredicate::field_numeric("status", NumericComparison::NotEqual, 503),
                LogPredicate::field_numeric("status", NumericComparison::LessThanOrEqual, 429),
            ])),
            LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                LogPredicate::field_numeric("status", NumericComparison::LessThan, 500),
                LogPredicate::field_numeric("status", NumericComparison::GreaterThan, 199),
            ])),
            LogQuery::new(partition()).where_predicate(LogPredicate::MatchNone),
        ];

        for query in queries {
            let hot = stripe
                .query(&query)
                .into_iter()
                .map(|matched| matched.record.record_ref.offset)
                .collect::<Vec<_>>();
            let cold = cold_matches(&index, &records, 10, &query)
                .into_iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>();
            assert_eq!(hot, cold);

            let decoded_candidates = index.candidate_hits(&query).into_iter().map(|hit| {
                decoded_blocks[usize::try_from(hit.block_ordinal).expect("block fits")]
                    [usize::try_from(hit.record_ordinal).expect("record fits")]
                .clone()
            });
            let decoded = query
                .select(decoded_candidates)
                .into_iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>();
            assert_eq!(hot, decoded);
        }
    }

    #[test]
    fn timestamp_cursor_pages_are_stable_across_hot_and_sealed_tiers() {
        let records = compatibility_records(48);
        let index = compatibility_index(&records, 8);
        let mut stripe =
            LogStripe::new(ShardId::new(1), StripeConfig::default()).expect("stripe opens");
        for record in records.iter().cloned() {
            stripe.apply_durable(record).expect("record indexes");
        }

        let first_query = LogQuery::new(partition())
            .where_predicate(LogPredicate::field_exists("service"))
            .sort_by_timestamp()
            .newest_first()
            .with_limit(7);
        let hot_first = stripe.query(&first_query);
        let cold_first = cold_matches(&index, &records, 8, &first_query);
        assert_eq!(
            hot_first
                .iter()
                .map(|matched| matched.record.record_ref.offset)
                .collect::<Vec<_>>(),
            cold_first
                .iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>()
        );

        let cursor = first_query.cursor_for(&hot_first.last().expect("first page exists").record);
        let second_query = first_query.clone().after(cursor);
        let hot_second = stripe.query(&second_query);
        let cold_second = cold_matches(&index, &records, 8, &second_query);
        assert_eq!(
            hot_second
                .iter()
                .map(|matched| matched.record.record_ref.offset)
                .collect::<Vec<_>>(),
            cold_second
                .iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>()
        );
        assert!(hot_first.iter().all(|first| {
            hot_second
                .iter()
                .all(|second| first.record.record_ref != second.record.record_ref)
        }));
    }

    #[test]
    fn residual_queries_do_not_apply_the_limit_before_exact_filtering() {
        let records = compatibility_records(40);
        let index = compatibility_index(&records, 10);
        let query = LogQuery::new(partition())
            .where_predicate(LogPredicate::or(vec![
                LogPredicate::message_contains("heartbeat"),
                LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
            ]))
            .newest_first()
            .with_limit(2);
        let candidates = index.candidate_hits(&query);
        assert_eq!(candidates.len(), records.len());
        assert_eq!(cold_matches(&index, &records, 10, &query).len(), 2);
    }

    #[test]
    fn residual_queries_stream_sealed_blocks_until_the_page_is_complete() {
        let records = compatibility_records(60);
        let index = compatibility_index(&records, 10);
        let query = LogQuery::new(partition())
            .where_predicate(LogPredicate::message_contains("heartbeat"))
            .newest_first()
            .with_limit(4);

        let mut streamed = Vec::new();
        let mut visited_blocks = 0usize;
        for block_ordinal in index.candidate_blocks(&query) {
            visited_blocks += 1;
            let candidates = index
                .candidate_hits_in_block(&query, block_ordinal)
                .into_iter()
                .map(|hit| {
                    records[usize::try_from(hit.block_ordinal).expect("block fits") * 10
                        + usize::try_from(hit.record_ordinal).expect("record fits")]
                    .clone()
                });
            streamed.extend(candidates.filter(|record| query.matches(record)));
            if streamed.len() >= query.limit.expect("query has a limit") {
                break;
            }
        }

        assert!(visited_blocks < index.blocks().len());
        assert_eq!(
            query
                .select(streamed)
                .into_iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>(),
            cold_matches(&index, &records, 10, &query)
                .into_iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn message_trigrams_reject_missing_literal_blocks_without_false_negatives() {
        let mut records = compatibility_records(20);
        records[7].message = "KELVIN alarm from 東京 worker".into();
        let index = compatibility_index(&records, 10);

        let unicode_query = LogQuery::new(partition())
            .where_predicate(LogPredicate::message_contains("kelvin alarm"));
        assert_eq!(index.candidate_blocks(&unicode_query), vec![0]);
        assert_eq!(
            cold_matches(&index, &records, 10, &unicode_query)
                .into_iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>(),
            vec![LogicalOffset::new(7)]
        );

        let missing_query = LogQuery::new(partition()).where_predicate(
            LogPredicate::message_contains("impossible-substring-9f82c4"),
        );
        assert!(index.candidate_blocks(&missing_query).is_empty());
        assert!(index.candidate_hits(&missing_query).is_empty());
        assert!(index.candidate_hits_in_block(&missing_query, 0).is_empty());
    }

    #[test]
    fn every_literal_mode_and_case_policy_retains_unicode_matches() {
        let messages = [
            "İSTANBUL 東京 suffix",
            "Straße/Δelta END",
            "ASCII prefix and suffix",
        ];
        let records = messages
            .iter()
            .enumerate()
            .map(|(offset, message)| {
                DurableLogRecord::new(
                    ShardId::new(1),
                    partition(),
                    LogicalOffset::new(u64::try_from(offset).expect("offset fits")),
                    u64::try_from(offset).expect("timestamp fits"),
                    *message,
                    CompressionCohortId::new(1),
                )
            })
            .collect::<Vec<_>>();
        let index = compatibility_index(&records, 1);
        let queries = [
            TextMatcher::new(
                "İSTANBUL 東京 suffix",
                TextMatchKind::Exact,
                CaseSensitivity::Sensitive,
            ),
            TextMatcher::new(
                "i\u{307}stanbul",
                TextMatchKind::Prefix,
                CaseSensitivity::Insensitive,
            ),
            TextMatcher::new("東京", TextMatchKind::Contains, CaseSensitivity::Sensitive),
            TextMatcher::new("end", TextMatchKind::Suffix, CaseSensitivity::Insensitive),
            TextMatcher::new(
                "ASCII prefix",
                TextMatchKind::Prefix,
                CaseSensitivity::Sensitive,
            ),
            TextMatcher::new("Δ", TextMatchKind::Contains, CaseSensitivity::Sensitive),
        ]
        .map(|matcher| LogQuery::new(partition()).where_predicate(LogPredicate::message(matcher)));

        for query in queries {
            let candidate_blocks = index.candidate_blocks(&query);
            for (block_ordinal, record) in records.iter().enumerate() {
                if query.matches(record) {
                    assert!(
                        candidate_blocks
                            .contains(&u32::try_from(block_ordinal).expect("block fits")),
                        "true literal match was pruned: {query:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn message_trigram_pruning_only_uses_literals_required_by_boolean_logic() {
        let records = compatibility_records(20);
        let index = compatibility_index(&records, 10);
        let missing = LogPredicate::message_contains("impossible-substring-9f82c4");

        let conjunction = LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::term("request"),
            missing.clone(),
        ]));
        assert!(index.candidate_blocks(&conjunction).is_empty());

        let disjunction = LogQuery::new(partition()).where_predicate(LogPredicate::or(vec![
            LogPredicate::term("request"),
            missing.clone(),
        ]));
        assert_eq!(index.candidate_blocks(&disjunction), vec![0, 1]);

        let negation = LogQuery::new(partition()).where_predicate(LogPredicate::negate(missing));
        assert_eq!(index.candidate_blocks(&negation), vec![0, 1]);
    }

    #[test]
    fn trigram_hash_collisions_can_only_create_extra_candidates() {
        let mut by_slot = vec![None::<[u8; 3]>; MESSAGE_TRIGRAM_FILTER_BITS];
        let mut collision = None;
        let lowercase_stable = (b'!'..=b'~')
            .filter(|byte| !byte.is_ascii_uppercase())
            .collect::<Vec<_>>();
        'search: for &first in &lowercase_stable {
            for &second in &lowercase_stable {
                for &third in &lowercase_stable {
                    let trigram = [first, second, third];
                    let slot = message_trigram_slot(trigram);
                    if let Some(previous) = by_slot[slot]
                        && previous != trigram
                    {
                        collision = Some((previous, trigram));
                        break 'search;
                    }
                    by_slot[slot] = Some(trigram);
                }
            }
        }
        let (stored, queried) = collision.expect("the bounded hash has a printable collision");
        let stored = String::from_utf8(stored.to_vec()).expect("printable bytes are UTF-8");
        let queried = String::from_utf8(queried.to_vec()).expect("printable bytes are UTF-8");
        let record = DurableLogRecord::new(
            ShardId::new(1),
            partition(),
            LogicalOffset::new(0),
            0,
            stored,
            CompressionCohortId::new(1),
        );
        let index = compatibility_index(std::slice::from_ref(&record), 1);
        let query =
            LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(queried));

        assert_eq!(index.candidate_blocks(&query), vec![0]);
        assert!(cold_matches(&index, &[record], 1, &query).is_empty());
    }

    #[test]
    fn message_trigram_memory_is_fixed_per_block() {
        let records = compatibility_records(30);
        let index = compatibility_index(&records, 10);
        assert_eq!(
            index.message_trigram_filter_bytes(),
            3 * MESSAGE_TRIGRAM_FILTER_BYTES
        );
    }
}
