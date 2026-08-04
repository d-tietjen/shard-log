//! Model-free compression-locality collation.
//!
//! Records receive a cheap integer temperature on ingress, but placement is a
//! block decision. A full block is scored against stripe-local compression
//! shards, split with farthest-point seeds when its internal variance is high,
//! and handed to the nearest shard in bounded owned byte prefixes.

use std::collections::VecDeque;
use std::mem::size_of;
use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig, HandoffBufferPolicy};

use crate::{CompressionCohortId, MetadataField, TelemetryError, TelemetryResult};

const MAX_ROUTER_STATE_BYTES: usize = 512 * 1024;
const MAX_COMPRESSION_SHARDS: usize = 16;
const MAX_SPLIT_DEPTH: u8 = 2;
const SPLIT_EXPLORATION_SLOTS: usize = 64;
const SPLIT_FAILURES_BEFORE_BACKOFF: u8 = 2;
const SPLIT_BACKOFF_BLOCKS: u8 = 63;
const INDEX_BYTES: usize = size_of::<u32>();
const SHAPE_MISMATCH_DISTANCE: u8 = 4;
const MAX_LOCALITY_DISTANCE: u8 = 16 + SHAPE_MISMATCH_DISTANCE;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const BASE_PLACEMENT_DOMAIN: u64 = 0x3a72_3e4b_b78f_19d5;
const COLLATED_PLACEMENT_DOMAIN: u64 = 0xd6e8_feb8_6659_fd93;

/// Stable identifier for a final compression placement.
///
/// Placement IDs select active blocks and immutable compression dictionaries.
/// They are independent of the producer-derived source cohort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressionPlacementId(u64);

impl CompressionPlacementId {
    /// Creates a placement identifier from a stable raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw placement identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the deterministic fail-open placement for a source cohort.
    #[must_use]
    pub fn from_source_cohort(source: CompressionCohortId) -> Self {
        Self(splitmix64(source.get() ^ BASE_PLACEMENT_DOMAIN))
    }

    fn from_temperature(
        source: CompressionCohortId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> Self {
        Self(splitmix64(
            source.get()
                ^ COLLATED_PLACEMENT_DOMAIN
                ^ u64::from(temperature.get()).rotate_left(19)
                ^ shape_hash.rotate_right(11),
        ))
    }
}

/// Integer compression-locality temperature.
///
/// The bits are a SimHash locality signature, so distance is measured with XOR
/// Hamming distance rather than numeric subtraction. It affects placement only
/// and is not needed to decode a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressionTemperature(u16);

impl CompressionTemperature {
    /// Creates a temperature from its integer value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the raw integer temperature.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Returns the Hamming distance between two compression temperatures.
    #[must_use]
    pub const fn distance(self, other: Self) -> u8 {
        (self.0 ^ other.0).count_ones() as u8
    }
}

/// Locality class selected for a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LocalityGranularity {
    /// Producer-derived OTLP service/scope cohort.
    Base = 0,
    /// Block was assigned to an active stripe-local compression shard.
    Collated = 1,
}

/// Final owner-local placement decision for one collated block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompressionPlacement {
    /// Stable block and dictionary placement identifier.
    pub placement_id: CompressionPlacementId,
    /// Byte-weighted centroid of record temperatures.
    pub temperature: CompressionTemperature,
    /// Whether the block uses the base cohort or a compression shard.
    pub granularity: LocalityGranularity,
    /// Locality distance from the block centroid to the selected shard.
    pub distance_to_shard: u8,
    /// Byte-weighted mean squared locality deviation, Q8.
    pub internal_variance_q8: u16,
}

impl CompressionPlacement {
    /// Returns the fail-open placement for a producer-derived source cohort.
    #[must_use]
    pub fn base(source: CompressionCohortId, temperature: CompressionTemperature) -> Self {
        Self {
            placement_id: CompressionPlacementId::from_source_cohort(source),
            temperature,
            granularity: LocalityGranularity::Base,
            distance_to_shard: 0,
            internal_variance_q8: 0,
        }
    }
}

/// Bounded settings for one stripe-local block collator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionLocalityConfig {
    /// Enables block scoring, splitting, and compression-shard placement.
    pub enabled: bool,
    /// Maximum active compression shards owned by one stripe.
    pub max_compression_shards: usize,
    /// Maximum recursive farthest-seed split depth.
    pub max_split_depth: u8,
    /// Minimum records required in each child of a split.
    pub min_split_records: usize,
    /// Minimum source bytes required in each child of a split.
    pub min_split_bytes: u64,
    /// Split blocks above this byte-weighted Q8 variance.
    pub split_variance_q8: u16,
    /// Do not train a specialized shard with a worse leaf variance.
    pub max_shard_variance_q8: u16,
    /// Maximum locality distance for assigning a block to an existing shard.
    pub max_assignment_distance: u8,
    /// Minimum leaf bytes needed to create a new compression shard.
    pub min_admission_bytes: u64,
    /// Maximum memory reserved for persistent collation profiles.
    pub state_budget_bytes: usize,
}

impl Default for CompressionLocalityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_compression_shards: MAX_COMPRESSION_SHARDS,
            max_split_depth: MAX_SPLIT_DEPTH,
            min_split_records: 8,
            min_split_bytes: 64 * 1024,
            split_variance_q8: 768,
            max_shard_variance_q8: 3_072,
            max_assignment_distance: 6,
            min_admission_bytes: 4 * 1024 * 1024,
            state_budget_bytes: MAX_ROUTER_STATE_BYTES,
        }
    }
}

impl CompressionLocalityConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.max_compression_shards == 0 || self.max_compression_shards > MAX_COMPRESSION_SHARDS
        {
            return Err("locality max_compression_shards must be between 1 and 16");
        }
        if self.max_split_depth > MAX_SPLIT_DEPTH {
            return Err("locality max_split_depth must be no larger than 2");
        }
        if self.min_split_records < 2 {
            return Err("locality min_split_records must be at least 2");
        }
        if self.min_split_bytes == 0 || self.min_admission_bytes == 0 {
            return Err("locality byte thresholds must be nonzero");
        }
        if self.split_variance_q8 == 0 || self.max_shard_variance_q8 < self.split_variance_q8 {
            return Err("locality variance thresholds are inconsistent");
        }
        if self.max_assignment_distance > MAX_LOCALITY_DISTANCE {
            return Err("locality max_assignment_distance must be no larger than 20");
        }
        if self.state_budget_bytes == 0 || self.state_budget_bytes > MAX_ROUTER_STATE_BYTES {
            return Err("locality state_budget_bytes must be between 1 and 512 KiB");
        }
        if estimated_state_bytes(self) > self.state_budget_bytes {
            return Err("locality tables exceed locality state_budget_bytes");
        }
        Ok(())
    }
}

/// Allocation-free message analysis used by routing and term indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageFingerprint {
    /// Stable template-shape hash with dynamic values removed.
    pub shape_hash: u64,
    /// Integer SimHash over static and type-class features.
    pub locality_signature: u16,
}

/// One record presented to block collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionLocalityRecord {
    /// Allocation-free message fingerprint.
    pub fingerprint: MessageFingerprint,
    /// Logical source bytes contributed by this record.
    pub source_bytes: u64,
}

/// Integer score for a complete block or sub-block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionBlockScore {
    /// Byte-weighted majority-bit centroid.
    pub temperature: CompressionTemperature,
    /// Representative exact template-shape hash.
    pub shape_hash: u64,
    /// Byte-weighted mean squared locality distance from the centroid, Q8.
    pub internal_variance_q8: u16,
    /// Largest record-to-centroid locality distance.
    pub max_deviation: u8,
    /// Logical source bytes represented by the score.
    pub source_bytes: u64,
    /// Records represented by the score.
    pub record_count: usize,
}

/// Final assignment of one complete sub-block.
#[derive(Debug, Clone)]
pub struct CompressionBlockAssignment {
    /// Selected compression shard and block diagnostics.
    pub placement: CompressionPlacement,
    /// Score calculated before assignment.
    pub score: CompressionBlockScore,
    membership: BlockMembership,
}

impl CompressionBlockAssignment {
    /// Iterates original input indices belonging to this sub-block.
    pub fn record_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.membership.indices()
    }
}

/// Read-only diagnostics for one active compression shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionShardProfile {
    /// Stable placement and dictionary identifier.
    pub placement_id: CompressionPlacementId,
    /// Producer-derived source cohort isolated by this shard.
    pub source_cohort: CompressionCohortId,
    /// Current byte-weighted shard temperature.
    pub temperature: CompressionTemperature,
    /// Current representative exact template-shape hash.
    pub shape_hash: u64,
    /// Integer EWMA of assigned block variance, Q8.
    pub variance_q8: u16,
    /// Blocks assigned to this shard.
    pub blocks: u64,
    /// Source bytes assigned to this shard.
    pub source_bytes: u64,
}

/// Cumulative diagnostics for one stripe-local collator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompressionLocalityStats {
    /// Records scored in complete blocks.
    pub observations: u64,
    /// Complete blocks and recursive sub-blocks scored.
    pub blocks_scored: u64,
    /// Parent blocks divided by farthest-seed partitioning.
    pub blocks_split: u64,
    /// Child blocks produced by splitting.
    pub subblocks_created: u64,
    /// High-variance blocks kept whole after repeated unproductive splits.
    pub split_explorations_suppressed: u64,
    /// Records ultimately kept in the base source cohort.
    pub base_placements: u64,
    /// Records assigned to active compression shards.
    pub collated_placements: u64,
    /// Records moved away from the block's tentative home shard.
    pub records_reassigned: u64,
    /// Source bytes moved away from the tentative home shard.
    pub bytes_reassigned: u64,
    /// Current number of active compression shards.
    pub active_compression_shards: usize,
    /// Largest scored internal variance, Q8.
    pub max_internal_variance_q8: u16,
    /// Packed membership bytes transferred through `bytes-handoff`.
    pub handoff_membership_bytes: u64,
    /// Preallocated persistent profile memory in bytes.
    pub allocated_state_bytes: usize,
}

#[derive(Debug, Clone)]
struct CompressionShard {
    snapshot: CompressionShardProfile,
    one_weights: [u64; 16],
    total_weight: u64,
    shape_vote_weight: u64,
}

#[derive(Debug)]
struct WorkBlock {
    membership: BlockMembership,
    depth: u8,
}

#[derive(Debug, Clone)]
enum BlockMembership {
    Contiguous(Range<usize>),
    Packed(Bytes),
}

impl BlockMembership {
    fn indices(&self) -> MembershipIndices<'_> {
        match self {
            Self::Contiguous(range) => MembershipIndices::Contiguous(range.clone()),
            Self::Packed(bytes) => MembershipIndices::Packed(bytes.chunks_exact(INDEX_BYTES)),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Contiguous(_) => 0,
            Self::Packed(bytes) => bytes.len(),
        }
    }
}

enum MembershipIndices<'a> {
    Contiguous(Range<usize>),
    Packed(std::slice::ChunksExact<'a, u8>),
}

impl Iterator for MembershipIndices<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Contiguous(range) => range.next(),
            Self::Packed(chunks) => chunks.next().map(|chunk| {
                usize::try_from(u32::from_le_bytes(
                    chunk.try_into().expect("membership entry is four bytes"),
                ))
                .expect("u32 membership index fits usize")
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.len();
        (length, Some(length))
    }
}

impl ExactSizeIterator for MembershipIndices<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Contiguous(range) => range.len(),
            Self::Packed(chunks) => chunks.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SplitExploration {
    source_tag: u64,
    occupied: bool,
    failed_explorations: u8,
    blocks_since_exploration: u8,
}

#[derive(Debug, Clone, Copy)]
struct ScoredMembership {
    score: CompressionBlockScore,
    seed_a: usize,
    seed_b: usize,
}

/// Fixed-capacity, owner-local block collator.
#[derive(Debug)]
pub struct CompressionBlockCollator {
    config: CompressionLocalityConfig,
    target_block_bytes: u64,
    shards: Vec<CompressionShard>,
    split_explorations: [SplitExploration; SPLIT_EXPLORATION_SLOTS],
    stats: CompressionLocalityStats,
}

impl CompressionBlockCollator {
    /// Creates preallocated collation state for one stripe.
    pub fn new(
        config: CompressionLocalityConfig,
        target_block_bytes: u64,
    ) -> TelemetryResult<Self> {
        config.validate().map_err(TelemetryError::InvalidConfig)?;
        if target_block_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "locality target_block_bytes must be nonzero",
            ));
        }
        let allocated_state_bytes = estimated_state_bytes(&config);
        let shard_capacity = config.max_compression_shards;
        Ok(Self {
            config,
            target_block_bytes,
            shards: Vec::with_capacity(shard_capacity),
            split_explorations: [SplitExploration::default(); SPLIT_EXPLORATION_SLOTS],
            stats: CompressionLocalityStats {
                allocated_state_bytes,
                ..CompressionLocalityStats::default()
            },
        })
    }

    /// Returns whether block collation is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns a snapshot of cumulative collation diagnostics.
    #[must_use]
    pub fn stats(&self) -> CompressionLocalityStats {
        let mut stats = self.stats;
        stats.active_compression_shards = self.shards.len();
        stats
    }

    /// Returns current read-only compression-shard profiles.
    pub fn compression_shards(
        &self,
    ) -> impl ExactSizeIterator<Item = CompressionShardProfile> + '_ {
        self.shards.iter().map(|shard| shard.snapshot)
    }

    /// Selects the tentative block collector for one record.
    ///
    /// This is only an ingress hint. The final placement is calculated when the
    /// complete block is scored and may differ after outlier filtering.
    #[must_use]
    pub fn tentative_placement(
        &self,
        source: CompressionCohortId,
        fingerprint: MessageFingerprint,
    ) -> CompressionPlacement {
        let temperature = CompressionTemperature::new(fingerprint.locality_signature);
        if !self.config.enabled {
            return CompressionPlacement::base(source, temperature);
        }
        if let Some((index, distance)) =
            self.nearest_shard(source, temperature, fingerprint.shape_hash)
            && distance <= self.config.max_assignment_distance
        {
            return CompressionPlacement {
                placement_id: self.shards[index].snapshot.placement_id,
                temperature,
                granularity: LocalityGranularity::Collated,
                distance_to_shard: distance,
                internal_variance_q8: 0,
            };
        }
        CompressionPlacement::base(source, temperature)
    }

    /// Scores, recursively splits, and assigns one complete candidate block.
    ///
    /// Membership lists are packed as `u32` indices. `bytes-handoff` splits the
    /// packed left prefix from the right tail without per-record channels or
    /// cloned log payloads.
    pub fn collate(
        &mut self,
        source: CompressionCohortId,
        home: CompressionPlacementId,
        records: &[CompressionLocalityRecord],
    ) -> Vec<CompressionBlockAssignment> {
        if records.is_empty() {
            return Vec::new();
        }
        self.stats.observations = self
            .stats
            .observations
            .saturating_add(u64::try_from(records.len()).unwrap_or(u64::MAX));
        let root = BlockMembership::Contiguous(0..records.len());

        let mut work = VecDeque::from([WorkBlock {
            membership: root,
            depth: 0,
        }]);
        let explore_splits = self.should_explore_splits(source);
        let mut split_attempted = false;
        let mut leaves = Vec::new();
        while let Some(block) = work.pop_front() {
            let scored = score_membership(records, &block.membership);
            let score = scored.score;
            self.observe_score(score);
            let nearest = self.nearest_shard(source, score.temperature, score.shape_hash);
            let closer_to_other_shard = nearest.is_some_and(|(index, distance)| {
                let nearest_id = self.shards[index].snapshot.placement_id;
                nearest_id != home
                    && self
                        .profile_distance(home, score.temperature, score.shape_hash)
                        .is_none_or(|home_distance| distance < home_distance)
            });
            let should_split = self.config.enabled
                && explore_splits
                && block.depth < self.config.max_split_depth
                && (score.internal_variance_q8 > self.config.split_variance_q8
                    || closer_to_other_shard);

            if should_split {
                split_attempted = true;
                if let Some((left, right)) =
                    split_membership(records, &block.membership, &self.config, scored)
                {
                    self.stats.blocks_split = self.stats.blocks_split.saturating_add(1);
                    self.stats.subblocks_created = self.stats.subblocks_created.saturating_add(2);
                    self.stats.handoff_membership_bytes =
                        self.stats.handoff_membership_bytes.saturating_add(
                            u64::try_from(left.encoded_len().saturating_add(right.encoded_len()))
                                .unwrap_or(u64::MAX),
                        );
                    work.push_back(WorkBlock {
                        membership: left,
                        depth: block.depth.saturating_add(1),
                    });
                    work.push_back(WorkBlock {
                        membership: right,
                        depth: block.depth.saturating_add(1),
                    });
                    continue;
                }
            }
            leaves.push((block.membership, score));
        }

        let mut assignments = Vec::with_capacity(leaves.len());
        for (membership, score) in leaves {
            let placement = self.assign_leaf(source, records, &membership, score);
            let record_count = u64::try_from(score.record_count).unwrap_or(u64::MAX);
            if placement.granularity == LocalityGranularity::Base {
                self.stats.base_placements =
                    self.stats.base_placements.saturating_add(record_count);
            } else {
                self.stats.collated_placements =
                    self.stats.collated_placements.saturating_add(record_count);
            }
            if placement.placement_id != home {
                self.stats.records_reassigned =
                    self.stats.records_reassigned.saturating_add(record_count);
                self.stats.bytes_reassigned = self
                    .stats
                    .bytes_reassigned
                    .saturating_add(score.source_bytes);
            }
            assignments.push(CompressionBlockAssignment {
                placement,
                score,
                membership,
            });
        }
        assignments.sort_unstable_by_key(|assignment| {
            assignment.record_indices().next().unwrap_or(usize::MAX)
        });
        let specialized = assignments
            .iter()
            .any(|assignment| assignment.placement.granularity == LocalityGranularity::Collated);
        self.observe_split_exploration(source, split_attempted, specialized);
        assignments
    }

    fn should_explore_splits(&mut self, source: CompressionCohortId) -> bool {
        if !self.config.enabled
            || self
                .shards
                .iter()
                .any(|shard| shard.snapshot.source_cohort == source)
        {
            return true;
        }
        let slot = &mut self.split_explorations[usize::try_from(splitmix64(source.get()))
            .unwrap_or(0)
            & (SPLIT_EXPLORATION_SLOTS - 1)];
        if !slot.occupied || slot.source_tag != source.get() {
            *slot = SplitExploration {
                source_tag: source.get(),
                occupied: true,
                ..SplitExploration::default()
            };
        }
        if slot.failed_explorations < SPLIT_FAILURES_BEFORE_BACKOFF
            || slot.blocks_since_exploration >= SPLIT_BACKOFF_BLOCKS
        {
            slot.blocks_since_exploration = 0;
            true
        } else {
            slot.blocks_since_exploration = slot.blocks_since_exploration.saturating_add(1);
            self.stats.split_explorations_suppressed =
                self.stats.split_explorations_suppressed.saturating_add(1);
            false
        }
    }

    fn observe_split_exploration(
        &mut self,
        source: CompressionCohortId,
        attempted: bool,
        specialized: bool,
    ) {
        if !attempted && !specialized {
            return;
        }
        let slot = &mut self.split_explorations[usize::try_from(splitmix64(source.get()))
            .unwrap_or(0)
            & (SPLIT_EXPLORATION_SLOTS - 1)];
        if slot.occupied && slot.source_tag == source.get() {
            if specialized {
                slot.failed_explorations = 0;
                slot.blocks_since_exploration = 0;
            } else if attempted {
                slot.failed_explorations = slot.failed_explorations.saturating_add(1);
            }
        }
    }

    fn assign_leaf(
        &mut self,
        source: CompressionCohortId,
        records: &[CompressionLocalityRecord],
        membership: &BlockMembership,
        score: CompressionBlockScore,
    ) -> CompressionPlacement {
        if !self.config.enabled || score.internal_variance_q8 > self.config.max_shard_variance_q8 {
            return CompressionPlacement {
                internal_variance_q8: score.internal_variance_q8,
                ..CompressionPlacement::base(source, score.temperature)
            };
        }

        let selected = self
            .nearest_shard(source, score.temperature, score.shape_hash)
            .filter(|(_, distance)| *distance <= self.config.max_assignment_distance)
            .or_else(|| {
                (score.source_bytes >= self.config.min_admission_bytes.min(self.target_block_bytes)
                    && self.shards.len() < self.config.max_compression_shards)
                    .then(|| {
                        let index = self.admit_shard(source, score.temperature, score.shape_hash);
                        (index, 0)
                    })
            });

        let Some((index, distance)) = selected else {
            return CompressionPlacement {
                internal_variance_q8: score.internal_variance_q8,
                ..CompressionPlacement::base(source, score.temperature)
            };
        };
        self.observe_shard(index, records, membership, score);
        CompressionPlacement {
            placement_id: self.shards[index].snapshot.placement_id,
            temperature: score.temperature,
            granularity: LocalityGranularity::Collated,
            distance_to_shard: distance,
            internal_variance_q8: score.internal_variance_q8,
        }
    }

    fn nearest_shard(
        &self,
        source: CompressionCohortId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> Option<(usize, u8)> {
        self.shards
            .iter()
            .enumerate()
            .filter(|(_, shard)| shard.snapshot.source_cohort == source)
            .map(|(index, shard)| {
                (
                    index,
                    locality_distance(
                        temperature,
                        shape_hash,
                        shard.snapshot.temperature,
                        shard.snapshot.shape_hash,
                    ),
                    shard.snapshot.variance_q8,
                    shard.snapshot.placement_id,
                )
            })
            .min_by_key(|(_, distance, variance, placement)| {
                (*distance, *variance, placement.get())
            })
            .map(|(index, distance, _, _)| (index, distance))
    }

    fn profile_distance(
        &self,
        placement_id: CompressionPlacementId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> Option<u8> {
        self.shards
            .iter()
            .find(|shard| shard.snapshot.placement_id == placement_id)
            .map(|shard| {
                locality_distance(
                    temperature,
                    shape_hash,
                    shard.snapshot.temperature,
                    shard.snapshot.shape_hash,
                )
            })
    }

    fn admit_shard(
        &mut self,
        source: CompressionCohortId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> usize {
        let mut placement_id =
            CompressionPlacementId::from_temperature(source, temperature, shape_hash);
        if self
            .shards
            .iter()
            .any(|shard| shard.snapshot.placement_id == placement_id)
        {
            placement_id = CompressionPlacementId::new(splitmix64(
                placement_id.get() ^ u64::try_from(self.shards.len()).unwrap_or(u64::MAX),
            ));
        }
        self.shards.push(CompressionShard {
            snapshot: CompressionShardProfile {
                placement_id,
                source_cohort: source,
                temperature,
                shape_hash,
                variance_q8: 0,
                blocks: 0,
                source_bytes: 0,
            },
            one_weights: [0; 16],
            total_weight: 0,
            shape_vote_weight: 0,
        });
        self.shards.len() - 1
    }

    fn observe_shard(
        &mut self,
        index: usize,
        records: &[CompressionLocalityRecord],
        membership: &BlockMembership,
        score: CompressionBlockScore,
    ) {
        let shard = &mut self.shards[index];
        if shard.total_weight > u64::MAX / 4 {
            shard.total_weight >>= 1;
            shard.shape_vote_weight >>= 1;
            for weight in &mut shard.one_weights {
                *weight >>= 1;
            }
        }
        for record_index in membership.indices() {
            let record = records[record_index];
            let weight = locality_weight(record.source_bytes);
            shard.total_weight = shard.total_weight.saturating_add(weight);
            for bit in 0..16 {
                if record.fingerprint.locality_signature & (1u16 << bit) != 0 {
                    shard.one_weights[bit] = shard.one_weights[bit].saturating_add(weight);
                }
            }
        }
        shard.snapshot.temperature = CompressionTemperature::new(centroid_from_weights(
            &shard.one_weights,
            shard.total_weight,
        ));
        if shard.snapshot.blocks == 0 || shard.snapshot.shape_hash == score.shape_hash {
            shard.snapshot.shape_hash = score.shape_hash;
            shard.shape_vote_weight = shard.shape_vote_weight.saturating_add(score.source_bytes);
        } else if shard.shape_vote_weight > score.source_bytes {
            shard.shape_vote_weight = shard.shape_vote_weight.saturating_sub(score.source_bytes);
        } else {
            shard.snapshot.shape_hash = score.shape_hash;
            shard.shape_vote_weight = score.source_bytes.saturating_sub(shard.shape_vote_weight);
        }
        shard.snapshot.variance_q8 = if shard.snapshot.blocks == 0 {
            score.internal_variance_q8
        } else {
            u16::try_from(
                (u32::from(shard.snapshot.variance_q8) * 7 + u32::from(score.internal_variance_q8))
                    / 8,
            )
            .expect("variance EWMA remains u16")
        };
        shard.snapshot.blocks = shard.snapshot.blocks.saturating_add(1);
        shard.snapshot.source_bytes = shard
            .snapshot
            .source_bytes
            .saturating_add(score.source_bytes);
    }

    fn observe_score(&mut self, score: CompressionBlockScore) {
        self.stats.blocks_scored = self.stats.blocks_scored.saturating_add(1);
        self.stats.max_internal_variance_q8 = self
            .stats
            .max_internal_variance_q8
            .max(score.internal_variance_q8);
    }
}

/// Scans a message once, calling `on_term` for every Unicode alphanumeric term.
///
/// Dynamic structural tokens contribute only type and logarithmic length
/// classes. Metadata values never enter either fingerprint.
pub fn analyze_message(
    message: &str,
    fields: &[MetadataField],
    mut on_term: impl FnMut(&str),
) -> MessageFingerprint {
    let (mut shape_hash, mut simhash) = if message.is_ascii() {
        analyze_ascii_body(message, &mut on_term)
    } else {
        analyze_unicode_body(message, &mut on_term)
    };

    for field in fields {
        shape_hash = fnv_bytes(shape_hash, &[0xf1]);
        shape_hash = fnv_bytes(shape_hash, field.key.as_bytes());
        add_simhash_feature(
            &mut simhash,
            fnv_bytes(FNV_OFFSET ^ 0x6d65_7461_6b65_7900, field.key.as_bytes()),
        );
    }

    let mut locality_signature = 0u16;
    for (bit, weight) in simhash.into_iter().enumerate() {
        if weight > 0 {
            locality_signature |= 1u16 << bit;
        }
    }
    MessageFingerprint {
        shape_hash,
        locality_signature,
    }
}

/// Scans only case-preserving search terms without calculating compression
/// fingerprints.
///
/// The default production locality policy is disabled. Keeping this path
/// separate avoids template hashing and 16-lane SimHash work when ingestion
/// only needs the inverted search index.
pub fn scan_message_terms(message: &str, mut on_term: impl FnMut(&str)) {
    if message.is_ascii() {
        let mut start = None;
        for (index, byte) in message.bytes().enumerate() {
            match (start, byte.is_ascii_alphanumeric()) {
                (None, true) => start = Some(index),
                (Some(term_start), false) => {
                    on_term(&message[term_start..index]);
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(term_start) = start {
            on_term(&message[term_start..]);
        }
        return;
    }

    let mut start = None;
    for (index, character) in message.char_indices() {
        match (start, character.is_alphanumeric()) {
            (None, true) => start = Some(index),
            (Some(term_start), false) => {
                on_term(&message[term_start..index]);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(term_start) = start {
        on_term(&message[term_start..]);
    }
}

fn analyze_ascii_body(message: &str, on_term: &mut impl FnMut(&str)) -> (u64, [i16; 16]) {
    let bytes = message.as_bytes();
    let mut shape_hash = FNV_OFFSET;
    let mut simhash = [0i16; 16];
    let mut structural_start = 0usize;
    let mut structural_is_token = None;
    let mut structural_has_digit = false;
    let mut term_start = None;

    for (index, byte) in bytes.iter().copied().enumerate() {
        let is_term = byte.is_ascii_alphanumeric();
        match (term_start, is_term) {
            (None, true) => term_start = Some(index),
            (Some(start), false) => {
                on_term(&message[start..index]);
                term_start = None;
            }
            _ => {}
        }

        let is_token = is_template_token_byte(byte);
        match structural_is_token {
            None => {
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = byte.is_ascii_digit();
            }
            Some(current) if current == is_token => {
                structural_has_digit |= byte.is_ascii_digit();
            }
            Some(current) => {
                analyze_structural_run(
                    &message[structural_start..index],
                    current,
                    structural_has_digit,
                    &mut shape_hash,
                    &mut simhash,
                );
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = byte.is_ascii_digit();
            }
        }
    }
    if let Some(start) = term_start {
        on_term(&message[start..]);
    }
    if let Some(is_token) = structural_is_token {
        analyze_structural_run(
            &message[structural_start..],
            is_token,
            structural_has_digit,
            &mut shape_hash,
            &mut simhash,
        );
    }
    (shape_hash, simhash)
}

fn analyze_unicode_body(message: &str, on_term: &mut impl FnMut(&str)) -> (u64, [i16; 16]) {
    let mut shape_hash = FNV_OFFSET;
    let mut simhash = [0i16; 16];
    let mut structural_start = 0usize;
    let mut structural_is_token = None;
    let mut structural_has_digit = false;
    let mut term_start = None;

    for (index, character) in message.char_indices() {
        let is_term = character.is_alphanumeric();
        match (term_start, is_term) {
            (None, true) => term_start = Some(index),
            (Some(start), false) => {
                on_term(&message[start..index]);
                term_start = None;
            }
            _ => {}
        }

        let is_token = is_template_token_character(character);
        match structural_is_token {
            None => {
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = character.is_ascii_digit();
            }
            Some(current) if current == is_token => {
                structural_has_digit |= character.is_ascii_digit();
            }
            Some(current) => {
                analyze_structural_run(
                    &message[structural_start..index],
                    current,
                    structural_has_digit,
                    &mut shape_hash,
                    &mut simhash,
                );
                structural_start = index;
                structural_is_token = Some(is_token);
                structural_has_digit = character.is_ascii_digit();
            }
        }
    }
    if let Some(start) = term_start {
        on_term(&message[start..]);
    }
    if let Some(is_token) = structural_is_token {
        analyze_structural_run(
            &message[structural_start..],
            is_token,
            structural_has_digit,
            &mut shape_hash,
            &mut simhash,
        );
    }
    (shape_hash, simhash)
}

/// Returns a fingerprint without observing search terms.
#[must_use]
pub fn fingerprint_message(message: &str, fields: &[MetadataField]) -> MessageFingerprint {
    analyze_message(message, fields, |_| {})
}

fn analyze_structural_run(
    run: &str,
    is_token: bool,
    has_digit: bool,
    shape_hash: &mut u64,
    simhash: &mut [i16; 16],
) {
    if is_token && has_digit {
        *shape_hash = fnv_bytes(*shape_hash, &[0xd1]);
        let class = dynamic_class(run);
        let length_class = length_class(run.len());
        add_simhash_feature(
            simhash,
            splitmix64(0x6479_6e61_6d69_6300 ^ (u64::from(class) << 8) ^ u64::from(length_class)),
        );
    } else {
        *shape_hash = fnv_bytes(*shape_hash, &[0x51]);
        *shape_hash = fnv_bytes(*shape_hash, run.as_bytes());
        add_simhash_feature(
            simhash,
            fnv_bytes(FNV_OFFSET ^ 0x6c69_7465_7261_6c00, run.as_bytes()),
        );
    }
}

fn is_template_token_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.' | '/' | ':')
}

fn is_template_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':')
}

fn dynamic_class(run: &str) -> u8 {
    let bytes = run.as_bytes();
    if bytes.iter().all(u8::is_ascii_digit) {
        1
    } else if bytes.iter().all(u8::is_ascii_hexdigit) {
        2
    } else if bytes.contains(&b'-') {
        3
    } else if bytes.contains(&b'.') || bytes.contains(&b':') || bytes.contains(&b'/') {
        4
    } else {
        5
    }
}

fn length_class(length: usize) -> u8 {
    let length = u64::try_from(length).unwrap_or(u64::MAX).max(1);
    u8::try_from(length.ilog2()).unwrap_or(u8::MAX).min(15)
}

fn add_simhash_feature(weights: &mut [i16; 16], feature_hash: u64) {
    let hash = splitmix64(feature_hash);
    for (bit, weight) in weights.iter_mut().enumerate() {
        if hash & (1u64 << bit) == 0 {
            *weight = weight.saturating_sub(1);
        } else {
            *weight = weight.saturating_add(1);
        }
    }
}

fn fnv_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn fingerprint_distance(left: MessageFingerprint, right: MessageFingerprint) -> u8 {
    locality_distance(
        CompressionTemperature::new(left.locality_signature),
        left.shape_hash,
        CompressionTemperature::new(right.locality_signature),
        right.shape_hash,
    )
}

fn locality_distance(
    left_temperature: CompressionTemperature,
    left_shape_hash: u64,
    right_temperature: CompressionTemperature,
    right_shape_hash: u64,
) -> u8 {
    left_temperature
        .distance(right_temperature)
        .saturating_add(u8::from(left_shape_hash != right_shape_hash) * SHAPE_MISMATCH_DISTANCE)
}

fn score_membership(
    records: &[CompressionLocalityRecord],
    membership: &BlockMembership,
) -> ScoredMembership {
    let seed_a = membership
        .indices()
        .next()
        .expect("scored membership is nonempty");
    let mut seed_b = seed_a;
    let mut seed_distance = 0u8;
    let mut one_weights = [0u64; 16];
    let mut total_weight = 0u64;
    let mut source_bytes = 0u64;
    let mut record_count = 0usize;
    let mut shape_hash = records[seed_a].fingerprint.shape_hash;
    let mut shape_vote_weight = 0u64;
    for index in membership.indices() {
        let record = records[index];
        let weight = locality_weight(record.source_bytes);
        total_weight = total_weight.saturating_add(weight);
        source_bytes = source_bytes.saturating_add(record.source_bytes);
        record_count = record_count.saturating_add(1);
        for (bit, one_weight) in one_weights.iter_mut().enumerate() {
            if record.fingerprint.locality_signature & (1u16 << bit) != 0 {
                *one_weight = one_weight.saturating_add(weight);
            }
        }
        if record.fingerprint.shape_hash == shape_hash {
            shape_vote_weight = shape_vote_weight.saturating_add(weight);
        } else if shape_vote_weight > weight {
            shape_vote_weight -= weight;
        } else {
            shape_hash = record.fingerprint.shape_hash;
            shape_vote_weight = weight - shape_vote_weight;
        }
        let distance = fingerprint_distance(records[seed_a].fingerprint, record.fingerprint);
        if distance > seed_distance {
            seed_b = index;
            seed_distance = distance;
        }
    }
    let temperature =
        CompressionTemperature::new(centroid_from_weights(&one_weights, total_weight));
    let mut squared_distance_weight = 0u128;
    let mut max_deviation = 0u8;
    for index in membership.indices() {
        let record = records[index];
        let distance = locality_distance(
            temperature,
            shape_hash,
            CompressionTemperature::new(record.fingerprint.locality_signature),
            record.fingerprint.shape_hash,
        );
        max_deviation = max_deviation.max(distance);
        squared_distance_weight = squared_distance_weight.saturating_add(
            u128::from(distance)
                .saturating_mul(u128::from(distance))
                .saturating_mul(u128::from(locality_weight(record.source_bytes))),
        );
    }
    let variance_q8 = squared_distance_weight
        .saturating_mul(256)
        .checked_div(u128::from(total_weight.max(1)))
        .unwrap_or(u128::from(u16::MAX))
        .min(u128::from(u16::MAX));
    ScoredMembership {
        score: CompressionBlockScore {
            temperature,
            shape_hash,
            internal_variance_q8: u16::try_from(variance_q8).expect("variance was bounded to u16"),
            max_deviation,
            source_bytes,
            record_count,
        },
        seed_a,
        seed_b,
    }
}

fn split_membership(
    records: &[CompressionLocalityRecord],
    membership: &BlockMembership,
    config: &CompressionLocalityConfig,
    scored: ScoredMembership,
) -> Option<(BlockMembership, BlockMembership)> {
    let seed_a = scored.seed_a;
    let seed_b = scored.seed_b;
    if seed_a == seed_b {
        return None;
    }
    let goes_left = |index: usize| {
        fingerprint_distance(records[index].fingerprint, records[seed_a].fingerprint)
            <= fingerprint_distance(records[index].fingerprint, records[seed_b].fingerprint)
    };

    let mut left_records = 0usize;
    let mut right_records = 0usize;
    let mut left_bytes = 0u64;
    let mut right_bytes = 0u64;
    let packed_capacity = membership.indices().len().saturating_mul(INDEX_BYTES);
    let mut left = BytesMut::with_capacity(packed_capacity);
    let mut right = BytesMut::with_capacity(packed_capacity / 2);
    for index in membership.indices() {
        if goes_left(index) {
            left_records = left_records.saturating_add(1);
            left_bytes = left_bytes.saturating_add(records[index].source_bytes);
            left.put_u32_le(
                u32::try_from(index).expect("a block cannot contain more than u32 rows"),
            );
        } else {
            right_records = right_records.saturating_add(1);
            right_bytes = right_bytes.saturating_add(records[index].source_bytes);
            right.put_u32_le(
                u32::try_from(index).expect("a block cannot contain more than u32 rows"),
            );
        }
    }
    if left_records < config.min_split_records
        || right_records < config.min_split_records
        || left_bytes < config.min_split_bytes
        || right_bytes < config.min_split_bytes
    {
        return None;
    }

    let left_len = left.len();
    left.unsplit(right);
    let packed = left;
    let max_len = packed.len().max(1);
    let mut handoff = HandoffBuffer::from_tail_with_policy(
        packed,
        HandoffBufferConfig::new(max_len),
        HandoffBufferPolicy::new().with_small_prefix_copy_max(0),
    )
    .expect("packed sub-block membership fits its exact handoff limit");
    let left = handoff
        .split_prefix(left_len)
        .expect("counted left prefix is present");
    let right = handoff.freeze_all();
    Some((
        BlockMembership::Packed(left),
        BlockMembership::Packed(right),
    ))
}

fn locality_weight(source_bytes: u64) -> u64 {
    source_bytes.max(1).min(u64::from(u32::MAX))
}

fn centroid_from_weights(one_weights: &[u64; 16], total_weight: u64) -> u16 {
    let mut centroid = 0u16;
    for (bit, one_weight) in one_weights.iter().enumerate() {
        if one_weight.saturating_mul(2) >= total_weight.max(1) {
            centroid |= 1u16 << bit;
        }
    }
    centroid
}

fn estimated_state_bytes(config: &CompressionLocalityConfig) -> usize {
    config
        .max_compression_shards
        .saturating_mul(size_of::<CompressionShard>())
        .saturating_add(size_of::<[SplitExploration; SPLIT_EXPLORATION_SLOTS]>())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CompressionLocalityConfig {
        CompressionLocalityConfig {
            enabled: true,
            min_split_records: 2,
            min_split_bytes: 1,
            split_variance_q8: 64,
            max_shard_variance_q8: u16::MAX,
            min_admission_bytes: 1,
            ..CompressionLocalityConfig::default()
        }
    }

    fn locality_record(signature: u16, source_bytes: u64) -> CompressionLocalityRecord {
        CompressionLocalityRecord {
            fingerprint: MessageFingerprint {
                shape_hash: u64::from(signature),
                locality_signature: signature,
            },
            source_bytes,
        }
    }

    #[test]
    fn dynamic_values_preserve_template_shape() {
        let first = fingerprint_message("2026-07-29T10:22:31Z request 12345 took 981 ms", &[]);
        let second = fingerprint_message("2027-01-03T09:00:02Z request 987654321 took 12 ms", &[]);
        assert_eq!(first.shape_hash, second.shape_hash);
    }

    #[test]
    fn static_changes_affect_the_fingerprint() {
        let first = fingerprint_message("request 123 failed", &[]);
        let second = fingerprint_message("request 123 succeeded", &[]);
        assert_ne!(first, second);
    }

    #[test]
    fn exact_shape_mismatch_adds_a_grouping_penalty() {
        let first = MessageFingerprint {
            shape_hash: 1,
            locality_signature: 0x1234,
        };
        let second = MessageFingerprint {
            shape_hash: 2,
            locality_signature: 0x1234,
        };
        assert_eq!(fingerprint_distance(first, second), SHAPE_MISMATCH_DISTANCE);
    }

    #[test]
    fn scanner_reports_unicode_terms_without_allocating_a_collection() {
        let mut terms = Vec::new();
        let _ = analyze_message("Échec: request-42 東京", &[], |term| {
            terms.push(term.to_owned());
        });
        assert_eq!(terms, ["Échec", "request", "42", "東京"]);
    }

    #[test]
    fn term_only_scanner_matches_full_analysis_for_ascii_and_unicode() {
        for message in [
            "ERROR request-42 took 981ms",
            "Échec: request-42 東京",
            "",
            "---",
        ] {
            let mut full = Vec::new();
            let _ = analyze_message(message, &[], |term| full.push(term.to_owned()));
            let mut terms_only = Vec::new();
            scan_message_terms(message, |term| terms_only.push(term.to_owned()));
            assert_eq!(terms_only, full, "{message:?}");
        }
    }

    #[test]
    fn homogeneous_block_admits_a_compression_shard() {
        let mut router =
            CompressionBlockCollator::new(test_config(), 1_024).expect("collator validates");
        let source = CompressionCohortId::new(9);
        let records = vec![locality_record(0x1234, 128); 16];
        let home = CompressionPlacementId::from_source_cohort(source);
        let assignments = router.collate(source, home, &records);
        assert_eq!(assignments.len(), 1);
        assert_eq!(
            assignments[0].placement.granularity,
            LocalityGranularity::Collated
        );
        assert_eq!(assignments[0].score.internal_variance_q8, 0);
        assert_eq!(router.stats().active_compression_shards, 1);
        assert_eq!(
            router
                .tentative_placement(source, records[0].fingerprint)
                .placement_id,
            assignments[0].placement.placement_id
        );
    }

    #[test]
    fn sparse_and_restarted_collators_fail_open_to_base() {
        let config = CompressionLocalityConfig {
            enabled: true,
            min_admission_bytes: 1_024,
            ..CompressionLocalityConfig::default()
        };
        let source = CompressionCohortId::new(4);
        let mut first = CompressionBlockCollator::new(config.clone(), 8 * 1024 * 1024)
            .expect("collator config validates");
        let records = [locality_record(0x1234, 128)];
        assert_eq!(
            first.collate(
                source,
                CompressionPlacementId::from_source_cohort(source),
                &records
            )[0]
            .placement
            .granularity,
            LocalityGranularity::Base
        );
        let mut restarted = CompressionBlockCollator::new(config, 8 * 1024 * 1024)
            .expect("collator config validates");
        assert_eq!(
            restarted.collate(
                source,
                CompressionPlacementId::from_source_cohort(source),
                &records
            )[0]
            .placement
            .granularity,
            LocalityGranularity::Base
        );
    }

    #[test]
    fn high_variance_blocks_split_with_farthest_seeds() {
        let mut router =
            CompressionBlockCollator::new(test_config(), 1_024).expect("collator validates");
        let source = CompressionCohortId::new(7);
        let records = (0..8)
            .map(|_| locality_record(0x0000, 128))
            .chain((0..8).map(|_| locality_record(0xffff, 128)))
            .collect::<Vec<_>>();
        let assignments = router.collate(
            source,
            CompressionPlacementId::from_source_cohort(source),
            &records,
        );
        assert_eq!(assignments.len(), 2);
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.score.internal_variance_q8 == 0)
        );
        assert_eq!(router.stats().blocks_split, 1);
        assert_eq!(router.stats().subblocks_created, 2);
        assert_eq!(router.stats().active_compression_shards, 2);
    }

    #[test]
    fn blocks_closer_to_another_shard_are_reassigned_after_splitting() {
        let mut router =
            CompressionBlockCollator::new(test_config(), 1_024).expect("collator validates");
        let source = CompressionCohortId::new(7);
        let first_records = vec![locality_record(0x0000, 128); 8];
        let first = router.collate(
            source,
            CompressionPlacementId::from_source_cohort(source),
            &first_records,
        );
        let first_home = first[0].placement.placement_id;
        let second_records = vec![locality_record(0xffff, 128); 8];
        let second = router.collate(source, first_home, &second_records);
        assert_ne!(second[0].placement.placement_id, first_home);
        assert_eq!(router.stats().records_reassigned, 16);
    }

    #[test]
    fn collation_is_deterministic_and_bounded() {
        let config = CompressionLocalityConfig {
            max_compression_shards: 4,
            ..test_config()
        };
        let mut first =
            CompressionBlockCollator::new(config.clone(), 512).expect("collator config validates");
        let mut second =
            CompressionBlockCollator::new(config, 512).expect("collator config validates");
        let source = CompressionCohortId::new(7);
        let records = (0..64)
            .map(|index| locality_record((index % 4 * 0x1111) as u16, 128))
            .collect::<Vec<_>>();
        let home = CompressionPlacementId::from_source_cohort(source);
        let first_assignments = first.collate(source, home, &records);
        let second_assignments = second.collate(source, home, &records);
        assert_eq!(
            first_assignments
                .iter()
                .map(|assignment| (
                    assignment.placement,
                    assignment.record_indices().collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>(),
            second_assignments
                .iter()
                .map(|assignment| (
                    assignment.placement,
                    assignment.record_indices().collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>()
        );
        assert!(first_assignments.len() <= 1usize << MAX_SPLIT_DEPTH);
        assert!(first.stats().active_compression_shards <= 4);
        assert!(first.stats().allocated_state_bytes <= MAX_ROUTER_STATE_BYTES);
    }

    #[test]
    fn excess_compression_shards_fall_back_instead_of_exceeding_the_cap() {
        let config = CompressionLocalityConfig {
            max_compression_shards: 1,
            max_assignment_distance: 0,
            ..test_config()
        };
        let mut router =
            CompressionBlockCollator::new(config, 512).expect("collator config validates");
        let source = CompressionCohortId::new(12);
        let home = CompressionPlacementId::from_source_cohort(source);
        let first = router.collate(source, home, &[locality_record(0, 128); 4]);
        let second = router.collate(source, home, &[locality_record(u16::MAX, 128); 4]);
        assert_eq!(router.stats().active_compression_shards, 1);
        assert_eq!(
            usize::from(first[0].placement.granularity != LocalityGranularity::Base)
                + usize::from(second[0].placement.granularity != LocalityGranularity::Base),
            1
        );
    }

    #[test]
    fn membership_handoff_preserves_every_record_exactly_once() {
        let records = (0..32)
            .map(|index| locality_record(if index % 2 == 0 { 0 } else { u16::MAX }, 64))
            .collect::<Vec<_>>();
        let membership = BlockMembership::Contiguous(0..records.len());
        let scored = score_membership(&records, &membership);
        let (left, right) =
            split_membership(&records, &membership, &test_config(), scored).expect("block splits");
        let mut observed = left.indices().chain(right.indices()).collect::<Vec<_>>();
        observed.sort_unstable();
        assert_eq!(observed, (0..records.len()).collect::<Vec<_>>());
    }

    #[test]
    fn repeated_unproductive_splits_enter_bounded_backoff() {
        let config = CompressionLocalityConfig {
            min_split_bytes: 1024,
            max_shard_variance_q8: 64,
            ..test_config()
        };
        let mut collator =
            CompressionBlockCollator::new(config, 512).expect("collator config validates");
        let source = CompressionCohortId::new(19);
        let home = CompressionPlacementId::from_source_cohort(source);
        let records = [
            locality_record(0, 64),
            locality_record(u16::MAX, 64),
            locality_record(0, 64),
            locality_record(u16::MAX, 64),
        ];
        for _ in 0..4 {
            let assignments = collator.collate(source, home, &records);
            assert!(
                assignments
                    .iter()
                    .all(|assignment| assignment.placement.granularity == LocalityGranularity::Base)
            );
        }
        assert!(collator.stats().split_explorations_suppressed >= 1);
        assert_eq!(collator.stats().active_compression_shards, 0);
    }

    #[test]
    fn production_defaults_fit_the_collator_memory_budget() {
        let config = CompressionLocalityConfig::default();
        config.validate().expect("default collator is bounded");
        assert!(estimated_state_bytes(&config) <= MAX_ROUTER_STATE_BYTES);
    }

    #[test]
    fn invalid_collator_limits_are_rejected_without_allocating() {
        let error = CompressionBlockCollator::new(
            CompressionLocalityConfig {
                max_compression_shards: 0,
                ..CompressionLocalityConfig::default()
            },
            8 * 1024 * 1024,
        )
        .expect_err("zero compression shards is invalid");
        assert_eq!(
            error,
            TelemetryError::InvalidConfig(
                "locality max_compression_shards must be between 1 and 16"
            )
        );
    }
}
