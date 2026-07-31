use std::collections::BTreeMap;
use std::sync::Arc;

use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::{CompressionCohortId, CompressionPlacementId, DictionaryId, LogDbError, LogDbResult};

/// Compression format used for one sealed log block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// A standalone Zstandard frame, optionally using the descriptor dictionary.
    Zstd,
}

/// Monotonically assigned identifier for a sealed log data block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockId(u64);

impl BlockId {
    /// Creates a block identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw block identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Immutable metadata describing a sealed compression block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockDescriptor {
    /// Stable local block identifier.
    pub block_id: BlockId,
    /// Owning physical shard-stream shard.
    pub stream_shard_id: ShardId,
    /// Logical partition represented by this block.
    pub topic_partition: TopicPartition,
    /// Producer-derived OTLP service/scope cohort retained for reconstruction.
    pub source_compression_cohort: CompressionCohortId,
    /// Final locality placement used to select this block and its dictionary.
    pub placement_id: CompressionPlacementId,
    /// Immutable compression dictionary required to decode the block, if any.
    pub dictionary_id: Option<DictionaryId>,
    /// Compression format used for the staged block payload.
    pub compression_codec: CompressionCodec,
    /// Encoder level used for the block.
    pub compression_level: i32,
    /// Lowest offset present in the block.
    pub first_offset: LogicalOffset,
    /// Highest offset present in the block. Interleaved cohorts may leave gaps.
    pub last_offset: LogicalOffset,
    /// Number of log records in this block.
    pub record_count: u32,
    /// Uncompressed message and metadata bytes represented by the block.
    pub source_bytes: u64,
    /// Bytes in the structural payload before the block's byte codec runs.
    pub structural_bytes: u64,
    /// Bytes in the compressed staged payload.
    pub stored_bytes: u64,
    /// Lowest event timestamp present in the block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp present in the block.
    pub max_timestamp_unix_nanos: u64,
    /// Byte-weighted majority-bit temperature for this complete block.
    pub compression_temperature: u16,
    /// Representative exact template-shape hash for this complete block.
    pub compression_shape_hash: u64,
    /// Byte-weighted mean squared locality deviation, Q8.
    pub compression_temperature_variance_q8: u16,
    /// Largest record-to-block locality distance.
    pub max_compression_temperature_deviation: u8,
    /// Object-store key after the block is offloaded.
    pub object_key: Option<Arc<str>>,
    /// Byte offset of this block inside its immutable object payload.
    pub object_offset: Option<u64>,
}

/// In-memory directory of sealed blocks owned by one log stripe.
#[derive(Debug, Default)]
pub struct BlockCatalog {
    next_block_id: u64,
    blocks: BTreeMap<BlockId, BlockDescriptor>,
    staged_payloads: BTreeMap<BlockId, Arc<[u8]>>,
}

impl BlockCatalog {
    /// Returns the number of sealed blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Returns whether no blocks have been sealed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Looks up a sealed block descriptor.
    #[must_use]
    pub fn get(&self, block_id: BlockId) -> Option<&BlockDescriptor> {
        self.blocks.get(&block_id)
    }

    /// Iterates sealed block descriptors in identifier order.
    pub fn iter(&self) -> impl Iterator<Item = &BlockDescriptor> {
        self.blocks.values()
    }

    /// Returns the compressed payload staged locally until an object-tier writer
    /// durably offloads the block.
    #[must_use]
    pub fn staged_payload(&self, block_id: BlockId) -> Option<Arc<[u8]>> {
        self.staged_payloads.get(&block_id).cloned()
    }

    pub(crate) fn seal(
        &mut self,
        mut block: BlockDescriptor,
        payload: Arc<[u8]>,
    ) -> BlockDescriptor {
        let block_id = BlockId::new(self.next_block_id);
        self.next_block_id = self.next_block_id.wrapping_add(1);
        block.block_id = block_id;
        self.blocks.insert(block_id, block.clone());
        self.staged_payloads.insert(block_id, payload);
        block
    }

    /// Associates a durable object-store key with an already sealed block.
    pub fn mark_offloaded(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
    ) -> LogDbResult<()> {
        self.mark_offloaded_range(block_id, object_key, 0)
    }

    /// Associates a durable object-store key and byte offset with a sealed block.
    pub fn mark_offloaded_range(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
        object_offset: u64,
    ) -> LogDbResult<()> {
        let block = self
            .blocks
            .get_mut(&block_id)
            .ok_or_else(|| LogDbError::UnknownBlock(block_id.get()))?;
        block.object_key = Some(object_key.into());
        block.object_offset = Some(object_offset);
        self.staged_payloads.remove(&block_id);
        Ok(())
    }
}
