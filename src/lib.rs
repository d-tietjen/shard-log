//! Stripe-aligned native and OTLP log indexes for shard-stream.
//!
//! A [`ShardLogDb`] owns one single-writer [`LogStripe`] per shard-stream
//! physical shard. Call [`ShardLogDb::apply_durable`] from the owning
//! shard-stream worker only after the associated append is durable. This keeps
//! the append log authoritative while allowing term and metadata indexes to be
//! queried through a per-partition indexed watermark.

#![warn(missing_docs)]

mod analytics;
mod block;
mod deletion;
mod dictionary;
mod error;
mod ingest_pack;
mod locality;
mod loki_api;
mod loki_store;
mod native_protocol;
mod native_server;
mod otlp;
mod production;
mod query;
mod query_index;
mod realtime_dictionary;
mod sink;
mod sink_journal;
mod storage_format;
mod stripe;
mod structural;
mod tier;
mod tier_ingest;
mod types;

pub use analytics::{
    ANALYTICS_SCHEMA_VERSION, AnalyticsColumn, AnalyticsLogRow, AnalyticsScanRequest,
    CLICKHOUSE_COMPATIBILITY_TARGET,
};
pub use block::{BlockCatalog, BlockDescriptor, BlockId, CompressionCodec};
pub use deletion::DeleteRequest;
pub use dictionary::{
    CompressionCohortId, DictionaryCache, DictionaryCatalog, DictionaryCatalogSnapshot,
    DictionaryId, DictionaryInsert, DictionaryPublication,
};
pub use error::{LogDbError, LogDbResult};
pub use locality::{
    CompressionBlockAssignment, CompressionBlockCollator, CompressionBlockScore,
    CompressionLocalityConfig, CompressionLocalityRecord, CompressionLocalityStats,
    CompressionPlacement, CompressionPlacementId, CompressionShardProfile, CompressionTemperature,
    LocalityGranularity, MessageFingerprint, analyze_message, fingerprint_message,
    scan_message_terms,
};
pub use loki_api::{
    LokiApiConfig, LokiApiError, LokiApiStore, LokiEntry, LokiStore, StoreHealth, StoreMetrics,
    loki_router, loki_router_with_clickhouse, single_tenant_loki_api_router,
    single_tenant_loki_router,
};
pub use loki_store::{DurableLokiConfig, DurableLokiStore, RetentionReport};
pub use native_protocol::{
    MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeAppendAck, NativeBatchInfo,
    NativeFrame, NativeFrameHeader, NativeLogBatch, NativeOpcode, NativeProtocolError, NativeQuery,
    NativeQueryDirection, NativeStatus, decode_native_log_batch, decode_native_log_events,
    decode_native_query, encode_native_log_batch, encode_native_query, inspect_native_log_batch,
    is_native_log_batch, validate_native_log_batch,
};
pub use native_server::{NativeRequestGate, NativeServerConfig, serve_native};
pub use otlp::{OtlpLogDecoder, OtlpLogEvent};
pub use production::{
    ProductionMetricsSnapshot, ProductionRuntime, ServiceLifecycle, ServiceState,
    SingleTenantConfig,
};
pub use query_index::{BlockQueryIndex, PersistentQueryIndex, QueryBlockMetadata, QueryHit};
pub use realtime_dictionary::{
    RealtimeDictionaryConfig, RealtimeDictionaryObserver, RealtimeDictionaryStats,
    RealtimeDictionaryTrainer,
};
pub use sink::{OtlpSinkConfig, ShardLogService, ShardLogSinkFactory, SinkObjectTierConfig};
pub use stripe::{IndexReceipt, LogStripe, ShardLogDb, ShardStreamDurableSink, StripeConfig};
pub use structural::{
    DecodedStructuralRecord, EmbeddedFrameIndex, IndexedStructuralBlock, StructuralRecordView,
    decode_embedded_frame_index, decode_structural_block, decode_structural_records,
    encode_indexed_structural_records, encode_structural_block, encode_structural_records,
    message_pattern,
};
pub use tier::{
    CatalogGroupEntry, CatalogPage, CatalogPageRef, CatalogPointer, CatalogRoot, LocalObjectStore,
    LogObjectStore, LogObjectTier, ObjectMetadata, ObjectTierConfig, SharedLogObjectStore,
    SsdCacheConfig, SsdObjectCache, TierArtifact, TierArtifactKind, TierArtifactSource,
    TierBlockEntry, TierCheckpoint, TierGroupManifest, TierGroupSource, TierQueryRange,
    mark_group_offloaded, write_staged_payload_pack,
};
pub use types::{
    CaseSensitivity, DurableLogRecord, LogMatch, LogPredicate, LogQuery, LogRegex, MetadataField,
    NumericComparison, QueryCursor, QueryOrder, QuerySort, RecordRef, TextMatchKind, TextMatcher,
};
