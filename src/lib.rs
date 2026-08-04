//! Signal-native logs, traces, and metrics storage for shard-stream.
//!
//! A [`ShardTelemetry`] owns one single-writer [`LogStripe`] per shard-stream
//! physical shard. Call [`ShardTelemetry::apply_durable`] from the owning
//! shard-stream worker only after the associated append is durable. This keeps
//! the append log authoritative while allowing term and metadata indexes to be
//! queried through a per-partition indexed watermark.

#![warn(missing_docs)]

mod analytics;
mod block;
mod deletion;
mod dictionary;
mod envelope;
mod error;
mod ingest_pack;
mod locality;
mod loki_api;
mod loki_store;
mod metric;
mod native_protocol;
mod native_server;
mod otlp;
mod otlp_server;
mod otlp_signal;
mod production;
mod prometheus_api;
mod prometheus_protocol;
mod promql;
mod query;
mod query_index;
mod realtime_dictionary;
mod remote_write;
mod signal_ingest;
mod sink;
mod sink_journal;
mod storage_format;
mod stripe;
mod structural;
mod telemetry;
mod tempo_api;
mod tempo_protocol;
mod tier;
mod tier_ingest;
mod trace;
mod traceql;
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
pub use envelope::{MAX_TELEMETRY_ENVELOPE_BYTES, TelemetryEnvelope};
pub use error::{TelemetryError, TelemetryResult};
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
pub use metric::{
    DurableMetricPoint, ExplicitHistogramValue, ExponentialHistogramBuckets,
    ExponentialHistogramValue, HistogramBucketSpan, HistogramCount, MetricApplyOutcome,
    MetricExemplar, MetricIdentity, MetricIngestProtocol, MetricKind, MetricQuery, MetricStripe,
    MetricValue, NumberValue, SeriesAccumulatorCheckpoint, SummaryQuantileValue, SummaryValue,
    decode_metric_chunk, encode_metric_chunk, prometheus_string_labels,
};
pub use native_protocol::{
    MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeFrame, NativeFrameHeader,
    NativeLogQueryResult, NativeOpcode, NativePartitionAck, NativePartitionAppend,
    NativeProtocolError, NativeQuery, NativeQueryDirection, NativeStatus, NativeTelemetryAppendAck,
    NativeTelemetryBatch, decode_native_log_query_result, decode_native_query,
    encode_native_log_query_result, encode_native_query, is_native_telemetry_batch,
};
pub use native_server::{NativeRequestGate, NativeServerConfig, serve_native};
pub use otlp::{OtlpLogDecoder, OtlpLogEvent};
pub use otlp_server::{OtlpIngestService, OtlpReceiverConfig, otlp_http_router, serve_otlp_grpc};
pub use otlp_signal::{OtlpMetricEvent, OtlpSpanEvent, OtlpTelemetryDecoder};
pub use production::{
    ProductionMetricsSnapshot, ProductionRuntime, ServiceLifecycle, ServiceState,
    SingleTenantConfig,
};
pub use prometheus_api::{PrometheusApiConfig, PrometheusService, prometheus_router};
pub use promql::{
    PromqlEngine, PromqlError, PromqlLimits, PromqlSample, PromqlSeries, PromqlValue,
};
pub use query_index::{BlockQueryIndex, PersistentQueryIndex, QueryBlockMetadata, QueryHit};
pub use realtime_dictionary::{
    RealtimeDictionaryConfig, RealtimeDictionaryObserver, RealtimeDictionaryStats,
    RealtimeDictionaryTrainer,
};
pub use remote_write::{
    DecodedRemoteWrite, METRIC_FLAG_STALE, PROMETHEUS_STALE_NAN_BITS, RemoteWriteDecoder,
    RemoteWriteStats, RemoteWriteVersion,
};
pub use signal_ingest::{
    prepare_log_envelope, prepare_loki_log_envelope, prepare_metric_envelope,
    prepare_metric_envelope_with_protocol, prepare_trace_envelope,
};
pub use sink::{OtlpSinkConfig, SinkObjectTierConfig, TelemetryService, TelemetrySinkFactory};
pub use stripe::{IndexReceipt, LogStripe, ShardStreamDurableSink, ShardTelemetry, StripeConfig};
pub use structural::{
    DecodedStructuralRecord, EmbeddedFrameIndex, IndexedStructuralBlock, StructuralLogMetadataRef,
    StructuralRecordView, decode_embedded_frame_index, decode_structural_block,
    decode_structural_records, encode_indexed_structural_records, encode_structural_block,
    encode_structural_records, message_pattern,
};
pub use telemetry::{
    LOGS_TOPIC_ID, METRICS_TOPIC_ID, ResourceContext, ScopeContext, SeriesFingerprint,
    ShardTelemetryConfig, SignalConfig, SpanId, TRACES_TOPIC_ID, TelemetryAttribute,
    TelemetryEntityRef, TelemetryRouter, TelemetrySignal, TelemetryValue, TraceId,
};
pub use tempo_api::{TempoApiConfig, TempoService, tempo_router};
pub use tier::{
    CatalogGroupEntry, CatalogPage, CatalogPageRef, CatalogPointer, CatalogRoot, LocalObjectStore,
    ObjectMetadata, ObjectTierConfig, SharedTelemetryObjectStore, SignalTierPayload,
    SsdCacheConfig, SsdObjectCache, TelemetryObjectStore, TelemetryObjectTier, TierArtifact,
    TierArtifactKind, TierArtifactSource, TierBlockEntry, TierCheckpoint, TierGroupManifest,
    TierGroupSource, TierQueryRange, decode_signal_recovery_state, mark_group_offloaded,
    stage_signal_group, write_staged_payload_pack,
};
pub use trace::{
    DurableSpan, SpanEvent, SpanLink, SpanStatus, TraceApplyOutcome, TraceDirectory, TraceQuery,
    TraceStripe, TraceSummary, decode_trace_block, encode_trace_block,
};
pub use traceql::{TraceqlEngine, TraceqlError, TraceqlLimits, TraceqlTrace};
pub use types::{
    CaseSensitivity, DurableLog, LogMatch, LogPredicate, LogQuery, LogRegex, MetadataField,
    NumericComparison, QueryCursor, QueryOrder, QuerySort, TelemetryRecordRef, TextMatchKind,
    TextMatcher,
};
