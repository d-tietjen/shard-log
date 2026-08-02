use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, PlacementSequence, TopicId, TopicPartition,
};
use shard_stream_engine::{
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkOptions, EngineConfig, EngineError,
    StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};

use crate::deletion::DeleteCatalog;
use crate::ingest_pack::{decode_ingest_pack, prepare_native_ingest_pack};
use crate::loki_api::{LogicalDeleteFilter, LokiApiError, apply_logical_deletes};
use crate::storage_format::DataDirectoryLease;
use crate::{
    AnalyticsLogRow, AnalyticsScanRequest, DeleteRequest, LogMatch, LogQuery, LokiEntry, LokiStore,
    NativeAppendAck, NativeQuery, NativeQueryDirection, OtlpSinkConfig, QueryCursor,
    ShardLogService, ShardLogSinkFactory, StoreHealth, StoreMetrics, StripeConfig,
    encode_native_log_batch,
};

const LOKI_TOPIC_ID: TopicId = TopicId::new(1);
const LABEL_PREFIX: &str = "resource.loki.label.";
const METADATA_PREFIX: &str = "attr.loki.metadata.";
const TENANT_FIELD: &str = "resource.loki.tenant";

/// Standalone durable Loki-store configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLokiConfig {
    /// Directory containing shard-stream packs, coordinator state, and index journals.
    pub data_directory: PathBuf,
    /// Optional local object-store directory used by shard-stream.
    pub object_store_directory: Option<PathBuf>,
    /// Retain a second raw-payload journal for faster hot-index recovery.
    ///
    /// When disabled, startup reconstructs the ephemeral hot index from the
    /// authoritative shard-stream packs and ingestion performs one durable
    /// payload write.
    pub recovery_journal: bool,
    /// Logical retention window. `None` retains records indefinitely.
    ///
    /// Queries never expose records older than this duration. Physical byte
    /// reclamation is performed by tier compaction, independently of the
    /// immediate logical cutoff.
    pub retention: Option<Duration>,
    /// Number of physical single-owner stripes.
    pub shard_count: u32,
    /// Stable tenant partitions spread across physical stripes.
    pub tenant_partitions: u32,
    /// Maximum time shard-stream may collect adjacent appends before one write and sync.
    pub append_linger: Duration,
    /// Stripe block, index, dictionary, and locality limits.
    pub stripe: StripeConfig,
    /// Maximum time a durable append may wait for indexed read visibility.
    pub indexed_ack_timeout: Duration,
}

impl DurableLokiConfig {
    fn validate(&self) -> Result<(), LokiApiError> {
        if self.shard_count == 0 {
            return Err(LokiApiError::configuration("shard_count must be nonzero"));
        }
        if self.tenant_partitions == 0 {
            return Err(LokiApiError::configuration(
                "tenant_partitions must be nonzero",
            ));
        }
        if self.indexed_ack_timeout.is_zero() {
            return Err(LokiApiError::configuration(
                "indexed_ack_timeout must be nonzero",
            ));
        }
        if self.retention.is_some_and(|retention| retention.is_zero()) {
            return Err(LokiApiError::configuration(
                "retention must be nonzero when configured",
            ));
        }
        if self.retention.is_some() && !self.recovery_journal {
            return Err(LokiApiError::configuration(
                "retention currently requires recovery_journal so the durable index checkpoint survives log truncation",
            ));
        }
        Ok(())
    }
}

/// Loki protocol backend whose acknowledged writes are durable shard-stream
/// appends and whose reads execute on the owning ShardLog stripe workers.
pub struct DurableLokiStore {
    _data_directory_lease: DataDirectoryLease,
    engine: Arc<StreamEngine>,
    service: ShardLogService,
    tenant_partitions: u32,
    ingest_stripes_per_tenant: u32,
    indexed_ack_timeout: Duration,
    next_request_id: AtomicU64,
    deletes: DeleteCatalog,
    retention: Option<Duration>,
    retention_runs: AtomicU64,
    retention_advanced_offsets: AtomicU64,
    retention_failures: AtomicU64,
}

/// Result of one batch-aligned physical retention pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionReport {
    /// Timestamp cutoff applied to every partition.
    pub cutoff_timestamp_unix_nanos: u64,
    /// Partitions whose durable log start advanced.
    pub advanced_partitions: u64,
    /// Logical records made eligible for pack reclamation.
    pub advanced_offsets: u64,
}

impl std::fmt::Debug for DurableLokiStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableLokiStore")
            .field("tenant_partitions", &self.tenant_partitions)
            .field("ingest_stripes_per_tenant", &self.ingest_stripes_per_tenant)
            .field("indexed_ack_timeout", &self.indexed_ack_timeout)
            .finish_non_exhaustive()
    }
}

impl DurableLokiStore {
    /// Opens or recovers a standalone durable store.
    pub fn open(config: DurableLokiConfig) -> Result<Self, LokiApiError> {
        config.validate()?;
        let data_directory_lease = DataDirectoryLease::acquire(&config.data_directory)?;
        let deletes = DeleteCatalog::open(config.data_directory.join("delete-catalog-v1.json"))?;
        let engine_config = EngineConfig {
            data_dir: config.data_directory.join("stream"),
            object_store_dir: config.object_store_directory,
            shard_count: config.shard_count,
            virtual_lane_count: config.shard_count,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 1_024,
            queue_bytes_per_shard: 32 * 1024 * 1024,
            target_pack_bytes: 8 * 1024 * 1024,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: 16 * 1024 * 1024,
            max_fetch_bytes: 16 * 1024 * 1024,
            append_linger: config.append_linger,
        };
        let sink_config = OtlpSinkConfig {
            stripe: config.stripe,
            state_directory: config
                .recovery_journal
                .then(|| config.data_directory.join("index-journal")),
            ..OtlpSinkConfig::default()
        };
        let factory = Arc::new(
            ShardLogSinkFactory::new(engine_config.shard_ids(), sink_config)
                .map_err(|error| LokiApiError::internal(error.to_string()))?,
        );
        let service = factory.service();
        let sink_options = DurableSinkOptions {
            worker_count: config.shard_count as usize,
            recovery_timeout: config.indexed_ack_timeout,
            ..DurableSinkOptions::default()
        };
        let engine = Arc::new(
            StreamEngine::open_with_durable_sink(
                engine_config,
                DurableSinkConfig::new(factory).with_options(sink_options),
            )
            .map_err(engine_error)?,
        );
        match engine.create_topic(TopicConfig {
            topic_id: LOKI_TOPIC_ID,
            partitions: config.tenant_partitions,
            shards: None,
        }) {
            Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
            Err(error) => return Err(engine_error(error)),
        }
        Ok(Self {
            _data_directory_lease: data_directory_lease,
            engine,
            service,
            tenant_partitions: config.tenant_partitions,
            ingest_stripes_per_tenant: config.shard_count.min(config.tenant_partitions),
            indexed_ack_timeout: config.indexed_ack_timeout,
            next_request_id: AtomicU64::new(1),
            deletes,
            retention: config.retention,
            retention_runs: AtomicU64::new(0),
            retention_advanced_offsets: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
        })
    }

    /// Attaches ShardLog's Loki/query surface to a stream engine opened by an
    /// external HA host.
    ///
    /// The host must install the matching [`ShardLogSinkFactory`] as the
    /// engine's durable sink before recovery. This constructor never opens a
    /// second WAL and never changes the host's replication or fencing policy.
    pub fn attach(
        data_directory: PathBuf,
        engine: Arc<StreamEngine>,
        service: ShardLogService,
        tenant_partitions: u32,
        ingest_stripes_per_tenant: u32,
        indexed_ack_timeout: Duration,
        retention: Option<Duration>,
    ) -> Result<Self, LokiApiError> {
        if tenant_partitions == 0 || ingest_stripes_per_tenant == 0 {
            return Err(LokiApiError::configuration(
                "tenant and ingest stripe counts must be nonzero",
            ));
        }
        if ingest_stripes_per_tenant > tenant_partitions {
            return Err(LokiApiError::configuration(
                "ingest stripe count cannot exceed tenant partitions",
            ));
        }
        if indexed_ack_timeout.is_zero() {
            return Err(LokiApiError::configuration(
                "indexed_ack_timeout must be nonzero",
            ));
        }
        if retention.is_some_and(|retention| retention.is_zero()) {
            return Err(LokiApiError::configuration(
                "retention must be nonzero when configured",
            ));
        }
        let data_directory_lease = DataDirectoryLease::acquire(&data_directory)?;
        let deletes = DeleteCatalog::open(data_directory.join("delete-catalog-v1.json"))?;
        match engine.create_topic(TopicConfig {
            topic_id: LOKI_TOPIC_ID,
            partitions: tenant_partitions,
            shards: None,
        }) {
            Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
            Err(error) => return Err(engine_error(error)),
        }
        Ok(Self {
            _data_directory_lease: data_directory_lease,
            engine,
            service,
            tenant_partitions,
            ingest_stripes_per_tenant,
            indexed_ack_timeout,
            next_request_id: AtomicU64::new(1),
            deletes,
            retention,
            retention_runs: AtomicU64::new(0),
            retention_advanced_offsets: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
        })
    }

    /// Atomically replaces one tenant's local delete view from replicated HA
    /// control state.
    ///
    /// The caller must supply only records which have already reached its
    /// cluster finality boundary. Query filtering observes the replacement
    /// only after the local catalog is durably synchronized.
    pub fn synchronize_delete_requests(
        &self,
        tenant: &str,
        requests: Vec<DeleteRequest>,
    ) -> Result<(), LokiApiError> {
        self.deletes.replace_tenant(tenant, requests)
    }

    fn tenant_partition_base(&self, tenant: &str) -> u32 {
        let hash = tenant
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        let groups = self.tenant_partitions / self.ingest_stripes_per_tenant;
        ((hash % u64::from(groups.max(1))) as u32) * self.ingest_stripes_per_tenant
    }

    fn write_partition(&self, tenant: &str, request_id: u64) -> TopicPartition {
        let partition = self.tenant_partition_base(tenant)
            + (request_id % u64::from(self.ingest_stripes_per_tenant)) as u32;
        TopicPartition::new(
            LOKI_TOPIC_ID,
            LogicalPartitionId::new(partition % self.tenant_partitions),
        )
    }

    fn tenant_partitions(&self, tenant: &str) -> impl Iterator<Item = TopicPartition> {
        let base = self.tenant_partition_base(tenant);
        let count = self.ingest_stripes_per_tenant;
        (0..count).map(move |stripe| {
            TopicPartition::new(
                LOKI_TOPIC_ID,
                LogicalPartitionId::new((base + stripe) % self.tenant_partitions),
            )
        })
    }

    fn retention_cutoff(&self) -> Option<u64> {
        let retention = self.retention?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let cutoff = now.saturating_sub(retention.as_nanos());
        Some(u64::try_from(cutoff).unwrap_or(u64::MAX))
    }

    fn retained_query_start(&self, requested: Option<u64>) -> Option<u64> {
        match (requested, self.retention_cutoff()) {
            (Some(requested), Some(cutoff)) => Some(requested.max(cutoff)),
            (None, Some(cutoff)) => Some(cutoff),
            (requested, None) => requested,
        }
    }

    /// Advances shard-stream retention at whole append-batch boundaries.
    ///
    /// The durable sink checkpoint is an engine-level retention pin, so this
    /// cannot reclaim a source pack before its query index has applied it.
    pub fn compact_retention(&self) -> Result<RetentionReport, LokiApiError> {
        let Some(cutoff) = self.retention_cutoff() else {
            return Ok(RetentionReport::default());
        };
        let result = self.compact_retention_before(cutoff);
        self.retention_runs.fetch_add(1, Ordering::Relaxed);
        match &result {
            Ok(report) => {
                self.retention_advanced_offsets
                    .fetch_add(report.advanced_offsets, Ordering::Relaxed);
            }
            Err(_) => {
                self.retention_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    fn compact_retention_before(&self, cutoff: u64) -> Result<RetentionReport, LokiApiError> {
        self.flush(self.indexed_ack_timeout)?;
        let mut report = RetentionReport {
            cutoff_timestamp_unix_nanos: cutoff,
            ..RetentionReport::default()
        };
        for partition_id in self.engine.topic_partitions(LOKI_TOPIC_ID) {
            let partition = TopicPartition::new(LOKI_TOPIC_ID, partition_id);
            let watermarks = self.engine.watermarks(partition).map_err(engine_error)?;
            let original_start = watermarks.log_start;
            let mut scan_offset = original_start;
            let mut retained_start = original_start;
            let mut reached_retained_batch = false;
            while scan_offset < watermarks.last_stable_offset && !reached_retained_batch {
                let batches = self
                    .engine
                    .fetch(FetchRequest {
                        request_id: 0,
                        topic_id: partition.topic_id,
                        partition_id: partition.partition_id,
                        start_offset: scan_offset,
                        max_bytes: 16 * 1024 * 1024,
                        mode: FetchMode::Ordered,
                    })
                    .map_err(engine_error)?;
                if batches.is_empty() {
                    break;
                }
                for batch in batches {
                    let records = decode_ingest_pack(&batch.payload)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    if records
                        .iter()
                        .any(|record| record.timestamp_unix_nanos >= cutoff)
                    {
                        reached_retained_batch = true;
                        break;
                    }
                    let next = batch
                        .last_offset
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| LokiApiError::internal("retention offset exhausted"))?;
                    retained_start = LogicalOffset::new(next);
                    scan_offset = retained_start;
                }
            }
            if retained_start > original_start {
                self.engine
                    .truncate_partition(partition, retained_start)
                    .map_err(engine_error)?;
                report.advanced_partitions += 1;
                report.advanced_offsets = report
                    .advanced_offsets
                    .saturating_add(retained_start.get().saturating_sub(original_start.get()));
            }
        }
        Ok(report)
    }

    /// Appends an already encoded grouped native batch without a protobuf
    /// transcode and waits until the stripe-owned index checkpoint is durable.
    pub fn append_native_batch(&self, payload: Vec<u8>) -> Result<NativeAppendAck, LokiApiError> {
        self.append_native_batch_with_visibility(payload, true)
    }

    /// Appends an already encoded grouped native batch and acknowledges as
    /// soon as its checksummed ingest pack is durable in shard-stream.
    ///
    /// The stripe index applies the append asynchronously under bounded
    /// shard-stream backpressure. Callers that require immediate query
    /// visibility should use [`Self::append_native_batch`].
    pub fn append_native_batch_durable(
        &self,
        payload: Vec<u8>,
    ) -> Result<NativeAppendAck, LokiApiError> {
        self.append_native_batch_with_visibility(payload, false)
    }

    pub(crate) fn append_native_batch_for_tenant(
        &self,
        payload: Vec<u8>,
        expected_tenant: &str,
        wait_for_index: bool,
    ) -> Result<(NativeAppendAck, u32), LokiApiError> {
        let prepared = prepare_native_ingest_pack(&payload)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        if prepared.tenant != expected_tenant {
            return Err(LokiApiError::forbidden(
                "native batch tenant does not match the authenticated tenant",
            ));
        }
        if prepared.record_count == 0 {
            return Err(LokiApiError::bad_request(
                "native append batch must contain at least one record",
            ));
        }
        let records = prepared.record_count;
        let acknowledgement = self.append_encoded(
            &prepared.tenant,
            records,
            prepared.ingest_pack.payload,
            prepared.ingest_pack.transient_context,
            wait_for_index,
        )?;
        Ok((acknowledgement, records))
    }

    fn append_native_batch_with_visibility(
        &self,
        payload: Vec<u8>,
        wait_for_index: bool,
    ) -> Result<NativeAppendAck, LokiApiError> {
        let prepared = prepare_native_ingest_pack(&payload)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        if prepared.record_count == 0 {
            return Err(LokiApiError::bad_request(
                "native append batch must contain at least one record",
            ));
        }
        self.append_encoded(
            &prepared.tenant,
            prepared.record_count,
            prepared.ingest_pack.payload,
            prepared.ingest_pack.transient_context,
            wait_for_index,
        )
    }

    /// Executes a native exact-label/token query directly against the bounded
    /// stripe indexes and merges tenant partitions by timestamp.
    pub fn query_native(&self, request: &NativeQuery) -> Result<Vec<LokiEntry>, LokiApiError> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        if !delete_filter.is_empty() {
            return self.query_native_with_deletes(request, &delete_filter);
        }
        let queries = self
            .tenant_partitions(&request.tenant)
            .map(|partition| {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(request.limit as usize)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                if request.direction == NativeQueryDirection::NewestFirst {
                    query = query.newest_first();
                }
                for (key, value) in &request.labels {
                    query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
                }
                for term in &request.terms {
                    query = query.with_term(term.as_str());
                }
                query
            })
            .collect::<Vec<_>>();
        let mut matches = self
            .service
            .query_partitions(&queries)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        matches.sort_unstable_by(|left, right| {
            let ordering = left
                .record
                .timestamp_unix_nanos
                .cmp(&right.record.timestamp_unix_nanos)
                .then_with(|| {
                    left.record
                        .record_ref
                        .offset
                        .cmp(&right.record.record_ref.offset)
                });
            match request.direction {
                NativeQueryDirection::OldestFirst => ordering,
                NativeQueryDirection::NewestFirst => ordering.reverse(),
            }
        });
        matches.truncate(request.limit as usize);
        matches.into_iter().map(log_match_to_entry).collect()
    }

    fn query_native_with_deletes(
        &self,
        request: &NativeQuery,
        delete_filter: &LogicalDeleteFilter,
    ) -> Result<Vec<LokiEntry>, LokiApiError> {
        let result_limit = request.limit as usize;
        let page_limit = result_limit.clamp(1_024, 8_192);
        let mut accepted = Vec::<(LokiEntry, u64)>::new();
        for partition in self.tenant_partitions(&request.tenant) {
            let mut after = None;
            let mut accepted_from_partition = 0usize;
            loop {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(page_limit)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                query.after = after;
                if request.direction == NativeQueryDirection::NewestFirst {
                    query = query.newest_first();
                }
                for (key, value) in &request.labels {
                    query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
                }
                for term in &request.terms {
                    query = query.with_term(term.as_str());
                }
                let matches = self
                    .service
                    .query_partitions(std::slice::from_ref(&query))
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if matches.is_empty() {
                    break;
                }
                let returned = matches.len();
                let last = matches.last().expect("non-empty query page");
                after = Some(QueryCursor::new(
                    last.record.timestamp_unix_nanos,
                    last.record.record_ref.offset,
                ));
                for matched in matches {
                    let offset = matched.record.record_ref.offset.get();
                    let entry = log_match_to_entry(matched)?;
                    if !delete_filter.matches(&entry) {
                        accepted.push((entry, offset));
                        accepted_from_partition += 1;
                        if accepted_from_partition == result_limit {
                            break;
                        }
                    }
                }
                if accepted_from_partition == result_limit || returned < page_limit {
                    break;
                }
            }
        }
        accepted.sort_unstable_by(|(left, left_offset), (right, right_offset)| {
            let ordering = left
                .timestamp_unix_nanos
                .cmp(&right.timestamp_unix_nanos)
                .then_with(|| left_offset.cmp(right_offset));
            match request.direction {
                NativeQueryDirection::OldestFirst => ordering,
                NativeQueryDirection::NewestFirst => ordering.reverse(),
            }
        });
        accepted.truncate(result_limit);
        Ok(accepted.into_iter().map(|(entry, _)| entry).collect())
    }

    fn append_encoded(
        &self,
        tenant: &str,
        record_count: u32,
        payload: Vec<u8>,
        transient_context: Bytes,
        wait_for_index: bool,
    ) -> Result<NativeAppendAck, LokiApiError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let partition = self.write_partition(tenant, request_id);
        let appended = self
            .engine
            .append_with_durable_sink_context(
                AppendRequest {
                    request_id: u128::from(request_id),
                    topic_id: partition.topic_id,
                    partition_id: partition.partition_id,
                    record_count,
                    payload: Bytes::from(payload),
                    durability: Durability::Leader,
                    producer: None,
                    atomic_group: None,
                    leader_epoch: None,
                    extension_context: None,
                },
                transient_context,
            )
            .map_err(engine_error)?;
        let target = DurableSinkCheckpoint {
            topic_partition: partition,
            next_placement_sequence: PlacementSequence::new(
                appended
                    .placement
                    .sequence
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| LokiApiError::internal("placement sequence exhausted"))?,
            ),
            next_offset: LogicalOffset::new(
                appended
                    .last_offset
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| LokiApiError::internal("logical offset exhausted"))?,
            ),
        };
        if wait_for_index
            && let Err(wait_error) = self.engine.wait_for_durable_sink_checkpoint(target)
        {
            let checkpoint = self
                .engine
                .durable_sink_checkpoint(partition)
                .map_err(engine_error)?
                .ok_or_else(|| LokiApiError::internal("durable sink has no checkpoint"))?;
            let stats = self.engine.durable_sink_stats();
            return Err(LokiApiError::internal(format!(
                "failed waiting for indexed partition {} through offset {}: {}; \
                 checkpoint is {}, pending items {}, pending bytes {}, applied {}, \
                 retries {}, failures {}, dirty partitions {}",
                partition.partition_id.get(),
                appended.last_offset.get(),
                wait_error,
                checkpoint.next_offset.get(),
                stats.pending_items,
                stats.pending_bytes,
                stats.applied_appends,
                stats.retry_attempts,
                stats.failed_attempts,
                stats.dirty_partitions,
            )));
        }
        Ok(NativeAppendAck {
            partition_id: partition.partition_id.get(),
            first_offset: appended.first_offset.get(),
            last_offset: appended.last_offset.get(),
        })
    }
}

impl LokiStore for DurableLokiStore {
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        if entries.is_empty() {
            return Ok(());
        }
        let record_count = u32::try_from(entries.len())
            .map_err(|_| LokiApiError::bad_request("push contains more than u32 entries"))?;
        let payload = encode_native_log_batch(tenant, entries)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let acknowledgement = self.append_native_batch(payload)?;
        debug_assert_eq!(
            acknowledgement
                .last_offset
                .saturating_sub(acknowledgement.first_offset)
                .saturating_add(1),
            u64::from(record_count)
        );
        Ok(())
    }

    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let queries = self
            .tenant_partitions(tenant)
            .map(|partition| {
                LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_field(TENANT_FIELD, tenant)
            })
            .collect::<Vec<_>>();
        let cutoff = self.retention_cutoff();
        let queries = queries
            .into_iter()
            .map(|mut query| {
                query.start_timestamp_unix_nanos = cutoff;
                query
            })
            .collect::<Vec<_>>();
        let matches = self
            .service
            .query_partitions(&queries)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let mut entries = matches
            .into_iter()
            .map(log_match_to_entry)
            .collect::<Result<Vec<_>, _>>()?;
        apply_logical_deletes(&mut entries, &self.deletes.list(tenant)?)?;
        entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
        Ok(entries)
    }

    fn scan_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsLogRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        let mut emitted = 0usize;
        for partition in self.tenant_partitions(&request.tenant) {
            let mut next_offset = None;
            loop {
                let page_limit = 8_192usize.min(limit.saturating_sub(emitted));
                if page_limit == 0 {
                    return Ok(());
                }
                let mut query = LogQuery::new(partition)
                    .with_limit(page_limit)
                    .with_field(TENANT_FIELD, request.tenant.as_ref());
                query.start_offset = next_offset.map(LogicalOffset::new);
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                for term in &request.terms {
                    query = query.with_term(Arc::clone(term));
                }
                for field in &request.labels {
                    query = query.with_field(
                        format!("{LABEL_PREFIX}{}", field.key),
                        Arc::clone(&field.value),
                    );
                }
                for field in &request.metadata {
                    query = query.with_field(
                        format!("{METADATA_PREFIX}{}", field.key),
                        Arc::clone(&field.value),
                    );
                }
                let matches = self
                    .service
                    .query_partitions(std::slice::from_ref(&query))
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if matches.is_empty() {
                    break;
                }
                let returned = matches.len();
                let final_offset = matches
                    .last()
                    .expect("non-empty page")
                    .record
                    .record_ref
                    .offset
                    .get();
                let rows = matches
                    .into_iter()
                    .map(|matched| analytics_row_and_entry(&request.tenant, matched))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter_map(|(row, entry)| (!delete_filter.matches(&entry)).then_some(row))
                    .collect::<Vec<_>>();
                if !rows.is_empty() {
                    emit(&rows)?;
                }
                emitted = emitted.saturating_add(rows.len());
                if emitted == limit || returned < page_limit {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        Ok(())
    }

    fn health(&self) -> Result<StoreHealth, LokiApiError> {
        let stats = self.engine.durable_sink_stats();
        if stats.dirty_partitions > 0 {
            return Ok(StoreHealth {
                ready: false,
                detail: Arc::from(format!(
                    "{} durable sink partitions require recovery",
                    stats.dirty_partitions
                )),
            });
        }
        let maximum_age = self
            .indexed_ack_timeout
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if stats.pending_items > 0 && stats.checkpoint_age_ms > maximum_age {
            return Ok(StoreHealth {
                ready: false,
                detail: Arc::from(format!(
                    "oldest pending index checkpoint is {} ms old",
                    stats.checkpoint_age_ms
                )),
            });
        }
        Ok(StoreHealth::default())
    }

    fn flush(&self, timeout: Duration) -> Result<(), LokiApiError> {
        self.engine.sync().map_err(engine_error)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| LokiApiError::internal("flush deadline overflow"))?;
        loop {
            let stats = self.engine.durable_sink_stats();
            if stats.dirty_partitions > 0 {
                return Err(LokiApiError::internal(format!(
                    "flush stopped with {} dirty partitions",
                    stats.dirty_partitions
                )));
            }
            if stats.pending_items == 0 && stats.pending_bytes == 0 {
                self.engine.sync().map_err(engine_error)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LokiApiError::unavailable(format!(
                    "flush timed out with {} pending items and {} pending bytes",
                    stats.pending_items, stats.pending_bytes
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn operational_metrics(&self) -> StoreMetrics {
        let stats = self.engine.durable_sink_stats();
        StoreMetrics {
            pending_items: stats.pending_items,
            pending_bytes: stats.pending_bytes,
            checkpoint_age_ms: stats.checkpoint_age_ms,
            applied_appends: stats.applied_appends,
            retry_attempts: stats.retry_attempts,
            failed_attempts: stats.failed_attempts,
            dirty_partitions: stats.dirty_partitions,
            retained_payload_bytes: None,
            retention_runs: self.retention_runs.load(Ordering::Relaxed),
            retention_advanced_offsets: self.retention_advanced_offsets.load(Ordering::Relaxed),
            retention_failures: self.retention_failures.load(Ordering::Relaxed),
        }
    }

    fn create_delete(
        &self,
        tenant: &str,
        start_time: i64,
        end_time: i64,
        query: String,
        created_at: i64,
    ) -> Result<String, LokiApiError> {
        self.deletes
            .create(tenant, start_time, end_time, query, created_at)
    }

    fn delete_requests(&self, tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        self.deletes.list(tenant)
    }

    fn cancel_delete(&self, tenant: &str, request_id: &str) -> Result<bool, LokiApiError> {
        self.deletes.cancel(tenant, request_id)
    }
}

fn analytics_row_and_entry(
    tenant: &Arc<str>,
    matched: LogMatch,
) -> Result<(AnalyticsLogRow, LokiEntry), LokiApiError> {
    let mut labels = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    for field in matched.record.fields.iter() {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name.to_owned(), field.value.to_string());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            metadata.insert(name.to_owned(), field.value.to_string());
        }
    }
    let timestamp_unix_nanos = i64::try_from(matched.record.timestamp_unix_nanos)
        .map_err(|_| LokiApiError::internal("timestamp exceeds ClickHouse i64 range"))?;
    let entry = LokiEntry {
        timestamp_unix_nanos,
        labels: labels.clone(),
        line: matched.record.message.to_string(),
        structured_metadata: metadata.clone(),
    };
    Ok((
        AnalyticsLogRow {
            tenant: Arc::clone(tenant),
            timestamp_unix_nanos,
            partition: matched.record.record_ref.topic_partition.partition_id.get(),
            offset: matched.record.record_ref.offset.get(),
            message: matched.record.message,
            labels,
            metadata,
        },
        entry,
    ))
}

fn log_match_to_entry(matched: LogMatch) -> Result<LokiEntry, LokiApiError> {
    let mut labels = BTreeMap::new();
    let mut structured_metadata = BTreeMap::new();
    for field in matched.record.fields.iter() {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name.to_owned(), field.value.to_string());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            structured_metadata.insert(name.to_owned(), field.value.to_string());
        }
    }
    Ok(LokiEntry {
        timestamp_unix_nanos: i64::try_from(matched.record.timestamp_unix_nanos)
            .map_err(|_| LokiApiError::internal("timestamp exceeds Loki i64 range"))?,
        labels,
        line: matched.record.message.to_string(),
        structured_metadata,
    })
}

fn engine_error(error: EngineError) -> LokiApiError {
    match error {
        EngineError::InvalidConfig(message) => LokiApiError::bad_request(message),
        error => LokiApiError::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use shard_stream_protocol::{FetchMode, FetchRequest};

    use super::*;
    use crate::ingest_pack::{decode_ingest_pack, is_ingest_pack, validate_ingest_pack};

    #[test]
    fn durable_store_acknowledges_and_queries_the_same_stripe_owned_record() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-log-loki-store-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableLokiStore::open(DurableLokiConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        for timestamp in 100..103 {
            store
                .push(
                    "tenant-a",
                    vec![LokiEntry {
                        timestamp_unix_nanos: timestamp,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: format!("durable message {timestamp}"),
                        structured_metadata: BTreeMap::from([(
                            "trace_id".to_owned(),
                            format!("abc-{timestamp}"),
                        )]),
                    }],
                )
                .expect("push is durable");
        }
        let entries = store.entries("tenant-a").expect("query");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].line, "durable message 100");
        assert_eq!(entries[0].labels["app"], "api");
        assert_eq!(entries[0].structured_metadata["trace_id"], "abc-100");
        drop(store);
        let recovered = DurableLokiStore::open(DurableLokiConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store recovers");
        let entries = recovered.entries("tenant-a").expect("recovered query");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].line, "durable message 102");
        assert!(!directory.join("index-journal").exists());
        drop(recovered);
        fs::remove_dir_all(directory).expect("remove test store");
    }

    #[test]
    fn logical_deletes_survive_restart_and_filter_native_and_analytical_reads() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-log-delete-store-{}-{nonce}",
            std::process::id()
        ));
        let config = DurableLokiConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let store = DurableLokiStore::open(config.clone()).expect("store");
        store
            .push(
                "tenant-a",
                (100..103)
                    .map(|timestamp| LokiEntry {
                        timestamp_unix_nanos: timestamp,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: format!("durable message {timestamp}"),
                        structured_metadata: BTreeMap::new(),
                    })
                    .collect(),
            )
            .expect("push");
        let request_id = store
            .create_delete(
                "tenant-a",
                1,
                200,
                "{app=\"api\"} |= \"101\"".to_owned(),
                1_000,
            )
            .expect("create delete");
        assert_eq!(request_id, "0000000000000001");
        assert_eq!(
            store
                .entries("tenant-a")
                .expect("Loki entries")
                .into_iter()
                .map(|entry| entry.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![100, 102]
        );

        let native = store
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                terms: vec!["message".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("native query");
        assert_eq!(native.len(), 2);
        assert!(native.iter().all(|entry| !entry.line.ends_with("101")));

        let mut rows = Vec::new();
        store
            .scan_analytics(&AnalyticsScanRequest::new("tenant-a"), &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("analytics scan");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| !row.message.ends_with("101")));
        drop(store);

        let recovered = DurableLokiStore::open(config).expect("recovered store");
        assert_eq!(recovered.delete_requests("tenant-a").unwrap().len(), 1);
        assert_eq!(recovered.entries("tenant-a").unwrap().len(), 2);
        assert!(recovered.cancel_delete("tenant-a", &request_id).unwrap());
        assert_eq!(recovered.entries("tenant-a").unwrap().len(), 3);
        drop(recovered);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn retention_cutoff_is_enforced_by_loki_native_and_analytical_reads() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-log-retention-store-{}-{nonce}",
            std::process::id()
        ));
        let now = i64::try_from(nonce).expect("current timestamp fits i64");
        let config = DurableLokiConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            recovery_journal: true,
            retention: Some(Duration::from_secs(60)),
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let store = DurableLokiStore::open(config.clone()).expect("store");
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: now - 120_000_000_000,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "expired message".to_owned(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push expired batch");
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: now,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "retained message".to_owned(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push retained batch");
        let entries = store.entries("tenant-a").expect("Loki query");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line, "retained message");

        let native = store
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::new(),
                terms: vec!["message".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("native query");
        assert_eq!(native.len(), 1);
        assert_eq!(native[0].line, "retained message");

        let mut rows = Vec::new();
        store
            .scan_analytics(&AnalyticsScanRequest::new("tenant-a"), &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("analytics scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message.as_ref(), "retained message");
        let report = store.compact_retention().expect("retention compaction");
        assert_eq!(report.advanced_partitions, 1);
        assert_eq!(report.advanced_offsets, 1);
        assert_eq!(store.operational_metrics().retention_runs, 1);
        drop(store);
        let recovered = DurableLokiStore::open(config).expect("restart after compaction");
        assert_eq!(recovered.entries("tenant-a").unwrap().len(), 1);
        drop(recovered);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn standalone_store_makes_the_compressed_ingest_pack_authoritative() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-log-ingest-pack-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableLokiStore::open(DurableLokiConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        let entry = LokiEntry {
            timestamp_unix_nanos: 123,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "request completed".to_owned(),
            structured_metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
        };
        let native = encode_native_log_batch("tenant-a", vec![entry]).expect("native batch");
        let acknowledgement = store
            .append_native_batch_durable(native)
            .expect("durable native append succeeds");
        let batches = store
            .engine
            .fetch(FetchRequest {
                request_id: 1,
                topic_id: LOKI_TOPIC_ID,
                partition_id: LogicalPartitionId::new(acknowledgement.partition_id),
                start_offset: LogicalOffset::new(acknowledgement.first_offset),
                max_bytes: 1024 * 1024,
                mode: FetchMode::Ordered,
            })
            .expect("authoritative batch fetches");
        assert_eq!(batches.len(), 1);
        assert!(is_ingest_pack(&batches[0].payload));
        assert!(!crate::is_native_log_batch(&batches[0].payload));
        validate_ingest_pack(&batches[0].payload, 1).expect("stored pack validates");
        let decoded = decode_ingest_pack(&batches[0].payload).expect("stored pack decodes");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].timestamp_unix_nanos, 123);
        assert_eq!(decoded[0].message.as_ref(), "request completed");
        assert!(decoded[0].fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() == "tenant-a"
        }));
        drop(store);
        fs::remove_dir_all(directory).expect("remove test store");
    }

    #[test]
    fn durable_analytics_scan_pushes_indexable_constraints_into_stripes() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-log-analytics-store-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableLokiStore::open(DurableLokiConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        store
            .push(
                "tenant-a",
                vec![
                    LokiEntry {
                        timestamp_unix_nanos: 100,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: "request completed".to_owned(),
                        structured_metadata: BTreeMap::from([(
                            "code".to_owned(),
                            "200".to_owned(),
                        )]),
                    },
                    LokiEntry {
                        timestamp_unix_nanos: 200,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: "request ERROR".to_owned(),
                        structured_metadata: BTreeMap::from([(
                            "code".to_owned(),
                            "500".to_owned(),
                        )]),
                    },
                ],
            )
            .expect("push");
        let mut request = AnalyticsScanRequest::new("tenant-a");
        request.start_timestamp_unix_nanos = Some(150);
        request.end_timestamp_unix_nanos = Some(250);
        request.terms.push(Arc::from("error"));
        request.labels.push(crate::MetadataField::new("app", "api"));
        request
            .metadata
            .push(crate::MetadataField::new("code", "500"));
        let mut rows = Vec::new();
        store
            .scan_analytics(&request, &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp_unix_nanos, 200);
        assert_eq!(rows[0].message.as_ref(), "request ERROR");
        assert_eq!(rows[0].labels["app"], "api");
        assert_eq!(rows[0].metadata["code"], "500");
        drop(store);
        fs::remove_dir_all(directory).expect("remove test store");
    }
}
