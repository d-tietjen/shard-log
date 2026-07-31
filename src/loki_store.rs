use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, PlacementSequence, TopicId, TopicPartition,
};
use shard_stream_engine::{
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkOptions, EngineConfig, EngineError,
    StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability};

use crate::ingest_pack::prepare_native_ingest_pack;
use crate::loki_api::LokiApiError;
use crate::{
    AnalyticsLogRow, AnalyticsScanRequest, LogMatch, LogQuery, LokiEntry, LokiStore,
    NativeAppendAck, NativeQuery, NativeQueryDirection, OtlpSinkConfig, ShardLogService,
    ShardLogSinkFactory, StripeConfig, encode_native_log_batch,
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
        Ok(())
    }
}

/// Loki protocol backend whose acknowledged writes are durable shard-stream
/// appends and whose reads execute on the owning ShardLog stripe workers.
pub struct DurableLokiStore {
    engine: StreamEngine,
    service: ShardLogService,
    tenant_partitions: u32,
    ingest_stripes_per_tenant: u32,
    indexed_ack_timeout: Duration,
    next_request_id: AtomicU64,
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
        let engine = StreamEngine::open_with_durable_sink(
            engine_config,
            DurableSinkConfig::new(factory).with_options(sink_options),
        )
        .map_err(engine_error)?;
        match engine.create_topic(TopicConfig {
            topic_id: LOKI_TOPIC_ID,
            partitions: config.tenant_partitions,
            shards: None,
        }) {
            Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
            Err(error) => return Err(engine_error(error)),
        }
        Ok(Self {
            engine,
            service,
            tenant_partitions: config.tenant_partitions,
            ingest_stripes_per_tenant: config.shard_count.min(config.tenant_partitions),
            indexed_ack_timeout: config.indexed_ack_timeout,
            next_request_id: AtomicU64::new(1),
        })
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
        let queries = self
            .tenant_partitions(&request.tenant)
            .map(|partition| {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(request.limit as usize)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos = request.start_timestamp_unix_nanos;
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
        let matches = self
            .service
            .query_partitions(&queries)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let mut entries = matches
            .into_iter()
            .map(log_match_to_entry)
            .collect::<Result<Vec<_>, _>>()?;
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
                query.start_timestamp_unix_nanos = request.start_timestamp_unix_nanos;
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
                    .map(|matched| analytics_row(&request.tenant, matched))
                    .collect::<Result<Vec<_>, _>>()?;
                emit(&rows)?;
                emitted = emitted.saturating_add(returned);
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
}

fn analytics_row(tenant: &Arc<str>, matched: LogMatch) -> Result<AnalyticsLogRow, LokiApiError> {
    let mut labels = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    for field in matched.record.fields.iter() {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name.to_owned(), field.value.to_string());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            metadata.insert(name.to_owned(), field.value.to_string());
        }
    }
    Ok(AnalyticsLogRow {
        tenant: Arc::clone(tenant),
        timestamp_unix_nanos: i64::try_from(matched.record.timestamp_unix_nanos)
            .map_err(|_| LokiApiError::internal("timestamp exceeds ClickHouse i64 range"))?,
        partition: matched.record.record_ref.topic_partition.partition_id.get(),
        offset: matched.record.record_ref.offset.get(),
        message: matched.record.message,
        labels,
        metadata,
    })
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
