use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use shard_stream_core::{ShardId, TopicPartition};
use shard_stream_engine::{
    DurableAppend, DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply,
    DurableSinkCheckpoint, EngineError, EngineResult,
};

use crate::ingest_pack::validate_ingest_pack;
use crate::sink_journal::{SinkJournal, checkpoint_allows_lane_gap};
use crate::{
    DictionaryCatalog, DurableMetricPoint, DurableSpan, LogMatch, LogQuery, LogStripe,
    MetricIngestProtocol, MetricQuery, MetricStripe, ObjectTierConfig, RealtimeDictionaryObserver,
    RealtimeDictionaryTrainer, ShardTelemetryConfig, SharedTelemetryObjectStore, SsdCacheConfig,
    SsdObjectCache, StripeConfig, TelemetryEnvelope, TelemetryError, TelemetryResult,
    TelemetrySignal, TraceQuery, TraceStripe, decode_metric_chunk, decode_trace_block,
};

/// Immutable object-tier and bounded SSD-cache settings shared by sink stripes.
#[derive(Debug, Clone)]
pub struct SinkObjectTierConfig {
    /// Object-store adapter used for immutable data and catalog publication.
    pub store: SharedTelemetryObjectStore,
    /// Local crash-safe staging directory for artifacts being published.
    pub spool_directory: PathBuf,
    /// Local SSD range-cache directory.
    pub cache_directory: PathBuf,
    /// Logical partitions whose catalogs must be opened without object listing.
    pub partitions: Vec<TopicPartition>,
    /// Immutable group and catalog bounds.
    pub tier: ObjectTierConfig,
    /// Recoverable SSD range-cache bounds.
    pub cache: SsdCacheConfig,
}

/// Configuration for shard-telemetry's per-shard native and OTLP index sinks.
#[derive(Debug, Clone)]
pub struct OtlpSinkConfig {
    /// Hot index and dictionary-cache limits for each physical shard.
    pub stripe: StripeConfig,
    /// Per-signal partition, retention, head-memory, and query limits.
    pub signals: ShardTelemetryConfig,
    /// Number of durable append batches waiting to be indexed per shard.
    pub queue_slots: usize,
    /// Directory containing crash-safe stripe-local sink journals.
    ///
    /// `None` is intended only for tests and embedded ephemeral operation.
    pub state_directory: Option<PathBuf>,
    /// Maximum bytes retained in each physical stripe's sink journal.
    pub max_journal_bytes: u64,
    /// Optional immutable object tier for bounded recovery and cold queries.
    pub object_tier: Option<SinkObjectTierConfig>,
}

impl Default for OtlpSinkConfig {
    fn default() -> Self {
        Self {
            stripe: StripeConfig::default(),
            signals: ShardTelemetryConfig::default(),
            queue_slots: 256,
            state_directory: None,
            max_journal_bytes: 64 * 1024 * 1024 * 1024,
            object_tier: None,
        }
    }
}

impl OtlpSinkConfig {
    fn validate(&self) -> TelemetryResult<()> {
        self.signals.validate()?;
        if self.queue_slots == 0 {
            return Err(TelemetryError::InvalidConfig(
                "log sink queue_slots must be nonzero",
            ));
        }
        if self.max_journal_bytes < 8 {
            return Err(TelemetryError::InvalidConfig(
                "log sink max_journal_bytes must fit its header",
            ));
        }
        if self.object_tier.as_ref().is_some_and(|tier| {
            tier.partitions.is_empty() || tier.partitions.windows(2).any(|pair| pair[0] >= pair[1])
        }) {
            return Err(TelemetryError::InvalidConfig(
                "object-tier partitions must be nonempty, sorted, and unique",
            ));
        }
        Ok(())
    }
}

fn checkpoint_covers(checkpoint: DurableSinkCheckpoint, candidate: DurableSinkCheckpoint) -> bool {
    checkpoint.topic_partition == candidate.topic_partition
        && checkpoint.next_placement_sequence >= candidate.next_placement_sequence
        && checkpoint.next_offset >= candidate.next_offset
}

fn merge_recovered_checkpoint(
    checkpoints: &mut HashMap<TopicPartition, DurableSinkCheckpoint>,
    candidate: DurableSinkCheckpoint,
) -> TelemetryResult<()> {
    match checkpoints.get(&candidate.topic_partition).copied() {
        None => {
            checkpoints.insert(candidate.topic_partition, candidate);
        }
        Some(current) if checkpoint_covers(current, candidate) => {}
        Some(current) if checkpoint_covers(candidate, current) => {
            checkpoints.insert(candidate.topic_partition, candidate);
        }
        Some(_) => {
            return Err(TelemetryError::CorruptTier(
                "object-tier checkpoints are not monotonically comparable".into(),
            ));
        }
    }
    Ok(())
}

/// Builds one owner-only log index worker for every shard-stream physical shard.
///
/// The factory validates each grouped native batch or OTLP protobuf before
/// shard-stream reserves an offset range. Once the primary append is durable,
/// shard-stream delivers the batch to the matching sink, whose dedicated
/// worker owns the mutable [`LogStripe`] without a shared-map lock on the
/// indexing path.
#[derive(Debug)]
pub struct TelemetrySinkFactory {
    config: OtlpSinkConfig,
    available: Mutex<HashMap<ShardId, TelemetryStripeState>>,
    checkpoints: Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    journals: Mutex<HashMap<ShardId, Arc<SinkJournal>>>,
    query_workers: Arc<Mutex<HashMap<ShardId, SyncSender<SinkCommand>>>>,
}

#[derive(Debug)]
struct TelemetryStripeState {
    stream_shard_id: ShardId,
    logs: LogStripe,
    traces: TraceStripe,
    metrics: MetricStripe,
}

impl TelemetrySinkFactory {
    /// Creates a factory with one stripe available for each physical shard.
    pub fn new(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(shard_ids, config, None, None)
    }

    /// Creates a sink factory whose stripe workers adopt immutable dictionary
    /// publications once per durable append batch.
    pub fn with_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(
            shard_ids,
            config,
            Some(dictionary_catalog),
            None,
        )
    }

    /// Creates sink workers that continuously sample sealed blocks and adopt
    /// admitted immutable dictionary generations at durable append boundaries.
    pub fn with_realtime_dictionary(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(
            shard_ids,
            config,
            Some(trainer.catalog()),
            Some(trainer.observer()),
        )
    }

    fn new_with_optional_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: OtlpSinkConfig,
        dictionary_catalog: Option<Arc<DictionaryCatalog>>,
        realtime_dictionary: Option<RealtimeDictionaryObserver>,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        let mut available = HashMap::new();
        let mut recovered_checkpoints = HashMap::new();
        let mut recovered_transactions = Vec::new();
        let mut journals = HashMap::new();
        let tier_cache = config
            .object_tier
            .as_ref()
            .map(|tier| SsdObjectCache::open(&tier.cache_directory, tier.cache).map(Arc::new))
            .transpose()?;
        for shard_id in shard_ids {
            let mut logs = match &dictionary_catalog {
                Some(dictionary_catalog) => LogStripe::with_dictionary_catalog(
                    shard_id,
                    config.stripe.clone(),
                    Arc::clone(dictionary_catalog),
                )?,
                None => LogStripe::new(shard_id, config.stripe.clone())?,
            };
            if let Some(observer) = &realtime_dictionary {
                logs.attach_realtime_dictionary(observer.clone());
            }
            if let (Some(tier), Some(cache)) = (&config.object_tier, &tier_cache) {
                for checkpoint in logs.attach_object_tier(
                    tier.store.clone(),
                    tier.spool_directory.clone(),
                    Arc::clone(cache),
                    tier.partitions.iter().copied(),
                    tier.tier,
                )? {
                    merge_recovered_checkpoint(&mut recovered_checkpoints, checkpoint)?;
                }
            }
            if let Some(directory) = &config.state_directory {
                let (journal, recovered) =
                    SinkJournal::open(directory, shard_id, config.max_journal_bytes)?;
                recovered_transactions.extend(
                    recovered
                        .into_iter()
                        .map(|transaction| (shard_id, transaction)),
                );
                journals.insert(shard_id, Arc::new(journal));
            }
            let stripe = TelemetryStripeState {
                stream_shard_id: shard_id,
                logs,
                traces: TraceStripe::new(config.signals.traces.head_memory_bytes_per_stripe)?,
                metrics: MetricStripe::new(config.signals.metrics.head_memory_bytes_per_stripe)?,
            };
            if available.insert(shard_id, stripe).is_some() {
                return Err(TelemetryError::DuplicateStripe(shard_id));
            }
        }
        if available.is_empty() {
            return Err(TelemetryError::InvalidConfig(
                "log sink requires at least one shard",
            ));
        }
        recovered_transactions.sort_unstable_by_key(|(_, transaction)| {
            (
                transaction.expected.topic_partition.topic_id,
                transaction.expected.topic_partition.partition_id,
                transaction.expected.next_placement_sequence,
            )
        });
        for (shard_id, transaction) in recovered_transactions {
            let actual = recovered_checkpoints
                .get(&transaction.expected.topic_partition)
                .copied()
                .unwrap_or_else(|| {
                    DurableSinkCheckpoint::initial(transaction.expected.topic_partition)
                });
            if checkpoint_covers(actual, transaction.next) {
                continue;
            }
            if !checkpoint_allows_lane_gap(actual, transaction.expected) {
                return Err(TelemetryError::CorruptSinkJournal(
                    "recovered checkpoint chain conflicts across stripes".into(),
                ));
            }
            let stripe = available
                .get_mut(&shard_id)
                .ok_or(TelemetryError::UnknownStripe(shard_id))?;
            for append in &transaction.appends {
                index_payload(
                    stripe,
                    append.topic_partition,
                    append.first_offset,
                    None,
                    &append.payload,
                    None,
                    (transaction.expected, transaction.next),
                )?;
            }
            recovered_checkpoints.insert(transaction.next.topic_partition, transaction.next);
        }
        Ok(Self {
            config,
            available: Mutex::new(available),
            checkpoints: Arc::new(Mutex::new(recovered_checkpoints)),
            journals: Mutex::new(journals),
            query_workers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Returns a cloneable coordinator for querying the owner-only stripe
    /// workers after they have been opened by shard-stream.
    #[must_use]
    pub fn service(&self) -> TelemetryService {
        TelemetryService {
            workers: Arc::clone(&self.query_workers),
        }
    }
}

impl DurableAppendSinkFactory for TelemetrySinkFactory {
    fn validate_append(&self, payload: &[u8], record_count: NonZeroU32) -> EngineResult<()> {
        if !TelemetryEnvelope::is_encoded(payload) {
            return Err(EngineError::InvalidConfig(
                "durable telemetry appends require the STEL envelope".into(),
            ));
        }
        let envelope = TelemetryEnvelope::decode(payload).map_err(log_error_to_engine)?;
        if envelope.item_count != record_count.get() {
            return Err(EngineError::InvalidConfig(format!(
                "STEL envelope contains {} items, request reserved {}",
                envelope.item_count,
                record_count.get()
            )));
        }
        let decoded_count = match envelope.signal {
            TelemetrySignal::Logs => {
                validate_ingest_pack(&envelope.payload, envelope.item_count)
                    .map_err(log_error_to_engine)?;
                envelope.item_count as usize
            }
            TelemetrySignal::Traces => decode_trace_block(&envelope.payload)
                .map_err(log_error_to_engine)?
                .len(),
            TelemetrySignal::Metrics => decode_metric_chunk(&envelope.payload)
                .map_err(log_error_to_engine)?
                .len(),
        };
        if decoded_count != envelope.item_count as usize {
            return Err(EngineError::InvalidConfig(
                "STEL signal payload item count mismatch".into(),
            ));
        }
        Ok(())
    }

    fn load_checkpoint(
        &self,
        topic_partition: TopicPartition,
    ) -> EngineResult<Option<DurableSinkCheckpoint>> {
        self.checkpoints
            .lock()
            .map(|checkpoints| checkpoints.get(&topic_partition).copied())
            .map_err(|_| {
                EngineError::DurableSinkUnavailable(
                    "shard-telemetry checkpoint lock poisoned".into(),
                )
            })
    }

    fn open_shard(&self, shard_id: ShardId) -> EngineResult<Arc<dyn DurableAppendSink>> {
        let stripe = self
            .available
            .lock()
            .map_err(|_| {
                EngineError::CorruptState("shard-telemetry sink factory lock poisoned".into())
            })?
            .remove(&shard_id)
            .ok_or(EngineError::UnknownShard(shard_id))?;
        let (sender, receiver) = sync_channel(self.config.queue_slots);
        let checkpoints = Arc::clone(&self.checkpoints);
        let journal = self
            .journals
            .lock()
            .map_err(|_| EngineError::CorruptState("shard-telemetry journal lock poisoned".into()))?
            .remove(&shard_id);
        let worker = thread::Builder::new()
            .name(format!("shard-telemetry-index-{shard_id}"))
            .spawn(move || run_sink_worker(stripe, checkpoints, journal, receiver))
            .map_err(|error| {
                EngineError::InvalidConfig(format!("failed to spawn shard-telemetry sink: {error}"))
            })?;
        self.query_workers
            .lock()
            .map_err(|_| {
                EngineError::CorruptState("shard-telemetry query registry poisoned".into())
            })?
            .insert(shard_id, sender.clone());
        Ok(Arc::new(ShardTelemetryStripeSink {
            state: Arc::new(SinkState {
                shard_id,
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(worker)),
                query_workers: Arc::clone(&self.query_workers),
            }),
        }))
    }
}

struct SinkApplyCommand {
    expected: DurableSinkCheckpoint,
    appends: Vec<DurableAppend>,
    next: DurableSinkCheckpoint,
    response: SyncSender<EngineResult<DurableSinkApply>>,
}

enum SinkCommand {
    Apply(SinkApplyCommand),
    Query {
        queries: Vec<LogQuery>,
        response: SyncSender<TelemetryResult<Vec<LogMatch>>>,
    },
    QueryTraces {
        query: TraceQuery,
        response: SyncSender<TelemetryResult<Vec<DurableSpan>>>,
    },
    QueryMetrics {
        query: MetricQuery,
        response: SyncSender<TelemetryResult<Vec<DurableMetricPoint>>>,
    },
    Flush {
        response: SyncSender<TelemetryResult<usize>>,
    },
    RetainedPayloadBytes {
        response: SyncSender<u64>,
    },
}

struct SinkState {
    shard_id: ShardId,
    sender: Mutex<Option<SyncSender<SinkCommand>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    query_workers: Arc<Mutex<HashMap<ShardId, SyncSender<SinkCommand>>>>,
}

impl Drop for SinkState {
    fn drop(&mut self) {
        if let Ok(mut workers) = self.query_workers.lock() {
            workers.remove(&self.shard_id);
        }
        if let Ok(sender) = self.sender.get_mut() {
            sender.take();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

#[derive(Clone)]
struct ShardTelemetryStripeSink {
    state: Arc<SinkState>,
}

impl fmt::Debug for ShardTelemetryStripeSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardTelemetryStripeSink")
            .field("shard_id", &self.state.shard_id)
            .finish_non_exhaustive()
    }
}

impl DurableAppendSink for ShardTelemetryStripeSink {
    fn apply(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        let (response, receiver) = sync_channel(1);
        let sender = self
            .state
            .sender
            .lock()
            .map_err(|_| EngineError::CorruptState("shard-telemetry sink lock poisoned".into()))?
            .as_ref()
            .cloned()
            .ok_or(EngineError::WorkerStopped(self.state.shard_id))?;
        sender
            .send(SinkCommand::Apply(SinkApplyCommand {
                expected,
                appends: appends.to_vec(),
                next,
                response,
            }))
            .map_err(|_| EngineError::WorkerStopped(self.state.shard_id))?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.state.shard_id))?
    }
}

fn run_sink_worker(
    mut stripe: TelemetryStripeState,
    checkpoints: Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    journal: Option<Arc<SinkJournal>>,
    receiver: Receiver<SinkCommand>,
) {
    let mut apply_failure_reported = false;
    while let Ok(command) = receiver.recv() {
        match command {
            SinkCommand::Apply(command) => {
                let result = apply_durable_appends(
                    &mut stripe,
                    &checkpoints,
                    journal.as_deref(),
                    command.expected,
                    &command.appends,
                    command.next,
                );
                if let Err(error) = &result {
                    if !apply_failure_reported {
                        eprintln!(
                            "shard-telemetry stripe {} durable apply failed and will be retried: {error}",
                            stripe.stream_shard_id
                        );
                    }
                    apply_failure_reported = true;
                } else {
                    apply_failure_reported = false;
                }
                let _ = command.response.send(result);
            }
            SinkCommand::Query { queries, response } => {
                let result = stripe.logs.query_partitions_checked(&queries);
                let _ = response.send(result);
            }
            SinkCommand::QueryTraces { query, response } => {
                let _ = response.send(stripe.traces.query(&query));
            }
            SinkCommand::QueryMetrics { query, response } => {
                let _ = response.send(stripe.metrics.query(&query));
            }
            SinkCommand::Flush { response } => {
                let result = stripe.logs.offload_indexed_groups(true);
                let _ = response.send(result);
            }
            SinkCommand::RetainedPayloadBytes { response } => {
                let _ = response.send(stripe.logs.retained_payload_bytes());
            }
        }
    }
}

/// Cloneable read service for the stripes owned by durable sink workers.
///
/// Queries are sent to every active owner thread first, allowing the bounded
/// stripe lookups to execute in parallel, and are then merged in the same
/// deterministic order used by [`crate::ShardTelemetry`].
#[derive(Debug, Clone)]
pub struct TelemetryService {
    workers: Arc<Mutex<HashMap<ShardId, SyncSender<SinkCommand>>>>,
}

impl TelemetryService {
    /// Fans a partition-local query across all active physical stripes.
    pub fn query_all(&self, query: &LogQuery) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partitions(std::slice::from_ref(query))
    }

    /// Fans a native trace query across all owner stripes and merges by trace/start/offset.
    pub fn query_traces(&self, query: &TraceQuery) -> TelemetryResult<Vec<DurableSpan>> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryTraces {
                    query: query.clone(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a trace query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut spans = Vec::new();
        for (shard_id, receiver) in responses {
            spans.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying traces"
                ))
            })??);
        }
        spans.sort_unstable_by_key(|span| {
            (
                span.trace_id,
                span.start_time_unix_nanos,
                span.record_ref.offset,
            )
        });
        spans.truncate(query.limit.max(1));
        Ok(spans)
    }

    /// Fans a native raw metric query across all owner stripes and merges by time/offset.
    pub fn query_metrics(&self, query: &MetricQuery) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryMetrics {
                    query: query.clone(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a metric query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut points = Vec::new();
        for (shard_id, receiver) in responses {
            points.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying metrics"
                ))
            })??);
        }
        points.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
        points.truncate(query.limit.max(1));
        Ok(points)
    }

    /// Forces every owner stripe to publish complete pending append boundaries.
    pub fn flush_object_tier(&self) -> TelemetryResult<usize> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender.send(SinkCommand::Flush { response }).map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped before accepting a flush"
                ))
            })?;
            responses.push((shard_id, receiver));
        }
        responses
            .into_iter()
            .try_fold(0usize, |total, (shard_id, receiver)| {
                let published = receiver.recv().map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped while flushing"
                    ))
                })??;
                Ok(total.saturating_add(published))
            })
    }

    /// Returns compressed bytes still resident while awaiting a complete group.
    pub fn retained_payload_bytes(&self) -> TelemetryResult<u64> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::RetainedPayloadBytes { response })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before reporting resident bytes"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        responses
            .into_iter()
            .try_fold(0u64, |total, (shard_id, receiver)| {
                let bytes = receiver.recv().map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped while reporting resident bytes"
                    ))
                })?;
                Ok(total.saturating_add(bytes))
            })
    }

    fn worker_senders(&self) -> TelemetryResult<Vec<(ShardId, SyncSender<SinkCommand>)>> {
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable("query registry lock is poisoned".into())
            })?
            .iter()
            .map(|(shard_id, sender)| (*shard_id, sender.clone()))
            .collect::<Vec<_>>();
        workers.sort_unstable_by_key(|(shard_id, _)| *shard_id);
        if workers.is_empty() {
            return Err(TelemetryError::QueryWorkerUnavailable(
                "no stripe workers are active".into(),
            ));
        }
        Ok(workers)
    }

    pub(crate) fn query_partitions(&self, queries: &[LogQuery]) -> TelemetryResult<Vec<LogMatch>> {
        let Some(ordering_query) = queries.first() else {
            return Ok(Vec::new());
        };
        let workers = self.worker_senders()?;

        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::Query {
                    queries: queries.to_vec(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }

        let mut matches = Vec::new();
        for (shard_id, receiver) in responses {
            matches.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while executing a query"
                ))
            })??);
        }
        matches.sort_unstable_by(|left, right| {
            ordering_query
                .compare(&left.record, &right.record)
                .then_with(|| {
                    left.record
                        .stream_shard_id
                        .cmp(&right.record.stream_shard_id)
                })
        });
        if let Some(limit) = ordering_query.limit {
            matches.truncate(limit);
        }
        Ok(matches)
    }
}

fn apply_durable_appends(
    stripe: &mut TelemetryStripeState,
    checkpoints: &Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>,
    journal: Option<&SinkJournal>,
    expected: DurableSinkCheckpoint,
    appends: &[DurableAppend],
    next: DurableSinkCheckpoint,
) -> EngineResult<DurableSinkApply> {
    if expected.topic_partition != next.topic_partition {
        return Err(EngineError::DurableSinkCheckpoint(
            "expected and next checkpoints refer to different partitions".into(),
        ));
    }
    if appends
        .iter()
        .any(|append| append.topic_partition() != expected.topic_partition)
    {
        return Err(EngineError::DurableSinkCheckpoint(
            "sink transaction contains appends from another partition".into(),
        ));
    }

    let actual = checkpoints
        .lock()
        .map_err(|_| {
            EngineError::DurableSinkUnavailable("shard-telemetry checkpoint lock poisoned".into())
        })?
        .get(&expected.topic_partition)
        .copied()
        .unwrap_or_else(|| DurableSinkCheckpoint::initial(expected.topic_partition));
    if !checkpoint_allows_lane_gap(actual, expected) {
        return Ok(DurableSinkApply::CheckpointConflict(actual));
    }

    if let Some(journal) = journal {
        journal
            .append(expected, appends, next)
            .map_err(log_error_to_engine)?;
    }
    index_durable_appends(stripe, appends, expected, next).map_err(log_error_to_engine)?;
    stripe
        .logs
        .offload_indexed_groups(false)
        .map_err(log_error_to_engine)?;
    checkpoints
        .lock()
        .map_err(|_| {
            EngineError::DurableSinkUnavailable("shard-telemetry checkpoint lock poisoned".into())
        })?
        .insert(next.topic_partition, next);
    Ok(DurableSinkApply::Applied)
}

fn index_durable_appends(
    stripe: &mut TelemetryStripeState,
    appends: &[DurableAppend],
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
) -> TelemetryResult<()> {
    for append in appends {
        if append.physical_shard_id != stripe.stream_shard_id {
            return Err(TelemetryError::WrongStripe {
                expected: stripe.stream_shard_id,
                observed: append.physical_shard_id,
            });
        }
        let topic_partition =
            TopicPartition::new(append.reservation.topic_id, append.reservation.partition_id);
        index_payload(
            stripe,
            topic_partition,
            append.reservation.first_offset,
            Some(append.reservation.record_count.get()),
            &append.payload,
            append.transient_context.as_deref(),
            (expected, next),
        )?;
    }
    Ok(())
}

fn index_payload(
    stripe: &mut TelemetryStripeState,
    topic_partition: TopicPartition,
    first_offset: shard_stream_core::LogicalOffset,
    expected_count: Option<u32>,
    payload: &[u8],
    _transient_context: Option<&[u8]>,
    checkpoints: (DurableSinkCheckpoint, DurableSinkCheckpoint),
) -> TelemetryResult<()> {
    if !TelemetryEnvelope::is_encoded(payload) {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "durable telemetry append is not a STEL envelope",
        ));
    }
    let envelope = TelemetryEnvelope::decode(payload)?;
    if envelope.signal.topic_id() != topic_partition.topic_id {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "signal does not match its shard-stream topic",
        ));
    }
    if expected_count.is_some_and(|count| count != envelope.item_count) {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "durable reservation count disagrees with envelope",
        ));
    }
    match envelope.signal {
        TelemetrySignal::Logs => {
            validate_ingest_pack(&envelope.payload, envelope.item_count)?;
            stripe.logs.apply_checkpointed_ingest_pack(
                topic_partition,
                first_offset,
                envelope.item_count,
                Bytes::copy_from_slice(&envelope.payload),
                None,
                checkpoints,
            )?;
        }
        TelemetrySignal::Traces => {
            let records = decode_trace_block(&envelope.payload)?;
            validate_relative_offsets(
                records.iter().map(|record| record.record_ref.offset),
                envelope.item_count,
            )?;
            for mut record in records {
                record.stream_shard_id = stripe.stream_shard_id;
                record.record_ref = crate::TelemetryRecordRef::for_signal(
                    TelemetrySignal::Traces,
                    topic_partition,
                    absolute_offset(topic_partition, first_offset, record.record_ref.offset)?,
                );
                let append_time = record.end_time_unix_nanos().unwrap_or(u64::MAX);
                stripe.traces.apply(record, append_time)?;
            }
        }
        TelemetrySignal::Metrics => {
            if envelope.routing_metadata.len() != 5 {
                return Err(TelemetryError::InvalidTelemetryEnvelope(
                    "metric routing metadata must contain partition and protocol",
                ));
            }
            let routed_partition = u32::from_le_bytes(
                envelope.routing_metadata[..4]
                    .try_into()
                    .expect("fixed metric partition bytes"),
            );
            if routed_partition != topic_partition.partition_id.get() {
                return Err(TelemetryError::InvalidTelemetryEnvelope(
                    "metric routing metadata partition mismatch",
                ));
            }
            let protocol = MetricIngestProtocol::from_wire(envelope.routing_metadata[4])?;
            let records = decode_metric_chunk(&envelope.payload)?;
            validate_relative_offsets(
                records.iter().map(|record| record.record_ref.offset),
                envelope.item_count,
            )?;
            for mut record in records {
                record.stream_shard_id = stripe.stream_shard_id;
                record.record_ref = crate::TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    topic_partition,
                    absolute_offset(topic_partition, first_offset, record.record_ref.offset)?,
                );
                stripe.metrics.apply(record, protocol)?;
            }
        }
    }
    Ok(())
}

fn validate_relative_offsets(
    offsets: impl IntoIterator<Item = shard_stream_core::LogicalOffset>,
    count: u32,
) -> TelemetryResult<()> {
    let mut seen = vec![false; count as usize];
    for offset in offsets {
        let ordinal = usize::try_from(offset.get()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let slot = seen
            .get_mut(ordinal)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "signal payload offset is outside its reservation",
            ))?;
        if *slot {
            return Err(TelemetryError::InvalidBlockEncoding(
                "signal payload contains a duplicate relative offset",
            ));
        }
        *slot = true;
    }
    if seen.iter().any(|value| !*value) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "signal payload offsets are not contiguous",
        ));
    }
    Ok(())
}

fn absolute_offset(
    topic_partition: TopicPartition,
    first_offset: shard_stream_core::LogicalOffset,
    relative_offset: shard_stream_core::LogicalOffset,
) -> TelemetryResult<shard_stream_core::LogicalOffset> {
    first_offset
        .get()
        .checked_add(relative_offset.get())
        .map(shard_stream_core::LogicalOffset::new)
        .ok_or(TelemetryError::OffsetExhausted(topic_partition))
}

fn log_error_to_engine(error: TelemetryError) -> EngineError {
    EngineError::InvalidConfig(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroU32;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    };
    use prost::Message;
    use shard_stream_core::{
        BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
        RecordId, RingEpoch, TopicPartition, VirtualLaneId,
    };
    use shard_stream_engine::{
        DurableAppendDelivery, DurableSinkApply, DurableSinkCheckpoint, DurableSinkConfig,
        EngineConfig, StreamEngine, TopicConfig,
    };
    use shard_stream_protocol::{AppendRequest, Durability};

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "shard-telemetry-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn engine_config(path: &Path) -> EngineConfig {
        EngineConfig {
            data_dir: path.to_path_buf(),
            object_store_dir: None,
            shard_count: 1,
            virtual_lane_count: 1,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 2 * 1024 * 1024,
            target_pack_bytes: 1024,
            max_pack_age: std::time::Duration::from_secs(1),
            max_batch_bytes: 64 * 1024,
            max_fetch_bytes: 1024 * 1024,
            append_linger: std::time::Duration::from_millis(1),
        }
    }

    fn payload() -> Vec<u8> {
        let protobuf = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        observed_time_unix_nano: 0,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("sink message".into())),
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
        }
        .encode_to_vec();
        let events = crate::OtlpLogDecoder
            .decode(&protobuf)
            .expect("OTLP decodes");
        crate::prepare_log_envelope("tenant-a", &events)
            .expect("STEL envelope")
            .encode()
            .expect("STEL encodes")
    }

    #[test]
    fn durable_otlp_sink_commits_its_checkpoint_with_the_index_update() {
        let factory = TelemetrySinkFactory::new([ShardId::new(0)], OtlpSinkConfig::default())
            .expect("factory opens");
        let service = factory.service();
        let payload = payload();
        factory
            .validate_append(&payload, NonZeroU32::new(1).expect("one"))
            .expect("payload validates");
        let sink = factory.open_shard(ShardId::new(0)).expect("sink opens");
        let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
        let append = DurableAppend {
            event_id: RecordId::for_batch(
                crate::LOGS_TOPIC_ID,
                LogicalPartitionId::new(0),
                BatchId::new(1),
            ),
            physical_shard_id: ShardId::new(0),
            reservation: shard_stream_core::Reservation {
                topic_id: crate::LOGS_TOPIC_ID,
                partition_id: LogicalPartitionId::new(0),
                batch_id: BatchId::new(1),
                first_offset: LogicalOffset::new(0),
                last_offset: LogicalOffset::new(0),
                record_count: NonZeroU32::new(1).expect("one"),
                placement: Placement {
                    virtual_lane_id: VirtualLaneId::new(0),
                    ring_epoch: RingEpoch::new(1),
                    leader_epoch: LeaderEpoch::new(0),
                    sequence: PlacementSequence::new(1),
                },
            },
            producer_event_id: None,
            atomic_group: None,
            delivery: DurableAppendDelivery::Publish,
            payload: payload.into(),
            transient_context: None,
        };
        let expected = DurableSinkCheckpoint::initial(topic_partition);
        let next = DurableSinkCheckpoint {
            topic_partition,
            next_placement_sequence: PlacementSequence::new(2),
            next_offset: LogicalOffset::new(1),
        };
        assert_eq!(
            sink.apply(expected, &[append], next)
                .expect("durable append indexes"),
            DurableSinkApply::Applied
        );
        assert_eq!(
            factory
                .load_checkpoint(topic_partition)
                .expect("checkpoint loads"),
            Some(next)
        );
        let matches = service
            .query_all(&LogQuery::new(topic_partition).with_term("message"))
            .expect("owner stripe is queryable");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].record.message.as_ref(), "sink message");
    }

    #[test]
    fn sink_journal_recovers_checkpoint_and_repairs_partial_tail() {
        let directory = TempDir::new("sink-journal-recovery");
        let config = OtlpSinkConfig {
            state_directory: Some(directory.0.join("sink")),
            ..OtlpSinkConfig::default()
        };
        let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
        let expected = DurableSinkCheckpoint::initial(topic_partition);
        let next = DurableSinkCheckpoint {
            topic_partition,
            next_placement_sequence: PlacementSequence::new(2),
            next_offset: LogicalOffset::new(1),
        };
        let append = DurableAppend {
            event_id: RecordId::for_batch(
                crate::LOGS_TOPIC_ID,
                LogicalPartitionId::new(0),
                BatchId::new(1),
            ),
            physical_shard_id: ShardId::new(0),
            reservation: shard_stream_core::Reservation {
                topic_id: crate::LOGS_TOPIC_ID,
                partition_id: LogicalPartitionId::new(0),
                batch_id: BatchId::new(1),
                first_offset: LogicalOffset::new(0),
                last_offset: LogicalOffset::new(0),
                record_count: NonZeroU32::new(1).expect("one"),
                placement: Placement {
                    virtual_lane_id: VirtualLaneId::new(0),
                    ring_epoch: RingEpoch::new(1),
                    leader_epoch: LeaderEpoch::new(0),
                    sequence: PlacementSequence::new(1),
                },
            },
            producer_event_id: None,
            atomic_group: None,
            delivery: DurableAppendDelivery::Publish,
            payload: Bytes::from(payload()),
            transient_context: None,
        };

        let factory =
            TelemetrySinkFactory::new([ShardId::new(0)], config.clone()).expect("factory opens");
        let sink = factory.open_shard(ShardId::new(0)).expect("sink opens");
        assert_eq!(
            sink.apply(expected, &[append], next)
                .expect("transaction is journaled"),
            DurableSinkApply::Applied
        );
        drop(sink);
        drop(factory);

        let journal_path = config
            .state_directory
            .as_ref()
            .expect("state directory")
            .join("shard-0.journal");
        let committed_bytes = fs::metadata(&journal_path).expect("journal metadata").len();
        use std::io::Write as _;
        fs::OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .expect("journal opens")
            .write_all(&[1, 2, 3])
            .expect("partial tail is written");

        let recovered =
            TelemetrySinkFactory::new([ShardId::new(0)], config).expect("factory recovers");
        assert_eq!(
            recovered
                .load_checkpoint(topic_partition)
                .expect("checkpoint loads"),
            Some(next)
        );
        assert_eq!(
            fs::metadata(journal_path).expect("journal metadata").len(),
            committed_bytes
        );
    }

    #[test]
    fn stream_engine_acks_only_after_the_otlp_sink_indexes_the_append() {
        let directory = TempDir::new("engine-otlp-sink");
        let config = engine_config(&directory.0);
        let factory = Arc::new(
            TelemetrySinkFactory::new(config.shard_ids(), OtlpSinkConfig::default())
                .expect("sink factory opens"),
        );
        let engine = StreamEngine::open_with_durable_sink(config, DurableSinkConfig::new(factory))
            .expect("engine with sink opens");
        engine
            .create_topic(TopicConfig {
                topic_id: crate::LOGS_TOPIC_ID,
                partitions: 1,
                shards: None,
            })
            .expect("topic creates");

        let response = engine
            .append(AppendRequest {
                request_id: 1,
                topic_id: crate::LOGS_TOPIC_ID,
                partition_id: LogicalPartitionId::new(0),
                record_count: 1,
                payload: Bytes::from(payload()),
                durability: Durability::Leader,
                producer: None,
                atomic_group: None,
                leader_epoch: None,
                extension_context: None,
            })
            .expect("durable OTLP append is indexed before acknowledgement");
        assert_eq!(response.first_offset, LogicalOffset::new(0));

        let error = engine
            .append(AppendRequest {
                request_id: 2,
                topic_id: crate::LOGS_TOPIC_ID,
                partition_id: LogicalPartitionId::new(0),
                record_count: 2,
                payload: Bytes::from(payload()),
                durability: Durability::Leader,
                producer: None,
                atomic_group: None,
                leader_epoch: None,
                extension_context: None,
            })
            .expect_err("mismatched OTLP record count rejects before it is durable");
        assert!(matches!(error, EngineError::InvalidConfig(_)));
    }
}
