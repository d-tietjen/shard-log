use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
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

use crate::correlation::{metric_matches_correlation, span_matches_correlation};
use crate::ingest_pack::validate_ingest_pack;
use crate::metric::metric_query_matches;
use crate::sink_journal::{SinkJournal, checkpoint_allows_lane_gap};
use crate::trace::trace_query_matches;
use crate::{
    CorrelationConfig, CorrelationIndex, CorrelationQuery, DictionaryCatalog, DurableMetricPoint,
    DurableSpan, LogMatch, LogPredicate, LogQuery, LogStripe, MetricApplyOutcome,
    MetricIngestProtocol, MetricQuery, MetricStripe, ObjectMetadata, ObjectTierConfig,
    RealtimeDictionaryObserver, RealtimeDictionaryTrainer, ShardTelemetryConfig,
    SharedTelemetryObjectStore, SsdCacheConfig, SsdCacheStats, SsdObjectCache, StripeConfig,
    TelemetryEnvelope, TelemetryError, TelemetryObjectTier, TelemetryRecordRef, TelemetryResult,
    TelemetryRouter, TelemetrySignal, TierArtifactKind, TierCheckpoint, TierQueryRange,
    TraceApplyOutcome, TraceQuery, TraceStripe, decode_metric_chunk, decode_signal_recovery_state,
    decode_trace_block, stage_signal_group,
};

/// Immutable object-tier and bounded SSD-cache settings shared by sink stripes.
#[derive(Debug, Clone)]
pub struct SinkObjectTierConfig {
    /// Object-store adapter used for immutable data and catalog publication.
    pub store: SharedTelemetryObjectStore,
    /// Local crash-safe staging directory for artifacts being published.
    pub spool_directory: PathBuf,
    /// Local SSD directory reserved for catalog, manifest, and index objects.
    pub control_cache_directory: PathBuf,
    /// Local SSD directory reserved for compressed payload ranges.
    pub payload_cache_directory: PathBuf,
    /// Logical partitions whose catalogs must be opened without object listing.
    pub partitions: Vec<TopicPartition>,
    /// Immutable group and catalog bounds.
    pub tier: ObjectTierConfig,
    /// Recoverable control/index cache bounds.
    pub control_cache: SsdCacheConfig,
    /// Recoverable payload cache bounds.
    pub payload_cache: SsdCacheConfig,
}

/// Cache occupancy and object-read counters for the isolated cold tiers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectTierCacheStats {
    /// Catalog, manifest, recovery, and query-index cache.
    pub control: SsdCacheStats,
    /// Compressed log, trace, and metric payload cache.
    pub payload: SsdCacheStats,
}

/// Configuration for shard-telemetry's per-shard native and OTLP index sinks.
#[derive(Debug, Clone)]
pub struct OtlpSinkConfig {
    /// Hot index and dictionary-cache limits for each physical shard.
    pub stripe: StripeConfig,
    /// Per-signal partition, retention, head-memory, and query limits.
    pub signals: ShardTelemetryConfig,
    /// Bounded stripe-local links across signal metadata and labels.
    pub correlations: CorrelationConfig,
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
            correlations: CorrelationConfig::default(),
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
        if self.correlations.max_keys == 0
            || self.correlations.max_refs_per_key == 0
            || self.correlations.max_total_refs == 0
        {
            return Err(TelemetryError::InvalidConfig(
                "correlation index bounds must be nonzero",
            ));
        }
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
        if self
            .object_tier
            .as_ref()
            .is_some_and(|tier| tier.control_cache_directory == tier.payload_cache_directory)
        {
            return Err(TelemetryError::InvalidConfig(
                "control and payload caches require separate directories",
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
    tier_caches: Option<TierCaches>,
}

#[derive(Debug, Clone)]
struct TierCaches {
    control: Arc<SsdObjectCache>,
    payload: Arc<SsdObjectCache>,
}

#[derive(Debug)]
struct TelemetryStripeState {
    stream_shard_id: ShardId,
    logs: LogStripe,
    traces: TraceStripe,
    metrics: MetricStripe,
    correlations: CorrelationIndex,
    log_partitions: u16,
    signal_tiers: HashMap<TelemetrySignal, SignalTierState>,
    router: TelemetryRouter,
}

#[derive(Debug)]
struct SignalTierState {
    signal: TelemetrySignal,
    tiers: HashMap<TopicPartition, TelemetryObjectTier<SharedTelemetryObjectStore>>,
    spool_directory: PathBuf,
    control_cache: Arc<SsdObjectCache>,
    payload_cache: Arc<SsdObjectCache>,
    config: ObjectTierConfig,
}

struct OpenedSignalTier {
    state: SignalTierState,
    checkpoints: Vec<DurableSinkCheckpoint>,
    recovery_states: Vec<Vec<u8>>,
}

impl SignalTierState {
    fn open(
        signal: TelemetrySignal,
        shard_id: ShardId,
        store: SharedTelemetryObjectStore,
        spool_directory: PathBuf,
        caches: &TierCaches,
        partitions: Vec<TopicPartition>,
        config: ObjectTierConfig,
    ) -> TelemetryResult<Option<OpenedSignalTier>> {
        if partitions.is_empty() {
            return Ok(None);
        }
        let mut tiers = HashMap::with_capacity(partitions.len());
        let mut checkpoints = Vec::new();
        let mut recovery_states = Vec::new();
        for partition in partitions {
            if partition.topic_id != signal.topic_id() {
                return Err(TelemetryError::InvalidConfig(
                    "signal object tier contains a partition from another signal",
                ));
            }
            let tier = TelemetryObjectTier::open(store.clone(), shard_id, partition, config)?;
            if let Some(checkpoint) = tier.root().latest_checkpoint {
                checkpoints.push(DurableSinkCheckpoint {
                    topic_partition: partition,
                    next_placement_sequence: shard_stream_core::PlacementSequence::new(
                        checkpoint.next_placement_sequence,
                    ),
                    next_offset: shard_stream_core::LogicalOffset::new(checkpoint.next_offset),
                });
            }
            if signal == TelemetrySignal::Metrics
                && let Some(entry) = tier.latest_group_cached(&caches.control)?
            {
                let manifest = tier.load_group_cached(&entry, &caches.control)?;
                let artifact =
                    manifest
                        .artifact(TierArtifactKind::QueryIndex)
                        .ok_or_else(|| {
                            TelemetryError::CorruptTier("metric group has no recovery index".into())
                        })?;
                let encoded = tier.read_artifact_cached(
                    artifact,
                    config.max_group_payload_bytes,
                    &caches.control,
                )?;
                recovery_states.push(decode_signal_recovery_state(
                    &encoded,
                    TelemetrySignal::Metrics,
                )?);
            }
            if tiers.insert(partition, tier).is_some() {
                return Err(TelemetryError::InvalidConfig(
                    "signal object tier contains a duplicate partition",
                ));
            }
        }
        let signal_name = match signal {
            TelemetrySignal::Traces => "traces",
            TelemetrySignal::Metrics => "metrics",
            TelemetrySignal::Logs => {
                return Err(TelemetryError::InvalidConfig(
                    "logs use the log-native object tier",
                ));
            }
        };
        Ok(Some(OpenedSignalTier {
            state: Self {
                signal,
                tiers,
                spool_directory: spool_directory
                    .join(format!("shard-{}", shard_id.get()))
                    .join(signal_name),
                control_cache: Arc::clone(&caches.control),
                payload_cache: Arc::clone(&caches.payload),
                config,
            },
            checkpoints,
            recovery_states,
        }))
    }
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
        let tier_caches = config
            .object_tier
            .as_ref()
            .map(|tier| {
                Ok::<_, TelemetryError>(TierCaches {
                    control: Arc::new(SsdObjectCache::open(
                        &tier.control_cache_directory,
                        tier.control_cache,
                    )?),
                    payload: Arc::new(SsdObjectCache::open(
                        &tier.payload_cache_directory,
                        tier.payload_cache,
                    )?),
                })
            })
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
            if let (Some(tier), Some(caches)) = (&config.object_tier, &tier_caches) {
                let log_partitions = tier
                    .partitions
                    .iter()
                    .copied()
                    .filter(|partition| partition.topic_id == TelemetrySignal::Logs.topic_id())
                    .collect::<Vec<_>>();
                if !log_partitions.is_empty() {
                    for checkpoint in logs.attach_object_tier(
                        tier.store.clone(),
                        tier.spool_directory.clone(),
                        Arc::clone(&caches.control),
                        Arc::clone(&caches.payload),
                        log_partitions,
                        tier.tier,
                    )? {
                        merge_recovered_checkpoint(&mut recovered_checkpoints, checkpoint)?;
                    }
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
            let mut signal_tiers = HashMap::new();
            let mut metric_recovery_states = Vec::new();
            if let (Some(tier), Some(caches)) = (&config.object_tier, &tier_caches) {
                for signal in [TelemetrySignal::Traces, TelemetrySignal::Metrics] {
                    let partitions = tier
                        .partitions
                        .iter()
                        .copied()
                        .filter(|partition| partition.topic_id == signal.topic_id())
                        .collect::<Vec<_>>();
                    if let Some(opened) = SignalTierState::open(
                        signal,
                        shard_id,
                        tier.store.clone(),
                        tier.spool_directory.clone(),
                        caches,
                        partitions,
                        tier.tier,
                    )? {
                        for checkpoint in opened.checkpoints {
                            merge_recovered_checkpoint(&mut recovered_checkpoints, checkpoint)?;
                        }
                        metric_recovery_states.extend(opened.recovery_states);
                        signal_tiers.insert(signal, opened.state);
                    }
                }
            }
            let mut metrics =
                MetricStripe::new(config.signals.metrics.head_memory_bytes_per_stripe)?;
            for recovery_state in metric_recovery_states {
                metrics.restore_accumulator_checkpoints(&recovery_state)?;
            }
            let stripe = TelemetryStripeState {
                stream_shard_id: shard_id,
                logs,
                traces: TraceStripe::new(config.signals.traces.head_memory_bytes_per_stripe)?,
                metrics,
                correlations: CorrelationIndex::new(config.correlations),
                log_partitions: config.signals.logs.logical_partitions.get(),
                signal_tiers,
                router: TelemetryRouter::from_config(&config.signals),
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
            tier_caches,
        })
    }

    /// Returns a cloneable coordinator for querying the owner-only stripe
    /// workers after they have been opened by shard-stream.
    #[must_use]
    pub fn service(&self) -> TelemetryService {
        TelemetryService {
            workers: Arc::clone(&self.query_workers),
            tier_caches: self.tier_caches.clone(),
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
    Correlate {
        query: CorrelationQuery,
        response: SyncSender<TelemetryResult<Vec<TelemetryRecordRef>>>,
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
                let _ = response.send(query_trace_stripe(&stripe, &query));
            }
            SinkCommand::QueryMetrics { query, response } => {
                let _ = response.send(query_metric_stripe(&stripe, &query));
            }
            SinkCommand::Correlate { query, response } => {
                let _ = response.send(query_correlation_stripe(&stripe, &query));
            }
            SinkCommand::Flush { response } => {
                let result = flush_object_tiers(&mut stripe, &checkpoints);
                let _ = response.send(result);
            }
            SinkCommand::RetainedPayloadBytes { response } => {
                let bytes = stripe
                    .logs
                    .retained_payload_bytes()
                    .saturating_add(stripe.traces.retained_payload_bytes())
                    .saturating_add(stripe.metrics.retained_payload_bytes());
                let _ = response.send(bytes);
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
    tier_caches: Option<TierCaches>,
}

impl TelemetryService {
    /// Returns shared cold-tier cache occupancy and object-read counters.
    #[must_use]
    pub fn object_tier_cache_stats(&self) -> Option<ObjectTierCacheStats> {
        self.tier_caches
            .as_ref()
            .map(|caches| ObjectTierCacheStats {
                control: caches.control.stats(),
                payload: caches.payload.stats(),
            })
    }

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

    /// Connects logs, spans, and metric exemplars through exact trace,
    /// resource, scope, and typed-label identities.
    pub fn query_correlations(
        &self,
        query: &CorrelationQuery,
    ) -> TelemetryResult<Vec<TelemetryRecordRef>> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::Correlate {
                    query: query.clone(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a correlation query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut refs = Vec::new();
        for (shard_id, receiver) in responses {
            refs.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying correlations"
                ))
            })??);
        }
        refs.sort_unstable();
        refs.dedup();
        if let Some(after) = query.after {
            refs.retain(|record| *record > after);
        }
        refs.truncate(query.limit);
        Ok(refs)
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
    if matches!(
        next.topic_partition.topic_id,
        crate::TRACES_TOPIC_ID | crate::METRICS_TOPIC_ID
    ) {
        offload_signal_partition(stripe, next.topic_partition, next, false)
            .map_err(log_error_to_engine)?;
    }
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
                let outcome = stripe.traces.apply(record.clone(), append_time)?;
                if matches!(
                    outcome,
                    TraceApplyOutcome::Inserted | TraceApplyOutcome::Replaced
                ) {
                    stripe.correlations.index_span(&record);
                }
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
                let outcome = stripe.metrics.apply(record.clone(), protocol)?;
                if matches!(
                    outcome,
                    MetricApplyOutcome::Inserted
                        | MetricApplyOutcome::Replaced
                        | MetricApplyOutcome::OutOfOrder
                ) {
                    stripe.correlations.index_metric(&record);
                }
            }
        }
    }
    Ok(())
}

fn offload_signal_partition(
    stripe: &mut TelemetryStripeState,
    partition: TopicPartition,
    checkpoint: DurableSinkCheckpoint,
    force: bool,
) -> TelemetryResult<usize> {
    let signal = if partition.topic_id == crate::TRACES_TOPIC_ID {
        TelemetrySignal::Traces
    } else if partition.topic_id == crate::METRICS_TOPIC_ID {
        TelemetrySignal::Metrics
    } else {
        return Ok(0);
    };
    let Some(state) = stripe.signal_tiers.get(&signal) else {
        return Ok(0);
    };
    if !state.tiers.contains_key(&partition) {
        return Ok(0);
    }
    let pending = match signal {
        TelemetrySignal::Traces => stripe.traces.pending_partition(partition),
        TelemetrySignal::Metrics => stripe.metrics.pending_partition(partition),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    };
    let pending_bytes = pending.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(
                u64::try_from(payload.payload.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            )
            .ok_or(TelemetryError::RecordTooLarge)
    })?;
    let block_trigger = state.config.max_blocks_per_group.saturating_div(2).max(1);
    if !force
        && pending_bytes < state.config.target_group_payload_bytes
        && pending.len() < block_trigger
    {
        return Ok(0);
    }

    match signal {
        TelemetrySignal::Traces => {
            let now_nanos = pending
                .iter()
                .map(|payload| payload.max_timestamp_unix_nanos)
                .max()
                .unwrap_or(0);
            stripe.traces.seal_partition(partition, now_nanos)?;
        }
        TelemetrySignal::Metrics => stripe.metrics.seal_partition(partition)?,
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    }
    let mut payloads = match signal {
        TelemetrySignal::Traces => stripe.traces.pending_partition(partition),
        TelemetrySignal::Metrics => stripe.metrics.pending_partition(partition),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    };
    if payloads.is_empty() {
        return Ok(0);
    }
    payloads.sort_unstable_by_key(|payload| payload.resident_id);
    let payload_bytes = payloads.iter().try_fold(0u64, |total, payload| {
        total
            .checked_add(
                u64::try_from(payload.payload.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            )
            .ok_or(TelemetryError::RecordTooLarge)
    })?;
    let recovery_state = match signal {
        TelemetrySignal::Metrics => stripe
            .metrics
            .accumulator_checkpoints_for_partition(partition)?,
        TelemetrySignal::Traces => Vec::new(),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    };
    let state = stripe
        .signal_tiers
        .get_mut(&signal)
        .expect("signal tier was checked above");
    if payloads.len() > state.config.max_blocks_per_group
        || payload_bytes > state.config.max_group_payload_bytes
    {
        return Err(TelemetryError::ObjectStore(format!(
            "complete {signal:?} partition boundary needs {} blocks and {payload_bytes} bytes, exceeding the configured object group bound",
            payloads.len()
        )));
    }
    let tier = state
        .tiers
        .get_mut(&partition)
        .expect("signal partition tier was checked above");
    let group_sequence = tier
        .root()
        .pages
        .last()
        .map_or(0, |page| page.last_group_sequence.saturating_add(1));
    let first_block_id = tier.root().next_block_id;
    let group_directory = state.spool_directory.join(format!(
        "topic-{:032x}-partition-{}/group-{group_sequence:020}",
        partition.topic_id.get(),
        partition.partition_id.get()
    ));
    let source = stage_signal_group(
        &group_directory,
        "signal",
        signal,
        group_sequence,
        first_block_id,
        TierCheckpoint {
            next_placement_sequence: checkpoint.next_placement_sequence.get(),
            next_offset: checkpoint.next_offset.get(),
        },
        &payloads,
        &recovery_state,
    )?;
    let staged_paths = source
        .artifacts
        .iter()
        .map(|artifact| artifact.path.clone())
        .collect::<Vec<_>>();
    tier.publish_group(source)?;
    for path in staged_paths {
        if let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "shard-telemetry retained published signal spool {} after cleanup failed: {error}",
                path.display()
            );
        }
    }
    let _ = fs::remove_dir(&group_directory);
    let resident_ids = payloads
        .iter()
        .map(|payload| payload.resident_id)
        .collect::<Vec<_>>();
    match signal {
        TelemetrySignal::Traces => stripe.traces.release_published_blocks(&resident_ids),
        TelemetrySignal::Metrics => stripe.metrics.release_published_chunks(&resident_ids),
        TelemetrySignal::Logs => unreachable!("signal was selected above"),
    }
    Ok(1)
}

fn flush_object_tiers(
    stripe: &mut TelemetryStripeState,
    checkpoints: &Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>,
) -> TelemetryResult<usize> {
    let mut published = stripe.logs.offload_indexed_groups(true)?;
    let checkpoints = checkpoints
        .lock()
        .map_err(|_| TelemetryError::StorageIo("sink checkpoint lock is poisoned".into()))?
        .clone();
    let mut partitions = stripe
        .signal_tiers
        .values()
        .flat_map(|state| state.tiers.keys().copied())
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    for partition in partitions {
        let Some(checkpoint) = checkpoints.get(&partition).copied() else {
            continue;
        };
        published = published.saturating_add(offload_signal_partition(
            stripe, partition, checkpoint, true,
        )?);
    }
    Ok(published)
}

fn read_signal_tier_payloads(
    state: &SignalTierState,
    partitions: &[TopicPartition],
    range: TierQueryRange,
    correlation: Option<&CorrelationQuery>,
) -> TelemetryResult<Vec<Vec<u8>>> {
    let mut payloads = Vec::new();
    let expected_codec = match state.signal {
        TelemetrySignal::Traces => "trace-native",
        TelemetrySignal::Metrics => "metric-native",
        TelemetrySignal::Logs => return Ok(payloads),
    };
    for partition in partitions {
        let Some(tier) = state.tiers.get(partition) else {
            continue;
        };
        for group in tier.candidate_groups_cached(range, &state.control_cache)? {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let metadata = ObjectMetadata {
                bytes: artifact.bytes,
                version_token: artifact.checksum.clone(),
                content_digest: artifact.checksum.clone(),
            };
            let blocks = manifest
                .blocks
                .iter()
                .filter(|block| {
                    block.compression_codec == expected_codec
                        && range.signal_identity.is_none_or(|identity| {
                            block
                                .min_signal_identity
                                .zip(block.max_signal_identity)
                                .is_some_and(|(minimum, maximum)| {
                                    identity >= minimum && identity <= maximum
                                })
                        })
                        && range
                            .min_timestamp_unix_nanos
                            .is_none_or(|minimum| block.max_timestamp_unix_nanos >= minimum)
                        && range
                            .max_timestamp_unix_nanos
                            .is_none_or(|maximum| block.min_timestamp_unix_nanos <= maximum)
                        && correlation.is_none_or(|query| {
                            block.correlation_filter.as_ref().is_some_and(|filter| {
                                if state.signal == TelemetrySignal::Traces {
                                    block
                                        .min_signal_identity
                                        .zip(block.max_signal_identity)
                                        .is_some_and(|(minimum, maximum)| {
                                            filter.may_match_trace_block(query, minimum, maximum)
                                        })
                                } else {
                                    filter.may_match(query)
                                }
                            })
                        })
                })
                .collect::<Vec<_>>();
            let ranges = blocks
                .iter()
                .map(|block| {
                    let end = block
                        .payload_offset
                        .checked_add(block.payload_bytes)
                        .ok_or(TelemetryError::RecordTooLarge)?;
                    Ok(block.payload_offset..end)
                })
                .collect::<TelemetryResult<Vec<_>>>()?;
            let encoded = state.payload_cache.read_ranges_with_metadata(
                tier.object_store(),
                &artifact.object_key,
                &metadata,
                &ranges,
            )?;
            for (block, payload) in blocks.into_iter().zip(encoded) {
                if blake3::hash(&payload).to_hex().as_str() != block.payload_checksum {
                    return Err(TelemetryError::CorruptTier(format!(
                        "signal block {} payload checksum failed",
                        block.block_id
                    )));
                }
                payloads.push(payload);
            }
        }
    }
    Ok(payloads)
}

fn query_trace_stripe(
    stripe: &TelemetryStripeState,
    query: &TraceQuery,
) -> TelemetryResult<Vec<DurableSpan>> {
    let mut winners = BTreeMap::new();
    for span in stripe.traces.query(query)? {
        winners.insert(
            (Arc::clone(&span.tenant), span.trace_id, span.span_id),
            span,
        );
    }
    if let Some(state) = stripe.signal_tiers.get(&TelemetrySignal::Traces) {
        let partitions = query.trace_id.map_or_else(
            || state.tiers.keys().copied().collect::<Vec<_>>(),
            |trace_id| vec![stripe.router.trace(&query.tenant, trace_id)],
        );
        let identity = query
            .trace_id
            .map(|trace_id| u128::from_be_bytes(*trace_id.as_bytes()));
        for payload in read_signal_tier_payloads(
            state,
            &partitions,
            TierQueryRange {
                min_timestamp_unix_nanos: query.start_time_unix_nanos,
                max_timestamp_unix_nanos: query.end_time_unix_nanos,
                signal_identity: identity,
                ..TierQueryRange::default()
            },
            None,
        )? {
            for span in decode_trace_block(&payload)?
                .into_iter()
                .filter(|span| trace_query_matches(query, span))
            {
                let key = (Arc::clone(&span.tenant), span.trace_id, span.span_id);
                if winners.get(&key).is_none_or(|existing: &DurableSpan| {
                    existing.record_ref.offset < span.record_ref.offset
                }) {
                    winners.insert(key, span);
                }
            }
        }
    }
    let mut spans = winners.into_values().collect::<Vec<_>>();
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

fn query_metric_stripe(
    stripe: &TelemetryStripeState,
    query: &MetricQuery,
) -> TelemetryResult<Vec<DurableMetricPoint>> {
    let mut winners = BTreeMap::new();
    for point in stripe.metrics.query(query)? {
        winners.insert(
            (point.series_fingerprint(), point.timestamp_unix_nanos),
            point,
        );
    }
    if let Some(state) = stripe.signal_tiers.get(&TelemetrySignal::Metrics) {
        let partitions = query.series.map_or_else(
            || state.tiers.keys().copied().collect::<Vec<_>>(),
            |series| vec![stripe.router.metric(&query.tenant, series)],
        );
        for payload in read_signal_tier_payloads(
            state,
            &partitions,
            TierQueryRange {
                min_timestamp_unix_nanos: query.start_time_unix_nanos,
                max_timestamp_unix_nanos: query.end_time_unix_nanos,
                signal_identity: query.series.map(crate::SeriesFingerprint::get),
                ..TierQueryRange::default()
            },
            None,
        )? {
            for point in decode_metric_chunk(&payload)?
                .into_iter()
                .filter(|point| metric_query_matches(query, point))
            {
                let key = (point.series_fingerprint(), point.timestamp_unix_nanos);
                if winners
                    .get(&key)
                    .is_none_or(|existing: &DurableMetricPoint| {
                        existing.record_ref.offset < point.record_ref.offset
                    })
                {
                    winners.insert(key, point);
                }
            }
        }
    }
    let mut points = winners.into_values().collect::<Vec<_>>();
    points.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
    points.truncate(query.limit.max(1));
    Ok(points)
}

fn query_correlation_stripe(
    stripe: &TelemetryStripeState,
    query: &CorrelationQuery,
) -> TelemetryResult<Vec<TelemetryRecordRef>> {
    if query.limit == 0
        || (query.trace_id.is_none()
            && query.resource_id.is_none()
            && query.scope_id.is_none()
            && query.attributes.is_empty())
    {
        return Ok(Vec::new());
    }
    let mut refs = stripe.correlations.query(query);
    if query
        .signal
        .is_none_or(|signal| signal == TelemetrySignal::Logs)
        && query
            .after
            .is_none_or(|after| after.signal <= TelemetrySignal::Logs)
        && query.attributes.len() == query.labels.len()
        && (query.trace_id.is_some()
            || query.resource_id.is_some()
            || query.scope_id.is_some()
            || !query.labels.is_empty())
    {
        let partitions = if let Some(trace_id) = query.trace_id {
            vec![stripe.router.log(&query.tenant, Some(trace_id), &[])]
        } else {
            (0..stripe.log_partitions)
                .map(|partition| {
                    TopicPartition::new(
                        TelemetrySignal::Logs.topic_id(),
                        shard_stream_core::LogicalPartitionId::new(u32::from(partition)),
                    )
                })
                .collect()
        };
        let label_predicates = query
            .labels
            .iter()
            .map(|(key, value)| {
                LogPredicate::or(
                    [
                        key.to_string(),
                        format!("resource.{key}"),
                        format!("scope.{key}"),
                        format!("attr.{key}"),
                        format!("resource.loki.label.{key}"),
                        format!("attr.loki.metadata.{key}"),
                    ]
                    .into_iter()
                    .map(|field| LogPredicate::field_equals(field, Arc::clone(value)))
                    .collect(),
                )
            })
            .collect::<Vec<_>>();
        for partition in partitions {
            if query.after.is_some_and(|after| {
                after.signal == TelemetrySignal::Logs && partition < after.topic_partition
            }) {
                continue;
            }
            let mut log_query = LogQuery::new(partition)
                .where_predicate(LogPredicate::and(label_predicates.clone()))
                .with_limit(query.limit);
            if let Some(after) = query.after.filter(|after| {
                after.signal == TelemetrySignal::Logs && after.topic_partition == partition
            }) {
                let Some(start_offset) = after.offset.get().checked_add(1) else {
                    continue;
                };
                log_query.start_offset = Some(shard_stream_core::LogicalOffset::new(start_offset));
            }
            if let Some(trace_id) = query.trace_id {
                log_query = log_query.with_field("otel.trace_id", trace_id.to_string());
            }
            if let Some(resource_id) = query.resource_id {
                log_query = log_query.with_field("otel.resource.id", resource_id.to_string());
            }
            if let Some(scope_id) = query.scope_id {
                log_query = log_query.with_field("otel.scope.id", scope_id.to_string());
            }
            refs.extend(stripe.logs.query_refs(&log_query));
        }
    }
    for signal in [TelemetrySignal::Traces, TelemetrySignal::Metrics] {
        if query.signal.is_some_and(|requested| requested != signal)
            || query.after.is_some_and(|after| after.signal > signal)
        {
            continue;
        }
        let Some(state) = stripe.signal_tiers.get(&signal) else {
            continue;
        };
        let mut partitions = state.tiers.keys().copied().collect::<Vec<_>>();
        partitions.sort_unstable();
        for payload in
            read_signal_tier_payloads(state, &partitions, TierQueryRange::default(), Some(query))?
        {
            match signal {
                TelemetrySignal::Traces => refs.extend(
                    decode_trace_block(&payload)?
                        .into_iter()
                        .filter(|span| span_matches_correlation(query, span))
                        .map(|span| span.record_ref),
                ),
                TelemetrySignal::Metrics => refs.extend(
                    decode_metric_chunk(&payload)?
                        .into_iter()
                        .filter(|point| metric_matches_correlation(query, point))
                        .map(|point| point.record_ref),
                ),
                TelemetrySignal::Logs => unreachable!("loop contains only cold native signals"),
            }
            refs.sort_unstable();
            refs.dedup();
            if let Some(after) = query.after {
                refs.retain(|record| *record > after);
            }
            refs.truncate(query.limit);
        }
    }
    refs.sort_unstable();
    refs.dedup();
    if let Some(after) = query.after {
        refs.retain(|record| *record > after);
    }
    refs.truncate(query.limit);
    Ok(refs)
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
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{AnyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        trace::v1::{ResourceSpans, ScopeSpans, Span, span::Link},
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

    use crate::{
        LocalObjectStore, MetricExemplar, MetricIdentity, MetricKind, MetricValue, NumberValue,
        OtlpMetricEvent, OtlpTelemetryDecoder, ResourceContext, ScopeContext,
        SharedTelemetryObjectStore, TelemetryRecordRef,
    };

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

    fn durable_append(
        topic_partition: TopicPartition,
        payload: Vec<u8>,
        batch: u128,
    ) -> (DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint) {
        let expected = DurableSinkCheckpoint::initial(topic_partition);
        let next = DurableSinkCheckpoint {
            topic_partition,
            next_placement_sequence: PlacementSequence::new(2),
            next_offset: LogicalOffset::new(1),
        };
        (
            DurableAppend {
                event_id: RecordId::for_batch(
                    topic_partition.topic_id,
                    topic_partition.partition_id,
                    BatchId::new(batch),
                ),
                physical_shard_id: ShardId::new(0),
                reservation: shard_stream_core::Reservation {
                    topic_id: topic_partition.topic_id,
                    partition_id: topic_partition.partition_id,
                    batch_id: BatchId::new(batch),
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
                payload: Bytes::from(payload),
                transient_context: None,
            },
            expected,
            next,
        )
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

    #[test]
    fn trace_and_metric_queries_survive_object_tier_restart() {
        let directory = TempDir::new("signal-object-tier-restart");
        let signals = ShardTelemetryConfig::default();
        let router = TelemetryRouter::from_config(&signals);
        let trace_id = crate::TraceId::from_bytes([1; 16]).expect("trace ID");
        let linked_trace_id = crate::TraceId::from_bytes([9; 16]).expect("linked trace ID");
        let trace_partition = router.trace("tenant-a", trace_id);

        let trace_request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        span_id: vec![2; 8],
                        name: "cold trace".into(),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        links: vec![Link {
                            trace_id: linked_trace_id.as_bytes().to_vec(),
                            span_id: vec![3; 8],
                            ..Link::default()
                        }],
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let trace_events = OtlpTelemetryDecoder
            .decode_traces("tenant-a", &trace_request.encode_to_vec())
            .expect("trace request decodes");
        let trace_payload = crate::prepare_trace_envelope(trace_partition, trace_events)
            .expect("trace envelope")
            .encode()
            .expect("trace STEL");

        let placeholder_metric_partition =
            TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0));
        let metric_point = DurableMetricPoint {
            stream_shard_id: ShardId::new(0),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Metrics,
                placeholder_metric_partition,
                LogicalOffset::new(0),
            ),
            identity: Arc::new(MetricIdentity {
                tenant: Arc::from("tenant-a"),
                resource: Arc::new(ResourceContext::default()),
                scope: Arc::new(ScopeContext::default()),
                name: Arc::from("cold_metric"),
                unit: Arc::from("1"),
                kind: MetricKind::Gauge,
                point_attributes: Arc::new(Vec::new()),
            }),
            description: Arc::from("cold metric"),
            metadata: Arc::new(Vec::new()),
            start_time_unix_nanos: 0,
            timestamp_unix_nanos: 30,
            flags: 0,
            value: MetricValue::Gauge(NumberValue::Integer(7)),
            exemplars: Arc::new(vec![MetricExemplar {
                filtered_attributes: Arc::new(Vec::new()),
                timestamp_unix_nanos: 30,
                value: NumberValue::Integer(7),
                span_id: None,
                trace_id: Some(trace_id),
            }]),
        };
        let series = metric_point.series_fingerprint();
        let metric_partition = router.metric("tenant-a", series);
        let metric_payload = crate::prepare_metric_envelope(
            metric_partition,
            vec![OtlpMetricEvent::from_durable(metric_point).expect("metric event")],
        )
        .expect("metric envelope")
        .encode()
        .expect("metric STEL");

        let object_store =
            LocalObjectStore::open(directory.0.join("objects")).expect("object store opens");
        let mut partitions = vec![trace_partition, metric_partition];
        partitions.sort_unstable();
        let config = OtlpSinkConfig {
            signals,
            object_tier: Some(SinkObjectTierConfig {
                store: SharedTelemetryObjectStore::from(object_store),
                spool_directory: directory.0.join("spool"),
                control_cache_directory: directory.0.join("control-cache"),
                payload_cache_directory: directory.0.join("payload-cache"),
                partitions,
                tier: ObjectTierConfig {
                    target_group_payload_bytes: 1,
                    max_group_payload_bytes: 8 * 1024 * 1024,
                    max_blocks_per_group: 64,
                    groups_per_page: 8,
                    max_control_object_bytes: 64 * 1024,
                },
                control_cache: SsdCacheConfig {
                    max_bytes: 16 * 1024 * 1024,
                    chunk_bytes: 64 * 1024,
                    max_read_bytes: 8 * 1024 * 1024,
                },
                payload_cache: SsdCacheConfig {
                    max_bytes: 16 * 1024 * 1024,
                    chunk_bytes: 64 * 1024,
                    max_read_bytes: 8 * 1024 * 1024,
                },
            }),
            ..OtlpSinkConfig::default()
        };

        let (trace_append, trace_expected, trace_next) =
            durable_append(trace_partition, trace_payload, 11);
        let (metric_append, metric_expected, metric_next) =
            durable_append(metric_partition, metric_payload, 12);
        {
            let factory = TelemetrySinkFactory::new([ShardId::new(0)], config.clone())
                .expect("factory opens");
            let service = factory.service();
            let sink = factory.open_shard(ShardId::new(0)).expect("sink opens");
            assert_eq!(
                sink.apply(trace_expected, &[trace_append], trace_next)
                    .expect("trace applies"),
                DurableSinkApply::Applied
            );
            assert_eq!(
                sink.apply(metric_expected, &[metric_append], metric_next)
                    .expect("metric applies"),
                DurableSinkApply::Applied
            );
            assert_eq!(service.flush_object_tier().expect("signals flush"), 2);
            assert_eq!(service.retained_payload_bytes().expect("resident bytes"), 0);
        }

        let recovered = TelemetrySinkFactory::new([ShardId::new(0)], config)
            .expect("factory reopens cold catalogs");
        assert_eq!(
            recovered
                .load_checkpoint(trace_partition)
                .expect("trace checkpoint"),
            Some(trace_next)
        );
        assert_eq!(
            recovered
                .load_checkpoint(metric_partition)
                .expect("metric checkpoint"),
            Some(metric_next)
        );
        let service = recovered.service();
        let _sink = recovered
            .open_shard(ShardId::new(0))
            .expect("recovered worker opens");
        let spans = service
            .query_traces(&TraceQuery {
                tenant: Arc::from("tenant-a"),
                trace_id: Some(trace_id),
                limit: 10,
                ..TraceQuery::default()
            })
            .expect("cold trace query");
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name.as_ref(), "cold trace");
        let points = service
            .query_metrics(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                series: Some(series),
                limit: 10,
                ..MetricQuery::default()
            })
            .expect("cold metric query");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value, MetricValue::Gauge(NumberValue::Integer(7)));
        let first_cache_stats = service
            .object_tier_cache_stats()
            .expect("object-tier cache diagnostics exist");
        assert!(first_cache_stats.control.misses > 0);
        assert!(first_cache_stats.payload.misses > 0);
        service
            .query_traces(&TraceQuery {
                tenant: Arc::from("tenant-a"),
                trace_id: Some(trace_id),
                limit: 10,
                ..TraceQuery::default()
            })
            .expect("warm trace query");
        service
            .query_metrics(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                series: Some(series),
                limit: 10,
                ..MetricQuery::default()
            })
            .expect("warm metric query");
        let warm_cache_stats = service
            .object_tier_cache_stats()
            .expect("object-tier cache diagnostics exist");
        assert_eq!(
            warm_cache_stats.control.misses,
            first_cache_stats.control.misses
        );
        assert_eq!(
            warm_cache_stats.payload.misses,
            first_cache_stats.payload.misses
        );
        assert!(warm_cache_stats.control.hits > first_cache_stats.control.hits);
        assert!(warm_cache_stats.payload.hits > first_cache_stats.payload.hits);
        let correlated = service
            .query_correlations(
                &CorrelationQuery::new("tenant-a")
                    .with_trace_id(trace_id)
                    .with_limit(10),
            )
            .expect("cold correlation query");
        assert_eq!(correlated.len(), 2);
        assert!(
            [TelemetrySignal::Traces, TelemetrySignal::Metrics]
                .into_iter()
                .all(|signal| correlated.iter().any(|record| record.signal == signal))
        );
        let linked = service
            .query_correlations(
                &CorrelationQuery::new("tenant-a")
                    .with_trace_id(linked_trace_id)
                    .with_limit(10),
            )
            .expect("cold linked-trace correlation query");
        assert_eq!(linked.len(), 1);
        assert_eq!(linked[0].signal, TelemetrySignal::Traces);
    }
}
