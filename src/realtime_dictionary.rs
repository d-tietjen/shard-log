use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::structural::dictionary_training_sections;
use crate::{CompressionPlacementId, DictionaryCatalog, DictionaryId, LogDbError, LogDbResult};

const TRAINING_SAMPLE_CHUNK_BYTES: usize = 16 * 1024;

/// Bounded policy for continuously training immutable Zstandard dictionaries.
///
/// Reusable structural lanes from sealed blocks are deterministically sampled
/// and submitted through a bounded, non-blocking queue. Training and held-out
/// compression comparisons run on one background control-plane worker, never
/// on a stripe's record path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealtimeDictionaryConfig {
    /// Maximum sampled bytes retained from one sealed structural block.
    pub max_block_sample_bytes: usize,
    /// Total sampled bytes used to train one candidate dictionary.
    pub training_sample_bytes: usize,
    /// Maximum generated Zstandard dictionary size.
    pub dictionary_bytes: usize,
    /// Number of later block samples used for candidate admission.
    pub holdout_blocks: usize,
    /// Number of complete block observations waiting for the trainer.
    pub queue_blocks: usize,
    /// Maximum placement-specific training states retained by the service.
    pub max_placements: usize,
    /// Minimum net held-out byte savings after charging the dictionary once.
    pub min_net_savings_bytes: u64,
    /// Minimum net held-out savings in basis points after dictionary accounting.
    pub min_net_savings_bps: u16,
    /// Observed structural bytes skipped after each accepted or rejected candidate.
    pub retrain_after_bytes: u64,
}

impl Default for RealtimeDictionaryConfig {
    fn default() -> Self {
        Self {
            max_block_sample_bytes: 256 * 1024,
            training_sample_bytes: 4 * 1024 * 1024,
            dictionary_bytes: 64 * 1024,
            holdout_blocks: 16,
            queue_blocks: 64,
            max_placements: 16,
            min_net_savings_bytes: 16 * 1024,
            min_net_savings_bps: 200,
            retrain_after_bytes: 256 * 1024 * 1024,
        }
    }
}

impl RealtimeDictionaryConfig {
    fn validate(&self) -> LogDbResult<()> {
        if self.max_block_sample_bytes < 8 {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary max_block_sample_bytes must be at least 8",
            ));
        }
        if self.training_sample_bytes < self.max_block_sample_bytes {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary training_sample_bytes must cover one block sample",
            ));
        }
        if self.dictionary_bytes < 256 || self.dictionary_bytes >= self.training_sample_bytes {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary dictionary_bytes must be at least 256 and smaller than the training sample",
            ));
        }
        if self.holdout_blocks == 0 || self.queue_blocks == 0 || self.max_placements == 0 {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary holdout_blocks, queue_blocks, and max_placements must be nonzero",
            ));
        }
        if self.min_net_savings_bps > 10_000 {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary min_net_savings_bps cannot exceed 10000",
            ));
        }
        if self.retrain_after_bytes == 0 {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary retrain_after_bytes must be nonzero",
            ));
        }
        Ok(())
    }
}

/// Snapshot of online dictionary-learning activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RealtimeDictionaryStats {
    /// Complete structural blocks offered to the learner.
    pub observed_blocks: u64,
    /// Structural bytes represented by offered blocks before bounded sampling.
    pub observed_bytes: u64,
    /// Bytes copied into bounded observations.
    pub sampled_bytes: u64,
    /// Observations dropped because the training queue was full.
    pub dropped_blocks: u64,
    /// Observations rejected because the placement-state budget was full.
    pub placement_budget_rejections: u64,
    /// Largest number of placement states retained concurrently.
    pub max_tracked_placements: u64,
    /// Candidate training attempts.
    pub training_runs: u64,
    /// Candidate training attempts rejected by the zstd trainer.
    pub training_failures: u64,
    /// Valid candidates that failed held-out admission.
    pub candidates_rejected: u64,
    /// Immutable dictionaries published for future append batches.
    pub dictionaries_published: u64,
    /// Total immutable dictionary bytes published.
    pub dictionary_bytes_published: u64,
    /// Held-out bytes produced by the active dictionary or dictionary-free path.
    pub holdout_baseline_bytes: u64,
    /// Held-out bytes produced by candidate dictionaries, excluding dictionary bytes.
    pub holdout_candidate_bytes: u64,
    /// Nanoseconds spent training candidate dictionaries.
    pub training_nanos: u64,
    /// Nanoseconds spent comparing candidates with held-out samples.
    pub evaluation_nanos: u64,
}

#[derive(Debug, Default)]
struct AtomicRealtimeDictionaryStats {
    observed_blocks: AtomicU64,
    observed_bytes: AtomicU64,
    sampled_bytes: AtomicU64,
    dropped_blocks: AtomicU64,
    placement_budget_rejections: AtomicU64,
    max_tracked_placements: AtomicU64,
    training_runs: AtomicU64,
    training_failures: AtomicU64,
    candidates_rejected: AtomicU64,
    dictionaries_published: AtomicU64,
    dictionary_bytes_published: AtomicU64,
    holdout_baseline_bytes: AtomicU64,
    holdout_candidate_bytes: AtomicU64,
    training_nanos: AtomicU64,
    evaluation_nanos: AtomicU64,
}

impl AtomicRealtimeDictionaryStats {
    fn snapshot(&self) -> RealtimeDictionaryStats {
        RealtimeDictionaryStats {
            observed_blocks: self.observed_blocks.load(Ordering::Relaxed),
            observed_bytes: self.observed_bytes.load(Ordering::Relaxed),
            sampled_bytes: self.sampled_bytes.load(Ordering::Relaxed),
            dropped_blocks: self.dropped_blocks.load(Ordering::Relaxed),
            placement_budget_rejections: self.placement_budget_rejections.load(Ordering::Relaxed),
            max_tracked_placements: self.max_tracked_placements.load(Ordering::Relaxed),
            training_runs: self.training_runs.load(Ordering::Relaxed),
            training_failures: self.training_failures.load(Ordering::Relaxed),
            candidates_rejected: self.candidates_rejected.load(Ordering::Relaxed),
            dictionaries_published: self.dictionaries_published.load(Ordering::Relaxed),
            dictionary_bytes_published: self.dictionary_bytes_published.load(Ordering::Relaxed),
            holdout_baseline_bytes: self.holdout_baseline_bytes.load(Ordering::Relaxed),
            holdout_candidate_bytes: self.holdout_candidate_bytes.load(Ordering::Relaxed),
            training_nanos: self.training_nanos.load(Ordering::Relaxed),
            evaluation_nanos: self.evaluation_nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct DictionaryObservation {
    placement_id: CompressionPlacementId,
    original_bytes: usize,
    holdout_sample: Box<[u8]>,
    training_sample: TrainingSample,
}

#[derive(Debug)]
struct TrainingSample {
    data: Box<[u8]>,
    sizes: Box<[usize]>,
}

enum TrainerCommand {
    Observe(DictionaryObservation),
    Flush(SyncSender<()>),
    Shutdown,
}

impl std::fmt::Debug for TrainerCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Observe(observation) => {
                formatter.debug_tuple("Observe").field(observation).finish()
            }
            Self::Flush(_) => formatter.write_str("Flush"),
            Self::Shutdown => formatter.write_str("Shutdown"),
        }
    }
}

/// Cloneable, non-blocking block-level submission handle for log stripes.
///
/// One observation is submitted after an entire block seals. A full queue
/// drops that training observation without affecting block compression or
/// durability.
#[derive(Debug, Clone)]
pub struct RealtimeDictionaryObserver {
    config: RealtimeDictionaryConfig,
    sender: SyncSender<TrainerCommand>,
    stats: Arc<AtomicRealtimeDictionaryStats>,
}

impl RealtimeDictionaryObserver {
    /// Offers one complete structural block to the online learner.
    ///
    /// Returns `true` when the bounded observation was queued and `false` when
    /// training was allowed to fall behind without applying backpressure.
    pub fn observe_structural_block(
        &self,
        placement_id: CompressionPlacementId,
        structural: Vec<u8>,
    ) -> bool {
        let original_bytes = structural.len();
        let training_sample =
            sample_structural_training(&structural, self.config.max_block_sample_bytes)
                .unwrap_or_else(|| {
                    chunk_training_sample(
                        sample_bytes(&structural, self.config.max_block_sample_bytes)
                            .into_boxed_slice(),
                    )
                });
        let holdout_sample = sample_block(structural, self.config.max_block_sample_bytes);
        self.submit(
            placement_id,
            original_bytes,
            holdout_sample,
            training_sample,
        )
    }

    fn submit(
        &self,
        placement_id: CompressionPlacementId,
        original_bytes: usize,
        holdout_sample: Box<[u8]>,
        training_sample: TrainingSample,
    ) -> bool {
        self.stats.observed_blocks.fetch_add(1, Ordering::Relaxed);
        self.stats.observed_bytes.fetch_add(
            u64::try_from(original_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.stats.sampled_bytes.fetch_add(
            u64::try_from(
                holdout_sample
                    .len()
                    .saturating_add(training_sample.data.len()),
            )
            .unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        match self
            .sender
            .try_send(TrainerCommand::Observe(DictionaryObservation {
                placement_id,
                original_bytes,
                holdout_sample,
                training_sample,
            })) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.stats.dropped_blocks.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }
}

/// Background real-time dictionary learner and immutable publication service.
///
/// The service owns one worker regardless of stripe count. Stripes use
/// [`Self::observer`] to submit bounded block observations. The worker trains
/// per-placement candidates, evaluates them on later samples, and publishes
/// accepted dictionaries through the shared [`DictionaryCatalog`].
#[derive(Debug)]
pub struct RealtimeDictionaryTrainer {
    catalog: Arc<DictionaryCatalog>,
    sender: SyncSender<TrainerCommand>,
    stats: Arc<AtomicRealtimeDictionaryStats>,
    worker: Option<JoinHandle<()>>,
    config: RealtimeDictionaryConfig,
}

impl RealtimeDictionaryTrainer {
    /// Starts one bounded online dictionary worker.
    pub fn start(
        config: RealtimeDictionaryConfig,
        compression_level: i32,
        catalog: Arc<DictionaryCatalog>,
    ) -> LogDbResult<Self> {
        config.validate()?;
        if !zstd::compression_level_range().contains(&compression_level) {
            return Err(LogDbError::InvalidConfig(
                "realtime dictionary compression level is outside zstd's supported range",
            ));
        }
        let (sender, receiver) = sync_channel(config.queue_blocks);
        let stats = Arc::new(AtomicRealtimeDictionaryStats::default());
        let worker_config = config.clone();
        let worker_catalog = Arc::clone(&catalog);
        let worker_stats = Arc::clone(&stats);
        let worker = thread::Builder::new()
            .name("shard-log-dictionary-trainer".to_owned())
            .spawn(move || {
                run_trainer(
                    receiver,
                    worker_config,
                    compression_level,
                    worker_catalog,
                    worker_stats,
                );
            })
            .map_err(|error| {
                LogDbError::DictionaryTrainingFailed(format!(
                    "failed to spawn dictionary trainer: {error}"
                ))
            })?;
        Ok(Self {
            catalog,
            sender,
            stats,
            worker: Some(worker),
            config,
        })
    }

    /// Returns a non-blocking observer suitable for any stripe sharing this catalog.
    #[must_use]
    pub fn observer(&self) -> RealtimeDictionaryObserver {
        RealtimeDictionaryObserver {
            config: self.config.clone(),
            sender: self.sender.clone(),
            stats: Arc::clone(&self.stats),
        }
    }

    /// Returns the immutable dictionary catalog updated by this trainer.
    #[must_use]
    pub fn catalog(&self) -> Arc<DictionaryCatalog> {
        Arc::clone(&self.catalog)
    }

    /// Waits until all observations submitted before this call have been handled.
    pub fn flush(&self) -> LogDbResult<()> {
        let (complete, receiver) = sync_channel(1);
        self.sender
            .send(TrainerCommand::Flush(complete))
            .map_err(|_| LogDbError::DictionaryTrainerUnavailable)?;
        receiver
            .recv()
            .map_err(|_| LogDbError::DictionaryTrainerUnavailable)
    }

    /// Returns a lock-free statistics snapshot.
    #[must_use]
    pub fn stats(&self) -> RealtimeDictionaryStats {
        self.stats.snapshot()
    }
}

impl Drop for RealtimeDictionaryTrainer {
    fn drop(&mut self) {
        let _ = self.sender.send(TrainerCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug, Clone)]
struct CandidateDictionary {
    dictionary_id: DictionaryId,
    payload: Arc<[u8]>,
}

#[derive(Debug, Default)]
struct PlacementTrainingState {
    training_samples: Vec<TrainingSample>,
    training_bytes: usize,
    holdout_samples: Vec<Box<[u8]>>,
    holdout_original_bytes: u64,
    holdout_baseline_bytes: u64,
    holdout_candidate_bytes: u64,
    candidate: Option<CandidateDictionary>,
    cooldown_bytes: u64,
}

fn run_trainer(
    receiver: Receiver<TrainerCommand>,
    config: RealtimeDictionaryConfig,
    compression_level: i32,
    catalog: Arc<DictionaryCatalog>,
    stats: Arc<AtomicRealtimeDictionaryStats>,
) {
    let mut placements = HashMap::<CompressionPlacementId, PlacementTrainingState>::new();
    while let Ok(command) = receiver.recv() {
        match command {
            TrainerCommand::Observe(observation) => {
                process_observation(
                    observation,
                    &config,
                    compression_level,
                    &catalog,
                    &stats,
                    &mut placements,
                );
            }
            TrainerCommand::Flush(complete) => {
                let _ = complete.send(());
            }
            TrainerCommand::Shutdown => break,
        }
    }
}

fn process_observation(
    observation: DictionaryObservation,
    config: &RealtimeDictionaryConfig,
    compression_level: i32,
    catalog: &DictionaryCatalog,
    stats: &AtomicRealtimeDictionaryStats,
    placements: &mut HashMap<CompressionPlacementId, PlacementTrainingState>,
) {
    if !placements.contains_key(&observation.placement_id)
        && placements.len() >= config.max_placements
    {
        stats
            .placement_budget_rejections
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    let tracked = placements.len().saturating_add(usize::from(
        !placements.contains_key(&observation.placement_id),
    ));
    stats.max_tracked_placements.fetch_max(
        u64::try_from(tracked).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let state = placements.entry(observation.placement_id).or_default();
    if state.cooldown_bytes > 0 {
        state.cooldown_bytes = state
            .cooldown_bytes
            .saturating_sub(u64::try_from(observation.original_bytes).unwrap_or(u64::MAX));
        return;
    }

    if state.candidate.is_some() {
        state.holdout_original_bytes = state
            .holdout_original_bytes
            .saturating_add(u64::try_from(observation.original_bytes).unwrap_or(u64::MAX));
        state.holdout_samples.push(observation.holdout_sample);
        if state.holdout_samples.len() >= config.holdout_blocks {
            evaluate_candidate(
                observation.placement_id,
                state,
                config,
                compression_level,
                catalog,
                stats,
            );
        }
        return;
    }

    let remaining = config
        .training_sample_bytes
        .saturating_sub(state.training_bytes);
    if remaining > 0 {
        let sample = truncate_training_sample(observation.training_sample, remaining);
        state.training_bytes = state.training_bytes.saturating_add(sample.data.len());
        state.training_samples.push(sample);
    }
    if state.training_bytes >= config.training_sample_bytes {
        train_candidate(state, config, stats);
    }
}

fn train_candidate(
    state: &mut PlacementTrainingState,
    config: &RealtimeDictionaryConfig,
    stats: &AtomicRealtimeDictionaryStats,
) {
    stats.training_runs.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let mut continuous = Vec::with_capacity(state.training_bytes);
    let mut sizes = Vec::new();
    for sample in &state.training_samples {
        continuous.extend_from_slice(&sample.data);
        sizes.extend_from_slice(&sample.sizes);
    }
    let trained = if sizes.len() >= 8 {
        zstd::dict::from_continuous(&continuous, &sizes, config.dictionary_bytes)
    } else {
        Err(std::io::Error::other(
            "dictionary training requires at least eight samples",
        ))
    };
    stats.training_nanos.fetch_add(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    state.training_samples.clear();
    state.training_bytes = 0;
    match trained {
        Ok(payload) if !payload.is_empty() => {
            let dictionary_id = dictionary_id(&payload);
            state.candidate = Some(CandidateDictionary {
                dictionary_id,
                payload: Arc::from(payload),
            });
        }
        Ok(_) | Err(_) => {
            stats.training_failures.fetch_add(1, Ordering::Relaxed);
            state.cooldown_bytes = config.retrain_after_bytes;
        }
    }
}

fn evaluate_candidate(
    placement_id: CompressionPlacementId,
    state: &mut PlacementTrainingState,
    config: &RealtimeDictionaryConfig,
    compression_level: i32,
    catalog: &DictionaryCatalog,
    stats: &AtomicRealtimeDictionaryStats,
) {
    let Some(candidate) = state.candidate.clone() else {
        return;
    };
    let started = Instant::now();
    let current = catalog
        .snapshot()
        .ok()
        .and_then(|snapshot| snapshot.dictionary_for(placement_id));
    if current
        .as_ref()
        .is_some_and(|(dictionary_id, _)| *dictionary_id == candidate.dictionary_id)
    {
        stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
        reset_after_candidate(state, config);
        return;
    }

    let baseline_dictionary = current
        .as_ref()
        .map_or(&[][..], |(_, payload)| payload.as_ref());
    let mut baseline =
        match zstd::bulk::Compressor::with_dictionary(compression_level, baseline_dictionary) {
            Ok(compressor) => compressor,
            Err(_) => {
                stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
                reset_after_candidate(state, config);
                return;
            }
        };
    let mut proposed =
        match zstd::bulk::Compressor::with_dictionary(compression_level, &candidate.payload) {
            Ok(compressor) => compressor,
            Err(_) => {
                stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
                reset_after_candidate(state, config);
                return;
            }
        };
    let mut baseline_bytes = 0u64;
    let mut candidate_bytes = 0u64;
    let mut candidate_round_trip = None;
    for sample in &state.holdout_samples {
        let baseline_frame = match baseline.compress(sample) {
            Ok(frame) => frame,
            Err(_) => {
                stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
                reset_after_candidate(state, config);
                return;
            }
        };
        let candidate_frame = match proposed.compress(sample) {
            Ok(frame) => frame,
            Err(_) => {
                stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
                reset_after_candidate(state, config);
                return;
            }
        };
        baseline_bytes =
            baseline_bytes.saturating_add(u64::try_from(baseline_frame.len()).unwrap_or(u64::MAX));
        candidate_bytes = candidate_bytes
            .saturating_add(u64::try_from(candidate_frame.len()).unwrap_or(u64::MAX));
        if candidate_round_trip.is_none() {
            candidate_round_trip = Some((candidate_frame, sample.as_ref()));
        }
    }
    if let Some((frame, expected)) = candidate_round_trip {
        let decoded = zstd::bulk::Decompressor::with_dictionary(&candidate.payload)
            .and_then(|mut decompressor| decompressor.decompress(&frame, expected.len()));
        if !decoded.as_deref().is_ok_and(|decoded| decoded == expected) {
            stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
            reset_after_candidate(state, config);
            return;
        }
    }

    stats
        .holdout_baseline_bytes
        .fetch_add(baseline_bytes, Ordering::Relaxed);
    stats
        .holdout_candidate_bytes
        .fetch_add(candidate_bytes, Ordering::Relaxed);
    state.holdout_baseline_bytes = state.holdout_baseline_bytes.saturating_add(baseline_bytes);
    state.holdout_candidate_bytes = state
        .holdout_candidate_bytes
        .saturating_add(candidate_bytes);
    stats.evaluation_nanos.fetch_add(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    state.holdout_samples.clear();
    let dictionary_bytes = u64::try_from(candidate.payload.len()).unwrap_or(u64::MAX);
    let charged_candidate = state
        .holdout_candidate_bytes
        .saturating_add(dictionary_bytes);
    let net_savings = state
        .holdout_baseline_bytes
        .saturating_sub(charged_candidate);
    let savings_bps = net_savings.saturating_mul(10_000) / state.holdout_baseline_bytes.max(1);
    let admitted = charged_candidate < state.holdout_baseline_bytes
        && net_savings >= config.min_net_savings_bytes
        && savings_bps >= u64::from(config.min_net_savings_bps);

    if admitted
        && catalog
            .publish(
                placement_id,
                candidate.dictionary_id,
                Arc::clone(&candidate.payload),
            )
            .is_ok()
    {
        stats.dictionaries_published.fetch_add(1, Ordering::Relaxed);
        stats.dictionary_bytes_published.fetch_add(
            u64::try_from(candidate.payload.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        reset_after_candidate(state, config);
    } else if state.holdout_candidate_bytes >= state.holdout_baseline_bytes
        || state.holdout_original_bytes >= config.retrain_after_bytes
    {
        stats.candidates_rejected.fetch_add(1, Ordering::Relaxed);
        reset_after_candidate(state, config);
    }
}

fn reset_after_candidate(state: &mut PlacementTrainingState, config: &RealtimeDictionaryConfig) {
    state.candidate = None;
    state.holdout_samples.clear();
    state.holdout_original_bytes = 0;
    state.holdout_baseline_bytes = 0;
    state.holdout_candidate_bytes = 0;
    state.cooldown_bytes = config.retrain_after_bytes;
}

fn sample_block(structural: Vec<u8>, max_bytes: usize) -> Box<[u8]> {
    if structural.len() <= max_bytes {
        return structural.into_boxed_slice();
    }
    sample_bytes(&structural, max_bytes).into_boxed_slice()
}

fn sample_bytes(source: &[u8], max_bytes: usize) -> Vec<u8> {
    if source.len() <= max_bytes {
        return source.to_vec();
    }
    const WINDOWS: usize = 8;
    let window_bytes = max_bytes / WINDOWS;
    let mut sampled = Vec::with_capacity(max_bytes);
    for index in 0..WINDOWS {
        let start = index.saturating_mul(source.len().saturating_sub(window_bytes)) / (WINDOWS - 1);
        sampled.extend_from_slice(&source[start..start + window_bytes]);
    }
    let remainder = max_bytes.saturating_sub(sampled.len());
    if remainder > 0 {
        sampled.extend_from_slice(&source[source.len() - remainder..]);
    }
    sampled
}

fn chunk_training_sample(data: Box<[u8]>) -> TrainingSample {
    let full_chunks = data.len() / TRAINING_SAMPLE_CHUNK_BYTES;
    let mut sizes = Vec::with_capacity(full_chunks.saturating_add(1));
    sizes.extend(std::iter::repeat_n(
        TRAINING_SAMPLE_CHUNK_BYTES,
        full_chunks,
    ));
    let remainder = data.len() % TRAINING_SAMPLE_CHUNK_BYTES;
    if remainder > 0 {
        sizes.push(remainder);
    }
    TrainingSample {
        data,
        sizes: sizes.into_boxed_slice(),
    }
}

fn sample_structural_training(structural: &[u8], max_bytes: usize) -> Option<TrainingSample> {
    let sections = dictionary_training_sections(structural).ok()?;
    let budgets = fair_section_budgets(&sections, max_bytes);
    let mut data = Vec::with_capacity(max_bytes);
    let mut sizes = Vec::new();
    for (section, budget) in sections.into_iter().zip(budgets) {
        append_training_windows(section, budget, &mut data, &mut sizes);
    }
    if data.is_empty() {
        return None;
    }
    Some(TrainingSample {
        data: data.into_boxed_slice(),
        sizes: sizes.into_boxed_slice(),
    })
}

fn fair_section_budgets(sections: &[&[u8]; 4], max_bytes: usize) -> [usize; 4] {
    let total = sections
        .iter()
        .fold(0usize, |total, section| total.saturating_add(section.len()));
    let mut remaining = max_bytes.min(total);
    let mut budgets = [0usize; 4];
    while remaining > 0 {
        let open = (0..sections.len())
            .filter(|index| budgets[*index] < sections[*index].len())
            .count();
        if open == 0 {
            break;
        }
        let fair_share = remaining.div_ceil(open);
        let mut distributed = 0usize;
        for index in 0..sections.len() {
            if remaining == 0 {
                break;
            }
            let take = sections[index]
                .len()
                .saturating_sub(budgets[index])
                .min(fair_share)
                .min(remaining);
            budgets[index] = budgets[index].saturating_add(take);
            remaining = remaining.saturating_sub(take);
            distributed = distributed.saturating_add(take);
        }
        if distributed == 0 {
            break;
        }
    }
    budgets
}

fn append_training_windows(
    source: &[u8],
    budget: usize,
    data: &mut Vec<u8>,
    sizes: &mut Vec<usize>,
) {
    if budget == 0 || source.is_empty() {
        return;
    }
    if source.len() <= budget {
        data.extend_from_slice(source);
        sizes.push(source.len());
        return;
    }
    const WINDOWS: usize = 4;
    let window_count = WINDOWS.min(budget);
    let base_window = budget / window_count;
    let extra = budget % window_count;
    for index in 0..window_count {
        let length = base_window + usize::from(index < extra);
        let start = if window_count == 1 {
            0
        } else {
            index.saturating_mul(source.len().saturating_sub(length)) / (window_count - 1)
        };
        data.extend_from_slice(&source[start..start + length]);
        sizes.push(length);
    }
}

fn truncate_training_sample(sample: TrainingSample, max_bytes: usize) -> TrainingSample {
    if sample.data.len() <= max_bytes {
        return sample;
    }
    let data = sample.data[..max_bytes].to_vec().into_boxed_slice();
    let mut retained = 0usize;
    let mut sizes = Vec::new();
    for size in &sample.sizes {
        let remaining = max_bytes.saturating_sub(retained);
        if remaining == 0 {
            break;
        }
        let kept = (*size).min(remaining);
        if kept > 0 {
            sizes.push(kept);
            retained = retained.saturating_add(kept);
        }
    }
    TrainingSample {
        data,
        sizes: sizes.into_boxed_slice(),
    }
}

fn dictionary_id(payload: &[u8]) -> DictionaryId {
    let digest = blake3::hash(payload);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    DictionaryId::new(u128::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RealtimeDictionaryConfig {
        RealtimeDictionaryConfig {
            max_block_sample_bytes: 1024,
            training_sample_bytes: 8 * 1024,
            dictionary_bytes: 1024,
            holdout_blocks: 8,
            queue_blocks: 64,
            max_placements: 4,
            min_net_savings_bytes: 1,
            min_net_savings_bps: 1,
            retrain_after_bytes: u64::MAX,
        }
    }

    fn repeated_sample(index: u64) -> Vec<u8> {
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        let mut sample = Vec::with_capacity(1024);
        for _ in 0..512 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            sample.push(state as u8);
        }
        sample.extend_from_slice(format!(" unique request suffix {index:020}").as_bytes());
        while sample.len() < 1024 {
            let position = sample.len() as u64;
            sample.push(index.wrapping_mul(31).wrapping_add(position) as u8);
        }
        sample
    }

    #[test]
    fn deterministic_sampling_is_bounded_and_covers_the_block() {
        let source = (0..4096)
            .map(|value| u8::try_from(value % 251).expect("bounded"))
            .collect::<Vec<_>>();
        let first = sample_block(source.clone(), 1024);
        let second = sample_block(source.clone(), 1024);
        assert_eq!(first, second);
        assert_eq!(first.len(), 1024);
        assert_eq!(&first[..128], &source[..128]);
        assert_eq!(&first[first.len() - 128..], &source[source.len() - 128..]);
    }

    #[test]
    fn structural_lane_budget_is_bounded_and_fills_unused_capacity() {
        let templates = vec![1u8; 20];
        let bodies = vec![2u8; 2_000];
        let attributes = vec![3u8; 40];
        let fields = vec![4u8; 2_000];
        let sections = [
            templates.as_slice(),
            bodies.as_slice(),
            attributes.as_slice(),
            fields.as_slice(),
        ];
        let budgets = fair_section_budgets(&sections, 1_024);
        assert_eq!(budgets.iter().sum::<usize>(), 1_024);
        assert_eq!(budgets[0], templates.len());
        assert_eq!(budgets[2], attributes.len());
        assert_eq!(budgets[1], budgets[3]);

        let mut sampled = Vec::new();
        let mut sizes = Vec::new();
        append_training_windows(&bodies, budgets[1], &mut sampled, &mut sizes);
        assert_eq!(sampled.len(), budgets[1]);
        assert_eq!(sizes.iter().sum::<usize>(), budgets[1]);
        assert!(sizes.len() <= 4);
    }

    #[test]
    fn invalid_training_budgets_are_rejected() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let mut config = test_config();
        config.dictionary_bytes = config.training_sample_bytes;
        let error = RealtimeDictionaryTrainer::start(config, 1, catalog)
            .expect_err("invalid config is rejected");
        assert!(matches!(error, LogDbError::InvalidConfig(_)));
    }

    #[test]
    fn repeated_real_time_samples_publish_an_immutable_dictionary() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let trainer = RealtimeDictionaryTrainer::start(test_config(), 1, Arc::clone(&catalog))
            .expect("trainer starts");
        let observer = trainer.observer();
        let placement_id = CompressionPlacementId::new(77);
        for index in 0..16 {
            assert!(observer.observe_structural_block(placement_id, repeated_sample(index),));
        }
        trainer.flush().expect("trainer flushes");
        let stats = trainer.stats();
        assert_eq!(stats.training_runs, 1);
        assert_eq!(stats.training_failures, 0);
        assert_eq!(stats.dictionaries_published, 1);
        let snapshot = catalog.snapshot().expect("catalog snapshot");
        let (observed_id, dictionary) = snapshot
            .dictionary_for(placement_id)
            .expect("dictionary is published");
        assert_eq!(observed_id, dictionary_id(dictionary.as_ref()));
        assert!(!dictionary.is_empty());
    }

    #[test]
    fn profitable_candidate_accumulates_shadow_savings_before_publication() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let mut config = test_config();
        config.holdout_blocks = 2;
        config.min_net_savings_bytes = 2_000;
        let trainer = RealtimeDictionaryTrainer::start(config, 1, Arc::clone(&catalog))
            .expect("trainer starts");
        let observer = trainer.observer();
        let placement_id = CompressionPlacementId::new(78);

        for index in 0..10 {
            assert!(observer.observe_structural_block(placement_id, repeated_sample(index)));
        }
        trainer.flush().expect("first shadow batch flushes");
        let first = trainer.stats();
        assert_eq!(first.training_runs, 1);
        assert_eq!(first.candidates_rejected, 0);
        assert_eq!(first.dictionaries_published, 0);

        for index in 10..16 {
            assert!(observer.observe_structural_block(placement_id, repeated_sample(index)));
        }
        trainer.flush().expect("remaining shadow batches flush");
        assert_eq!(trainer.stats().dictionaries_published, 1);
        assert!(
            catalog
                .snapshot()
                .expect("snapshot")
                .dictionary_for(placement_id)
                .is_some()
        );
    }

    #[test]
    fn incompressible_holdout_rejects_dictionary_without_mutating_catalog() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let mut config = test_config();
        config.min_net_savings_bytes = u64::MAX;
        config.retrain_after_bytes = 1;
        let trainer = RealtimeDictionaryTrainer::start(config, 1, Arc::clone(&catalog))
            .expect("trainer starts");
        let observer = trainer.observer();
        let placement_id = CompressionPlacementId::new(91);
        for index in 0..16 {
            assert!(observer.observe_structural_block(placement_id, repeated_sample(index),));
        }
        trainer.flush().expect("trainer flushes");
        let stats = trainer.stats();
        assert_eq!(stats.training_runs, 1);
        assert_eq!(stats.dictionaries_published, 0);
        assert_eq!(stats.candidates_rejected, 1);
        assert!(
            catalog
                .snapshot()
                .expect("snapshot")
                .dictionary_for(placement_id)
                .is_none()
        );
    }

    #[test]
    fn placement_training_state_is_strictly_bounded() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let mut config = test_config();
        config.max_placements = 1;
        let trainer = RealtimeDictionaryTrainer::start(config, 1, catalog).expect("trainer starts");
        let observer = trainer.observer();
        assert!(
            observer.observe_structural_block(CompressionPlacementId::new(1), repeated_sample(1),)
        );
        assert!(
            observer.observe_structural_block(CompressionPlacementId::new(2), repeated_sample(2),)
        );
        trainer.flush().expect("trainer flushes");
        let stats = trainer.stats();
        assert_eq!(stats.max_tracked_placements, 1);
        assert_eq!(stats.placement_budget_rejections, 1);
    }
}
