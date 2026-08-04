use std::borrow::Cow;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use memmap2::{Mmap, MmapOptions};
use serde::Deserialize;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};
use shard_telemetry::{
    BlockQueryIndex, CompressionBlockCollator, CompressionCohortId, CompressionLocalityConfig,
    CompressionLocalityRecord, CompressionLocalityStats, CompressionPlacementId,
    DecodedStructuralRecord, DictionaryCatalog, DictionaryId, MessageFingerprint,
    PersistentQueryIndex, QueryBlockMetadata, RealtimeDictionaryConfig, RealtimeDictionaryObserver,
    RealtimeDictionaryStats, RealtimeDictionaryTrainer, StructuralRecordView,
    decode_structural_block, encode_indexed_structural_records, fingerprint_message,
};

const DEFAULT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_BLOCK_BYTES: usize = 8 * 1024 * 1024;
const PROGRESS_BYTES: u64 = 1024 * 1024 * 1024;
const ZSTD_LEVEL: i32 = 1;
const LOCALITY_CONTAINER_MAGIC: &[u8; 8] = b"SLOGLOC1";
const DOCKER_JSON_PREFIX: &[u8] = b"{\"log\":\"";
const DOCKER_JSON_STDERR_SUFFIX: &[u8] = b"\",\"stream\":\"stderr\",\"time\":\"";
const DOCKER_JSON_STDOUT_SUFFIX: &[u8] = b"\",\"stream\":\"stdout\",\"time\":\"";
const DOCKER_MESSAGE_CACHE_ENTRIES: usize = 256;
const QUERY_PARTITION: TopicPartition =
    TopicPartition::new(TopicId::new(0), LogicalPartitionId::new(0));

#[derive(Debug)]
struct Settings {
    input: PathBuf,
    report_path: Option<PathBuf>,
    limit_bytes: u64,
    block_bytes: usize,
    workers: usize,
    output_dir: Option<PathBuf>,
    locality_routing: bool,
    realtime_dictionary: bool,
    persistent_query_index: bool,
}

#[derive(Debug, Deserialize)]
struct DockerJsonLine<'a> {
    #[serde(borrow)]
    log: Cow<'a, str>,
    #[serde(borrow)]
    stream: Cow<'a, str>,
    #[serde(borrow)]
    time: Cow<'a, str>,
}

struct DockerStructuralRecord<'a> {
    offset: LogicalOffset,
    timestamp_unix_nanos: u64,
    message: Rc<str>,
    stream: Cow<'a, str>,
}

impl StructuralRecordView for DockerStructuralRecord<'_> {
    fn structural_offset(&self) -> LogicalOffset {
        self.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        1
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        (index == 0).then_some(("docker.stream", self.stream.as_ref()))
    }
}

struct DockerMessageCacheEntry<'a> {
    raw: &'a [u8],
    decoded: Rc<str>,
}

struct DockerMessageCache<'a> {
    entries: Vec<Option<DockerMessageCacheEntry<'a>>>,
}

impl<'a> DockerMessageCache<'a> {
    fn new() -> Self {
        Self {
            entries: std::iter::repeat_with(|| None)
                .take(DOCKER_MESSAGE_CACHE_ENTRIES)
                .collect(),
        }
    }

    fn decode(&mut self, raw: &'a [u8]) -> Option<Rc<str>> {
        let slot = docker_message_cache_slot(raw);
        if let Some(entry) = &self.entries[slot]
            && entry.raw == raw
        {
            return Some(Rc::clone(&entry.decoded));
        }
        let decoded = decode_common_json_message(raw)?;
        self.entries[slot] = Some(DockerMessageCacheEntry {
            raw,
            decoded: Rc::clone(&decoded),
        });
        Some(decoded)
    }
}

#[derive(Default)]
struct DockerTimestampPrefixCache {
    prefix: [u8; 19],
    base_nanos: u64,
    initialized: bool,
}

#[derive(Debug, Default)]
struct Benchmark {
    input_bytes: u64,
    source_bytes: u64,
    leading_discarded_bytes: u64,
    rejected_records: u64,
    rejected_bytes: u64,
    structural_bytes: u64,
    embedded_index_bytes: u64,
    structural_stored_bytes: u64,
    manifest_bytes: u64,
    query_index_bytes: u64,
    blocks: u64,
    records: u64,
    structural_compression_time: Duration,
    elapsed: Duration,
    verification_elapsed: Duration,
    verified_blocks: u64,
    workers: usize,
    locality: CompressionLocalityStats,
    dictionary_bytes: u64,
    dictionary_stats: RealtimeDictionaryStats,
}

struct RawBlock<'a> {
    ordinal: usize,
    source_offset: u64,
    raw: &'a [u8],
    verify: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockSpan {
    ordinal: usize,
    start: u64,
    length: usize,
}

struct BlockResult {
    ordinal: usize,
    source_offset: u64,
    input_bytes: u64,
    source_bytes: u64,
    record_count: u64,
    rejected_records: u64,
    rejected_bytes: u64,
    structural_bytes: u64,
    embedded_index_bytes: u64,
    structural_stored_bytes: u64,
    pack_worker: usize,
    pack_offset: u64,
    payload_checksum: u64,
    dictionary_id: Option<DictionaryId>,
    query_index: Option<(QueryBlockMetadata, BlockQueryIndex)>,
    structural_compression_time: Duration,
}

struct WorkerResult {
    blocks: Vec<BlockResult>,
    locality: CompressionLocalityStats,
}

struct BenchmarkCompressor {
    zstd: zstd::bulk::Compressor<'static>,
    active_dictionary: Option<DictionaryId>,
    dictionary_catalog: Option<Arc<DictionaryCatalog>>,
    dictionary_observer: Option<RealtimeDictionaryObserver>,
}

impl BenchmarkCompressor {
    fn new(
        dictionary_catalog: Option<Arc<DictionaryCatalog>>,
        dictionary_observer: Option<RealtimeDictionaryObserver>,
    ) -> Result<Self, String> {
        Ok(Self {
            zstd: zstd::bulk::Compressor::new(ZSTD_LEVEL).map_err(|error| error.to_string())?,
            active_dictionary: None,
            dictionary_catalog,
            dictionary_observer,
        })
    }

    fn compress_log_block(
        &mut self,
        placement_id: CompressionPlacementId,
        structural: Vec<u8>,
    ) -> Result<CompressedStructural, String> {
        let dictionary = self
            .dictionary_catalog
            .as_ref()
            .map(|catalog| catalog.snapshot().map_err(|error| error.to_string()))
            .transpose()?
            .and_then(|snapshot| snapshot.dictionary_for(placement_id));
        let dictionary_id = dictionary.as_ref().map(|(dictionary_id, _)| *dictionary_id);
        if self.active_dictionary != dictionary_id {
            let payload = dictionary
                .as_ref()
                .map_or(&[][..], |(_, payload)| payload.as_ref());
            self.zstd
                .set_dictionary(ZSTD_LEVEL, payload)
                .map_err(|error| error.to_string())?;
            self.active_dictionary = dictionary_id;
        }
        let payload = self
            .zstd
            .compress(&structural)
            .map_err(|error| error.to_string())?;
        let structural_len = structural.len();
        if let Some(observer) = &self.dictionary_observer {
            let _ = observer.observe_structural_block(placement_id, structural);
        }
        Ok(CompressedStructural {
            structural_len,
            payload,
            dictionary_id,
            dictionary_payload: dictionary.map(|(_, payload)| payload),
        })
    }
}

struct CompressedStructural {
    structural_len: usize,
    payload: Vec<u8>,
    dictionary_id: Option<DictionaryId>,
    dictionary_payload: Option<Arc<[u8]>>,
}

struct CompressedGroups {
    structural_bytes: u64,
    embedded_index_bytes: u64,
    payload: Vec<u8>,
    dictionary_id: Option<DictionaryId>,
    dictionary_payload: Option<Arc<[u8]>>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    let benchmark = run_benchmark(&settings)?;
    let report = render_report(&settings, &benchmark);
    if let Some(path) = &settings.report_path {
        let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
        output.write_all(report.as_bytes())?;
        output.flush()?;
    }
    print!("{report}");
    Ok(())
}

fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let input = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: shard-telemetry-structural-bench <docker-json.log> [--report PATH] [--output-dir PATH] [--limit-bytes N] [--block-bytes N] [--workers N] [--locality enabled|disabled] [--dictionary disabled|realtime] [--index disabled|persistent]")?;
    let mut settings = Settings {
        input,
        report_path: None,
        limit_bytes: DEFAULT_LIMIT_BYTES,
        block_bytes: DEFAULT_BLOCK_BYTES,
        workers: 1,
        output_dir: None,
        locality_routing: false,
        realtime_dictionary: false,
        persistent_query_index: false,
    };
    while let Some(flag) = arguments.next() {
        match flag.to_string_lossy().as_ref() {
            "--report" => {
                settings.report_path = Some(PathBuf::from(
                    arguments.next().ok_or("--report requires a path")?,
                ));
            }
            "--output-dir" => {
                settings.output_dir = Some(PathBuf::from(
                    arguments.next().ok_or("--output-dir requires a path")?,
                ));
            }
            "--limit-bytes" => {
                settings.limit_bytes = parse_byte_count(
                    &arguments
                        .next()
                        .ok_or("--limit-bytes requires a value")?
                        .to_string_lossy(),
                )?;
            }
            "--block-bytes" => {
                settings.block_bytes = usize::try_from(parse_byte_count(
                    &arguments
                        .next()
                        .ok_or("--block-bytes requires a value")?
                        .to_string_lossy(),
                )?)?;
            }
            "--workers" => {
                settings.workers = arguments
                    .next()
                    .ok_or("--workers requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--locality" => {
                settings.locality_routing = match arguments
                    .next()
                    .ok_or("--locality requires enabled or disabled")?
                    .to_string_lossy()
                    .as_ref()
                {
                    "enabled" => true,
                    "disabled" => false,
                    value => {
                        return Err(
                            format!("--locality must be enabled or disabled, got {value}").into(),
                        );
                    }
                };
            }
            "--dictionary" => {
                settings.realtime_dictionary = match arguments
                    .next()
                    .ok_or("--dictionary requires disabled or realtime")?
                    .to_string_lossy()
                    .as_ref()
                {
                    "disabled" => false,
                    "realtime" => true,
                    value => {
                        return Err(format!(
                            "--dictionary must be disabled or realtime, got {value}"
                        )
                        .into());
                    }
                };
            }
            "--index" => {
                settings.persistent_query_index = match arguments
                    .next()
                    .ok_or("--index requires disabled or persistent")?
                    .to_string_lossy()
                    .as_ref()
                {
                    "disabled" => false,
                    "persistent" => true,
                    value => {
                        return Err(
                            format!("--index must be disabled or persistent, got {value}").into(),
                        );
                    }
                };
            }
            _ => return Err(format!("unknown argument: {}", flag.to_string_lossy()).into()),
        }
    }
    if settings.limit_bytes == 0 || settings.block_bytes == 0 || settings.workers == 0 {
        return Err("byte limits must be nonzero".into());
    }
    if settings.realtime_dictionary && settings.locality_routing {
        return Err(
            "the real-time dictionary benchmark currently requires --locality disabled".into(),
        );
    }
    if settings.persistent_query_index && settings.locality_routing {
        return Err(
            "the persistent query-index benchmark currently requires --locality disabled".into(),
        );
    }
    Ok(settings)
}

fn parse_byte_count(input: &str) -> Result<u64, Box<dyn Error>> {
    let trimmed = input.trim();
    let split = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(split);
    if digits.is_empty() {
        return Err(format!("invalid byte count: {input}").into());
    }
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => 1024 * 1024 * 1024,
        _ => return Err(format!("unsupported byte suffix: {suffix}").into()),
    };
    digits
        .parse::<u64>()?
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte count overflows u64: {input}").into())
}

fn run_benchmark(settings: &Settings) -> Result<Benchmark, Box<dyn Error>> {
    let started = Instant::now();
    let (source_start, spans) = build_block_spans(settings)?;
    let span_build_time = started.elapsed();
    if let Some(output_dir) = &settings.output_dir {
        std::fs::create_dir(output_dir)?;
    }
    let spans = Arc::<[BlockSpan]>::from(spans);
    let completed_input = AtomicU64::new(0);
    let mut manifest_entries = Vec::with_capacity(spans.len());
    let dictionary_catalog = settings
        .realtime_dictionary
        .then(|| Arc::new(DictionaryCatalog::new()));
    let dictionary_trainer = dictionary_catalog
        .as_ref()
        .map(|catalog| {
            RealtimeDictionaryTrainer::start(
                RealtimeDictionaryConfig::default(),
                ZSTD_LEVEL,
                Arc::clone(catalog),
            )
        })
        .transpose()?;
    let dictionary_observer = dictionary_trainer
        .as_ref()
        .map(RealtimeDictionaryTrainer::observer);
    let mut benchmark = Benchmark {
        input_bytes: spans
            .iter()
            .map(|span| u64::try_from(span.length).unwrap_or(u64::MAX))
            .sum(),
        leading_discarded_bytes: source_start,
        blocks: u64::try_from(spans.len())?,
        workers: settings.workers,
        ..Benchmark::default()
    };
    let input_file = File::open(&settings.input)?;
    let mapped_input = map_read_only(&input_file)?;
    thread::scope(|scope| -> Result<(), Box<dyn Error>> {
        let mut handles = Vec::with_capacity(settings.workers);
        for worker_id in 0..settings.workers {
            let spans = Arc::clone(&spans);
            let mapped_input = &mapped_input;
            let output_dir = settings.output_dir.clone();
            let completed_input = &completed_input;
            let total_blocks = spans.len();
            let locality_routing = settings.locality_routing;
            let worker_count = settings.workers;
            let block_bytes = settings.block_bytes;
            let persistent_query_index = settings.persistent_query_index;
            let dictionary_catalog = dictionary_catalog.clone();
            let dictionary_observer = dictionary_observer.clone();
            handles.push(scope.spawn(move || -> Result<WorkerResult, String> {
                let mut compressor =
                    BenchmarkCompressor::new(dictionary_catalog, dictionary_observer)?;
                let mut locality = CompressionBlockCollator::new(
                    CompressionLocalityConfig {
                        enabled: locality_routing,
                        ..CompressionLocalityConfig::default()
                    },
                    u64::try_from(block_bytes).map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())?;
                let mut pack = output_dir
                    .map(|directory| {
                        OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(directory.join(format!("worker-{worker_id:02}.pack")))
                    })
                    .transpose()
                    .map_err(|error| error.to_string())?;
                let mut results = Vec::new();
                let mut index = worker_id;
                while let Some(span) = spans.get(index).copied() {
                    let start = usize::try_from(span.start).map_err(|error| error.to_string())?;
                    let end = start
                        .checked_add(span.length)
                        .ok_or_else(|| "mapped block range overflows".to_owned())?;
                    let raw = mapped_input
                        .get(start..end)
                        .ok_or_else(|| "mapped block range exceeds input".to_owned())?;
                    let (mut result, compressed) = process_block(
                        RawBlock {
                            ordinal: span.ordinal,
                            source_offset: span.start.saturating_sub(source_start),
                            raw,
                            verify: span.ordinal == 0,
                        },
                        &mut compressor,
                        &mut locality,
                        persistent_query_index,
                    )?;
                    result.pack_worker = worker_id;
                    if let Some(pack) = pack.as_mut() {
                        result.pack_offset =
                            pack.stream_position().map_err(|error| error.to_string())?;
                        pack.write_all(&compressed)
                            .map_err(|error| error.to_string())?;
                    }
                    results.push(result);
                    let span_bytes =
                        u64::try_from(span.length).map_err(|error| error.to_string())?;
                    let previous = completed_input.fetch_add(span_bytes, Ordering::Relaxed);
                    let completed = previous.saturating_add(span_bytes);
                    let mut boundary = previous / PROGRESS_BYTES + 1;
                    while boundary.saturating_mul(PROGRESS_BYTES) <= completed {
                        eprintln!(
                            "progress: {} GiB complete, {total_blocks} blocks total",
                            boundary
                        );
                        boundary = boundary.saturating_add(1);
                    }
                    index = index.saturating_add(worker_count);
                }
                if let Some(pack) = pack.as_mut() {
                    pack.sync_all().map_err(|error| error.to_string())?;
                }
                Ok(WorkerResult {
                    blocks: results,
                    locality: locality.stats(),
                })
            }));
        }

        for handle in handles {
            let worker = handle.join().map_err(|_| "benchmark worker panicked")??;
            merge_locality_stats(&mut benchmark.locality, worker.locality);
            for result in worker.blocks {
                benchmark.source_bytes = benchmark.source_bytes.saturating_add(result.source_bytes);
                benchmark.rejected_records = benchmark
                    .rejected_records
                    .saturating_add(result.rejected_records);
                benchmark.rejected_bytes = benchmark
                    .rejected_bytes
                    .saturating_add(result.rejected_bytes);
                benchmark.structural_bytes = benchmark
                    .structural_bytes
                    .saturating_add(result.structural_bytes);
                benchmark.embedded_index_bytes = benchmark
                    .embedded_index_bytes
                    .saturating_add(result.embedded_index_bytes);
                benchmark.records = benchmark.records.saturating_add(result.record_count);
                benchmark.structural_stored_bytes = benchmark
                    .structural_stored_bytes
                    .saturating_add(result.structural_stored_bytes);
                benchmark.structural_compression_time += result.structural_compression_time;
                manifest_entries.push(result);
            }
        }
        Ok(())
    })?;
    if let Some(trainer) = &dictionary_trainer {
        trainer.flush()?;
        benchmark.dictionary_stats = trainer.stats();
    }
    if let Some(output_dir) = &settings.output_dir {
        if settings.persistent_query_index {
            benchmark.query_index_bytes = write_query_index(output_dir, &mut manifest_entries)?;
        }
        if let Some(catalog) = &dictionary_catalog {
            let snapshot = catalog.snapshot()?;
            benchmark.dictionary_bytes = write_dictionaries(output_dir, &snapshot)?;
        }
        benchmark.manifest_bytes = write_manifest(output_dir, &mut manifest_entries)?;
        if settings.realtime_dictionary {
            benchmark.manifest_bytes =
                benchmark
                    .manifest_bytes
                    .saturating_add(write_dictionary_assignments(
                        output_dir,
                        &mut manifest_entries,
                    )?);
        }
    } else {
        if let Some(catalog) = &dictionary_catalog {
            benchmark.dictionary_bytes = catalog
                .snapshot()?
                .dictionaries()
                .map(|(_, payload)| u64::try_from(payload.len()).unwrap_or(u64::MAX))
                .sum();
        }
        if settings.persistent_query_index {
            benchmark.query_index_bytes =
                u64::try_from(encode_query_index(&mut manifest_entries)?.len())?;
        }
    }
    benchmark.elapsed = started.elapsed();
    if let Some(output_dir) = &settings.output_dir {
        let verification_started = Instant::now();
        benchmark.verified_blocks = verify_durable_output(output_dir)?;
        benchmark.verification_elapsed = verification_started.elapsed();
    }
    eprintln!(
        "span directory: {} blocks in {:.3} seconds",
        benchmark.blocks,
        span_build_time.as_secs_f64()
    );
    Ok(benchmark)
}

#[allow(unsafe_code)]
fn map_read_only(file: &File) -> std::io::Result<Mmap> {
    // SAFETY: the benchmark corpus is immutable for the complete run. The map
    // is read-only, remains owned until every scoped worker exits, and no code
    // in this process can resize or mutate the underlying file.
    unsafe { MmapOptions::new().map(file) }
}

fn build_block_spans(settings: &Settings) -> Result<(u64, Vec<BlockSpan>), Box<dyn Error>> {
    let file = File::open(&settings.input)?;
    let file_bytes = file.metadata()?.len();
    let first_line_end =
        find_newline_forward(&file, 0, file_bytes)?.ok_or("input contains no complete line")?;
    let mut first_line = vec![0; usize::try_from(first_line_end)?];
    file.read_exact_at(&mut first_line, 0)?;
    let source_start = if serde_json::from_slice::<DockerJsonLine<'_>>(&first_line).is_ok() {
        0
    } else {
        first_line_end
    };
    let nominal_end = source_start
        .saturating_add(settings.limit_bytes)
        .min(file_bytes);
    let source_end = find_newline_backward(&file, nominal_end, source_start)?
        .ok_or("input contains no complete Docker JSON records")?;
    if source_end <= source_start {
        return Err("input contained no complete Docker JSON log records".into());
    }
    let mut spans = Vec::new();
    let mut start = source_start;
    let mut nominal = source_start.saturating_add(u64::try_from(settings.block_bytes)?);
    while nominal < source_end {
        let boundary = find_newline_forward(&file, nominal, source_end)?.unwrap_or(source_end);
        if boundary > start {
            spans.push(BlockSpan {
                ordinal: spans.len(),
                start,
                length: usize::try_from(boundary - start)?,
            });
            start = boundary;
        }
        nominal = nominal.saturating_add(u64::try_from(settings.block_bytes)?);
    }
    if start < source_end {
        spans.push(BlockSpan {
            ordinal: spans.len(),
            start,
            length: usize::try_from(source_end - start)?,
        });
    }
    Ok((source_start, spans))
}

fn find_newline_forward(
    file: &File,
    mut offset: u64,
    limit: u64,
) -> Result<Option<u64>, Box<dyn Error>> {
    let mut buffer = [0u8; 4096];
    while offset < limit {
        let length = usize::try_from((limit - offset).min(buffer.len() as u64))?;
        let read = file.read_at(&mut buffer[..length], offset)?;
        if read == 0 {
            break;
        }
        if let Some(position) = buffer[..read].iter().position(|byte| *byte == b'\n') {
            return Ok(Some(offset + u64::try_from(position)? + 1));
        }
        offset = offset.saturating_add(u64::try_from(read)?);
    }
    Ok(None)
}

fn find_newline_backward(
    file: &File,
    mut end: u64,
    lower_bound: u64,
) -> Result<Option<u64>, Box<dyn Error>> {
    let mut buffer = [0u8; 4096];
    while end > lower_bound {
        let start = end.saturating_sub(buffer.len() as u64).max(lower_bound);
        let length = usize::try_from(end - start)?;
        file.read_exact_at(&mut buffer[..length], start)?;
        if let Some(position) = buffer[..length].iter().rposition(|byte| *byte == b'\n') {
            return Ok(Some(start + u64::try_from(position)? + 1));
        }
        end = start;
    }
    Ok(None)
}

fn process_block(
    block: RawBlock<'_>,
    compressor: &mut BenchmarkCompressor,
    locality: &mut CompressionBlockCollator,
    persistent_query_index: bool,
) -> Result<(BlockResult, Vec<u8>), String> {
    let source_cohort = CompressionCohortId::UNCLASSIFIED;
    let estimated_records = block.raw.len() / 128 + 1;
    let mut parsed_records = Vec::<DockerStructuralRecord<'_>>::with_capacity(estimated_records);
    let mut message_cache = DockerMessageCache::new();
    let mut timestamp_cache = DockerTimestampPrefixCache::default();
    let mut locality_records = locality
        .is_enabled()
        .then(|| Vec::<CompressionLocalityRecord>::with_capacity(estimated_records));
    let mut local_offset = 0u64;
    let mut accepted_source_bytes = 0u64;
    let mut rejected_records = 0u64;
    let mut rejected_bytes = 0u64;
    let mut line_start = 0usize;
    let line_ends = memchr::memchr_iter(b'\n', block.raw)
        .map(|newline| newline + 1)
        .chain(std::iter::once(block.raw.len()));
    for line_end in line_ends {
        if line_end == line_start {
            continue;
        }
        let line = &block.raw[line_start..line_end];
        line_start = line_end;
        let line_bytes = u64::try_from(line.len()).map_err(|error| error.to_string())?;
        let (message, stream, timestamp_unix_nanos) = if let Some(parsed) =
            parse_canonical_docker_json(line, &mut message_cache, &mut timestamp_cache)
        {
            parsed
        } else {
            let docker: DockerJsonLine<'_> = match serde_json::from_slice(line) {
                Ok(docker) => docker,
                Err(_) => {
                    rejected_records = rejected_records.saturating_add(1);
                    rejected_bytes = rejected_bytes.saturating_add(line_bytes);
                    local_offset = local_offset.saturating_add(line_bytes);
                    continue;
                }
            };
            let timestamp_unix_nanos = match parse_docker_timestamp(&docker.time) {
                Ok(timestamp) => timestamp,
                Err(_) => {
                    rejected_records = rejected_records.saturating_add(1);
                    rejected_bytes = rejected_bytes.saturating_add(line_bytes);
                    local_offset = local_offset.saturating_add(line_bytes);
                    continue;
                }
            };
            (
                Rc::<str>::from(docker.log),
                docker.stream,
                timestamp_unix_nanos,
            )
        };
        let fingerprint = if locality.is_enabled() {
            fingerprint_message(&message, &[])
        } else {
            MessageFingerprint {
                shape_hash: 0,
                locality_signature: 0,
            }
        };
        if let Some(locality_records) = &mut locality_records {
            locality_records.push(CompressionLocalityRecord {
                fingerprint,
                source_bytes: line_bytes,
            });
        }
        parsed_records.push(DockerStructuralRecord {
            offset: LogicalOffset::new(block.source_offset.saturating_add(local_offset)),
            timestamp_unix_nanos,
            message,
            stream,
        });
        local_offset = local_offset.saturating_add(line_bytes);
        accepted_source_bytes = accepted_source_bytes.saturating_add(line_bytes);
    }
    let query_index = if persistent_query_index && !parsed_records.is_empty() {
        let first_offset = parsed_records
            .first()
            .expect("nonempty records have a first offset")
            .offset;
        let last_offset = parsed_records
            .last()
            .expect("nonempty records have a last offset")
            .offset;
        let (min_timestamp_unix_nanos, max_timestamp_unix_nanos) =
            parsed_records
                .iter()
                .fold((u64::MAX, 0u64), |(minimum, maximum), record| {
                    (
                        minimum.min(record.timestamp_unix_nanos),
                        maximum.max(record.timestamp_unix_nanos),
                    )
                });
        Some((
            QueryBlockMetadata {
                block_ordinal: u32::try_from(block.ordinal).map_err(|error| error.to_string())?,
                topic_partition: QUERY_PARTITION,
                first_offset,
                last_offset,
                min_timestamp_unix_nanos,
                max_timestamp_unix_nanos,
                record_count: u32::try_from(parsed_records.len())
                    .map_err(|error| error.to_string())?,
            },
            BlockQueryIndex::build(&parsed_records).map_err(|error| error.to_string())?,
        ))
    } else {
        None
    };
    let home = CompressionPlacementId::from_source_cohort(source_cohort);
    let (record_count, compressed_groups, structural_compression_time) =
        if let Some(locality_records) = locality_records {
            let mut groups =
                BTreeMap::<CompressionPlacementId, Vec<DockerStructuralRecord<'_>>>::new();
            let assignments = locality.collate(source_cohort, home, &locality_records);
            let mut record_placements = vec![home; parsed_records.len()];
            for assignment in assignments {
                for index in assignment.record_indices() {
                    record_placements[index] = assignment.placement.placement_id;
                }
            }
            for (record, placement_id) in parsed_records.into_iter().zip(record_placements) {
                groups.entry(placement_id).or_default().push(record);
            }
            if groups.is_empty() {
                groups.insert(home, Vec::new());
            }
            let record_count = groups.values().map(Vec::len).sum::<usize>();
            let started = Instant::now();
            let compressed_groups = compress_groups(&groups, compressor)?;
            let structural_compression_time = started.elapsed();
            if block.verify {
                let mut decoded = decode_locality_payload(
                    &compressed_groups.payload,
                    compressed_groups.structural_bytes,
                    compressed_groups.dictionary_payload.as_deref(),
                )?;
                decoded.sort_unstable_by_key(|record| record.offset);
                let mut expected = groups.values().flatten().collect::<Vec<_>>();
                expected.sort_unstable_by_key(|record| record.offset);
                verify_decoded_records(&decoded, expected.into_iter())?;
            }
            (record_count, compressed_groups, structural_compression_time)
        } else {
            let record_count = parsed_records.len();
            let started = Instant::now();
            let compressed_groups = compress_single_group(home, &parsed_records, compressor)?;
            let structural_compression_time = started.elapsed();
            if block.verify {
                let decoded = decode_locality_payload(
                    &compressed_groups.payload,
                    compressed_groups.structural_bytes,
                    compressed_groups.dictionary_payload.as_deref(),
                )?;
                verify_decoded_records(&decoded, parsed_records.iter())?;
            }
            (record_count, compressed_groups, structural_compression_time)
        };
    let structural_bytes = compressed_groups.structural_bytes;
    let embedded_index_bytes = compressed_groups.embedded_index_bytes;
    let compressed = compressed_groups.payload;
    let dictionary_id = compressed_groups.dictionary_id;
    let result = BlockResult {
        ordinal: block.ordinal,
        source_offset: block.source_offset,
        input_bytes: u64::try_from(block.raw.len()).map_err(|error| error.to_string())?,
        source_bytes: accepted_source_bytes,
        record_count: u64::try_from(record_count).map_err(|error| error.to_string())?,
        rejected_records,
        rejected_bytes,
        structural_bytes,
        embedded_index_bytes,
        structural_stored_bytes: u64::try_from(compressed.len())
            .map_err(|error| error.to_string())?,
        pack_worker: 0,
        pack_offset: 0,
        payload_checksum: fnv1a64(&compressed),
        dictionary_id,
        query_index,
        structural_compression_time,
    };
    Ok((result, compressed))
}

fn verify_decoded_records<'a>(
    decoded: &[DecodedStructuralRecord],
    expected: impl ExactSizeIterator<Item = &'a DockerStructuralRecord<'a>>,
) -> Result<(), String> {
    if decoded.len() != expected.len()
        || decoded.iter().zip(expected).any(|(decoded, record)| {
            decoded.offset != record.offset
                || decoded.timestamp_unix_nanos != record.timestamp_unix_nanos
                || decoded.message.as_ref() != record.message.as_ref()
                || decoded.fields.len() != 1
                || decoded.fields[0].key.as_ref() != "docker.stream"
                || decoded.fields[0].value.as_ref() != record.stream
        })
    {
        return Err("structural first-block round trip failed".to_owned());
    }
    Ok(())
}

fn compress_single_group(
    placement_id: CompressionPlacementId,
    records: &[DockerStructuralRecord<'_>],
    compressor: &mut BenchmarkCompressor,
) -> Result<CompressedGroups, String> {
    let indexed = encode_indexed_structural_records(records).map_err(|error| error.to_string())?;
    let embedded_index_bytes =
        u64::try_from(indexed.embedded_index_bytes).map_err(|error| error.to_string())?;
    let structural = indexed.structural;
    let structural_bytes = u64::try_from(structural.len()).map_err(|error| error.to_string())?;
    let frame = compressor.compress_log_block(placement_id, structural)?;
    Ok(CompressedGroups {
        structural_bytes,
        embedded_index_bytes,
        payload: frame.payload,
        dictionary_id: frame.dictionary_id,
        dictionary_payload: frame.dictionary_payload,
    })
}

fn compress_groups(
    groups: &BTreeMap<CompressionPlacementId, Vec<DockerStructuralRecord<'_>>>,
    compressor: &mut BenchmarkCompressor,
) -> Result<CompressedGroups, String> {
    let mut frames = Vec::with_capacity(groups.len());
    let mut structural_total = 0u64;
    let mut embedded_index_total = 0u64;
    for (&placement_id, records) in groups {
        let indexed =
            encode_indexed_structural_records(records).map_err(|error| error.to_string())?;
        embedded_index_total = embedded_index_total.saturating_add(
            u64::try_from(indexed.embedded_index_bytes).map_err(|error| error.to_string())?,
        );
        let structural = indexed.structural;
        structural_total = structural_total
            .saturating_add(u64::try_from(structural.len()).map_err(|error| error.to_string())?);
        frames.push(compressor.compress_log_block(placement_id, structural)?);
    }
    if frames.len() == 1 {
        let frame = frames.pop().expect("one frame exists");
        return Ok(CompressedGroups {
            structural_bytes: structural_total,
            embedded_index_bytes: embedded_index_total,
            payload: frame.payload,
            dictionary_id: frame.dictionary_id,
            dictionary_payload: frame.dictionary_payload,
        });
    }
    if frames.iter().any(|frame| frame.dictionary_id.is_some()) {
        return Err(
            "real-time dictionaries cannot be combined with locality containers yet".to_owned(),
        );
    }

    let payload_capacity = frames.iter().fold(
        LOCALITY_CONTAINER_MAGIC.len() + size_of::<u32>(),
        |total, frame| {
            total
                .saturating_add(size_of::<u64>() * 2)
                .saturating_add(frame.payload.len())
        },
    );
    let mut payload = Vec::with_capacity(payload_capacity);
    payload.extend_from_slice(LOCALITY_CONTAINER_MAGIC);
    payload.extend_from_slice(
        &u32::try_from(frames.len())
            .map_err(|error| error.to_string())?
            .to_le_bytes(),
    );
    for frame in frames {
        payload.extend_from_slice(
            &u64::try_from(frame.structural_len)
                .map_err(|error| error.to_string())?
                .to_le_bytes(),
        );
        payload.extend_from_slice(
            &u64::try_from(frame.payload.len())
                .map_err(|error| error.to_string())?
                .to_le_bytes(),
        );
        payload.extend_from_slice(&frame.payload);
    }
    Ok(CompressedGroups {
        structural_bytes: structural_total,
        embedded_index_bytes: embedded_index_total,
        payload,
        dictionary_id: None,
        dictionary_payload: None,
    })
}

fn decode_locality_payload(
    payload: &[u8],
    structural_total: u64,
    dictionary: Option<&[u8]>,
) -> Result<Vec<DecodedStructuralRecord>, String> {
    if !payload.starts_with(LOCALITY_CONTAINER_MAGIC) {
        let mut decompressor =
            zstd::bulk::Decompressor::with_dictionary(dictionary.unwrap_or_default())
                .map_err(|error| error.to_string())?;
        let structural = decompressor
            .decompress(
                payload,
                usize::try_from(structural_total).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        return decode_structural_block(&structural).map_err(|error| error.to_string());
    }
    if dictionary.is_some() {
        return Err("a locality container cannot use one shared dictionary".to_owned());
    }

    let mut cursor = LOCALITY_CONTAINER_MAGIC.len();
    let count_end = cursor.saturating_add(size_of::<u32>());
    let frame_count = u32::from_le_bytes(
        payload
            .get(cursor..count_end)
            .ok_or("truncated locality frame count")?
            .try_into()
            .map_err(|_| "invalid locality frame count")?,
    );
    cursor = count_end;
    let mut decoded = Vec::new();
    let mut observed_structural = 0u64;
    for _ in 0..frame_count {
        let structural_len = read_container_u64(payload, &mut cursor)?;
        let compressed_len = read_container_u64(payload, &mut cursor)?;
        let compressed_len = usize::try_from(compressed_len).map_err(|error| error.to_string())?;
        let end = cursor
            .checked_add(compressed_len)
            .ok_or("locality frame length overflow")?;
        let frame = payload
            .get(cursor..end)
            .ok_or("truncated locality frame payload")?;
        cursor = end;
        let structural = zstd::bulk::decompress(
            frame,
            usize::try_from(structural_len).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        observed_structural = observed_structural.saturating_add(structural_len);
        decoded.extend(decode_structural_block(&structural).map_err(|error| error.to_string())?);
    }
    if cursor != payload.len() || observed_structural != structural_total {
        return Err("invalid locality frame container length".to_owned());
    }
    Ok(decoded)
}

fn read_container_u64(payload: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let end = cursor
        .checked_add(size_of::<u64>())
        .ok_or("locality frame cursor overflow")?;
    let value = u64::from_le_bytes(
        payload
            .get(*cursor..end)
            .ok_or("truncated locality frame header")?
            .try_into()
            .map_err(|_| "invalid locality frame header")?,
    );
    *cursor = end;
    Ok(value)
}

fn write_manifest(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<u64, Box<dyn Error>> {
    entries.sort_unstable_by_key(|entry| entry.ordinal);
    let path = output_dir.join("manifest.bin");
    let mut manifest = OpenOptions::new().write(true).create_new(true).open(path)?;
    manifest.write_all(b"SLOGPACK2")?;
    manifest.write_all(&u64::try_from(entries.len())?.to_le_bytes())?;
    for entry in entries {
        manifest.write_all(&u64::try_from(entry.ordinal)?.to_le_bytes())?;
        manifest.write_all(&entry.source_offset.to_le_bytes())?;
        manifest.write_all(&entry.input_bytes.to_le_bytes())?;
        manifest.write_all(&entry.source_bytes.to_le_bytes())?;
        manifest.write_all(&entry.record_count.to_le_bytes())?;
        manifest.write_all(&entry.structural_bytes.to_le_bytes())?;
        manifest.write_all(&u64::try_from(entry.pack_worker)?.to_le_bytes())?;
        manifest.write_all(&entry.pack_offset.to_le_bytes())?;
        manifest.write_all(&entry.structural_stored_bytes.to_le_bytes())?;
        manifest.write_all(&entry.payload_checksum.to_le_bytes())?;
    }
    manifest.sync_all()?;
    Ok(manifest.metadata()?.len())
}

fn encode_query_index(entries: &mut [BlockResult]) -> Result<Vec<u8>, Box<dyn Error>> {
    entries.sort_unstable_by_key(|entry| entry.ordinal);
    let blocks = entries
        .iter_mut()
        .filter_map(|entry| entry.query_index.take())
        .collect::<Vec<_>>();
    PersistentQueryIndex::from_blocks(blocks)?
        .encode_compressed(ZSTD_LEVEL)
        .map_err(Into::into)
}

fn write_query_index(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<u64, Box<dyn Error>> {
    let encoded = encode_query_index(entries)?;
    let path = output_dir.join("query-index.bin");
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    output.write_all(&encoded)?;
    output.sync_all()?;
    Ok(output.metadata()?.len())
}

fn write_dictionary_assignments(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<u64, Box<dyn Error>> {
    entries.sort_unstable_by_key(|entry| entry.ordinal);
    let mut runs = Vec::<(usize, usize, DictionaryId)>::new();
    for entry in entries.iter() {
        let Some(dictionary_id) = entry.dictionary_id else {
            continue;
        };
        if let Some((start, length, previous_id)) = runs.last_mut()
            && *previous_id == dictionary_id
            && start.saturating_add(*length) == entry.ordinal
        {
            *length = length.saturating_add(1);
        } else {
            runs.push((entry.ordinal, 1, dictionary_id));
        }
    }
    if runs.is_empty() {
        return Ok(0);
    }
    let path = output_dir.join("dictionary-assignments.bin");
    let mut assignments = OpenOptions::new().write(true).create_new(true).open(path)?;
    assignments.write_all(b"SLOGDICT2")?;
    assignments.write_all(&u64::try_from(runs.len())?.to_le_bytes())?;
    for (start, length, dictionary_id) in runs {
        assignments.write_all(&u64::try_from(start)?.to_le_bytes())?;
        assignments.write_all(&u64::try_from(length)?.to_le_bytes())?;
        assignments.write_all(&dictionary_id.get().to_le_bytes())?;
    }
    assignments.sync_all()?;
    Ok(assignments.metadata()?.len())
}

fn write_dictionaries(
    output_dir: &std::path::Path,
    snapshot: &shard_telemetry::DictionaryCatalogSnapshot,
) -> Result<u64, Box<dyn Error>> {
    let dictionaries = snapshot.dictionaries().collect::<Vec<_>>();
    if dictionaries.is_empty() {
        return Ok(0);
    }
    let directory = output_dir.join("dictionaries");
    std::fs::create_dir(&directory)?;
    let mut total = 0u64;
    for (dictionary_id, payload) in dictionaries {
        let path = directory.join(dictionary_file_name(dictionary_id));
        let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
        output.write_all(&payload)?;
        output.sync_all()?;
        total = total.saturating_add(u64::try_from(payload.len())?);
    }
    Ok(total)
}

fn dictionary_file_name(dictionary_id: DictionaryId) -> String {
    format!("{:032x}.zdict", dictionary_id.get())
}

fn verify_durable_output(output_dir: &std::path::Path) -> Result<u64, Box<dyn Error>> {
    let mut entries = read_manifest(output_dir)?;
    read_dictionary_assignments(output_dir, &mut entries)?;
    let query_index_path = output_dir.join("query-index.bin");
    if query_index_path.exists() {
        let query_index =
            PersistentQueryIndex::decode_compressed(&std::fs::read(query_index_path)?)?;
        for block in query_index.blocks() {
            let entry = entries
                .get(usize::try_from(block.block_ordinal)?)
                .ok_or("query index references an unknown manifest block")?;
            if entry.ordinal != usize::try_from(block.block_ordinal)?
                || entry.record_count != u64::from(block.record_count)
            {
                return Err("query index block metadata does not match the manifest".into());
            }
        }
    }
    for (ordinal, entry) in entries.iter().enumerate() {
        if entry.ordinal != ordinal {
            return Err("manifest block ordinals are not contiguous".into());
        }
    }
    for adjacent in entries.windows(2) {
        if adjacent[0]
            .source_offset
            .saturating_add(adjacent[0].input_bytes)
            != adjacent[1].source_offset
        {
            return Err("manifest source spans contain a gap or overlap".into());
        }
    }
    let mut packs = std::collections::HashMap::new();
    let sampled_ordinals = [
        entries.first().map(|entry| entry.ordinal),
        entries.get(entries.len() / 2).map(|entry| entry.ordinal),
        entries.last().map(|entry| entry.ordinal),
    ];
    for entry in &entries {
        let pack = match packs.entry(entry.pack_worker) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let worker_id = *entry.key();
                entry.insert(File::open(
                    output_dir.join(format!("worker-{worker_id:02}.pack")),
                )?)
            }
        };
        let mut payload = vec![0; usize::try_from(entry.structural_stored_bytes)?];
        pack.read_exact_at(&mut payload, entry.pack_offset)?;
        if fnv1a64(&payload) != entry.payload_checksum {
            return Err(format!("payload checksum mismatch for block {}", entry.ordinal).into());
        }
        if sampled_ordinals.contains(&Some(entry.ordinal)) {
            let dictionary = entry
                .dictionary_id
                .map(|dictionary_id| {
                    std::fs::read(
                        output_dir
                            .join("dictionaries")
                            .join(dictionary_file_name(dictionary_id)),
                    )
                })
                .transpose()?;
            let decoded =
                decode_locality_payload(&payload, entry.structural_bytes, dictionary.as_deref())?;
            if decoded.len() != usize::try_from(entry.record_count)? {
                return Err(format!(
                    "sampled block {} decoded {} records, expected {}",
                    entry.ordinal,
                    decoded.len(),
                    entry.record_count
                )
                .into());
            }
        }
    }
    Ok(u64::try_from(entries.len())?)
}

fn read_dictionary_assignments(
    output_dir: &std::path::Path,
    entries: &mut [BlockResult],
) -> Result<(), Box<dyn Error>> {
    let path = output_dir.join("dictionary-assignments.bin");
    if !path.exists() {
        return Ok(());
    }
    const HEADER_BYTES: usize = 17;
    const ENTRY_BYTES: usize = 32;
    let assignments = File::open(path)?;
    let mut header = [0; HEADER_BYTES];
    assignments.read_exact_at(&mut header, 0)?;
    if &header[..9] != b"SLOGDICT2" {
        return Err("dictionary assignments have invalid magic".into());
    }
    let run_count = usize::try_from(u64::from_le_bytes(header[9..17].try_into()?))?;
    let expected_bytes = u64::try_from(HEADER_BYTES)?
        .saturating_add(u64::try_from(ENTRY_BYTES)?.saturating_mul(u64::try_from(run_count)?));
    if assignments.metadata()?.len() != expected_bytes {
        return Err("dictionary assignment length does not match run count".into());
    }
    let mut encoded = [0; ENTRY_BYTES];
    for index in 0..run_count {
        let offset = u64::try_from(HEADER_BYTES)?
            .saturating_add(u64::try_from(ENTRY_BYTES)?.saturating_mul(u64::try_from(index)?));
        assignments.read_exact_at(&mut encoded, offset)?;
        let start = usize::try_from(u64::from_le_bytes(encoded[..8].try_into()?))?;
        let length = usize::try_from(u64::from_le_bytes(encoded[8..16].try_into()?))?;
        let end = start
            .checked_add(length)
            .ok_or("dictionary assignment run overflows")?;
        let assigned = entries
            .get_mut(start..end)
            .ok_or("dictionary assignment run exceeds manifest")?;
        if assigned.is_empty() || assigned.iter().any(|entry| entry.dictionary_id.is_some()) {
            return Err("invalid or overlapping dictionary assignment run".into());
        }
        let dictionary_id = DictionaryId::new(u128::from_le_bytes(encoded[16..].try_into()?));
        for entry in assigned {
            entry.dictionary_id = Some(dictionary_id);
        }
    }
    Ok(())
}

fn read_manifest(output_dir: &std::path::Path) -> Result<Vec<BlockResult>, Box<dyn Error>> {
    const HEADER_BYTES: usize = 17;
    const ENTRY_BYTES: usize = 80;

    let manifest = File::open(output_dir.join("manifest.bin"))?;
    let mut header = [0u8; HEADER_BYTES];
    manifest.read_exact_at(&mut header, 0)?;
    if &header[..9] != b"SLOGPACK2" {
        return Err("manifest has invalid magic".into());
    }
    let entry_count = usize::try_from(u64::from_le_bytes(header[9..17].try_into()?))?;
    let expected_bytes = u64::try_from(HEADER_BYTES)?
        .checked_add(
            u64::try_from(entry_count)?
                .checked_mul(u64::try_from(ENTRY_BYTES)?)
                .ok_or("manifest length overflow")?,
        )
        .ok_or("manifest length overflow")?;
    if manifest.metadata()?.len() != expected_bytes {
        return Err("manifest length does not match its entry count".into());
    }

    let mut entries = Vec::with_capacity(entry_count);
    let mut encoded = [0u8; ENTRY_BYTES];
    for index in 0..entry_count {
        let offset = u64::try_from(HEADER_BYTES)?
            .checked_add(
                u64::try_from(index)?
                    .checked_mul(u64::try_from(ENTRY_BYTES)?)
                    .ok_or("manifest offset overflow")?,
            )
            .ok_or("manifest offset overflow")?;
        manifest.read_exact_at(&mut encoded, offset)?;
        let mut cursor = 0usize;
        let mut next = || {
            let end = cursor + 8;
            let value = u64::from_le_bytes(
                encoded[cursor..end]
                    .try_into()
                    .expect("manifest field is eight bytes"),
            );
            cursor = end;
            value
        };
        entries.push(BlockResult {
            ordinal: usize::try_from(next())?,
            source_offset: next(),
            input_bytes: next(),
            source_bytes: next(),
            record_count: next(),
            rejected_records: 0,
            rejected_bytes: 0,
            structural_bytes: next(),
            embedded_index_bytes: 0,
            pack_worker: usize::try_from(next())?,
            pack_offset: next(),
            structural_stored_bytes: next(),
            payload_checksum: next(),
            dictionary_id: None,
            query_index: None,
            structural_compression_time: Duration::ZERO,
        });
    }
    Ok(entries)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn parse_canonical_docker_json<'a>(
    line: &'a [u8],
    message_cache: &mut DockerMessageCache<'a>,
    timestamp_cache: &mut DockerTimestampPrefixCache,
) -> Option<(Rc<str>, Cow<'a, str>, u64)> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if !line.starts_with(DOCKER_JSON_PREFIX) || !line.ends_with(b"\"}") {
        return None;
    }
    let timestamp_end = line.len().checked_sub(2)?;
    let earliest_t = timestamp_end.checked_sub(20)?;
    let latest_t = timestamp_end.checked_sub(10)?;
    let timestamp_t = earliest_t.checked_add(
        line.get(earliest_t..=latest_t)?
            .iter()
            .position(|byte| *byte == b'T')?,
    )?;
    let timestamp_start = timestamp_t.checked_sub(10)?;
    if line.get(timestamp_start.checked_sub(1)?) != Some(&b'"') {
        return None;
    }
    let timestamp = parse_ascii_docker_timestamp_cached(
        line.get(timestamp_start..timestamp_end)?,
        timestamp_cache,
    )?;
    let before_timestamp = line.get(..timestamp_start)?;
    let (message_end, stream) = if before_timestamp.ends_with(DOCKER_JSON_STDERR_SUFFIX) {
        (
            timestamp_start.checked_sub(DOCKER_JSON_STDERR_SUFFIX.len())?,
            "stderr",
        )
    } else if before_timestamp.ends_with(DOCKER_JSON_STDOUT_SUFFIX) {
        (
            timestamp_start.checked_sub(DOCKER_JSON_STDOUT_SUFFIX.len())?,
            "stdout",
        )
    } else {
        return None;
    };
    let raw_message = line.get(DOCKER_JSON_PREFIX.len()..message_end)?;
    let message = message_cache.decode(raw_message)?;
    Some((message, Cow::Borrowed(stream), timestamp))
}

fn docker_message_cache_slot(message: &[u8]) -> usize {
    let length = message.len();
    let first = sampled_message_u64(message, 0);
    let middle = sampled_message_u64(message, length.saturating_sub(8) / 2);
    let last = sampled_message_u64(message, length.saturating_sub(8));
    let mut hash = (length as u64)
        .wrapping_mul(0x9e37_79b1_85eb_ca87)
        .rotate_left(17);
    hash ^= first.wrapping_mul(0xc2b2_ae3d_27d4_eb4f);
    hash ^= middle.wrapping_mul(0x1656_67b1_9e37_79f9);
    hash ^= last.wrapping_mul(0x85eb_ca77_c2b2_ae63);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash as usize & (DOCKER_MESSAGE_CACHE_ENTRIES - 1)
}

fn sampled_message_u64(message: &[u8], start: usize) -> u64 {
    if let Some(sample) = message.get(start..start.saturating_add(8))
        && let Ok(sample) = <[u8; 8]>::try_from(sample)
    {
        return u64::from_le_bytes(sample);
    }
    let end = start.saturating_add(8).min(message.len());
    let mut bytes = [0; 8];
    bytes[..end.saturating_sub(start)].copy_from_slice(&message[start..end]);
    u64::from_le_bytes(bytes)
}

fn decode_common_json_message(raw: &[u8]) -> Option<Rc<str>> {
    if !raw.contains(&b'\\') {
        return std::str::from_utf8(raw).ok().map(Rc::from);
    }
    let mut decoded = Vec::with_capacity(raw.len());
    let mut cursor = 0usize;
    while let Some(&byte) = raw.get(cursor) {
        match byte {
            b'\\' => {
                cursor = cursor.checked_add(1)?;
                let escaped = *raw.get(cursor)?;
                if escaped == b'u' {
                    let (character, next_cursor) = decode_json_unicode_escape(raw, cursor)?;
                    let mut utf8 = [0; 4];
                    decoded.extend_from_slice(character.encode_utf8(&mut utf8).as_bytes());
                    cursor = next_cursor;
                    continue;
                }
                decoded.push(match escaped {
                    b'"' => b'"',
                    b'\\' => b'\\',
                    b'/' => b'/',
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    _ => return None,
                });
            }
            b'"' | 0x00..=0x1f => return None,
            _ => decoded.push(byte),
        }
        cursor = cursor.checked_add(1)?;
    }
    String::from_utf8(decoded).ok().map(Rc::from)
}

fn decode_json_unicode_escape(raw: &[u8], unicode_marker: usize) -> Option<(char, usize)> {
    let first = parse_json_hex_quad(raw, unicode_marker.checked_add(1)?)?;
    let mut next_cursor = unicode_marker.checked_add(5)?;
    let scalar = if (0xd800..=0xdbff).contains(&first) {
        if raw.get(next_cursor..next_cursor.checked_add(2)?)? != b"\\u" {
            return None;
        }
        let second = parse_json_hex_quad(raw, next_cursor.checked_add(2)?)?;
        if !(0xdc00..=0xdfff).contains(&second) {
            return None;
        }
        next_cursor = next_cursor.checked_add(6)?;
        0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
    } else if (0xdc00..=0xdfff).contains(&first) {
        return None;
    } else {
        u32::from(first)
    };
    char::from_u32(scalar).map(|character| (character, next_cursor))
}

fn parse_json_hex_quad(raw: &[u8], start: usize) -> Option<u16> {
    let mut value = 0u16;
    for byte in raw.get(start..start.checked_add(4)?)? {
        value = value.checked_mul(16)?.checked_add(u16::from(match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        }))?;
    }
    Some(value)
}

fn parse_docker_timestamp(input: &str) -> Result<u64, Box<dyn Error>> {
    let bytes = input.as_bytes();
    if let Some(timestamp) = parse_ascii_docker_timestamp(bytes) {
        return Ok(timestamp);
    }
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(format!("unsupported Docker timestamp: {input}").into());
    }
    let year = i64::try_from(parse_digits(&bytes[0..4])?)?;
    let month = u32::try_from(parse_digits(&bytes[5..7])?)?;
    let day = u32::try_from(parse_digits(&bytes[8..10])?)?;
    let hour = parse_digits(&bytes[11..13])?;
    let minute = parse_digits(&bytes[14..16])?;
    let second = parse_digits(&bytes[17..19])?;
    if !(1..=12).contains(&month) || day == 0 || hour >= 24 || minute >= 60 || second >= 60 {
        return Err(format!("invalid Docker timestamp: {input}").into());
    }
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!("month range was checked"),
    };
    if day > days_in_month {
        return Err(format!("invalid Docker timestamp: {input}").into());
    }
    let fraction = match &bytes[19..] {
        [b'Z'] => 0,
        [b'.', digits @ .., b'Z'] if !digits.is_empty() && digits.len() <= 9 => {
            let value = parse_digits(digits)?;
            value
                .checked_mul(10u64.pow(u32::try_from(9 - digits.len())?))
                .ok_or("timestamp fraction overflows")?
        }
        _ => return Err(format!("unsupported Docker timestamp timezone: {input}").into()),
    };
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return Err(format!("timestamp predates Unix epoch: {input}").into());
    }
    let seconds = u64::try_from(days)?
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
        .ok_or("timestamp seconds overflow")?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| "timestamp nanoseconds overflow".into())
}

fn parse_ascii_docker_timestamp(bytes: &[u8]) -> Option<u64> {
    parse_ascii_docker_timestamp_cached(bytes, &mut DockerTimestampPrefixCache::default())
}

fn parse_ascii_docker_timestamp_cached(
    bytes: &[u8],
    cache: &mut DockerTimestampPrefixCache,
) -> Option<u64> {
    if !(20..=30).contains(&bytes.len()) {
        return None;
    }
    let fraction = match bytes.get(19..) {
        Some([b'Z']) => 0,
        Some([b'.', digits @ .., b'Z']) if !digits.is_empty() && digits.len() <= 9 => {
            ascii_fraction_nanos(digits)?
        }
        _ => return None,
    };
    let prefix = bytes.get(..19)?;
    let base_nanos = if cache.initialized && cache.prefix.as_slice() == prefix {
        cache.base_nanos
    } else {
        let base_nanos = parse_ascii_docker_timestamp_prefix(prefix)?;
        cache.prefix.copy_from_slice(prefix);
        cache.base_nanos = base_nanos;
        cache.initialized = true;
        base_nanos
    };
    base_nanos.checked_add(fraction)
}

fn parse_ascii_docker_timestamp_prefix(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = i64::try_from(four_ascii_digits(bytes, 0)?).ok()?;
    let month = u32::try_from(two_ascii_digits(bytes, 5)?).ok()?;
    let day = u32::try_from(two_ascii_digits(bytes, 8)?).ok()?;
    let hour = two_ascii_digits(bytes, 11)?;
    let minute = two_ascii_digits(bytes, 14)?;
    let second = two_ascii_digits(bytes, 17)?;
    if !(1..=12).contains(&month) || day == 0 || hour >= 24 || minute >= 60 || second >= 60 {
        return None;
    }
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    };
    if day > days_in_month {
        return None;
    }
    let days = u64::try_from(days_from_civil(year, month, day)).ok()?;
    days.checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)?
        .checked_mul(1_000_000_000)
}

fn two_ascii_digits(bytes: &[u8], start: usize) -> Option<u64> {
    let high = bytes.get(start)?.wrapping_sub(b'0');
    let low = bytes.get(start.checked_add(1)?)?.wrapping_sub(b'0');
    (high < 10 && low < 10).then_some(u64::from(high) * 10 + u64::from(low))
}

fn four_ascii_digits(bytes: &[u8], start: usize) -> Option<u64> {
    let high = two_ascii_digits(bytes, start)?;
    let low = two_ascii_digits(bytes, start.checked_add(2)?)?;
    Some(high * 100 + low)
}

fn ascii_fraction_nanos(bytes: &[u8]) -> Option<u64> {
    let digit = |index: usize| {
        let digit = bytes.get(index)?.wrapping_sub(b'0');
        (digit < 10).then_some(u64::from(digit))
    };
    match bytes.len() {
        1 => Some(digit(0)? * 100_000_000),
        2 => Some(two_ascii_digits(bytes, 0)? * 10_000_000),
        3 => Some((two_ascii_digits(bytes, 0)? * 10 + digit(2)?) * 1_000_000),
        4 => Some(four_ascii_digits(bytes, 0)? * 100_000),
        5 => Some((four_ascii_digits(bytes, 0)? * 10 + digit(4)?) * 10_000),
        6 => Some((four_ascii_digits(bytes, 0)? * 100 + two_ascii_digits(bytes, 4)?) * 1_000),
        7 => Some(
            (four_ascii_digits(bytes, 0)? * 1_000 + two_ascii_digits(bytes, 4)? * 10 + digit(6)?)
                * 100,
        ),
        8 => Some(eight_ascii_digits(bytes)? * 10),
        9 => Some(eight_ascii_digits(bytes)? * 10 + digit(8)?),
        _ => None,
    }
}

#[inline]
fn eight_ascii_digits(bytes: &[u8]) -> Option<u64> {
    let ascii = u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
    let lower = ascii.wrapping_sub(0x3030_3030_3030_3030);
    let upper = ascii.wrapping_add(0x4646_4646_4646_4646);
    if (lower | upper) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let digits = ascii & 0x0f0f_0f0f_0f0f_0f0f;
    let pairs = ((digits & 0x00ff_00ff_00ff_00ff) * 10) + ((digits >> 8) & 0x00ff_00ff_00ff_00ff);
    let quads = ((pairs & 0x0000_ffff_0000_ffff) * 100) + ((pairs >> 16) & 0x0000_ffff_0000_ffff);
    Some((quads & 0x0000_0000_ffff_ffff) * 10_000 + (quads >> 32))
}

fn parse_digits(bytes: &[u8]) -> Result<u64, Box<dyn Error>> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err("timestamp contains a non-digit".into());
    }
    bytes.iter().try_fold(0u64, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or_else(|| "timestamp number overflows".into())
    })
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn merge_locality_stats(
    aggregate: &mut CompressionLocalityStats,
    worker: CompressionLocalityStats,
) {
    aggregate.observations = aggregate.observations.saturating_add(worker.observations);
    aggregate.blocks_scored = aggregate.blocks_scored.saturating_add(worker.blocks_scored);
    aggregate.blocks_split = aggregate.blocks_split.saturating_add(worker.blocks_split);
    aggregate.subblocks_created = aggregate
        .subblocks_created
        .saturating_add(worker.subblocks_created);
    aggregate.split_explorations_suppressed = aggregate
        .split_explorations_suppressed
        .saturating_add(worker.split_explorations_suppressed);
    aggregate.base_placements = aggregate
        .base_placements
        .saturating_add(worker.base_placements);
    aggregate.collated_placements = aggregate
        .collated_placements
        .saturating_add(worker.collated_placements);
    aggregate.records_reassigned = aggregate
        .records_reassigned
        .saturating_add(worker.records_reassigned);
    aggregate.bytes_reassigned = aggregate
        .bytes_reassigned
        .saturating_add(worker.bytes_reassigned);
    aggregate.active_compression_shards = aggregate
        .active_compression_shards
        .saturating_add(worker.active_compression_shards);
    aggregate.max_internal_variance_q8 = aggregate
        .max_internal_variance_q8
        .max(worker.max_internal_variance_q8);
    aggregate.handoff_membership_bytes = aggregate
        .handoff_membership_bytes
        .saturating_add(worker.handoff_membership_bytes);
    aggregate.allocated_state_bytes = aggregate
        .allocated_state_bytes
        .saturating_add(worker.allocated_state_bytes);
}

fn render_report(settings: &Settings, benchmark: &Benchmark) -> String {
    let mut report = String::from("shard-telemetry Docker JSON structural benchmark\n");
    report.push_str(&format!("input: {}\n", settings.input.display()));
    if let Some(output_dir) = &settings.output_dir {
        report.push_str(&format!("output directory: {}\n", output_dir.display()));
    }
    report.push_str(&format!(
        "complete-line input bytes: {}\n",
        benchmark.input_bytes
    ));
    report.push_str(&format!(
        "leading partial bytes discarded: {}\n",
        benchmark.leading_discarded_bytes
    ));
    report.push_str(&format!("source bytes: {}\n", benchmark.source_bytes));
    report.push_str(&format!(
        "rejected complete records: {}\n",
        benchmark.rejected_records
    ));
    report.push_str(&format!(
        "rejected complete-record bytes: {}\n",
        benchmark.rejected_bytes
    ));
    report.push_str(&format!("block target: {}\n", settings.block_bytes));
    report.push_str(&format!("workers: {}\n", benchmark.workers));
    report.push_str(&format!(
        "locality routing: {}\n",
        if settings.locality_routing {
            "enabled"
        } else {
            "disabled"
        }
    ));
    report.push_str(&format!(
        "real-time dictionary: {}\n",
        if settings.realtime_dictionary {
            "enabled"
        } else {
            "disabled"
        }
    ));
    report.push_str(&format!(
        "persistent query index: {}\n",
        if settings.persistent_query_index {
            "enabled"
        } else {
            "disabled"
        }
    ));
    report.push_str(&format!("blocks: {}\n", benchmark.blocks));
    report.push_str(&format!("records: {}\n", benchmark.records));
    report.push_str(&format!(
        "structural payload before zstd: {}\n",
        storage_line(benchmark.structural_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!(
        "embedded compression-derived index before zstd: {}\n",
        storage_line(benchmark.embedded_index_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!(
        "structural payload zstd-1: {}\n",
        storage_line(benchmark.structural_stored_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!("manifest bytes: {}\n", benchmark.manifest_bytes));
    report.push_str(&format!(
        "persistent term/field index bytes: {}\n",
        benchmark.query_index_bytes
    ));
    report.push_str(&format!(
        "compression dictionary bytes: {}\n",
        benchmark.dictionary_bytes
    ));
    let durable_bytes = benchmark
        .structural_stored_bytes
        .saturating_add(benchmark.manifest_bytes)
        .saturating_add(benchmark.query_index_bytes)
        .saturating_add(benchmark.dictionary_bytes);
    report.push_str(&format!(
        "durable pack plus manifest: {}\n",
        storage_line(durable_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!(
        "structural zstd-1 CPU-time throughput: {:.2} MiB/s\n",
        throughput_mib(
            benchmark.structural_bytes,
            benchmark.structural_compression_time
        )
    ));
    report.push_str(&format!(
        "durable end-to-end ingest throughput: {:.2} MiB/s\n",
        throughput_mib(benchmark.source_bytes, benchmark.elapsed)
    ));
    report.push_str(&format!(
        "ingest elapsed seconds: {:.6}\n",
        benchmark.elapsed.as_secs_f64()
    ));
    report.push_str(&format!(
        "temperature placement distribution collated/base: {}/{}\n",
        benchmark.locality.collated_placements, benchmark.locality.base_placements
    ));
    report.push_str(&format!(
        "locality fallback rate: {:.6}\n",
        benchmark.locality.base_placements as f64 / benchmark.locality.observations.max(1) as f64
    ));
    report.push_str(&format!(
        "blocks scored/split/sub-blocks: {}/{}/{}\n",
        benchmark.locality.blocks_scored,
        benchmark.locality.blocks_split,
        benchmark.locality.subblocks_created
    ));
    report.push_str(&format!(
        "split explorations suppressed: {}\n",
        benchmark.locality.split_explorations_suppressed
    ));
    report.push_str(&format!(
        "reassigned records/bytes: {}/{}\n",
        benchmark.locality.records_reassigned, benchmark.locality.bytes_reassigned
    ));
    report.push_str(&format!(
        "active compression shards: {}\n",
        benchmark.locality.active_compression_shards
    ));
    report.push_str(&format!(
        "maximum internal variance Q8: {}\n",
        benchmark.locality.max_internal_variance_q8
    ));
    report.push_str(&format!(
        "bytes-handoff membership bytes: {}\n",
        benchmark.locality.handoff_membership_bytes
    ));
    report.push_str(&format!(
        "collator state bytes across workers: {}\n",
        benchmark.locality.allocated_state_bytes
    ));
    if settings.realtime_dictionary {
        let stats = benchmark.dictionary_stats;
        report.push_str(&format!(
            "dictionary observed/sampled/dropped blocks: {}/{}/{}\n",
            stats.observed_blocks,
            stats.observed_blocks.saturating_sub(stats.dropped_blocks),
            stats.dropped_blocks
        ));
        report.push_str(&format!(
            "dictionary placement budget rejections/max tracked: {}/{}\n",
            stats.placement_budget_rejections, stats.max_tracked_placements
        ));
        report.push_str(&format!(
            "dictionary observed/sampled bytes: {}/{}\n",
            stats.observed_bytes, stats.sampled_bytes
        ));
        report.push_str(&format!(
            "dictionary training runs/failures/rejections/publications: {}/{}/{}/{}\n",
            stats.training_runs,
            stats.training_failures,
            stats.candidates_rejected,
            stats.dictionaries_published
        ));
        report.push_str(&format!(
            "dictionary holdout baseline/candidate bytes: {}/{}\n",
            stats.holdout_baseline_bytes, stats.holdout_candidate_bytes
        ));
        report.push_str(&format!(
            "dictionary training/evaluation seconds: {:.6}/{:.6}\n",
            stats.training_nanos as f64 / 1_000_000_000.0,
            stats.evaluation_nanos as f64 / 1_000_000_000.0
        ));
    }
    if benchmark.verified_blocks > 0 {
        report.push_str(&format!(
            "post-ingest verification: {} block checksums plus first/middle/last decode in {:.3} seconds\n",
            benchmark.verified_blocks,
            benchmark.verification_elapsed.as_secs_f64()
        ));
    }
    report.push_str(
        "note: elapsed time includes deterministic line-boundary discovery, Docker JSON parsing, structural encoding, zstd-1 compression, pack writes, sync_all, manifest creation, and manifest sync.\n",
    );
    report.push_str(
        "note: post-ingest checksum and sampled decode verification is intentionally excluded from ingest throughput.\n",
    );
    report.push_str(
        "note: Docker JSON is normalized into body, RFC3339 timestamp, and docker.stream before structural encoding; the durable representation retains those typed semantics but not JSON wrapper bytes.\n",
    );
    report
}

fn storage_line(stored_bytes: u64, source_bytes: u64) -> String {
    let ratio = source_bytes as f64 / stored_bytes.max(1) as f64;
    let retained = stored_bytes as f64 * 100.0 / source_bytes.max(1) as f64;
    format!("{stored_bytes} bytes ({ratio:.2}x, {retained:.2}% of source)")
}

fn throughput_mib(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_docker_rfc3339_nanos() {
        assert_eq!(
            parse_docker_timestamp("1970-01-01T00:00:00.123Z").expect("timestamp parses"),
            123_000_000
        );
        assert_eq!(
            parse_docker_timestamp("2024-02-29T01:02:03.123456789Z")
                .expect("leap-day timestamp parses"),
            1_709_168_523_123_456_789
        );
    }

    #[test]
    fn canonical_docker_parser_reuses_exact_decoded_messages() {
        let first = br#"{"log":"quoted \"value\" and newline\n","stream":"stderr","time":"2024-02-29T01:02:03.123456789Z"}"#;
        let second = br#"{"log":"quoted \"value\" and newline\n","stream":"stderr","time":"2024-02-29T01:02:04.123456789Z"}"#;
        let mut cache = DockerMessageCache::new();
        let mut timestamp_cache = DockerTimestampPrefixCache::default();
        let (first_message, first_stream, first_timestamp) =
            parse_canonical_docker_json(first, &mut cache, &mut timestamp_cache)
                .expect("first line uses fast path");
        let (second_message, second_stream, second_timestamp) =
            parse_canonical_docker_json(second, &mut cache, &mut timestamp_cache)
                .expect("second line uses fast path");
        assert_eq!(first_message.as_ref(), "quoted \"value\" and newline\n");
        assert_eq!(first_stream, "stderr");
        assert_eq!(second_stream, "stderr");
        assert_eq!(second_timestamp - first_timestamp, 1_000_000_000);
        assert!(Rc::ptr_eq(&first_message, &second_message));
    }

    #[test]
    fn unicode_json_escapes_use_the_cached_fast_path() {
        let line = br#"{"log":"snowman \u2603 rocket \uD83D\uDE80\n","stream":"stdout","time":"2024-02-29T01:02:03.123456789Z"}"#;
        let mut cache = DockerMessageCache::new();
        let mut timestamp_cache = DockerTimestampPrefixCache::default();
        let (message, stream, _) =
            parse_canonical_docker_json(line, &mut cache, &mut timestamp_cache)
                .expect("Unicode line uses fast path");
        let parsed: DockerJsonLine<'_> = serde_json::from_slice(line).expect("serde parses line");
        assert_eq!(message.as_ref(), parsed.log);
        assert_eq!(message.as_ref(), "snowman \u{2603} rocket \u{1f680}\n");
        assert_eq!(stream, "stdout");
    }

    #[test]
    fn timestamp_parser_handles_fraction_widths_and_date_rollover() {
        let before_midnight = parse_ascii_docker_timestamp(b"2024-02-29T23:59:59.1Z")
            .expect("first timestamp parses");
        let after_midnight = parse_ascii_docker_timestamp(b"2024-03-01T00:00:00.000000001Z")
            .expect("rollover timestamp parses");
        assert_eq!(after_midnight - before_midnight, 900_000_001);
        assert_eq!(
            parse_ascii_docker_timestamp(b"2024-03-01T00:00:00Z")
                .expect("whole-second timestamp parses"),
            after_midnight - 1
        );

        let mut cache = DockerTimestampPrefixCache::default();
        let first = parse_ascii_docker_timestamp_cached(b"2024-03-01T00:00:01.1Z", &mut cache)
            .expect("cached first timestamp parses");
        let second =
            parse_ascii_docker_timestamp_cached(b"2024-03-01T00:00:01.123456789Z", &mut cache)
                .expect("same-second timestamp parses");
        assert_eq!(second - first, 23_456_789);
        assert_eq!(cache.prefix.as_slice(), b"2024-03-01T00:00:01");
    }

    #[test]
    fn fractional_timestamp_parser_scales_every_width() {
        for (digits, expected) in [
            (b"1".as_slice(), 100_000_000),
            (b"12".as_slice(), 120_000_000),
            (b"123".as_slice(), 123_000_000),
            (b"1234".as_slice(), 123_400_000),
            (b"12345".as_slice(), 123_450_000),
            (b"123456".as_slice(), 123_456_000),
            (b"1234567".as_slice(), 123_456_700),
            (b"12345678".as_slice(), 123_456_780),
            (b"123456789".as_slice(), 123_456_789),
        ] {
            assert_eq!(ascii_fraction_nanos(digits), Some(expected));
        }
        assert_eq!(ascii_fraction_nanos(b""), None);
        assert_eq!(ascii_fraction_nanos(b"1234567890"), None);
        assert_eq!(ascii_fraction_nanos(b"1234x"), None);
        assert_eq!(eight_ascii_digits(b"00000000"), Some(0));
        assert_eq!(eight_ascii_digits(b"99999999"), Some(99_999_999));
        for index in 0..8 {
            for byte in u8::MIN..=u8::MAX {
                if byte.is_ascii_digit() {
                    continue;
                }
                let mut invalid = *b"12345678";
                invalid[index] = byte;
                assert_eq!(eight_ascii_digits(&invalid), None);
            }
        }
    }

    #[test]
    fn canonical_parser_finds_every_supported_timestamp_width() {
        for timestamp in [
            "2024-03-01T00:00:00Z",
            "2024-03-01T00:00:00.1Z",
            "2024-03-01T00:00:00.12345Z",
            "2024-03-01T00:00:00.123456789Z",
        ] {
            let line = format!(
                "{{\"log\":\"contains T safely\\\\n\",\"stream\":\"stderr\",\"time\":\"{timestamp}\"}}"
            );
            let mut cache = DockerMessageCache::new();
            let mut timestamp_cache = DockerTimestampPrefixCache::default();
            let (_, stream, parsed_timestamp) =
                parse_canonical_docker_json(line.as_bytes(), &mut cache, &mut timestamp_cache)
                    .expect("canonical line uses bounded timestamp probe");
            assert_eq!(stream, "stderr");
            assert_eq!(
                parsed_timestamp,
                parse_docker_timestamp(timestamp).expect("reference timestamp parses")
            );
        }
    }

    #[test]
    fn byte_count_parser_uses_iec_units() {
        assert_eq!(
            parse_byte_count("2GiB").expect("size parses"),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn deterministic_spans_skip_partial_edges_without_gaps() {
        let path = std::env::temp_dir().join(format!(
            "shard-telemetry-spans-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after epoch")
                .as_nanos()
        ));
        let complete = concat!(
            "{\"log\":\"alpha 1\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000001Z\"}\n",
            "{\"log\":\"alpha 2\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000002Z\"}\n",
            "{\"log\":\"alpha 3\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000003Z\"}\n"
        );
        let input = format!("partial prefix\n{complete}partial suffix");
        std::fs::write(&path, input).expect("fixture writes");
        let settings = Settings {
            input: path.clone(),
            report_path: None,
            limit_bytes: u64::MAX,
            block_bytes: 97,
            workers: 2,
            output_dir: None,
            locality_routing: true,
            realtime_dictionary: false,
            persistent_query_index: false,
        };

        let (source_start, spans) = build_block_spans(&settings).expect("spans build");
        let (_, repeated) = build_block_spans(&settings).expect("spans repeat");
        assert_eq!(spans, repeated);
        assert_eq!(source_start, "partial prefix\n".len() as u64);
        assert_eq!(spans.first().expect("first span").start, source_start);
        for (ordinal, span) in spans.iter().enumerate() {
            assert_eq!(span.ordinal, ordinal);
        }
        for adjacent in spans.windows(2) {
            assert_eq!(
                adjacent[0].start + adjacent[0].length as u64,
                adjacent[1].start
            );
        }

        let file = File::open(&path).expect("fixture opens");
        let mut recovered = Vec::new();
        for span in spans {
            let mut bytes = vec![0; span.length];
            file.read_exact_at(&mut bytes, span.start)
                .expect("span reads");
            recovered.extend_from_slice(&bytes);
        }
        assert_eq!(recovered, complete.as_bytes());
        std::fs::remove_file(path).expect("fixture removes");
    }

    #[test]
    fn payload_checksum_detects_changes() {
        let checksum = fnv1a64(b"durable payload");
        assert_eq!(checksum, fnv1a64(b"durable payload"));
        assert_ne!(checksum, fnv1a64(b"durable payloaD"));
    }

    #[test]
    fn dictionary_assignments_are_sparse_run_length_encoded() {
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-dictionary-runs-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&directory).expect("fixture directory creates");
        let make_entry = |ordinal, dictionary_id| BlockResult {
            ordinal,
            source_offset: 0,
            input_bytes: 1,
            source_bytes: 1,
            record_count: 1,
            rejected_records: 0,
            rejected_bytes: 0,
            structural_bytes: 1,
            embedded_index_bytes: 0,
            structural_stored_bytes: 1,
            pack_worker: 0,
            pack_offset: 0,
            payload_checksum: 0,
            dictionary_id,
            query_index: None,
            structural_compression_time: Duration::ZERO,
        };
        let first_dictionary = DictionaryId::new(7);
        let second_dictionary = DictionaryId::new(8);
        let mut entries = vec![
            make_entry(0, None),
            make_entry(1, Some(first_dictionary)),
            make_entry(2, Some(first_dictionary)),
            make_entry(3, None),
            make_entry(4, Some(second_dictionary)),
        ];
        let bytes =
            write_dictionary_assignments(&directory, &mut entries).expect("assignments write");
        assert_eq!(bytes, 17 + 2 * 32);

        let mut decoded = (0..entries.len())
            .map(|ordinal| make_entry(ordinal, None))
            .collect::<Vec<_>>();
        read_dictionary_assignments(&directory, &mut decoded).expect("assignments read");
        assert_eq!(
            decoded
                .iter()
                .map(|entry| entry.dictionary_id)
                .collect::<Vec<_>>(),
            vec![
                None,
                Some(first_dictionary),
                Some(first_dictionary),
                None,
                Some(second_dictionary)
            ]
        );
        std::fs::remove_dir_all(directory).expect("fixture directory removes");
    }

    #[test]
    fn benchmark_compressor_adopts_a_validated_realtime_dictionary() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let config = RealtimeDictionaryConfig {
            max_block_sample_bytes: 1024,
            training_sample_bytes: 8 * 1024,
            dictionary_bytes: 1024,
            holdout_blocks: 8,
            queue_blocks: 64,
            max_placements: 4,
            min_net_savings_bytes: 1,
            min_net_savings_bps: 1,
            retrain_after_bytes: u64::MAX,
        };
        let trainer = RealtimeDictionaryTrainer::start(config, ZSTD_LEVEL, Arc::clone(&catalog))
            .expect("trainer starts");
        let mut compressor = BenchmarkCompressor::new(Some(catalog), Some(trainer.observer()))
            .expect("compressor starts");
        let placement_id = CompressionPlacementId::new(88);
        let sample = |index: u64| {
            let mut state = 0x4d59_5df4_d0f3_3173u64;
            let mut bytes = Vec::with_capacity(1024);
            for _ in 0..512 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.push(state as u8);
            }
            bytes.extend_from_slice(format!(" unique request suffix {index:020}").as_bytes());
            while bytes.len() < 1024 {
                bytes.push(index.wrapping_mul(31).wrapping_add(bytes.len() as u64) as u8);
            }
            bytes
        };
        for index in 0..16 {
            let structural = sample(index);
            compressor
                .compress_log_block(placement_id, structural)
                .expect("training block compresses");
        }
        trainer.flush().expect("trainer flushes");
        assert_eq!(trainer.stats().dictionaries_published, 1);

        let expected = sample(100);
        let compressed = compressor
            .compress_log_block(placement_id, expected.clone())
            .expect("dictionary block compresses");
        assert!(compressed.dictionary_id.is_some());
        let dictionary = compressed
            .dictionary_payload
            .expect("dictionary payload is retained");
        let decoded = zstd::bulk::Decompressor::with_dictionary(&dictionary)
            .expect("decompressor opens")
            .decompress(&compressed.payload, expected.len())
            .expect("dictionary frame decompresses");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn locality_container_round_trips_multiple_ordered_placement_groups() {
        let mut groups = BTreeMap::new();
        groups.insert(
            CompressionPlacementId::new(1),
            vec![DockerStructuralRecord {
                offset: LogicalOffset::new(0),
                timestamp_unix_nanos: 1,
                message: Rc::from("alpha request 1"),
                stream: Cow::Borrowed("stderr"),
            }],
        );
        groups.insert(
            CompressionPlacementId::new(2),
            vec![DockerStructuralRecord {
                offset: LogicalOffset::new(1),
                timestamp_unix_nanos: 2,
                message: Rc::from("beta request 2"),
                stream: Cow::Borrowed("stdout"),
            }],
        );
        let mut compressor = BenchmarkCompressor::new(None, None).expect("compressor");
        let compressed = compress_groups(&groups, &mut compressor).expect("groups compress");
        assert_eq!(compressed.dictionary_id, None);
        assert!(compressed.payload.starts_with(LOCALITY_CONTAINER_MAGIC));
        let mut decoded =
            decode_locality_payload(&compressed.payload, compressed.structural_bytes, None)
                .expect("container decodes");
        decoded.sort_unstable_by_key(|record| record.offset);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].message.as_ref(), "alpha request 1");
        assert_eq!(decoded[1].message.as_ref(), "beta request 2");
        assert_eq!(decoded[0].fields[0].value.as_ref(), "stderr");
        assert_eq!(decoded[1].fields[0].value.as_ref(), "stdout");
    }

    #[test]
    fn malformed_complete_records_are_counted_and_skipped() {
        let first = b"{\"log\":\"one\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000001Z\"}\n";
        let malformed = b"{not-json}\n";
        let second = b"{\"log\":\"two\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000002Z\"}\n";
        let mut raw = Vec::new();
        raw.extend_from_slice(first);
        raw.extend_from_slice(malformed);
        raw.extend_from_slice(second);
        let mut compressor = BenchmarkCompressor::new(None, None).expect("compressor");
        let mut locality = CompressionBlockCollator::new(
            CompressionLocalityConfig {
                enabled: false,
                ..CompressionLocalityConfig::default()
            },
            8 * 1024 * 1024,
        )
        .expect("collator config validates");

        let (result, payload) = process_block(
            RawBlock {
                ordinal: 0,
                source_offset: 0,
                raw: &raw,
                verify: true,
            },
            &mut compressor,
            &mut locality,
            false,
        )
        .expect("block processes");

        assert_eq!(result.record_count, 2);
        assert_eq!(result.rejected_records, 1);
        assert_eq!(result.rejected_bytes, malformed.len() as u64);
        assert_eq!(
            result.source_bytes,
            u64::try_from(first.len() + second.len()).expect("fixture fits")
        );
        let structural = zstd::bulk::decompress(&payload, result.structural_bytes as usize)
            .expect("decompresses");
        let decoded = decode_structural_block(&structural).expect("decodes");
        assert_eq!(
            decoded[1].offset.get(),
            u64::try_from(first.len() + malformed.len()).expect("fixture fits")
        );
    }
}
