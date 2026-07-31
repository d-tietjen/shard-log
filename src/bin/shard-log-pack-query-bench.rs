use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use shard_log::{
    CaseSensitivity, DecodedStructuralRecord, LogPredicate, LogQuery, PersistentQueryIndex,
    QueryHit, QuerySort, decode_structural_records,
};
use shard_stream_core::{LogicalPartitionId, TopicId, TopicPartition};

const MANIFEST_HEADER_BYTES: usize = 17;
const MANIFEST_ENTRY_BYTES: usize = 80;
const DEFAULT_ITERATIONS: usize = 100;
const QUERY_PARTITION: TopicPartition =
    TopicPartition::new(TopicId::new(0), LogicalPartitionId::new(0));

struct Measurement {
    total: Duration,
    samples: Vec<Duration>,
}

struct Settings {
    output_dir: PathBuf,
    terms: Vec<String>,
    fields: Vec<(String, String)>,
    limit: usize,
    newest_first: bool,
    iterations: usize,
    cold_iterations: usize,
    workers: usize,
    contains: Vec<String>,
    regexes: Vec<String>,
    emit_results: Option<PathBuf>,
}

struct QueryExecution {
    records: Vec<DecodedStructuralRecord>,
    candidate_hits: usize,
    candidate_blocks: usize,
}

struct BlockExecution {
    records: Vec<DecodedStructuralRecord>,
    candidate_hits: usize,
}

#[derive(Clone)]
struct ManifestEntry {
    ordinal: u32,
    structural_bytes: usize,
    pack_worker: usize,
    pack_offset: u64,
    stored_bytes: usize,
    payload_checksum: u64,
}

struct PackReader {
    output_dir: PathBuf,
    entries: Arc<[ManifestEntry]>,
    packs: HashMap<usize, File>,
    decompressor: zstd::bulk::Decompressor<'static>,
}

impl PackReader {
    fn open(output_dir: &Path) -> Result<Self, Box<dyn Error>> {
        let entries = Arc::from(read_manifest(output_dir)?);
        Self::from_shared(output_dir.to_path_buf(), entries)
    }

    fn from_shared(
        output_dir: PathBuf,
        entries: Arc<[ManifestEntry]>,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            output_dir,
            entries,
            packs: HashMap::new(),
            decompressor: zstd::bulk::Decompressor::new()?,
        })
    }

    fn read_structural(&mut self, block_ordinal: u32) -> Result<Vec<u8>, Box<dyn Error>> {
        let entry = self
            .entries
            .get(usize::try_from(block_ordinal)?)
            .ok_or("query hit references an unknown manifest block")?;
        if entry.ordinal != block_ordinal {
            return Err("manifest block ordinal mismatch".into());
        }
        let pack = match self.packs.entry(entry.pack_worker) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let worker = *entry.key();
                entry.insert(File::open(
                    self.output_dir.join(format!("worker-{worker:02}.pack")),
                )?)
            }
        };
        let mut payload = vec![0; entry.stored_bytes];
        pack.read_exact_at(&mut payload, entry.pack_offset)?;
        if fnv1a64(&payload) != entry.payload_checksum {
            return Err("query payload checksum mismatch".into());
        }
        Ok(self
            .decompressor
            .decompress(&payload, entry.structural_bytes)?)
    }

    #[cfg(target_os = "linux")]
    fn evict_pack_cache(&mut self) -> Result<(), Box<dyn Error>> {
        use rustix::fs::{Advice, fadvise};

        let workers = self
            .entries
            .iter()
            .map(|entry| entry.pack_worker)
            .collect::<HashSet<_>>();
        for worker in workers {
            let pack = match self.packs.entry(worker) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(File::open(
                    self.output_dir.join(format!("worker-{worker:02}.pack")),
                )?),
            };
            fadvise(pack, 0, None, Advice::DontNeed)?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn evict_pack_cache(&mut self) -> Result<(), Box<dyn Error>> {
        Err("cold pack-cache eviction is supported only on Linux".into())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(settings.workers)
        .build_global()?;
    let load_started = Instant::now();
    let index = PersistentQueryIndex::decode_compressed(&std::fs::read(
        settings.output_dir.join("query-index.bin"),
    )?)?;
    let index_load_elapsed = load_started.elapsed();
    let mut query = LogQuery::new(QUERY_PARTITION).with_limit(settings.limit);
    if settings.newest_first {
        query = query.newest_first();
    }
    for term in &settings.terms {
        query = query.with_term(term.as_str());
    }
    for (key, value) in &settings.fields {
        query = query.with_field(key.as_str(), value.as_str());
    }
    for value in &settings.contains {
        query = query.with_predicate(LogPredicate::message_contains(value.as_str()));
    }
    for pattern in &settings.regexes {
        query = query.with_predicate(LogPredicate::message_regex(
            pattern.as_str(),
            CaseSensitivity::Sensitive,
        )?);
    }

    let plan_elapsed = if query.requires_post_decode() {
        benchmark(settings.iterations, || {
            black_box(index.candidate_blocks(black_box(&query)));
            Ok(())
        })?
    } else {
        benchmark(settings.iterations, || {
            black_box(index.candidate_hits(black_box(&query)));
            Ok(())
        })?
    };

    let mut reader = PackReader::open(&settings.output_dir)?;
    let warm_execution = execute_query(&index, &mut reader, &query, settings.workers)?;
    verify_matches(&warm_execution.records, &query)?;
    if let Some(path) = &settings.emit_results {
        emit_results(path, &warm_execution.records)?;
    }
    let materialize_elapsed = benchmark_query(
        settings.iterations,
        &index,
        &mut reader,
        &query,
        settings.workers,
        false,
    )?;
    let cold_elapsed = (settings.cold_iterations > 0)
        .then(|| {
            benchmark_query(
                settings.cold_iterations,
                &index,
                &mut reader,
                &query,
                settings.workers,
                true,
            )
        })
        .transpose()?;

    let cached_elapsed = if query.requires_post_decode() {
        None
    } else {
        let warm_hits = index.candidate_hits(&query);
        if warm_hits.len() > settings.limit {
            return Err("posting-only planner exceeded the query limit".into());
        }
        let mut structural_cache = BTreeMap::<u32, Vec<u8>>::new();
        for block_ordinal in warm_hits.iter().map(|hit| hit.block_ordinal) {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                structural_cache.entry(block_ordinal)
            {
                entry.insert(reader.read_structural(block_ordinal)?);
            }
        }
        Some(benchmark(settings.iterations, || {
            let hits = index.candidate_hits(black_box(&query));
            black_box(materialize_cached(&structural_cache, &hits)?);
            Ok(())
        })?)
    };

    println!("shard-log sealed-pack query benchmark");
    println!("output directory: {}", settings.output_dir.display());
    println!(
        "index bytes: {}",
        std::fs::metadata(settings.output_dir.join("query-index.bin"))?.len()
    );
    println!("indexed blocks: {}", index.blocks().len());
    println!("logical posting ordinals: {}", index.posting_cardinality());
    println!(
        "resident posting storage bytes: {}",
        index.posting_storage_bytes()
    );
    println!(
        "index load seconds: {:.6}",
        index_load_elapsed.as_secs_f64()
    );
    println!(
        "requires post-decode filtering: {}",
        query.requires_post_decode()
    );
    println!("candidate hits: {}", warm_execution.candidate_hits);
    println!("candidate blocks: {}", warm_execution.candidate_blocks);
    println!("materialized records: {}", warm_execution.records.len());
    println!("iterations: {}", settings.iterations);
    println!("cold iterations: {}", settings.cold_iterations);
    println!("residual workers: {}", settings.workers);
    print_metric("plan", plan_elapsed, settings.iterations);
    print_metric(
        "plan_read_decompress_selective_decode",
        materialize_elapsed,
        settings.iterations,
    );
    if let Some(cached_elapsed) = cached_elapsed {
        print_metric(
            "plan_cached_structural_selective_decode",
            cached_elapsed,
            settings.iterations,
        );
    }
    if let Some(cold_elapsed) = cold_elapsed {
        print_metric(
            "plan_cold_pack_read_decompress_selective_decode",
            cold_elapsed,
            settings.cold_iterations,
        );
    }
    Ok(())
}

fn benchmark(
    iterations: usize,
    mut operation: impl FnMut() -> Result<(), Box<dyn Error>>,
) -> Result<Measurement, Box<dyn Error>> {
    let mut total = Duration::ZERO;
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        operation()?;
        let elapsed = started.elapsed();
        total += elapsed;
        samples.push(elapsed);
    }
    Ok(Measurement { total, samples })
}

fn benchmark_query(
    iterations: usize,
    index: &PersistentQueryIndex,
    reader: &mut PackReader,
    query: &LogQuery,
    workers: usize,
    cold_pack_cache: bool,
) -> Result<Measurement, Box<dyn Error>> {
    let mut total = Duration::ZERO;
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        if cold_pack_cache {
            reader.evict_pack_cache()?;
        }
        let started = Instant::now();
        black_box(execute_query(index, reader, black_box(query), workers)?);
        let elapsed = started.elapsed();
        total += elapsed;
        samples.push(elapsed);
    }
    Ok(Measurement { total, samples })
}

fn print_metric(name: &str, mut measurement: Measurement, iterations: usize) {
    measurement.samples.sort_unstable();
    let seconds = measurement.total.as_secs_f64();
    let count = iterations as f64;
    println!(
        "{name}: mean {:.2} us/query, p50 {:.2} us, p95 {:.2} us, p99 {:.2} us, {:.2} queries/s",
        seconds * 1_000_000.0 / count,
        percentile_micros(&measurement.samples, 50),
        percentile_micros(&measurement.samples, 95),
        percentile_micros(&measurement.samples, 99),
        count / seconds
    );
}

fn percentile_micros(samples: &[Duration], percentile: usize) -> f64 {
    let index = samples
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index].as_secs_f64() * 1_000_000.0
}

fn execute_query(
    index: &PersistentQueryIndex,
    reader: &mut PackReader,
    query: &LogQuery,
    workers: usize,
) -> Result<QueryExecution, Box<dyn Error>> {
    if !query.requires_post_decode() {
        let hits = index.candidate_hits(query);
        let candidate_blocks = hits
            .iter()
            .map(|hit| hit.block_ordinal)
            .collect::<HashSet<_>>()
            .len();
        let candidate_hits = hits.len();
        let records = query.select(materialize(reader, &hits)?);
        return Ok(QueryExecution {
            records,
            candidate_hits,
            candidate_blocks,
        });
    }

    let mut records = Vec::new();
    let mut candidate_hits = 0usize;
    let mut candidate_blocks = 0usize;
    let blocks = index.candidate_blocks(query);
    let Some((&first_block, remaining_blocks)) = blocks.split_first() else {
        return Ok(QueryExecution {
            records,
            candidate_hits,
            candidate_blocks,
        });
    };

    let first = execute_block(index, reader, query, first_block)?;
    candidate_blocks += 1;
    candidate_hits = candidate_hits.saturating_add(first.candidate_hits);
    records.extend(first.records);
    if page_is_complete(query, records.len()) {
        return Ok(QueryExecution {
            records: query.select(records),
            candidate_hits,
            candidate_blocks,
        });
    }

    if workers == 1 {
        for &block_ordinal in remaining_blocks {
            let block = execute_block(index, reader, query, block_ordinal)?;
            candidate_blocks = candidate_blocks.saturating_add(1);
            candidate_hits = candidate_hits.saturating_add(block.candidate_hits);
            records.extend(block.records);
            if page_is_complete(query, records.len()) {
                break;
            }
        }
    } else {
        let output_dir = reader.output_dir.clone();
        let entries = Arc::clone(&reader.entries);
        let block_results = remaining_blocks
            .par_iter()
            .map_init(
                || {
                    PackReader::from_shared(output_dir.clone(), Arc::clone(&entries))
                        .map_err(|error| error.to_string())
                },
                |reader, &block_ordinal| match reader {
                    Ok(reader) => execute_block(index, reader, query, block_ordinal)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error.clone()),
                },
            )
            .collect::<Vec<_>>();
        candidate_blocks = candidate_blocks.saturating_add(block_results.len());
        for block in block_results {
            let block = block.map_err(|error| -> Box<dyn Error> { error.into() })?;
            candidate_hits = candidate_hits.saturating_add(block.candidate_hits);
            if !page_is_complete(query, records.len()) {
                records.extend(block.records);
            }
        }
    }
    Ok(QueryExecution {
        records: query.select(records),
        candidate_hits,
        candidate_blocks,
    })
}

fn execute_block(
    index: &PersistentQueryIndex,
    reader: &mut PackReader,
    query: &LogQuery,
    block_ordinal: u32,
) -> Result<BlockExecution, Box<dyn Error>> {
    let hits = index.candidate_hits_in_block(query, block_ordinal);
    let candidate_hits = hits.len();
    let records = if hits.is_empty() {
        Vec::new()
    } else {
        query.select(materialize(reader, &hits)?)
    };
    Ok(BlockExecution {
        records,
        candidate_hits,
    })
}

fn page_is_complete(query: &LogQuery, records: usize) -> bool {
    query.sort == QuerySort::Offset && query.limit.is_some_and(|limit| records >= limit)
}

fn materialize(
    reader: &mut PackReader,
    hits: &[QueryHit],
) -> Result<Vec<DecodedStructuralRecord>, Box<dyn Error>> {
    let groups = group_hits(hits);
    let mut records = HashMap::with_capacity(hits.len());
    for (block_ordinal, record_ordinals) in groups {
        let structural = reader.read_structural(block_ordinal)?;
        for (record_ordinal, record) in record_ordinals
            .iter()
            .copied()
            .zip(decode_structural_records(&structural, &record_ordinals)?)
        {
            records.insert((block_ordinal, record_ordinal), record);
        }
    }
    order_materialized(hits, records)
}

fn materialize_cached(
    cache: &BTreeMap<u32, Vec<u8>>,
    hits: &[QueryHit],
) -> Result<Vec<DecodedStructuralRecord>, Box<dyn Error>> {
    let groups = group_hits(hits);
    let mut records = HashMap::with_capacity(hits.len());
    for (block_ordinal, record_ordinals) in groups {
        let structural = cache
            .get(&block_ordinal)
            .ok_or("structural cache missed a planned block")?;
        for (record_ordinal, record) in record_ordinals
            .iter()
            .copied()
            .zip(decode_structural_records(structural, &record_ordinals)?)
        {
            records.insert((block_ordinal, record_ordinal), record);
        }
    }
    order_materialized(hits, records)
}

fn order_materialized(
    hits: &[QueryHit],
    mut records: HashMap<(u32, u32), DecodedStructuralRecord>,
) -> Result<Vec<DecodedStructuralRecord>, Box<dyn Error>> {
    hits.iter()
        .map(|hit| {
            records
                .remove(&(hit.block_ordinal, hit.record_ordinal))
                .ok_or_else(|| "selective decode missed a planned record".into())
        })
        .collect()
}

fn group_hits(hits: &[QueryHit]) -> BTreeMap<u32, Vec<u32>> {
    let mut groups = BTreeMap::<u32, Vec<u32>>::new();
    for hit in hits {
        groups
            .entry(hit.block_ordinal)
            .or_default()
            .push(hit.record_ordinal);
    }
    for ordinals in groups.values_mut() {
        ordinals.sort_unstable();
        ordinals.dedup();
    }
    groups
}

fn verify_matches(
    records: &[DecodedStructuralRecord],
    query: &LogQuery,
) -> Result<(), Box<dyn Error>> {
    if records.iter().any(|record| !query.matches(record)) {
        return Err("sealed query returned a non-matching record".into());
    }
    if records
        .windows(2)
        .any(|pair| query.compare(&pair[0], &pair[1]).is_gt())
    {
        return Err("sealed query returned records out of order".into());
    }
    if query.limit.is_some_and(|limit| records.len() > limit) {
        return Err("sealed query exceeded its result limit".into());
    }
    Ok(())
}

fn emit_results(path: &Path, records: &[DecodedStructuralRecord]) -> Result<(), Box<dyn Error>> {
    let mut output = BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?,
    );
    for record in records {
        let stream = record
            .fields
            .iter()
            .find(|field| field.key.as_ref() == "docker.stream")
            .map_or("", |field| field.value.as_ref());
        write!(output, "{}\t", record.timestamp_unix_nanos)?;
        write_hex(&mut output, stream.as_bytes())?;
        output.write_all(b"\t")?;
        write_hex(&mut output, record.message.as_bytes())?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}

fn write_hex(output: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        output.write_all(&[HEX[usize::from(*byte >> 4)], HEX[usize::from(*byte & 0x0f)]])?;
    }
    Ok(())
}

fn read_manifest(output_dir: &Path) -> Result<Vec<ManifestEntry>, Box<dyn Error>> {
    let manifest = File::open(output_dir.join("manifest.bin"))?;
    let mut header = [0u8; MANIFEST_HEADER_BYTES];
    manifest.read_exact_at(&mut header, 0)?;
    if &header[..9] != b"SLOGPACK2" {
        return Err("manifest has invalid magic".into());
    }
    let entry_count = usize::try_from(u64::from_le_bytes(header[9..17].try_into()?))?;
    let mut entries = Vec::with_capacity(entry_count);
    let mut encoded = [0u8; MANIFEST_ENTRY_BYTES];
    for index in 0..entry_count {
        let offset = u64::try_from(MANIFEST_HEADER_BYTES)?
            .checked_add(
                u64::try_from(index)?
                    .checked_mul(u64::try_from(MANIFEST_ENTRY_BYTES)?)
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
        let ordinal = u32::try_from(next())?;
        let _source_offset = next();
        let _input_bytes = next();
        let _source_bytes = next();
        let _record_count = next();
        let structural_bytes = usize::try_from(next())?;
        let pack_worker = usize::try_from(next())?;
        let pack_offset = next();
        let stored_bytes = usize::try_from(next())?;
        let payload_checksum = next();
        entries.push(ManifestEntry {
            ordinal,
            structural_bytes,
            pack_worker,
            pack_offset,
            stored_bytes,
            payload_checksum,
        });
    }
    Ok(entries)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let output_dir = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: shard-log-pack-query-bench <pack-directory> [--term TERM] [--field KEY=VALUE] [--contains TEXT] [--regex PATTERN] [--limit N] [--oldest] [--iterations N] [--cold-iterations N] [--workers N] [--emit-results PATH]")?;
    let mut settings = Settings {
        output_dir,
        terms: Vec::new(),
        fields: Vec::new(),
        limit: 100,
        newest_first: true,
        iterations: DEFAULT_ITERATIONS,
        cold_iterations: 0,
        workers: 1,
        contains: Vec::new(),
        regexes: Vec::new(),
        emit_results: None,
    };
    while let Some(argument) = arguments.next() {
        match argument.to_string_lossy().as_ref() {
            "--term" => settings.terms.push(
                arguments
                    .next()
                    .ok_or("--term requires a value")?
                    .to_string_lossy()
                    .into_owned(),
            ),
            "--field" => {
                let field = arguments
                    .next()
                    .ok_or("--field requires KEY=VALUE")?
                    .to_string_lossy()
                    .into_owned();
                let (key, value) = field.split_once('=').ok_or("--field requires KEY=VALUE")?;
                settings.fields.push((key.to_owned(), value.to_owned()));
            }
            "--contains" => settings.contains.push(
                arguments
                    .next()
                    .ok_or("--contains requires a value")?
                    .to_string_lossy()
                    .into_owned(),
            ),
            "--regex" => settings.regexes.push(
                arguments
                    .next()
                    .ok_or("--regex requires a pattern")?
                    .to_string_lossy()
                    .into_owned(),
            ),
            "--limit" => {
                settings.limit = arguments
                    .next()
                    .ok_or("--limit requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--oldest" => settings.newest_first = false,
            "--iterations" => {
                settings.iterations = arguments
                    .next()
                    .ok_or("--iterations requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--cold-iterations" => {
                settings.cold_iterations = arguments
                    .next()
                    .ok_or("--cold-iterations requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--workers" => {
                settings.workers = arguments
                    .next()
                    .ok_or("--workers requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--emit-results" => {
                settings.emit_results = Some(PathBuf::from(
                    arguments.next().ok_or("--emit-results requires a path")?,
                ));
            }
            value => return Err(format!("unknown argument: {value}").into()),
        }
    }
    if settings.limit == 0 || settings.iterations == 0 || settings.workers == 0 {
        return Err("limit, iterations, and workers must be nonzero".into());
    }
    Ok(settings)
}
