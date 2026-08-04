use std::fs;
use std::hint::black_box;
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use shard_telemetry::{LocalObjectStore, SsdCacheConfig, SsdObjectCache, TelemetryObjectStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut object_bytes = 64 * 1024 * 1024usize;
    let mut chunk_bytes = 4 * 1024 * 1024u64;
    let mut range_bytes = 64 * 1024u64;
    let mut ranges = 64usize;
    let mut iterations = 100usize;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--object-bytes" => object_bytes = parse(args.next(), "--object-bytes")?,
            "--chunk-bytes" => chunk_bytes = parse(args.next(), "--chunk-bytes")?,
            "--range-bytes" => range_bytes = parse(args.next(), "--range-bytes")?,
            "--ranges" => ranges = parse(args.next(), "--ranges")?,
            "--iterations" => iterations = parse(args.next(), "--iterations")?,
            _ => return Err(format!("unknown argument {argument}").into()),
        }
    }
    if object_bytes == 0 || chunk_bytes == 0 || range_bytes == 0 || ranges == 0 || iterations == 0 {
        return Err("all benchmark bounds must be nonzero".into());
    }
    let selected_bytes = range_bytes
        .checked_mul(u64::try_from(ranges)?)
        .ok_or("selected byte count overflow")?;
    if selected_bytes > chunk_bytes || chunk_bytes > u64::try_from(object_bytes)? {
        return Err("selected ranges must fit one cache chunk and object".into());
    }

    let root = benchmark_directory();
    fs::create_dir_all(&root)?;
    let store = LocalObjectStore::open(root.join("objects"))?;
    let payload = (0..object_bytes)
        .map(|index| (index as u64).wrapping_mul(131).wrapping_add(17) as u8)
        .collect::<Vec<_>>();
    let metadata = store.put_bytes_if_absent("payload/pack", &payload)?;
    let selected = (0..ranges)
        .map(|index| {
            let start = u64::try_from(index).expect("range index fits") * range_bytes;
            start..start + range_bytes
        })
        .collect::<Vec<Range<u64>>>();
    let config = SsdCacheConfig {
        max_bytes: 2 * (chunk_bytes + 64),
        chunk_bytes,
        max_read_bytes: chunk_bytes,
    };
    let legacy = SsdObjectCache::open(root.join("legacy-cache"), config)?;
    let batched = SsdObjectCache::open(root.join("batched-cache"), config)?;

    let (legacy_cold_elapsed, legacy_cold) = time(|| {
        selected
            .iter()
            .map(|range| {
                legacy.read_range_with_metadata(&store, "payload/pack", &metadata, range.clone())
            })
            .collect::<Result<Vec<_>, _>>()
    });
    let legacy_cold = legacy_cold?;
    let (batched_cold_elapsed, batched_cold) =
        time(|| batched.read_ranges_with_metadata(&store, "payload/pack", &metadata, &selected));
    let batched_cold = batched_cold?;
    let legacy_expected = legacy_cold.iter().flatten().copied().collect::<Vec<_>>();
    let batched_expected = batched_cold.iter().flatten().copied().collect::<Vec<_>>();
    if legacy_expected != batched_expected || legacy_expected != payload[..selected_bytes as usize]
    {
        return Err("batched and independent range reads disagree".into());
    }

    let legacy_start = Instant::now();
    for _ in 0..iterations {
        for range in &selected {
            black_box(legacy.read_range_with_metadata(
                &store,
                "payload/pack",
                &metadata,
                range.clone(),
            )?);
        }
    }
    let legacy_warm = legacy_start.elapsed();
    let batched_start = Instant::now();
    for _ in 0..iterations {
        black_box(batched.read_ranges_with_metadata(
            &store,
            "payload/pack",
            &metadata,
            &selected,
        )?);
    }
    let batched_warm = batched_start.elapsed();
    let legacy_stats = legacy.stats();
    let batched_stats = batched.stats();

    println!("ShardTelemetry tier-cache benchmark");
    println!(
        "object_bytes={object_bytes} chunk_bytes={chunk_bytes} range_bytes={range_bytes} ranges={ranges} iterations={iterations}"
    );
    println!(
        "cold_us legacy={:.3} batched={:.3} speedup={:.2}x",
        legacy_cold_elapsed.as_secs_f64() * 1e6,
        batched_cold_elapsed.as_secs_f64() * 1e6,
        legacy_cold_elapsed.as_secs_f64() / batched_cold_elapsed.as_secs_f64(),
    );
    println!(
        "warm_us_per_query legacy={:.3} batched={:.3} speedup={:.2}x",
        legacy_warm.as_secs_f64() * 1e6 / iterations as f64,
        batched_warm.as_secs_f64() * 1e6 / iterations as f64,
        legacy_warm.as_secs_f64() / batched_warm.as_secs_f64(),
    );
    println!(
        "cache legacy_hits={} legacy_misses={} legacy_source_bytes={} batched_hits={} batched_misses={} batched_source_bytes={}",
        legacy_stats.hits,
        legacy_stats.misses,
        legacy_stats.source_bytes,
        batched_stats.hits,
        batched_stats.misses,
        batched_stats.source_bytes,
    );
    println!("evidence_directory={}", root.display());
    Ok(())
}

fn time<T>(operation: impl FnOnce() -> T) -> (std::time::Duration, T) {
    let start = Instant::now();
    let result = operation();
    (start.elapsed(), result)
}

fn parse<T: std::str::FromStr>(value: Option<String>, name: &str) -> Result<T, String> {
    value
        .ok_or_else(|| format!("missing value for {name}"))?
        .parse()
        .map_err(|_| format!("invalid value for {name}"))
}

fn benchmark_directory() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shard-telemetry-tier-cache-bench-{}-{nanos}",
        std::process::id()
    ))
}
