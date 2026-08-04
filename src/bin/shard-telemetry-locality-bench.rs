use std::env;
use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};
use shard_telemetry::{
    CompressionBlockCollator, CompressionCohortId, CompressionLocalityConfig,
    CompressionLocalityRecord, CompressionPlacementId, DurableLog, LogStripe, MessageFingerprint,
    StripeConfig, fingerprint_message,
};

const DEFAULT_ITERATIONS: usize = 1_000_000;
const DEFAULT_SEAL_RECORDS: usize = 50_000;

#[derive(Debug)]
struct Settings {
    iterations: usize,
    seal_records: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    println!("shard-telemetry compression-locality microbenchmark");
    println!("iterations: {}", settings.iterations);
    benchmark_fingerprints(settings.iterations);
    benchmark_routes(settings.iterations);
    benchmark_block_collation(settings.iterations);
    benchmark_combined(settings.iterations);
    benchmark_seals(settings.seal_records)?;
    Ok(())
}

fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut settings = Settings {
        iterations: DEFAULT_ITERATIONS,
        seal_records: DEFAULT_SEAL_RECORDS,
    };
    let mut arguments = env::args().skip(1);
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--iterations" => {
                settings.iterations = arguments
                    .next()
                    .ok_or("--iterations requires a value")?
                    .parse()?;
            }
            "--seal-records" => {
                settings.seal_records = arguments
                    .next()
                    .ok_or("--seal-records requires a value")?
                    .parse()?;
            }
            _ => return Err(format!("unknown argument: {flag}").into()),
        }
    }
    if settings.iterations == 0 || settings.seal_records == 0 {
        return Err("benchmark iteration counts must be nonzero".into());
    }
    Ok(settings)
}

fn benchmark_fingerprints(iterations: usize) {
    for size in [64usize, 256, 1_024, 4_096] {
        let message = message_of_size(size);
        let started = Instant::now();
        for _ in 0..iterations {
            black_box(fingerprint_message(black_box(&message), &[]));
        }
        let elapsed = started.elapsed();
        println!(
            "fingerprint_{size}_bytes: {:.2} MiB/s ({:.2} ns/record)",
            throughput_mib(size.saturating_mul(iterations), elapsed),
            nanos_per(iterations, elapsed)
        );
    }
}

fn benchmark_routes(iterations: usize) {
    let source = CompressionCohortId::new(7);
    let fingerprint = fingerprint_message("worker request 123 completed in 45 ms", &[]);
    let mut router = CompressionBlockCollator::new(small_benchmark_config(), 8 * 1024 * 1024)
        .expect("benchmark collator config validates");
    let seed = vec![
        CompressionLocalityRecord {
            fingerprint,
            source_bytes: 512 * 1024,
        };
        16
    ];
    black_box(router.collate(
        source,
        CompressionPlacementId::from_source_cohort(source),
        &seed,
    ));
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(router.tentative_placement(source, fingerprint));
    }
    let elapsed = started.elapsed();
    println!(
        "tentative_existing_shard: {:.2} Mrecords/s ({:.2} ns/record)",
        records_per_second(iterations, elapsed) / 1_000_000.0,
        nanos_per(iterations, elapsed)
    );

    let fingerprints = (0..4_096u64)
        .map(|index| MessageFingerprint {
            shape_hash: index.wrapping_mul(0x9e37_79b9_7f4a_7c15),
            locality_signature: u16::try_from(index).expect("fingerprint index fits u16"),
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    for index in 0..iterations {
        black_box(router.tentative_placement(source, fingerprints[index & 4_095]));
    }
    let elapsed = started.elapsed();
    println!(
        "tentative_16_shard_probe: {:.2} Mrecords/s ({:.2} ns/record)",
        records_per_second(iterations, elapsed) / 1_000_000.0,
        nanos_per(iterations, elapsed)
    );
    println!(
        "collator_preallocated_state_bytes: {}",
        router.stats().allocated_state_bytes
    );
}

fn benchmark_block_collation(iterations: usize) {
    let source = CompressionCohortId::new(13);
    let block_records = 4_096usize;
    let records = (0..block_records)
        .map(|index| CompressionLocalityRecord {
            fingerprint: MessageFingerprint {
                shape_hash: u64::try_from(index & 3).expect("shape fits"),
                locality_signature: match index & 3 {
                    0 => 0x0000,
                    1 => 0x00ff,
                    2 => 0xff00,
                    _ => 0xffff,
                },
            },
            source_bytes: 256,
        })
        .collect::<Vec<_>>();
    let mut router = CompressionBlockCollator::new(small_benchmark_config(), 1024 * 1024)
        .expect("benchmark collator config validates");
    let rounds = iterations.div_ceil(block_records).max(1);
    let started = Instant::now();
    for _ in 0..rounds {
        black_box(router.collate(
            source,
            CompressionPlacementId::from_source_cohort(source),
            &records,
        ));
    }
    let elapsed = started.elapsed();
    let scored_records = rounds.saturating_mul(block_records);
    let stats = router.stats();
    println!(
        "block_score_split_assign: {:.2} MiB/s ({:.2} Mrecords/s)",
        throughput_mib(scored_records.saturating_mul(256), elapsed),
        records_per_second(scored_records, elapsed) / 1_000_000.0
    );
    println!(
        "collation_blocks_scored_split_subblocks: {} {} {}",
        stats.blocks_scored, stats.blocks_split, stats.subblocks_created
    );
    println!(
        "collation_handoff_membership_bytes: {}",
        stats.handoff_membership_bytes
    );
}

fn benchmark_combined(iterations: usize) {
    let source = CompressionCohortId::new(11);
    let messages = (0..256)
        .map(|index| format!("api request {index:08} completed with status 200 in 42 ms"))
        .collect::<Vec<_>>();
    let bytes_per_cycle = messages.iter().map(String::len).sum::<usize>();
    let router = CompressionBlockCollator::new(small_benchmark_config(), 1024 * 1024)
        .expect("benchmark collator config validates");
    let started = Instant::now();
    for index in 0..iterations {
        let message = &messages[index & 255];
        let fingerprint = fingerprint_message(message, &[]);
        black_box(router.tentative_placement(source, fingerprint));
    }
    let elapsed = started.elapsed();
    let total_bytes = bytes_per_cycle
        .saturating_mul(iterations / messages.len())
        .saturating_add(
            messages
                .iter()
                .take(iterations % messages.len())
                .map(String::len)
                .sum(),
        );
    println!(
        "fingerprint_plus_tentative_route_single_thread: {:.2} MiB/s ({:.2} Mrecords/s)",
        throughput_mib(total_bytes, elapsed),
        records_per_second(iterations, elapsed) / 1_000_000.0
    );
}

fn benchmark_seals(record_count: usize) -> Result<(), Box<dyn Error>> {
    let mut stripe = LogStripe::new(
        ShardId::new(0),
        StripeConfig {
            target_block_bytes: 64 * 1024,
            dictionary_cache_bytes: 1024 * 1024,
            compression_level: 1,
            compression_locality: small_benchmark_config(),
        },
    )?;
    let topic_partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    let messages = (0..256)
        .map(|index| {
            Arc::<str>::from(format!(
                "worker request {index:08} completed status=200 elapsed=42ms"
            ))
        })
        .collect::<Vec<_>>();
    let source = CompressionCohortId::new(5);
    let started = Instant::now();
    let mut seal_latencies = Vec::new();
    for index in 0..record_count {
        if index.is_multiple_of(256) {
            stripe.begin_append_batch()?;
        }
        let apply_started = Instant::now();
        let receipt = stripe.apply_durable(DurableLog::new(
            ShardId::new(0),
            topic_partition,
            LogicalOffset::new(u64::try_from(index)?),
            u64::try_from(index)?.saturating_mul(1_000),
            Arc::clone(&messages[index & 255]),
            source,
        ))?;
        if !receipt.sealed_blocks.is_empty() {
            seal_latencies.push(apply_started.elapsed());
        }
    }
    let elapsed = started.elapsed();
    seal_latencies.sort_unstable();
    println!(
        "stripe_single_thread: {:.2} Mrecords/s",
        records_per_second(record_count, elapsed) / 1_000_000.0
    );
    println!("sealed_blocks_sampled: {}", seal_latencies.len());
    println!(
        "block_seal_latency_p50_us: {:.2}",
        percentile(&seal_latencies, 50).as_secs_f64() * 1_000_000.0
    );
    println!(
        "block_seal_latency_p99_us: {:.2}",
        percentile(&seal_latencies, 99).as_secs_f64() * 1_000_000.0
    );
    Ok(())
}

fn small_benchmark_config() -> CompressionLocalityConfig {
    CompressionLocalityConfig {
        enabled: true,
        min_split_records: 8,
        min_split_bytes: 1,
        min_admission_bytes: 1,
        ..CompressionLocalityConfig::default()
    }
}

fn message_of_size(size: usize) -> String {
    let suffix = " request 12345678 failed";
    let mut message = "static-log-literal ".repeat(size.div_ceil(19));
    message.truncate(size.saturating_sub(suffix.len()));
    message.push_str(suffix);
    message
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let index = samples.len().saturating_sub(1).saturating_mul(percentile) / 100;
    samples[index]
}

fn throughput_mib(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
}

fn records_per_second(records: usize, elapsed: Duration) -> f64 {
    records as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
}

fn nanos_per(records: usize, elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1_000_000_000.0 / records.max(1) as f64
}
