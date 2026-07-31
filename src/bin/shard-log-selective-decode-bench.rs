use std::error::Error;
use std::hint::black_box;
use std::time::{Duration, Instant};

use shard_log::{
    BlockQueryIndex, CompressionCohortId, DurableLogRecord, LogQuery, PersistentQueryIndex,
    QueryBlockMetadata, decode_structural_block, decode_structural_records,
    encode_structural_block,
};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

const RECORD_COUNT: usize = 60_000;
const SELECTED_COUNT: usize = 100;
const SELECTIVE_ITERATIONS: usize = 1_000;
const FULL_ITERATIONS: usize = 20;
const INDEX_ITERATIONS: usize = 10;
const PLAN_ITERATIONS: usize = 100_000;

fn main() -> Result<(), Box<dyn Error>> {
    let records = build_records()?;
    let structural = encode_structural_block(&records)?;
    let compressed = zstd::bulk::compress(&structural, 1)?;
    let full = decode_structural_block(&structural)?;
    if full.len() != records.len() {
        return Err("full decode record count mismatch".into());
    }

    let selections = [
        (
            "latest-contiguous-100",
            ((RECORD_COUNT - SELECTED_COUNT)..RECORD_COUNT)
                .map(u32::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (
            "middle-contiguous-100",
            ((RECORD_COUNT / 2)..(RECORD_COUNT / 2 + SELECTED_COUNT))
                .map(u32::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (
            "latest-every-100",
            (0..SELECTED_COUNT)
                .map(|index| RECORD_COUNT - 1 - index * 100)
                .map(u32::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        ),
    ];

    println!("shard-log structural selective-decode benchmark");
    println!("records: {RECORD_COUNT}");
    println!("structural bytes: {}", structural.len());
    println!("zstd-1 bytes: {}", compressed.len());
    let persistent_index = PersistentQueryIndex::from_blocks(vec![(
        QueryBlockMetadata {
            block_ordinal: 0,
            topic_partition: records[0].record_ref.topic_partition,
            first_offset: records[0].record_ref.offset,
            last_offset: records[RECORD_COUNT - 1].record_ref.offset,
            min_timestamp_unix_nanos: records[0].timestamp_unix_nanos,
            max_timestamp_unix_nanos: records[RECORD_COUNT - 1].timestamp_unix_nanos,
            record_count: u32::try_from(RECORD_COUNT)?,
        },
        BlockQueryIndex::build(&records)?,
    )])?;
    println!(
        "query-index zstd bytes: {}",
        persistent_index.encode_compressed(1)?.len()
    );
    println!(
        "logical posting ordinals: {}",
        persistent_index.posting_cardinality()
    );
    println!(
        "resident posting storage bytes: {}",
        persistent_index.posting_storage_bytes()
    );
    let query = LogQuery::new(records[0].record_ref.topic_partition)
        .with_term("error")
        .with_field("service.name", "clickhouse")
        .newest_first()
        .with_limit(100);
    let plan_elapsed = benchmark(PLAN_ITERATIONS, || {
        black_box(persistent_index.candidate_hits(black_box(&query)));
        Ok(())
    })?;
    print_metric(
        "persistent-plan-dense-and-limit-100",
        plan_elapsed,
        PLAN_ITERATIONS,
    );
    let index_elapsed = benchmark(INDEX_ITERATIONS, || {
        black_box(BlockQueryIndex::build(black_box(&records))?);
        Ok(())
    })?;
    print_metric("query-index-build", index_elapsed, INDEX_ITERATIONS);
    println!(
        "query-index-build records/s: {:.2}",
        RECORD_COUNT as f64 * INDEX_ITERATIONS as f64 / index_elapsed.as_secs_f64()
    );
    let full_elapsed = benchmark(FULL_ITERATIONS, || {
        black_box(decode_structural_block(black_box(&structural))?);
        Ok(())
    })?;
    print_metric("full-decode", full_elapsed, FULL_ITERATIONS);

    for (name, mut ordinals) in selections {
        ordinals.sort_unstable();
        let decoded = decode_structural_records(&structural, &ordinals)?;
        if decoded.len() != SELECTED_COUNT {
            return Err("selective decode record count mismatch".into());
        }
        let elapsed = benchmark(SELECTIVE_ITERATIONS, || {
            black_box(decode_structural_records(
                black_box(&structural),
                black_box(&ordinals),
            )?);
            Ok(())
        })?;
        print_metric(name, elapsed, SELECTIVE_ITERATIONS);
    }
    Ok(())
}

fn build_records() -> Result<Vec<DurableLogRecord>, Box<dyn Error>> {
    let partition = TopicPartition::new(TopicId::new(0), LogicalPartitionId::new(0));
    (0..RECORD_COUNT)
        .map(|ordinal| {
            let offset = u64::try_from(ordinal)?;
            Ok(DurableLogRecord::new(
                ShardId::new(0),
                partition,
                LogicalOffset::new(offset),
                1_775_000_000_000_000_000 + offset * 1_000_000,
                format!(
                    "2026.07.29 02:01:{:02}.{:06} [ 404 ] {{}} <Error> \
                     Application: Code: 76. DB::ErrnoException: Cannot open file \
                     /var/lib/clickhouse/store/{:08x}/data.bin: Permission denied",
                    ordinal % 60,
                    ordinal % 1_000_000,
                    ordinal % 4_096,
                ),
                CompressionCohortId::new(0),
            )
            .with_field("docker.stream", "stderr")
            .with_field("service.name", "clickhouse")
            .with_field("severity", "ERROR"))
        })
        .collect()
}

fn benchmark(
    iterations: usize,
    mut operation: impl FnMut() -> Result<(), Box<dyn Error>>,
) -> Result<Duration, Box<dyn Error>> {
    let started = Instant::now();
    for _ in 0..iterations {
        operation()?;
    }
    Ok(started.elapsed())
}

fn print_metric(name: &str, elapsed: Duration, iterations: usize) {
    let seconds = elapsed.as_secs_f64();
    let count = iterations as f64;
    println!(
        "{name}: {:.2} us/op, {:.2} ops/s",
        seconds * 1_000_000.0 / count,
        count / seconds
    );
}
