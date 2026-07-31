use std::error::Error;
use std::hint::black_box;
use std::time::{Duration, Instant};

use shard_log::{
    CaseSensitivity, CompressionCohortId, DurableLogRecord, LogPredicate, LogQuery, LogStripe,
    NumericComparison, QueryCursor, StripeConfig,
};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

const DEFAULT_RECORDS: usize = 100_000;
const DEFAULT_ITERATIONS: usize = 100;

struct Settings {
    records: usize,
    iterations: usize,
}

struct Workload {
    name: &'static str,
    query: LogQuery,
    expected_matches: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    let partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3));
    let shard_id = ShardId::new(0);
    let mut stripe = LogStripe::new(
        shard_id,
        StripeConfig {
            target_block_bytes: u64::MAX,
            ..StripeConfig::default()
        },
    )?;

    let build_started = Instant::now();
    for index in 0..settings.records {
        let mut message = format!("common log event request_id={index}");
        if index % 10 == 0 {
            message.push_str(" medium");
        }
        if index % 1_000 == 0 {
            message.push_str(" rare");
        }
        stripe.apply_durable(
            DurableLogRecord::new(
                shard_id,
                partition,
                LogicalOffset::new(u64::try_from(index)?),
                u64::try_from(index)?.saturating_mul(1_000),
                message,
                CompressionCohortId::new(1),
            )
            .with_field("service.name", format!("service-{}", index % 16))
            .with_field("severity", if index % 4 == 0 { "ERROR" } else { "INFO" })
            .with_field("status", if index % 10 == 0 { "503" } else { "200" }),
        )?;
    }
    let build_elapsed = build_started.elapsed();

    let rare_matches = settings.records.div_ceil(1_000);
    let service_rare_matches = (0..settings.records)
        .filter(|index| index % 1_000 == 0 && index % 16 == 0)
        .count();
    let error_matches = settings.records.div_ceil(4);
    let elevated_status_matches = settings.records.div_ceil(10);
    let selected_service_matches = (0..settings.records)
        .filter(|index| matches!(index % 16, 0 | 1))
        .count();
    let window_start = settings.records / 2;
    let window_end = window_start.saturating_add(100).min(settings.records);
    let next_page_matches = settings.records.saturating_sub(100).min(100);
    let next_page_cursor_offset = settings.records.saturating_sub(100);
    let workloads = vec![
        Workload {
            name: "term_rare",
            query: LogQuery::new(partition).with_term("rare"),
            expected_matches: rare_matches,
        },
        Workload {
            name: "latest_common_limit_100",
            query: LogQuery::new(partition)
                .with_term("common")
                .newest_first()
                .with_limit(100),
            expected_matches: settings.records.min(100),
        },
        Workload {
            name: "latest_all_limit_100",
            query: LogQuery::new(partition).newest_first().with_limit(100),
            expected_matches: settings.records.min(100),
        },
        Workload {
            name: "common_offset_window_100",
            query: LogQuery::new(partition)
                .with_term("common")
                .with_offset_range(
                    LogicalOffset::new(u64::try_from(window_start)?),
                    LogicalOffset::new(u64::try_from(window_end)?),
                ),
            expected_matches: window_end.saturating_sub(window_start),
        },
        Workload {
            name: "and_common_rare",
            query: LogQuery::new(partition)
                .with_term("common")
                .with_term("rare"),
            expected_matches: rare_matches,
        },
        Workload {
            name: "and_rare_common",
            query: LogQuery::new(partition)
                .with_term("rare")
                .with_term("common"),
            expected_matches: rare_matches,
        },
        Workload {
            name: "and_common_medium_rare",
            query: LogQuery::new(partition)
                .with_term("common")
                .with_term("medium")
                .with_term("rare"),
            expected_matches: rare_matches,
        },
        Workload {
            name: "field_service_rare",
            query: LogQuery::new(partition)
                .with_term("rare")
                .with_field("service.name", "service-0"),
            expected_matches: service_rare_matches,
        },
        Workload {
            name: "term_miss",
            query: LogQuery::new(partition).with_term("does-not-exist"),
            expected_matches: 0,
        },
        Workload {
            name: "boolean_common_and_error_or_rare",
            query: LogQuery::new(partition).where_predicate(LogPredicate::and(vec![
                LogPredicate::term("common"),
                LogPredicate::or(vec![
                    LogPredicate::field_equals("severity", "ERROR"),
                    LogPredicate::term("rare"),
                ]),
            ])),
            expected_matches: error_matches,
        },
        Workload {
            name: "message_contains_rare",
            query: LogQuery::new(partition)
                .where_predicate(LogPredicate::message_contains(" rare")),
            expected_matches: rare_matches,
        },
        Workload {
            name: "message_regex_rare",
            query: LogQuery::new(partition).where_predicate(LogPredicate::message_regex(
                r"request_id=\d+.*\brare$",
                CaseSensitivity::Sensitive,
            )?),
            expected_matches: rare_matches,
        },
        Workload {
            name: "field_exists",
            query: LogQuery::new(partition)
                .where_predicate(LogPredicate::field_exists("service.name")),
            expected_matches: settings.records,
        },
        Workload {
            name: "field_in_two_services",
            query: LogQuery::new(partition).where_predicate(LogPredicate::field_in(
                "service.name",
                ["service-0", "service-1"],
            )),
            expected_matches: selected_service_matches,
        },
        Workload {
            name: "field_numeric_gte_500",
            query: LogQuery::new(partition).where_predicate(LogPredicate::field_numeric(
                "status",
                NumericComparison::GreaterThanOrEqual,
                500,
            )),
            expected_matches: elevated_status_matches,
        },
        Workload {
            name: "latest_timestamp_limit_100",
            query: LogQuery::new(partition)
                .sort_by_timestamp()
                .newest_first()
                .with_limit(100),
            expected_matches: settings.records.min(100),
        },
        Workload {
            name: "offset_cursor_next_100",
            query: LogQuery::new(partition)
                .newest_first()
                .after(QueryCursor::new(
                    u64::try_from(next_page_cursor_offset)?.saturating_mul(1_000),
                    LogicalOffset::new(u64::try_from(next_page_cursor_offset)?),
                ))
                .with_limit(100),
            expected_matches: next_page_matches,
        },
    ];

    println!("shard-log hot-query benchmark");
    println!("records: {}", settings.records);
    println!("iterations: {}", settings.iterations);
    println!("index build seconds: {:.6}", build_elapsed.as_secs_f64());
    println!(
        "index build records/s: {:.2}",
        settings.records as f64 / build_elapsed.as_secs_f64()
    );
    println!("workload,matches,total_seconds,ns_per_query,queries_per_second");
    for workload in workloads {
        let warm = stripe.query(black_box(&workload.query));
        if warm.len() != workload.expected_matches {
            return Err(format!(
                "{} returned {}, expected {}",
                workload.name,
                warm.len(),
                workload.expected_matches
            )
            .into());
        }
        black_box(warm);

        let started = Instant::now();
        let mut observed = 0usize;
        for _ in 0..settings.iterations {
            let matches = stripe.query(black_box(&workload.query));
            observed ^= matches.len();
            black_box(matches);
        }
        let elapsed = started.elapsed();
        black_box(observed);
        print_result(
            workload.name,
            workload.expected_matches,
            elapsed,
            settings.iterations,
        );
    }
    Ok(())
}

fn print_result(name: &str, matches: usize, elapsed: Duration, iterations: usize) {
    let seconds = elapsed.as_secs_f64();
    let queries = iterations as f64;
    println!(
        "{name},{matches},{seconds:.6},{:.2},{:.2}",
        seconds * 1_000_000_000.0 / queries,
        queries / seconds
    );
}

fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut settings = Settings {
        records: DEFAULT_RECORDS,
        iterations: DEFAULT_ITERATIONS,
    };
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.to_string_lossy().as_ref() {
            "--records" => {
                settings.records = arguments
                    .next()
                    .ok_or("--records requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--iterations" => {
                settings.iterations = arguments
                    .next()
                    .ok_or("--iterations requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            value => return Err(format!("unknown argument: {value}").into()),
        }
    }
    if settings.records == 0 || settings.iterations == 0 {
        return Err("records and iterations must be nonzero".into());
    }
    Ok(settings)
}
