use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};
use shard_telemetry::{
    CompressionCohortId, CorrelationBlockFilter, CorrelationConfig, CorrelationIndex,
    CorrelationQuery, DurableLog, DurableMetricPoint, DurableSpan, LOGS_TOPIC_ID, LogQuery,
    LogStripe, METRICS_TOPIC_ID, MetadataField, MetricIdentity, MetricIngestProtocol, MetricKind,
    MetricQuery, MetricStripe, MetricValue, NumberValue, OtlpLogEvent, ResourceContext,
    ScopeContext, SeriesFingerprint, SpanId, SpanStatus, StripeConfig, TRACES_TOPIC_ID,
    TelemetryAttribute, TelemetryRecordRef, TelemetrySignal, TelemetryValue, TraceId, TraceQuery,
    TraceStripe, decode_metric_chunk, decode_structural_block, decode_trace_block,
    encode_metric_chunk, encode_trace_block,
};

const TENANT: &str = "production-example";
const TRACE_BLOCK_SOURCE_BYTES: usize = 8 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut records = 32_768usize;
    let mut iterations = 2_000usize;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--records" => records = parse_usize(args.next(), "--records")?,
            "--iterations" => iterations = parse_usize(args.next(), "--iterations")?,
            _ => return Err(format!("unknown argument {argument}").into()),
        }
    }
    if records < 128 || iterations == 0 {
        return Err("--records must be at least 128 and --iterations must be nonzero".into());
    }

    let corpus = Corpus::generate(records)?;
    println!("ShardTelemetry signal benchmark (v1)");
    println!("records_per_signal={records} lookup_iterations={iterations}");

    let log_result = benchmark_logs(&corpus, iterations)?;
    let trace_result = benchmark_traces(&corpus, iterations)?;
    let metric_result = benchmark_metrics(&corpus, iterations)?;
    let correlation_result = benchmark_correlations(&corpus, iterations);
    print_result("logs", log_result);
    print_result("traces", trace_result);
    print_result("metrics", metric_result);
    println!(
        "correlation refs={} lookup_ops_s={:.2} p50_us={:.3} p99_us={:.3}",
        correlation_result.lookup_count,
        correlation_result.lookup_ops_per_second,
        correlation_result.lookup_p50.as_secs_f64() * 1e6,
        correlation_result.lookup_p99.as_secs_f64() * 1e6,
    );
    Ok(())
}

fn parse_usize(value: Option<String>, flag: &str) -> Result<usize, Box<dyn std::error::Error>> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()
        .map_err(Into::into)
}

struct Corpus {
    durable_logs: Vec<DurableLog>,
    spans: Vec<DurableSpan>,
    points: Vec<DurableMetricPoint>,
    resource: Arc<ResourceContext>,
    label: TelemetryAttribute,
}

impl Corpus {
    fn generate(count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let label = TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("checkout-api")),
        );
        let resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![
                label.clone(),
                TelemetryAttribute::new(
                    "deployment.environment",
                    TelemetryValue::String(Arc::from("production")),
                ),
                TelemetryAttribute::new(
                    "cloud.region",
                    TelemetryValue::String(Arc::from("us-east-1")),
                ),
            ]),
            schema_url: Arc::from("https://opentelemetry.io/schemas/1.37.0"),
            ..ResourceContext::default()
        });
        let scope = Arc::new(ScopeContext {
            name: Arc::from("checkout/http"),
            version: Arc::from("2026.8.3"),
            ..ScopeContext::default()
        });
        let log_partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(3));
        let trace_partition = TopicPartition::new(TRACES_TOPIC_ID, LogicalPartitionId::new(3));
        let metric_partition = TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(3));
        let mut durable_logs = Vec::with_capacity(count);
        let mut spans = Vec::with_capacity(count);
        let mut points = Vec::with_capacity(count);
        let base = 1_785_700_000_000_000_000u64;
        for ordinal in 0..count {
            let trace_id = trace_id(ordinal / 8 + 1)?;
            let span_id = make_span_id(ordinal + 1)?;
            let status = [200, 200, 200, 400, 404, 500, 502, 503][ordinal & 7];
            let route = ["/checkout", "/cart", "/products", "/payment"][ordinal & 3];
            let message = match ordinal & 3 {
                0 => format!(
                    "completed POST {route} status={status} duration_ms={} request_id={:016x}",
                    4 + ordinal % 91,
                    mix(ordinal as u64)
                ),
                1 => format!(
                    "inventory reservation item={} warehouse={} quantity={} trace={trace_id}",
                    ordinal % 10_003,
                    ordinal % 17,
                    1 + ordinal % 5
                ),
                2 => format!(
                    "payment authorization provider=stripe result={} amount_cents={} customer={:012x}",
                    if status < 400 { "approved" } else { "declined" },
                    100 + ordinal % 50_000,
                    mix((ordinal as u64) ^ 0xa5a5)
                ),
                _ => format!(
                    "worker checkpoint partition={} offset={} lag_ms={} node=node-{}",
                    ordinal % 256,
                    ordinal * 97,
                    ordinal % 31,
                    ordinal % 16
                ),
            };
            let attributes = Arc::new(vec![
                TelemetryAttribute::new("http.route", TelemetryValue::String(Arc::from(route))),
                TelemetryAttribute::new(
                    "http.response.status_code",
                    TelemetryValue::Integer(status),
                ),
            ]);
            let fields = Arc::new(vec![
                MetadataField::new("service.name", "checkout-api"),
                MetadataField::new("resource.service.name", "checkout-api"),
                MetadataField::new("attr.http.route", route),
                MetadataField::new("attr.http.response.status_code", status.to_string()),
                MetadataField::new("otel.trace_id", trace_id.to_string()),
                MetadataField::new("otel.span_id", span_id.to_string()),
                MetadataField::new("otel.resource.id", resource.id().to_string()),
                MetadataField::new("otel.scope.id", scope.id().to_string()),
            ]);
            let event = OtlpLogEvent {
                timestamp_unix_nanos: base + ordinal as u64 * 1_000_000,
                observed_timestamp_unix_nanos: base + ordinal as u64 * 1_000_000 + 5_000,
                body: Some(TelemetryValue::String(Arc::from(message.as_str()))),
                message: Arc::from(message),
                fields,
                attributes: Arc::clone(&attributes),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                severity_number: if status >= 500 { 17 } else { 9 },
                severity_text: Arc::from(if status >= 500 { "ERROR" } else { "INFO" }),
                trace_id: Some(trace_id),
                span_id: Some(span_id),
                compression_cohort: CompressionCohortId::new(7),
                ..OtlpLogEvent::default()
            };
            durable_logs.push(event.clone().into_durable(
                ShardId::new(0),
                log_partition,
                LogicalOffset::new(ordinal as u64),
            ));
            spans.push(DurableSpan {
                stream_shard_id: ShardId::new(0),
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Traces,
                    trace_partition,
                    LogicalOffset::new(ordinal as u64),
                ),
                tenant: Arc::from(TENANT),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                trace_id,
                span_id,
                parent_span_id: (ordinal % 8 != 0)
                    .then(|| make_span_id(ordinal))
                    .transpose()?,
                trace_state: Arc::from("vendor=production"),
                flags: 1,
                name: Arc::from(
                    [
                        "POST /checkout",
                        "GET /cart",
                        "GET /products",
                        "POST /payment",
                    ][ordinal & 3],
                ),
                kind: 2,
                start_time_unix_nanos: base + ordinal as u64 * 1_000_000,
                duration_nanos: (4 + ordinal as u64 % 91) * 1_000_000,
                attributes: Arc::clone(&attributes),
                dropped_attributes_count: 0,
                events: Arc::new(Vec::new()),
                dropped_events_count: 0,
                links: Arc::new(Vec::new()),
                dropped_links_count: 0,
                status: Some(SpanStatus {
                    message: Arc::from(if status >= 500 {
                        "upstream failure"
                    } else {
                        ""
                    }),
                    code: if status >= 500 { 2 } else { 1 },
                }),
            });

            let series_ordinal = ordinal % 128;
            let identity = Arc::new(MetricIdentity {
                tenant: Arc::from(TENANT),
                resource: Arc::clone(&resource),
                scope: Arc::clone(&scope),
                name: Arc::from("http.server.request.duration"),
                unit: Arc::from("ms"),
                kind: MetricKind::Gauge,
                point_attributes: Arc::new(vec![
                    TelemetryAttribute::new(
                        "http.route",
                        TelemetryValue::String(Arc::from(
                            ["/checkout", "/cart", "/products", "/payment"][series_ordinal & 3],
                        )),
                    ),
                    TelemetryAttribute::new(
                        "http.response.status_code",
                        TelemetryValue::Integer([200, 400, 404, 500][(series_ordinal / 4) & 3]),
                    ),
                    TelemetryAttribute::new(
                        "instance",
                        TelemetryValue::String(Arc::from(format!("node-{}", series_ordinal % 16))),
                    ),
                ]),
            });
            points.push(DurableMetricPoint {
                stream_shard_id: ShardId::new(0),
                record_ref: TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    metric_partition,
                    LogicalOffset::new(ordinal as u64),
                ),
                identity,
                description: Arc::from("HTTP server request duration"),
                metadata: Arc::new(Vec::new()),
                start_time_unix_nanos: base,
                timestamp_unix_nanos: base + (ordinal / 128) as u64 * 15_000_000_000,
                flags: 0,
                value: MetricValue::Gauge(NumberValue::from_f64(
                    4.0 + ((ordinal * 17) % 910) as f64 / 10.0,
                )),
                exemplars: Arc::new(Vec::new()),
            });
        }
        Ok(Self {
            durable_logs,
            spans,
            points,
            resource,
            label,
        })
    }
}

#[derive(Clone, Copy)]
struct ResultRow {
    source_bytes: usize,
    payload_bytes: usize,
    auxiliary_bytes: usize,
    encode_mib_per_second: f64,
    decode_mib_per_second: f64,
    lookup_count: usize,
    lookup_ops_per_second: f64,
    lookup_p50: Duration,
    lookup_p99: Duration,
}

fn benchmark_logs(
    corpus: &Corpus,
    iterations: usize,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let source_bytes = corpus
        .durable_logs
        .iter()
        .try_fold(0usize, |total, record| {
            canonical_log_bytes(record).map(|bytes| total + bytes)
        })?;
    let start = Instant::now();
    let mut stripe = LogStripe::new(ShardId::new(0), StripeConfig::default())?;
    for record in &corpus.durable_logs {
        stripe.apply_durable(record.clone())?;
    }
    stripe.seal_active_blocks()?;
    let encode_elapsed = start.elapsed();
    let payload_bytes = stripe
        .catalog()
        .iter()
        .map(|block| block.stored_bytes as usize)
        .sum();
    let start = Instant::now();
    let mut decoded_count = 0usize;
    for block in stripe.catalog().iter() {
        let compressed = stripe
            .catalog()
            .staged_payload(block.block_id)
            .ok_or("sealed log block has no staged payload")?;
        let structural = zstd::bulk::decompress(&compressed, block.structural_bytes as usize)?;
        decoded_count += decode_structural_block(&structural)?.len();
    }
    let decode_elapsed = start.elapsed();
    assert_eq!(decoded_count, corpus.durable_logs.len());
    let query = LogQuery::new(corpus.durable_logs[0].record_ref.topic_partition)
        .with_field("service.name", "checkout-api")
        .with_term("completed")
        .with_limit(100);
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p99) =
        measure_lookup(iterations, || stripe.query(&query).len());
    Ok(ResultRow {
        source_bytes,
        payload_bytes,
        auxiliary_bytes: 0,
        encode_mib_per_second: mib_per_second(source_bytes, encode_elapsed),
        decode_mib_per_second: mib_per_second(source_bytes, decode_elapsed),
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p99,
    })
}

fn benchmark_traces(
    corpus: &Corpus,
    iterations: usize,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let source_bytes = serialized_bytes(&corpus.spans)?;
    let grouped = group_trace_blocks(&corpus.spans)?;
    let start = Instant::now();
    let encoded = grouped
        .iter()
        .map(|spans| {
            let payload = encode_trace_block(spans)?;
            let filter = serde_json::to_vec(&CorrelationBlockFilter::for_spans(spans))?;
            Ok::<_, Box<dyn std::error::Error>>((payload, filter.len()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let encode_elapsed = start.elapsed();
    let payload_bytes = encoded.iter().map(|(payload, _)| payload.len()).sum();
    let auxiliary_bytes = encoded.iter().map(|(_, bytes)| *bytes).sum();
    let start = Instant::now();
    let decoded_count = encoded.iter().try_fold(0usize, |count, (block, _)| {
        decode_trace_block(block).map(|spans| count + spans.len())
    })?;
    let decode_elapsed = start.elapsed();
    assert_eq!(decoded_count, corpus.spans.len());

    let mut stripe = TraceStripe::new(512 * 1024 * 1024)?;
    for span in &corpus.spans {
        stripe.apply(span.clone(), span.start_time_unix_nanos)?;
    }
    let query = TraceQuery {
        tenant: Arc::from(TENANT),
        trace_id: Some(corpus.spans[corpus.spans.len() / 2].trace_id),
        limit: 32,
        ..TraceQuery::default()
    };
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p99) =
        measure_lookup(iterations, || {
            stripe.query(&query).map_or(0, |value| value.len())
        });
    Ok(ResultRow {
        source_bytes,
        payload_bytes,
        auxiliary_bytes,
        encode_mib_per_second: mib_per_second(source_bytes, encode_elapsed),
        decode_mib_per_second: mib_per_second(source_bytes, decode_elapsed),
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p99,
    })
}

fn benchmark_metrics(
    corpus: &Corpus,
    iterations: usize,
) -> Result<ResultRow, Box<dyn std::error::Error>> {
    let source_bytes = serialized_bytes(&corpus.points)?;
    let mut series = BTreeMap::<SeriesFingerprint, Vec<DurableMetricPoint>>::new();
    for point in &corpus.points {
        series
            .entry(point.series_fingerprint())
            .or_default()
            .push(point.clone());
    }
    let start = Instant::now();
    let encoded = series
        .values()
        .map(|points| {
            let payload = encode_metric_chunk(points)?;
            let filter = serde_json::to_vec(&CorrelationBlockFilter::for_metrics(points))?;
            Ok::<_, Box<dyn std::error::Error>>((payload, filter.len()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let encode_elapsed = start.elapsed();
    let payload_bytes = encoded.iter().map(|(payload, _)| payload.len()).sum();
    let auxiliary_bytes = encoded.iter().map(|(_, bytes)| *bytes).sum();
    let start = Instant::now();
    let decoded_count = encoded.iter().try_fold(0usize, |count, (chunk, _)| {
        decode_metric_chunk(chunk).map(|points| count + points.len())
    })?;
    let decode_elapsed = start.elapsed();
    assert_eq!(decoded_count, corpus.points.len());

    let mut stripe = MetricStripe::new(512 * 1024 * 1024)?;
    for point in &corpus.points {
        stripe.apply(point.clone(), MetricIngestProtocol::Otlp)?;
    }
    let selected = &corpus.points[corpus.points.len() / 2];
    let query = MetricQuery {
        tenant: Arc::from(TENANT),
        series: Some(selected.series_fingerprint()),
        limit: 100,
        ..MetricQuery::default()
    };
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p99) =
        measure_lookup(iterations, || {
            stripe.query(&query).map_or(0, |value| value.len())
        });
    Ok(ResultRow {
        source_bytes,
        payload_bytes,
        auxiliary_bytes,
        encode_mib_per_second: mib_per_second(source_bytes, encode_elapsed),
        decode_mib_per_second: mib_per_second(source_bytes, decode_elapsed),
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p99,
    })
}

fn benchmark_correlations(corpus: &Corpus, iterations: usize) -> ResultRow {
    let mut index = CorrelationIndex::new(CorrelationConfig {
        max_keys: corpus.spans.len().saturating_mul(8),
        max_refs_per_key: corpus.spans.len().saturating_mul(3),
        max_total_refs: corpus.spans.len().saturating_mul(32),
    });
    for log in &corpus.durable_logs {
        index.index_log(TENANT, log);
    }
    for span in &corpus.spans {
        index.index_span(span);
    }
    for point in &corpus.points {
        index.index_metric(point);
    }
    let query = CorrelationQuery::new(TENANT)
        .with_resource_id(corpus.resource.id())
        .with_attribute(&corpus.label)
        .with_limit(1_000);
    let (lookup_count, lookup_ops_per_second, lookup_p50, lookup_p99) =
        measure_lookup(iterations, || index.query(&query).len());
    ResultRow {
        source_bytes: 0,
        payload_bytes: 0,
        auxiliary_bytes: 0,
        encode_mib_per_second: 0.0,
        decode_mib_per_second: 0.0,
        lookup_count,
        lookup_ops_per_second,
        lookup_p50,
        lookup_p99,
    }
}

fn serialized_bytes<T: serde::Serialize>(records: &[T]) -> Result<usize, rmp_serde::encode::Error> {
    records.iter().try_fold(0usize, |total, record| {
        Ok(total + rmp_serde::to_vec(record)?.len())
    })
}

fn canonical_log_bytes(record: &DurableLog) -> Result<usize, rmp_serde::encode::Error> {
    let fields = record
        .fields
        .iter()
        .map(|field| 16 + field.key.len() + field.value.len())
        .sum::<usize>();
    Ok(8 * 8
        + record.message.len()
        + fields
        + rmp_serde::to_vec(&record.body)?.len()
        + rmp_serde::to_vec(&record.attributes)?.len()
        + rmp_serde::to_vec(&record.resource)?.len()
        + rmp_serde::to_vec(&record.scope)?.len()
        + record.severity_text.len()
        + record.event_name.len())
}

fn measure_lookup(
    mut iterations: usize,
    mut lookup: impl FnMut() -> usize,
) -> (usize, f64, Duration, Duration) {
    let warm_count = black_box(lookup());
    let mut samples = Vec::with_capacity(iterations);
    let started = Instant::now();
    while iterations > 0 {
        let start = Instant::now();
        black_box(lookup());
        samples.push(start.elapsed());
        iterations -= 1;
    }
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
    (
        warm_count,
        samples.len() as f64 / elapsed.as_secs_f64(),
        p50,
        p99,
    )
}

fn print_result(signal: &str, result: ResultRow) {
    let stored_bytes = result.payload_bytes + result.auxiliary_bytes;
    println!(
        "{signal} source_bytes={} payload_bytes={} auxiliary_bytes={} stored_bytes={} ratio={:.2}x encode_mib_s={:.2} decode_mib_s={:.2} lookup_results={} lookup_ops_s={:.2} p50_us={:.3} p99_us={:.3}",
        result.source_bytes,
        result.payload_bytes,
        result.auxiliary_bytes,
        stored_bytes,
        result.source_bytes as f64 / stored_bytes as f64,
        result.encode_mib_per_second,
        result.decode_mib_per_second,
        result.lookup_count,
        result.lookup_ops_per_second,
        result.lookup_p50.as_secs_f64() * 1e6,
        result.lookup_p99.as_secs_f64() * 1e6,
    );
}

fn group_trace_blocks(
    spans: &[DurableSpan],
) -> Result<Vec<Vec<DurableSpan>>, Box<dyn std::error::Error>> {
    let mut traces = BTreeMap::<(Arc<str>, TraceId), Vec<DurableSpan>>::new();
    for span in spans {
        traces
            .entry((Arc::clone(&span.tenant), span.trace_id))
            .or_default()
            .push(span.clone());
    }
    let mut blocks = Vec::new();
    let mut block = Vec::new();
    let mut block_bytes = 0usize;
    for trace in traces.into_values() {
        let trace_bytes = serialized_bytes(&trace)?;
        if !block.is_empty() && block_bytes.saturating_add(trace_bytes) > TRACE_BLOCK_SOURCE_BYTES {
            blocks.push(std::mem::take(&mut block));
            block_bytes = 0;
        }
        block_bytes = block_bytes.saturating_add(trace_bytes);
        block.extend(trace);
    }
    if !block.is_empty() {
        blocks.push(block);
    }
    Ok(blocks)
}

fn mib_per_second(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

fn trace_id(value: usize) -> Result<TraceId, Box<dyn std::error::Error>> {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&(value as u64).to_be_bytes());
    Ok(TraceId::from_bytes(bytes)?)
}

fn make_span_id(value: usize) -> Result<SpanId, Box<dyn std::error::Error>> {
    Ok(SpanId::from_bytes((value as u64).to_be_bytes())?)
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
