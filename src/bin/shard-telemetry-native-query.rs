use std::collections::BTreeMap;
use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use clap::Parser;
use shard_telemetry::{
    NATIVE_FRAME_HEADER_BYTES, NativeFrame, NativeFrameHeader, NativeOpcode, NativeQuery,
    NativeQueryDirection, NativeStatus, decode_native_log_query_result, encode_native_query,
};

#[derive(Debug, Parser)]
#[command(
    name = "shard-telemetry-native-query",
    about = "Exact indexed-query latency client for ShardTelemetry's native protocol"
)]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 3_101)]
    port: u16,
    #[arg(long)]
    tenant: String,
    #[arg(long = "label", value_parser = parse_pair)]
    labels: Vec<(String, String)>,
    #[arg(long = "term")]
    terms: Vec<String>,
    #[arg(long)]
    start: Option<u64>,
    #[arg(long)]
    end: Option<u64>,
    #[arg(long, default_value_t = 100)]
    limit: u32,
    #[arg(long, default_value_t = false)]
    newest_first: bool,
    #[arg(long, default_value_t = 5)]
    warmup: usize,
    #[arg(long, default_value_t = 100)]
    iterations: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = Arguments::parse();
    if arguments.limit == 0 || arguments.iterations == 0 {
        return Err("limit and iterations must be nonzero".into());
    }
    let labels = arguments.labels.into_iter().collect::<BTreeMap<_, _>>();
    let query = NativeQuery {
        tenant: arguments.tenant,
        labels,
        terms: arguments.terms,
        start_timestamp_unix_nanos: arguments.start,
        end_timestamp_unix_nanos: arguments.end,
        limit: arguments.limit,
        direction: if arguments.newest_first {
            NativeQueryDirection::NewestFirst
        } else {
            NativeQueryDirection::OldestFirst
        },
    };
    let payload = encode_native_query(&query)?;
    let mut stream = TcpStream::connect((arguments.host.as_str(), arguments.port))?;
    stream.set_nodelay(true)?;
    for request_id in 0..arguments.warmup {
        let _ = execute(&mut stream, request_id as u128, &payload)?;
    }
    let mut latencies = Vec::with_capacity(arguments.iterations);
    let mut result_count = None;
    let mut response_bytes = None;
    for iteration in 0..arguments.iterations {
        let started = Instant::now();
        let (records, bytes) = execute(
            &mut stream,
            u128::try_from(arguments.warmup + iteration)?,
            &payload,
        )?;
        latencies.push(started.elapsed());
        if result_count
            .replace(records)
            .is_some_and(|prior| prior != records)
            || response_bytes
                .replace(bytes)
                .is_some_and(|prior| prior != bytes)
        {
            return Err("query results changed between iterations".into());
        }
    }
    latencies.sort_unstable();
    let total = latencies.iter().sum::<Duration>();
    println!("iterations: {}", latencies.len());
    println!("records per response: {}", result_count.unwrap_or(0));
    println!("response payload bytes: {}", response_bytes.unwrap_or(0));
    println!(
        "mean latency ms: {:.6}",
        total.as_secs_f64() * 1_000.0 / latencies.len() as f64
    );
    println!("p50 latency ms: {:.6}", percentile(&latencies, 0.50));
    println!("p95 latency ms: {:.6}", percentile(&latencies, 0.95));
    println!("p99 latency ms: {:.6}", percentile(&latencies, 0.99));
    Ok(())
}

fn execute(
    stream: &mut TcpStream,
    request_id: u128,
    payload: &[u8],
) -> Result<(usize, usize), Box<dyn Error>> {
    let request = NativeFrame::request(NativeOpcode::Query, request_id, payload.to_vec())?;
    stream.write_all(&request.header.encode())?;
    stream.write_all(&request.payload)?;
    let mut header = [0; NATIVE_FRAME_HEADER_BYTES];
    stream.read_exact(&mut header)?;
    let header = NativeFrameHeader::decode(&header)?;
    let mut response = vec![0; header.payload_len as usize];
    stream.read_exact(&mut response)?;
    header.verify_payload(&response)?;
    if !header.is_response
        || header.request_id != request_id
        || header.opcode != NativeOpcode::Query
        || header.status != NativeStatus::Ok
    {
        return Err(format!(
            "query failed with status {:?}: {}",
            header.status,
            String::from_utf8_lossy(&response)
        )
        .into());
    }
    let response_bytes = response.len();
    let result = decode_native_log_query_result(&response)?;
    Ok((result.entries.len(), response_bytes))
}

fn percentile(latencies: &[Duration], percentile: f64) -> f64 {
    let index = ((latencies.len() - 1) as f64 * percentile).round() as usize;
    latencies[index].as_secs_f64() * 1_000.0
}

fn parse_pair(input: &str) -> Result<(String, String), String> {
    let (key, value) = input
        .split_once('=')
        .ok_or_else(|| "labels must use key=value".to_owned())?;
    if key.is_empty() {
        return Err("label keys must not be empty".to_owned());
    }
    Ok((key.to_owned(), value.to_owned()))
}
