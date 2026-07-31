use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use serde::Deserialize;
use shard_log::{
    LokiEntry, NATIVE_FRAME_HEADER_BYTES, NativeAppendAck, NativeFrame, NativeFrameHeader,
    NativeOpcode, NativeStatus, encode_native_log_batch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LoadProtocol {
    Loki,
    Native,
}

#[derive(Debug, Parser)]
#[command(
    name = "shard-log-loki-load",
    about = "Pinned-core Loki push load generator for Docker JSON logs"
)]
struct Arguments {
    /// Immutable Docker json-file log corpus.
    source: PathBuf,
    /// Loki or ShardLog host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Loki or ShardLog HTTP port.
    #[arg(long, default_value_t = 3_100)]
    port: u16,
    /// Wire protocol used by the load generator.
    #[arg(long, value_enum, default_value_t = LoadProtocol::Loki)]
    protocol: LoadProtocol,
    /// Parallel file spans and persistent HTTP connections.
    #[arg(long, default_value_t = 16)]
    workers: usize,
    /// Maximum JSON request body size before a batch is sent.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    batch_bytes: usize,
    /// Native requests allowed in flight on each persistent connection.
    #[arg(long, default_value_t = 1)]
    pipeline_depth: usize,
    /// Loki tenant header.
    #[arg(long, default_value = "benchmark")]
    tenant: String,
    /// Optional source-byte prefix for bounded smoke runs.
    #[arg(long)]
    limit_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DockerRecord {
    log: String,
    #[serde(default)]
    stream: String,
    time: String,
}

#[derive(Debug, Default)]
struct WorkerResult {
    source_bytes: u64,
    records: u64,
    malformed_records: u64,
    pushed_bytes: u64,
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let arguments = Arguments::parse();
    if arguments.workers == 0
        || arguments.batch_bytes < 1_024
        || arguments.pipeline_depth == 0
        || arguments.pipeline_depth > 64
    {
        return Err(
            "workers must be nonzero, batch-bytes must be at least 1024, and pipeline-depth must be in 1..=64"
                .into(),
        );
    }
    let file_bytes = std::fs::metadata(&arguments.source)?.len();
    let source_bytes = arguments
        .limit_bytes
        .map_or(file_bytes, |limit| limit.min(file_bytes));
    let next_request = Arc::new(AtomicU64::new(1));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(arguments.workers);
    for worker in 0..arguments.workers {
        let start = source_bytes.saturating_mul(worker as u64) / arguments.workers as u64;
        let end = source_bytes.saturating_mul((worker + 1) as u64) / arguments.workers as u64;
        let source = arguments.source.clone();
        let host = arguments.host.clone();
        let tenant = arguments.tenant.clone();
        let next_request = Arc::clone(&next_request);
        let port = arguments.port;
        let batch_bytes = arguments.batch_bytes;
        let pipeline_depth = arguments.pipeline_depth;
        let protocol = arguments.protocol;
        workers.push(thread::spawn(move || {
            run_worker(
                &source,
                &host,
                port,
                &tenant,
                worker,
                start,
                end,
                batch_bytes,
                pipeline_depth,
                protocol,
                &next_request,
            )
        }));
    }

    let mut aggregate = WorkerResult::default();
    for worker in workers {
        let result = worker.join().map_err(|_| "Loki load worker panicked")??;
        aggregate.source_bytes += result.source_bytes;
        aggregate.records += result.records;
        aggregate.malformed_records += result.malformed_records;
        aggregate.pushed_bytes += result.pushed_bytes;
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!("source bytes: {}", aggregate.source_bytes);
    println!("records: {}", aggregate.records);
    println!("malformed records skipped: {}", aggregate.malformed_records);
    println!("wire protocol: {:?}", arguments.protocol);
    println!("pushed wire bytes: {}", aggregate.pushed_bytes);
    println!("ingest elapsed seconds: {elapsed:.6}");
    println!(
        "source throughput MiB/s: {:.2}",
        aggregate.source_bytes as f64 / 1_048_576.0 / elapsed
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    source: &PathBuf,
    host: &str,
    port: u16,
    tenant: &str,
    _worker: usize,
    start: u64,
    end: u64,
    batch_bytes: usize,
    pipeline_depth: usize,
    protocol: LoadProtocol,
    next_request: &AtomicU64,
) -> Result<WorkerResult, Box<dyn Error + Send + Sync>> {
    let mut file = File::open(source)?;
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut position = start;
    if start > 0 {
        let mut partial = Vec::new();
        position += reader.read_until(b'\n', &mut partial)? as u64;
    }
    let mut connection = Connection::connect(protocol, host, port)?;
    let mut result = WorkerResult::default();
    let mut batch = LoadBatch::new(protocol, batch_bytes);
    let mut line = Vec::with_capacity(4096);
    while position < end {
        line.clear();
        let bytes = reader.read_until(b'\n', &mut line)?;
        if bytes == 0 {
            break;
        }
        position += bytes as u64;
        result.source_bytes += bytes as u64;
        if !line.ends_with(b"\n") && position >= end {
            break;
        }
        let record: DockerRecord = match serde_json::from_slice(&line) {
            Ok(record) => record,
            Err(_) => {
                result.malformed_records = result.malformed_records.saturating_add(1);
                continue;
            }
        };
        let timestamp = parse_docker_timestamp(&record.time)?;
        batch.push(timestamp, record)?;
        result.records += 1;
        if batch.estimated_bytes() >= batch_bytes {
            result.pushed_bytes +=
                connection.push(tenant, &mut batch, next_request, pipeline_depth)? as u64;
        }
    }
    if !batch.is_empty() {
        result.pushed_bytes +=
            connection.push(tenant, &mut batch, next_request, pipeline_depth)? as u64;
    }
    connection.finish()?;
    Ok(result)
}

enum LoadBatch {
    Loki {
        values: String,
        records: usize,
    },
    Native {
        entries: Vec<LokiEntry>,
        estimated_bytes: usize,
    },
}

impl LoadBatch {
    fn new(protocol: LoadProtocol, capacity: usize) -> Self {
        match protocol {
            LoadProtocol::Loki => Self::Loki {
                values: String::with_capacity(capacity + 1024),
                records: 0,
            },
            LoadProtocol::Native => Self::Native {
                entries: Vec::with_capacity(capacity / 256),
                estimated_bytes: 0,
            },
        }
    }

    fn push(
        &mut self,
        timestamp: u64,
        record: DockerRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Self::Loki { values, records } => {
                if *records > 0 {
                    values.push(',');
                }
                values.push_str("[\"");
                values.push_str(&timestamp.to_string());
                values.push_str("\",");
                values.push_str(&serde_json::to_string(&record.log)?);
                if !record.stream.is_empty() {
                    values.push_str(",{\"docker_stream\":");
                    values.push_str(&serde_json::to_string(&record.stream)?);
                    values.push('}');
                }
                values.push(']');
                *records += 1;
            }
            Self::Native {
                entries,
                estimated_bytes,
            } => {
                let timestamp_unix_nanos =
                    i64::try_from(timestamp).map_err(|_| "timestamp exceeds i64")?;
                let metadata_bytes = record.stream.len();
                *estimated_bytes = estimated_bytes
                    .saturating_add(record.log.len())
                    .saturating_add(metadata_bytes)
                    .saturating_add(32);
                entries.push(LokiEntry {
                    timestamp_unix_nanos,
                    labels: BTreeMap::from([("source".to_owned(), "clickhouse-docker".to_owned())]),
                    line: record.log,
                    structured_metadata: if record.stream.is_empty() {
                        BTreeMap::new()
                    } else {
                        BTreeMap::from([("docker_stream".to_owned(), record.stream)])
                    },
                });
            }
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Loki { values, .. } => values.len(),
            Self::Native {
                estimated_bytes, ..
            } => *estimated_bytes,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Loki { records, .. } => *records == 0,
            Self::Native { entries, .. } => entries.is_empty(),
        }
    }
}

enum Connection {
    Loki(HttpConnection),
    Native(NativeConnection),
}

impl Connection {
    fn connect(
        protocol: LoadProtocol,
        host: &str,
        port: u16,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match protocol {
            LoadProtocol::Loki => HttpConnection::connect(host, port).map(Self::Loki),
            LoadProtocol::Native => NativeConnection::connect(host, port).map(Self::Native),
        }
    }

    fn push(
        &mut self,
        tenant: &str,
        batch: &mut LoadBatch,
        next_request: &AtomicU64,
        pipeline_depth: usize,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        match (self, batch) {
            (
                Self::Loki(connection),
                LoadBatch::Loki {
                    values, records, ..
                },
            ) => {
                let pushed = connection.push(tenant, values, next_request)?;
                values.clear();
                *records = 0;
                Ok(pushed)
            }
            (
                Self::Native(connection),
                LoadBatch::Native {
                    entries,
                    estimated_bytes,
                },
            ) => {
                let pushed = connection.push(tenant, std::mem::take(entries), next_request)?;
                *estimated_bytes = 0;
                if connection.pending() >= pipeline_depth {
                    connection.receive()?;
                }
                Ok(pushed)
            }
            _ => Err("connection and load batch protocols disagree".into()),
        }
    }

    fn finish(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            Self::Loki(_) => Ok(()),
            Self::Native(connection) => {
                while connection.pending() > 0 {
                    connection.receive()?;
                }
                Ok(())
            }
        }
    }
}

struct HttpConnection {
    host: String,
    stream: BufReader<TcpStream>,
}

impl HttpConnection {
    fn connect(host: &str, port: u16) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let socket = TcpStream::connect((host, port))?;
        socket.set_nodelay(true)?;
        Ok(Self {
            host: host.to_owned(),
            stream: BufReader::new(socket),
        })
    }

    fn push(
        &mut self,
        tenant: &str,
        values: &str,
        next_request: &AtomicU64,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let request_id = next_request.fetch_add(1, Ordering::Relaxed);
        let prefix = r#"{"streams":[{"stream":{"source":"clickhouse-docker"},"values":["#;
        let suffix = "]}]}";
        let content_length = prefix.len() + values.len() + suffix.len();
        write!(
            self.stream.get_mut(),
            "POST /loki/api/v1/push HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nX-Scope-OrgID: {}\r\nX-ShardLog-Request-ID: {}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}{}{}",
            self.host,
            tenant,
            request_id,
            content_length,
            prefix,
            values,
            suffix
        )?;
        self.stream.get_mut().flush()?;

        let mut status = String::new();
        self.stream.read_line(&mut status)?;
        let accepted = status.starts_with("HTTP/1.1 204") || status.starts_with("HTTP/1.1 200");
        let mut content_bytes = 0usize;
        loop {
            let mut header = String::new();
            self.stream.read_line(&mut header)?;
            if header == "\r\n" || header.is_empty() {
                break;
            }
            if let Some(value) = header
                .split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim())
            {
                content_bytes = value.parse()?;
            }
        }
        let mut response = vec![0; content_bytes];
        self.stream.read_exact(&mut response)?;
        if !accepted {
            return Err(format!(
                "push failed with {}: {}",
                status.trim(),
                String::from_utf8_lossy(&response)
            )
            .into());
        }
        Ok(content_length)
    }
}

struct NativeConnection {
    stream: TcpStream,
    pending_request_ids: Vec<u128>,
}

impl NativeConnection {
    fn connect(host: &str, port: u16) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            pending_request_ids: Vec::new(),
        })
    }

    fn push(
        &mut self,
        tenant: &str,
        entries: Vec<LokiEntry>,
        next_request: &AtomicU64,
    ) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let request_id = u128::from(next_request.fetch_add(1, Ordering::Relaxed));
        let payload = encode_native_log_batch(tenant, entries)?;
        let request = NativeFrame::request(NativeOpcode::Append, request_id, payload)?;
        self.stream.write_all(&request.header.encode())?;
        self.stream.write_all(&request.payload)?;
        self.pending_request_ids.push(request_id);
        Ok(NATIVE_FRAME_HEADER_BYTES + request.payload.len())
    }

    fn receive(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut header = [0; NATIVE_FRAME_HEADER_BYTES];
        self.stream.read_exact(&mut header)?;
        let header = NativeFrameHeader::decode(&header)?;
        let mut response = vec![0; header.payload_len as usize];
        self.stream.read_exact(&mut response)?;
        header.verify_payload(&response)?;
        if !header.is_response
            || header.opcode != NativeOpcode::Append
            || header.status != NativeStatus::Ok
        {
            return Err(format!(
                "native push failed with status {:?}: {}",
                header.status,
                String::from_utf8_lossy(&response)
            )
            .into());
        }
        let Some(pending_index) = self
            .pending_request_ids
            .iter()
            .position(|request_id| *request_id == header.request_id)
        else {
            return Err(format!(
                "native push returned unknown request id {}",
                header.request_id
            )
            .into());
        };
        self.pending_request_ids.swap_remove(pending_index);
        let _ = NativeAppendAck::decode(&response)?;
        Ok(())
    }

    fn pending(&self) -> usize {
        self.pending_request_ids.len()
    }
}

fn parse_docker_timestamp(input: &str) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let bytes = input.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(format!("unsupported Docker timestamp: {input}").into());
    }
    let year = parse_digits(&bytes[0..4])? as i64;
    let month = parse_digits(&bytes[5..7])? as u32;
    let day = parse_digits(&bytes[8..10])? as u32;
    let hour = parse_digits(&bytes[11..13])?;
    let minute = parse_digits(&bytes[14..16])?;
    let second = parse_digits(&bytes[17..19])?;
    let fraction = match &bytes[19..] {
        [b'Z'] => 0,
        [b'.', digits @ .., b'Z'] if !digits.is_empty() && digits.len() <= 9 => {
            parse_digits(digits)?
                .checked_mul(10u64.pow((9 - digits.len()) as u32))
                .ok_or("timestamp fraction overflows")?
        }
        _ => return Err(format!("unsupported Docker timestamp timezone: {input}").into()),
    };
    let days = days_from_civil(year, month, day);
    if days < 0 || hour >= 24 || minute >= 60 || second >= 60 {
        return Err(format!("invalid Docker timestamp: {input}").into());
    }
    (days as u64)
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
        .and_then(|value| value.checked_mul(1_000_000_000))
        .and_then(|value| value.checked_add(fraction))
        .ok_or_else(|| "timestamp nanoseconds overflow".into())
}

fn parse_digits(bytes: &[u8]) -> Result<u64, Box<dyn Error + Send + Sync>> {
    bytes.iter().try_fold(0u64, |value, byte| {
        if !byte.is_ascii_digit() {
            return Err("timestamp contains a non-digit".into());
        }
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or_else(|| "timestamp number overflows".into())
    })
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
