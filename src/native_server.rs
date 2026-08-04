use std::future::Future;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};

use crate::loki_api::LokiApiError;
use crate::{
    DurableLokiStore, MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeFrame,
    NativeFrameHeader, NativeOpcode, NativeStatus, NativeTelemetryBatch, ProductionRuntime,
    ServiceState, decode_native_query, encode_native_log_query_result, is_native_telemetry_batch,
};

/// Product-owned admission check evaluated for every native append and query.
///
/// HA distributions use this to fence direct native traffic on followers while
/// allowing an existing connection to follow leadership changes safely.
pub trait NativeRequestGate: Send + Sync + std::fmt::Debug + 'static {
    /// Returns `Ok` only when this process may execute the request.
    fn check(&self) -> Result<(), String>;

    /// Returns `Ok` only when this process may append every routed partition.
    ///
    /// The default preserves coordinator-only gates. HA products override this
    /// method to fence each signal partition after the complete STB2 request is
    /// decoded and before any partition append starts.
    fn check_partitions(&self, _partitions: &[crate::NativePartitionAppend]) -> Result<(), String> {
        self.check()
    }
}

/// Runtime limits for the native TCP listener.
#[derive(Debug, Clone)]
pub struct NativeServerConfig {
    /// Largest accepted payload, bounded by [`MAX_NATIVE_FRAME_BYTES`].
    pub max_frame_bytes: usize,
    /// Maximum requests executing concurrently on one connection.
    pub max_in_flight_per_connection: usize,
    /// Wait for exact-query visibility before acknowledging native appends.
    ///
    /// When false, acknowledgement means the authoritative checksummed
    /// STEL envelope is durable and indexing continues under bounded
    /// shard-stream backpressure.
    pub wait_for_index: bool,
    /// Shared authentication, tenant, lifecycle, and admission controls.
    ///
    /// `None` is intended only for tests and explicit development mode.
    pub production: Option<Arc<ProductionRuntime>>,
    /// Optional product-owned fencing check for append and query operations.
    pub request_gate: Option<Arc<dyn NativeRequestGate>>,
}

impl Default for NativeServerConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_NATIVE_FRAME_BYTES,
            max_in_flight_per_connection: 64,
            wait_for_index: true,
            production: None,
            request_gate: None,
        }
    }
}

impl NativeServerConfig {
    fn validate(self) -> io::Result<Self> {
        if self.max_frame_bytes == 0 || self.max_frame_bytes > MAX_NATIVE_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("native max frame bytes must be in 1..={MAX_NATIVE_FRAME_BYTES}"),
            ));
        }
        if self.max_in_flight_per_connection == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native max in-flight requests must be nonzero",
            ));
        }
        Ok(self)
    }
}

/// Serves multiplexed native connections until `shutdown` resolves.
///
/// Responses may complete out of order and are correlated by the request ID
/// copied from each request frame.
pub async fn serve_native<F>(
    listener: TcpListener,
    store: Arc<DurableLokiStore>,
    config: NativeServerConfig,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()>,
{
    let config = config.validate()?;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                socket.set_nodelay(true)?;
                let store = Arc::clone(&store);
                let config = config.clone();
                let connection_permit = match &config.production {
                    Some(runtime) => match runtime.try_native_connection() {
                        Some(permit) => Some(permit),
                        None => continue,
                    },
                    None => None,
                };
                tokio::spawn(async move {
                    let _connection_permit = connection_permit;
                    if let Err(error) = serve_connection(socket, store, config).await
                        && error.kind() != io::ErrorKind::UnexpectedEof
                        && error.kind() != io::ErrorKind::ConnectionReset
                    {
                        eprintln!("native ShardTelemetry connection failed: {error}");
                    }
                });
            }
        }
    }
}

async fn serve_connection(
    socket: TcpStream,
    store: Arc<DurableLokiStore>,
    config: NativeServerConfig,
) -> io::Result<()> {
    let (mut reader, mut writer) = socket.into_split();
    let (responses, mut response_receiver) =
        mpsc::channel::<NativeFrame>(config.max_in_flight_per_connection);
    let writer_task = tokio::spawn(async move {
        while let Some(response) = response_receiver.recv().await {
            writer.write_all(&response.header.encode()).await?;
            writer.write_all(&response.payload).await?;
        }
        writer.shutdown().await
    });
    let permits = Arc::new(Semaphore::new(config.max_in_flight_per_connection));
    let mut authenticated = config.production.is_none();
    let mut shutdown = config
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().subscribe_shutdown());
    let authentication_deadline = config
        .production
        .as_ref()
        .map(|runtime| tokio::time::Instant::now() + runtime.native_auth_timeout());

    loop {
        let mut encoded_header = [0; NATIVE_FRAME_HEADER_BYTES];
        if shutdown.as_ref().is_some_and(|shutdown| *shutdown.borrow()) {
            break;
        }
        let read = reader.read_exact(&mut encoded_header);
        let read_result = match (shutdown.as_mut(), authenticated) {
            (Some(shutdown), false) => tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep_until(authentication_deadline.expect("production deadline")) => break,
                result = read => result,
            },
            (Some(shutdown), true) => tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                result = read => result,
            },
            (None, _) => read.await,
        };
        match read_result {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let header = NativeFrameHeader::decode(&encoded_header).map_err(invalid_data)?;
        if header.payload_len as usize > config.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native frame exceeds the configured connection limit",
            ));
        }
        let mut payload = vec![0; header.payload_len as usize];
        if !authenticated {
            match shutdown.as_mut() {
                Some(shutdown) => tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep_until(authentication_deadline.expect("production deadline")) => break,
                    result = reader.read_exact(&mut payload) => { result?; }
                },
                None => reader.read_exact(&mut payload).await.map(|_| ())?,
            }
        } else {
            reader.read_exact(&mut payload).await?;
        }
        header.verify_payload(&payload).map_err(invalid_data)?;
        if !authenticated {
            let response = authenticate_frame(header, &payload, &config);
            authenticated = response.header.status == NativeStatus::Ok;
            responses
                .send(response)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native writer stopped"))?;
            if !authenticated {
                break;
            }
            continue;
        }
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?;
        let responses = responses.clone();
        let store = Arc::clone(&store);
        let request_config = config.clone();
        tokio::spawn(async move {
            let response = if header.is_response || header.status != NativeStatus::Ok {
                error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "clients must send request frames with status OK",
                )
            } else {
                dispatch(header, payload, store, &request_config).await
            };
            let _ = responses.send(response).await;
            drop(permit);
        });
    }

    drop(responses);
    writer_task
        .await
        .map_err(|error| io::Error::other(format!("native writer task failed: {error}")))?
}

fn authenticate_frame(
    header: NativeFrameHeader,
    payload: &[u8],
    config: &NativeServerConfig,
) -> NativeFrame {
    if header.is_response || header.status != NativeStatus::Ok {
        return error_frame(
            header,
            NativeStatus::BadRequest,
            "authentication must be a request frame with status OK",
        );
    }
    if header.opcode != NativeOpcode::Authenticate {
        if let Some(runtime) = &config.production {
            runtime.record_authentication_failure();
        }
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "authenticate before sending native operations",
        );
    }
    let Some(runtime) = &config.production else {
        return ok_frame(header, Vec::new());
    };
    let Ok(token) = std::str::from_utf8(payload) else {
        runtime.record_authentication_failure();
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "native credential is not valid UTF-8",
        );
    };
    if runtime.authenticates(token) {
        ok_frame(header, Vec::new())
    } else {
        error_frame(
            header,
            NativeStatus::Unauthorized,
            "invalid native production credential",
        )
    }
}

async fn dispatch(
    header: NativeFrameHeader,
    payload: Vec<u8>,
    store: Arc<DurableLokiStore>,
    config: &NativeServerConfig,
) -> NativeFrame {
    if matches!(header.opcode, NativeOpcode::Query)
        && let Some(gate) = &config.request_gate
        && let Err(error) = gate.check()
    {
        return error_frame(header, NativeStatus::Unavailable, &error);
    }
    match header.opcode {
        NativeOpcode::Ping => ok_frame(header, payload),
        NativeOpcode::Authenticate => error_frame(
            header,
            NativeStatus::BadRequest,
            "connection is already authenticated",
        ),
        NativeOpcode::Append => {
            let runtime = config.production.clone();
            let ingest_permit = match runtime.as_ref() {
                Some(runtime) => match runtime.try_ingest(payload.len()) {
                    Some(permit) => Some(permit),
                    None if runtime.lifecycle().state() != ServiceState::Ready => {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native ingestion is draining or unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native ingest concurrency or rate limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let source_bytes = payload.len();
            if !is_native_telemetry_batch(&payload) {
                return error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "native append requires the signal-aware STB2 payload",
                );
            }
            let telemetry_batch = match NativeTelemetryBatch::decode(&payload) {
                Ok(batch) => batch,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if let Some(gate) = &config.request_gate
                && let Err(error) = gate.check_partitions(&telemetry_batch.partitions)
            {
                return error_frame(header, NativeStatus::Unavailable, &error);
            }
            if let Some(runtime) = &runtime
                && telemetry_batch
                    .partitions
                    .iter()
                    .any(|partition| partition.envelope.tenant.as_ref() != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native telemetry tenant does not match the authenticated tenant",
                );
            }
            let wait_for_index = config.wait_for_index;
            let append = move || {
                let records = telemetry_batch
                    .partitions
                    .iter()
                    .fold(0_u32, |total, partition| {
                        total.saturating_add(partition.envelope.item_count)
                    });
                let result = store
                    .append_telemetry_batch(&telemetry_batch, wait_for_index)
                    .and_then(|ack| {
                        ack.encode()
                            .map(|encoded| (encoded, records))
                            .map_err(|error| LokiApiError::internal(error.to_string()))
                    });
                drop(ingest_permit);
                result
            };
            match tokio::task::spawn_blocking(append).await {
                Ok(Ok((ack, records))) => {
                    if let Some(runtime) = &config.production {
                        runtime.record_ingest(source_bytes, records as usize);
                    }
                    ok_frame(header, ack)
                }
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native append worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::Query => {
            let query_permit = match config.production.as_ref() {
                Some(runtime) => match runtime.try_query() {
                    Some(permit) => {
                        runtime.record_query();
                        Some(permit)
                    }
                    None if matches!(
                        runtime.lifecycle().state(),
                        ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
                    ) =>
                    {
                        return error_frame(
                            header,
                            NativeStatus::Unavailable,
                            "native query service is unavailable",
                        );
                    }
                    None => {
                        return error_frame(
                            header,
                            NativeStatus::TooManyRequests,
                            "native query concurrency limit exceeded",
                        );
                    }
                },
                None => None,
            };
            let query = match decode_native_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            if config
                .production
                .as_ref()
                .is_some_and(|runtime| query.tenant != runtime.tenant())
            {
                return error_frame(
                    header,
                    NativeStatus::Unauthorized,
                    "native query tenant does not match the authenticated tenant",
                );
            }
            let tenant = query.tenant.clone();
            let query_timeout = config
                .production
                .as_ref()
                .map(|runtime| runtime.query_timeout());
            let worker = tokio::task::spawn_blocking(move || {
                let result = store.query_native(&query);
                drop(query_permit);
                result
            });
            let result = match query_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, worker).await {
                    Ok(result) => result,
                    Err(_) => {
                        return error_frame(
                            header,
                            NativeStatus::Timeout,
                            "native query deadline exceeded",
                        );
                    }
                },
                None => worker.await,
            };
            match result {
                Ok(Ok(entries)) => match encode_native_log_query_result(&tenant, entries) {
                    Ok(encoded) => ok_frame(header, encoded),
                    Err(error) => error_frame(header, NativeStatus::Internal, &error.to_string()),
                },
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native query worker failed: {error}"),
                ),
            }
        }
    }
}

fn ok_frame(header: NativeFrameHeader, payload: Vec<u8>) -> NativeFrame {
    NativeFrame::response(header.opcode, header.request_id, NativeStatus::Ok, payload)
        .expect("bounded request produced a bounded response")
}

fn store_error_frame(header: NativeFrameHeader, error: LokiApiError) -> NativeFrame {
    let status = match error.status() {
        axum::http::StatusCode::BAD_REQUEST => NativeStatus::BadRequest,
        axum::http::StatusCode::UNAUTHORIZED | axum::http::StatusCode::FORBIDDEN => {
            NativeStatus::Unauthorized
        }
        axum::http::StatusCode::SERVICE_UNAVAILABLE => NativeStatus::Unavailable,
        axum::http::StatusCode::TOO_MANY_REQUESTS => NativeStatus::TooManyRequests,
        _ => NativeStatus::Internal,
    };
    error_frame(header, status, &error.to_string())
}

fn error_frame(header: NativeFrameHeader, status: NativeStatus, message: &str) -> NativeFrame {
    let mut payload = message.as_bytes();
    if payload.len() > 4_096 {
        payload = &payload[..4_096];
    }
    NativeFrame::response(header.opcode, header.request_id, status, payload.to_vec())
        .expect("error payload is bounded")
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use shard_stream_core::{LogicalPartitionId, TopicPartition};
    use tokio::net::TcpStream;

    use super::*;
    use crate::{
        DurableLokiConfig, LokiEntry, NativeLogQueryResult, NativePartitionAppend, NativeQuery,
        NativeQueryDirection, NativeTelemetryAppendAck, NativeTelemetryBatch, ServiceLifecycle,
        SingleTenantConfig, StripeConfig, decode_native_log_query_result, encode_native_query,
        prepare_loki_log_envelope,
    };

    #[derive(Debug)]
    struct DenyGate;

    impl NativeRequestGate for DenyGate {
        fn check(&self) -> Result<(), String> {
            Err("not the current leader".into())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_protocol_pings_appends_and_queries_with_request_ids() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-server-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableLokiStore::open(DurableLokiConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                recovery_journal: false,
                retention: None,
                shard_count: 2,
                tenant_partitions: 8,
                append_linger: std::time::Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: std::time::Duration::from_secs(30),
            })
            .expect("store"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_store = Arc::clone(&store);
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_store,
                NativeServerConfig::default(),
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        let mut client = TcpStream::connect(address).await.expect("connect");

        let ping = NativeFrame::request(NativeOpcode::Ping, 7, b"hello".to_vec()).expect("ping");
        write_frame(&mut client, &ping).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 7);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(response.payload, b"hello");

        let entry = LokiEntry {
            timestamp_unix_nanos: 100,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "native timeout".to_owned(),
            structured_metadata: BTreeMap::from([("trace".to_owned(), "abc".to_owned())]),
        };
        let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
        let batch = NativeTelemetryBatch {
            partitions: vec![NativePartitionAppend {
                topic_partition,
                envelope: prepare_loki_log_envelope("tenant-a", vec![entry.clone()])
                    .expect("log envelope"),
            }],
        }
        .encode()
        .expect("batch");
        let append = NativeFrame::request(NativeOpcode::Append, 8, batch).expect("append");
        write_frame(&mut client, &append).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 8);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(
            NativeTelemetryAppendAck::decode(&response.payload)
                .expect("ack")
                .partitions[0]
                .first_offset,
            0
        );

        let query = NativeQuery {
            tenant: "tenant-a".to_owned(),
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            terms: vec!["timeout".to_owned()],
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: 10,
            direction: NativeQueryDirection::OldestFirst,
        };
        let query = NativeFrame::request(
            NativeOpcode::Query,
            9,
            encode_native_query(&query).expect("query"),
        )
        .expect("query frame");
        write_frame(&mut client, &query).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 9);
        assert_eq!(response.header.status, NativeStatus::Ok);
        let NativeLogQueryResult { tenant, entries } =
            decode_native_log_query_result(&response.payload).expect("results");
        assert_eq!(tenant, "tenant-a");
        assert_eq!(entries, vec![entry]);

        drop(client);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_native_protocol_requires_authentication_before_operations() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-native-auth-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableLokiStore::open(DurableLokiConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                recovery_journal: false,
                retention: None,
                shard_count: 1,
                tenant_partitions: 1,
                append_linger: std::time::Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: std::time::Duration::from_secs(30),
            })
            .expect("store"),
        );
        let lifecycle = Arc::new(ServiceLifecycle::new());
        lifecycle.mark_ready();
        let runtime = Arc::new(
            ProductionRuntime::new(
                SingleTenantConfig {
                    tenant: Arc::from("tenant-a"),
                    bearer_token: Arc::from("0123456789abcdef"),
                    max_http_in_flight: 4,
                    max_ingest_in_flight: 2,
                    max_query_in_flight: 2,
                    ingest_bytes_per_second: 0,
                    ingest_burst_bytes: 0,
                    max_tail_subscribers: 1,
                    max_native_connections: 4,
                    query_timeout: std::time::Duration::from_secs(30),
                    native_auth_timeout: std::time::Duration::from_millis(50),
                },
                lifecycle,
            )
            .expect("runtime"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_store = Arc::clone(&store);
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_store,
                NativeServerConfig {
                    production: Some(runtime),
                    request_gate: Some(Arc::new(DenyGate)),
                    ..NativeServerConfig::default()
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });

        let mut idle = TcpStream::connect(address).await.expect("idle connect");
        let mut closed = [0_u8; 1];
        let read = tokio::time::timeout(std::time::Duration::from_secs(1), idle.read(&mut closed))
            .await
            .expect("authentication deadline closes idle connection")
            .expect("idle read");
        assert_eq!(read, 0);

        let mut unauthenticated = TcpStream::connect(address).await.expect("connect");
        let ping = NativeFrame::request(NativeOpcode::Ping, 1, b"hello".to_vec()).expect("ping");
        write_frame(&mut unauthenticated, &ping).await;
        let response = read_frame(&mut unauthenticated).await;
        assert_eq!(response.header.status, NativeStatus::Unauthorized);

        let mut client = TcpStream::connect(address).await.expect("connect");
        let authenticate =
            NativeFrame::request(NativeOpcode::Authenticate, 2, b"0123456789abcdef".to_vec())
                .expect("authenticate");
        write_frame(&mut client, &authenticate).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 2);
        assert_eq!(response.header.status, NativeStatus::Ok);

        let ping = NativeFrame::request(NativeOpcode::Ping, 3, b"ready".to_vec()).expect("ping");
        write_frame(&mut client, &ping).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 3);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(response.payload, b"ready");

        let query = NativeFrame::request(NativeOpcode::Query, 4, Vec::new()).expect("query");
        write_frame(&mut client, &query).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 4);
        assert_eq!(response.header.status, NativeStatus::Unavailable);
        assert_eq!(response.payload, b"not the current leader");

        drop(unauthenticated);
        drop(client);
        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(store);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    async fn write_frame(stream: &mut TcpStream, frame: &NativeFrame) {
        stream
            .write_all(&frame.header.encode())
            .await
            .expect("header");
        stream.write_all(&frame.payload).await.expect("payload");
    }

    async fn read_frame(stream: &mut TcpStream) -> NativeFrame {
        let mut header = [0; NATIVE_FRAME_HEADER_BYTES];
        stream.read_exact(&mut header).await.expect("header");
        let header = NativeFrameHeader::decode(&header).expect("decode header");
        let mut payload = vec![0; header.payload_len as usize];
        stream.read_exact(&mut payload).await.expect("payload");
        header.verify_payload(&payload).expect("checksum");
        NativeFrame { header, payload }
    }
}
