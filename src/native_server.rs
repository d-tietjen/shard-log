use std::future::Future;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};

use crate::loki_api::LokiApiError;
use crate::{
    DurableLokiStore, MAX_NATIVE_FRAME_BYTES, NATIVE_FRAME_HEADER_BYTES, NativeFrame,
    NativeFrameHeader, NativeOpcode, NativeStatus, decode_native_query, encode_native_log_batch,
};

/// Runtime limits for the native TCP listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeServerConfig {
    /// Largest accepted payload, bounded by [`MAX_NATIVE_FRAME_BYTES`].
    pub max_frame_bytes: usize,
    /// Maximum requests executing concurrently on one connection.
    pub max_in_flight_per_connection: usize,
    /// Wait for exact-query visibility before acknowledging native appends.
    ///
    /// When false, acknowledgement means the authoritative checksummed
    /// compressed ingest pack is durable and indexing continues under bounded
    /// shard-stream backpressure.
    pub wait_for_index: bool,
}

impl Default for NativeServerConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_NATIVE_FRAME_BYTES,
            max_in_flight_per_connection: 64,
            wait_for_index: true,
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
                tokio::spawn(async move {
                    if let Err(error) = serve_connection(socket, store, config).await
                        && error.kind() != io::ErrorKind::UnexpectedEof
                        && error.kind() != io::ErrorKind::ConnectionReset
                    {
                        eprintln!("native ShardLog connection failed: {error}");
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

    loop {
        let mut encoded_header = [0; NATIVE_FRAME_HEADER_BYTES];
        match reader.read_exact(&mut encoded_header).await {
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
        reader.read_exact(&mut payload).await?;
        header.verify_payload(&payload).map_err(invalid_data)?;
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?;
        let responses = responses.clone();
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let response = if header.is_response || header.status != NativeStatus::Ok {
                error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "clients must send request frames with status OK",
                )
            } else {
                dispatch(header, payload, store, config.wait_for_index).await
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

async fn dispatch(
    header: NativeFrameHeader,
    payload: Vec<u8>,
    store: Arc<DurableLokiStore>,
    wait_for_index: bool,
) -> NativeFrame {
    match header.opcode {
        NativeOpcode::Ping => ok_frame(header, payload),
        NativeOpcode::Append => {
            let append = move || {
                if wait_for_index {
                    store.append_native_batch(payload)
                } else {
                    store.append_native_batch_durable(payload)
                }
            };
            match tokio::task::spawn_blocking(append).await {
                Ok(Ok(ack)) => ok_frame(header, ack.encode().to_vec()),
                Ok(Err(error)) => store_error_frame(header, error),
                Err(error) => error_frame(
                    header,
                    NativeStatus::Internal,
                    &format!("native append worker failed: {error}"),
                ),
            }
        }
        NativeOpcode::Query => {
            let query = match decode_native_query(&payload) {
                Ok(query) => query,
                Err(error) => {
                    return error_frame(header, NativeStatus::BadRequest, &error.to_string());
                }
            };
            let tenant = query.tenant.clone();
            match tokio::task::spawn_blocking(move || store.query_native(&query)).await {
                Ok(Ok(entries)) => match encode_native_log_batch(&tenant, entries) {
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
    let status = if error.is_bad_request() {
        NativeStatus::BadRequest
    } else {
        NativeStatus::Internal
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

    use tokio::net::TcpStream;

    use super::*;
    use crate::{
        DurableLokiConfig, LokiEntry, NativeAppendAck, NativeLogBatch, NativeQuery,
        NativeQueryDirection, StripeConfig, decode_native_log_batch, encode_native_log_batch,
        encode_native_query,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_protocol_pings_appends_and_queries_with_request_ids() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-log-native-server-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableLokiStore::open(DurableLokiConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                recovery_journal: false,
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
        let batch = encode_native_log_batch("tenant-a", vec![entry.clone()]).expect("batch");
        let append = NativeFrame::request(NativeOpcode::Append, 8, batch).expect("append");
        write_frame(&mut client, &append).await;
        let response = read_frame(&mut client).await;
        assert_eq!(response.header.request_id, 8);
        assert_eq!(response.header.status, NativeStatus::Ok);
        assert_eq!(
            NativeAppendAck::decode(&response.payload)
                .expect("ack")
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
        let NativeLogBatch { tenant, entries } =
            decode_native_log_batch(&response.payload).expect("results");
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
