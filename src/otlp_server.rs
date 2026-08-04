use std::collections::BTreeMap;
use std::future::Future;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
};
use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response as GrpcResponse, Status};

use crate::{
    DurableLokiStore, NativePartitionAppend, NativeTelemetryBatch, OtlpLogDecoder,
    OtlpTelemetryDecoder, ProductionRuntime, ServiceState, ShardTelemetryConfig, TelemetryError,
    TelemetryResult, TelemetryRouter, prepare_log_envelope, prepare_metric_envelope,
    prepare_trace_envelope,
};

/// Production OTLP receiver limits and single-tenant identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpReceiverConfig {
    /// Authenticated tenant assigned to OTLP data.
    pub tenant: Arc<str>,
    /// Maximum decompressed request body. Defaults to 64 MiB.
    pub max_request_bytes: usize,
    /// Per-signal partition and bounded storage configuration.
    pub signals: ShardTelemetryConfig,
    /// Wait for owner-stripe query visibility before acknowledging.
    pub wait_for_index: bool,
}

impl Default for OtlpReceiverConfig {
    fn default() -> Self {
        Self {
            tenant: Arc::from("default"),
            max_request_bytes: 64 * 1024 * 1024,
            signals: ShardTelemetryConfig::default(),
            wait_for_index: true,
        }
    }
}

impl OtlpReceiverConfig {
    fn validate(&self) -> TelemetryResult<()> {
        self.signals.validate()?;
        if self.tenant.is_empty() {
            return Err(TelemetryError::InvalidConfiguration(
                "OTLP tenant must not be empty".into(),
            ));
        }
        if self.max_request_bytes == 0 || self.max_request_bytes > 64 * 1024 * 1024 {
            return Err(TelemetryError::InvalidConfiguration(
                "OTLP request limit must be in 1..=64 MiB".into(),
            ));
        }
        Ok(())
    }
}

/// Shared OTLP transport implementation backed by the durable telemetry store.
#[derive(Clone)]
pub struct OtlpIngestService {
    store: Arc<DurableLokiStore>,
    config: OtlpReceiverConfig,
    router: TelemetryRouter,
    production: Option<Arc<ProductionRuntime>>,
}

impl std::fmt::Debug for OtlpIngestService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OtlpIngestService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OtlpIngestService {
    /// Creates a transport-independent OTLP ingestion service.
    pub fn new(store: Arc<DurableLokiStore>, config: OtlpReceiverConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let router = TelemetryRouter::from_config(&config.signals);
        Ok(Self {
            store,
            router,
            config,
            production: None,
        })
    }

    /// Attaches the shared fail-closed authentication, lifecycle, and admission runtime.
    #[must_use]
    pub fn with_production(mut self, production: Option<Arc<ProductionRuntime>>) -> Self {
        self.production = production;
        self
    }

    fn ingest_logs(&self, request: ExportLogsServiceRequest) -> Result<usize, String> {
        let events = OtlpLogDecoder
            .decode(&request.encode_to_vec())
            .map_err(|error| error.to_string())?;
        let item_count = events.len();
        let mut partitions = BTreeMap::new();
        for event in events {
            let identity = rmp_serde::to_vec(&(event.resource.as_ref(), event.scope.as_ref()))
                .map_err(|error| error.to_string())?;
            let partition = self
                .router
                .log(&self.config.tenant, event.trace_id, &identity);
            partitions
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(event);
        }
        let mut appends = Vec::with_capacity(partitions.len());
        for (topic_partition, events) in partitions {
            appends.push(NativePartitionAppend {
                topic_partition,
                envelope: prepare_log_envelope(&self.config.tenant, &events)
                    .map_err(|error| error.to_string())?,
            });
        }
        self.append(appends)?;
        Ok(item_count)
    }

    fn ingest_traces(&self, request: ExportTraceServiceRequest) -> Result<usize, String> {
        let decoder = OtlpTelemetryDecoder;
        let events = decoder
            .decode_traces(&self.config.tenant, &request.encode_to_vec())
            .map_err(|error| error.to_string())?;
        let item_count = events.len();
        let partitions = decoder.partition_traces(&self.router, events);
        let mut appends = Vec::with_capacity(partitions.len());
        for (topic_partition, events) in partitions {
            appends.push(NativePartitionAppend {
                topic_partition,
                envelope: prepare_trace_envelope(topic_partition, events)
                    .map_err(|error| error.to_string())?,
            });
        }
        self.append(appends)?;
        Ok(item_count)
    }

    fn ingest_metrics(&self, request: ExportMetricsServiceRequest) -> Result<usize, String> {
        let decoder = OtlpTelemetryDecoder;
        let events = decoder
            .decode_metrics(&self.config.tenant, &request.encode_to_vec())
            .map_err(|error| error.to_string())?;
        let item_count = events.len();
        let partitions = decoder.partition_metrics(&self.router, events);
        let mut appends = Vec::with_capacity(partitions.len());
        for (topic_partition, events) in partitions {
            appends.push(NativePartitionAppend {
                topic_partition,
                envelope: prepare_metric_envelope(topic_partition, events)
                    .map_err(|error| error.to_string())?,
            });
        }
        self.append(appends)?;
        Ok(item_count)
    }

    fn append(&self, partitions: Vec<NativePartitionAppend>) -> Result<(), String> {
        if partitions.is_empty() {
            return Ok(());
        }
        self.store
            .append_telemetry_batch(
                &NativeTelemetryBatch { partitions },
                self.config.wait_for_index,
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn validate_grpc_size<T: Message>(&self, request: &T) -> Result<(), Status> {
        if request.encoded_len() > self.config.max_request_bytes {
            return Err(Status::resource_exhausted(
                "OTLP request exceeds the configured decompressed limit",
            ));
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn authorize_http(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, Response> {
        validate_http_tenant(headers, &self.config.tenant)?;
        let Some(runtime) = &self.production else {
            return Ok(None);
        };
        let authenticated = bearer_token(headers).is_some_and(|token| runtime.authenticates(token));
        if !authenticated {
            if bearer_token(headers).is_none() {
                runtime.record_authentication_failure();
            }
            return Err(otlp_http_error(
                StatusCode::UNAUTHORIZED,
                "valid production bearer token is required",
            ));
        }
        runtime.try_http().map(Some).ok_or_else(|| {
            otlp_http_error(
                StatusCode::TOO_MANY_REQUESTS,
                "HTTP concurrency limit exceeded",
            )
        })
    }

    fn authorize_grpc<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let observed_tenant = request
            .metadata()
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok());
        if observed_tenant.is_some_and(|tenant| tenant != self.config.tenant.as_ref()) {
            return Err(Status::permission_denied(
                "OTLP tenant does not match the authenticated tenant",
            ));
        }
        let Some(runtime) = &self.production else {
            return Ok(());
        };
        let authenticated = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(strip_bearer)
            .is_some_and(|token| runtime.authenticates(token));
        if !authenticated {
            return Err(Status::unauthenticated(
                "valid production bearer token is required",
            ));
        }
        Ok(())
    }

    fn reserve_ingest(
        &self,
        source_bytes: usize,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, StatusCode> {
        let Some(runtime) = &self.production else {
            return Ok(None);
        };
        runtime.try_ingest(source_bytes).map(Some).ok_or_else(|| {
            if runtime.lifecycle().state() == ServiceState::Ready {
                StatusCode::TOO_MANY_REQUESTS
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        })
    }

    fn record_ingest(&self, source_bytes: usize, records: usize) {
        if let Some(runtime) = &self.production {
            runtime.record_ingest(source_bytes, records);
        }
    }
}

/// Builds the OTLP/HTTP router for `/v1/{logs,traces,metrics}`.
///
/// `/otlp/v1/logs` remains as the Loki-compatible log alias.
pub fn otlp_http_router(service: OtlpIngestService) -> Router {
    let max_request_bytes = service.config.max_request_bytes;
    Router::new()
        .route("/v1/logs", post(http_logs))
        .route("/otlp/v1/logs", post(http_logs))
        .route("/v1/traces", post(http_traces))
        .route("/v1/metrics", post(http_metrics))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(service)
}

async fn http_logs(
    State(service): State<OtlpIngestService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    http_export::<ExportLogsServiceRequest, ExportLogsServiceResponse>(
        &service,
        &headers,
        &body,
        OtlpIngestService::ingest_logs,
    )
    .await
}

async fn http_traces(
    State(service): State<OtlpIngestService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    http_export::<ExportTraceServiceRequest, ExportTraceServiceResponse>(
        &service,
        &headers,
        &body,
        OtlpIngestService::ingest_traces,
    )
    .await
}

async fn http_metrics(
    State(service): State<OtlpIngestService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    http_export::<ExportMetricsServiceRequest, ExportMetricsServiceResponse>(
        &service,
        &headers,
        &body,
        OtlpIngestService::ingest_metrics,
    )
    .await
}

async fn http_export<RequestMessage, ResponseMessage>(
    service: &OtlpIngestService,
    headers: &HeaderMap,
    body: &[u8],
    ingest: fn(&OtlpIngestService, RequestMessage) -> Result<usize, String>,
) -> Response
where
    RequestMessage: Message + Default + DeserializeOwned + Send + 'static,
    ResponseMessage: Message + Default + Serialize,
{
    let _http_permit = match service.authorize_http(headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let decoded_body = match decode_http_body(headers, body, service.config.max_request_bytes) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    let request = if json {
        serde_json::from_slice(&decoded_body).map_err(|error| error.to_string())
    } else {
        RequestMessage::decode(decoded_body.as_slice()).map_err(|error| error.to_string())
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => return otlp_http_error(StatusCode::BAD_REQUEST, &error),
    };
    let _ingest_permit = match service.reserve_ingest(decoded_body.len()) {
        Ok(permit) => permit,
        Err(status) => return otlp_http_error(status, "OTLP ingestion is unavailable or limited"),
    };
    let owned_service = service.clone();
    let item_count =
        match tokio::task::spawn_blocking(move || ingest(&owned_service, request)).await {
            Ok(Ok(item_count)) => item_count,
            Ok(Err(error)) => return otlp_http_error(StatusCode::BAD_REQUEST, &error),
            Err(error) => {
                return otlp_http_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("OTLP ingest worker failed: {error}"),
                );
            }
        };
    service.record_ingest(decoded_body.len(), item_count);
    let response = ResponseMessage::default();
    if json {
        match serde_json::to_vec(&response) {
            Ok(body) => (
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                body,
            )
                .into_response(),
            Err(error) => otlp_http_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
        }
    } else {
        (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            )],
            response.encode_to_vec(),
        )
            .into_response()
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(strip_bearer)
}

fn strip_bearer(value: &str) -> Option<&str> {
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
}

#[allow(clippy::result_large_err)]
fn validate_http_tenant(headers: &HeaderMap, configured: &str) -> Result<(), Response> {
    if let Some(observed) = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        && observed != configured
    {
        return Err(otlp_http_error(
            StatusCode::FORBIDDEN,
            "OTLP tenant does not match the authenticated tenant",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn decode_http_body(
    headers: &HeaderMap,
    body: &[u8],
    max_request_bytes: usize,
) -> Result<Vec<u8>, Response> {
    let gzip = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));
    if !gzip {
        if body.len() > max_request_bytes {
            return Err(otlp_http_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "OTLP request exceeds the configured limit",
            ));
        }
        return Ok(body.to_vec());
    }
    let mut decoder = GzDecoder::new(body);
    let mut decoded = Vec::new();
    decoder
        .by_ref()
        .take((max_request_bytes as u64).saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| otlp_http_error(StatusCode::BAD_REQUEST, &error.to_string()))?;
    if decoded.len() > max_request_bytes {
        return Err(otlp_http_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "decompressed OTLP request exceeds the configured limit",
        ));
    }
    Ok(decoded)
}

fn otlp_http_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        serde_json::json!({"code": status.as_u16(), "message": message}).to_string(),
    )
        .into_response()
}

#[tonic::async_trait]
impl LogsService for OtlpIngestService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<GrpcResponse<ExportLogsServiceResponse>, Status> {
        self.authorize_grpc(&request)?;
        self.validate_grpc_size(request.get_ref())?;
        let source_bytes = request.get_ref().encoded_len();
        let _permit = self
            .reserve_ingest(source_bytes)
            .map_err(grpc_admission_status)?;
        let service = self.clone();
        let item_count =
            tokio::task::spawn_blocking(move || service.ingest_logs(request.into_inner()))
                .await
                .map_err(|error| Status::internal(format!("OTLP ingest worker failed: {error}")))?
                .map_err(Status::invalid_argument)?;
        self.record_ingest(source_bytes, item_count);
        Ok(GrpcResponse::new(ExportLogsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpIngestService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<GrpcResponse<ExportTraceServiceResponse>, Status> {
        self.authorize_grpc(&request)?;
        self.validate_grpc_size(request.get_ref())?;
        let source_bytes = request.get_ref().encoded_len();
        let _permit = self
            .reserve_ingest(source_bytes)
            .map_err(grpc_admission_status)?;
        let service = self.clone();
        let item_count =
            tokio::task::spawn_blocking(move || service.ingest_traces(request.into_inner()))
                .await
                .map_err(|error| Status::internal(format!("OTLP ingest worker failed: {error}")))?
                .map_err(Status::invalid_argument)?;
        self.record_ingest(source_bytes, item_count);
        Ok(GrpcResponse::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpIngestService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<GrpcResponse<ExportMetricsServiceResponse>, Status> {
        self.authorize_grpc(&request)?;
        self.validate_grpc_size(request.get_ref())?;
        let source_bytes = request.get_ref().encoded_len();
        let _permit = self
            .reserve_ingest(source_bytes)
            .map_err(grpc_admission_status)?;
        let service = self.clone();
        let item_count =
            tokio::task::spawn_blocking(move || service.ingest_metrics(request.into_inner()))
                .await
                .map_err(|error| Status::internal(format!("OTLP ingest worker failed: {error}")))?
                .map_err(Status::invalid_argument)?;
        self.record_ingest(source_bytes, item_count);
        Ok(GrpcResponse::new(ExportMetricsServiceResponse::default()))
    }
}

fn grpc_admission_status(status: StatusCode) -> Status {
    if status == StatusCode::SERVICE_UNAVAILABLE {
        Status::unavailable("OTLP ingestion is unavailable")
    } else {
        Status::resource_exhausted("OTLP ingestion concurrency or rate limit exceeded")
    }
}

/// Serves OTLP/gRPC logs, traces, and metrics with gzip support.
pub async fn serve_otlp_grpc<F>(
    address: SocketAddr,
    service: OtlpIngestService,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    tonic::transport::Server::builder()
        .add_service(
            LogsServiceServer::new(service.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            TraceServiceServer::new(service.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            MetricsServiceServer::new(service)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve_with_shutdown(address, shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_decoder_enforces_decompressed_limit() {
        use std::io::Write;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&vec![7; 1_025]).unwrap();
        let body = encoder.finish().unwrap();
        let headers =
            HeaderMap::from_iter([(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"))]);
        assert!(decode_http_body(&headers, &body, 1_024).is_err());
    }
}
