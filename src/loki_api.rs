use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, RawQuery, Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use prost::Message;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::deletion::DeleteCatalog;
use crate::{AnalyticsLogRow, AnalyticsScanRequest, DeleteRequest};
use crate::{ProductionRuntime, ServiceState};

const DEFAULT_TENANT: &str = "fake";
const DEFAULT_QUERY_LIMIT: usize = 100;
const MAX_QUERY_LIMIT: usize = 5_000;

/// Configuration for the Loki-compatible HTTP boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiApiConfig {
    /// Tenant used when Loki multi-tenancy headers are absent.
    pub default_tenant: Arc<str>,
    /// Largest materialized result accepted by query APIs.
    pub max_query_limit: usize,
}

impl Default for LokiApiConfig {
    fn default() -> Self {
        Self {
            default_tenant: Arc::from(DEFAULT_TENANT),
            max_query_limit: MAX_QUERY_LIMIT,
        }
    }
}

/// One normalized Loki entry accepted by the compatibility boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiEntry {
    /// Nanosecond Unix timestamp.
    pub timestamp_unix_nanos: i64,
    /// Stream labels used by LogQL selectors.
    pub labels: BTreeMap<String, String>,
    /// Original log line.
    pub line: String,
    /// Structured metadata attached to the entry.
    pub structured_metadata: BTreeMap<String, String>,
}

/// Health snapshot supplied by a Loki storage backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreHealth {
    /// Whether durable writes and indexed reads can be served safely.
    pub ready: bool,
    /// Bounded operator-facing explanation when readiness is false.
    pub detail: Arc<str>,
}

impl Default for StoreHealth {
    fn default() -> Self {
        Self {
            ready: true,
            detail: Arc::from("ready"),
        }
    }
}

/// Storage counters rendered with protocol counters by `/metrics`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreMetrics {
    /// Durable sink work waiting to be indexed.
    pub pending_items: u64,
    /// Bytes represented by pending durable sink work.
    pub pending_bytes: u64,
    /// Age in milliseconds of the oldest pending checkpoint.
    pub checkpoint_age_ms: u64,
    /// Durable appends applied to the log index.
    pub applied_appends: u64,
    /// Durable sink retries.
    pub retry_attempts: u64,
    /// Durable sink failures.
    pub failed_attempts: u64,
    /// Partitions requiring explicit recovery.
    pub dirty_partitions: u64,
    /// Source payload bytes still retained in shard-stream.
    pub retained_payload_bytes: Option<u64>,
    /// Completed batch-aligned retention passes.
    pub retention_runs: u64,
    /// Logical offsets made eligible for pack reclamation.
    pub retention_advanced_offsets: u64,
    /// Failed retention passes.
    pub retention_failures: u64,
}

#[derive(Debug, Default)]
struct TenantStore {
    entries: Vec<LokiEntry>,
}

/// Storage contract used by the Loki-compatible protocol boundary.
pub trait LokiStore: Send + Sync + std::fmt::Debug {
    /// Atomically accepts one normalized push for a tenant.
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError>;

    /// Returns entries for a tenant. Exact query filtering is performed by the
    /// protocol evaluator after storage-level pruning.
    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError>;

    /// Streams bounded batches through the analytical columnar boundary.
    ///
    /// Durable stores override this method to push constraints into their
    /// stripe indexes. Reference stores retain exact behavior through this
    /// entry-based implementation.
    fn scan_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsLogRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        crate::analytics::scan_entries(self.entries(&request.tenant)?, request, emit)
    }

    /// Returns a bounded health snapshot without scanning stored records.
    fn health(&self) -> Result<StoreHealth, LokiApiError> {
        Ok(StoreHealth::default())
    }

    /// Synchronizes accepted durable writes and their query-visible indexes.
    fn flush(&self, _timeout: Duration) -> Result<(), LokiApiError> {
        Ok(())
    }

    /// Returns a bounded lock-free storage counter snapshot.
    fn operational_metrics(&self) -> StoreMetrics {
        StoreMetrics::default()
    }

    /// Durably records one validated logical deletion request.
    fn create_delete(
        &self,
        _tenant: &str,
        _start_time: i64,
        _end_time: i64,
        _query: String,
        _created_at: i64,
    ) -> Result<String, LokiApiError> {
        Err(LokiApiError::configuration(
            "the configured store does not support deletion",
        ))
    }

    /// Returns active logical deletion requests for one tenant.
    fn delete_requests(&self, _tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        Ok(Vec::new())
    }

    /// Durably cancels an active logical deletion request.
    fn cancel_delete(&self, _tenant: &str, _request_id: &str) -> Result<bool, LokiApiError> {
        Ok(false)
    }
}

/// Thread-safe in-memory reference backend used by differential API tests.
#[derive(Debug, Default)]
pub struct LokiApiStore {
    tenants: RwLock<HashMap<String, TenantStore>>,
    deletes: DeleteCatalog,
}

impl LokiApiStore {
    /// Appends normalized entries to one tenant.
    fn append(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        let mut tenants = self
            .tenants
            .write()
            .map_err(|_| LokiApiError::internal("tenant store lock is poisoned"))?;
        let store = tenants.entry(tenant.to_owned()).or_default();
        store.entries.extend(entries);
        store
            .entries
            .sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
        Ok(())
    }

    fn snapshot(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let tenants = self
            .tenants
            .read()
            .map_err(|_| LokiApiError::internal("tenant store lock is poisoned"))?;
        Ok(tenants
            .get(tenant)
            .map(|store| store.entries.clone())
            .unwrap_or_default())
    }
}

impl LokiStore for LokiApiStore {
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        self.append(tenant, entries)
    }

    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let mut entries = self.snapshot(tenant)?;
        apply_logical_deletes(&mut entries, &self.deletes.list(tenant)?)?;
        Ok(entries)
    }

    fn create_delete(
        &self,
        tenant: &str,
        start_time: i64,
        end_time: i64,
        query: String,
        created_at: i64,
    ) -> Result<String, LokiApiError> {
        self.deletes
            .create(tenant, start_time, end_time, query, created_at)
    }

    fn delete_requests(&self, tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        self.deletes.list(tenant)
    }

    fn cancel_delete(&self, tenant: &str, request_id: &str) -> Result<bool, LokiApiError> {
        self.deletes.cancel(tenant, request_id)
    }
}

/// Builds the stable Loki 3.7-compatible HTTP route surface.
pub fn loki_router(store: Arc<dyn LokiStore>, api_config: LokiApiConfig) -> Router {
    build_loki_router(store, api_config, None, None, Duration::from_secs(30), true)
}

/// Builds the Loki surface plus the authenticated ClickHouse Arrow scan route.
///
/// The analytical route is deliberately absent from [`loki_router`]. Servers
/// must opt in with a non-empty bearer token loaded from a protected source.
pub fn loki_router_with_clickhouse(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    bearer_token: Arc<str>,
) -> Result<Router, LokiApiError> {
    if bearer_token.is_empty() {
        return Err(LokiApiError::configuration(
            "ClickHouse bearer token must not be empty",
        ));
    }
    Ok(build_loki_router(
        store,
        api_config,
        Some(bearer_token),
        None,
        Duration::from_secs(30),
        true,
    ))
}

/// Builds the fail-closed single-tenant Loki surface used by the standalone server.
pub fn single_tenant_loki_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    runtime: Arc<ProductionRuntime>,
    analytics_bearer_token: Option<Arc<str>>,
    flush_timeout: Duration,
) -> Result<Router, LokiApiError> {
    if flush_timeout.is_zero() {
        return Err(LokiApiError::configuration(
            "production flush timeout must be nonzero",
        ));
    }
    if let Some(token) = analytics_bearer_token.as_deref()
        && token.is_empty()
    {
        return Err(LokiApiError::configuration(
            "ClickHouse bearer token must not be empty",
        ));
    }
    Ok(build_loki_router(
        store,
        api_config,
        analytics_bearer_token,
        Some(runtime),
        flush_timeout,
        true,
    ))
}

/// Builds the authenticated Loki/OTLP compatibility routes for embedding in a
/// host which already owns health, readiness, metrics, and shutdown routes.
pub fn single_tenant_loki_api_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    runtime: Arc<ProductionRuntime>,
    analytics_bearer_token: Option<Arc<str>>,
    flush_timeout: Duration,
) -> Result<Router, LokiApiError> {
    if flush_timeout.is_zero() {
        return Err(LokiApiError::configuration(
            "production flush timeout must be nonzero",
        ));
    }
    Ok(build_loki_router(
        store,
        api_config,
        analytics_bearer_token,
        Some(runtime),
        flush_timeout,
        false,
    ))
}

fn build_loki_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    analytics_bearer_token: Option<Arc<str>>,
    production: Option<Arc<ProductionRuntime>>,
    flush_timeout: Duration,
    include_operational_routes: bool,
) -> Router {
    let (live, _) = broadcast::channel(1_024);
    let analytics_enabled = analytics_bearer_token.is_some();
    let state = ApiState {
        store,
        config: api_config,
        live,
        analytics_bearer_token,
        production,
        flush_timeout,
    };
    let router = Router::new()
        .route("/loki/api/v1/status/buildinfo", get(build_info))
        .route("/loki/api/v1/push", post(push_logs))
        .route("/otlp/v1/logs", post(push_otlp))
        .route("/loki/api/v1/query", get(query_instant).post(query_instant))
        .route(
            "/loki/api/v1/query_range",
            get(query_range).post(query_range),
        )
        .route("/loki/api/v1/labels", get(labels).post(labels))
        .route(
            "/loki/api/v1/label/{name}/values",
            get(label_values).post(label_values),
        )
        .route("/loki/api/v1/series", get(series).post(series))
        .route(
            "/loki/api/v1/index/stats",
            get(index_stats).post(index_stats),
        )
        .route(
            "/loki/api/v1/index/volume",
            get(index_volume).post(index_volume),
        )
        .route(
            "/loki/api/v1/index/volume_range",
            get(index_volume_range).post(index_volume_range),
        )
        .route("/loki/api/v1/patterns", get(patterns).post(patterns))
        .route(
            "/loki/api/v1/detected_fields",
            get(detected_fields).post(detected_fields),
        )
        .route(
            "/loki/api/v1/detected_field/{name}/values",
            get(detected_field_values).post(detected_field_values),
        )
        .route("/loki/api/v1/tail", get(tail))
        .route(
            "/loki/api/v1/delete",
            get(list_deletes)
                .post(create_delete)
                .put(create_delete)
                .delete(cancel_delete),
        )
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query),
        )
        .route("/api/prom/push", post(push_logs))
        .route("/api/prom/query", get(query_range))
        .route("/api/prom/label", get(labels))
        .route("/api/prom/label/{name}/values", get(label_values))
        .route("/api/prom/series", get(series))
        .route("/api/prom/tail", get(tail));
    let router = if include_operational_routes {
        router
            .route("/ready", get(ready))
            .route("/metrics", get(metrics))
            .route("/config", get(current_config))
            .route("/services", get(services))
            .route("/log_level", get(log_level).post(log_level))
            .route("/flush", post(flush))
            .route("/ingester/prepare_shutdown", post(prepare_shutdown))
            .route("/ingester/shutdown", post(shutdown))
    } else {
        router
    };
    let router = if analytics_enabled {
        router.route("/shardlog/api/v1/clickhouse/scan", get(clickhouse_scan))
    } else {
        router
    };
    router
        .layer(middleware::from_fn_with_state(
            state.clone(),
            production_gate,
        ))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state)
}

#[derive(Clone)]
struct ApiState {
    store: Arc<dyn LokiStore>,
    config: LokiApiConfig,
    live: broadcast::Sender<LivePush>,
    analytics_bearer_token: Option<Arc<str>>,
    production: Option<Arc<ProductionRuntime>>,
    flush_timeout: Duration,
}

async fn production_gate(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    let Some(runtime) = state.production.as_ref() else {
        return next.run(request).await;
    };
    let path = request.uri().path();
    if matches!(path, "/ready" | "/metrics") {
        return next.run(request).await;
    }
    let analytics = path == "/shardlog/api/v1/clickhouse/scan";
    if !analytics {
        let observed = bearer_token(request.headers());
        let authenticated = observed.is_some_and(|observed| runtime.authenticates(observed));
        if !authenticated {
            if observed.is_none() {
                runtime.record_authentication_failure();
            }
            return LokiApiError::unauthorized("valid production bearer token is required")
                .into_response();
        }
    }
    if request
        .headers()
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|tenant| tenant != runtime.tenant())
    {
        return LokiApiError::forbidden("tenant header does not match the configured tenant")
            .into_response();
    }
    let Some(_http_permit) = runtime.try_http() else {
        return LokiApiError::too_many_requests("HTTP concurrency limit exceeded").into_response();
    };
    let query_path = analytics
        || path.starts_with("/loki/api/v1/query")
        || path.starts_with("/loki/api/v1/label")
        || path.starts_with("/loki/api/v1/series")
        || path.starts_with("/loki/api/v1/index")
        || path.starts_with("/loki/api/v1/patterns")
        || path.starts_with("/loki/api/v1/detected")
        || path.starts_with("/api/prom/query")
        || path.starts_with("/api/prom/label")
        || path.starts_with("/api/prom/series");
    let _query_permit = if query_path {
        let Some(permit) = runtime.try_query() else {
            if matches!(
                runtime.lifecycle().state(),
                ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
            ) {
                return LokiApiError::unavailable("query service is unavailable").into_response();
            }
            return LokiApiError::too_many_requests("query concurrency limit exceeded")
                .into_response();
        };
        runtime.record_query();
        Some(permit)
    } else {
        None
    };
    if query_path {
        match tokio::time::timeout(runtime.query_timeout(), next.run(request)).await {
            Ok(response) => response,
            Err(_) => LokiApiError::timeout("query deadline exceeded").into_response(),
        }
    } else {
        next.run(request).await
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

async fn clickhouse_scan(
    State(state): State<ApiState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, LokiApiError> {
    let expected = state
        .analytics_bearer_token
        .as_deref()
        .ok_or_else(|| LokiApiError::not_found("analytical scan route is disabled"))?;
    if !authorized_bearer(&headers, expected) {
        return Err(LokiApiError::unauthorized(
            "valid ClickHouse bearer token is required",
        ));
    }
    let request = crate::analytics::parse_scan_request(
        tenant(&headers, &state.config),
        raw_query.as_deref(),
    )?;
    Ok(crate::analytics::arrow_stream_response(
        state.store,
        request,
    ))
}

fn authorized_bearer(headers: &HeaderMap, expected: &str) -> bool {
    let Some(observed) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return false;
    };
    let observed = blake3::hash(observed.as_bytes());
    let expected = blake3::hash(expected.as_bytes());
    observed
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[derive(Debug, Clone)]
struct LivePush {
    tenant: String,
    entries: Vec<LokiEntry>,
}

/// HTTP-boundary failure with a stable response status and safe message.
#[derive(Debug)]
pub struct LokiApiError {
    status: StatusCode,
    message: String,
}

impl std::fmt::Display for LokiApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LokiApiError {}

impl LokiApiError {
    /// Creates a retryable storage/control-plane availability error for an
    /// embedded backend implementation.
    pub fn backend_unavailable(message: impl Into<String>) -> Self {
        Self::unavailable(message)
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: message.into(),
        }
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    fn timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::internal(message)
    }

    pub(crate) const fn status(&self) -> StatusCode {
        self.status
    }
}

impl IntoResponse for LokiApiError {
    fn into_response(self) -> Response {
        let error_type = match self.status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::UNAUTHORIZED => "unauthorized",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::TOO_MANY_REQUESTS => "rate_limited",
            StatusCode::SERVICE_UNAVAILABLE => "unavailable",
            StatusCode::GATEWAY_TIMEOUT => "timeout",
            _ => "internal",
        };
        (
            self.status,
            Json(json!({
                "status": "error",
                "errorType": error_type,
                "error": self.message,
            })),
        )
            .into_response()
    }
}

#[derive(Debug, Deserialize)]
struct JsonPushRequest {
    streams: Vec<JsonPushStream>,
}

#[derive(Debug, Deserialize)]
struct JsonPushStream {
    stream: BTreeMap<String, String>,
    values: Vec<Vec<Value>>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<ProtoStream>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoStream {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<ProtoEntry>,
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoEntry {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<prost_types::Timestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<ProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    parsed: Vec<ProtoLabelPair>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLabelPair {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

async fn push_logs(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, LokiApiError> {
    let source_bytes = body.len();
    let _ingest_permit = production_ingest_permit(&state, source_bytes)?;
    let tenant = tenant(&headers, &state.config);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let json = content_type.starts_with("application/json");
    let store = Arc::clone(&state.store);
    let durable_tenant = tenant.clone();
    let entries = tokio::task::spawn_blocking(move || {
        let entries = if json {
            decode_json_push(&body)?
        } else {
            decode_protobuf_push(&body)?
        };
        store.push(&durable_tenant, entries.clone())?;
        Ok::<_, LokiApiError>(entries)
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("ingest worker failed: {error}")))??;
    if let Some(runtime) = &state.production {
        runtime.record_ingest(source_bytes, entries.len());
    }
    let _ = state.live.send(LivePush { tenant, entries });
    Ok(StatusCode::NO_CONTENT)
}

fn decode_json_push(body: &[u8]) -> Result<Vec<LokiEntry>, LokiApiError> {
    let request: JsonPushRequest = serde_json::from_slice(body)
        .map_err(|error| LokiApiError::bad_request(format!("invalid push JSON: {error}")))?;
    let mut entries = Vec::new();
    for stream in request.streams {
        validate_labels(&stream.stream)?;
        let labels = normalize_stream_labels(stream.stream);
        for value in stream.values {
            if !(2..=3).contains(&value.len()) {
                return Err(LokiApiError::bad_request(
                    "push value must contain timestamp, line, and optional metadata",
                ));
            }
            let timestamp = value[0]
                .as_str()
                .ok_or_else(|| LokiApiError::bad_request("push timestamp must be a string"))?
                .parse::<i64>()
                .map_err(|_| LokiApiError::bad_request("push timestamp is not an integer"))?;
            let line = value[1]
                .as_str()
                .ok_or_else(|| LokiApiError::bad_request("push line must be a string"))?
                .to_owned();
            let metadata = value
                .get(2)
                .map(parse_metadata)
                .transpose()?
                .unwrap_or_default();
            entries.push(LokiEntry {
                timestamp_unix_nanos: timestamp,
                labels: labels.clone(),
                line,
                structured_metadata: metadata,
            });
        }
    }
    Ok(entries)
}

fn decode_protobuf_push(body: &[u8]) -> Result<Vec<LokiEntry>, LokiApiError> {
    let decoded = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|error| LokiApiError::bad_request(format!("invalid Snappy payload: {error}")))?;
    let request = ProtoPushRequest::decode(decoded.as_slice())
        .map_err(|error| LokiApiError::bad_request(format!("invalid push protobuf: {error}")))?;
    let mut entries = Vec::new();
    for stream in request.streams {
        let labels = normalize_stream_labels(parse_label_set(&stream.labels)?);
        for entry in stream.entries {
            let timestamp = entry
                .timestamp
                .ok_or_else(|| LokiApiError::bad_request("push entry has no timestamp"))?;
            let nanos = timestamp
                .seconds
                .checked_mul(1_000_000_000)
                .and_then(|seconds| seconds.checked_add(i64::from(timestamp.nanos)))
                .ok_or_else(|| LokiApiError::bad_request("push timestamp is out of range"))?;
            entries.push(LokiEntry {
                timestamp_unix_nanos: nanos,
                labels: labels.clone(),
                line: entry.line,
                structured_metadata: entry
                    .structured_metadata
                    .into_iter()
                    .map(|pair| (pair.name, pair.value))
                    .collect(),
            });
        }
    }
    Ok(entries)
}

fn parse_metadata(value: &Value) -> Result<BTreeMap<String, String>, LokiApiError> {
    let object = value
        .as_object()
        .ok_or_else(|| LokiApiError::bad_request("structured metadata must be an object"))?;
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_owned()))
                .ok_or_else(|| {
                    LokiApiError::bad_request("structured metadata values must be strings")
                })
        })
        .collect()
}

async fn push_otlp(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, LokiApiError> {
    let source_bytes = body.len();
    let _ingest_permit = production_ingest_permit(&state, source_bytes)?;
    let tenant = tenant(&headers, &state.config);
    let store = Arc::clone(&state.store);
    let durable_tenant = tenant.clone();
    let entries = tokio::task::spawn_blocking(move || {
        let events = crate::OtlpLogDecoder
            .decode(&body)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let entries = events
            .into_iter()
            .map(|event| {
                let mut labels = BTreeMap::new();
                let mut structured_metadata = BTreeMap::new();
                for field in event.fields.iter() {
                    let key = field.key.to_string();
                    let value = field.value.to_string();
                    if key == "service.name" || key == "resource.service.name" {
                        labels.insert("service_name".to_owned(), value);
                    } else {
                        structured_metadata.insert(normalize_otlp_name(&key), value);
                    }
                }
                LokiEntry {
                    timestamp_unix_nanos: event.timestamp_unix_nanos.min(i64::MAX as u64) as i64,
                    labels: normalize_stream_labels(labels),
                    line: event.message.to_string(),
                    structured_metadata,
                }
            })
            .collect::<Vec<_>>();
        store.push(&durable_tenant, entries.clone())?;
        Ok::<_, LokiApiError>(entries)
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("OTLP ingest worker failed: {error}")))??;
    if let Some(runtime) = &state.production {
        runtime.record_ingest(source_bytes, entries.len());
    }
    let _ = state.live.send(LivePush { tenant, entries });
    Ok(StatusCode::OK)
}

fn production_ingest_permit(
    state: &ApiState,
    source_bytes: usize,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, LokiApiError> {
    let Some(runtime) = &state.production else {
        return Ok(None);
    };
    runtime.try_ingest(source_bytes).map(Some).ok_or_else(|| {
        if runtime.lifecycle().state() != ServiceState::Ready {
            LokiApiError::unavailable("ingestion is draining or unavailable")
        } else {
            LokiApiError::too_many_requests("ingest concurrency or rate limit exceeded")
        }
    })
}

fn normalize_otlp_name(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Debug, Default, Deserialize)]
struct QueryParams {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    time: Option<String>,
    since: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
    step: Option<String>,
    line_limit: Option<usize>,
    field_limit: Option<usize>,
}

async fn query_range(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    execute_query(state, headers, params, QueryMode::Range).await
}

async fn query_instant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    execute_query(state, headers, params, QueryMode::Instant).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryMode {
    Instant,
    Range,
}

async fn execute_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
    mode: QueryMode,
) -> Result<Json<Value>, LokiApiError> {
    let expression = params
        .query
        .as_deref()
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    if expression.trim_start().starts_with('{') {
        execute_stream_query(state, headers, params).await
    } else {
        execute_metric_query(state, headers, params, mode).await
    }
}

fn merge_form_query_params(
    headers: &HeaderMap,
    mut url: QueryParams,
    body: &[u8],
) -> Result<QueryParams, LokiApiError> {
    if body.is_empty() {
        return Ok(url);
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type
        .split(';')
        .next()
        .is_none_or(|value| value.trim() != "application/x-www-form-urlencoded")
    {
        return Err(LokiApiError::bad_request(
            "POST query bodies must use application/x-www-form-urlencoded",
        ));
    }
    let form: QueryParams = serde_urlencoded::from_bytes(body)
        .map_err(|error| LokiApiError::bad_request(format!("invalid form body: {error}")))?;
    macro_rules! overlay {
        ($($field:ident),+ $(,)?) => {
            $(if form.$field.is_some() { url.$field = form.$field; })+
        };
    }
    overlay!(
        query,
        start,
        end,
        time,
        since,
        limit,
        direction,
        step,
        line_limit,
        field_limit,
    );
    Ok(url)
}

async fn execute_stream_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, LokiApiError> {
    let started = std::time::Instant::now();
    let expression = params
        .query
        .as_deref()
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    let selector = parse_log_query(expression)?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let limit = params
        .limit
        .unwrap_or(DEFAULT_QUERY_LIMIT)
        .min(state.config.max_query_limit);
    let backward = params.direction.as_deref().unwrap_or("backward") != "forward";
    let scanned = entries_for(&state, &tenant).await?;
    let total_lines_processed = scanned.len();
    let total_bytes_processed = scanned.iter().map(|entry| entry.line.len()).sum::<usize>();
    let mut entries = scanned
        .into_iter()
        .filter_map(|entry| {
            (entry.timestamp_unix_nanos >= start && entry.timestamp_unix_nanos <= end)
                .then(|| selector.process(entry))
                .flatten()
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
    if backward {
        entries.reverse();
    }
    entries.truncate(limit);
    let total_entries_returned = entries.len();

    let mut streams: BTreeMap<BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
    for entry in entries {
        let mut response_labels = entry.labels;
        response_labels.extend(entry.structured_metadata);
        let level = detected_level(&response_labels, &entry.line);
        response_labels
            .entry("detected_level".to_owned())
            .or_insert(level);
        let value = vec![
            Value::String(entry.timestamp_unix_nanos.to_string()),
            Value::String(entry.line),
        ];
        streams
            .entry(response_labels)
            .or_default()
            .push(Value::Array(value));
    }
    let result = streams
        .into_iter()
        .map(|(stream, values)| json!({ "stream": stream, "values": values }))
        .collect::<Vec<_>>();
    let elapsed = started.elapsed().as_secs_f64();
    Ok(Json(success(
        "streams",
        result,
        query_stats(
            total_bytes_processed,
            total_lines_processed,
            total_entries_returned,
            elapsed,
        ),
    )))
}

#[derive(Debug, Clone)]
enum MetricExpression {
    Scalar(f64),
    Range {
        operation: RangeOperation,
        selector: LogSelector,
        window_nanos: i64,
        parameter: Option<f64>,
    },
    Aggregate {
        operation: AggregateOperation,
        grouping: Option<MetricGrouping>,
        parameter: Option<usize>,
        expression: Box<Self>,
    },
    Binary {
        operation: BinaryOperation,
        bool_mode: bool,
        matching: VectorMatching,
        left: Box<Self>,
        right: Box<Self>,
    },
}

#[derive(Debug, Clone, Copy)]
enum RangeOperation {
    Count,
    Rate,
    Bytes,
    BytesRate,
    Absent,
    Sum,
    Average,
    Minimum,
    Maximum,
    Stddev,
    Stdvar,
    Quantile,
    First,
    Last,
    RateCounter,
}

#[derive(Debug, Clone, Copy)]
enum AggregateOperation {
    Sum,
    Average,
    Minimum,
    Maximum,
    Count,
    Stddev,
    Stdvar,
    TopK,
    BottomK,
    Sort,
    SortDescending,
}

#[derive(Debug, Clone)]
enum MetricGrouping {
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    And,
    Or,
    Unless,
}

#[derive(Debug, Clone, Default)]
struct VectorMatching {
    on: Option<Vec<String>>,
    ignoring: Vec<String>,
    cardinality: VectorCardinality,
    include: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum VectorCardinality {
    #[default]
    OneToOne,
    ManyToOne,
    OneToMany,
}

#[derive(Debug, Clone)]
struct MetricSample {
    labels: BTreeMap<String, String>,
    value: f64,
}

#[derive(Debug, Clone)]
enum MetricValue {
    Scalar(f64),
    Vector(Vec<MetricSample>),
}

async fn execute_metric_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
    mode: QueryMode,
) -> Result<Json<Value>, LokiApiError> {
    let started = std::time::Instant::now();
    let expression = parse_metric_expression(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let tenant = tenant(&headers, &state.config);
    let entries = entries_for(&state, &tenant).await?;
    let total_lines_processed = entries.len();
    let total_bytes_processed = entries.iter().map(|entry| entry.line.len()).sum::<usize>();
    let (_, end) = query_range_bounds(&params)?;
    let evaluation_times = match mode {
        QueryMode::Instant => vec![end],
        QueryMode::Range => metric_evaluation_times(&params, state.config.max_query_limit)?,
    };
    let limit = params
        .limit
        .unwrap_or(state.config.max_query_limit)
        .min(state.config.max_query_limit);
    let mut series = BTreeMap::<BTreeMap<String, String>, Vec<Value>>::new();
    let mut scalar = None;
    for timestamp in evaluation_times {
        match evaluate_metric_expression(&expression, &entries, timestamp)? {
            MetricValue::Scalar(value) => scalar = Some((timestamp, value)),
            MetricValue::Vector(mut values) => {
                values.sort_by(|left, right| left.labels.cmp(&right.labels));
                values.truncate(limit);
                for sample in values {
                    series.entry(sample.labels).or_default().push(json!([
                        timestamp as f64 / 1_000_000_000.0,
                        format_metric_value(sample.value)
                    ]));
                }
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let returned = series.values().map(Vec::len).sum();
    let stats = query_stats(
        total_bytes_processed,
        total_lines_processed,
        returned,
        elapsed,
    );
    if let Some((timestamp, value)) = scalar {
        return Ok(Json(json!({
            "status": "success",
            "data": {
                "resultType": "scalar",
                "result": [
                    timestamp as f64 / 1_000_000_000.0,
                    format_metric_value(value)
                ],
                "stats": stats
            }
        })));
    }
    let result_type = if mode == QueryMode::Instant {
        "vector"
    } else {
        "matrix"
    };
    let result = series
        .into_iter()
        .map(|(metric, mut values)| {
            if mode == QueryMode::Instant {
                json!({"metric": metric, "value": values.pop().unwrap_or(Value::Null)})
            } else {
                json!({"metric": metric, "values": values})
            }
        })
        .collect::<Vec<_>>();
    Ok(Json(success(result_type, result, stats)))
}

fn metric_evaluation_times(
    params: &QueryParams,
    maximum_points: usize,
) -> Result<Vec<i64>, LokiApiError> {
    let (start, end) = query_range_bounds(params)?;
    let span = end.saturating_sub(start);
    let default_step = (span / 250).max(1_000_000_000);
    let step = match params.step.as_deref() {
        Some(value) => parse_positive_step(value)?,
        None => default_step,
    };
    let point_count = usize::try_from(span.div_euclid(step).saturating_add(1))
        .map_err(|_| LokiApiError::bad_request("metric query has too many evaluation points"))?;
    if point_count > maximum_points {
        return Err(LokiApiError::bad_request(format!(
            "metric query exceeds the {maximum_points}-point limit"
        )));
    }
    Ok((0..point_count)
        .map(|index| {
            start.saturating_add(step.saturating_mul(i64::try_from(index).unwrap_or(i64::MAX)))
        })
        .collect())
}

fn parse_positive_step(value: &str) -> Result<i64, LokiApiError> {
    let step = parse_duration_nanos(value)
        .or_else(|| {
            value
                .parse::<f64>()
                .ok()
                .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                .map(|seconds| (seconds * 1_000_000_000.0) as i64)
        })
        .filter(|step| *step > 0)
        .ok_or_else(|| LokiApiError::bad_request("step must be a positive duration"))?;
    Ok(step)
}

fn parse_metric_expression(input: &str) -> Result<MetricExpression, LokiApiError> {
    let input = strip_outer_parentheses(input.trim());
    if let Some((index, operator)) = find_top_level_binary_operator(input) {
        let left = parse_metric_expression(&input[..index])?;
        let (bool_mode, matching, right) = parse_vector_matching(
            &input[index + operator.len()..],
            binary_operation(operator)?,
        )?;
        return Ok(MetricExpression::Binary {
            operation: binary_operation(operator)?,
            bool_mode,
            matching,
            left: Box::new(left),
            right: Box::new(parse_metric_expression(right)?),
        });
    }
    if let Some(expression) = parse_aggregate_expression(input)? {
        return Ok(expression);
    }
    if let Some(expression) = parse_range_expression(input)? {
        return Ok(expression);
    }
    if let Ok(value) = input.parse::<f64>()
        && value.is_finite()
    {
        return Ok(MetricExpression::Scalar(value));
    }
    Err(LokiApiError::bad_request(
        "unsupported or invalid metric LogQL expression",
    ))
}

fn parse_aggregate_expression(input: &str) -> Result<Option<MetricExpression>, LokiApiError> {
    const OPERATIONS: [(&str, AggregateOperation); 13] = [
        ("sort_desc", AggregateOperation::SortDescending),
        ("bottomk", AggregateOperation::BottomK),
        ("stddev", AggregateOperation::Stddev),
        ("stdvar", AggregateOperation::Stdvar),
        ("topk", AggregateOperation::TopK),
        ("count", AggregateOperation::Count),
        ("average", AggregateOperation::Average),
        ("avg", AggregateOperation::Average),
        ("maximum", AggregateOperation::Maximum),
        ("max", AggregateOperation::Maximum),
        ("minimum", AggregateOperation::Minimum),
        ("min", AggregateOperation::Minimum),
        ("sum", AggregateOperation::Sum),
    ];
    for (name, operation) in OPERATIONS {
        let Some(mut rest) = input.strip_prefix(name) else {
            continue;
        };
        if rest
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            continue;
        }
        rest = rest.trim_start();
        let grouping = if let Some(after) = rest.strip_prefix("by") {
            let (labels, remaining) = parse_grouping_clause(after)?;
            rest = remaining;
            Some(MetricGrouping::By(labels))
        } else if let Some(after) = rest.strip_prefix("without") {
            let (labels, remaining) = parse_grouping_clause(after)?;
            rest = remaining;
            Some(MetricGrouping::Without(labels))
        } else {
            None
        };
        let (arguments, trailing) = extract_parenthesized(rest.trim_start())?;
        if !trailing.trim().is_empty() {
            return Err(LokiApiError::bad_request(
                "aggregation expression has trailing input",
            ));
        }
        let (parameter, expression) = if matches!(
            operation,
            AggregateOperation::TopK | AggregateOperation::BottomK
        ) {
            let (parameter, expression) = split_top_level_once(arguments, ',')
                .ok_or_else(|| LokiApiError::bad_request("topk/bottomk requires k and a vector"))?;
            let parameter = parameter
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| LokiApiError::bad_request("topk/bottomk k must be positive"))?;
            (Some(parameter), expression)
        } else {
            (None, arguments)
        };
        return Ok(Some(MetricExpression::Aggregate {
            operation,
            grouping,
            parameter,
            expression: Box::new(parse_metric_expression(expression)?),
        }));
    }
    if let Some(arguments) = function_arguments(input, "sort")? {
        return Ok(Some(MetricExpression::Aggregate {
            operation: AggregateOperation::Sort,
            grouping: None,
            parameter: None,
            expression: Box::new(parse_metric_expression(arguments)?),
        }));
    }
    Ok(None)
}

fn parse_range_expression(input: &str) -> Result<Option<MetricExpression>, LokiApiError> {
    const OPERATIONS: [(&str, RangeOperation); 13] = [
        ("count_over_time", RangeOperation::Count),
        ("bytes_over_time", RangeOperation::Bytes),
        ("bytes_rate", RangeOperation::BytesRate),
        ("absent_over_time", RangeOperation::Absent),
        ("sum_over_time", RangeOperation::Sum),
        ("avg_over_time", RangeOperation::Average),
        ("min_over_time", RangeOperation::Minimum),
        ("max_over_time", RangeOperation::Maximum),
        ("stddev_over_time", RangeOperation::Stddev),
        ("stdvar_over_time", RangeOperation::Stdvar),
        ("first_over_time", RangeOperation::First),
        ("last_over_time", RangeOperation::Last),
        ("rate_counter", RangeOperation::RateCounter),
    ];
    if let Some(arguments) = function_arguments(input, "rate")? {
        return parse_range_source(arguments, RangeOperation::Rate, None).map(Some);
    }
    if let Some(arguments) = function_arguments(input, "quantile_over_time")? {
        let (quantile, source) = split_top_level_once(arguments, ',').ok_or_else(|| {
            LokiApiError::bad_request("quantile_over_time requires q and a range")
        })?;
        let quantile = quantile
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
            .ok_or_else(|| LokiApiError::bad_request("quantile must be between zero and one"))?;
        return parse_range_source(source, RangeOperation::Quantile, Some(quantile)).map(Some);
    }
    for (name, operation) in OPERATIONS {
        if let Some(arguments) = function_arguments(input, name)? {
            return parse_range_source(arguments, operation, None).map(Some);
        }
    }
    Ok(None)
}

fn parse_range_source(
    input: &str,
    operation: RangeOperation,
    parameter: Option<f64>,
) -> Result<MetricExpression, LokiApiError> {
    let input = input.trim();
    let open = find_range_open(input)
        .ok_or_else(|| LokiApiError::bad_request("range function requires [duration]"))?;
    if !input.ends_with(']') {
        return Err(LokiApiError::bad_request("range duration is unterminated"));
    }
    let window_nanos = parse_duration_nanos(input[open + 1..input.len() - 1].trim())
        .filter(|duration| *duration > 0)
        .ok_or_else(|| LokiApiError::bad_request("range duration must be positive"))?;
    let selector = parse_log_query(input[..open].trim())?;
    let unwrap = selector
        .stages
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)));
    if matches!(
        operation,
        RangeOperation::Sum
            | RangeOperation::Average
            | RangeOperation::Minimum
            | RangeOperation::Maximum
            | RangeOperation::Stddev
            | RangeOperation::Stdvar
            | RangeOperation::Quantile
            | RangeOperation::First
            | RangeOperation::Last
            | RangeOperation::RateCounter
    ) && !unwrap
    {
        return Err(LokiApiError::bad_request(
            "unwrapped range function requires an unwrap stage",
        ));
    }
    Ok(MetricExpression::Range {
        operation,
        selector,
        window_nanos,
        parameter,
    })
}

fn evaluate_metric_expression(
    expression: &MetricExpression,
    entries: &[LokiEntry],
    timestamp: i64,
) -> Result<MetricValue, LokiApiError> {
    match expression {
        MetricExpression::Scalar(value) => Ok(MetricValue::Scalar(*value)),
        MetricExpression::Range {
            operation,
            selector,
            window_nanos,
            parameter,
        } => evaluate_range_metric(
            *operation,
            selector,
            *window_nanos,
            *parameter,
            entries,
            timestamp,
        ),
        MetricExpression::Aggregate {
            operation,
            grouping,
            parameter,
            expression,
        } => evaluate_aggregate(
            *operation,
            grouping.as_ref(),
            *parameter,
            evaluate_metric_expression(expression, entries, timestamp)?,
        ),
        MetricExpression::Binary {
            operation,
            bool_mode,
            matching,
            left,
            right,
        } => evaluate_binary(
            *operation,
            *bool_mode,
            matching,
            evaluate_metric_expression(left, entries, timestamp)?,
            evaluate_metric_expression(right, entries, timestamp)?,
        ),
    }
}

fn evaluate_range_metric(
    operation: RangeOperation,
    selector: &LogSelector,
    window_nanos: i64,
    parameter: Option<f64>,
    entries: &[LokiEntry],
    timestamp: i64,
) -> Result<MetricValue, LokiApiError> {
    let start = timestamp.saturating_sub(window_nanos);
    let unwrap_label = selector.stages.iter().find_map(|stage| match stage {
        PipelineStage::Unwrap(label) => Some(label.as_str()),
        _ => None,
    });
    let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
    for entry in entries {
        if entry.timestamp_unix_nanos <= start || entry.timestamp_unix_nanos > timestamp {
            continue;
        }
        let Some(processed) = selector.process(entry.clone()) else {
            continue;
        };
        let value = match operation {
            RangeOperation::Count | RangeOperation::Rate | RangeOperation::Absent => 1.0,
            RangeOperation::Bytes | RangeOperation::BytesRate => processed.line.len() as f64,
            _ => {
                let label = unwrap_label.expect("validated unwrapped operation");
                let Some(raw) = processed
                    .labels
                    .get(label)
                    .or_else(|| processed.structured_metadata.get(label))
                else {
                    continue;
                };
                let Some(value) = parse_unwrapped_value(raw) else {
                    continue;
                };
                value
            }
        };
        groups
            .entry(processed.labels)
            .or_default()
            .push((processed.timestamp_unix_nanos, value));
    }
    if matches!(operation, RangeOperation::Absent) {
        return Ok(MetricValue::Vector(if groups.is_empty() {
            vec![MetricSample {
                labels: BTreeMap::new(),
                value: 1.0,
            }]
        } else {
            Vec::new()
        }));
    }
    let seconds = window_nanos as f64 / 1_000_000_000.0;
    let samples = groups
        .into_iter()
        .filter_map(|(labels, mut values)| {
            values.sort_unstable_by_key(|(timestamp, _)| *timestamp);
            let numeric = values.iter().map(|(_, value)| *value).collect::<Vec<_>>();
            let value = match operation {
                RangeOperation::Count => numeric.len() as f64,
                RangeOperation::Rate => numeric.len() as f64 / seconds,
                RangeOperation::Bytes => numeric.iter().sum(),
                RangeOperation::BytesRate => numeric.iter().sum::<f64>() / seconds,
                RangeOperation::Sum => numeric.iter().sum(),
                RangeOperation::Average => mean(&numeric)?,
                RangeOperation::Minimum => numeric.iter().copied().reduce(f64::min)?,
                RangeOperation::Maximum => numeric.iter().copied().reduce(f64::max)?,
                RangeOperation::Stddev => variance(&numeric)?.sqrt(),
                RangeOperation::Stdvar => variance(&numeric)?,
                RangeOperation::Quantile => quantile(&numeric, parameter.unwrap_or(0.5))?,
                RangeOperation::First => values.first()?.1,
                RangeOperation::Last => values.last()?.1,
                RangeOperation::RateCounter => counter_increase(&numeric) / seconds,
                RangeOperation::Absent => unreachable!(),
            };
            Some(MetricSample { labels, value })
        })
        .collect();
    Ok(MetricValue::Vector(samples))
}

fn evaluate_aggregate(
    operation: AggregateOperation,
    grouping: Option<&MetricGrouping>,
    parameter: Option<usize>,
    value: MetricValue,
) -> Result<MetricValue, LokiApiError> {
    let MetricValue::Vector(samples) = value else {
        return Err(LokiApiError::bad_request(
            "vector aggregation cannot consume a scalar",
        ));
    };
    if matches!(
        operation,
        AggregateOperation::Sort | AggregateOperation::SortDescending
    ) {
        let mut samples = samples;
        samples.sort_by(|left, right| left.value.total_cmp(&right.value));
        if matches!(operation, AggregateOperation::SortDescending) {
            samples.reverse();
        }
        return Ok(MetricValue::Vector(samples));
    }
    if matches!(
        operation,
        AggregateOperation::TopK | AggregateOperation::BottomK
    ) {
        let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<MetricSample>>::new();
        for sample in samples {
            groups
                .entry(group_metric_labels(&sample.labels, grouping))
                .or_default()
                .push(sample);
        }
        let mut output = Vec::new();
        for mut samples in groups.into_values() {
            samples.sort_by(|left, right| right.value.total_cmp(&left.value));
            if matches!(operation, AggregateOperation::BottomK) {
                samples.reverse();
            }
            samples.truncate(parameter.unwrap_or(1));
            output.extend(samples);
        }
        return Ok(MetricValue::Vector(output));
    }
    let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<f64>>::new();
    for sample in samples {
        groups
            .entry(group_metric_labels(&sample.labels, grouping))
            .or_default()
            .push(sample.value);
    }
    Ok(MetricValue::Vector(
        groups
            .into_iter()
            .filter_map(|(labels, values)| {
                let value = match operation {
                    AggregateOperation::Sum => values.iter().sum(),
                    AggregateOperation::Average => mean(&values)?,
                    AggregateOperation::Minimum => values.iter().copied().reduce(f64::min)?,
                    AggregateOperation::Maximum => values.iter().copied().reduce(f64::max)?,
                    AggregateOperation::Count => values.len() as f64,
                    AggregateOperation::Stddev => variance(&values)?.sqrt(),
                    AggregateOperation::Stdvar => variance(&values)?,
                    AggregateOperation::TopK
                    | AggregateOperation::BottomK
                    | AggregateOperation::Sort
                    | AggregateOperation::SortDescending => unreachable!(),
                };
                Some(MetricSample { labels, value })
            })
            .collect(),
    ))
}

fn evaluate_binary(
    operation: BinaryOperation,
    bool_mode: bool,
    matching: &VectorMatching,
    left: MetricValue,
    right: MetricValue,
) -> Result<MetricValue, LokiApiError> {
    match (left, right) {
        (MetricValue::Scalar(left), MetricValue::Scalar(right)) => Ok(MetricValue::Scalar(
            apply_binary_number(operation, bool_mode, left, right).unwrap_or(f64::NAN),
        )),
        (MetricValue::Vector(samples), MetricValue::Scalar(scalar)) => Ok(MetricValue::Vector(
            samples
                .into_iter()
                .filter_map(|sample| {
                    apply_binary_number(operation, bool_mode, sample.value, scalar).map(|value| {
                        MetricSample {
                            labels: sample.labels,
                            value,
                        }
                    })
                })
                .collect(),
        )),
        (MetricValue::Scalar(scalar), MetricValue::Vector(samples)) => Ok(MetricValue::Vector(
            samples
                .into_iter()
                .filter_map(|sample| {
                    apply_binary_number(operation, bool_mode, scalar, sample.value).map(|value| {
                        MetricSample {
                            labels: sample.labels,
                            value,
                        }
                    })
                })
                .collect(),
        )),
        (MetricValue::Vector(left), MetricValue::Vector(right)) => {
            evaluate_vector_binary(operation, bool_mode, matching, left, right)
        }
    }
}

fn evaluate_vector_binary(
    operation: BinaryOperation,
    bool_mode: bool,
    matching: &VectorMatching,
    left: Vec<MetricSample>,
    right: Vec<MetricSample>,
) -> Result<MetricValue, LokiApiError> {
    if matches!(
        operation,
        BinaryOperation::And | BinaryOperation::Or | BinaryOperation::Unless
    ) {
        let left_keys = left
            .iter()
            .map(|sample| metric_match_key(&sample.labels, matching))
            .collect::<BTreeSet<_>>();
        let right_keys = right
            .iter()
            .map(|sample| metric_match_key(&sample.labels, matching))
            .collect::<BTreeSet<_>>();
        let mut output = match operation {
            BinaryOperation::And => left
                .into_iter()
                .filter(|sample| right_keys.contains(&metric_match_key(&sample.labels, matching)))
                .collect::<Vec<_>>(),
            BinaryOperation::Unless => left
                .into_iter()
                .filter(|sample| !right_keys.contains(&metric_match_key(&sample.labels, matching)))
                .collect::<Vec<_>>(),
            BinaryOperation::Or => left,
            _ => unreachable!(),
        };
        if operation == BinaryOperation::Or {
            output.extend(
                right.into_iter().filter(|sample| {
                    !left_keys.contains(&metric_match_key(&sample.labels, matching))
                }),
            );
        }
        return Ok(MetricValue::Vector(output));
    }
    if matching.cardinality == VectorCardinality::OneToMany {
        return evaluate_one_to_many_binary(operation, bool_mode, matching, left, right);
    }
    let mut right_by_key = BTreeMap::<BTreeMap<String, String>, MetricSample>::new();
    for sample in right {
        let key = metric_match_key(&sample.labels, matching);
        if right_by_key.insert(key, sample).is_some() {
            return Err(LokiApiError::bad_request(
                "many-to-many vector matching is not allowed",
            ));
        }
    }
    let mut output = Vec::new();
    let mut left_keys = BTreeSet::new();
    for sample in left {
        let key = metric_match_key(&sample.labels, matching);
        if matching.cardinality == VectorCardinality::OneToOne && !left_keys.insert(key.clone()) {
            return Err(LokiApiError::bad_request(
                "many-to-many vector matching is not allowed",
            ));
        }
        let other = right_by_key.get(&key);
        if let Some(other) = other
            && let Some(value) =
                apply_binary_number(operation, bool_mode, sample.value, other.value)
        {
            let mut labels = sample.labels;
            for name in &matching.include {
                if let Some(included) = other.labels.get(name) {
                    labels.insert(name.clone(), included.clone());
                }
            }
            output.push(MetricSample { labels, value });
        }
    }
    Ok(MetricValue::Vector(output))
}

fn evaluate_one_to_many_binary(
    operation: BinaryOperation,
    bool_mode: bool,
    matching: &VectorMatching,
    left: Vec<MetricSample>,
    right: Vec<MetricSample>,
) -> Result<MetricValue, LokiApiError> {
    if matches!(
        operation,
        BinaryOperation::And | BinaryOperation::Or | BinaryOperation::Unless
    ) {
        return Err(LokiApiError::bad_request(
            "group_left/group_right cannot be used with set operators",
        ));
    }
    let mut left_by_key = BTreeMap::<BTreeMap<String, String>, MetricSample>::new();
    for sample in left {
        let key = metric_match_key(&sample.labels, matching);
        if left_by_key.insert(key, sample).is_some() {
            return Err(LokiApiError::bad_request(
                "many-to-many vector matching is not allowed",
            ));
        }
    }
    let mut output = Vec::new();
    for sample in right {
        let key = metric_match_key(&sample.labels, matching);
        let Some(other) = left_by_key.get(&key) else {
            continue;
        };
        let Some(value) = apply_binary_number(operation, bool_mode, other.value, sample.value)
        else {
            continue;
        };
        let mut labels = sample.labels;
        for name in &matching.include {
            if let Some(included) = other.labels.get(name) {
                labels.insert(name.clone(), included.clone());
            }
        }
        output.push(MetricSample { labels, value });
    }
    Ok(MetricValue::Vector(output))
}

fn apply_binary_number(
    operation: BinaryOperation,
    bool_mode: bool,
    left: f64,
    right: f64,
) -> Option<f64> {
    let comparison = match operation {
        BinaryOperation::Equal => Some(left == right),
        BinaryOperation::NotEqual => Some(left != right),
        BinaryOperation::Greater => Some(left > right),
        BinaryOperation::GreaterEqual => Some(left >= right),
        BinaryOperation::Less => Some(left < right),
        BinaryOperation::LessEqual => Some(left <= right),
        _ => None,
    };
    if let Some(matches) = comparison {
        return if bool_mode {
            Some(f64::from(u8::from(matches)))
        } else {
            matches.then_some(left)
        };
    }
    match operation {
        BinaryOperation::Add => Some(left + right),
        BinaryOperation::Subtract => Some(left - right),
        BinaryOperation::Multiply => Some(left * right),
        BinaryOperation::Divide => Some(left / right),
        BinaryOperation::Modulo => Some(left % right),
        BinaryOperation::Power => Some(left.powf(right)),
        BinaryOperation::And | BinaryOperation::Or | BinaryOperation::Unless => None,
        _ => unreachable!(),
    }
}

fn group_metric_labels(
    labels: &BTreeMap<String, String>,
    grouping: Option<&MetricGrouping>,
) -> BTreeMap<String, String> {
    match grouping {
        Some(MetricGrouping::By(names)) => labels
            .iter()
            .filter(|(name, _)| names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        Some(MetricGrouping::Without(names)) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        None => BTreeMap::new(),
    }
}

fn metric_match_key(
    labels: &BTreeMap<String, String>,
    matching: &VectorMatching,
) -> BTreeMap<String, String> {
    if let Some(on) = &matching.on {
        labels
            .iter()
            .filter(|(name, _)| on.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    } else {
        labels
            .iter()
            .filter(|(name, _)| !matching.ignoring.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
}

fn parse_unwrapped_value(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .or_else(|| parse_duration_nanos(value).map(|value| value as f64 / 1_000_000_000.0))
        .or_else(|| parse_byte_quantity(value).map(|value| value as f64))
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn variance(values: &[f64]) -> Option<f64> {
    let mean = mean(values)?;
    Some(
        values
            .iter()
            .map(|value| {
                let deviation = value - mean;
                deviation * deviation
            })
            .sum::<f64>()
            / values.len() as f64,
    )
}

fn quantile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let rank = quantile * (values.len().saturating_sub(1)) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let fraction = rank - lower as f64;
    Some(values[lower] + (values[upper] - values[lower]) * fraction)
}

fn counter_increase(values: &[f64]) -> f64 {
    values.windows(2).fold(0.0, |increase, pair| {
        if pair[1] >= pair[0] {
            increase + pair[1] - pair[0]
        } else {
            increase + pair[1].max(0.0)
        }
    })
}

fn format_metric_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}

fn parse_grouping_clause(input: &str) -> Result<(Vec<String>, &str), LokiApiError> {
    let (labels, remaining) = extract_parenthesized(input.trim_start())?;
    let labels = labels
        .split(',')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    labels
        .iter()
        .try_for_each(|label| validate_label_name(label))?;
    Ok((labels, remaining))
}

fn function_arguments<'a>(input: &'a str, name: &str) -> Result<Option<&'a str>, LokiApiError> {
    let Some(rest) = input.strip_prefix(name) else {
        return Ok(None);
    };
    if rest
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Ok(None);
    }
    let (arguments, trailing) = extract_parenthesized(rest.trim_start())?;
    if !trailing.trim().is_empty() {
        return Err(LokiApiError::bad_request(format!(
            "{name} has trailing input"
        )));
    }
    Ok(Some(arguments))
}

fn extract_parenthesized(input: &str) -> Result<(&str, &str), LokiApiError> {
    if !input.starts_with('(') {
        return Err(LokiApiError::bad_request(
            "expected parenthesized expression",
        ));
    }
    let mut quoted = false;
    let mut escaped = false;
    let mut depth = 0usize;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if !quoted && character == '(' {
            depth += 1;
        } else if !quoted && character == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Ok((&input[1..index], &input[index + 1..]));
            }
        }
    }
    Err(LokiApiError::bad_request(
        "unterminated parenthesized expression",
    ))
}

fn strip_outer_parentheses(mut input: &str) -> &str {
    loop {
        let Ok((inner, trailing)) = extract_parenthesized(input) else {
            return input;
        };
        if !trailing.trim().is_empty() {
            return input;
        }
        input = inner.trim();
    }
}

fn split_top_level_once(input: &str, separator: char) -> Option<(&str, &str)> {
    let mut quoted = false;
    let mut escaped = false;
    let mut parentheses = 0usize;
    let mut braces = 0usize;
    let mut brackets = 0usize;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match character {
            '(' => parentheses += 1,
            ')' => parentheses = parentheses.saturating_sub(1),
            '{' => braces += 1,
            '}' => braces = braces.saturating_sub(1),
            '[' => brackets += 1,
            ']' => brackets = brackets.saturating_sub(1),
            _ if character == separator && parentheses == 0 && braces == 0 && brackets == 0 => {
                return Some((&input[..index], &input[index + character.len_utf8()..]));
            }
            _ => {}
        }
    }
    None
}

fn find_range_open(input: &str) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    let mut candidate = None;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == '[' && !quoted {
            candidate = Some(index);
        }
    }
    candidate
}

fn find_top_level_binary_operator(input: &str) -> Option<(usize, &'static str)> {
    const PRECEDENCE: [&[&str]; 6] = [
        &[" or ", " unless ", " and "],
        &["==", "!=", ">=", "<=", ">", "<"],
        &["+", "-"],
        &["*", "/", "%"],
        &["^"],
        &[],
    ];
    for operators in PRECEDENCE {
        let mut found = None;
        walk_top_level(input, |index| {
            for operator in operators {
                if input[index..].starts_with(operator)
                    && !(matches!(*operator, "+" | "-") && input[..index].trim().is_empty())
                {
                    found = Some((index, *operator));
                    break;
                }
            }
        });
        if found.is_some() {
            return found;
        }
    }
    None
}

fn walk_top_level(mut input: &str, mut visit: impl FnMut(usize)) {
    let original_length = input.len();
    let mut quoted = false;
    let mut escaped = false;
    let mut parentheses = 0usize;
    let mut braces = 0usize;
    let mut brackets = 0usize;
    while !input.is_empty() {
        let index = original_length - input.len();
        let character = input.chars().next().expect("nonempty input");
        if escaped {
            escaped = false;
        } else if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if !quoted {
            match character {
                '(' => parentheses += 1,
                ')' => parentheses = parentheses.saturating_sub(1),
                '{' => braces += 1,
                '}' => braces = braces.saturating_sub(1),
                '[' => brackets += 1,
                ']' => brackets = brackets.saturating_sub(1),
                _ => {}
            }
            if parentheses == 0 && braces == 0 && brackets == 0 {
                visit(index);
            }
        }
        input = &input[character.len_utf8()..];
    }
}

fn binary_operation(operator: &str) -> Result<BinaryOperation, LokiApiError> {
    match operator.trim() {
        "+" => Ok(BinaryOperation::Add),
        "-" => Ok(BinaryOperation::Subtract),
        "*" => Ok(BinaryOperation::Multiply),
        "/" => Ok(BinaryOperation::Divide),
        "%" => Ok(BinaryOperation::Modulo),
        "^" => Ok(BinaryOperation::Power),
        "==" => Ok(BinaryOperation::Equal),
        "!=" => Ok(BinaryOperation::NotEqual),
        ">" => Ok(BinaryOperation::Greater),
        ">=" => Ok(BinaryOperation::GreaterEqual),
        "<" => Ok(BinaryOperation::Less),
        "<=" => Ok(BinaryOperation::LessEqual),
        "and" => Ok(BinaryOperation::And),
        "or" => Ok(BinaryOperation::Or),
        "unless" => Ok(BinaryOperation::Unless),
        _ => Err(LokiApiError::bad_request("invalid binary operator")),
    }
}

fn parse_vector_matching(
    input: &str,
    operation: BinaryOperation,
) -> Result<(bool, VectorMatching, &str), LokiApiError> {
    let mut input = input.trim_start();
    let mut bool_mode = false;
    let mut matching = VectorMatching::default();
    loop {
        if let Some(rest) = input.strip_prefix("bool")
            && rest.chars().next().is_none_or(char::is_whitespace)
        {
            if !matches!(
                operation,
                BinaryOperation::Equal
                    | BinaryOperation::NotEqual
                    | BinaryOperation::Greater
                    | BinaryOperation::GreaterEqual
                    | BinaryOperation::Less
                    | BinaryOperation::LessEqual
            ) {
                return Err(LokiApiError::bad_request(
                    "bool is valid only for comparison operators",
                ));
            }
            bool_mode = true;
            input = rest.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("on") {
            let (labels, trailing) = parse_grouping_clause(rest)?;
            matching.on = Some(labels);
            input = trailing.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("ignoring") {
            let (labels, trailing) = parse_grouping_clause(rest)?;
            matching.ignoring = labels;
            input = trailing.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("group_left") {
            matching.cardinality = VectorCardinality::ManyToOne;
            let (include, rest) = parse_optional_grouping_clause(rest)?;
            matching.include = include;
            input = rest.trim_start();
            continue;
        }
        if let Some(rest) = input.strip_prefix("group_right") {
            matching.cardinality = VectorCardinality::OneToMany;
            let (include, rest) = parse_optional_grouping_clause(rest)?;
            matching.include = include;
            input = rest.trim_start();
            continue;
        }
        break;
    }
    Ok((bool_mode, matching, input))
}

fn parse_optional_grouping_clause(input: &str) -> Result<(Vec<String>, &str), LokiApiError> {
    let input = input.trim_start();
    if input.starts_with('(') {
        parse_grouping_clause(input)
    } else {
        Ok((Vec::new(), input))
    }
}

fn normalize_stream_labels(mut labels: BTreeMap<String, String>) -> BTreeMap<String, String> {
    if !labels.contains_key("service_name") {
        const SERVICE_CANDIDATES: [&str; 11] = [
            "service",
            "app",
            "application",
            "name",
            "app_kubernetes_io_name",
            "container",
            "container_name",
            "component",
            "workload",
            "job",
            "service.name",
        ];
        if let Some(service_name) = SERVICE_CANDIDATES
            .into_iter()
            .find_map(|name| labels.get(name).cloned())
        {
            labels.insert("service_name".to_owned(), service_name);
        }
    }
    labels
}

fn detected_level(labels: &BTreeMap<String, String>, line: &str) -> String {
    const LEVEL_LABELS: [&str; 5] = ["level", "severity", "severity_text", "lvl", "log_level"];
    if let Some(level) = LEVEL_LABELS.into_iter().find_map(|name| labels.get(name)) {
        return level.to_ascii_lowercase();
    }
    let lowercase = line.to_ascii_lowercase();
    ["trace", "debug", "info", "warn", "error", "fatal"]
        .into_iter()
        .find(|level| {
            lowercase
                .split(|character: char| !character.is_ascii_alphanumeric())
                .any(|term| term == *level)
        })
        .unwrap_or("unknown")
        .to_owned()
}

#[derive(Debug, Clone)]
struct LogSelector {
    matchers: Vec<LabelMatcher>,
    stages: Vec<PipelineStage>,
}

impl LogSelector {
    fn matches(&self, entry: &LokiEntry) -> bool {
        self.process(entry.clone()).is_some()
    }

    fn process(&self, mut entry: LokiEntry) -> Option<LokiEntry> {
        if !self
            .matchers
            .iter()
            .all(|matcher| matcher.matches(&entry.labels))
        {
            return None;
        }
        for stage in &self.stages {
            if !stage.apply(&mut entry) {
                return None;
            }
        }
        Some(entry)
    }
}

pub(crate) struct LogicalDeleteFilter {
    compiled: Vec<(i64, i64, LogSelector)>,
}

impl LogicalDeleteFilter {
    pub(crate) fn compile(requests: &[DeleteRequest]) -> Result<Self, LokiApiError> {
        let compiled = requests
            .iter()
            .filter(|request| request.status == "received")
            .map(|request| {
                Ok((
                    request.start_time,
                    request.end_time,
                    parse_log_query(&request.query)?,
                ))
            })
            .collect::<Result<Vec<_>, LokiApiError>>()?;
        Ok(Self { compiled })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.compiled.is_empty()
    }

    pub(crate) fn matches(&self, entry: &LokiEntry) -> bool {
        self.compiled.iter().any(|(start, end, selector)| {
            entry.timestamp_unix_nanos >= *start
                && entry.timestamp_unix_nanos <= *end
                && selector.matches(entry)
        })
    }
}

pub(crate) fn apply_logical_deletes(
    entries: &mut Vec<LokiEntry>,
    requests: &[DeleteRequest],
) -> Result<(), LokiApiError> {
    let filter = LogicalDeleteFilter::compile(requests)?;
    entries.retain(|entry| !filter.matches(entry));
    Ok(())
}

#[derive(Debug, Clone)]
struct LabelMatcher {
    name: String,
    operation: MatchOperation,
    value: String,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
enum MatchOperation {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

impl LabelMatcher {
    fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        let observed = labels.get(&self.name).map(String::as_str).unwrap_or("");
        match self.operation {
            MatchOperation::Equal => observed == self.value,
            MatchOperation::NotEqual => observed != self.value,
            MatchOperation::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(observed)),
            MatchOperation::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(observed)),
        }
    }
}

#[derive(Debug, Clone)]
struct LineFilter {
    operation: MatchOperation,
    value: String,
    regex: Option<Regex>,
}

impl LineFilter {
    fn matches(&self, line: &str) -> bool {
        match self.operation {
            MatchOperation::Equal => line.contains(&self.value),
            MatchOperation::NotEqual => !line.contains(&self.value),
            MatchOperation::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(line)),
            MatchOperation::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(line)),
        }
    }
}

#[derive(Debug, Clone)]
enum PipelineStage {
    Line(LineFilter),
    Json(Vec<(String, Vec<String>)>),
    Logfmt,
    Regexp(Regex),
    Pattern(Regex),
    LabelFilter(LabelFilter),
    LineFormat(String),
    LabelFormat(Vec<(String, LabelFormatValue)>),
    Drop(Vec<String>),
    Keep(Vec<String>),
    Decolorize,
    Unpack,
    Unwrap(String),
}

#[derive(Debug, Clone)]
struct LabelFilter {
    name: String,
    operation: LabelFilterOperation,
    value: String,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
enum LabelFilterOperation {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone)]
enum LabelFormatValue {
    Rename(String),
    Template(String),
}

impl PipelineStage {
    fn apply(&self, entry: &mut LokiEntry) -> bool {
        match self {
            Self::Line(filter) => filter.matches(&entry.line),
            Self::Json(expressions) => {
                match serde_json::from_str::<Value>(&entry.line) {
                    Ok(Value::Object(object)) => {
                        if expressions.is_empty() {
                            flatten_json_object("", &object, &mut entry.labels);
                        } else {
                            for (label, path) in expressions {
                                if let Some(value) = json_path(&Value::Object(object.clone()), path)
                                    && let Some(value) = scalar_label_value(value)
                                {
                                    insert_extracted_label(&mut entry.labels, label, value);
                                }
                            }
                        }
                    }
                    Ok(_) | Err(_) => set_parser_error(entry, "JSONParserErr"),
                }
                true
            }
            Self::Logfmt => {
                let parsed = parse_logfmt_labels(&entry.line);
                if parsed.is_empty() {
                    set_parser_error(entry, "LogfmtParserErr");
                } else {
                    for (name, value) in parsed {
                        insert_extracted_label(&mut entry.labels, &name, value);
                    }
                }
                true
            }
            Self::Regexp(regex) | Self::Pattern(regex) => {
                let Some(captures) = regex.captures(&entry.line) else {
                    set_parser_error(entry, "RegexpParserErr");
                    return true;
                };
                for name in regex.capture_names().flatten() {
                    if let Some(value) = captures.name(name) {
                        insert_extracted_label(&mut entry.labels, name, value.as_str().to_owned());
                    }
                }
                true
            }
            Self::LabelFilter(filter) => filter.matches(entry),
            Self::LineFormat(template) => {
                entry.line = render_logql_template(template, entry);
                true
            }
            Self::LabelFormat(assignments) => {
                for (target, value) in assignments {
                    let rendered = match value {
                        LabelFormatValue::Rename(source) => {
                            entry.labels.remove(source).unwrap_or_default()
                        }
                        LabelFormatValue::Template(template) => {
                            render_logql_template(template, entry)
                        }
                    };
                    entry.labels.insert(target.clone(), rendered);
                }
                true
            }
            Self::Drop(names) => {
                for name in names {
                    entry.labels.remove(name);
                }
                true
            }
            Self::Keep(names) => {
                entry
                    .labels
                    .retain(|name, _| names.iter().any(|candidate| candidate == name));
                true
            }
            Self::Decolorize => {
                entry.line = strip_ansi(&entry.line);
                true
            }
            Self::Unpack => {
                match serde_json::from_str::<Value>(&entry.line) {
                    Ok(Value::Object(mut object)) => {
                        let unpacked = object
                            .remove("_entry")
                            .and_then(|value| value.as_str().map(str::to_owned));
                        for (name, value) in object {
                            if let Some(value) = scalar_label_value(&value) {
                                insert_extracted_label(&mut entry.labels, &name, value);
                            }
                        }
                        if let Some(unpacked) = unpacked {
                            entry.line = unpacked;
                        } else {
                            set_parser_error(entry, "JSONParserErr");
                        }
                    }
                    Ok(_) | Err(_) => set_parser_error(entry, "JSONParserErr"),
                }
                true
            }
            Self::Unwrap(label) => {
                let _ = label;
                true
            }
        }
    }
}

impl LabelFilter {
    fn matches(&self, entry: &LokiEntry) -> bool {
        let observed = entry
            .labels
            .get(&self.name)
            .or_else(|| entry.structured_metadata.get(&self.name))
            .map(String::as_str)
            .unwrap_or("");
        match self.operation {
            LabelFilterOperation::Equal => observed == self.value,
            LabelFilterOperation::NotEqual => observed != self.value,
            LabelFilterOperation::Regex => self
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(observed)),
            LabelFilterOperation::NotRegex => self
                .regex
                .as_ref()
                .is_some_and(|regex| !regex.is_match(observed)),
            operation => {
                compare_typed_label(observed, &self.value).is_some_and(|ordering| match operation {
                    LabelFilterOperation::Greater => ordering.is_gt(),
                    LabelFilterOperation::GreaterEqual => ordering.is_ge(),
                    LabelFilterOperation::Less => ordering.is_lt(),
                    LabelFilterOperation::LessEqual => ordering.is_le(),
                    _ => false,
                })
            }
        }
    }
}

fn parse_log_query(expression: &str) -> Result<LogSelector, LokiApiError> {
    let end = matching_brace(expression)
        .ok_or_else(|| LokiApiError::bad_request("LogQL query requires a stream selector"))?;
    let matchers = parse_selector_matchers(&expression[..=end])?;
    let stages = parse_pipeline_stages(&expression[end + 1..])?;
    Ok(LogSelector { matchers, stages })
}

fn parse_selector_matchers(input: &str) -> Result<Vec<LabelMatcher>, LokiApiError> {
    let input = input.trim();
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(LokiApiError::bad_request("invalid stream selector"));
    }
    let mut matchers = Vec::new();
    let mut remaining = &input[1..input.len() - 1];
    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }
        let name_end = remaining
            .find(|character: char| {
                character == '=' || character == '!' || character.is_whitespace()
            })
            .ok_or_else(|| LokiApiError::bad_request("invalid label matcher"))?;
        let name = remaining[..name_end].trim();
        validate_label_name(name)?;
        remaining = remaining[name_end..].trim_start();
        let (operation, operator) = if remaining.starts_with("=~") {
            (MatchOperation::Regex, "=~")
        } else if remaining.starts_with("!~") {
            (MatchOperation::NotRegex, "!~")
        } else if remaining.starts_with("!=") {
            (MatchOperation::NotEqual, "!=")
        } else if remaining.starts_with('=') {
            (MatchOperation::Equal, "=")
        } else {
            return Err(LokiApiError::bad_request("invalid label matcher operation"));
        };
        remaining = remaining[operator.len()..].trim_start();
        let (value, rest) = parse_quoted(remaining)?;
        let regex = matches!(operation, MatchOperation::Regex | MatchOperation::NotRegex)
            .then(|| Regex::new(&format!("^(?:{value})$")))
            .transpose()
            .map_err(|error| LokiApiError::bad_request(format!("invalid label regex: {error}")))?;
        matchers.push(LabelMatcher {
            name: name.to_owned(),
            operation,
            value,
            regex,
        });
        remaining = rest.trim_start();
        if let Some(rest) = remaining.strip_prefix(',') {
            remaining = rest;
        } else if !remaining.is_empty() {
            return Err(LokiApiError::bad_request(
                "expected comma between label matchers",
            ));
        }
    }
    Ok(matchers)
}

fn parse_pipeline_stages(mut input: &str) -> Result<Vec<PipelineStage>, LokiApiError> {
    let mut stages = Vec::new();
    loop {
        input = input.trim_start();
        if input.is_empty() {
            return Ok(stages);
        }
        let (operation, rest) = if let Some(rest) = input.strip_prefix("|=") {
            (Some(MatchOperation::Equal), rest)
        } else if let Some(rest) = input.strip_prefix("!=") {
            (Some(MatchOperation::NotEqual), rest)
        } else if let Some(rest) = input.strip_prefix("|~") {
            (Some(MatchOperation::Regex), rest)
        } else if let Some(rest) = input.strip_prefix("!~") {
            (Some(MatchOperation::NotRegex), rest)
        } else {
            (None, input)
        };
        if let Some(operation) = operation {
            let (value, rest) = parse_quoted(rest.trim_start())?;
            let regex = matches!(operation, MatchOperation::Regex | MatchOperation::NotRegex)
                .then(|| Regex::new(&value))
                .transpose()
                .map_err(|error| LokiApiError::bad_request(format!("invalid regex: {error}")))?;
            stages.push(PipelineStage::Line(LineFilter {
                operation,
                value,
                regex,
            }));
            input = rest;
            continue;
        }
        let Some(rest) = input.strip_prefix('|') else {
            return Err(LokiApiError::bad_request(
                "unsupported or invalid LogQL pipeline stage",
            ));
        };
        let (segment, remaining) = take_pipeline_segment(rest.trim_start());
        let (name, arguments) = segment
            .trim()
            .split_once(char::is_whitespace)
            .map_or((segment.trim(), ""), |(name, arguments)| {
                (name, arguments.trim())
            });
        match name {
            "json" => stages.push(PipelineStage::Json(parse_json_expressions(arguments)?)),
            "logfmt" => stages.push(PipelineStage::Logfmt),
            "regexp" => {
                let (expression, trailing) = parse_quoted(arguments)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request("regexp stage has trailing input"));
                }
                stages.push(PipelineStage::Regexp(Regex::new(&expression).map_err(
                    |error| LokiApiError::bad_request(format!("invalid regexp stage: {error}")),
                )?));
            }
            "pattern" => {
                let (expression, trailing) = parse_quoted(arguments)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request(
                        "pattern stage has trailing input",
                    ));
                }
                stages.push(PipelineStage::Pattern(compile_pattern_parser(&expression)?));
            }
            "line_format" => {
                let (template, trailing) = parse_quoted(arguments)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request(
                        "line_format stage has trailing input",
                    ));
                }
                stages.push(PipelineStage::LineFormat(template));
            }
            "label_format" => stages.push(PipelineStage::LabelFormat(
                parse_label_format_assignments(arguments)?,
            )),
            "drop" => stages.push(PipelineStage::Drop(parse_label_list(arguments)?)),
            "keep" => stages.push(PipelineStage::Keep(parse_label_list(arguments)?)),
            "decolorize" if arguments.is_empty() => stages.push(PipelineStage::Decolorize),
            "unpack" if arguments.is_empty() => stages.push(PipelineStage::Unpack),
            "unwrap" => {
                let label = arguments
                    .split_ascii_whitespace()
                    .next()
                    .filter(|label| validate_label_name(label).is_ok())
                    .ok_or_else(|| LokiApiError::bad_request("unwrap requires a label name"))?;
                stages.push(PipelineStage::Unwrap(label.to_owned()));
            }
            _ => {
                stages.extend(parse_label_filter_expression(segment.trim())?);
            }
        }
        input = remaining;
    }
}

fn take_pipeline_segment(input: &str) -> (&str, &str) {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == '|' && !quoted {
            return (&input[..index], &input[index..]);
        }
    }
    (input, "")
}

fn parse_json_expressions(input: &str) -> Result<Vec<(String, Vec<String>)>, LokiApiError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    split_unquoted(input, ',')
        .into_iter()
        .map(|assignment| {
            let (label, path) = assignment
                .split_once('=')
                .ok_or_else(|| LokiApiError::bad_request("invalid json extraction expression"))?;
            let label = label.trim();
            validate_label_name(label)?;
            let (path, trailing) = parse_quoted(path.trim())?;
            if !trailing.trim().is_empty() || path.is_empty() {
                return Err(LokiApiError::bad_request("invalid json extraction path"));
            }
            Ok((
                label.to_owned(),
                path.trim_start_matches('.')
                    .split('.')
                    .map(str::to_owned)
                    .collect(),
            ))
        })
        .collect()
}

fn parse_label_format_assignments(
    input: &str,
) -> Result<Vec<(String, LabelFormatValue)>, LokiApiError> {
    let assignments = split_unquoted(input, ',');
    if assignments.is_empty() {
        return Err(LokiApiError::bad_request(
            "label_format requires at least one assignment",
        ));
    }
    assignments
        .into_iter()
        .map(|assignment| {
            let (target, value) = assignment
                .split_once('=')
                .ok_or_else(|| LokiApiError::bad_request("invalid label_format assignment"))?;
            let target = target.trim();
            validate_label_name(target)?;
            let value = value.trim();
            let value = if value.starts_with('"') {
                let (template, trailing) = parse_quoted(value)?;
                if !trailing.trim().is_empty() {
                    return Err(LokiApiError::bad_request(
                        "label_format template has trailing input",
                    ));
                }
                LabelFormatValue::Template(template)
            } else {
                validate_label_name(value)?;
                LabelFormatValue::Rename(value.to_owned())
            };
            Ok((target.to_owned(), value))
        })
        .collect()
}

fn parse_label_list(input: &str) -> Result<Vec<String>, LokiApiError> {
    let labels = input
        .split([',', ' '])
        .filter(|label| !label.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if labels.is_empty() {
        return Err(LokiApiError::bad_request(
            "label list stage requires at least one label",
        ));
    }
    labels
        .iter()
        .try_for_each(|label| validate_label_name(label))?;
    Ok(labels)
}

fn parse_label_filter_expression(input: &str) -> Result<Vec<PipelineStage>, LokiApiError> {
    split_keyword_unquoted(input, "and")
        .into_iter()
        .map(|condition| parse_label_filter(condition).map(PipelineStage::LabelFilter))
        .collect()
}

fn parse_label_filter(input: &str) -> Result<LabelFilter, LokiApiError> {
    let (index, operator) = ["=~", "!~", ">=", "<=", "!=", "=", ">", "<"]
        .into_iter()
        .filter_map(|operator| find_unquoted(input, operator).map(|index| (index, operator)))
        .min_by_key(|(index, _)| *index)
        .ok_or_else(|| LokiApiError::bad_request("invalid label filter"))?;
    let name = input[..index].trim();
    validate_label_name(name)?;
    let raw_value = input[index + operator.len()..].trim();
    let value = if raw_value.starts_with('"') {
        let (value, trailing) = parse_quoted(raw_value)?;
        if !trailing.trim().is_empty() {
            return Err(LokiApiError::bad_request("label filter has trailing input"));
        }
        value
    } else if !raw_value.is_empty() {
        raw_value.to_owned()
    } else {
        return Err(LokiApiError::bad_request("label filter value is missing"));
    };
    let operation = match operator {
        "=" => LabelFilterOperation::Equal,
        "!=" => LabelFilterOperation::NotEqual,
        "=~" => LabelFilterOperation::Regex,
        "!~" => LabelFilterOperation::NotRegex,
        ">" => LabelFilterOperation::Greater,
        ">=" => LabelFilterOperation::GreaterEqual,
        "<" => LabelFilterOperation::Less,
        "<=" => LabelFilterOperation::LessEqual,
        _ => unreachable!(),
    };
    let regex = matches!(
        operation,
        LabelFilterOperation::Regex | LabelFilterOperation::NotRegex
    )
    .then(|| Regex::new(&format!("^(?:{value})$")))
    .transpose()
    .map_err(|error| LokiApiError::bad_request(format!("invalid label filter regex: {error}")))?;
    Ok(LabelFilter {
        name: name.to_owned(),
        operation,
        value,
        regex,
    })
}

fn split_unquoted(input: &str, separator: char) -> Vec<&str> {
    let mut output = Vec::new();
    let mut start = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
        } else if character == '"' {
            quoted = !quoted;
        } else if character == separator && !quoted {
            let value = input[start..index].trim();
            if !value.is_empty() {
                output.push(value);
            }
            start = index + character.len_utf8();
        }
    }
    let value = input[start..].trim();
    if !value.is_empty() {
        output.push(value);
    }
    output
}

fn split_keyword_unquoted<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let needle = format!(" {keyword} ");
    let mut output = Vec::new();
    let mut start = 0usize;
    while let Some(relative) = find_unquoted(&input[start..], &needle) {
        let index = start + relative;
        let value = input[start..index].trim();
        if !value.is_empty() {
            output.push(value);
        }
        start = index + needle.len();
    }
    let value = input[start..].trim();
    if !value.is_empty() {
        output.push(value);
    }
    output
}

fn find_unquoted(input: &str, needle: &str) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            continue;
        }
        if !quoted && input[index..].starts_with(needle) {
            return Some(index);
        }
    }
    None
}

fn flatten_json_object(
    prefix: &str,
    object: &serde_json::Map<String, Value>,
    labels: &mut BTreeMap<String, String>,
) {
    for (name, value) in object {
        let normalized = normalize_extracted_label_name(name);
        let path = if prefix.is_empty() {
            normalized
        } else {
            format!("{prefix}_{normalized}")
        };
        match value {
            Value::Object(nested) => flatten_json_object(&path, nested, labels),
            value => {
                if let Some(value) = scalar_label_value(value) {
                    insert_extracted_label(labels, &path, value);
                }
            }
        }
    }
}

fn json_path<'a>(value: &'a Value, path: &[String]) -> Option<&'a Value> {
    path.iter().try_fold(value, |value, component| match value {
        Value::Object(object) => object.get(component),
        Value::Array(values) => component
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index)),
        _ => None,
    })
}

fn scalar_label_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => Some(String::new()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn normalize_extracted_label_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .enumerate()
        .map(|(index, character)| {
            if character == '_'
                || character.is_ascii_alphabetic()
                || (index > 0 && character.is_ascii_digit())
            {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_digit)
    {
        normalized.insert(0, '_');
    }
    normalized
}

fn insert_extracted_label(labels: &mut BTreeMap<String, String>, requested: &str, value: String) {
    let mut name = normalize_extracted_label_name(requested);
    while labels.contains_key(&name) {
        name.push_str("_extracted");
    }
    labels.insert(name, value);
}

fn set_parser_error(entry: &mut LokiEntry, error: &str) {
    entry
        .labels
        .entry("__error__".to_owned())
        .or_insert_with(|| error.to_owned());
}

fn parse_logfmt_labels(line: &str) -> Vec<(String, String)> {
    let bytes = line.as_bytes();
    let mut output = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        let key_start = cursor;
        while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() && bytes[cursor] != b'='
        {
            cursor += 1;
        }
        if cursor == key_start || bytes.get(cursor) != Some(&b'=') {
            while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            continue;
        }
        let key = &line[key_start..cursor];
        cursor += 1;
        let value = if bytes.get(cursor) == Some(&b'"') {
            let value_start = cursor;
            cursor += 1;
            let mut escaped = false;
            while cursor < bytes.len() {
                if escaped {
                    escaped = false;
                } else if bytes[cursor] == b'\\' {
                    escaped = true;
                } else if bytes[cursor] == b'"' {
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
            serde_json::from_str::<String>(&line[value_start..cursor]).unwrap_or_default()
        } else {
            let value_start = cursor;
            while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            line[value_start..cursor].to_owned()
        };
        if validate_label_name(key).is_ok() {
            output.push((key.to_owned(), value));
        }
    }
    output
}

fn compile_pattern_parser(pattern: &str) -> Result<Regex, LokiApiError> {
    let mut regex = String::from("^");
    let mut remaining = pattern;
    let mut capture_count = 0usize;
    while let Some(start) = remaining.find('<') {
        regex.push_str(&regex::escape(&remaining[..start]));
        let after = &remaining[start + 1..];
        let end = after
            .find('>')
            .ok_or_else(|| LokiApiError::bad_request("unterminated pattern capture"))?;
        let capture = &after[..end];
        if capture == "_" {
            regex.push_str("(?s:.*?)");
        } else {
            validate_label_name(capture)?;
            regex.push_str("(?P<");
            regex.push_str(capture);
            regex.push_str(">(?s:.*?))");
            capture_count += 1;
        }
        remaining = &after[end + 1..];
    }
    regex.push_str(&regex::escape(remaining));
    regex.push('$');
    if capture_count == 0 {
        return Err(LokiApiError::bad_request(
            "pattern parser requires at least one named capture",
        ));
    }
    Regex::new(&regex)
        .map_err(|error| LokiApiError::bad_request(format!("invalid pattern parser: {error}")))
}

fn render_logql_template(template: &str, entry: &LokiEntry) -> String {
    let mut output = String::new();
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        output.push_str(&remaining[..start]);
        let after = &remaining[start + 2..];
        let Some(end) = after.find("}}") else {
            output.push_str(&remaining[start..]);
            return output;
        };
        let expression = after[..end].trim();
        let value = if expression == "__line__" {
            entry.line.as_str()
        } else if expression == "__timestamp__" {
            output.push_str(&entry.timestamp_unix_nanos.to_string());
            ""
        } else if let Some(name) = expression.strip_prefix('.') {
            entry
                .labels
                .get(name)
                .or_else(|| entry.structured_metadata.get(name))
                .map(String::as_str)
                .unwrap_or("")
        } else {
            ""
        };
        output.push_str(value);
        remaining = &after[end + 2..];
    }
    output.push_str(remaining);
    output
}

fn strip_ansi(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut cursor = 0usize;
    let mut literal_start = 0usize;
    while cursor + 1 < bytes.len() {
        if bytes[cursor] == 0x1b && bytes[cursor + 1] == b'[' {
            output.push_str(&input[literal_start..cursor]);
            cursor += 2;
            while cursor < bytes.len() {
                let byte = bytes[cursor];
                cursor += 1;
                if (0x40..=0x7e).contains(&byte) {
                    break;
                }
            }
            literal_start = cursor;
        } else {
            cursor += 1;
        }
    }
    output.push_str(&input[literal_start..]);
    output
}

fn compare_typed_label(left: &str, right: &str) -> Option<std::cmp::Ordering> {
    if let (Some(left), Some(right)) = (parse_duration_nanos(left), parse_duration_nanos(right)) {
        return Some(left.cmp(&right));
    }
    if let (Some(left), Some(right)) = (parse_byte_quantity(left), parse_byte_quantity(right)) {
        return Some(left.cmp(&right));
    }
    let left = left.parse::<f64>().ok()?;
    let right = right.parse::<f64>().ok()?;
    left.partial_cmp(&right)
}

fn parse_label_set(input: &str) -> Result<BTreeMap<String, String>, LokiApiError> {
    let input = input.trim();
    if !input.starts_with('{') || !input.ends_with('}') {
        return Err(LokiApiError::bad_request("invalid label set"));
    }
    let mut labels = BTreeMap::new();
    let mut remaining = &input[1..input.len() - 1];
    loop {
        remaining = remaining.trim_start();
        if remaining.is_empty() {
            break;
        }
        let name_end = remaining
            .find(|character: char| character == '=' || character.is_whitespace())
            .ok_or_else(|| LokiApiError::bad_request("invalid label matcher"))?;
        let name = remaining[..name_end].trim();
        validate_label_name(name)?;
        remaining = remaining[name_end..].trim_start();
        let operation = ["=~", "!~", "!=", "="]
            .into_iter()
            .find(|operation| remaining.starts_with(operation))
            .ok_or_else(|| LokiApiError::bad_request("invalid label matcher operation"))?;
        remaining = remaining[operation.len()..].trim_start();
        let (value, rest) = parse_quoted(remaining)?;
        if operation != "=" {
            return Err(LokiApiError::bad_request(
                "non-equality matchers are not valid in pushed label sets",
            ));
        }
        if labels.insert(name.to_owned(), value).is_some() {
            return Err(LokiApiError::bad_request("duplicate label name"));
        }
        remaining = rest.trim_start();
        if let Some(rest) = remaining.strip_prefix(',') {
            remaining = rest;
        } else if !remaining.is_empty() {
            return Err(LokiApiError::bad_request("expected comma between labels"));
        }
    }
    validate_labels(&labels)?;
    Ok(labels)
}

fn parse_quoted(input: &str) -> Result<(String, &str), LokiApiError> {
    if !input.starts_with('"') {
        return Err(LokiApiError::bad_request("expected quoted string"));
    }
    let bytes = input.as_bytes();
    let mut escaped = false;
    for index in 1..bytes.len() {
        if escaped {
            escaped = false;
            continue;
        }
        match bytes[index] {
            b'\\' => escaped = true,
            b'"' => {
                let encoded = &input[..=index];
                let value: String = serde_json::from_str(encoded)
                    .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
                return Ok((value, &input[index + 1..]));
            }
            _ => {}
        }
    }
    Err(LokiApiError::bad_request("unterminated quoted string"))
}

fn matching_brace(input: &str) -> Option<usize> {
    input
        .char_indices()
        .find_map(|(index, character)| (character == '}').then_some(index))
}

fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), LokiApiError> {
    if labels.is_empty() {
        return Err(LokiApiError::bad_request(
            "at least one stream label is required",
        ));
    }
    labels.keys().try_for_each(|name| validate_label_name(name))
}

fn validate_label_name(name: &str) -> Result<(), LokiApiError> {
    let valid = !name.is_empty()
        && name.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        });
    if valid {
        Ok(())
    } else {
        Err(LokiApiError::bad_request(format!(
            "invalid label name {name:?}"
        )))
    }
}

fn query_range_bounds(params: &QueryParams) -> Result<(i64, i64), LokiApiError> {
    let now = now_nanos();
    let end = params
        .end
        .as_deref()
        .or(params.time.as_deref())
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or(now);
    let start = params
        .start
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or_else(|| {
            params
                .since
                .as_deref()
                .and_then(parse_duration_nanos)
                .and_then(|duration| end.checked_sub(duration))
                .unwrap_or(0)
        });
    if start > end {
        return Err(LokiApiError::bad_request("start is after end"));
    }
    Ok((start, end))
}

fn parse_duration_nanos(value: &str) -> Option<i64> {
    let (number, scale) = if let Some(number) = value.strip_suffix("ns") {
        (number, 1)
    } else if let Some(number) = value.strip_suffix("us") {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix("ms") {
        (number, 1_000_000)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_000_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60 * 1_000_000_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 60 * 60 * 1_000_000_000)
    } else {
        return None;
    };
    number
        .parse::<i64>()
        .ok()
        .and_then(|number| number.checked_mul(scale))
}

fn parse_timestamp(value: &str) -> Result<i64, LokiApiError> {
    if let Ok(integer) = value.parse::<i64>() {
        return Ok(integer);
    }
    if let Ok(float) = value.parse::<f64>()
        && float.is_finite()
    {
        return Ok((float * 1_000_000_000.0) as i64);
    }
    if let Some(timestamp) = parse_rfc3339_nanos(value) {
        return Ok(timestamp);
    }
    Err(LokiApiError::bad_request(format!(
        "invalid timestamp {value:?}"
    )))
}

fn parse_delete_timestamp(value: &str) -> Result<i64, LokiApiError> {
    if let Ok(seconds) = value.parse::<i64>() {
        return seconds
            .checked_mul(1_000_000_000)
            .ok_or_else(|| LokiApiError::bad_request("delete timestamp is out of range"));
    }
    parse_timestamp(value)
}

fn parse_rfc3339_nanos(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let year = i64::try_from(parse_decimal(&bytes[0..4])?).ok()?;
    let month = u32::try_from(parse_decimal(&bytes[5..7])?).ok()?;
    let day = u32::try_from(parse_decimal(&bytes[8..10])?).ok()?;
    let hour = parse_decimal(&bytes[11..13])?;
    let minute = parse_decimal(&bytes[14..16])?;
    let second = parse_decimal(&bytes[17..19])?;
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour >= 24
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    let timezone_start = bytes[19..]
        .iter()
        .position(|byte| matches!(byte, b'Z' | b'+' | b'-'))?
        + 19;
    let fraction = match &bytes[19..timezone_start] {
        [] => 0,
        [b'.', digits @ ..] if !digits.is_empty() && digits.len() <= 9 => {
            parse_decimal(digits)?.checked_mul(10_u64.pow(u32::try_from(9 - digits.len()).ok()?))?
        }
        _ => return None,
    };
    let offset_seconds = match &bytes[timezone_start..] {
        [b'Z'] => 0_i64,
        [
            sign @ (b'+' | b'-'),
            hour_tens,
            hour_ones,
            b':',
            minute_tens,
            minute_ones,
        ] => {
            let offset_hours = i64::try_from(parse_decimal(&[*hour_tens, *hour_ones])?).ok()?;
            let offset_minutes =
                i64::try_from(parse_decimal(&[*minute_tens, *minute_ones])?).ok()?;
            if offset_hours >= 24 || offset_minutes >= 60 {
                return None;
            }
            let magnitude = offset_hours
                .checked_mul(3_600)?
                .checked_add(offset_minutes.checked_mul(60)?)?;
            if *sign == b'-' { -magnitude } else { magnitude }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    let local_seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::try_from(hour.checked_mul(3_600)?).ok()?)?
        .checked_add(i64::try_from(minute.checked_mul(60)?).ok()?)?
        .checked_add(i64::try_from(second).ok()?)?;
    local_seconds
        .checked_sub(offset_seconds)?
        .checked_mul(1_000_000_000)?
        .checked_add(i64::try_from(fraction).ok()?)
}

fn parse_decimal(bytes: &[u8]) -> Option<u64> {
    bytes.iter().try_fold(0_u64, |value, byte| {
        byte.is_ascii_digit().then_some(())?;
        value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))
    })
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    }
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

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

async fn labels(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let mut names = BTreeSet::new();
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos >= start && entry.timestamp_unix_nanos <= end {
            names.extend(entry.labels.into_keys());
        }
    }
    Ok(Json(json!({"status": "success", "data": names})))
}

async fn label_values(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    validate_label_name(&name)?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let values = entries_for(&state, &tenant)
        .await?
        .into_iter()
        .filter(|entry| entry.timestamp_unix_nanos >= start && entry.timestamp_unix_nanos <= end)
        .filter_map(|entry| entry.labels.get(&name).cloned())
        .collect::<BTreeSet<_>>();
    Ok(Json(json!({"status": "success", "data": values})))
}

async fn series(
    State(state): State<ApiState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let mut pairs = form_urlencoded::parse(raw_query.as_deref().unwrap_or_default().as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if !body.is_empty() {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if content_type
            .split(';')
            .next()
            .is_none_or(|value| value.trim() != "application/x-www-form-urlencoded")
        {
            return Err(LokiApiError::bad_request(
                "POST series bodies must use application/x-www-form-urlencoded",
            ));
        }
        pairs.extend(
            form_urlencoded::parse(&body)
                .map(|(key, value)| (key.into_owned(), value.into_owned())),
        );
    }
    let params = QueryParams {
        start: pairs
            .iter()
            .rev()
            .find_map(|(key, value)| (key == "start").then(|| value.clone())),
        end: pairs
            .iter()
            .rev()
            .find_map(|(key, value)| (key == "end").then(|| value.clone())),
        ..QueryParams::default()
    };
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let mut selector_values = pairs
        .iter()
        .filter(|(key, _)| key == "match[]")
        .map(|(_, value)| value.clone())
        .collect::<Vec<_>>();
    if selector_values.is_empty() {
        selector_values.push("{}".to_owned());
    }
    let selectors = selector_values
        .into_iter()
        .map(|selector| parse_log_query(&selector))
        .collect::<Result<Vec<_>, _>>()?;
    let streams = entries_for(&state, &tenant)
        .await?
        .into_iter()
        .filter(|entry| {
            entry.timestamp_unix_nanos >= start
                && entry.timestamp_unix_nanos <= end
                && selectors.iter().any(|selector| selector.matches(entry))
        })
        .map(|entry| entry.labels)
        .collect::<BTreeSet<_>>();
    Ok(Json(json!({"status": "success", "data": streams})))
}

async fn index_stats(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let entries = entries_for(&state, &tenant)
        .await?
        .into_iter()
        .filter(|entry| {
            entry.timestamp_unix_nanos >= start
                && entry.timestamp_unix_nanos <= end
                && selector.matches(entry)
        })
        .collect::<Vec<_>>();
    let streams = entries
        .iter()
        .map(|entry| &entry.labels)
        .collect::<BTreeSet<_>>()
        .len();
    let bytes = entries.iter().map(|entry| entry.line.len()).sum::<usize>();
    Ok(Json(json!({
        "streams": streams,
        "chunks": streams,
        "entries": entries.len(),
        "bytes": bytes
    })))
}

async fn index_volume(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    volume_response(state, headers, params).await
}

async fn index_volume_range(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    volume_response(state, headers, params).await
}

async fn volume_response(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let mut volumes: BTreeMap<String, usize> = BTreeMap::new();
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos >= start
            && entry.timestamp_unix_nanos <= end
            && selector.matches(&entry)
        {
            *volumes.entry(format_label_set(&entry.labels)).or_default() += entry.line.len();
        }
    }
    let volumes = volumes
        .into_iter()
        .map(|(name, volume)| json!({"name": name, "volume": volume}))
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"status": "success", "data": {"resultType": "vector", "result": volumes}}),
    ))
}

async fn patterns(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let selector = parse_log_query(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let step = pattern_step_nanos(params.step.as_deref())?;
    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    let mut matched = 0usize;
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos < start
            || entry.timestamp_unix_nanos > end
            || !selector.matches(&entry)
        {
            continue;
        }
        let bucket = start
            .saturating_add(
                entry
                    .timestamp_unix_nanos
                    .saturating_sub(start)
                    .div_euclid(step)
                    .saturating_mul(step),
            )
            .div_euclid(1_000_000_000);
        *patterns
            .entry(crate::message_pattern(&entry.line))
            .or_default()
            .entry(bucket)
            .or_default() += 1;
        matched += 1;
        if matched == state.config.max_query_limit {
            break;
        }
    }
    let data = patterns
        .into_iter()
        .map(|(pattern, samples)| {
            json!({
                "pattern": pattern,
                "samples": samples.into_iter().map(|(timestamp, count)| {
                    json!([timestamp, count])
                }).collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"status": "success", "data": data})))
}

fn pattern_step_nanos(value: Option<&str>) -> Result<i64, LokiApiError> {
    let step = match value {
        None => 10_000_000_000,
        Some(value) => parse_duration_nanos(value)
            .or_else(|| {
                value
                    .parse::<f64>()
                    .ok()
                    .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                    .map(|seconds| (seconds * 1_000_000_000.0) as i64)
            })
            .ok_or_else(|| LokiApiError::bad_request("step must be a positive duration"))?,
    };
    if step <= 0 {
        return Err(LokiApiError::bad_request(
            "step must be a positive duration",
        ));
    }
    Ok(step)
}

async fn detected_fields(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let line_limit = params
        .line_limit
        .unwrap_or(100)
        .min(state.config.max_query_limit);
    let field_limit = params
        .field_limit
        .or(params.limit)
        .unwrap_or(1_000)
        .min(state.config.max_query_limit);
    let mut fields = BTreeMap::<String, DetectedField>::new();
    let mut scanned = 0usize;
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos < start
            || entry.timestamp_unix_nanos > end
            || !selector.matches(&entry)
        {
            continue;
        }
        for (name, value) in &entry.structured_metadata {
            fields
                .entry(name.clone())
                .or_default()
                .values
                .insert(value.clone());
        }
        for (name, value, parser) in detect_line_fields(&entry.line) {
            let field = fields.entry(name).or_default();
            field.values.insert(value);
            field.parsers.insert(parser);
        }
        scanned += 1;
        if scanned == line_limit {
            break;
        }
    }
    let fields = fields
        .into_iter()
        .take(field_limit)
        .map(|(label, field)| {
            json!({
                "label": label,
                "type": inferred_field_type(&field.values),
                "cardinality": field.values.len(),
                "parsers": field.parsers,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"fields": fields, "limit": field_limit})))
}

async fn detected_field_values(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let line_limit = params
        .line_limit
        .unwrap_or(100)
        .min(state.config.max_query_limit);
    let value_limit = params
        .field_limit
        .or(params.limit)
        .unwrap_or(1_000)
        .min(state.config.max_query_limit);
    let mut values = BTreeSet::new();
    let mut scanned = 0usize;
    for entry in entries_for(&state, &tenant).await? {
        if entry.timestamp_unix_nanos < start
            || entry.timestamp_unix_nanos > end
            || !selector.matches(&entry)
        {
            continue;
        }
        if let Some(value) = entry.structured_metadata.get(&name) {
            values.insert(value.clone());
        }
        for (field, value, _) in detect_line_fields(&entry.line) {
            if field == name {
                values.insert(value);
            }
        }
        scanned += 1;
        if scanned == line_limit || values.len() == value_limit {
            break;
        }
    }
    Ok(Json(json!({"values": values, "limit": value_limit})))
}

#[derive(Debug, Default)]
struct DetectedField {
    values: BTreeSet<String>,
    parsers: BTreeSet<&'static str>,
}

fn detect_line_fields(line: &str) -> Vec<(String, String, &'static str)> {
    if let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) {
        return object
            .into_iter()
            .filter_map(|(name, value)| match value {
                Value::String(value) => Some((name, value, "json")),
                Value::Number(value) => Some((name, value.to_string(), "json")),
                Value::Bool(value) => Some((name, value.to_string(), "json")),
                _ => None,
            })
            .collect();
    }
    line.split_ascii_whitespace()
        .filter_map(|term| {
            let (name, value) = term.split_once('=')?;
            validate_label_name(name).ok()?;
            let value = value.trim_matches(|character| matches!(character, '"' | '\'' | ','));
            (!value.is_empty()).then(|| (name.to_owned(), value.to_owned(), "logfmt"))
        })
        .collect()
}

fn inferred_field_type(values: &BTreeSet<String>) -> &'static str {
    if values.iter().all(|value| value.parse::<bool>().is_ok()) {
        "boolean"
    } else if values.iter().all(|value| value.parse::<i128>().is_ok()) {
        "int"
    } else if values.iter().all(|value| value.parse::<f64>().is_ok()) {
        "float"
    } else if values
        .iter()
        .all(|value| parse_duration_nanos(value).is_some())
    {
        "duration"
    } else if values
        .iter()
        .all(|value| parse_byte_quantity(value).is_some())
    {
        "bytes"
    } else {
        "string"
    }
}

fn parse_byte_quantity(value: &str) -> Option<u64> {
    for (suffix, scale) in [
        ("KiB", 1_u64 << 10),
        ("MiB", 1_u64 << 20),
        ("GiB", 1_u64 << 30),
        ("KB", 1_000),
        ("MB", 1_000_000),
        ("GB", 1_000_000_000),
        ("B", 1),
    ] {
        if let Some(number) = value.strip_suffix(suffix) {
            return number
                .parse::<u64>()
                .ok()
                .and_then(|number| number.checked_mul(scale));
        }
    }
    None
}

async fn tail(
    websocket: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, LokiApiError> {
    let tail_permit = state
        .production
        .as_ref()
        .map(|runtime| {
            runtime
                .try_tail()
                .ok_or_else(|| LokiApiError::too_many_requests("tail subscriber limit exceeded"))
        })
        .transpose()?;
    let tenant_name = tenant(&headers, &state.config);
    let selector = parse_log_query(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let mut live = state.live.subscribe();
    let store = Arc::clone(&state.store);
    let mut service_shutdown = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().subscribe_shutdown());
    let result = execute_stream_query(state, headers, params).await?.0;
    Ok(websocket
        .on_upgrade(move |mut socket| async move {
            let _tail_permit = tail_permit;
            let payload = result
                .get("data")
                .and_then(|data| data.get("result"))
                .cloned()
                .unwrap_or_else(|| json!([]));
            let _ = socket
                .send(axum::extract::ws::Message::Text(
                    json!({"streams": payload, "dropped_entries": []})
                        .to_string()
                        .into(),
                ))
                .await;
            loop {
                let shutdown = async {
                    match service_shutdown.as_mut() {
                        Some(shutdown) => {
                            let _ = shutdown.changed().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                };
                let received = tokio::select! {
                    biased;
                    () = shutdown => break,
                    received = live.recv() => received,
                };
                match received {
                    Ok(push) if push.tenant == tenant_name => {
                        let Ok(delete_filter) = store
                            .delete_requests(&tenant_name)
                            .and_then(|requests| LogicalDeleteFilter::compile(&requests))
                        else {
                            break;
                        };
                        let entries = push
                            .entries
                            .into_iter()
                            .filter_map(|entry| {
                                if delete_filter.matches(&entry) {
                                    None
                                } else {
                                    selector.process(entry)
                                }
                            })
                            .collect::<Vec<_>>();
                        if entries.is_empty() {
                            continue;
                        }
                        let payload = tail_streams(entries);
                        if socket
                            .send(axum::extract::ws::Message::Text(
                                json!({"streams": payload, "dropped_entries": []})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        if socket
                            .send(axum::extract::ws::Message::Text(
                                json!({
                                    "streams": [],
                                    "dropped_entries": [{
                                        "labels": {},
                                        "timestamp": now_nanos().to_string(),
                                        "dropped": dropped
                                    }]
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
        .into_response())
}

fn tail_streams(entries: Vec<LokiEntry>) -> Vec<Value> {
    let mut streams = BTreeMap::<BTreeMap<String, String>, Vec<Value>>::new();
    for entry in entries {
        let mut labels = entry.labels;
        labels.extend(entry.structured_metadata);
        let level = detected_level(&labels, &entry.line);
        labels.entry("detected_level".to_owned()).or_insert(level);
        streams
            .entry(labels)
            .or_default()
            .push(json!([entry.timestamp_unix_nanos.to_string(), entry.line]));
    }
    streams
        .into_iter()
        .map(|(stream, values)| json!({"stream": stream, "values": values}))
        .collect()
}

#[derive(Debug, Deserialize)]
struct DeleteParams {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    request_id: Option<String>,
}

async fn create_delete(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode, LokiApiError> {
    let query = params
        .query
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    parse_log_query(&query)?;
    let start = parse_delete_timestamp(
        params
            .start
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("start parameter is required"))?,
    )?;
    let end = params
        .end
        .as_deref()
        .map(parse_delete_timestamp)
        .transpose()?
        .unwrap_or_else(now_nanos);
    let tenant = tenant(&headers, &state.config);
    let store = Arc::clone(&state.store);
    tokio::task::spawn_blocking(move || {
        store.create_delete(&tenant, start, end, query, now_nanos())
    })
    .await
    .map_err(|error| LokiApiError::internal(format!("delete worker failed: {error}")))??;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_deletes(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let range = match (params.start.as_deref(), params.end.as_deref()) {
        (None, None) => None,
        (Some(start), Some(end)) => {
            Some((parse_delete_timestamp(start)?, parse_delete_timestamp(end)?))
        }
        _ => {
            return Err(LokiApiError::bad_request(
                "delete list start and end must be provided together",
            ));
        }
    };
    let deletes = state
        .store
        .delete_requests(&tenant)?
        .into_iter()
        .filter(|request| {
            range.is_none_or(|(start, end)| request.start_time <= end && request.end_time >= start)
        })
        .collect::<Vec<_>>();
    Ok(Json(json!(deletes)))
}

async fn cancel_delete(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
) -> Result<StatusCode, LokiApiError> {
    let request_id = params
        .request_id
        .ok_or_else(|| LokiApiError::bad_request("request_id parameter is required"))?;
    let tenant = tenant(&headers, &state.config);
    let store = Arc::clone(&state.store);
    tokio::task::spawn_blocking(move || store.cancel_delete(&tenant, &request_id))
        .await
        .map_err(|error| LokiApiError::internal(format!("delete worker failed: {error}")))??;
    Ok(StatusCode::NO_CONTENT)
}

async fn format_query(
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> Result<Json<Value>, LokiApiError> {
    let params = merge_form_query_params(&headers, params, &body)?;
    let query = params
        .query
        .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?;
    parse_log_query(&query)?;
    Ok(Json(json!({"status": "success", "data": query})))
}

fn tenant(headers: &HeaderMap, config: &LokiApiConfig) -> String {
    headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or(&config.default_tenant)
        .to_owned()
}

async fn entries_for(state: &ApiState, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
    let store = Arc::clone(&state.store);
    let tenant = tenant.to_owned();
    tokio::task::spawn_blocking(move || store.entries(&tenant))
        .await
        .map_err(|error| LokiApiError::internal(format!("query worker failed: {error}")))?
}

fn format_label_set(labels: &BTreeMap<String, String>) -> String {
    let contents = labels
        .iter()
        .map(|(name, value)| {
            format!(
                "{name}={}",
                serde_json::to_string(value).expect("string serialization cannot fail")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{contents}}}")
}

fn success(result_type: &str, result: Vec<Value>, stats: Value) -> Value {
    json!({
        "status": "success",
        "data": {
            "resultType": result_type,
            "result": result,
            "stats": stats,
        }
    })
}

fn query_stats(bytes: usize, lines: usize, returned: usize, elapsed: f64) -> Value {
    let denominator = elapsed.max(f64::EPSILON);
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": bytes,
            "decompressedLines": lines,
            "headChunkBytes": bytes,
        },
        "summary": {
            "bytesProcessedPerSecond": (bytes as f64 / denominator) as u64,
            "linesProcessedPerSecond": (lines as f64 / denominator) as u64,
            "totalBytesProcessed": bytes,
            "totalLinesProcessed": lines,
            "execTime": elapsed,
            "queueTime": 0,
            "subqueries": 0,
            "totalEntriesReturned": returned,
            "splits": 0,
            "shards": 0
        }
    })
}

async fn ready(State(state): State<ApiState>) -> Response {
    let lifecycle = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().state())
        .unwrap_or(ServiceState::Ready);
    let health = match state.store.health() {
        Ok(health) => health,
        Err(error) => {
            return (StatusCode::SERVICE_UNAVAILABLE, format!("{error}\n")).into_response();
        }
    };
    if lifecycle == ServiceState::Ready && health.ready {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{}: {}\n", lifecycle.as_str(), health.detail),
        )
            .into_response()
    }
}

async fn metrics(State(state): State<ApiState>) -> Response {
    let store = state.store.operational_metrics();
    let lifecycle = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().state())
        .unwrap_or(ServiceState::Ready);
    let ready = state
        .store
        .health()
        .is_ok_and(|health| health.ready && lifecycle == ServiceState::Ready);
    let mut output = format!(
        "# HELP shard_log_ready Whether ShardLog is ready.\n\
         # TYPE shard_log_ready gauge\n\
         shard_log_ready {}\n\
         # HELP shard_log_durable_sink_pending_items Durable sink items waiting for indexing.\n\
         # TYPE shard_log_durable_sink_pending_items gauge\n\
         shard_log_durable_sink_pending_items {}\n\
         shard_log_durable_sink_pending_bytes {}\n\
         shard_log_durable_sink_checkpoint_age_milliseconds {}\n\
         shard_log_durable_sink_applied_appends_total {}\n\
         shard_log_durable_sink_retries_total {}\n\
         shard_log_durable_sink_failures_total {}\n\
         shard_log_durable_sink_dirty_partitions {}\n\
         shard_log_retention_runs_total {}\n\
         shard_log_retention_advanced_offsets_total {}\n\
         shard_log_retention_failures_total {}\n",
        u8::from(ready),
        store.pending_items,
        store.pending_bytes,
        store.checkpoint_age_ms,
        store.applied_appends,
        store.retry_attempts,
        store.failed_attempts,
        store.dirty_partitions,
        store.retention_runs,
        store.retention_advanced_offsets,
        store.retention_failures,
    );
    if let Some(retained_payload_bytes) = store.retained_payload_bytes {
        output.push_str(&format!(
            "shard_log_retained_payload_bytes {retained_payload_bytes}\n"
        ));
    }
    if let Some(runtime) = &state.production {
        let protocol = runtime.metrics();
        let (http, ingest, query, tail, native) = runtime.admission_in_flight();
        output.push_str(&format!(
            "shard_log_http_requests_total {}\n\
             shard_log_authentication_failures_total {}\n\
             shard_log_rejected_requests_total {}\n\
             shard_log_ingest_requests_total {}\n\
             shard_log_ingest_bytes_total {}\n\
             shard_log_ingest_records_total {}\n\
             shard_log_query_requests_total {}\n\
             shard_log_native_connections_total {}\n\
             shard_log_tail_subscriptions_total {}\n\
             shard_log_http_in_flight {}\n\
             shard_log_ingest_in_flight {}\n\
             shard_log_query_in_flight {}\n\
             shard_log_tail_in_flight {}\n\
             shard_log_native_connections_in_flight {}\n",
            protocol.http_requests,
            protocol.authentication_failures,
            protocol.rejected_requests,
            protocol.ingest_requests,
            protocol.ingest_bytes,
            protocol.ingest_records,
            protocol.query_requests,
            protocol.native_connections,
            protocol.tail_subscriptions,
            http,
            ingest,
            query,
            tail,
            native,
        ));
    }
    (StatusCode::OK, output).into_response()
}

async fn current_config(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({
        "target": "all",
        "auth_enabled": state.production.is_some(),
        "single_tenant": state.production.is_some()
    }))
}

async fn services(State(state): State<ApiState>) -> Json<Value> {
    let status = state
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().state().as_str())
        .unwrap_or("ready");
    Json(json!({"services": [{"service": "shard-log", "status": status}]}))
}

async fn log_level() -> Json<Value> {
    Json(json!({"status": "success", "message": "current log level is info"}))
}

async fn flush(State(state): State<ApiState>) -> Result<StatusCode, LokiApiError> {
    flush_store(&state).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn prepare_shutdown(State(state): State<ApiState>) -> Result<StatusCode, LokiApiError> {
    if let Some(runtime) = &state.production {
        runtime.lifecycle().begin_draining();
    }
    if let Err(error) = flush_store(&state).await {
        if let Some(runtime) = &state.production {
            runtime.lifecycle().mark_failed(error.to_string());
        }
        return Err(error);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn shutdown(State(state): State<ApiState>) -> Result<StatusCode, LokiApiError> {
    prepare_shutdown(State(state.clone())).await?;
    if let Some(runtime) = &state.production {
        runtime.lifecycle().request_shutdown();
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn flush_store(state: &ApiState) -> Result<(), LokiApiError> {
    let store = Arc::clone(&state.store);
    let timeout = state.flush_timeout;
    tokio::task::spawn_blocking(move || store.flush(timeout))
        .await
        .map_err(|error| LokiApiError::internal(format!("flush worker failed: {error}")))?
}

async fn build_info() -> Json<Value> {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": option_env!("SHARD_LOG_GIT_REVISION").unwrap_or("unknown"),
        "branch": "unknown",
        "buildUser": "cargo",
        "buildDate": option_env!("SHARD_LOG_BUILD_DATE").unwrap_or("unknown"),
        "goVersion": "",
    }))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_array::{StringArray, TimestampNanosecondArray};
    use arrow_ipc::reader::StreamReader;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    use super::*;
    use crate::{ServiceLifecycle, SingleTenantConfig};

    fn production_runtime() -> (Arc<ProductionRuntime>, Arc<ServiceLifecycle>) {
        let lifecycle = Arc::new(ServiceLifecycle::new());
        let runtime = Arc::new(
            ProductionRuntime::new(
                SingleTenantConfig {
                    tenant: Arc::from("tenant-a"),
                    bearer_token: Arc::from("0123456789abcdef"),
                    max_http_in_flight: 8,
                    max_ingest_in_flight: 4,
                    max_query_in_flight: 4,
                    ingest_bytes_per_second: 0,
                    ingest_burst_bytes: 0,
                    max_tail_subscribers: 2,
                    max_native_connections: 4,
                    query_timeout: Duration::from_secs(30),
                    native_auth_timeout: Duration::from_secs(5),
                },
                Arc::clone(&lifecycle),
            )
            .expect("valid production runtime"),
        );
        (runtime, lifecycle)
    }

    #[test]
    fn json_push_requires_string_timestamps_and_preserves_metadata() {
        let entries = decode_json_push(
            br#"{"streams":[{"stream":{"app":"api"},"values":[["100","hello",{"trace_id":"abc"}]]}]}"#,
        )
        .expect("valid push");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].timestamp_unix_nanos, 100);
        assert_eq!(entries[0].structured_metadata["trace_id"], "abc");
        assert!(
            decode_json_push(br#"{"streams":[{"stream":{"app":"api"},"values":[[100,"hello"]]}]}"#)
                .is_err()
        );
    }

    #[test]
    fn exact_and_regex_line_filters_are_lossless() {
        let selector =
            parse_log_query(r#"{app="api"} |= "request" !~ "health|metrics""#).expect("query");
        let entry = LokiEntry {
            timestamp_unix_nanos: 1,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "request completed".to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        assert!(selector.matches(&entry));
    }

    #[test]
    fn parser_filter_and_format_pipeline_stages_match_logql_semantics() {
        let selector = parse_log_query(
            r#"{app="api"} | json | duration >= 40ms and status =~ "5.." | label_format code=status | drop status | line_format "{{.method}} {{.code}} {{ __line__ }}""#,
        )
        .expect("query");
        let entry = LokiEntry {
            timestamp_unix_nanos: 1,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: r#"{"method":"GET","status":"500","duration":"42ms"}"#.to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        let processed = selector.process(entry).expect("matching entry");
        assert_eq!(processed.labels["method"], "GET");
        assert_eq!(processed.labels["code"], "500");
        assert!(!processed.labels.contains_key("status"));
        assert_eq!(
            processed.line,
            r#"GET 500 {"method":"GET","status":"500","duration":"42ms"}"#
        );

        let rejected = LokiEntry {
            timestamp_unix_nanos: 2,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: r#"{"method":"GET","status":"200","duration":"2ms"}"#.to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        assert!(selector.process(rejected).is_none());
    }

    #[test]
    fn regexp_pattern_logfmt_unpack_and_decolorize_are_lossless() {
        let regexp = parse_log_query(r#"{} | regexp "user=(?P<user>[^ ]+)""#).expect("regexp");
        let logfmt = parse_log_query(r#"{} | logfmt | status = 500"#).expect("logfmt");
        let pattern =
            parse_log_query(r#"{} | pattern "request <method> <path>""#).expect("pattern");
        let unpack = parse_log_query(r#"{} | unpack | decolorize"#).expect("unpack");
        let base = |line: &str| LokiEntry {
            timestamp_unix_nanos: 1,
            labels: BTreeMap::new(),
            line: line.to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        assert_eq!(
            regexp.process(base("user=alice ok")).unwrap().labels["user"],
            "alice"
        );
        assert!(logfmt.process(base("status=500 duration=42ms")).is_some());
        let patterned = pattern.process(base("request GET /health")).unwrap();
        assert_eq!(patterned.labels["method"], "GET");
        assert_eq!(patterned.labels["path"], "/health");
        let unpacked = unpack
            .process(base(r#"{"_entry":"\u001b[31mfailed\u001b[0m","pod":"a"}"#))
            .unwrap();
        assert_eq!(unpacked.line, "failed");
        assert_eq!(unpacked.labels["pod"], "a");
    }

    #[test]
    fn parser_errors_are_labels_and_can_be_filtered_explicitly() {
        let selector = parse_log_query(r#"{} | json | __error__ != """#).expect("query");
        let entry = LokiEntry {
            timestamp_unix_nanos: 1,
            labels: BTreeMap::new(),
            line: "not json".to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        let processed = selector.process(entry).expect("parser error is selected");
        assert_eq!(processed.labels["__error__"], "JSONParserErr");
    }

    #[test]
    fn metric_logql_range_aggregation_unwrap_and_binary_matching_are_exact() {
        let entries = vec![
            LokiEntry {
                timestamp_unix_nanos: 1_000_000_000,
                labels: BTreeMap::from([
                    ("app".to_owned(), "api".to_owned()),
                    ("pod".to_owned(), "a".to_owned()),
                ]),
                line: "duration=100ms".to_owned(),
                structured_metadata: BTreeMap::new(),
            },
            LokiEntry {
                timestamp_unix_nanos: 2_000_000_000,
                labels: BTreeMap::from([
                    ("app".to_owned(), "api".to_owned()),
                    ("pod".to_owned(), "b".to_owned()),
                ]),
                line: "duration=300ms".to_owned(),
                structured_metadata: BTreeMap::new(),
            },
        ];
        let count = parse_metric_expression(r#"sum by (app) (count_over_time({app="api"}[2s]))"#)
            .expect("count query");
        let MetricValue::Vector(count) =
            evaluate_metric_expression(&count, &entries, 2_000_000_000).expect("evaluation")
        else {
            panic!("expected vector");
        };
        assert_eq!(count.len(), 1);
        assert_eq!(count[0].labels["app"], "api");
        assert_eq!(count[0].value, 2.0);

        let average = parse_metric_expression(
            r#"avg_over_time({app="api"} | logfmt | unwrap duration [2s])"#,
        )
        .expect("unwrap query");
        let MetricValue::Vector(average) =
            evaluate_metric_expression(&average, &entries, 2_000_000_000).expect("evaluation")
        else {
            panic!("expected vector");
        };
        assert_eq!(average.len(), 2);
        assert!(average.iter().any(|sample| sample.value == 0.1));
        assert!(average.iter().any(|sample| sample.value == 0.3));

        let comparison =
            parse_metric_expression(r#"sum(count_over_time({app="api"}[2s])) > bool 1"#)
                .expect("comparison");
        let MetricValue::Vector(comparison) =
            evaluate_metric_expression(&comparison, &entries, 2_000_000_000).expect("evaluation")
        else {
            panic!("expected vector");
        };
        assert_eq!(comparison[0].value, 1.0);
    }

    #[test]
    fn timestamps_and_detected_line_fields_follow_loki_types() {
        assert_eq!(
            parse_timestamp("1970-01-01T00:00:01.000000002Z").unwrap(),
            1_000_000_002
        );
        assert_eq!(
            parse_timestamp("1970-01-01T01:00:01+01:00").unwrap(),
            1_000_000_000
        );
        assert_eq!(parse_delete_timestamp("2").unwrap(), 2_000_000_000);

        let fields = detect_line_fields("status=500 duration=42ms enabled=true");
        assert_eq!(fields.len(), 3);
        let values = BTreeSet::from(["42ms".to_owned(), "84ms".to_owned()]);
        assert_eq!(inferred_field_type(&values), "duration");
        let json = detect_line_fields(r#"{"status":500,"cached":true}"#);
        assert!(json.contains(&("status".to_owned(), "500".to_owned(), "json")));
    }

    #[test]
    fn native_snappy_protobuf_push_round_trips_loki_logproto_fields() {
        let protobuf = ProtoPushRequest {
            streams: vec![ProtoStream {
                labels: r#"{app="api"}"#.to_owned(),
                entries: vec![ProtoEntry {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 23,
                    }),
                    line: "protobuf message".to_owned(),
                    structured_metadata: vec![ProtoLabelPair {
                        name: "trace_id".to_owned(),
                        value: "abc".to_owned(),
                    }],
                    parsed: Vec::new(),
                }],
                hash: 0,
            }],
        }
        .encode_to_vec();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&protobuf)
            .expect("Snappy encoding");
        let entries = decode_protobuf_push(&compressed).expect("push decoding");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].timestamp_unix_nanos, 1_000_000_023);
        assert_eq!(entries[0].line, "protobuf message");
        assert_eq!(entries[0].labels["service_name"], "api");
        assert_eq!(entries[0].structured_metadata["trace_id"], "abc");
    }

    #[tokio::test]
    async fn stable_loki_route_surface_has_no_missing_or_wrong_method_routes() {
        let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let cases = [
            (Method::GET, "/ready"),
            (Method::GET, "/metrics"),
            (Method::GET, "/config"),
            (Method::GET, "/services"),
            (Method::GET, "/log_level"),
            (Method::POST, "/log_level"),
            (Method::POST, "/flush"),
            (Method::POST, "/ingester/prepare_shutdown"),
            (Method::POST, "/ingester/shutdown"),
            (Method::GET, "/loki/api/v1/status/buildinfo"),
            (Method::POST, "/loki/api/v1/push"),
            (Method::POST, "/otlp/v1/logs"),
            (
                Method::GET,
                "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D",
            ),
            (
                Method::POST,
                "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D",
            ),
            (
                Method::GET,
                "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D",
            ),
            (
                Method::POST,
                "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D",
            ),
            (Method::GET, "/loki/api/v1/labels"),
            (Method::POST, "/loki/api/v1/labels"),
            (Method::GET, "/loki/api/v1/label/app/values"),
            (Method::POST, "/loki/api/v1/label/app/values"),
            (Method::GET, "/loki/api/v1/series"),
            (Method::POST, "/loki/api/v1/series"),
            (Method::GET, "/loki/api/v1/index/stats"),
            (Method::POST, "/loki/api/v1/index/stats"),
            (Method::GET, "/loki/api/v1/index/volume"),
            (Method::POST, "/loki/api/v1/index/volume"),
            (Method::GET, "/loki/api/v1/index/volume_range"),
            (Method::POST, "/loki/api/v1/index/volume_range"),
            (Method::GET, "/loki/api/v1/patterns"),
            (Method::POST, "/loki/api/v1/patterns"),
            (Method::GET, "/loki/api/v1/detected_fields"),
            (Method::POST, "/loki/api/v1/detected_fields"),
            (Method::GET, "/loki/api/v1/detected_field/level/values"),
            (Method::POST, "/loki/api/v1/detected_field/level/values"),
            (Method::GET, "/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D"),
            (Method::GET, "/loki/api/v1/delete"),
            (
                Method::POST,
                "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1",
            ),
            (
                Method::PUT,
                "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1",
            ),
            (Method::DELETE, "/loki/api/v1/delete?request_id=missing"),
            (
                Method::GET,
                "/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D",
            ),
            (
                Method::POST,
                "/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D",
            ),
            (Method::POST, "/api/prom/push"),
            (Method::GET, "/api/prom/query?query=%7Bapp%3D%22api%22%7D"),
            (Method::GET, "/api/prom/label"),
            (Method::GET, "/api/prom/label/app/values"),
            (Method::GET, "/api/prom/series"),
            (Method::GET, "/api/prom/tail?query=%7Bapp%3D%22api%22%7D"),
        ];
        for (method, uri) in cases {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            r#"{"streams":[{"stream":{"app":"api"},"values":[]}]}"#,
                        ))
                        .expect("request"),
                )
                .await
                .expect("route response");
            assert_ne!(
                response.status(),
                StatusCode::NOT_FOUND,
                "missing route for {method} {uri}"
            );
            assert_ne!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "wrong method for {method} {uri}"
            );
        }
    }

    #[tokio::test]
    async fn push_query_labels_series_stats_and_detected_fields_round_trip() {
        let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let push = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                r#"{"streams":[{"stream":{"app":"api","env":"prod"},"values":[["100","request completed",{"trace_id":"abc"}],["200","health check"]]}]}"#,
            ))
            .expect("push request");
        assert_eq!(
            app.clone().oneshot(push).await.expect("push").status(),
            StatusCode::NO_CONTENT
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22request%22&start=1&end=300&direction=forward")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await
            .expect("query");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("query body");
        let body: Value = serde_json::from_slice(&body).expect("query JSON");
        assert_eq!(body["data"]["resultType"], "streams");
        assert_eq!(body["data"]["result"][0]["values"][0][0], "100");
        assert_eq!(body["data"]["result"][0]["stream"]["trace_id"], "abc");
        assert_eq!(body["data"]["result"][0]["stream"]["service_name"], "api");

        for (uri, pointer, expected) in [
            ("/loki/api/v1/labels?start=1&end=300", "/data/0", "app"),
            (
                "/loki/api/v1/label/env/values?start=1&end=300",
                "/data/0",
                "prod",
            ),
            (
                "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D&start=1&end=300",
                "/data/0/app",
                "api",
            ),
            (
                "/loki/api/v1/detected_field/trace_id/values?start=1&end=300",
                "/values/0",
                "abc",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .header("x-scope-orgid", "tenant-a")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            let body: Value = serde_json::from_slice(&body).expect("JSON");
            assert_eq!(
                body.pointer(pointer),
                Some(&Value::String(expected.to_owned()))
            );
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=1&end=300")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("stats request"),
            )
            .await
            .expect("stats");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("stats body");
        let body: Value = serde_json::from_slice(&body).expect("stats JSON");
        assert_eq!(body["entries"], 2);
        assert_eq!(body["streams"], 1);
    }

    #[tokio::test]
    async fn post_form_parameters_match_url_query_parameters() {
        let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let push = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                r#"{"streams":[{"stream":{"app":"api"},"values":[["100","request complete"]]}]}"#,
            ))
            .expect("push request");
        assert_eq!(
            app.clone().oneshot(push).await.expect("push").status(),
            StatusCode::NO_CONTENT
        );

        let query = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/query_range?limit=5")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                "query=%7Bapp%3D%22api%22%7D&start=1&end=200&direction=forward",
            ))
            .expect("form query");
        let response = app.clone().oneshot(query).await.expect("query response");
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(
            body["data"]["result"][0]["values"][0][1],
            "request complete"
        );

        let series = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/series")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                "match%5B%5D=%7Bapp%3D%22api%22%7D&start=1&end=200",
            ))
            .expect("form series");
        let response = app.oneshot(series).await.expect("series response");
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(body["data"][0]["app"], "api");
    }

    #[tokio::test]
    async fn metric_query_range_returns_loki_matrix_samples() {
        let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let push = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                r#"{"streams":[{"stream":{"app":"api"},"values":[["1000000000","one"],["2000000000","two"]]}]}"#,
            ))
            .expect("push request");
        assert_eq!(
            app.clone().oneshot(push).await.expect("push").status(),
            StatusCode::NO_CONTENT
        );
        let expression = form_urlencoded::byte_serialize(
            r#"sum by (app) (count_over_time({app="api"}[2s]))"#.as_bytes(),
        )
        .collect::<String>();
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/query_range?query={expression}&start=2000000000&end=3000000000&step=1s"
                    ))
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await
            .expect("query response");
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("JSON");
        assert_eq!(body["data"]["resultType"], "matrix");
        assert_eq!(body["data"]["result"][0]["metric"]["app"], "api");
        assert_eq!(body["data"]["result"][0]["values"][0][1], "2");
        assert_eq!(body["data"]["result"][0]["values"][1][1], "1");
    }

    #[tokio::test]
    async fn delete_requests_hide_matching_logs_and_cancel_restores_visibility() {
        let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let push = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                r#"{"streams":[{"stream":{"app":"api"},"values":[["100","remove this"],["200","retain this"]]}]}"#,
            ))
            .expect("push request");
        assert_eq!(
            app.clone().oneshot(push).await.expect("push").status(),
            StatusCode::NO_CONTENT
        );

        let create = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22remove%22&start=0&end=1")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .expect("delete request");
        assert_eq!(
            app.clone().oneshot(create).await.expect("delete").status(),
            StatusCode::NO_CONTENT
        );

        let listed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/loki/api/v1/delete")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("list request"),
            )
            .await
            .expect("list response");
        let body = to_bytes(listed.into_body(), usize::MAX)
            .await
            .expect("list body");
        let deletes: Vec<DeleteRequest> = serde_json::from_slice(&body).expect("delete JSON");
        assert_eq!(deletes.len(), 1);

        let query_uri = "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1&end=300&direction=forward";
        let query = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(query_uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await
            .expect("query response");
        let body = to_bytes(query.into_body(), usize::MAX)
            .await
            .expect("query body");
        let body: Value = serde_json::from_slice(&body).expect("query JSON");
        assert_eq!(
            body["data"]["result"][0]["values"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(body["data"]["result"][0]["values"][0][1], "retain this");

        let cancel = Request::builder()
            .method(Method::DELETE)
            .uri(format!(
                "/loki/api/v1/delete?request_id={}",
                deletes[0].request_id
            ))
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .expect("cancel request");
        assert_eq!(
            app.clone().oneshot(cancel).await.expect("cancel").status(),
            StatusCode::NO_CONTENT
        );

        let query = app
            .oneshot(
                Request::builder()
                    .uri(query_uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await
            .expect("query response");
        let body = to_bytes(query.into_body(), usize::MAX)
            .await
            .expect("query body");
        let body: Value = serde_json::from_slice(&body).expect("query JSON");
        assert_eq!(
            body["data"]["result"][0]["values"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn patterns_group_structurally_similar_messages_into_time_samples() {
        let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let push = Request::builder()
            .method(Method::POST)
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::from(
                r#"{"streams":[{"stream":{"app":"api"},"values":[["1000000000","request id=123456 duration=42ms complete"],["2000000000","request id=654321 duration=84ms complete"]]}]}"#,
            ))
            .expect("push request");
        assert_eq!(
            app.clone().oneshot(push).await.expect("push").status(),
            StatusCode::NO_CONTENT
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=3000000000&step=1s")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("patterns request"),
            )
            .await
            .expect("patterns response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("patterns body");
        let body: Value = serde_json::from_slice(&body).expect("patterns JSON");
        assert_eq!(
            body["data"][0]["pattern"],
            "request id=<_> duration=<_> complete"
        );
        assert_eq!(body["data"][0]["samples"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn clickhouse_scan_is_absent_by_default_and_requires_its_bearer_token() {
        let disabled = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
        let response = disabled
            .oneshot(
                Request::builder()
                    .uri("/shardlog/api/v1/clickhouse/scan")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let store = Arc::new(LokiApiStore::default());
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: 123,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "request failed".to_owned(),
                    structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
                }],
            )
            .expect("push");
        let app = loki_router_with_clickhouse(
            store,
            LokiApiConfig::default(),
            Arc::from("analytics-secret"),
        )
        .expect("router");
        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/shardlog/api/v1/clickhouse/scan")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app
            .oneshot(
                Request::builder()
                    .uri("/shardlog/api/v1/clickhouse/scan?term=failed&label.app=api&metadata.code=500")
                    .header("authorization", "Bearer analytics-secret")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(authorized.status(), StatusCode::OK);
        assert_eq!(
            authorized.headers()[header::CONTENT_TYPE],
            "application/vnd.apache.arrow.stream"
        );
        let body = to_bytes(authorized.into_body(), usize::MAX)
            .await
            .expect("Arrow body");
        let mut reader =
            StreamReader::try_new(Cursor::new(body.to_vec()), None).expect("Arrow stream");
        let batch = reader.next().expect("one batch").expect("valid batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp")
                .value(0),
            123
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("message")
                .value(0),
            "request failed"
        );
    }

    #[tokio::test]
    async fn production_surface_is_authenticated_single_tenant_and_drains_fail_closed() {
        let (runtime, lifecycle) = production_runtime();
        let app = single_tenant_loki_router(
            Arc::new(LokiApiStore::default()),
            LokiApiConfig {
                default_tenant: Arc::from("tenant-a"),
                ..LokiApiConfig::default()
            },
            Arc::clone(&runtime),
            None,
            Duration::from_secs(1),
        )
        .expect("production router");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .expect("readiness request"),
            )
            .await
            .expect("readiness response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        lifecycle.mark_ready();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .expect("readiness request"),
            )
            .await
            .expect("readiness response");
        assert_eq!(response.status(), StatusCode::OK);

        let push_body = r#"{"streams":[{"stream":{"app":"api"},"values":[["100","ready"]]}]}"#;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/loki/api/v1/push")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(push_body))
                    .expect("push request"),
            )
            .await
            .expect("push response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/loki/api/v1/push")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                    .header("x-scope-orgid", "another-tenant")
                    .body(Body::from(push_body))
                    .expect("push request"),
            )
            .await
            .expect("push response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/loki/api/v1/push")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(push_body))
                    .expect("push request"),
            )
            .await
            .expect("push response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/ingester/prepare_shutdown")
                    .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                    .body(Body::empty())
                    .expect("drain request"),
            )
            .await
            .expect("drain response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(lifecycle.state(), ServiceState::Draining);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/loki/api/v1/push")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                    .body(Body::from(push_body))
                    .expect("push request"),
            )
            .await
            .expect("push response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .expect("readiness request"),
            )
            .await
            .expect("readiness response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let metrics = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);
        let body = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("UTF-8 metrics");
        assert!(body.contains("shard_log_authentication_failures_total"));
        assert!(body.contains("shard_log_ingest_records_total"));
        let counters = runtime.metrics();
        assert_eq!(counters.authentication_failures, 1);
        assert_eq!(counters.ingest_records, 1);
    }
}
