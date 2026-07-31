use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, RawQuery, State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use prost::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::{AnalyticsLogRow, AnalyticsScanRequest};

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
}

/// Thread-safe in-memory reference backend used by differential API tests.
#[derive(Debug, Default)]
pub struct LokiApiStore {
    tenants: RwLock<HashMap<String, TenantStore>>,
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
        self.snapshot(tenant)
    }
}

/// Builds the stable Loki 3.7-compatible HTTP route surface.
pub fn loki_router(store: Arc<dyn LokiStore>, api_config: LokiApiConfig) -> Router {
    build_loki_router(store, api_config, None)
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
    Ok(build_loki_router(store, api_config, Some(bearer_token)))
}

fn build_loki_router(
    store: Arc<dyn LokiStore>,
    api_config: LokiApiConfig,
    analytics_bearer_token: Option<Arc<str>>,
) -> Router {
    let (live, _) = broadcast::channel(1_024);
    let analytics_enabled = analytics_bearer_token.is_some();
    let state = ApiState {
        store,
        config: api_config,
        deletes: Arc::new(RwLock::new(HashMap::new())),
        live,
        analytics_bearer_token,
    };
    let router = Router::new()
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/config", get(current_config))
        .route("/services", get(services))
        .route("/log_level", get(log_level).post(log_level))
        .route("/flush", post(no_content))
        .route("/ingester/prepare_shutdown", post(no_content))
        .route("/ingester/shutdown", post(no_content))
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
    let router = if analytics_enabled {
        router.route("/shardlog/api/v1/clickhouse/scan", get(clickhouse_scan))
    } else {
        router
    };
    router
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(state)
}

#[derive(Clone)]
struct ApiState {
    store: Arc<dyn LokiStore>,
    config: LokiApiConfig,
    deletes: Arc<RwLock<HashMap<String, Vec<DeleteRequest>>>>,
    live: broadcast::Sender<LivePush>,
    analytics_bearer_token: Option<Arc<str>>,
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

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::internal(message)
    }

    pub(crate) fn is_bad_request(&self) -> bool {
        self.status == StatusCode::BAD_REQUEST
    }
}

impl IntoResponse for LokiApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "status": "error",
                "errorType": if self.status == StatusCode::BAD_REQUEST { "bad_data" } else { "internal" },
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
    let tenant = tenant(&headers, &state.config);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let entries = if content_type.starts_with("application/json") {
        decode_json_push(&body)?
    } else {
        decode_protobuf_push(&body)?
    };
    state.store.push(&tenant, entries.clone())?;
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
    let tenant = tenant(&headers, &state.config);
    state.store.push(&tenant, entries.clone())?;
    let _ = state.live.send(LivePush { tenant, entries });
    Ok(StatusCode::OK)
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
}

async fn query_range(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Json<Value>, LokiApiError> {
    execute_stream_query(state, headers, params).await
}

async fn query_instant(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Json<Value>, LokiApiError> {
    execute_stream_query(state, headers, params).await
}

async fn execute_stream_query(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, LokiApiError> {
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
    let mut entries = state
        .store
        .entries(&tenant)?
        .into_iter()
        .filter(|entry| {
            entry.timestamp_unix_nanos >= start
                && entry.timestamp_unix_nanos <= end
                && selector.matches(entry)
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
    if backward {
        entries.reverse();
    }
    entries.truncate(limit);

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
    Ok(Json(success("streams", result)))
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
    line_filters: Vec<LineFilter>,
}

impl LogSelector {
    fn matches(&self, entry: &LokiEntry) -> bool {
        self.matchers
            .iter()
            .all(|matcher| matcher.matches(&entry.labels))
            && self
                .line_filters
                .iter()
                .all(|filter| filter.matches(&entry.line))
    }
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

fn parse_log_query(expression: &str) -> Result<LogSelector, LokiApiError> {
    let end = matching_brace(expression)
        .ok_or_else(|| LokiApiError::bad_request("LogQL query requires a stream selector"))?;
    let matchers = parse_selector_matchers(&expression[..=end])?;
    let line_filters = parse_line_filters(&expression[end + 1..])?;
    Ok(LogSelector {
        matchers,
        line_filters,
    })
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

fn parse_line_filters(mut input: &str) -> Result<Vec<LineFilter>, LokiApiError> {
    let mut filters = Vec::new();
    loop {
        input = input.trim_start();
        if input.is_empty() {
            return Ok(filters);
        }
        let (operation, rest) = if let Some(rest) = input.strip_prefix("|=") {
            (MatchOperation::Equal, rest)
        } else if let Some(rest) = input.strip_prefix("!=") {
            (MatchOperation::NotEqual, rest)
        } else if let Some(rest) = input.strip_prefix("|~") {
            (MatchOperation::Regex, rest)
        } else if let Some(rest) = input.strip_prefix("!~") {
            (MatchOperation::NotRegex, rest)
        } else {
            return Err(LokiApiError::bad_request(
                "unsupported or invalid LogQL pipeline stage",
            ));
        };
        let (value, rest) = parse_quoted(rest.trim_start())?;
        let regex = matches!(operation, MatchOperation::Regex | MatchOperation::NotRegex)
            .then(|| Regex::new(&value))
            .transpose()
            .map_err(|error| LokiApiError::bad_request(format!("invalid regex: {error}")))?;
        filters.push(LineFilter {
            operation,
            value,
            regex,
        });
        input = rest;
    }
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
    Err(LokiApiError::bad_request(format!(
        "invalid timestamp {value:?}"
    )))
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
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let mut names = BTreeSet::new();
    for entry in state.store.entries(&tenant)? {
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
) -> Result<Json<Value>, LokiApiError> {
    validate_label_name(&name)?;
    let tenant = tenant(&headers, &state.config);
    let (start, end) = query_range_bounds(&params)?;
    let values = state
        .store
        .entries(&tenant)?
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
) -> Result<Json<Value>, LokiApiError> {
    let pairs = form_urlencoded::parse(raw_query.as_deref().unwrap_or_default().as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let params = QueryParams {
        start: pairs
            .iter()
            .find_map(|(key, value)| (key == "start").then(|| value.clone())),
        end: pairs
            .iter()
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
    let streams = state
        .store
        .entries(&tenant)?
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
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let entries = state
        .store
        .entries(&tenant)?
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
) -> Result<Json<Value>, LokiApiError> {
    volume_response(state, headers, params)
}

async fn index_volume_range(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Json<Value>, LokiApiError> {
    volume_response(state, headers, params)
}

fn volume_response(
    state: ApiState,
    headers: HeaderMap,
    params: QueryParams,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let selector = parse_log_query(params.query.as_deref().unwrap_or("{}"))?;
    let (start, end) = query_range_bounds(&params)?;
    let mut volumes: BTreeMap<String, usize> = BTreeMap::new();
    for entry in state.store.entries(&tenant)? {
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

async fn patterns() -> Json<Value> {
    Json(json!({"status": "success", "data": []}))
}

async fn detected_fields(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let mut fields = BTreeMap::<String, BTreeSet<String>>::new();
    for entry in state.store.entries(&tenant)? {
        for (name, value) in entry.structured_metadata {
            fields.entry(name).or_default().insert(value);
        }
    }
    let fields = fields
        .into_iter()
        .map(|(label, values)| {
            json!({"label": label, "type": "string", "cardinality": values.len(), "parsers": []})
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"fields": fields, "limit": 1000})))
}

async fn detected_field_values(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let values = state
        .store
        .entries(&tenant)?
        .into_iter()
        .filter_map(|entry| entry.structured_metadata.get(&name).cloned())
        .collect::<BTreeSet<_>>();
    Ok(Json(json!({"values": values, "limit": 1000})))
}

async fn tail(
    websocket: WebSocketUpgrade,
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, LokiApiError> {
    let tenant_name = tenant(&headers, &state.config);
    let selector = parse_log_query(
        params
            .query
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("query parameter is required"))?,
    )?;
    let mut live = state.live.subscribe();
    let result = execute_stream_query(state, headers, params).await?.0;
    Ok(websocket
        .on_upgrade(move |mut socket| async move {
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
                match live.recv().await {
                    Ok(push) if push.tenant == tenant_name => {
                        let entries = push
                            .entries
                            .into_iter()
                            .filter(|entry| selector.matches(entry))
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeleteRequest {
    request_id: String,
    start_time: i64,
    end_time: i64,
    query: String,
    status: String,
    created_at: i64,
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
    let start = parse_timestamp(
        params
            .start
            .as_deref()
            .ok_or_else(|| LokiApiError::bad_request("start parameter is required"))?,
    )?;
    let end = params
        .end
        .as_deref()
        .map(parse_timestamp)
        .transpose()?
        .unwrap_or_else(now_nanos);
    let tenant = tenant(&headers, &state.config);
    let mut tenants = state
        .deletes
        .write()
        .map_err(|_| LokiApiError::internal("delete store lock is poisoned"))?;
    let deletes = tenants.entry(tenant).or_default();
    let request_id = format!("{:016x}", deletes.len() + 1);
    deletes.push(DeleteRequest {
        request_id,
        start_time: start,
        end_time: end,
        query,
        status: "received".to_owned(),
        created_at: now_nanos(),
    });
    Ok(StatusCode::NO_CONTENT)
}

async fn list_deletes(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, LokiApiError> {
    let tenant = tenant(&headers, &state.config);
    let tenants = state
        .deletes
        .read()
        .map_err(|_| LokiApiError::internal("delete store lock is poisoned"))?;
    let deletes = tenants.get(&tenant).cloned().unwrap_or_default();
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
    let mut tenants = state
        .deletes
        .write()
        .map_err(|_| LokiApiError::internal("delete store lock is poisoned"))?;
    if let Some(deletes) = tenants.get_mut(&tenant) {
        deletes.retain(|request| request.request_id != request_id);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn format_query(Query(params): Query<QueryParams>) -> Result<Json<Value>, LokiApiError> {
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

fn success(result_type: &str, result: Vec<Value>) -> Value {
    json!({
        "status": "success",
        "data": {
            "resultType": result_type,
            "result": result,
            "stats": empty_stats(),
        }
    })
}

fn empty_stats() -> Value {
    json!({
        "summary": {
            "bytesProcessedPerSecond": 0,
            "linesProcessedPerSecond": 0,
            "totalBytesProcessed": 0,
            "totalLinesProcessed": 0,
            "execTime": 0,
            "queueTime": 0,
            "subqueries": 0,
            "totalEntriesReturned": 0,
            "splits": 0,
            "shards": 0
        }
    })
}

async fn ready() -> impl IntoResponse {
    (StatusCode::OK, "ready\n")
}

async fn metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        "# HELP shard_log_ready Whether ShardLog is ready.\n# TYPE shard_log_ready gauge\nshard_log_ready 1\n",
    )
}

async fn current_config() -> Json<Value> {
    Json(json!({"target": "all", "auth_enabled": false}))
}

async fn services() -> Json<Value> {
    Json(json!({"services": [{"service": "shard-log", "status": "Running"}]}))
}

async fn log_level() -> Json<Value> {
    Json(json!({"status": "success", "message": "current log level is info"}))
}

async fn no_content() -> StatusCode {
    StatusCode::NO_CONTENT
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
                "/loki/api/v1/detected_field/trace_id/values",
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
}
