use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::tempo_protocol::trace_by_id_response;
use crate::{
    DurableLokiStore, DurableSpan, ProductionRuntime, ServiceState, TelemetryError,
    TelemetryResult, TelemetryValue, TraceId, TraceqlEngine, TraceqlLimits, TraceqlTrace,
};

/// Single-tenant Tempo compatibility limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempoApiConfig {
    /// Authenticated tenant assigned to every query.
    pub tenant: Arc<str>,
    /// Maximum traces returned by a search.
    pub max_traces: usize,
    /// Maximum spans inspected by one query.
    pub max_spans: usize,
}

impl Default for TempoApiConfig {
    fn default() -> Self {
        Self {
            tenant: Arc::from("default"),
            max_traces: 1_000,
            max_spans: 1_000_000,
        }
    }
}

impl TempoApiConfig {
    fn validate(&self) -> TelemetryResult<()> {
        if self.tenant.is_empty() || self.max_traces == 0 || self.max_spans == 0 {
            return Err(TelemetryError::InvalidConfiguration(
                "Tempo tenant and query limits must be nonempty".into(),
            ));
        }
        Ok(())
    }
}

/// Shared Tempo-compatible query service.
#[derive(Clone)]
pub struct TempoService {
    store: Arc<DurableLokiStore>,
    config: TempoApiConfig,
    production: Option<Arc<ProductionRuntime>>,
}

impl std::fmt::Debug for TempoService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TempoService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TempoService {
    /// Creates a bounded Tempo query service.
    pub fn new(store: Arc<DurableLokiStore>, config: TempoApiConfig) -> TelemetryResult<Self> {
        config.validate()?;
        Ok(Self {
            store,
            config,
            production: None,
        })
    }

    /// Attaches fail-closed production authentication and query admission.
    #[must_use]
    pub fn with_production(mut self, production: Option<Arc<ProductionRuntime>>) -> Self {
        self.production = production;
        self
    }

    fn engine(&self) -> TraceqlEngine {
        TraceqlEngine::new(
            Arc::clone(&self.store),
            Arc::clone(&self.config.tenant),
            TraceqlLimits {
                max_spans: self.config.max_spans,
                max_traces: self.config.max_traces,
            },
        )
    }

    #[allow(clippy::result_large_err)]
    fn authorize(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, Response> {
        if headers
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|tenant| tenant != self.config.tenant.as_ref())
        {
            return Err(tempo_error(
                StatusCode::FORBIDDEN,
                "tenant header does not match the configured tenant",
            ));
        }
        let Some(runtime) = &self.production else {
            return Ok(None);
        };
        let credential = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !credential.is_some_and(|value| runtime.authenticates(value)) {
            if credential.is_none() {
                runtime.record_authentication_failure();
            }
            return Err(tempo_error(
                StatusCode::UNAUTHORIZED,
                "valid production bearer token is required",
            ));
        }
        if !matches!(runtime.lifecycle().state(), ServiceState::Ready) {
            return Err(tempo_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "trace query service is unavailable",
            ));
        }
        runtime.try_query().map(Some).ok_or_else(|| {
            tempo_error(
                StatusCode::TOO_MANY_REQUESTS,
                "trace query concurrency limit exceeded",
            )
        })
    }
}

/// Builds Tempo v2, search, tag-discovery, and TraceQL routes.
pub fn tempo_router(service: TempoService) -> Router {
    Router::new()
        .route("/api/v2/traces/{trace_id}", get(trace_by_id))
        .route("/api/search", get(search))
        .route("/api/v2/search/tags", get(tags))
        .route("/api/v2/search/tag/{tag}/values", get(tag_values))
        .with_state(service)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TraceByIdParameters {
    start: Option<u64>,
    end: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SearchParameters {
    #[serde(default)]
    q: String,
    start: Option<u64>,
    end: Option<u64>,
    limit: Option<usize>,
}

async fn trace_by_id(
    State(service): State<TempoService>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(parameters): Query<TraceByIdParameters>,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    if parameters
        .start
        .zip(parameters.end)
        .is_some_and(|(start, end)| start > end)
    {
        return tempo_error(StatusCode::BAD_REQUEST, "invalid trace time range");
    }
    let trace_id = match parse_trace_id(&trace_id) {
        Ok(trace_id) => trace_id,
        Err(error) => return tempo_error(StatusCode::BAD_REQUEST, &error),
    };
    let engine = service.engine();
    let result = tokio::task::spawn_blocking(move || engine.trace_by_id(trace_id)).await;
    let trace = match result {
        Ok(Ok(Some(trace))) => trace,
        Ok(Ok(None)) => return tempo_error(StatusCode::NOT_FOUND, "trace not found"),
        Ok(Err(error)) => {
            return tempo_error(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string());
        }
        Err(error) => {
            return tempo_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("trace worker failed: {error}"),
            );
        }
    };
    let response = trace_by_id_response(&trace);
    if headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("json"))
    {
        return (
            StatusCode::OK,
            axum::Json(json!({
                "batches": response.trace.map_or_else(Vec::new, |trace| trace.batches),
                "metrics": {"inspectedBytes": 0}
            })),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf"),
        )],
        response.encode_to_vec(),
    )
        .into_response()
}

async fn search(
    State(service): State<TempoService>,
    headers: HeaderMap,
    Query(parameters): Query<SearchParameters>,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let expression = if parameters.q.trim().is_empty() {
        "{}".to_owned()
    } else {
        parameters.q
    };
    let start = parameters
        .start
        .and_then(|seconds| seconds.checked_mul(1_000_000_000));
    let end = parameters
        .end
        .and_then(|seconds| seconds.checked_mul(1_000_000_000));
    let limit = parameters
        .limit
        .unwrap_or(service.config.max_traces)
        .min(service.config.max_traces);
    let engine = service.engine();
    let result =
        tokio::task::spawn_blocking(move || engine.search(&expression, start, end, limit)).await;
    match result {
        Ok(Ok(traces)) => tempo_search_success(traces),
        Ok(Err(error)) => tempo_error(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string()),
        Err(error) => tempo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("TraceQL worker failed: {error}"),
        ),
    }
}

async fn tags(State(service): State<TempoService>, headers: HeaderMap) -> Response {
    let traces = match all_traces(&service, &headers).await {
        Ok(traces) => traces,
        Err(response) => return response,
    };
    let mut resource = BTreeSet::new();
    let mut span = BTreeSet::new();
    let mut event = BTreeSet::new();
    let mut link = BTreeSet::new();
    for trace in traces {
        for item in trace.spans {
            resource.extend(
                item.resource
                    .attributes
                    .iter()
                    .map(|value| value.key.to_string()),
            );
            span.extend(item.attributes.iter().map(|value| value.key.to_string()));
            for item in item.events.iter() {
                event.extend(item.attributes.iter().map(|value| value.key.to_string()));
            }
            for item in item.links.iter() {
                link.extend(item.attributes.iter().map(|value| value.key.to_string()));
            }
        }
    }
    tempo_success(json!({
        "scopes": [
            {"name": "intrinsic", "tags": ["duration", "kind", "name", "parent", "span:id", "status", "statusMessage", "trace:id"]},
            {"name": "resource", "tags": resource},
            {"name": "span", "tags": span},
            {"name": "event", "tags": event},
            {"name": "link", "tags": link}
        ],
        "metrics": {"inspectedBytes": 0}
    }))
}

async fn tag_values(
    State(service): State<TempoService>,
    headers: HeaderMap,
    Path(tag): Path<String>,
) -> Response {
    let traces = match all_traces(&service, &headers).await {
        Ok(traces) => traces,
        Err(response) => return response,
    };
    let mut values = BTreeSet::new();
    for trace in traces {
        for span in trace.spans {
            if let Some(value) = trace_tag_value(&span, &tag) {
                values.insert(value);
            }
        }
    }
    let values = values
        .into_iter()
        .map(|value| json!({"type": "string", "value": value}))
        .collect::<Vec<_>>();
    tempo_success(json!({
        "tagValues": values,
        "metrics": {"inspectedBytes": 0}
    }))
}

#[allow(clippy::result_large_err)]
async fn all_traces(
    service: &TempoService,
    headers: &HeaderMap,
) -> Result<Vec<TraceqlTrace>, Response> {
    let _permit = service.authorize(headers)?;
    let engine = service.engine();
    let limit = service.config.max_traces;
    match tokio::task::spawn_blocking(move || engine.search("{}", None, None, limit)).await {
        Ok(Ok(traces)) => Ok(traces),
        Ok(Err(error)) => Err(tempo_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &error.to_string(),
        )),
        Err(error) => Err(tempo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("trace discovery worker failed: {error}"),
        )),
    }
}

fn tempo_search_success(traces: Vec<TraceqlTrace>) -> Response {
    let traces = traces
        .into_iter()
        .map(|trace| {
            json!({
                "traceID": trace.trace_id.to_string(),
                "rootServiceName": trace.root_service_name.as_deref().unwrap_or(""),
                "rootTraceName": trace.root_name.as_deref().unwrap_or(""),
                "startTimeUnixNano": trace.start_time_unix_nanos.to_string(),
                "durationMs": trace.end_time_unix_nanos.saturating_sub(trace.start_time_unix_nanos) as f64 / 1_000_000.0,
                "spanSet": {
                    "spans": trace.spans.iter().map(|span| json!({
                        "spanID": span.span_id.to_string(),
                        "startTimeUnixNano": span.start_time_unix_nanos.to_string(),
                        "durationNanos": span.duration_nanos.to_string(),
                        "name": span.name
                    })).collect::<Vec<_>>(),
                    "matched": trace.spans.len()
                }
            })
        })
        .collect::<Vec<_>>();
    let inspected_traces = traces.len();
    tempo_success(json!({
        "traces": traces,
        "metrics": {"inspectedBytes": 0, "inspectedTraces": inspected_traces}
    }))
}

fn trace_tag_value(span: &DurableSpan, tag: &str) -> Option<String> {
    match tag.trim_start_matches('.') {
        "name" | "span:name" => Some(span.name.to_string()),
        "duration" | "span:duration" => Some(span.duration_nanos.to_string()),
        "kind" | "span:kind" => Some(span.kind.to_string()),
        "status" | "span:status" => Some(
            span.status
                .as_ref()
                .map_or(0, |status| status.code)
                .to_string(),
        ),
        "trace:id" => Some(span.trace_id.to_string()),
        "span:id" => Some(span.span_id.to_string()),
        value => value
            .strip_prefix("resource.")
            .and_then(|key| typed_attribute(&span.resource.attributes, key))
            .or_else(|| {
                value
                    .strip_prefix("span.")
                    .and_then(|key| typed_attribute(&span.attributes, key))
            }),
    }
}

fn typed_attribute(attributes: &[crate::TelemetryAttribute], key: &str) -> Option<String> {
    attributes
        .iter()
        .rev()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| attribute.value.as_ref())
        .map(typed_value)
}

fn typed_value(value: &TelemetryValue) -> String {
    match value {
        TelemetryValue::Empty => String::new(),
        TelemetryValue::String(value) => value.to_string(),
        TelemetryValue::Boolean(value) => value.to_string(),
        TelemetryValue::Integer(value) => value.to_string(),
        TelemetryValue::DoubleBits(bits) => f64::from_bits(*bits).to_string(),
        TelemetryValue::Bytes(value) => value.iter().map(|value| format!("{value:02x}")).collect(),
        TelemetryValue::Array(_) | TelemetryValue::Map(_) => {
            serde_json::to_string(value).unwrap_or_default()
        }
        TelemetryValue::StringTableIndex(value) => value.to_string(),
    }
}

fn parse_trace_id(value: &str) -> Result<TraceId, String> {
    if value.len() != 32 {
        return Err("trace ID must contain 32 hexadecimal characters".into());
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| "trace ID contains a non-hexadecimal character")?;
    }
    TraceId::from_bytes(bytes).map_err(|error| error.to_string())
}

fn tempo_success(data: Value) -> Response {
    (StatusCode::OK, axum::Json(data)).into_response()
}

fn tempo_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"))],
        message.to_owned(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_ids_fail_closed_on_length_hex_and_zero() {
        assert!(parse_trace_id("01").is_err());
        assert!(parse_trace_id("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_err());
        assert!(parse_trace_id("00000000000000000000000000000000").is_err());
        assert_eq!(
            parse_trace_id("01010101010101010101010101010101").unwrap(),
            TraceId::from_bytes([1; 16]).unwrap()
        );
    }
}
