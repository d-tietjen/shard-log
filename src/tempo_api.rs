use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    DurableSpan, DurableTelemetryStore, ProductionRuntime, ServiceState, TelemetryError,
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
    /// Maximum duration accepted by one TraceQL metrics query.
    pub max_metrics_duration: Duration,
    /// Maximum number of samples in one TraceQL metrics series.
    pub max_metric_steps: usize,
    /// Maximum number of trace-derived metric series.
    pub max_metric_series: usize,
    /// Maximum exemplars returned by one trace-derived metric query.
    pub max_exemplars: usize,
    /// Maximum cross-signal record links returned by one page.
    pub max_correlations: usize,
}

impl Default for TempoApiConfig {
    fn default() -> Self {
        Self {
            tenant: Arc::from("default"),
            max_traces: 1_000,
            max_spans: 1_000_000,
            max_metrics_duration: Duration::from_secs(24 * 60 * 60),
            max_metric_steps: 11_000,
            max_metric_series: 100_000,
            max_exemplars: 100,
            max_correlations: 1_000,
        }
    }
}

impl TempoApiConfig {
    fn validate(&self) -> TelemetryResult<()> {
        if self.tenant.is_empty()
            || self.max_traces == 0
            || self.max_spans == 0
            || self.max_metrics_duration.is_zero()
            || self.max_metric_steps == 0
            || self.max_metric_series == 0
            || self.max_correlations == 0
        {
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
    store: Arc<DurableTelemetryStore>,
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
    pub fn new(store: Arc<DurableTelemetryStore>, config: TempoApiConfig) -> TelemetryResult<Self> {
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
        .route(
            "/api/shard-telemetry/v1/traces/{trace_id}/correlations",
            get(trace_correlations),
        )
        .route("/api/v2/traces/{trace_id}", get(trace_by_id))
        .route("/api/search", get(search))
        .route("/api/v2/search/tags", get(tags))
        .route("/api/v2/search/tag/{tag}/values", get(tag_values))
        .route("/api/metrics/query_range", get(metrics_query_range))
        .route("/api/metrics/query", get(metrics_query_instant))
        .with_state(service)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TraceByIdParameters {
    start: Option<u64>,
    end: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CorrelationParameters {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SearchParameters {
    #[serde(default)]
    q: String,
    start: Option<u64>,
    end: Option<u64>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MetricsParameters {
    q: String,
    start: Option<String>,
    end: Option<String>,
    since: Option<String>,
    step: Option<String>,
    exemplars: Option<usize>,
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

async fn trace_correlations(
    State(service): State<TempoService>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(parameters): Query<CorrelationParameters>,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let trace_id = match parse_trace_id(&trace_id) {
        Ok(trace_id) => trace_id,
        Err(error) => return tempo_error(StatusCode::BAD_REQUEST, &error),
    };
    let limit = parameters
        .limit
        .unwrap_or(100)
        .min(service.config.max_correlations);
    if limit == 0 {
        return tempo_error(StatusCode::BAD_REQUEST, "correlation limit must be nonzero");
    }
    let after = match parameters.cursor.as_deref().map(parse_correlation_cursor) {
        Some(Ok(cursor)) => Some(cursor),
        Some(Err(error)) => return tempo_error(StatusCode::BAD_REQUEST, &error),
        None => None,
    };
    let mut query = crate::CorrelationQuery::new(Arc::clone(&service.config.tenant))
        .with_trace_id(trace_id)
        .with_limit(limit.saturating_add(1));
    if let Some(after) = after {
        query = query.after(after);
    }
    let store = Arc::clone(&service.store);
    let result = tokio::task::spawn_blocking(move || store.query_correlations(&query)).await;
    let mut records = match result {
        Ok(Ok(records)) => records,
        Ok(Err(error)) => {
            return tempo_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
        Err(error) => {
            return tempo_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("correlation worker failed: {error}"),
            );
        }
    };
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_cursor = has_more
        .then(|| records.last().copied())
        .flatten()
        .map(correlation_cursor);
    tempo_success(json!({
        "traceId": trace_id.to_string(),
        "records": records
            .iter()
            .map(|record| json!({
                "signal": match record.signal {
                    crate::TelemetrySignal::Logs => "logs",
                    crate::TelemetrySignal::Traces => "traces",
                    crate::TelemetrySignal::Metrics => "metrics",
                },
                "partition": record.topic_partition.partition_id.get(),
                "offset": record.offset.get().to_string(),
            }))
            .collect::<Vec<_>>(),
        "nextCursor": next_cursor,
    }))
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

async fn metrics_query_range(
    State(service): State<TempoService>,
    headers: HeaderMap,
    Query(parameters): Query<MetricsParameters>,
) -> Response {
    metrics_query(service, headers, parameters, false).await
}

async fn metrics_query_instant(
    State(service): State<TempoService>,
    headers: HeaderMap,
    Query(parameters): Query<MetricsParameters>,
) -> Response {
    metrics_query(service, headers, parameters, true).await
}

async fn metrics_query(
    service: TempoService,
    headers: HeaderMap,
    parameters: MetricsParameters,
    instant: bool,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    if parameters.q.trim().is_empty() {
        return tempo_error(StatusCode::BAD_REQUEST, "TraceQL metrics query is required");
    }
    let (start, end, step, exemplars) =
        match resolve_metrics_range(&parameters, &service.config, instant) {
            Ok(range) => range,
            Err(error) => return tempo_error(StatusCode::BAD_REQUEST, &error),
        };
    let expression = parameters.q;
    let engine = service.engine();
    let max_series = service.config.max_metric_series;
    let result = tokio::task::spawn_blocking(move || {
        let series = engine.query_metrics(&expression, start, end, step, instant, exemplars)?;
        if series.len() > max_series {
            return Err(crate::TraceqlError::new(
                "TraceQL metrics series limit exceeded",
            ));
        }
        Ok(series)
    })
    .await;
    match result {
        Ok(Ok(series)) => traceql_metrics_success(series),
        Ok(Err(error)) => tempo_error(StatusCode::UNPROCESSABLE_ENTITY, &error.to_string()),
        Err(error) => tempo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("TraceQL metrics worker failed: {error}"),
        ),
    }
}

fn resolve_metrics_range(
    parameters: &MetricsParameters,
    config: &TempoApiConfig,
    instant: bool,
) -> Result<(u64, u64, u64, usize), String> {
    if parameters.since.is_some() && (parameters.start.is_some() || parameters.end.is_some()) {
        return Err("Tempo metrics since cannot be combined with start or end".into());
    }
    let now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock precedes the Unix epoch")?
        .as_nanos()
        .try_into()
        .map_err(|_| "system clock exceeds the telemetry timestamp range")?;
    let (start, end) =
        if let Some(since) = &parameters.since {
            let duration = parse_tempo_duration_nanos(since)?;
            (now.saturating_sub(duration), now)
        } else if parameters.start.is_some() || parameters.end.is_some() {
            let start = parameters.start.as_deref().ok_or_else(|| {
                "Tempo metrics start and end must be provided together".to_owned()
            })?;
            let end = parameters.end.as_deref().ok_or_else(|| {
                "Tempo metrics start and end must be provided together".to_owned()
            })?;
            (parse_tempo_time(start)?, parse_tempo_time(end)?)
        } else {
            (now.saturating_sub(60 * 60 * 1_000_000_000), now)
        };
    if start >= end {
        return Err("Tempo metrics start must precede end".into());
    }
    let duration = end - start;
    if duration > u64::try_from(config.max_metrics_duration.as_nanos()).unwrap_or(u64::MAX) {
        return Err("Tempo metrics query exceeds the duration limit".into());
    }
    let step = if instant {
        duration
    } else if let Some(step) = &parameters.step {
        parse_tempo_duration_nanos(step)?
    } else {
        (duration / 100_u64).max(1_000_000_000_u64)
    };
    if step == 0 {
        return Err("Tempo metrics step must be positive".into());
    }
    let steps = usize::try_from(duration / step)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    if steps > config.max_metric_steps {
        return Err("Tempo metrics query exceeds the step limit".into());
    }
    Ok((
        start,
        end,
        step,
        parameters
            .exemplars
            .unwrap_or(config.max_exemplars)
            .min(config.max_exemplars),
    ))
}

fn parse_tempo_time(value: &str) -> Result<u64, String> {
    if let Ok(value) = value.parse::<u64>() {
        return if value >= 10_000_000_000 {
            Ok(value)
        } else {
            value
                .checked_mul(1_000_000_000)
                .ok_or_else(|| "Tempo metrics timestamp overflows nanoseconds".into())
        };
    }
    let timestamp = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| format!("invalid Tempo metrics timestamp {value:?}"))?
        .timestamp_nanos_opt()
        .ok_or_else(|| "Tempo metrics RFC3339 timestamp exceeds nanoseconds".to_owned())?;
    u64::try_from(timestamp).map_err(|_| "Tempo metrics timestamps must be after epoch".into())
}

fn parse_tempo_duration_nanos(value: &str) -> Result<u64, String> {
    let (number, multiplier) = [
        ("ns", 1_f64),
        ("us", 1_000.0),
        ("ms", 1_000_000.0),
        ("s", 1_000_000_000.0),
        ("m", 60_000_000_000.0),
        ("h", 3_600_000_000_000.0),
        ("d", 86_400_000_000_000.0),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .ok_or_else(|| format!("invalid Tempo duration {value:?}"))?;
    let nanos = number
        .parse::<f64>()
        .map_err(|_| format!("invalid Tempo duration {value:?}"))?
        * multiplier;
    if !nanos.is_finite() || nanos <= 0.0 || nanos > u64::MAX as f64 {
        return Err(format!("invalid Tempo duration {value:?}"));
    }
    Ok(nanos.round() as u64)
}

fn traceql_metrics_success(series: Vec<crate::TraceqlMetricSeries>) -> Response {
    let series = series
        .into_iter()
        .map(|series| {
            json!({
                "labels": series.labels.into_iter().map(|(key, value)| json!({
                    "key": key,
                    "value": {"stringValue": value}
                })).collect::<Vec<_>>(),
                "samples": series.samples.into_iter().map(|sample| json!({
                    "timestampMs": sample.timestamp_ms.to_string(),
                    "value": sample.value
                })).collect::<Vec<_>>(),
                "exemplars": series.exemplars.into_iter().map(|exemplar| json!({
                    "labels": [
                        {"key": "trace:id", "value": {"stringValue": exemplar.trace_id.to_string()}},
                        {"key": "span:id", "value": {"stringValue": exemplar.span_id.to_string()}}
                    ],
                    "value": exemplar.value,
                    "timestampMs": exemplar.timestamp_ms.to_string()
                })).collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    tempo_success(json!({
        "series": series,
        "metrics": {
            "inspectedBytes": "0",
            "inspectedTraces": 0,
            "totalJobs": 1,
            "completedJobs": 1
        },
        "status": "COMPLETE"
    }))
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
    tempo_success(tempo_search_value(traces))
}

fn tempo_search_value(traces: Vec<TraceqlTrace>) -> Value {
    let traces = traces
        .into_iter()
        .map(|trace| {
            json!({
                "traceID": trace.trace_id.to_string(),
                "rootServiceName": trace.root_service_name.as_deref().unwrap_or(""),
                "rootTraceName": trace.root_name.as_deref().unwrap_or(""),
                "startTimeUnixNano": trace.start_time_unix_nanos.to_string(),
                "durationMs": trace.end_time_unix_nanos.saturating_sub(trace.start_time_unix_nanos) as f64 / 1_000_000.0,
                "spanSets": [{
                    "spans": trace.spans.iter().map(|span| json!({
                        "spanID": span.span_id.to_string(),
                        "startTimeUnixNano": span.start_time_unix_nanos.to_string(),
                        "durationNanos": span.duration_nanos.to_string(),
                        "name": span.name,
                        "attributes": trace.selected_fields.iter().filter_map(|field| {
                            trace_tag_value(span, field).map(|value| json!({
                                "key": field,
                                "value": {"stringValue": value}
                            }))
                        }).collect::<Vec<_>>()
                    })).collect::<Vec<_>>(),
                    "matched": trace.spans.len()
                }]
            })
        })
        .collect::<Vec<_>>();
    let inspected_traces = traces.len();
    json!({
        "traces": traces,
        "metrics": {"inspectedBytes": 0, "inspectedTraces": inspected_traces}
    })
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

fn correlation_cursor(record: crate::TelemetryRecordRef) -> String {
    format!(
        "{}:{}:{}",
        record.signal as u8,
        record.topic_partition.partition_id.get(),
        record.offset.get()
    )
}

fn parse_correlation_cursor(value: &str) -> Result<crate::TelemetryRecordRef, String> {
    let mut parts = value.split(':');
    let signal = parts
        .next()
        .ok_or("correlation cursor has no signal")?
        .parse::<u8>()
        .map_err(|_| "correlation cursor signal is invalid")
        .and_then(|value| {
            crate::TelemetrySignal::from_wire(value)
                .map_err(|_| "correlation cursor signal is invalid")
        })?;
    let partition = parts
        .next()
        .ok_or("correlation cursor has no partition")?
        .parse::<u32>()
        .map_err(|_| "correlation cursor partition is invalid")?;
    let offset = parts
        .next()
        .ok_or("correlation cursor has no offset")?
        .parse::<u64>()
        .map_err(|_| "correlation cursor offset is invalid")?;
    if parts.next().is_some() {
        return Err("correlation cursor contains trailing fields".into());
    }
    Ok(crate::TelemetryRecordRef::for_signal(
        signal,
        shard_stream_core::TopicPartition::new(
            signal.topic_id(),
            shard_stream_core::LogicalPartitionId::new(partition),
        ),
        shard_stream_core::LogicalOffset::new(offset),
    ))
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
    use std::fs;

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::{DurableTelemetryConfig, StripeConfig};

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

    #[test]
    fn correlation_cursor_round_trips_and_rejects_trailing_fields() {
        let record = crate::TelemetryRecordRef::for_signal(
            crate::TelemetrySignal::Metrics,
            shard_stream_core::TopicPartition::new(
                crate::METRICS_TOPIC_ID,
                shard_stream_core::LogicalPartitionId::new(17),
            ),
            shard_stream_core::LogicalOffset::new(42),
        );
        assert_eq!(
            parse_correlation_cursor(&correlation_cursor(record)).unwrap(),
            record
        );
        assert!(parse_correlation_cursor("3:17:42:extra").is_err());
    }

    #[test]
    fn search_response_uses_tempo_span_sets_shape() {
        let value = tempo_search_value(vec![TraceqlTrace {
            trace_id: TraceId::from_bytes([1; 16]).unwrap(),
            spans: Vec::new(),
            start_time_unix_nanos: 1,
            end_time_unix_nanos: 2,
            root_name: None,
            root_service_name: None,
            error_count: 0,
            selected_fields: Arc::default(),
        }]);
        assert!(value["traces"][0]["spanSets"].is_array());
        assert!(value["traces"][0].get("spanSet").is_none());
    }

    #[test]
    fn metrics_time_parser_accepts_seconds_nanoseconds_and_rfc3339() {
        assert_eq!(parse_tempo_time("2").unwrap(), 2_000_000_000);
        assert_eq!(parse_tempo_time("20000000000").unwrap(), 20_000_000_000);
        assert_eq!(
            parse_tempo_time("1970-01-01T00:00:02Z").unwrap(),
            2_000_000_000
        );
        assert_eq!(parse_tempo_duration_nanos("1.5s").unwrap(), 1_500_000_000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn traceql_metrics_routes_return_tempo_series_envelope() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-tempo-metrics-{}-{nonce}",
            std::process::id()
        ));
        let store = Arc::new(
            DurableTelemetryStore::open(DurableTelemetryConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                recovery_journal: true,
                retention: None,
                shard_count: 1,
                tenant_partitions: 1,
                append_linger: Duration::ZERO,
                stripe: StripeConfig::default(),
                indexed_ack_timeout: Duration::from_secs(30),
            })
            .unwrap(),
        );
        let service = TempoService::new(store, TempoApiConfig::default()).unwrap();
        let response = tempo_router(service.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/metrics/query_range?q=%7B%7D%20%7C%20rate%28%29&start=1&end=2&step=1s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1_024 * 1_024).await.unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["status"], "COMPLETE");
        assert_eq!(value["series"], json!([]));

        drop(service);
        fs::remove_dir_all(directory).unwrap();
    }
}
