use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::prometheus_protocol::v1 as prometheus_v1;
use crate::{
    DurableLokiStore, MetricKind, MetricValue, NumberValue, ProductionRuntime, PromqlEngine,
    PromqlLimits, PromqlValue, RemoteWriteDecoder, RemoteWriteStats, RemoteWriteVersion,
    ServiceState, TelemetryError, TelemetryResult, TelemetryRouter, TelemetryValue,
};

/// Single-tenant Prometheus compatibility limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrometheusApiConfig {
    /// Authenticated tenant assigned to every series.
    pub tenant: Arc<str>,
    /// Maximum compressed and Snappy-decompressed request bytes.
    pub max_request_bytes: usize,
    /// Stable logical metric partitions.
    pub logical_partitions: NonZeroU16,
}

impl Default for PrometheusApiConfig {
    fn default() -> Self {
        Self {
            tenant: Arc::from("default"),
            max_request_bytes: 64 * 1024 * 1024,
            logical_partitions: NonZeroU16::new(256).expect("constant is nonzero"),
        }
    }
}

impl PrometheusApiConfig {
    fn validate(&self) -> TelemetryResult<()> {
        if self.tenant.is_empty() {
            return Err(TelemetryError::InvalidConfiguration(
                "Prometheus tenant must not be empty".into(),
            ));
        }
        if self.max_request_bytes == 0 || self.max_request_bytes > 64 * 1024 * 1024 {
            return Err(TelemetryError::InvalidConfiguration(
                "Prometheus request limit must be in 1..=64 MiB".into(),
            ));
        }
        Ok(())
    }
}

/// Shared Prometheus protocol service backed by signal-native metric stripes.
#[derive(Clone)]
pub struct PrometheusService {
    store: Arc<DurableLokiStore>,
    config: PrometheusApiConfig,
    router: TelemetryRouter,
    production: Option<Arc<ProductionRuntime>>,
}

impl std::fmt::Debug for PrometheusService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrometheusService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PrometheusService {
    /// Creates a Prometheus API service.
    pub fn new(store: Arc<DurableLokiStore>, config: PrometheusApiConfig) -> TelemetryResult<Self> {
        config.validate()?;
        Ok(Self {
            store,
            router: TelemetryRouter::new(config.logical_partitions),
            config,
            production: None,
        })
    }

    /// Attaches shared fail-closed production admission.
    #[must_use]
    pub fn with_production(mut self, production: Option<Arc<ProductionRuntime>>) -> Self {
        self.production = production;
        self
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
            return Err(write_error(
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
            return Err(write_error(
                StatusCode::UNAUTHORIZED,
                "valid production bearer token is required",
            ));
        }
        runtime.try_http().map(Some).ok_or_else(|| {
            write_error(
                StatusCode::TOO_MANY_REQUESTS,
                "HTTP concurrency limit exceeded",
            )
        })
    }
}

/// Builds Prometheus Remote Write and query compatibility routes.
pub fn prometheus_router(service: PrometheusService) -> Router {
    let max_request_bytes = service.config.max_request_bytes;
    Router::new()
        .route("/api/v1/write", post(remote_write))
        .route("/api/v1/read", post(remote_read))
        .route("/api/v1/query", get(query_get).post(query_post))
        .route(
            "/api/v1/query_range",
            get(query_range_get).post(query_range_post),
        )
        .route("/api/v1/series", get(series_get).post(series_post))
        .route("/api/v1/labels", get(labels_get).post(labels_post))
        .route(
            "/api/v1/label/{name}/values",
            get(label_values_get).post(label_values_post),
        )
        .route("/api/v1/metadata", get(metadata_get))
        .route(
            "/api/v1/query_exemplars",
            get(exemplars_get).post(exemplars_post),
        )
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(service)
}

#[derive(Debug, Clone, Deserialize)]
struct InstantQueryParameters {
    query: String,
    time: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RangeQueryParameters {
    query: String,
    start: String,
    end: String,
    step: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct DiscoveryParameters {
    #[serde(default, rename = "match[]")]
    selectors: Vec<String>,
    start: Option<String>,
    end: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct MetadataParameters {
    metric: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExemplarParameters {
    query: String,
    start: String,
    end: String,
}

async fn query_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<InstantQueryParameters>,
) -> Response {
    execute_instant_query(service, headers, parameters).await
}

async fn query_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<InstantQueryParameters>,
) -> Response {
    execute_instant_query(service, headers, parameters).await
}

async fn execute_instant_query(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: InstantQueryParameters,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let time_ms = match parameters
        .time
        .as_deref()
        .map(parse_prometheus_time)
        .transpose()
    {
        Ok(Some(value)) => value,
        Ok(None) => current_time_ms(),
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let expression = parameters.query;
    match tokio::task::spawn_blocking(move || engine.query(&expression, time_ms)).await {
        Ok(Ok(value)) => query_success(value),
        Ok(Err(error)) => query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        ),
        Err(error) => query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("PromQL worker failed: {error}"),
        ),
    }
}

async fn query_range_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<RangeQueryParameters>,
) -> Response {
    execute_range_query(service, headers, parameters).await
}

async fn query_range_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<RangeQueryParameters>,
) -> Response {
    execute_range_query(service, headers, parameters).await
}

async fn execute_range_query(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: RangeQueryParameters,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let start_ms = match parse_prometheus_time(&parameters.start) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let end_ms = match parse_prometheus_time(&parameters.end) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let step_ms = match parse_prometheus_duration(&parameters.step) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let expression = parameters.query;
    match tokio::task::spawn_blocking(move || {
        engine.query_range(&expression, start_ms, end_ms, step_ms)
    })
    .await
    {
        Ok(Ok(value)) => query_success(value),
        Ok(Err(error)) => query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        ),
        Err(error) => query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("PromQL worker failed: {error}"),
        ),
    }
}

async fn series_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<DiscoveryParameters>,
) -> Response {
    execute_series(service, headers, parameters).await
}

async fn series_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<DiscoveryParameters>,
) -> Response {
    execute_series(service, headers, parameters).await
}

async fn execute_series(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: DiscoveryParameters,
) -> Response {
    let points = match discovery_points(&service, &headers, &parameters).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let series = points
        .iter()
        .map(metric_labels)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    api_success(json!(series))
}

async fn labels_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<DiscoveryParameters>,
) -> Response {
    execute_labels(service, headers, parameters).await
}

async fn labels_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<DiscoveryParameters>,
) -> Response {
    execute_labels(service, headers, parameters).await
}

async fn execute_labels(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: DiscoveryParameters,
) -> Response {
    let points = match discovery_points(&service, &headers, &parameters).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let mut labels = BTreeSet::new();
    for point in &points {
        labels.extend(metric_labels(point).into_keys());
    }
    api_success(json!(labels))
}

async fn label_values_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(parameters): Query<DiscoveryParameters>,
) -> Response {
    execute_label_values(service, headers, name, parameters).await
}

async fn label_values_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Form(parameters): Form<DiscoveryParameters>,
) -> Response {
    execute_label_values(service, headers, name, parameters).await
}

async fn execute_label_values(
    service: PrometheusService,
    headers: HeaderMap,
    name: String,
    parameters: DiscoveryParameters,
) -> Response {
    if name.is_empty() {
        return query_error(StatusCode::BAD_REQUEST, "bad_data", "label name is empty");
    }
    let points = match discovery_points(&service, &headers, &parameters).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let values = points
        .iter()
        .filter_map(|point| metric_labels(point).remove(&name))
        .collect::<BTreeSet<_>>();
    api_success(json!(values))
}

async fn metadata_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<MetadataParameters>,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let selectors = parameters
        .metric
        .as_ref()
        .map(|metric| vec![metric.clone()])
        .unwrap_or_default();
    let points = match tokio::task::spawn_blocking(move || {
        engine.raw_points(&selectors, 0, current_time_ms())
    })
    .await
    {
        Ok(Ok(points)) => points,
        Ok(Err(error)) => {
            return query_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "execution",
                &error.to_string(),
            );
        }
        Err(error) => {
            return query_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &format!("metadata worker failed: {error}"),
            );
        }
    };
    let limit = parameters.limit.unwrap_or(usize::MAX);
    let mut metadata = BTreeMap::<String, BTreeSet<(String, String, String)>>::new();
    for point in points {
        if metadata.len() >= limit && !metadata.contains_key(point.identity.name.as_ref()) {
            continue;
        }
        metadata
            .entry(point.identity.name.to_string())
            .or_default()
            .insert((
                prometheus_metric_type(&point.identity.kind).into(),
                point.description.to_string(),
                point.identity.unit.to_string(),
            ));
    }
    let data = metadata
        .into_iter()
        .map(|(name, entries)| {
            let entries = entries
                .into_iter()
                .map(|(metric_type, help, unit)| {
                    json!({"type": metric_type, "help": help, "unit": unit})
                })
                .collect::<Vec<_>>();
            (name, Value::Array(entries))
        })
        .collect::<serde_json::Map<_, _>>();
    api_success(Value::Object(data))
}

async fn exemplars_get(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Query(parameters): Query<ExemplarParameters>,
) -> Response {
    execute_exemplars(service, headers, parameters).await
}

async fn exemplars_post(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    Form(parameters): Form<ExemplarParameters>,
) -> Response {
    execute_exemplars(service, headers, parameters).await
}

async fn execute_exemplars(
    service: PrometheusService,
    headers: HeaderMap,
    parameters: ExemplarParameters,
) -> Response {
    let start_ms = match parse_prometheus_time(&parameters.start) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let end_ms = match parse_prometheus_time(&parameters.end) {
        Ok(value) => value,
        Err(error) => return query_error(StatusCode::BAD_REQUEST, "bad_data", &error),
    };
    let discovery = DiscoveryParameters {
        selectors: vec![parameters.query],
        start: Some((start_ms as f64 / 1_000.0).to_string()),
        end: Some((end_ms as f64 / 1_000.0).to_string()),
    };
    let points = match discovery_points(&service, &headers, &discovery).await {
        Ok(points) => points,
        Err(response) => return response,
    };
    let mut series = BTreeMap::<BTreeMap<String, String>, Vec<Value>>::new();
    for point in points {
        let labels = metric_labels(&point);
        for exemplar in point.exemplars.iter() {
            let exemplar_labels = exemplar
                .filtered_attributes
                .iter()
                .filter_map(|attribute| {
                    attribute
                        .value
                        .as_ref()
                        .map(|value| (attribute.key.to_string(), render_telemetry_value(value)))
                })
                .collect::<BTreeMap<_, _>>();
            series.entry(labels.clone()).or_default().push(json!({
                "labels": exemplar_labels,
                "value": format_number(exemplar.value),
                "timestamp": timestamp_seconds(
                    i64::try_from(exemplar.timestamp_unix_nanos / 1_000_000).unwrap_or(i64::MAX)
                ),
                "traceID": exemplar.trace_id.map(|id| id.to_string()).unwrap_or_default(),
                "spanID": exemplar.span_id.map(|id| id.to_string()).unwrap_or_default()
            }));
        }
    }
    let data = series
        .into_iter()
        .map(|(series_labels, exemplars)| {
            json!({"seriesLabels": series_labels, "exemplars": exemplars})
        })
        .collect::<Vec<_>>();
    api_success(json!(data))
}

#[allow(clippy::result_large_err)]
async fn discovery_points(
    service: &PrometheusService,
    headers: &HeaderMap,
    parameters: &DiscoveryParameters,
) -> Result<Vec<crate::DurableMetricPoint>, Response> {
    let _permit = service.authorize(headers)?;
    let start_ms = parameters
        .start
        .as_deref()
        .map(parse_prometheus_time)
        .transpose()
        .map_err(|error| query_error(StatusCode::BAD_REQUEST, "bad_data", &error))?
        .unwrap_or(0);
    let end_ms = parameters
        .end
        .as_deref()
        .map(parse_prometheus_time)
        .transpose()
        .map_err(|error| query_error(StatusCode::BAD_REQUEST, "bad_data", &error))?
        .unwrap_or_else(current_time_ms);
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let selectors = parameters.selectors.clone();
    match tokio::task::spawn_blocking(move || engine.raw_points(&selectors, start_ms, end_ms)).await
    {
        Ok(Ok(points)) => Ok(points),
        Ok(Err(error)) => Err(query_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
            &error.to_string(),
        )),
        Err(error) => Err(query_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &format!("discovery worker failed: {error}"),
        )),
    }
}

async fn remote_write(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _http_permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    if body.len() > service.config.max_request_bytes {
        return write_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Remote Write body is too large",
        );
    }
    if !headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("snappy"))
    {
        return write_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Remote Write requires Content-Encoding: snappy",
        );
    }
    let content_type = match headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => value,
        None => {
            return write_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Remote Write requires a protobuf Content-Type",
            );
        }
    };
    let version = match RemoteWriteDecoder::version_from_content_type(content_type) {
        Ok(version) => version,
        Err(error) => return write_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, &error.to_string()),
    };
    if let Err(response) = validate_version_header(&headers, version) {
        return response;
    }
    let decompressed_len = match snap::raw::decompress_len(&body) {
        Ok(length) if length <= service.config.max_request_bytes => length,
        Ok(_) => {
            return write_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Remote Write Snappy payload expands beyond 64 MiB",
            );
        }
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let mut protobuf = vec![0_u8; decompressed_len];
    if let Err(error) = snap::raw::Decoder::new().decompress(&body, &mut protobuf) {
        return write_error(StatusCode::BAD_REQUEST, &error.to_string());
    }
    let decoded = match RemoteWriteDecoder.decode(&service.config.tenant, version, &protobuf) {
        Ok(decoded) => decoded,
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let stats = decoded.stats;
    let batch = match decoded.into_native_batch(&service.router) {
        Ok(batch) => batch,
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    if batch.partitions.is_empty() {
        return write_success(stats);
    }
    let _ingest_permit = match &service.production {
        Some(runtime) => match runtime.try_ingest(protobuf.len()) {
            Some(permit) => Some(permit),
            None if runtime.lifecycle().state() == ServiceState::Ready => {
                return write_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Remote Write concurrency or rate limit exceeded",
                );
            }
            None => {
                return write_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Remote Write ingestion is unavailable",
                );
            }
        },
        None => None,
    };
    let store = Arc::clone(&service.store);
    let result = tokio::task::spawn_blocking(move || store.append_remote_write_batch(&batch)).await;
    match result {
        Ok(Ok(_)) => {
            if let Some(runtime) = &service.production {
                let records = stats.samples.saturating_add(stats.histograms);
                runtime.record_ingest(protobuf.len(), records as usize);
            }
            write_success(stats)
        }
        Ok(Err(error)) => write_error(error.status(), &error.to_string()),
        Err(error) => write_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Remote Write worker failed: {error}"),
        ),
    }
}

async fn remote_read(
    State(service): State<PrometheusService>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _permit = match service.authorize(&headers) {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    if body.len() > service.config.max_request_bytes {
        return write_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Remote Read body is too large",
        );
    }
    if !headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("snappy"))
    {
        return write_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Remote Read requires Content-Encoding: snappy",
        );
    }
    let decompressed_len = match snap::raw::decompress_len(&body) {
        Ok(length) if length <= service.config.max_request_bytes => length,
        Ok(_) => {
            return write_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Remote Read Snappy payload expands beyond 64 MiB",
            );
        }
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let mut protobuf = vec![0_u8; decompressed_len];
    if let Err(error) = snap::raw::Decoder::new().decompress(&body, &mut protobuf) {
        return write_error(StatusCode::BAD_REQUEST, &error.to_string());
    }
    let request = match prometheus_v1::ReadRequest::decode(protobuf.as_slice()) {
        Ok(request) => request,
        Err(error) => return write_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let accepts_samples = request.accepted_response_types.is_empty()
        || request.accepted_response_types.iter().any(|response_type| {
            *response_type == prometheus_v1::ReadRequestResponseType::Samples as i32
        });
    if !accepts_samples {
        return write_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "streamed XOR Remote Read is not enabled until its conformance gate passes",
        );
    }
    let engine = PromqlEngine::new(
        Arc::clone(&service.store),
        Arc::clone(&service.config.tenant),
        PromqlLimits::default(),
    );
    let result = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(request.queries.len());
        for query in request.queries {
            let selector = remote_read_selector(&query.matchers)?;
            let points = engine.raw_points(
                &[selector],
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )?;
            let mut series =
                BTreeMap::<BTreeMap<String, String>, Vec<prometheus_v1::Sample>>::new();
            for point in points {
                if let Some(value) = remote_read_float(&point.value) {
                    series
                        .entry(metric_labels(&point))
                        .or_default()
                        .push(prometheus_v1::Sample {
                            value,
                            timestamp: i64::try_from(point.timestamp_unix_nanos / 1_000_000)
                                .unwrap_or(i64::MAX),
                        });
                }
            }
            let timeseries = series
                .into_iter()
                .map(|(labels, mut samples)| {
                    samples.sort_unstable_by_key(|sample| sample.timestamp);
                    prometheus_v1::TimeSeries {
                        labels: labels
                            .into_iter()
                            .map(|(name, value)| prometheus_v1::Label { name, value })
                            .collect(),
                        samples,
                        exemplars: Vec::new(),
                        histograms: Vec::new(),
                    }
                })
                .collect();
            results.push(prometheus_v1::QueryResult { timeseries });
        }
        Ok::<_, crate::PromqlError>(prometheus_v1::ReadResponse { results })
    })
    .await;
    let response = match result {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return query_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "execution",
                &error.to_string(),
            );
        }
        Err(error) => {
            return query_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &format!("Remote Read worker failed: {error}"),
            );
        }
    };
    let protobuf = response.encode_to_vec();
    let compressed = match snap::raw::Encoder::new().compress_vec(&protobuf) {
        Ok(compressed) => compressed,
        Err(error) => return write_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            ),
            (header::CONTENT_ENCODING, HeaderValue::from_static("snappy")),
        ],
        compressed,
    )
        .into_response()
}

#[allow(clippy::result_large_err)]
fn validate_version_header(
    headers: &HeaderMap,
    version: RemoteWriteVersion,
) -> Result<(), Response> {
    let Some(observed) = headers
        .get("x-prometheus-remote-write-version")
        .and_then(|value| value.to_str().ok())
    else {
        return Err(write_error(
            StatusCode::BAD_REQUEST,
            "missing X-Prometheus-Remote-Write-Version",
        ));
    };
    let valid = match version {
        RemoteWriteVersion::V1 => observed == "0.1.0" || observed.starts_with("1."),
        RemoteWriteVersion::V2 => observed.starts_with("2."),
    };
    if valid {
        Ok(())
    } else {
        Err(write_error(
            StatusCode::BAD_REQUEST,
            "Remote Write version header conflicts with Content-Type",
        ))
    }
}

fn write_success(stats: RemoteWriteStats) -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    for (name, value) in [
        ("x-prometheus-remote-write-samples-written", stats.samples),
        (
            "x-prometheus-remote-write-histograms-written",
            stats.histograms,
        ),
        (
            "x-prometheus-remote-write-exemplars-written",
            stats.exemplars,
        ),
    ] {
        response.headers_mut().insert(
            name,
            HeaderValue::from_str(&value.to_string()).expect("u64 is a valid header value"),
        );
    }
    response
}

fn write_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"))],
        message.to_owned(),
    )
        .into_response()
}

fn query_success(value: PromqlValue) -> Response {
    let (result_type, result) = match value {
        PromqlValue::Scalar {
            timestamp_ms,
            value,
        } => (
            "scalar",
            json!([timestamp_seconds(timestamp_ms), format_sample(value)]),
        ),
        PromqlValue::String {
            timestamp_ms,
            value,
        } => ("string", json!([timestamp_seconds(timestamp_ms), value])),
        PromqlValue::Vector(samples) => (
            "vector",
            Value::Array(
                samples
                    .into_iter()
                    .map(|sample| {
                        json!({
                            "metric": sample.labels,
                            "value": [
                                timestamp_seconds(sample.timestamp_ms),
                                format_sample(sample.value)
                            ]
                        })
                    })
                    .collect(),
            ),
        ),
        PromqlValue::Matrix(series) => (
            "matrix",
            Value::Array(
                series
                    .into_iter()
                    .map(|series| {
                        let values = series
                            .samples
                            .into_iter()
                            .map(|(timestamp, value)| {
                                json!([timestamp_seconds(timestamp), format_sample(value)])
                            })
                            .collect::<Vec<_>>();
                        json!({"metric": series.labels, "values": values})
                    })
                    .collect(),
            ),
        ),
    };
    (
        StatusCode::OK,
        axum::Json(json!({
            "status": "success",
            "data": {"resultType": result_type, "result": result}
        })),
    )
        .into_response()
}

fn api_success(data: Value) -> Response {
    (
        StatusCode::OK,
        axum::Json(json!({"status": "success", "data": data})),
    )
        .into_response()
}

fn metric_labels(point: &crate::DurableMetricPoint) -> BTreeMap<String, String> {
    let mut labels = crate::prometheus_string_labels(&point.identity)
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("__name__".into(), point.identity.name.to_string());
    labels
}

fn remote_read_selector(
    matchers: &[prometheus_v1::LabelMatcher],
) -> Result<String, crate::PromqlError> {
    if matchers.is_empty() {
        return Ok("{__name__=~\".+\"}".into());
    }
    let mut selector = String::from("{");
    for (index, matcher) in matchers.iter().enumerate() {
        if matcher.name.is_empty() {
            return Err(crate::PromqlError::new(
                "Remote Read matcher has an empty label name",
            ));
        }
        if index != 0 {
            selector.push(',');
        }
        selector.push_str(&matcher.name);
        selector.push_str(
            match prometheus_v1::LabelMatcherType::try_from(matcher.r#type) {
                Ok(prometheus_v1::LabelMatcherType::Equal) => "=",
                Ok(prometheus_v1::LabelMatcherType::NotEqual) => "!=",
                Ok(prometheus_v1::LabelMatcherType::RegexMatch) => "=~",
                Ok(prometheus_v1::LabelMatcherType::RegexNoMatch) => "!~",
                Err(_) => {
                    return Err(crate::PromqlError::new(
                        "Remote Read matcher has an unknown type",
                    ));
                }
            },
        );
        selector.push_str(
            &serde_json::to_string(&matcher.value)
                .map_err(|error| crate::PromqlError::new(error.to_string()))?,
        );
    }
    selector.push('}');
    Ok(selector)
}

fn remote_read_float(value: &MetricValue) -> Option<f64> {
    match value {
        MetricValue::Gauge(value) | MetricValue::Sum(value) => Some(match value {
            NumberValue::Integer(value) => *value as f64,
            NumberValue::DoubleBits(bits) => f64::from_bits(*bits),
        }),
        MetricValue::ExplicitHistogram(_)
        | MetricValue::ExponentialHistogram(_)
        | MetricValue::Summary(_) => None,
    }
}

fn prometheus_metric_type(kind: &MetricKind) -> &'static str {
    match kind {
        MetricKind::Gauge => "gauge",
        MetricKind::Sum {
            monotonic: true, ..
        } => "counter",
        MetricKind::Sum { .. } => "gauge",
        MetricKind::ExplicitHistogram { .. } | MetricKind::ExponentialHistogram { .. } => {
            "histogram"
        }
        MetricKind::Summary => "summary",
    }
}

fn format_number(value: NumberValue) -> String {
    match value {
        NumberValue::Integer(value) => value.to_string(),
        NumberValue::DoubleBits(bits) => format_sample(f64::from_bits(bits)),
    }
}

fn render_telemetry_value(value: &TelemetryValue) -> String {
    match value {
        TelemetryValue::Empty => String::new(),
        TelemetryValue::String(value) => value.to_string(),
        TelemetryValue::Boolean(value) => value.to_string(),
        TelemetryValue::Integer(value) => value.to_string(),
        TelemetryValue::DoubleBits(bits) => format_sample(f64::from_bits(*bits)),
        TelemetryValue::Bytes(value) => value.iter().map(|byte| format!("{byte:02x}")).collect(),
        TelemetryValue::Array(_) | TelemetryValue::Map(_) => {
            serde_json::to_string(value).unwrap_or_default()
        }
        TelemetryValue::StringTableIndex(value) => value.to_string(),
    }
}

fn query_error(status: StatusCode, error_type: &str, message: &str) -> Response {
    (
        status,
        axum::Json(json!({
            "status": "error",
            "errorType": error_type,
            "error": message
        })),
    )
        .into_response()
}

fn format_sample(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        value.to_string()
    }
}

fn timestamp_seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms as f64 / 1_000.0
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

fn parse_prometheus_time(value: &str) -> Result<i64, String> {
    let seconds = value
        .parse::<f64>()
        .map_err(|_| format!("invalid Prometheus timestamp {value:?}"))?;
    if !seconds.is_finite() {
        return Err(format!("invalid Prometheus timestamp {value:?}"));
    }
    let milliseconds = seconds * 1_000.0;
    if milliseconds < i64::MIN as f64 || milliseconds > i64::MAX as f64 {
        return Err("Prometheus timestamp is out of range".into());
    }
    Ok(milliseconds.round() as i64)
}

fn parse_prometheus_duration(value: &str) -> Result<i64, String> {
    if let Ok(seconds) = value.parse::<f64>()
        && seconds.is_finite()
        && seconds > 0.0
    {
        return Ok((seconds * 1_000.0).round() as i64);
    }
    let (number, multiplier) = [
        ("ms", 1_i64),
        ("s", 1_000),
        ("m", 60_000),
        ("h", 3_600_000),
        ("d", 86_400_000),
        ("w", 604_800_000),
        ("y", 31_536_000_000),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })
    .ok_or_else(|| format!("invalid Prometheus duration {value:?}"))?;
    let number = number
        .parse::<f64>()
        .map_err(|_| format!("invalid Prometheus duration {value:?}"))?;
    let milliseconds = number * multiplier as f64;
    if !milliseconds.is_finite() || milliseconds <= 0.0 || milliseconds > i64::MAX as f64 {
        return Err(format!("invalid Prometheus duration {value:?}"));
    }
    Ok(milliseconds.round() as i64)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::{DurableLokiConfig, StripeConfig};

    fn test_service() -> (PrometheusService, std::path::PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-prometheus-api-{}-{nonce}",
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
                append_linger: Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: Duration::from_secs(30),
            })
            .expect("store opens"),
        );
        let service = PrometheusService::new(
            store,
            PrometheusApiConfig {
                tenant: Arc::from("tenant-a"),
                logical_partitions: NonZeroU16::new(8).unwrap(),
                ..PrometheusApiConfig::default()
            },
        )
        .expect("service");
        (service, directory)
    }

    #[test]
    fn version_header_must_match_negotiated_schema() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-prometheus-remote-write-version",
            HeaderValue::from_static("2.0.0"),
        );
        assert!(validate_version_header(&headers, RemoteWriteVersion::V2).is_ok());
        assert!(validate_version_header(&headers, RemoteWriteVersion::V1).is_err());
    }

    #[test]
    fn success_reports_all_required_written_headers() {
        let response = write_success(RemoteWriteStats {
            samples: 3,
            histograms: 2,
            exemplars: 1,
        });
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()["x-prometheus-remote-write-samples-written"],
            "3"
        );
        assert_eq!(
            response.headers()["x-prometheus-remote-write-histograms-written"],
            "2"
        );
        assert_eq!(
            response.headers()["x-prometheus-remote-write-exemplars-written"],
            "1"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_discovery_and_remote_read_routes_use_prometheus_envelopes() {
        let (service, directory) = test_service();
        let app = prometheus_router(service.clone());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=1%2B2&time=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("query response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1_024 * 1_024)
            .await
            .expect("query body");
        let body: Value = serde_json::from_slice(&body).expect("query JSON");
        assert_eq!(body["status"], "success");
        assert_eq!(body["data"]["resultType"], "scalar");
        assert_eq!(body["data"]["result"][1], "3");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/series?start=0&end=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("series response");
        assert_eq!(response.status(), StatusCode::OK);

        let request = prometheus_v1::ReadRequest {
            queries: Vec::new(),
            accepted_response_types: vec![prometheus_v1::ReadRequestResponseType::Samples as i32],
        };
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .expect("Snappy request");
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/read")
                    .header(header::CONTENT_ENCODING, "snappy")
                    .body(Body::from(compressed))
                    .unwrap(),
            )
            .await
            .expect("read response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1_024 * 1_024)
            .await
            .expect("read body");
        let protobuf = snap::raw::Decoder::new()
            .decompress_vec(&body)
            .expect("Snappy response");
        assert!(
            prometheus_v1::ReadResponse::decode(protobuf.as_slice())
                .expect("read response protobuf")
                .results
                .is_empty()
        );

        drop(service);
        fs::remove_dir_all(directory).expect("remove test store");
    }
}
