use std::num::NonZeroU16;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Form, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    DurableLokiStore, ProductionRuntime, PromqlEngine, PromqlLimits, PromqlValue,
    RemoteWriteDecoder, RemoteWriteStats, RemoteWriteVersion, ServiceState, TelemetryError,
    TelemetryResult, TelemetryRouter,
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
        .route("/api/v1/query", get(query_get).post(query_post))
        .route(
            "/api/v1/query_range",
            get(query_range_get).post(query_range_post),
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
    use super::*;

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
}
