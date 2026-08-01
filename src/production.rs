use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

/// Runtime lifecycle exposed by the production HTTP and native servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ServiceState {
    /// Durable recovery and listener initialization are still in progress.
    Starting = 0,
    /// The service accepts ingestion and queries.
    Ready = 1,
    /// New ingestion is rejected while accepted work is flushed.
    Draining = 2,
    /// A process shutdown has been requested.
    Stopping = 3,
    /// A durable or operational invariant failed.
    Failed = 4,
}

impl ServiceState {
    fn from_raw(value: u8) -> Self {
        match value {
            0 => Self::Starting,
            1 => Self::Ready,
            2 => Self::Draining,
            3 => Self::Stopping,
            _ => Self::Failed,
        }
    }

    /// Returns the stable lowercase name used by metrics and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
        }
    }
}

/// Shared lifecycle controller for HTTP, native TCP, and process shutdown.
pub struct ServiceLifecycle {
    state: AtomicU8,
    failure: RwLock<Option<Arc<str>>>,
    shutdown: watch::Sender<bool>,
}

impl fmt::Debug for ServiceLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceLifecycle")
            .field("state", &self.state())
            .field("failure", &self.failure())
            .finish_non_exhaustive()
    }
}

impl Default for ServiceLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceLifecycle {
    /// Creates a lifecycle in the starting state.
    #[must_use]
    pub fn new() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            state: AtomicU8::new(ServiceState::Starting as u8),
            failure: RwLock::new(None),
            shutdown,
        }
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        ServiceState::from_raw(self.state.load(Ordering::Acquire))
    }

    /// Marks successful durable recovery and enables ingestion.
    pub fn mark_ready(&self) {
        let _ = self.state.compare_exchange(
            ServiceState::Starting as u8,
            ServiceState::Ready as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Rejects new ingestion while allowing reads and an explicit flush.
    pub fn begin_draining(&self) {
        loop {
            let current = self.state();
            if matches!(
                current,
                ServiceState::Draining | ServiceState::Stopping | ServiceState::Failed
            ) {
                return;
            }
            if self
                .state
                .compare_exchange(
                    current as u8,
                    ServiceState::Draining as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    /// Requests process shutdown after transitioning out of readiness.
    pub fn request_shutdown(&self) {
        if self.state() != ServiceState::Failed {
            self.state
                .store(ServiceState::Stopping as u8, Ordering::Release);
        }
        let _ = self.shutdown.send(true);
    }

    /// Fails readiness and preserves a bounded operator-facing reason.
    pub fn mark_failed(&self, reason: impl Into<Arc<str>>) {
        if let Ok(mut failure) = self.failure.write() {
            *failure = Some(reason.into());
        }
        self.state
            .store(ServiceState::Failed as u8, Ordering::Release);
    }

    /// Returns the retained failure reason, when one exists.
    #[must_use]
    pub fn failure(&self) -> Option<Arc<str>> {
        self.failure.read().ok().and_then(|failure| failure.clone())
    }

    /// Returns whether the API may accept a new ingest request.
    #[must_use]
    pub fn accepts_ingest(&self) -> bool {
        self.state() == ServiceState::Ready
    }

    /// Subscribes to the process-shutdown request.
    #[must_use]
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }
}

/// Fail-closed limits and credentials for one production tenant.
#[derive(Clone, PartialEq, Eq)]
pub struct SingleTenantConfig {
    /// Tenant identity attached to every accepted HTTP and native operation.
    pub tenant: Arc<str>,
    /// Shared bearer secret required by HTTP and native authentication.
    pub bearer_token: Arc<str>,
    /// Maximum concurrent HTTP requests across the process.
    pub max_http_in_flight: usize,
    /// Maximum concurrent ingest requests across HTTP and native paths.
    pub max_ingest_in_flight: usize,
    /// Maximum concurrent query requests across HTTP and native paths.
    pub max_query_in_flight: usize,
    /// Sustained accepted ingest bytes per second. Zero disables rate limiting.
    pub ingest_bytes_per_second: u64,
    /// Maximum token-bucket burst. Required when rate limiting is enabled.
    pub ingest_burst_bytes: u64,
    /// Maximum simultaneously connected Loki tail subscribers.
    pub max_tail_subscribers: usize,
    /// Maximum simultaneously connected native TCP clients.
    pub max_native_connections: usize,
    /// Maximum wall-clock time for one HTTP or native query response.
    pub query_timeout: Duration,
    /// Maximum time a native connection may remain unauthenticated.
    pub native_auth_timeout: Duration,
}

impl fmt::Debug for SingleTenantConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SingleTenantConfig")
            .field("tenant", &self.tenant)
            .field("bearer_token", &"<redacted>")
            .field("max_http_in_flight", &self.max_http_in_flight)
            .field("max_ingest_in_flight", &self.max_ingest_in_flight)
            .field("max_query_in_flight", &self.max_query_in_flight)
            .field("ingest_bytes_per_second", &self.ingest_bytes_per_second)
            .field("ingest_burst_bytes", &self.ingest_burst_bytes)
            .field("max_tail_subscribers", &self.max_tail_subscribers)
            .field("max_native_connections", &self.max_native_connections)
            .field("query_timeout", &self.query_timeout)
            .field("native_auth_timeout", &self.native_auth_timeout)
            .finish()
    }
}

/// Monotonic protocol counters exposed by the Prometheus endpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProductionMetricsSnapshot {
    /// HTTP requests admitted after authentication.
    pub http_requests: u64,
    /// Authentication failures across HTTP and native TCP.
    pub authentication_failures: u64,
    /// Requests rejected by concurrency, lifecycle, or rate limits.
    pub rejected_requests: u64,
    /// Accepted ingest operations.
    pub ingest_requests: u64,
    /// Accepted source bytes at protocol boundaries.
    pub ingest_bytes: u64,
    /// Accepted normalized log records.
    pub ingest_records: u64,
    /// Admitted query operations.
    pub query_requests: u64,
    /// Native TCP connections accepted by the listener.
    pub native_connections: u64,
    /// Tail subscriptions admitted by the HTTP API.
    pub tail_subscriptions: u64,
}

#[derive(Debug, Default)]
struct ProductionMetrics {
    http_requests: AtomicU64,
    authentication_failures: AtomicU64,
    rejected_requests: AtomicU64,
    ingest_requests: AtomicU64,
    ingest_bytes: AtomicU64,
    ingest_records: AtomicU64,
    query_requests: AtomicU64,
    native_connections: AtomicU64,
    tail_subscriptions: AtomicU64,
}

impl ProductionMetrics {
    fn snapshot(&self) -> ProductionMetricsSnapshot {
        ProductionMetricsSnapshot {
            http_requests: self.http_requests.load(Ordering::Relaxed),
            authentication_failures: self.authentication_failures.load(Ordering::Relaxed),
            rejected_requests: self.rejected_requests.load(Ordering::Relaxed),
            ingest_requests: self.ingest_requests.load(Ordering::Relaxed),
            ingest_bytes: self.ingest_bytes.load(Ordering::Relaxed),
            ingest_records: self.ingest_records.load(Ordering::Relaxed),
            query_requests: self.query_requests.load(Ordering::Relaxed),
            native_connections: self.native_connections.load(Ordering::Relaxed),
            tail_subscriptions: self.tail_subscriptions.load(Ordering::Relaxed),
        }
    }
}

impl SingleTenantConfig {
    /// Validates the production boundary before listeners are exposed.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.tenant.is_empty() {
            return Err("single-tenant identity must not be empty");
        }
        if self.bearer_token.len() < 16 {
            return Err("production bearer token must contain at least 16 bytes");
        }
        if self.bearer_token.len() > 4_096 {
            return Err("production bearer token must not exceed 4096 bytes");
        }
        if self.max_http_in_flight == 0
            || self.max_ingest_in_flight == 0
            || self.max_query_in_flight == 0
            || self.max_tail_subscribers == 0
            || self.max_native_connections == 0
        {
            return Err("production concurrency limits must be nonzero");
        }
        if self.max_ingest_in_flight > self.max_http_in_flight
            || self.max_query_in_flight > self.max_http_in_flight
        {
            return Err("ingest and query concurrency cannot exceed the HTTP limit");
        }
        if self.ingest_bytes_per_second > 0 && self.ingest_burst_bytes == 0 {
            return Err("an enabled ingest rate requires a nonzero burst");
        }
        if self.query_timeout.is_zero() {
            return Err("production query timeout must be nonzero");
        }
        if self.native_auth_timeout.is_zero() {
            return Err("native authentication timeout must be nonzero");
        }
        Ok(())
    }

    /// Performs a fixed-work comparison of a supplied bearer token.
    #[must_use]
    pub fn authenticates(&self, observed: &str) -> bool {
        let observed = blake3::hash(observed.as_bytes());
        let expected = blake3::hash(self.bearer_token.as_bytes());
        observed
            .as_bytes()
            .iter()
            .zip(expected.as_bytes())
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
    }
}

#[derive(Debug)]
struct RateState {
    tokens: u64,
    remainder: u128,
    updated: Instant,
}

/// Integer token bucket shared by the single tenant's protocol boundaries.
#[derive(Debug)]
pub(crate) struct IngestRateLimiter {
    bytes_per_second: u64,
    burst_bytes: u64,
    state: Mutex<RateState>,
}

impl IngestRateLimiter {
    pub(crate) fn new(bytes_per_second: u64, burst_bytes: u64) -> Self {
        Self {
            bytes_per_second,
            burst_bytes,
            state: Mutex::new(RateState {
                tokens: burst_bytes,
                remainder: 0,
                updated: Instant::now(),
            }),
        }
    }

    pub(crate) fn try_acquire(&self, bytes: usize) -> bool {
        if self.bytes_per_second == 0 {
            return true;
        }
        let Ok(bytes) = u64::try_from(bytes) else {
            return false;
        };
        if bytes > self.burst_bytes {
            return false;
        }
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(state.updated).as_nanos();
        state.updated = now;
        let generated = elapsed
            .saturating_mul(u128::from(self.bytes_per_second))
            .saturating_add(state.remainder);
        let added = generated / 1_000_000_000;
        state.remainder = generated % 1_000_000_000;
        state.tokens = state
            .tokens
            .saturating_add(u64::try_from(added).unwrap_or(u64::MAX))
            .min(self.burst_bytes);
        if state.tokens < bytes {
            return false;
        }
        state.tokens -= bytes;
        true
    }
}

/// Shared single-tenant admission and lifecycle state for every protocol.
#[derive(Debug)]
pub struct ProductionRuntime {
    config: SingleTenantConfig,
    lifecycle: Arc<ServiceLifecycle>,
    http: Arc<Semaphore>,
    ingest: Arc<Semaphore>,
    query: Arc<Semaphore>,
    tail: Arc<Semaphore>,
    native_connections: Arc<Semaphore>,
    rate: IngestRateLimiter,
    metrics: ProductionMetrics,
}

impl ProductionRuntime {
    /// Creates a fail-closed runtime after validating credentials and limits.
    pub fn new(
        config: SingleTenantConfig,
        lifecycle: Arc<ServiceLifecycle>,
    ) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            http: Arc::new(Semaphore::new(config.max_http_in_flight)),
            ingest: Arc::new(Semaphore::new(config.max_ingest_in_flight)),
            query: Arc::new(Semaphore::new(config.max_query_in_flight)),
            tail: Arc::new(Semaphore::new(config.max_tail_subscribers)),
            native_connections: Arc::new(Semaphore::new(config.max_native_connections)),
            rate: IngestRateLimiter::new(config.ingest_bytes_per_second, config.ingest_burst_bytes),
            config,
            lifecycle,
            metrics: ProductionMetrics::default(),
        })
    }

    /// Returns the configured immutable tenant identity.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.config.tenant
    }

    /// Returns the shared lifecycle controller.
    #[must_use]
    pub fn lifecycle(&self) -> &Arc<ServiceLifecycle> {
        &self.lifecycle
    }

    /// Returns the configured query response deadline.
    #[must_use]
    pub fn query_timeout(&self) -> Duration {
        self.config.query_timeout
    }

    /// Returns the native connection authentication deadline.
    #[must_use]
    pub fn native_auth_timeout(&self) -> Duration {
        self.config.native_auth_timeout
    }

    /// Authenticates one protocol credential.
    #[must_use]
    pub fn authenticates(&self, observed: &str) -> bool {
        let accepted = self.config.authenticates(observed);
        if !accepted {
            self.record_authentication_failure();
        }
        accepted
    }

    /// Records a failed authentication that could not supply a valid token.
    pub fn record_authentication_failure(&self) {
        self.metrics
            .authentication_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Tries to reserve one complete HTTP request.
    pub fn try_http(&self) -> Option<OwnedSemaphorePermit> {
        let permit = Arc::clone(&self.http).try_acquire_owned().ok();
        if permit.is_some() {
            self.metrics.http_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.reject();
        }
        permit
    }

    /// Tries to reserve an ingest operation and its source bytes.
    pub fn try_ingest(&self, source_bytes: usize) -> Option<OwnedSemaphorePermit> {
        if !self.lifecycle.accepts_ingest() || !self.rate.try_acquire(source_bytes) {
            self.reject();
            return None;
        }
        let permit = Arc::clone(&self.ingest).try_acquire_owned().ok();
        if permit.is_none() {
            self.reject();
        }
        permit
    }

    /// Tries to reserve a bounded query operation.
    pub fn try_query(&self) -> Option<OwnedSemaphorePermit> {
        if matches!(
            self.lifecycle.state(),
            ServiceState::Starting | ServiceState::Stopping | ServiceState::Failed
        ) {
            self.reject();
            return None;
        }
        let permit = Arc::clone(&self.query).try_acquire_owned().ok();
        if permit.is_none() {
            self.reject();
        }
        permit
    }

    /// Tries to reserve one live-tail subscription.
    pub fn try_tail(&self) -> Option<OwnedSemaphorePermit> {
        let permit = Arc::clone(&self.tail).try_acquire_owned().ok();
        if permit.is_some() {
            self.metrics
                .tail_subscriptions
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.reject();
        }
        permit
    }

    /// Records a successfully decoded ingest operation.
    pub fn record_ingest(&self, source_bytes: usize, records: usize) {
        self.metrics.ingest_requests.fetch_add(1, Ordering::Relaxed);
        self.metrics.ingest_bytes.fetch_add(
            u64::try_from(source_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.metrics.ingest_records.fetch_add(
            u64::try_from(records).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Records one admitted query.
    pub fn record_query(&self) {
        self.metrics.query_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Tries to reserve one native TCP connection for its complete lifetime.
    pub fn try_native_connection(&self) -> Option<OwnedSemaphorePermit> {
        let permit = Arc::clone(&self.native_connections)
            .try_acquire_owned()
            .ok();
        if permit.is_some() {
            self.metrics
                .native_connections
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.reject();
        }
        permit
    }

    /// Records a request rejected after reaching a protocol boundary.
    pub fn reject(&self) {
        self.metrics
            .rejected_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a lock-free operational counter snapshot.
    #[must_use]
    pub fn metrics(&self) -> ProductionMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Returns current admission usage for metrics.
    #[must_use]
    pub fn admission_in_flight(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.config
                .max_http_in_flight
                .saturating_sub(self.http.available_permits()),
            self.config
                .max_ingest_in_flight
                .saturating_sub(self.ingest.available_permits()),
            self.config
                .max_query_in_flight
                .saturating_sub(self.query.available_permits()),
            self.config
                .max_tail_subscribers
                .saturating_sub(self.tail.available_permits()),
            self.config
                .max_native_connections
                .saturating_sub(self.native_connections.available_permits()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_never_reopens_after_drain() {
        let lifecycle = ServiceLifecycle::new();
        lifecycle.mark_ready();
        assert!(lifecycle.accepts_ingest());
        lifecycle.begin_draining();
        lifecycle.mark_ready();
        assert_eq!(lifecycle.state(), ServiceState::Draining);
        assert!(!lifecycle.accepts_ingest());
        lifecycle.request_shutdown();
        assert_eq!(lifecycle.state(), ServiceState::Stopping);
    }

    #[test]
    fn credentials_and_limits_fail_closed() {
        let config = SingleTenantConfig {
            tenant: Arc::from("production"),
            bearer_token: Arc::from("0123456789abcdef"),
            max_http_in_flight: 64,
            max_ingest_in_flight: 32,
            max_query_in_flight: 32,
            ingest_bytes_per_second: 1_024,
            ingest_burst_bytes: 2_048,
            max_tail_subscribers: 16,
            max_native_connections: 128,
            query_timeout: Duration::from_secs(30),
            native_auth_timeout: Duration::from_secs(5),
        };
        assert_eq!(config.validate(), Ok(()));
        assert!(config.authenticates("0123456789abcdef"));
        assert!(!config.authenticates("0123456789abcdeg"));
    }

    #[test]
    fn integer_rate_limiter_bounds_a_burst() {
        let limiter = IngestRateLimiter::new(1, 8);
        assert!(limiter.try_acquire(8));
        assert!(!limiter.try_acquire(1));
        assert!(!limiter.try_acquire(9));
    }

    #[test]
    fn runtime_enforces_lifecycle_and_concurrency() {
        let lifecycle = Arc::new(ServiceLifecycle::new());
        let runtime = ProductionRuntime::new(
            SingleTenantConfig {
                tenant: Arc::from("production"),
                bearer_token: Arc::from("0123456789abcdef"),
                max_http_in_flight: 1,
                max_ingest_in_flight: 1,
                max_query_in_flight: 1,
                ingest_bytes_per_second: 0,
                ingest_burst_bytes: 0,
                max_tail_subscribers: 1,
                max_native_connections: 1,
                query_timeout: Duration::from_secs(30),
                native_auth_timeout: Duration::from_secs(5),
            },
            Arc::clone(&lifecycle),
        )
        .expect("runtime");
        assert!(runtime.try_ingest(1).is_none());
        lifecycle.mark_ready();
        let permit = runtime.try_http().expect("first request");
        assert!(runtime.try_http().is_none());
        drop(permit);
        assert!(runtime.try_http().is_some());
        let native = runtime.try_native_connection().expect("native connection");
        assert!(runtime.try_native_connection().is_none());
        drop(native);
        assert!(runtime.try_native_connection().is_some());
        lifecycle.begin_draining();
        assert!(runtime.try_ingest(1).is_none());
        assert!(runtime.try_query().is_some());
    }
}
