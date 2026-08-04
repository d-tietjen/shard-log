use std::fmt;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalPartitionId, TopicId, TopicPartition};

use crate::{TelemetryError, TelemetryResult};

/// Fixed shard-stream topic for log records.
pub const LOGS_TOPIC_ID: TopicId = TopicId::new(0x0000_0000_3156_5f53_474f_4c5f_4c45_5453);
/// Fixed shard-stream topic for spans.
pub const TRACES_TOPIC_ID: TopicId = TopicId::new(0x0000_3156_5f53_4543_4152_545f_4c45_5453);
/// Fixed shard-stream topic for metric points.
pub const METRICS_TOPIC_ID: TopicId = TopicId::new(0x0031_565f_5343_4952_5445_4d5f_4c45_5453);

/// Telemetry signal stored by ShardTelemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum TelemetrySignal {
    /// OpenTelemetry logs and Loki streams.
    Logs = 1,
    /// OpenTelemetry spans and Tempo-compatible traces.
    Traces = 2,
    /// OpenTelemetry and Prometheus metric points.
    Metrics = 3,
}

impl TelemetrySignal {
    /// Returns the stable shard-stream topic assigned to this signal.
    #[must_use]
    pub const fn topic_id(self) -> TopicId {
        match self {
            Self::Logs => LOGS_TOPIC_ID,
            Self::Traces => TRACES_TOPIC_ID,
            Self::Metrics => METRICS_TOPIC_ID,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> TelemetryResult<Self> {
        match value {
            1 => Ok(Self::Logs),
            2 => Ok(Self::Traces),
            3 => Ok(Self::Metrics),
            _ => Err(TelemetryError::InvalidTelemetryEnvelope(
                "unknown telemetry signal",
            )),
        }
    }
}

/// Exact OpenTelemetry attribute value.
///
/// Floating-point values are represented by their IEEE-754 bits so NaN
/// payloads, negative zero, and infinities survive every storage tier exactly.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TelemetryValue {
    /// An explicitly present `AnyValue` with no selected variant.
    Empty,
    /// UTF-8 string.
    String(Arc<str>),
    /// Boolean value.
    Boolean(bool),
    /// Signed 64-bit integer.
    Integer(i64),
    /// Exact IEEE-754 double bits.
    DoubleBits(u64),
    /// Opaque bytes.
    Bytes(Arc<[u8]>),
    /// Ordered, recursively typed array.
    Array(Arc<Vec<TelemetryValue>>),
    /// Ordered key/value list. Order and duplicate keys are retained.
    Map(Arc<Vec<TelemetryAttribute>>),
    /// Development-only OTLP string-table reference retained losslessly.
    StringTableIndex(i32),
}

impl TelemetryValue {
    /// Creates a bit-exact floating-point value.
    #[must_use]
    pub const fn from_f64(value: f64) -> Self {
        Self::DoubleBits(value.to_bits())
    }

    /// Returns this value as a floating-point value when applicable.
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::DoubleBits(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    /// Appends a stable, type-tagged representation for hashing and identity.
    pub(crate) fn append_canonical(&self, output: &mut Vec<u8>) {
        match self {
            Self::Empty => output.push(0),
            Self::String(value) => {
                output.push(1);
                append_bytes(output, value.as_bytes());
            }
            Self::Boolean(value) => {
                output.push(2);
                output.push(u8::from(*value));
            }
            Self::Integer(value) => {
                output.push(3);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::DoubleBits(bits) => {
                output.push(4);
                output.extend_from_slice(&bits.to_le_bytes());
            }
            Self::Bytes(value) => {
                output.push(5);
                append_bytes(output, value);
            }
            Self::Array(values) => {
                output.push(6);
                append_len(output, values.len());
                for value in values.iter() {
                    value.append_canonical(output);
                }
            }
            Self::Map(values) => {
                output.push(7);
                append_len(output, values.len());
                for value in values.iter() {
                    value.append_canonical(output);
                }
            }
            Self::StringTableIndex(value) => {
                output.push(8);
                output.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
}

impl fmt::Debug for TelemetryValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("Empty"),
            Self::String(value) => formatter.debug_tuple("String").field(value).finish(),
            Self::Boolean(value) => formatter.debug_tuple("Boolean").field(value).finish(),
            Self::Integer(value) => formatter.debug_tuple("Integer").field(value).finish(),
            Self::DoubleBits(bits) => formatter
                .debug_struct("Double")
                .field("value", &f64::from_bits(*bits))
                .field("bits", &format_args!("{bits:#018x}"))
                .finish(),
            Self::Bytes(value) => formatter.debug_tuple("Bytes").field(value).finish(),
            Self::Array(value) => formatter.debug_tuple("Array").field(value).finish(),
            Self::Map(value) => formatter.debug_tuple("Map").field(value).finish(),
            Self::StringTableIndex(value) => formatter
                .debug_tuple("StringTableIndex")
                .field(value)
                .finish(),
        }
    }
}

/// One exact OTLP key/value pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TelemetryAttribute {
    /// Literal key. It may be empty when `key_strindex` is populated.
    pub key: Arc<str>,
    /// Profiles string-table key reference, retained even for other signals.
    pub key_strindex: i32,
    /// `None` distinguishes an absent `AnyValue` from [`TelemetryValue::Empty`].
    pub value: Option<TelemetryValue>,
}

impl TelemetryAttribute {
    /// Creates a conventional literal-key attribute.
    #[must_use]
    pub fn new(key: impl Into<Arc<str>>, value: TelemetryValue) -> Self {
        Self {
            key: key.into(),
            key_strindex: 0,
            value: Some(value),
        }
    }

    pub(crate) fn append_canonical(&self, output: &mut Vec<u8>) {
        append_bytes(output, self.key.as_bytes());
        output.extend_from_slice(&self.key_strindex.to_le_bytes());
        match &self.value {
            Some(value) => {
                output.push(1);
                value.append_canonical(output);
            }
            None => output.push(0),
        }
    }

    /// Returns the stable, type-aware identity used to connect the same
    /// metadata key/value across logs, traces, and metrics.
    #[must_use]
    pub fn fingerprint(&self) -> AttributeFingerprint {
        let mut canonical = Vec::new();
        self.append_canonical(&mut canonical);
        AttributeFingerprint(fingerprint128(b"shard-telemetry/attribute/v1", &canonical))
    }
}

/// An OTLP Resource entity reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TelemetryEntityRef {
    /// Schema URL for this entity.
    pub schema_url: Arc<str>,
    /// Entity type.
    pub entity_type: Arc<str>,
    /// Identifying resource attribute keys.
    pub id_keys: Arc<Vec<Arc<str>>>,
    /// Descriptive resource attribute keys.
    pub description_keys: Arc<Vec<Arc<str>>>,
}

/// Exact resource context shared by records from one OTLP resource group.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceContext {
    /// Resource attributes in wire order.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Number of resource attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// Resource schema URL.
    pub schema_url: Arc<str>,
    /// Resource entity references.
    pub entity_refs: Arc<Vec<TelemetryEntityRef>>,
}

impl ResourceContext {
    /// Returns the exact, content-addressed resource identity shared by every
    /// telemetry signal.
    #[must_use]
    pub fn id(&self) -> ResourceContextId {
        let mut canonical = Vec::new();
        self.append_identity(&mut canonical);
        ResourceContextId(fingerprint128(
            b"shard-telemetry/resource-context/v1",
            &canonical,
        ))
    }

    pub(crate) fn append_identity(&self, output: &mut Vec<u8>) {
        append_bytes(output, self.schema_url.as_bytes());
        output.extend_from_slice(&self.dropped_attributes_count.to_le_bytes());
        let mut attributes = self
            .attributes
            .iter()
            .map(|attribute| {
                let mut encoded = Vec::new();
                attribute.append_canonical(&mut encoded);
                encoded
            })
            .collect::<Vec<_>>();
        attributes.sort_unstable();
        append_len(output, attributes.len());
        for attribute in attributes {
            append_bytes(output, &attribute);
        }
        append_len(output, self.entity_refs.len());
        for entity in self.entity_refs.iter() {
            append_bytes(output, entity.schema_url.as_bytes());
            append_bytes(output, entity.entity_type.as_bytes());
            append_len(output, entity.id_keys.len());
            for key in entity.id_keys.iter() {
                append_bytes(output, key.as_bytes());
            }
            append_len(output, entity.description_keys.len());
            for key in entity.description_keys.iter() {
                append_bytes(output, key.as_bytes());
            }
        }
    }
}

/// Exact instrumentation scope context shared by records from one OTLP scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeContext {
    /// Instrumentation scope name.
    pub name: Arc<str>,
    /// Instrumentation scope version.
    pub version: Arc<str>,
    /// Scope attributes in wire order.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Number of scope attributes dropped before export.
    pub dropped_attributes_count: u32,
    /// Scope schema URL.
    pub schema_url: Arc<str>,
}

impl ScopeContext {
    /// Returns the exact, content-addressed instrumentation-scope identity
    /// shared by every telemetry signal.
    #[must_use]
    pub fn id(&self) -> ScopeContextId {
        let mut canonical = Vec::new();
        self.append_identity(&mut canonical);
        ScopeContextId(fingerprint128(
            b"shard-telemetry/scope-context/v1",
            &canonical,
        ))
    }

    pub(crate) fn append_identity(&self, output: &mut Vec<u8>) {
        append_bytes(output, self.name.as_bytes());
        append_bytes(output, self.version.as_bytes());
        append_bytes(output, self.schema_url.as_bytes());
        output.extend_from_slice(&self.dropped_attributes_count.to_le_bytes());
        let mut attributes = self
            .attributes
            .iter()
            .map(|attribute| {
                let mut encoded = Vec::new();
                attribute.append_canonical(&mut encoded);
                encoded
            })
            .collect::<Vec<_>>();
        attributes.sort_unstable();
        append_len(output, attributes.len());
        for attribute in attributes {
            append_bytes(output, &attribute);
        }
    }
}

macro_rules! context_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(u128);

        impl $name {
            /// Returns the process-independent identity bits.
            #[must_use]
            pub const fn get(self) -> u128 {
                self.0
            }

            /// Reconstructs an identity previously returned by [`Self::get`].
            #[must_use]
            pub const fn from_raw(value: u128) -> Self {
                Self(value)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:032x}", self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:032x}", self.0)
            }
        }
    };
}

context_identity!(
    ResourceContextId,
    "Stable 128-bit identity of an exact resource context."
);
context_identity!(
    ScopeContextId,
    "Stable 128-bit identity of an exact instrumentation scope context."
);
context_identity!(
    AttributeFingerprint,
    "Stable 128-bit identity of one exact typed metadata key/value."
);

/// A validated 128-bit OpenTelemetry trace ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TraceId([u8; 16]);

impl TraceId {
    /// Creates a trace ID. All-zero IDs are rejected.
    pub fn from_bytes(bytes: [u8; 16]) -> TelemetryResult<Self> {
        if bytes == [0; 16] {
            return Err(TelemetryError::InvalidTraceId);
        }
        Ok(Self(bytes))
    }

    /// Parses a trace ID from its OTLP byte representation.
    pub fn from_slice(bytes: &[u8]) -> TelemetryResult<Self> {
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| TelemetryError::InvalidTraceId)?;
        Self::from_bytes(bytes)
    }

    /// Returns the exact ID bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// A validated 64-bit OpenTelemetry span ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SpanId([u8; 8]);

impl SpanId {
    /// Creates a span ID. All-zero IDs are rejected.
    pub fn from_bytes(bytes: [u8; 8]) -> TelemetryResult<Self> {
        if bytes == [0; 8] {
            return Err(TelemetryError::InvalidSpanId);
        }
        Ok(Self(bytes))
    }

    /// Parses a span ID from its OTLP byte representation.
    pub fn from_slice(bytes: &[u8]) -> TelemetryResult<Self> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| TelemetryError::InvalidSpanId)?;
        Self::from_bytes(bytes)
    }

    /// Returns the exact ID bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Stable 128-bit identity of a metric series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeriesFingerprint(u128);

impl SeriesFingerprint {
    /// Returns the fingerprint bits.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }

    pub(crate) const fn from_raw(value: u128) -> Self {
        Self(value)
    }

    pub(crate) fn from_canonical(canonical: &[u8]) -> Self {
        let digest = blake3::hash(canonical);
        Self(u128::from_le_bytes(
            digest.as_bytes()[..16].try_into().expect("fixed digest"),
        ))
    }
}

/// Bounded configuration shared by a single telemetry signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalConfig {
    /// Logical shard-stream partitions. The production default is 256.
    pub logical_partitions: NonZeroU16,
    /// Single-writer physical owner stripes. The production default is 16.
    pub physical_stripes: NonZeroU16,
    /// Optional retention. `None` retains exact data indefinitely.
    pub retention: Option<Duration>,
    /// Maximum mutable signal state per physical stripe.
    pub head_memory_bytes_per_stripe: usize,
    /// Target immutable block or chunk bytes.
    pub target_block_bytes: usize,
    /// Maximum records materialized by one query.
    pub max_query_records: usize,
}

impl SignalConfig {
    fn validate(&self, signal: TelemetrySignal) -> TelemetryResult<()> {
        if self.physical_stripes.get() > self.logical_partitions.get() {
            return Err(TelemetryError::InvalidConfiguration(format!(
                "{signal:?} physical stripes exceed logical partitions"
            )));
        }
        if self.head_memory_bytes_per_stripe == 0
            || self.target_block_bytes == 0
            || self.max_query_records == 0
        {
            return Err(TelemetryError::InvalidConfiguration(format!(
                "{signal:?} bounded limits must be nonzero"
            )));
        }
        Ok(())
    }
}

/// Complete single-node ShardTelemetry configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardTelemetryConfig {
    /// Log storage and query limits.
    pub logs: SignalConfig,
    /// Trace storage and query limits.
    pub traces: SignalConfig,
    /// Metric storage and query limits.
    pub metrics: SignalConfig,
    /// Maximum decompressed OTLP request body.
    pub max_otlp_request_bytes: usize,
    /// Maximum partition appends executing concurrently per request.
    pub max_parallel_partition_appends: NonZeroU16,
}

impl Default for ShardTelemetryConfig {
    fn default() -> Self {
        let common = |head_memory_bytes_per_stripe, target_block_bytes| SignalConfig {
            logical_partitions: NonZeroU16::new(256).expect("constant is nonzero"),
            physical_stripes: NonZeroU16::new(16).expect("constant is nonzero"),
            retention: None,
            head_memory_bytes_per_stripe,
            target_block_bytes,
            max_query_records: 1_000_000,
        };
        Self {
            logs: common(64 * 1024 * 1024, 8 * 1024 * 1024),
            traces: common(256 * 1024 * 1024, 8 * 1024 * 1024),
            metrics: common(512 * 1024 * 1024, 64 * 1024),
            max_otlp_request_bytes: 64 * 1024 * 1024,
            max_parallel_partition_appends: NonZeroU16::new(16).expect("constant is nonzero"),
        }
    }
}

impl ShardTelemetryConfig {
    /// Validates all bounded production limits.
    pub fn validate(&self) -> TelemetryResult<()> {
        self.logs.validate(TelemetrySignal::Logs)?;
        self.traces.validate(TelemetrySignal::Traces)?;
        self.metrics.validate(TelemetrySignal::Metrics)?;
        if self.max_otlp_request_bytes == 0 {
            return Err(TelemetryError::InvalidConfiguration(
                "OTLP request limit must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

/// Deterministic signal-aware logical partition router.
#[derive(Debug, Clone, Copy)]
pub struct TelemetryRouter {
    logical_partitions: [u16; 3],
}

impl TelemetryRouter {
    /// Creates a router with a fixed logical partition count.
    #[must_use]
    pub const fn new(logical_partitions: NonZeroU16) -> Self {
        Self {
            logical_partitions: [logical_partitions.get(); 3],
        }
    }

    /// Creates a router using each signal's independently configured partition count.
    #[must_use]
    pub const fn from_config(config: &ShardTelemetryConfig) -> Self {
        Self {
            logical_partitions: [
                config.logs.logical_partitions.get(),
                config.traces.logical_partitions.get(),
                config.metrics.logical_partitions.get(),
            ],
        }
    }

    /// Routes a trace by tenant and trace ID.
    #[must_use]
    pub fn trace(&self, tenant: &str, trace_id: TraceId) -> TopicPartition {
        self.route(TelemetrySignal::Traces, tenant, trace_id.as_bytes())
    }

    /// Routes a metric series by tenant and canonical series fingerprint.
    #[must_use]
    pub fn metric(&self, tenant: &str, series: SeriesFingerprint) -> TopicPartition {
        self.route(
            TelemetrySignal::Metrics,
            tenant,
            &series.get().to_le_bytes(),
        )
    }

    /// Routes a log by trace ID when present, otherwise by its stream/resource identity.
    #[must_use]
    pub fn log(
        &self,
        tenant: &str,
        trace_id: Option<TraceId>,
        stream_or_resource_fingerprint: &[u8],
    ) -> TopicPartition {
        match trace_id {
            Some(trace_id) => self.route(TelemetrySignal::Logs, tenant, trace_id.as_bytes()),
            None => self.route(
                TelemetrySignal::Logs,
                tenant,
                stream_or_resource_fingerprint,
            ),
        }
    }

    fn route(&self, signal: TelemetrySignal, tenant: &str, identity: &[u8]) -> TopicPartition {
        let signal_index = match signal {
            TelemetrySignal::Logs => 0,
            TelemetrySignal::Traces => 1,
            TelemetrySignal::Metrics => 2,
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"shard-telemetry-route-v1\0");
        hasher.update(&[signal as u8]);
        hasher.update(&(tenant.len() as u64).to_le_bytes());
        hasher.update(tenant.as_bytes());
        hasher.update(identity);
        let digest = hasher.finalize();
        let hash = u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("fixed digest"));
        TopicPartition::new(
            signal.topic_id(),
            LogicalPartitionId::new(
                (hash % u64::from(self.logical_partitions[signal_index])) as u32,
            ),
        )
    }
}

fn append_len(output: &mut Vec<u8>, len: usize) {
    output.extend_from_slice(&(len as u64).to_le_bytes());
}

fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    append_len(output, bytes.len());
    output.extend_from_slice(bytes);
}

fn fingerprint128(domain: &[u8], canonical: &[u8]) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&[0]);
    hasher.update(&(canonical.len() as u64).to_le_bytes());
    hasher.update(canonical);
    let digest = hasher.finalize();
    u128::from_le_bytes(
        digest.as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest contains 16 bytes"),
    )
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_point_values_preserve_every_bit() {
        let bits = 0x7ff8_0000_0000_0042;
        let value = TelemetryValue::from_f64(f64::from_bits(bits));
        assert_eq!(value.as_f64().expect("double").to_bits(), bits);
        assert_eq!(value, TelemetryValue::DoubleBits(bits));
    }

    #[test]
    fn signal_topics_and_routes_are_distinct_and_stable() {
        let router = TelemetryRouter::new(NonZeroU16::new(256).unwrap());
        let trace_id = TraceId::from_bytes([7; 16]).unwrap();
        let series = SeriesFingerprint::from_canonical(b"series");
        let trace = router.trace("tenant", trace_id);
        let metric = router.metric("tenant", series);
        let log = router.log("tenant", Some(trace_id), b"ignored");
        assert_eq!(trace.topic_id, TRACES_TOPIC_ID);
        assert_eq!(metric.topic_id, METRICS_TOPIC_ID);
        assert_eq!(log.topic_id, LOGS_TOPIC_ID);
        assert_eq!(
            trace.partition_id,
            router.trace("tenant", trace_id).partition_id
        );
        assert_ne!(trace.topic_id, metric.topic_id);
    }

    #[test]
    fn signal_router_honors_independent_partition_counts() {
        let mut config = ShardTelemetryConfig::default();
        config.logs.logical_partitions = NonZeroU16::new(3).unwrap();
        config.traces.logical_partitions = NonZeroU16::new(1).unwrap();
        config.metrics.logical_partitions = NonZeroU16::new(2).unwrap();
        config.logs.physical_stripes = NonZeroU16::new(1).unwrap();
        config.traces.physical_stripes = NonZeroU16::new(1).unwrap();
        config.metrics.physical_stripes = NonZeroU16::new(1).unwrap();
        let router = TelemetryRouter::from_config(&config);
        let trace_id = TraceId::from_bytes([9; 16]).unwrap();
        let series = SeriesFingerprint::from_canonical(b"independent-series");
        assert!(router.log("tenant", None, b"stream").partition_id.get() < 3);
        assert_eq!(router.trace("tenant", trace_id).partition_id.get(), 0);
        assert!(router.metric("tenant", series).partition_id.get() < 2);
    }

    #[test]
    fn production_defaults_are_bounded() {
        let config = ShardTelemetryConfig::default();
        config.validate().unwrap();
        assert_eq!(config.logs.logical_partitions.get(), 256);
        assert_eq!(
            config.traces.head_memory_bytes_per_stripe,
            256 * 1024 * 1024
        );
        assert_eq!(
            config.metrics.head_memory_bytes_per_stripe,
            512 * 1024 * 1024
        );
        assert_eq!(config.max_otlp_request_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn context_and_attribute_identities_are_exact_and_order_independent() {
        let service = TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("checkout")),
        );
        let region = TelemetryAttribute::new(
            "cloud.region",
            TelemetryValue::String(Arc::from("us-east-1")),
        );
        let left = ResourceContext {
            attributes: Arc::new(vec![service.clone(), region.clone()]),
            ..ResourceContext::default()
        };
        let right = ResourceContext {
            attributes: Arc::new(vec![region, service.clone()]),
            ..ResourceContext::default()
        };
        assert_eq!(left.id(), right.id());
        assert_eq!(left.id().to_string().len(), 32);
        assert_eq!(service.fingerprint(), service.clone().fingerprint());

        let mut changed = right;
        changed.dropped_attributes_count = 1;
        assert_ne!(left.id(), changed.id());
        assert_ne!(
            service.fingerprint(),
            TelemetryAttribute::new(
                "service.name",
                TelemetryValue::String(Arc::from("payments")),
            )
            .fingerprint()
        );
    }
}
