use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use regex::Regex;

use crate::{
    DurableLokiStore, DurableSpan, TelemetryAttribute, TelemetryValue, TraceId, TraceQuery,
};

/// Bounded TraceQL execution limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlLimits {
    /// Maximum spans materialized before trace grouping.
    pub max_spans: usize,
    /// Maximum traces returned by one search.
    pub max_traces: usize,
}

impl Default for TraceqlLimits {
    fn default() -> Self {
        Self {
            max_spans: 1_000_000,
            max_traces: 1_000,
        }
    }
}

/// One trace selected by the clean-room TraceQL evaluator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlTrace {
    /// Trace ID.
    pub trace_id: TraceId,
    /// Winning spans ordered by start time and durable offset.
    pub spans: Vec<DurableSpan>,
    /// Earliest span start.
    pub start_time_unix_nanos: u64,
    /// Latest span end.
    pub end_time_unix_nanos: u64,
    /// Root span name when present.
    pub root_name: Option<Arc<str>>,
    /// Root resource service name when present.
    pub root_service_name: Option<Arc<str>>,
    /// Number of error spans.
    pub error_count: u32,
}

/// TraceQL parse or execution error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlError(String);

impl TraceqlError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TraceqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TraceqlError {}

/// Clean-room Rust TraceQL evaluator backed by trace-owner stripes.
#[derive(Clone)]
pub struct TraceqlEngine {
    store: Arc<DurableLokiStore>,
    tenant: Arc<str>,
    limits: TraceqlLimits,
}

impl fmt::Debug for TraceqlEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceqlEngine")
            .field("tenant", &self.tenant)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl TraceqlEngine {
    /// Creates a bounded single-tenant evaluator.
    #[must_use]
    pub fn new(store: Arc<DurableLokiStore>, tenant: Arc<str>, limits: TraceqlLimits) -> Self {
        Self {
            store,
            tenant,
            limits,
        }
    }

    /// Executes a TraceQL spanset filter over a bounded trace/time window.
    pub fn search(
        &self,
        expression: &str,
        start_time_unix_nanos: Option<u64>,
        end_time_unix_nanos: Option<u64>,
        limit: usize,
    ) -> Result<Vec<TraceqlTrace>, TraceqlError> {
        let filter = TraceFilter::parse(expression)?;
        let pushed_trace_id = filter.exact_trace_id();
        let spans = self
            .store
            .query_traces(&TraceQuery {
                tenant: Arc::clone(&self.tenant),
                trace_id: pushed_trace_id,
                start_time_unix_nanos,
                end_time_unix_nanos,
                limit: self.limits.max_spans,
                ..TraceQuery::default()
            })
            .map_err(|error| TraceqlError::new(error.to_string()))?;
        let mut traces = BTreeMap::<TraceId, Vec<DurableSpan>>::new();
        for span in spans {
            if filter.matches(&span) {
                traces.entry(span.trace_id).or_default().push(span);
            }
        }
        let result_limit = limit.max(1).min(self.limits.max_traces);
        let mut results = traces
            .into_iter()
            .map(|(trace_id, mut spans)| {
                spans.sort_unstable_by_key(|span| {
                    (span.start_time_unix_nanos, span.record_ref.offset)
                });
                summarize(trace_id, spans)
            })
            .collect::<Vec<_>>();
        results.sort_unstable_by_key(|trace| {
            (
                std::cmp::Reverse(trace.start_time_unix_nanos),
                trace.trace_id,
            )
        });
        results.truncate(result_limit);
        Ok(results)
    }

    /// Performs an indexed trace-ID lookup without parsing TraceQL.
    pub fn trace_by_id(&self, trace_id: TraceId) -> Result<Option<TraceqlTrace>, TraceqlError> {
        let spans = self
            .store
            .query_traces(&TraceQuery {
                tenant: Arc::clone(&self.tenant),
                trace_id: Some(trace_id),
                limit: self.limits.max_spans,
                ..TraceQuery::default()
            })
            .map_err(|error| TraceqlError::new(error.to_string()))?;
        Ok((!spans.is_empty()).then(|| summarize(trace_id, spans)))
    }
}

fn summarize(trace_id: TraceId, spans: Vec<DurableSpan>) -> TraceqlTrace {
    let start_time_unix_nanos = spans
        .iter()
        .map(|span| span.start_time_unix_nanos)
        .min()
        .unwrap_or(0);
    let end_time_unix_nanos = spans
        .iter()
        .filter_map(DurableSpan::end_time_unix_nanos)
        .max()
        .unwrap_or(start_time_unix_nanos);
    let root = spans.iter().find(|span| span.parent_span_id.is_none());
    let root_name = root.map(|span| Arc::clone(&span.name));
    let root_service_name = root
        .and_then(|span| attribute(&span.resource.attributes, "service.name"))
        .and_then(value_string)
        .map(Arc::from);
    let error_count = spans
        .iter()
        .filter(|span| span.status.as_ref().is_some_and(|status| status.code == 2))
        .count() as u32;
    TraceqlTrace {
        trace_id,
        spans,
        start_time_unix_nanos,
        end_time_unix_nanos,
        root_name,
        root_service_name,
        error_count,
    }
}

#[derive(Debug, Clone)]
enum TraceFilter {
    True,
    Condition(Condition),
    And(Vec<TraceFilter>),
    Or(Vec<TraceFilter>),
}

impl TraceFilter {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        let input = input.trim();
        if input.is_empty() || input == "{}" {
            return Ok(Self::True);
        }
        let body = input
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
            .ok_or_else(|| TraceqlError::new("TraceQL filter must be enclosed in braces"))?
            .trim();
        if body.is_empty() {
            return Ok(Self::True);
        }
        let or_parts = split_quoted(body, "||");
        if or_parts.len() > 1 {
            return or_parts
                .into_iter()
                .map(Self::parse_body)
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Or);
        }
        Self::parse_body(body)
    }

    fn parse_body(body: &str) -> Result<Self, TraceqlError> {
        let parts = split_quoted(body, "&&");
        if parts.len() > 1 {
            return parts
                .into_iter()
                .map(|part| Condition::parse(part).map(Self::Condition))
                .collect::<Result<Vec<_>, _>>()
                .map(Self::And);
        }
        Condition::parse(body).map(Self::Condition)
    }

    fn matches(&self, span: &DurableSpan) -> bool {
        match self {
            Self::True => true,
            Self::Condition(condition) => condition.matches(span),
            Self::And(filters) => filters.iter().all(|filter| filter.matches(span)),
            Self::Or(filters) => filters.iter().any(|filter| filter.matches(span)),
        }
    }

    fn exact_trace_id(&self) -> Option<TraceId> {
        match self {
            Self::Condition(condition) => condition.exact_trace_id(),
            Self::And(filters) => filters.iter().find_map(Self::exact_trace_id),
            Self::True | Self::Or(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
struct Condition {
    field: String,
    operation: Comparison,
    value: Literal,
}

impl Condition {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        for (token, operation) in [
            ("=~", Comparison::Regex),
            ("!~", Comparison::NotRegex),
            (">=", Comparison::GreaterOrEqual),
            ("<=", Comparison::LessOrEqual),
            ("!=", Comparison::NotEqual),
            ("=", Comparison::Equal),
            (">", Comparison::Greater),
            ("<", Comparison::Less),
        ] {
            if let Some(index) = find_unquoted(input, token) {
                let field = input[..index].trim().trim_start_matches('.').to_owned();
                if field.is_empty() {
                    return Err(TraceqlError::new("TraceQL condition has an empty field"));
                }
                let value = Literal::parse(input[index + token.len()..].trim())?;
                return Ok(Self {
                    field,
                    operation,
                    value,
                });
            }
        }
        Err(TraceqlError::new(format!(
            "invalid TraceQL condition {input:?}"
        )))
    }

    fn matches(&self, span: &DurableSpan) -> bool {
        let observed = field_value(span, &self.field);
        compare(observed.as_ref(), &self.value, self.operation)
    }

    fn exact_trace_id(&self) -> Option<TraceId> {
        (self.field == "trace:id" && self.operation == Comparison::Equal)
            .then(|| match &self.value {
                Literal::String(value) => parse_trace_id(value),
                _ => None,
            })
            .flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone)]
enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(u64),
}

impl Literal {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        if input.starts_with('"') {
            return serde_json::from_str::<String>(input)
                .map(Self::String)
                .map_err(|error| TraceqlError::new(error.to_string()));
        }
        if let Some(duration) = parse_duration_nanos(input) {
            return Ok(Self::Duration(duration));
        }
        if input == "true" || input == "false" {
            return Ok(Self::Boolean(input == "true"));
        }
        if let Ok(value) = input.parse::<i64>() {
            return Ok(Self::Integer(value));
        }
        if let Ok(value) = input.parse::<f64>() {
            return Ok(Self::Float(value));
        }
        Ok(Self::String(input.to_owned()))
    }
}

#[derive(Debug, Clone)]
enum ObservedValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(u64),
}

fn field_value(span: &DurableSpan, field: &str) -> Option<ObservedValue> {
    match field {
        "name" | "span:name" => Some(ObservedValue::String(span.name.to_string())),
        "duration" | "span:duration" => Some(ObservedValue::Duration(span.duration_nanos)),
        "kind" | "span:kind" => Some(ObservedValue::Integer(i64::from(span.kind))),
        "status" | "span:status" => Some(ObservedValue::Integer(i64::from(
            span.status.as_ref().map_or(0, |status| status.code),
        ))),
        "statusMessage" | "span:statusMessage" => Some(ObservedValue::String(
            span.status
                .as_ref()
                .map_or_else(String::new, |status| status.message.to_string()),
        )),
        "trace:id" => Some(ObservedValue::String(span.trace_id.to_string())),
        "span:id" => Some(ObservedValue::String(span.span_id.to_string())),
        "parent" | "span:parent" => Some(ObservedValue::String(
            span.parent_span_id
                .map_or_else(String::new, |id| id.to_string()),
        )),
        _ => field
            .strip_prefix("resource.")
            .and_then(|key| attribute(&span.resource.attributes, key))
            .or_else(|| {
                field
                    .strip_prefix("span.")
                    .and_then(|key| attribute(&span.attributes, key))
            })
            .or_else(|| attribute(&span.attributes, field))
            .and_then(observed_telemetry_value),
    }
}

fn attribute<'a>(attributes: &'a [TelemetryAttribute], key: &str) -> Option<&'a TelemetryValue> {
    attributes
        .iter()
        .rev()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| attribute.value.as_ref())
}

fn observed_telemetry_value(value: &TelemetryValue) -> Option<ObservedValue> {
    match value {
        TelemetryValue::String(value) => Some(ObservedValue::String(value.to_string())),
        TelemetryValue::Boolean(value) => Some(ObservedValue::Boolean(*value)),
        TelemetryValue::Integer(value) => Some(ObservedValue::Integer(*value)),
        TelemetryValue::DoubleBits(bits) => Some(ObservedValue::Float(f64::from_bits(*bits))),
        TelemetryValue::Empty
        | TelemetryValue::Bytes(_)
        | TelemetryValue::Array(_)
        | TelemetryValue::Map(_)
        | TelemetryValue::StringTableIndex(_) => None,
    }
}

fn value_string(value: &TelemetryValue) -> Option<&str> {
    match value {
        TelemetryValue::String(value) => Some(value),
        _ => None,
    }
}

fn compare(observed: Option<&ObservedValue>, expected: &Literal, operation: Comparison) -> bool {
    let Some(observed) = observed else {
        return operation == Comparison::NotEqual || operation == Comparison::NotRegex;
    };
    if matches!(operation, Comparison::Regex | Comparison::NotRegex) {
        let observed = observed_string(observed);
        let expected = literal_string(expected);
        let matched = Regex::new(&expected).is_ok_and(|regex| regex.is_match(&observed));
        return if operation == Comparison::Regex {
            matched
        } else {
            !matched
        };
    }
    match (numeric_observed(observed), numeric_literal(expected)) {
        (Some(left), Some(right)) => compare_order(left, right, operation),
        _ => {
            let equal = observed_string(observed) == literal_string(expected);
            match operation {
                Comparison::Equal => equal,
                Comparison::NotEqual => !equal,
                Comparison::Greater
                | Comparison::GreaterOrEqual
                | Comparison::Less
                | Comparison::LessOrEqual
                | Comparison::Regex
                | Comparison::NotRegex => false,
            }
        }
    }
}

fn compare_order(left: f64, right: f64, operation: Comparison) -> bool {
    match operation {
        Comparison::Equal => left == right,
        Comparison::NotEqual => left != right,
        Comparison::Greater => left > right,
        Comparison::GreaterOrEqual => left >= right,
        Comparison::Less => left < right,
        Comparison::LessOrEqual => left <= right,
        Comparison::Regex | Comparison::NotRegex => false,
    }
}

fn numeric_observed(value: &ObservedValue) -> Option<f64> {
    match value {
        ObservedValue::Integer(value) => Some(*value as f64),
        ObservedValue::Float(value) => Some(*value),
        ObservedValue::Duration(value) => Some(*value as f64),
        ObservedValue::String(_) | ObservedValue::Boolean(_) => None,
    }
}

fn numeric_literal(value: &Literal) -> Option<f64> {
    match value {
        Literal::Integer(value) => Some(*value as f64),
        Literal::Float(value) => Some(*value),
        Literal::Duration(value) => Some(*value as f64),
        Literal::String(_) | Literal::Boolean(_) => None,
    }
}

fn observed_string(value: &ObservedValue) -> String {
    match value {
        ObservedValue::String(value) => value.clone(),
        ObservedValue::Integer(value) => value.to_string(),
        ObservedValue::Float(value) => value.to_string(),
        ObservedValue::Boolean(value) => value.to_string(),
        ObservedValue::Duration(value) => value.to_string(),
    }
}

fn literal_string(value: &Literal) -> String {
    match value {
        Literal::String(value) => value.clone(),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => value.to_string(),
        Literal::Boolean(value) => value.to_string(),
        Literal::Duration(value) => value.to_string(),
    }
}

fn split_quoted<'a>(input: &'a str, delimiter: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    let bytes = input.as_bytes();
    let delimiter = delimiter.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if escaped {
            escaped = false;
        } else if byte == b'\\' && quoted {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && bytes[index..].starts_with(delimiter) {
            parts.push(input[start..index].trim());
            index += delimiter.len();
            start = index;
            continue;
        }
        index += 1;
    }
    parts.push(input[start..].trim());
    parts
}

fn find_unquoted(input: &str, needle: &str) -> Option<usize> {
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in input.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' && quoted {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && input[index..].starts_with(needle) {
            return Some(index);
        }
    }
    None
}

fn parse_duration_nanos(input: &str) -> Option<u64> {
    let (number, multiplier) = [
        ("ns", 1_f64),
        ("us", 1_000.0),
        ("ms", 1_000_000.0),
        ("s", 1_000_000_000.0),
        ("m", 60_000_000_000.0),
        ("h", 3_600_000_000_000.0),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        input
            .strip_suffix(suffix)
            .map(|number| (number, multiplier))
    })?;
    let value = number.parse::<f64>().ok()? * multiplier;
    (value.is_finite() && value >= 0.0 && value <= u64::MAX as f64).then_some(value as u64)
}

fn parse_trace_id(value: &str) -> Option<TraceId> {
    if value.len() != 32 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    TraceId::from_bytes(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_room_filter_parses_typed_conditions_and_boolean_groups() {
        let filter = TraceFilter::parse(
            r#"{ resource.service.name = "api" && duration >= 250ms || status = 2 }"#,
        )
        .expect("filter parses");
        assert!(matches!(filter, TraceFilter::Or(_)));
    }

    #[test]
    fn exact_trace_id_is_available_for_index_pushdown() {
        let filter = TraceFilter::parse(
            "{ trace:id = \"01010101010101010101010101010101\" && duration > 1ms }",
        )
        .unwrap();
        assert_eq!(filter.exact_trace_id(), TraceId::from_bytes([1; 16]).ok());
    }
}
