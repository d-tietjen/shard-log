use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use regex::Regex;

use crate::{
    DurableSpan, DurableTelemetryStore, TelemetryAttribute, TelemetryValue, TraceId, TraceQuery,
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
    store: Arc<DurableTelemetryStore>,
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
    pub fn new(store: Arc<DurableTelemetryStore>, tenant: Arc<str>, limits: TraceqlLimits) -> Self {
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
        let expression = SpansetExpr::parse(expression)?;
        let pushed_trace_id = expression.exact_trace_id();
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
            traces.entry(span.trace_id).or_default().push(span);
        }
        let result_limit = limit.max(1).min(self.limits.max_traces);
        let mut results = traces
            .into_iter()
            .filter_map(|(trace_id, mut spans)| {
                spans.sort_unstable_by_key(|span| {
                    (span.start_time_unix_nanos, span.record_ref.offset)
                });
                let selected = expression.evaluate(&spans);
                (!selected.is_empty()).then(|| {
                    let spans = selected
                        .into_iter()
                        .map(|index| spans[index].clone())
                        .collect();
                    summarize(trace_id, spans)
                })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StructuralRelation {
    Descendant,
    Ancestor,
    Child,
    Parent,
    Sibling,
}

#[derive(Debug, Clone)]
enum SpansetExpr {
    Selector(TraceFilter),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Structural {
        left: Box<Self>,
        right: Box<Self>,
        relation: StructuralRelation,
        union: bool,
        negated: bool,
    },
}

impl SpansetExpr {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        let input = trim_enclosing_parentheses(input.trim());
        if input.is_empty() {
            return Ok(Self::Selector(TraceFilter::True));
        }
        if let Some((index, token)) = find_top_level_operator(input, &["||"]) {
            return Ok(Self::Or(
                Box::new(Self::parse(&input[..index])?),
                Box::new(Self::parse(&input[index + token.len()..])?),
            ));
        }
        if let Some((index, token)) = find_top_level_operator(input, &["&&"]) {
            return Ok(Self::And(
                Box::new(Self::parse(&input[..index])?),
                Box::new(Self::parse(&input[index + token.len()..])?),
            ));
        }
        if let Some((index, token)) = find_top_level_operator(
            input,
            &[
                "!>>", "!<<", "&>>", "&<<", "!>", "!<", "!~", "&>", "&<", "&~", ">>", "<<", ">",
                "<", "~",
            ],
        ) {
            let (relation, union, negated) = match token {
                ">>" => (StructuralRelation::Descendant, false, false),
                "<<" => (StructuralRelation::Ancestor, false, false),
                ">" => (StructuralRelation::Child, false, false),
                "<" => (StructuralRelation::Parent, false, false),
                "~" => (StructuralRelation::Sibling, false, false),
                "&>>" => (StructuralRelation::Descendant, true, false),
                "&<<" => (StructuralRelation::Ancestor, true, false),
                "&>" => (StructuralRelation::Child, true, false),
                "&<" => (StructuralRelation::Parent, true, false),
                "&~" => (StructuralRelation::Sibling, true, false),
                "!>>" => (StructuralRelation::Descendant, false, true),
                "!<<" => (StructuralRelation::Ancestor, false, true),
                "!>" => (StructuralRelation::Child, false, true),
                "!<" => (StructuralRelation::Parent, false, true),
                "!~" => (StructuralRelation::Sibling, false, true),
                _ => unreachable!("operator table is exhaustive"),
            };
            return Ok(Self::Structural {
                left: Box::new(Self::parse(&input[..index])?),
                right: Box::new(Self::parse(&input[index + token.len()..])?),
                relation,
                union,
                negated,
            });
        }
        Ok(Self::Selector(TraceFilter::parse(input)?))
    }

    fn evaluate(&self, spans: &[DurableSpan]) -> Vec<usize> {
        match self {
            Self::Selector(filter) => spans
                .iter()
                .enumerate()
                .filter_map(|(index, span)| filter.matches(span, spans).then_some(index))
                .collect(),
            Self::And(left, right) => {
                let left = left.evaluate(spans);
                let right = right.evaluate(spans);
                if left.is_empty() || right.is_empty() {
                    Vec::new()
                } else {
                    ordered_union(left, right)
                }
            }
            Self::Or(left, right) => ordered_union(left.evaluate(spans), right.evaluate(spans)),
            Self::Structural {
                left,
                right,
                relation,
                union,
                negated,
            } => {
                let left = left.evaluate(spans);
                let right = right.evaluate(spans);
                let mut matching_left = Vec::new();
                let mut matching_right = Vec::new();
                for right_index in right {
                    let related = left.iter().copied().filter(|left_index| {
                        spans_related(spans, *left_index, right_index, *relation)
                    });
                    let related = related.collect::<Vec<_>>();
                    if (*negated && related.is_empty()) || (!*negated && !related.is_empty()) {
                        matching_right.push(right_index);
                        if *union && !*negated {
                            matching_left.extend(related);
                        }
                    }
                }
                if *union {
                    ordered_union(matching_left, matching_right)
                } else {
                    sorted_unique(matching_right)
                }
            }
        }
    }

    fn exact_trace_id(&self) -> Option<TraceId> {
        match self {
            Self::Selector(filter) => filter.exact_trace_id(),
            Self::And(left, right) | Self::Structural { left, right, .. } => {
                left.exact_trace_id().or_else(|| right.exact_trace_id())
            }
            Self::Or(_, _) => None,
        }
    }
}

fn sorted_unique(mut indexes: Vec<usize>) -> Vec<usize> {
    indexes.sort_unstable();
    indexes.dedup();
    indexes
}

fn ordered_union(mut left: Vec<usize>, right: Vec<usize>) -> Vec<usize> {
    left.extend(right);
    sorted_unique(left)
}

fn spans_related(
    spans: &[DurableSpan],
    left_index: usize,
    right_index: usize,
    relation: StructuralRelation,
) -> bool {
    if left_index == right_index {
        return false;
    }
    let left = &spans[left_index];
    let right = &spans[right_index];
    match relation {
        StructuralRelation::Descendant => is_ancestor(spans, left.span_id, right_index, false),
        StructuralRelation::Ancestor => is_ancestor(spans, right.span_id, left_index, false),
        StructuralRelation::Child => right.parent_span_id == Some(left.span_id),
        StructuralRelation::Parent => left.parent_span_id == Some(right.span_id),
        StructuralRelation::Sibling => {
            left.parent_span_id.is_some() && left.parent_span_id == right.parent_span_id
        }
    }
}

fn is_ancestor(
    spans: &[DurableSpan],
    ancestor: crate::SpanId,
    descendant_index: usize,
    include_self: bool,
) -> bool {
    let mut current = if include_self {
        Some(spans[descendant_index].span_id)
    } else {
        spans[descendant_index].parent_span_id
    };
    for _ in 0..spans.len() {
        let Some(span_id) = current else {
            return false;
        };
        if span_id == ancestor {
            return true;
        }
        current = spans
            .iter()
            .find(|span| span.span_id == span_id)
            .and_then(|span| span.parent_span_id);
    }
    false
}

fn find_top_level_operator<'a>(input: &'a str, operators: &[&'a str]) -> Option<(usize, &'a str)> {
    let mut quoted = false;
    let mut escaped = false;
    let mut braces = 0_u32;
    let mut parentheses = 0_u32;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
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
            '{' => braces = braces.saturating_add(1),
            '}' => braces = braces.saturating_sub(1),
            '(' => parentheses = parentheses.saturating_add(1),
            ')' => parentheses = parentheses.saturating_sub(1),
            _ if braces == 0 && parentheses == 0 => {
                if let Some(operator) = operators
                    .iter()
                    .copied()
                    .find(|operator| input[index..].starts_with(operator))
                {
                    return Some((index, operator));
                }
            }
            _ => {}
        }
    }
    None
}

fn trim_enclosing_parentheses(mut input: &str) -> &str {
    loop {
        let Some(inner) = input
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
        else {
            return input;
        };
        if find_matching_parenthesis(input) != Some(input.len() - 1) {
            return input;
        }
        input = inner.trim();
    }
}

fn find_matching_parenthesis(input: &str) -> Option<usize> {
    let mut depth = 0_u32;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
        } else if !quoted && character == '(' {
            depth = depth.saturating_add(1);
        } else if !quoted && character == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
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

    fn matches(&self, span: &DurableSpan, trace: &[DurableSpan]) -> bool {
        match self {
            Self::True => true,
            Self::Condition(condition) => condition.matches(span, trace),
            Self::And(filters) => filters.iter().all(|filter| filter.matches(span, trace)),
            Self::Or(filters) => filters.iter().any(|filter| filter.matches(span, trace)),
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

    fn matches(&self, span: &DurableSpan, trace: &[DurableSpan]) -> bool {
        let observed = field_values(span, trace, &self.field);
        compare(&observed, &self.value, self.operation)
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
    Nil,
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(u64),
}

impl Literal {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        if input == "nil" {
            return Ok(Self::Nil);
        }
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

fn field_values(span: &DurableSpan, trace: &[DurableSpan], field: &str) -> Vec<ObservedValue> {
    let singleton = match field {
        "name" | "span:name" => Some(ObservedValue::String(span.name.to_string())),
        "duration" | "span:duration" => Some(ObservedValue::Duration(span.duration_nanos)),
        "traceDuration" | "trace:duration" => trace_duration(trace).map(ObservedValue::Duration),
        "rootName" | "trace:rootName" => trace
            .iter()
            .find(|candidate| candidate.parent_span_id.is_none())
            .map(|root| ObservedValue::String(root.name.to_string())),
        "rootServiceName" | "trace:rootServiceName" => trace
            .iter()
            .find(|candidate| candidate.parent_span_id.is_none())
            .and_then(|root| attribute(&root.resource.attributes, "service.name"))
            .and_then(observed_telemetry_value),
        "kind" | "span:kind" => Some(ObservedValue::Integer(i64::from(span.kind))),
        "status" | "span:status" => Some(ObservedValue::Integer(i64::from(
            span.status.as_ref().map_or(0, |status| status.code),
        ))),
        "statusMessage" | "span:statusMessage" => span
            .status
            .as_ref()
            .map(|status| ObservedValue::String(status.message.to_string())),
        "trace:id" => Some(ObservedValue::String(span.trace_id.to_string())),
        "span:id" => Some(ObservedValue::String(span.span_id.to_string())),
        "parent" | "span:parent" => span
            .parent_span_id
            .map(|id| ObservedValue::String(id.to_string())),
        "instrumentation:name" | "scope:name" => {
            Some(ObservedValue::String(span.scope.name.to_string()))
        }
        "instrumentation:version" | "scope:version" => {
            Some(ObservedValue::String(span.scope.version.to_string()))
        }
        _ => None,
    };
    if let Some(value) = singleton {
        return vec![value];
    }

    let mut observed = Vec::new();
    match field {
        "event:name" => observed.extend(
            span.events
                .iter()
                .map(|event| ObservedValue::String(event.name.to_string())),
        ),
        "link:traceID" | "link:traceId" => observed.extend(
            span.links
                .iter()
                .map(|link| ObservedValue::String(link.trace_id.to_string())),
        ),
        "link:spanID" | "link:spanId" => observed.extend(
            span.links
                .iter()
                .map(|link| ObservedValue::String(link.span_id.to_string())),
        ),
        _ => {
            let value = field
                .strip_prefix("resource.")
                .and_then(|key| attribute(&span.resource.attributes, key))
                .or_else(|| {
                    field
                        .strip_prefix("span.")
                        .and_then(|key| attribute(&span.attributes, key))
                })
                .or_else(|| {
                    field
                        .strip_prefix("instrumentation.")
                        .and_then(|key| attribute(&span.scope.attributes, key))
                })
                .or_else(|| attribute(&span.attributes, field));
            if let Some(value) = value {
                append_observed_values(value, &mut observed);
            } else if let Some(key) = field.strip_prefix("event.") {
                for event in span.events.iter() {
                    if let Some(value) = attribute(&event.attributes, key) {
                        append_observed_values(value, &mut observed);
                    }
                }
            } else if let Some(key) = field.strip_prefix("link.") {
                for link in span.links.iter() {
                    if let Some(value) = attribute(&link.attributes, key) {
                        append_observed_values(value, &mut observed);
                    }
                }
            }
        }
    }
    observed
}

fn trace_duration(trace: &[DurableSpan]) -> Option<u64> {
    let start = trace.iter().map(|span| span.start_time_unix_nanos).min()?;
    let end = trace
        .iter()
        .filter_map(DurableSpan::end_time_unix_nanos)
        .max()?;
    end.checked_sub(start)
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
        TelemetryValue::StringTableIndex(value) => Some(ObservedValue::Integer(i64::from(*value))),
        TelemetryValue::Empty
        | TelemetryValue::Bytes(_)
        | TelemetryValue::Array(_)
        | TelemetryValue::Map(_) => None,
    }
}

fn append_observed_values(value: &TelemetryValue, output: &mut Vec<ObservedValue>) {
    match value {
        TelemetryValue::Array(values) => {
            for value in values.iter() {
                append_observed_values(value, output);
            }
        }
        _ => output.extend(observed_telemetry_value(value)),
    }
}

fn value_string(value: &TelemetryValue) -> Option<&str> {
    match value {
        TelemetryValue::String(value) => Some(value),
        _ => None,
    }
}

fn compare(observed: &[ObservedValue], expected: &Literal, operation: Comparison) -> bool {
    if matches!(expected, Literal::Nil) {
        return match operation {
            Comparison::Equal => observed.is_empty(),
            Comparison::NotEqual => !observed.is_empty(),
            _ => false,
        };
    }
    if observed.is_empty() {
        return false;
    }
    match operation {
        Comparison::NotEqual => observed
            .iter()
            .all(|value| !compare_one(value, expected, Comparison::Equal)),
        Comparison::NotRegex => observed
            .iter()
            .all(|value| !compare_one(value, expected, Comparison::Regex)),
        _ => observed
            .iter()
            .any(|value| compare_one(value, expected, operation)),
    }
}

fn compare_one(observed: &ObservedValue, expected: &Literal, operation: Comparison) -> bool {
    if operation == Comparison::Regex {
        let observed = observed_string(observed);
        let expected = literal_string(expected);
        return Regex::new(&format!("^(?:{expected})$"))
            .is_ok_and(|regex| regex.is_match(&observed));
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
        Literal::Nil => None,
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
        Literal::Nil => "nil".to_owned(),
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
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use crate::{ResourceContext, ScopeContext, SpanId, TelemetryRecordRef, TelemetrySignal};

    fn span(index: u8, parent: Option<u8>, name: &str) -> DurableSpan {
        DurableSpan {
            stream_shard_id: ShardId::new(0),
            record_ref: TelemetryRecordRef::for_signal(
                TelemetrySignal::Traces,
                TopicPartition::new(TopicId::new(2), LogicalPartitionId::new(0)),
                LogicalOffset::new(u64::from(index)),
            ),
            tenant: Arc::from("tenant"),
            resource: Arc::new(ResourceContext::default()),
            scope: Arc::new(ScopeContext::default()),
            trace_id: TraceId::from_bytes([1; 16]).unwrap(),
            span_id: SpanId::from_bytes([index; 8]).unwrap(),
            parent_span_id: parent.map(|parent| SpanId::from_bytes([parent; 8]).unwrap()),
            trace_state: Arc::from(""),
            flags: 0,
            name: Arc::from(name),
            kind: 0,
            start_time_unix_nanos: u64::from(index),
            duration_nanos: 1,
            attributes: Arc::default(),
            dropped_attributes_count: 0,
            events: Arc::default(),
            dropped_events_count: 0,
            links: Arc::default(),
            dropped_links_count: 0,
            status: None,
        }
    }

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

    #[test]
    fn structural_operators_select_related_spans() {
        let spans = vec![
            span(1, None, "root"),
            span(2, Some(1), "child"),
            span(3, Some(2), "grandchild"),
            span(4, Some(1), "sibling"),
            span(5, None, "orphan"),
        ];
        let descendants = SpansetExpr::parse(r#"{ name = "root" } >> { name = "grandchild" }"#)
            .unwrap()
            .evaluate(&spans);
        assert_eq!(descendants, vec![2]);

        let siblings = SpansetExpr::parse(r#"{ name = "child" } ~ { name = "sibling" }"#)
            .unwrap()
            .evaluate(&spans);
        assert_eq!(siblings, vec![3]);

        let negative = SpansetExpr::parse(r#"{ name = "root" } !>> { name = "orphan" }"#)
            .unwrap()
            .evaluate(&spans);
        assert_eq!(negative, vec![4]);
    }

    #[test]
    fn union_structural_operator_returns_both_sides() {
        let spans = vec![span(1, None, "root"), span(2, Some(1), "child")];
        let selected = SpansetExpr::parse(r#"{ name = "root" } &> { name = "child" }"#)
            .unwrap()
            .evaluate(&spans);
        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn arrays_nil_and_regex_follow_traceql_match_semantics() {
        let mut record = span(1, None, "checkout-handler");
        record.attributes = Arc::new(vec![TelemetryAttribute::new(
            "roles",
            TelemetryValue::Array(Arc::new(vec![
                TelemetryValue::String(Arc::from("reader")),
                TelemetryValue::String(Arc::from("writer")),
            ])),
        )]);
        let spans = vec![record];

        assert_eq!(
            SpansetExpr::parse(r#"{ span.roles = "writer" }"#)
                .unwrap()
                .evaluate(&spans),
            vec![0]
        );
        assert!(
            SpansetExpr::parse(r#"{ span.roles != "reader" }"#)
                .unwrap()
                .evaluate(&spans)
                .is_empty()
        );
        assert_eq!(
            SpansetExpr::parse(r#"{ span.missing = nil }"#)
                .unwrap()
                .evaluate(&spans),
            vec![0]
        );
        assert!(
            SpansetExpr::parse(r#"{ name =~ "checkout" }"#)
                .unwrap()
                .evaluate(&spans)
                .is_empty(),
            "TraceQL regexes are anchored"
        );
    }
}
