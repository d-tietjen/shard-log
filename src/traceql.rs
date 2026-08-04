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
    /// Fields requested by the final `select(...)` pipeline stage.
    pub selected_fields: Arc<Vec<String>>,
}

/// One trace-linked exemplar emitted by a TraceQL metrics query.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceqlMetricExemplar {
    /// Trace that contributed the sample.
    pub trace_id: TraceId,
    /// Span that contributed the sample.
    pub span_id: crate::SpanId,
    /// Bucket timestamp in Unix milliseconds.
    pub timestamp_ms: u64,
    /// Aggregate value for the bucket.
    pub value: f64,
}

/// One TraceQL metrics sample.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceqlMetricSample {
    /// Bucket timestamp in Unix milliseconds.
    pub timestamp_ms: u64,
    /// Aggregate value.
    pub value: f64,
}

/// One Prometheus-like time series derived directly from matching spans.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceqlMetricSeries {
    /// TraceQL grouping labels.
    pub labels: BTreeMap<String, String>,
    /// Samples ordered by time.
    pub samples: Vec<TraceqlMetricSample>,
    /// Bounded trace-linked exemplars.
    pub exemplars: Vec<TraceqlMetricExemplar>,
}

/// TraceQL parse or execution error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceqlError(String);

impl TraceqlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
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
        let query = TraceqlQuery::parse(expression)?;
        let pushed_trace_id = query.exact_trace_id();
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
        let selected_fields = query.selected_fields();
        let mut results = traces
            .into_iter()
            .filter_map(|(trace_id, mut spans)| {
                spans.sort_unstable_by_key(|span| {
                    (span.start_time_unix_nanos, span.record_ref.offset)
                });
                let selected = query.evaluate(&spans);
                (!selected.is_empty()).then(|| {
                    let spans = selected
                        .into_iter()
                        .map(|index| spans[index].clone())
                        .collect();
                    summarize(trace_id, spans, Arc::clone(&selected_fields))
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
        Ok((!spans.is_empty()).then(|| summarize(trace_id, spans, Arc::default())))
    }

    /// Evaluates a bounded TraceQL metrics expression over an inclusive time range.
    pub fn query_metrics(
        &self,
        expression: &str,
        start_time_unix_nanos: u64,
        end_time_unix_nanos: u64,
        step_nanos: u64,
        instant: bool,
        max_exemplars: usize,
    ) -> Result<Vec<TraceqlMetricSeries>, TraceqlError> {
        if start_time_unix_nanos > end_time_unix_nanos || step_nanos == 0 {
            return Err(TraceqlError::new("invalid TraceQL metrics time range"));
        }
        let metric = TraceMetricQuery::parse(expression)?;
        let spans = self
            .store
            .query_traces(&TraceQuery {
                tenant: Arc::clone(&self.tenant),
                trace_id: metric.spanset.exact_trace_id(),
                start_time_unix_nanos: Some(start_time_unix_nanos),
                end_time_unix_nanos: end_time_unix_nanos.checked_add(1),
                limit: self.limits.max_spans,
                ..TraceQuery::default()
            })
            .map_err(|error| TraceqlError::new(error.to_string()))?;
        let mut traces = BTreeMap::<TraceId, Vec<DurableSpan>>::new();
        for span in spans {
            traces.entry(span.trace_id).or_default().push(span);
        }
        let mut buckets = BTreeMap::<MetricGroup, BTreeMap<u64, MetricBucket>>::new();
        for (trace_id, mut spans) in traces {
            spans.sort_unstable_by_key(|span| (span.start_time_unix_nanos, span.record_ref.offset));
            for index in metric.spanset.evaluate(&spans) {
                let span = &spans[index];
                let Some(labels) = metric.labels(span, &spans) else {
                    continue;
                };
                let Some(value) = metric.observed_value(span, &spans) else {
                    continue;
                };
                let bucket_timestamp = if instant {
                    end_time_unix_nanos
                } else {
                    let ordinal = span
                        .start_time_unix_nanos
                        .saturating_sub(start_time_unix_nanos)
                        / step_nanos;
                    start_time_unix_nanos
                        .saturating_add(ordinal.saturating_add(1).saturating_mul(step_nanos))
                        .min(end_time_unix_nanos)
                };
                let bucket = buckets
                    .entry(MetricGroup(labels))
                    .or_default()
                    .entry(bucket_timestamp)
                    .or_default();
                bucket.values.push(value);
                if bucket.exemplar.is_none() && max_exemplars > 0 {
                    bucket.exemplar = Some((trace_id, span.span_id));
                }
            }
        }

        let denominator_seconds = if instant {
            end_time_unix_nanos
                .saturating_sub(start_time_unix_nanos)
                .max(1) as f64
                / 1_000_000_000.0
        } else {
            step_nanos as f64 / 1_000_000_000.0
        };
        let mut remaining_exemplars = max_exemplars;
        let mut series = Vec::with_capacity(buckets.len());
        for (MetricGroup(labels), samples) in buckets {
            let mut output_samples = Vec::with_capacity(samples.len());
            let mut exemplars = Vec::new();
            for (timestamp_nanos, bucket) in samples {
                let value = metric.aggregate(&bucket.values, denominator_seconds);
                if !metric.passes(value) {
                    continue;
                }
                let timestamp_ms = timestamp_nanos / 1_000_000;
                output_samples.push(TraceqlMetricSample {
                    timestamp_ms,
                    value,
                });
                if remaining_exemplars > 0
                    && let Some((trace_id, span_id)) = bucket.exemplar
                {
                    exemplars.push(TraceqlMetricExemplar {
                        trace_id,
                        span_id,
                        timestamp_ms,
                        value,
                    });
                    remaining_exemplars -= 1;
                }
            }
            if !output_samples.is_empty() {
                series.push(TraceqlMetricSeries {
                    labels,
                    samples: output_samples,
                    exemplars,
                });
            }
        }
        metric.limit_series(series)
    }
}

fn summarize(
    trace_id: TraceId,
    spans: Vec<DurableSpan>,
    selected_fields: Arc<Vec<String>>,
) -> TraceqlTrace {
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
        selected_fields,
    }
}

#[derive(Debug, Clone)]
struct TraceqlQuery {
    spanset: SpansetExpr,
    pipeline: Vec<PipelineStage>,
}

impl TraceqlQuery {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        let parts = split_traceql_pipeline(input);
        let Some((spanset, pipeline)) = parts.split_first() else {
            return Ok(Self {
                spanset: SpansetExpr::Selector(TraceFilter::True),
                pipeline: Vec::new(),
            });
        };
        Ok(Self {
            spanset: SpansetExpr::parse(spanset)?,
            pipeline: pipeline
                .iter()
                .map(|stage| PipelineStage::parse(stage))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    fn evaluate(&self, spans: &[DurableSpan]) -> Vec<usize> {
        let selected = self.spanset.evaluate(spans);
        if selected.is_empty() {
            return selected;
        }
        let mut groups = vec![selected];
        for stage in &self.pipeline {
            groups = stage.apply(groups, spans);
            if groups.is_empty() {
                break;
            }
        }
        sorted_unique(groups.into_iter().flatten().collect())
    }

    fn exact_trace_id(&self) -> Option<TraceId> {
        self.spanset.exact_trace_id()
    }

    fn selected_fields(&self) -> Arc<Vec<String>> {
        Arc::new(
            self.pipeline
                .iter()
                .rev()
                .find_map(|stage| match stage {
                    PipelineStage::Select(fields) => Some(fields.clone()),
                    PipelineStage::By(_) | PipelineStage::Aggregate { .. } => None,
                })
                .unwrap_or_default(),
        )
    }
}

#[derive(Debug, Clone)]
enum PipelineStage {
    By(String),
    Select(Vec<String>),
    Aggregate {
        operation: TraceAggregate,
        field: Option<String>,
        comparison: Comparison,
        expected: Literal,
    },
}

impl PipelineStage {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        let input = input.trim();
        if let Some(field) = input
            .strip_prefix("by(")
            .and_then(|value| value.strip_suffix(')'))
        {
            let field = field.trim().trim_start_matches('.');
            if field.is_empty() {
                return Err(TraceqlError::new("TraceQL by() has an empty field"));
            }
            return Ok(Self::By(field.to_owned()));
        }
        if let Some(fields) = input
            .strip_prefix("select(")
            .and_then(|value| value.strip_suffix(')'))
        {
            let fields = split_quoted(fields, ",")
                .into_iter()
                .map(|field| field.trim().trim_start_matches('.').to_owned())
                .filter(|field| !field.is_empty())
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Err(TraceqlError::new("TraceQL select() has no fields"));
            }
            return Ok(Self::Select(fields));
        }
        for (token, comparison) in comparison_tokens() {
            if let Some(index) = find_unquoted(input, token) {
                let aggregate = input[..index].trim();
                let open = aggregate
                    .find('(')
                    .ok_or_else(|| TraceqlError::new("TraceQL aggregate is missing '('"))?;
                let field = aggregate[open + 1..]
                    .strip_suffix(')')
                    .ok_or_else(|| TraceqlError::new("TraceQL aggregate is missing ')'"))?
                    .trim()
                    .trim_start_matches('.');
                let operation = TraceAggregate::parse(aggregate[..open].trim())?;
                if operation != TraceAggregate::Count && field.is_empty() {
                    return Err(TraceqlError::new(
                        "TraceQL numeric aggregate requires a field",
                    ));
                }
                if operation == TraceAggregate::Count && !field.is_empty() {
                    return Err(TraceqlError::new("TraceQL count() takes no field"));
                }
                return Ok(Self::Aggregate {
                    operation,
                    field: (!field.is_empty()).then(|| field.to_owned()),
                    comparison,
                    expected: Literal::parse(input[index + token.len()..].trim())?,
                });
            }
        }
        Err(TraceqlError::new(format!(
            "unsupported TraceQL pipeline stage {input:?}"
        )))
    }

    fn apply(&self, groups: Vec<Vec<usize>>, spans: &[DurableSpan]) -> Vec<Vec<usize>> {
        match self {
            Self::By(field) => groups
                .into_iter()
                .flat_map(|group| group_by_field(group, spans, field))
                .collect(),
            Self::Select(fields) => {
                let _ = fields;
                groups
            }
            Self::Aggregate {
                operation,
                field,
                comparison,
                expected,
            } => groups
                .into_iter()
                .filter(|group| {
                    aggregate_group(*operation, field.as_deref(), group, spans).is_some_and(
                        |value| compare(&[ObservedValue::Float(value)], expected, *comparison),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceAggregate {
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
}

impl TraceAggregate {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        match input {
            "count" => Ok(Self::Count),
            "sum" => Ok(Self::Sum),
            "avg" => Ok(Self::Average),
            "min" => Ok(Self::Minimum),
            "max" => Ok(Self::Maximum),
            _ => Err(TraceqlError::new(format!(
                "unsupported TraceQL aggregate {input:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
struct TraceMetricQuery {
    spanset: TraceqlQuery,
    function: TraceMetricFunction,
    field: Option<String>,
    quantile: Option<f64>,
    group_by: Vec<String>,
    threshold: Option<(Comparison, Literal)>,
    series_limit: Option<(bool, usize)>,
}

impl TraceMetricQuery {
    fn parse(input: &str) -> Result<Self, TraceqlError> {
        let parts = split_traceql_pipeline(input);
        let metric_index = parts
            .iter()
            .position(|part| TraceMetricFunction::recognizes(part))
            .ok_or_else(|| TraceqlError::new("TraceQL metrics query has no metrics function"))?;
        if metric_index == 0 {
            return Err(TraceqlError::new(
                "TraceQL metrics query requires a spanset before the function",
            ));
        }
        let spanset = TraceqlQuery::parse(&parts[..metric_index].join(" | "))?;
        let (function, field, quantile, group_by, threshold) =
            parse_trace_metric_stage(parts[metric_index])?;
        let mut series_limit = None;
        for stage in &parts[metric_index + 1..] {
            let stage = stage.trim();
            let (descending, value) = if let Some(value) = stage
                .strip_prefix("topk(")
                .and_then(|value| value.strip_suffix(')'))
            {
                (true, value)
            } else if let Some(value) = stage
                .strip_prefix("bottomk(")
                .and_then(|value| value.strip_suffix(')'))
            {
                (false, value)
            } else {
                return Err(TraceqlError::new(format!(
                    "unsupported TraceQL metrics pipeline stage {stage:?}"
                )));
            };
            let limit = value
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|limit| *limit > 0)
                .ok_or_else(|| TraceqlError::new("TraceQL topk/bottomk requires positive k"))?;
            if series_limit.replace((descending, limit)).is_some() {
                return Err(TraceqlError::new(
                    "TraceQL metrics query has multiple series limits",
                ));
            }
        }
        Ok(Self {
            spanset,
            function,
            field,
            quantile,
            group_by,
            threshold,
            series_limit,
        })
    }

    fn labels(
        &self,
        span: &DurableSpan,
        trace: &[DurableSpan],
    ) -> Option<BTreeMap<String, String>> {
        let mut labels = BTreeMap::new();
        for field in &self.group_by {
            let value = field_values(span, trace, field).into_iter().next()?;
            labels.insert(field.clone(), observed_string(&value));
        }
        Some(labels)
    }

    fn observed_value(&self, span: &DurableSpan, trace: &[DurableSpan]) -> Option<f64> {
        match self.function {
            TraceMetricFunction::Rate | TraceMetricFunction::Count => Some(1.0),
            TraceMetricFunction::Sum
            | TraceMetricFunction::Minimum
            | TraceMetricFunction::Maximum
            | TraceMetricFunction::Average
            | TraceMetricFunction::Quantile => self
                .field
                .as_deref()
                .and_then(|field| field_values(span, trace, field).into_iter().next())
                .and_then(|value| numeric_observed(&value)),
        }
    }

    fn aggregate(&self, values: &[f64], denominator_seconds: f64) -> f64 {
        match self.function {
            TraceMetricFunction::Rate => values.len() as f64 / denominator_seconds,
            TraceMetricFunction::Count => values.len() as f64,
            TraceMetricFunction::Sum => values.iter().sum(),
            TraceMetricFunction::Minimum => {
                values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN)
            }
            TraceMetricFunction::Maximum => {
                values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN)
            }
            TraceMetricFunction::Average => values.iter().sum::<f64>() / values.len().max(1) as f64,
            TraceMetricFunction::Quantile => trace_quantile(values, self.quantile.unwrap_or(0.5)),
        }
    }

    fn passes(&self, value: f64) -> bool {
        self.threshold
            .as_ref()
            .is_none_or(|(comparison, expected)| {
                compare(&[ObservedValue::Float(value)], expected, *comparison)
            })
    }

    fn limit_series(
        &self,
        mut series: Vec<TraceqlMetricSeries>,
    ) -> Result<Vec<TraceqlMetricSeries>, TraceqlError> {
        if let Some((descending, limit)) = self.series_limit {
            series.sort_by(|left, right| {
                let left = left.samples.last().map_or(f64::NAN, |sample| sample.value);
                let right = right.samples.last().map_or(f64::NAN, |sample| sample.value);
                let order = left.total_cmp(&right);
                if descending { order.reverse() } else { order }
            });
            series.truncate(limit);
        }
        Ok(series)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceMetricFunction {
    Rate,
    Count,
    Sum,
    Minimum,
    Maximum,
    Average,
    Quantile,
}

impl TraceMetricFunction {
    fn recognizes(input: &str) -> bool {
        let input = input.trim();
        [
            "rate(",
            "count_over_time(",
            "sum_over_time(",
            "min_over_time(",
            "max_over_time(",
            "avg_over_time(",
            "quantile_over_time(",
        ]
        .iter()
        .any(|prefix| input.starts_with(prefix))
    }

    fn parse(input: &str) -> Result<Self, TraceqlError> {
        match input {
            "rate" => Ok(Self::Rate),
            "count_over_time" => Ok(Self::Count),
            "sum_over_time" => Ok(Self::Sum),
            "min_over_time" => Ok(Self::Minimum),
            "max_over_time" => Ok(Self::Maximum),
            "avg_over_time" => Ok(Self::Average),
            "quantile_over_time" => Ok(Self::Quantile),
            _ => Err(TraceqlError::new(format!(
                "unsupported TraceQL metrics function {input:?}"
            ))),
        }
    }
}

type ParsedTraceMetricStage = (
    TraceMetricFunction,
    Option<String>,
    Option<f64>,
    Vec<String>,
    Option<(Comparison, Literal)>,
);

fn parse_trace_metric_stage(input: &str) -> Result<ParsedTraceMetricStage, TraceqlError> {
    let input = input.split(" with (").next().unwrap_or(input).trim();
    let open = input
        .find('(')
        .ok_or_else(|| TraceqlError::new("TraceQL metrics function is missing '('"))?;
    let close = find_closing_parenthesis_at(input, open)
        .ok_or_else(|| TraceqlError::new("TraceQL metrics function is missing ')'"))?;
    let function = TraceMetricFunction::parse(input[..open].trim())?;
    let arguments = split_quoted(&input[open + 1..close], ",");
    let (field, quantile) = match function {
        TraceMetricFunction::Rate | TraceMetricFunction::Count => {
            if arguments.len() != 1 || !arguments[0].is_empty() {
                return Err(TraceqlError::new(
                    "TraceQL rate/count_over_time takes no field",
                ));
            }
            (None, None)
        }
        TraceMetricFunction::Sum
        | TraceMetricFunction::Minimum
        | TraceMetricFunction::Maximum
        | TraceMetricFunction::Average => {
            if arguments.len() != 1 || arguments[0].is_empty() {
                return Err(TraceqlError::new(
                    "TraceQL metrics function requires one field",
                ));
            }
            (Some(arguments[0].trim_start_matches('.').to_owned()), None)
        }
        TraceMetricFunction::Quantile => {
            if arguments.len() != 2 || arguments[0].is_empty() {
                return Err(TraceqlError::new(
                    "TraceQL quantile_over_time requires field and quantile",
                ));
            }
            let quantile = arguments[1]
                .parse::<f64>()
                .ok()
                .filter(|value| (0.0..=1.0).contains(value))
                .ok_or_else(|| {
                    TraceqlError::new("TraceQL quantile must be between zero and one")
                })?;
            (
                Some(arguments[0].trim_start_matches('.').to_owned()),
                Some(quantile),
            )
        }
    };

    let mut suffix = input[close + 1..].trim();
    let mut group_by = Vec::new();
    if let Some(group) = suffix.strip_prefix("by") {
        let group = group.trim_start();
        let group_open = group
            .strip_prefix('(')
            .ok_or_else(|| TraceqlError::new("TraceQL metrics by is missing '('"))?;
        let group_close = group_open
            .find(')')
            .ok_or_else(|| TraceqlError::new("TraceQL metrics by is missing ')'"))?;
        group_by = split_quoted(&group_open[..group_close], ",")
            .into_iter()
            .map(|field| field.trim().trim_start_matches('.').to_owned())
            .filter(|field| !field.is_empty())
            .collect();
        suffix = group_open[group_close + 1..].trim();
    }
    let threshold = if suffix.is_empty() {
        None
    } else {
        comparison_tokens()
            .into_iter()
            .find_map(|(token, comparison)| {
                suffix.strip_prefix(token).map(|expected| {
                    Literal::parse(expected.trim()).map(|expected| (comparison, expected))
                })
            })
            .transpose()?
            .ok_or_else(|| TraceqlError::new("invalid TraceQL metrics comparison"))?
            .into()
    };
    Ok((function, field, quantile, group_by, threshold))
}

fn find_closing_parenthesis_at(input: &str, open: usize) -> Option<usize> {
    let mut depth = 0_u32;
    for (index, character) in input.char_indices().skip_while(|(index, _)| *index < open) {
        match character {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn trace_quantile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut values = values.to_vec();
    values.sort_by(|left, right| left.total_cmp(right));
    if values.len() == 1 {
        return values[0];
    }
    let rank = quantile * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    values[lower] + (values[upper] - values[lower]) * (rank - lower as f64)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricGroup(BTreeMap<String, String>);

#[derive(Debug, Default)]
struct MetricBucket {
    values: Vec<f64>,
    exemplar: Option<(TraceId, crate::SpanId)>,
}

fn group_by_field(group: Vec<usize>, spans: &[DurableSpan], field: &str) -> Vec<Vec<usize>> {
    let mut grouped = BTreeMap::<String, Vec<usize>>::new();
    for index in group {
        let values = field_values(&spans[index], spans, field);
        for value in values {
            grouped
                .entry(observed_string(&value))
                .or_default()
                .push(index);
        }
    }
    grouped
        .into_values()
        .map(sorted_unique)
        .filter(|group| !group.is_empty())
        .collect()
}

fn aggregate_group(
    operation: TraceAggregate,
    field: Option<&str>,
    group: &[usize],
    spans: &[DurableSpan],
) -> Option<f64> {
    if operation == TraceAggregate::Count {
        return Some(group.len() as f64);
    }
    let field = field?;
    let values = group
        .iter()
        .flat_map(|index| field_values(&spans[*index], spans, field))
        .filter_map(|value| numeric_observed(&value))
        .collect::<Vec<_>>();
    match operation {
        TraceAggregate::Count => unreachable!("count returned before field collection"),
        TraceAggregate::Sum => Some(values.iter().sum()),
        TraceAggregate::Average => {
            (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
        }
        TraceAggregate::Minimum => values.into_iter().reduce(f64::min),
        TraceAggregate::Maximum => values.into_iter().reduce(f64::max),
    }
}

fn split_traceql_pipeline(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    let mut braces = 0_u32;
    let mut parentheses = 0_u32;
    let bytes = input.as_bytes();
    for (index, byte) in bytes.iter().copied().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted {
            match byte {
                b'{' => braces = braces.saturating_add(1),
                b'}' => braces = braces.saturating_sub(1),
                b'(' => parentheses = parentheses.saturating_add(1),
                b')' => parentheses = parentheses.saturating_sub(1),
                b'|' if braces == 0
                    && parentheses == 0
                    && bytes.get(index.wrapping_sub(1)) != Some(&b'|')
                    && bytes.get(index + 1) != Some(&b'|') =>
                {
                    parts.push(input[start..index].trim());
                    start = index + 1;
                }
                _ => {}
            }
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn comparison_tokens() -> [(&'static str, Comparison); 8] {
    [
        ("=~", Comparison::Regex),
        ("!~", Comparison::NotRegex),
        (">=", Comparison::GreaterOrEqual),
        ("<=", Comparison::LessOrEqual),
        ("!=", Comparison::NotEqual),
        ("=", Comparison::Equal),
        (">", Comparison::Greater),
        ("<", Comparison::Less),
    ]
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

    #[test]
    fn pipeline_groups_and_filters_spansets_with_typed_aggregates() {
        let spans = vec![
            span(1, None, "root"),
            span(2, Some(1), "worker"),
            span(3, Some(1), "worker"),
        ];
        assert_eq!(
            TraceqlQuery::parse("{} | count() >= 3")
                .unwrap()
                .evaluate(&spans),
            vec![0, 1, 2]
        );
        assert!(
            TraceqlQuery::parse("{} | count() > 3")
                .unwrap()
                .evaluate(&spans)
                .is_empty()
        );
        let selected =
            TraceqlQuery::parse("{} | by(name) | count() >= 2 | select(name, duration)").unwrap();
        assert_eq!(selected.evaluate(&spans), vec![1, 2]);
        assert_eq!(selected.selected_fields().as_ref(), &["name", "duration"]);
        assert_eq!(
            TraceqlQuery::parse("{} | sum(duration) >= 3ns")
                .unwrap()
                .evaluate(&spans),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn metrics_parser_accepts_grouping_threshold_quantile_and_series_limits() {
        let rate = TraceMetricQuery::parse("{} | rate() by (resource.service.name) > 1 | topk(10)")
            .unwrap();
        assert_eq!(rate.function, TraceMetricFunction::Rate);
        assert_eq!(rate.group_by, vec!["resource.service.name"]);
        assert_eq!(rate.series_limit, Some((true, 10)));
        assert!(rate.passes(2.0));
        assert!(!rate.passes(1.0));

        let quantile =
            TraceMetricQuery::parse("{ status = 2 } | quantile_over_time(duration, .99)").unwrap();
        assert_eq!(quantile.function, TraceMetricFunction::Quantile);
        assert_eq!(quantile.field.as_deref(), Some("duration"));
        assert_eq!(quantile.quantile, Some(0.99));
        assert_eq!(quantile.aggregate(&[1.0, 2.0, 3.0], 1.0), 2.98);
    }
}
