use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinaryExpr, Call, Expr, LabelModifier, MatrixSelector, Offset,
    SubqueryExpr, VectorMatchCardinality, VectorSelector, parse,
};

use crate::{
    DurableMetricPoint, DurableTelemetryStore, MetricQuery, MetricValue, NumberValue,
    prometheus_string_labels,
};

const DEFAULT_LOOKBACK: Duration = Duration::from_secs(5 * 60);
const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Limits for one native PromQL evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlLimits {
    /// Maximum raw points materialized from storage.
    pub max_points: usize,
    /// Maximum output series.
    pub max_series: usize,
    /// Maximum range-query steps.
    pub max_steps: usize,
    /// Selector lookback used when a query does not specify a range.
    pub lookback: Duration,
}

impl Default for PromqlLimits {
    fn default() -> Self {
        Self {
            max_points: 1_000_000,
            max_series: 100_000,
            max_steps: 11_000,
            lookback: DEFAULT_LOOKBACK,
        }
    }
}

/// One Prometheus float sample with its complete label set.
#[derive(Debug, Clone, PartialEq)]
pub struct PromqlSample {
    /// Prometheus-visible labels, including `__name__` when retained.
    pub labels: BTreeMap<String, String>,
    /// Evaluation timestamp in milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Floating-point sample value.
    pub value: f64,
}

/// One matrix series returned by a range query.
#[derive(Debug, Clone, PartialEq)]
pub struct PromqlSeries {
    /// Prometheus-visible labels.
    pub labels: BTreeMap<String, String>,
    /// Timestamp/value pairs in evaluation order.
    pub samples: Vec<(i64, f64)>,
}

/// Native PromQL result value.
#[derive(Debug, Clone, PartialEq)]
pub enum PromqlValue {
    /// Scalar at an evaluation timestamp.
    Scalar {
        /// Evaluation timestamp.
        timestamp_ms: i64,
        /// Scalar value.
        value: f64,
    },
    /// String at an evaluation timestamp.
    String {
        /// Evaluation timestamp.
        timestamp_ms: i64,
        /// String value.
        value: String,
    },
    /// Instant vector.
    Vector(Vec<PromqlSample>),
    /// Range vector or range-query output.
    Matrix(Vec<PromqlSeries>),
}

/// Parse or evaluation error returned through the Prometheus API envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlError(String);

impl PromqlError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for PromqlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PromqlError {}

/// Rust PromQL evaluator backed by ShardTelemetry metric stripes.
#[derive(Clone)]
pub struct PromqlEngine {
    store: Arc<DurableTelemetryStore>,
    tenant: Arc<str>,
    limits: PromqlLimits,
}

impl fmt::Debug for PromqlEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PromqlEngine")
            .field("tenant", &self.tenant)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl PromqlEngine {
    /// Creates a bounded single-tenant evaluator.
    #[must_use]
    pub fn new(store: Arc<DurableTelemetryStore>, tenant: Arc<str>, limits: PromqlLimits) -> Self {
        Self {
            store,
            tenant,
            limits,
        }
    }

    /// Evaluates one PromQL expression at one instant.
    pub fn query(&self, expression: &str, time_ms: i64) -> Result<PromqlValue, PromqlError> {
        let expr = parse(expression).map_err(PromqlError::new)?;
        let context = EvalContext {
            eval_ms: time_ms,
            start_ms: time_ms,
            end_ms: time_ms,
            lookback: self.limits.lookback,
        };
        self.eval(&expr, &context)
    }

    /// Evaluates one PromQL expression over an inclusive stepped range.
    pub fn query_range(
        &self,
        expression: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<PromqlValue, PromqlError> {
        if step_ms <= 0 || start_ms > end_ms {
            return Err(PromqlError::new("invalid PromQL query range"));
        }
        let steps = ((end_ms - start_ms) / step_ms) as usize + 1;
        if steps > self.limits.max_steps {
            return Err(PromqlError::new("PromQL range exceeds the step limit"));
        }
        let expr = parse(expression).map_err(PromqlError::new)?;
        let mut series = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for ordinal in 0..steps {
            let eval_ms = start_ms + i64::try_from(ordinal).unwrap_or(i64::MAX) * step_ms;
            let value = self.eval(
                &expr,
                &EvalContext {
                    eval_ms,
                    start_ms,
                    end_ms,
                    lookback: self.limits.lookback,
                },
            )?;
            match value {
                PromqlValue::Scalar { value, .. } => {
                    series
                        .entry(BTreeMap::new())
                        .or_default()
                        .push((eval_ms, value));
                }
                PromqlValue::Vector(samples) => {
                    for sample in samples {
                        series
                            .entry(sample.labels)
                            .or_default()
                            .push((eval_ms, sample.value));
                    }
                }
                PromqlValue::Matrix(_) | PromqlValue::String { .. } => {
                    return Err(PromqlError::new(
                        "range query expression must return a scalar or instant vector",
                    ));
                }
            }
        }
        Ok(PromqlValue::Matrix(
            series
                .into_iter()
                .map(|(labels, samples)| PromqlSeries { labels, samples })
                .collect(),
        ))
    }

    /// Selects exact raw points for Prometheus discovery, metadata, and exemplar APIs.
    pub(crate) fn raw_points(
        &self,
        selectors: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<DurableMetricPoint>, PromqlError> {
        if start_ms > end_ms {
            return Err(PromqlError::new("invalid Prometheus discovery range"));
        }
        if selectors.is_empty() {
            return self
                .store
                .query_metrics(&MetricQuery {
                    tenant: Arc::clone(&self.tenant),
                    start_time_unix_nanos: Some(millis_to_nanos(start_ms)?),
                    end_time_unix_nanos: Some(millis_to_nanos(end_ms)?),
                    limit: self.limits.max_points,
                    ..MetricQuery::default()
                })
                .map_err(|error| PromqlError::new(error.to_string()));
        }
        let mut points = BTreeMap::new();
        for expression in selectors {
            let selector = match parse(expression).map_err(PromqlError::new)? {
                Expr::VectorSelector(selector) => selector,
                _ => {
                    return Err(PromqlError::new(
                        "series matchers must be instant vector selectors",
                    ));
                }
            };
            for point in self.scan_selector(&selector, start_ms, end_ms)? {
                let labels = point_labels(&point);
                if selector_matches(&selector, &labels) {
                    points.insert(
                        (point.record_ref.topic_partition, point.record_ref.offset),
                        point,
                    );
                }
            }
        }
        Ok(points.into_values().collect())
    }

    fn eval(&self, expr: &Expr, context: &EvalContext) -> Result<PromqlValue, PromqlError> {
        match expr {
            Expr::NumberLiteral(value) => Ok(PromqlValue::Scalar {
                timestamp_ms: context.eval_ms,
                value: value.val,
            }),
            Expr::StringLiteral(value) => Ok(PromqlValue::String {
                timestamp_ms: context.eval_ms,
                value: value.val.clone(),
            }),
            Expr::VectorSelector(selector) => self.select_vector(selector, context),
            Expr::MatrixSelector(selector) => self.select_matrix(selector, context),
            Expr::Paren(paren) => self.eval(&paren.expr, context),
            Expr::Unary(unary) => negate(self.eval(&unary.expr, context)?),
            Expr::Aggregate(aggregate) => self.aggregate(aggregate, context),
            Expr::Binary(binary) => self.binary(binary, context),
            Expr::Call(call) => self.call(call, context),
            Expr::Subquery(subquery) => self.eval_subquery(subquery, context),
            Expr::Extension(_) => Err(PromqlError::new("unsupported PromQL extension node")),
        }
    }

    fn select_vector(
        &self,
        selector: &VectorSelector,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let eval_ms = selector_time(selector, context)?;
        let lookback_ms = i64::try_from(context.lookback.as_millis()).unwrap_or(i64::MAX);
        let points = self.scan_selector(selector, eval_ms.saturating_sub(lookback_ms), eval_ms)?;
        let mut latest = BTreeMap::<BTreeMap<String, String>, DurableMetricPoint>::new();
        for point in points {
            let labels = point_labels(&point);
            if !selector_matches(selector, &labels) {
                continue;
            }
            let replace = latest.get(&labels).is_none_or(|prior| {
                (point.timestamp_unix_nanos, point.record_ref.offset)
                    > (prior.timestamp_unix_nanos, prior.record_ref.offset)
            });
            if replace {
                latest.insert(labels, point);
            }
        }
        let mut samples = Vec::with_capacity(latest.len());
        for (labels, point) in latest {
            if let Some(value) = point_float(&point) {
                if value.to_bits() == PROMETHEUS_STALE_NAN_BITS {
                    continue;
                }
                samples.push(PromqlSample {
                    labels,
                    timestamp_ms: eval_ms,
                    value,
                });
            }
        }
        self.bound_vector(samples).map(PromqlValue::Vector)
    }

    fn select_matrix(
        &self,
        selector: &MatrixSelector,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let eval_ms = selector_time(&selector.vs, context)?;
        let range_ms = i64::try_from(selector.range.as_millis()).unwrap_or(i64::MAX);
        let start_ms = eval_ms.saturating_sub(range_ms);
        let points = self.scan_selector(&selector.vs, start_ms, eval_ms)?;
        let mut series = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for point in points {
            let labels = point_labels(&point);
            let timestamp_ms = nanos_to_millis(point.timestamp_unix_nanos)?;
            if timestamp_ms <= start_ms || !selector_matches(&selector.vs, &labels) {
                continue;
            }
            if let Some(value) = point_float(&point)
                && value.to_bits() != PROMETHEUS_STALE_NAN_BITS
            {
                series
                    .entry(labels)
                    .or_default()
                    .push((timestamp_ms, value));
            }
        }
        if series.len() > self.limits.max_series {
            return Err(PromqlError::new("PromQL series limit exceeded"));
        }
        Ok(PromqlValue::Matrix(
            series
                .into_iter()
                .map(|(labels, mut samples)| {
                    samples.sort_unstable_by_key(|(timestamp, _)| *timestamp);
                    PromqlSeries { labels, samples }
                })
                .collect(),
        ))
    }

    fn eval_subquery(
        &self,
        subquery: &SubqueryExpr,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let end_ms = subquery_time(subquery, context)?;
        let range_ms = i64::try_from(subquery.range.as_millis()).unwrap_or(i64::MAX);
        let start_ms = end_ms.saturating_sub(range_ms);
        let step = subquery.step.unwrap_or(Duration::from_secs(60));
        let step_ms = i64::try_from(step.as_millis()).unwrap_or(i64::MAX);
        if step_ms <= 0 {
            return Err(PromqlError::new("PromQL subquery step must be positive"));
        }
        let first_ms = start_ms
            .div_euclid(step_ms)
            .saturating_add(1)
            .saturating_mul(step_ms);
        let step_count = if first_ms > end_ms {
            0
        } else {
            usize::try_from((end_ms - first_ms) / step_ms)
                .unwrap_or(usize::MAX)
                .saturating_add(1)
        };
        if step_count > self.limits.max_steps {
            return Err(PromqlError::new("PromQL subquery exceeds the step limit"));
        }

        let mut series = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
        for ordinal in 0..step_count {
            let eval_ms = first_ms.saturating_add(
                i64::try_from(ordinal)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(step_ms),
            );
            match self.eval(
                &subquery.expr,
                &EvalContext {
                    eval_ms,
                    start_ms: context.start_ms,
                    end_ms: context.end_ms,
                    lookback: context.lookback,
                },
            )? {
                PromqlValue::Vector(samples) => {
                    for sample in samples {
                        if sample.value.to_bits() != PROMETHEUS_STALE_NAN_BITS {
                            series
                                .entry(sample.labels)
                                .or_default()
                                .push((eval_ms, sample.value));
                        }
                    }
                }
                PromqlValue::Scalar { value, .. } => {
                    series
                        .entry(BTreeMap::new())
                        .or_default()
                        .push((eval_ms, value));
                }
                PromqlValue::Matrix(_) | PromqlValue::String { .. } => {
                    return Err(PromqlError::new(
                        "PromQL subquery expression must return an instant vector or scalar",
                    ));
                }
            }
            if series.len() > self.limits.max_series {
                return Err(PromqlError::new("PromQL series limit exceeded"));
            }
        }
        Ok(PromqlValue::Matrix(
            series
                .into_iter()
                .map(|(labels, samples)| PromqlSeries { labels, samples })
                .collect(),
        ))
    }

    fn scan_selector(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<DurableMetricPoint>, PromqlError> {
        if start_ms < 0 || end_ms < 0 {
            return Err(PromqlError::new(
                "pre-epoch metric timestamps are outside the current storage epoch",
            ));
        }
        let name = selector_name(selector);
        let exact_labels = selector
            .matchers
            .matchers
            .iter()
            .filter_map(|matcher| match matcher.op {
                MatchOp::Equal if matcher.name != "__name__" => Some((
                    Arc::<str>::from(matcher.name.as_str()),
                    Arc::<str>::from(matcher.value.as_str()),
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.store
            .query_metrics(&MetricQuery {
                tenant: Arc::clone(&self.tenant),
                series: None,
                name: name.map(Arc::from),
                exact_labels: Arc::new(exact_labels),
                start_time_unix_nanos: Some(millis_to_nanos(start_ms)?),
                end_time_unix_nanos: Some(millis_to_nanos(end_ms)?),
                limit: self.limits.max_points,
            })
            .map_err(|error| PromqlError::new(error.to_string()))
    }

    fn aggregate(
        &self,
        aggregate: &AggregateExpr,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let PromqlValue::Vector(samples) = self.eval(&aggregate.expr, context)? else {
            return Err(PromqlError::new("aggregation requires an instant vector"));
        };
        let mut groups = BTreeMap::<BTreeMap<String, String>, Vec<f64>>::new();
        for sample in samples {
            let labels = grouped_labels(sample.labels, aggregate.modifier.as_ref());
            groups.entry(labels).or_default().push(sample.value);
        }
        let operation = aggregate.op.to_string();
        let mut output = Vec::with_capacity(groups.len());
        for (labels, values) in groups {
            let value = match operation.as_str() {
                "sum" => values.iter().sum(),
                "avg" => values.iter().sum::<f64>() / values.len() as f64,
                "count" => values.len() as f64,
                "group" => 1.0,
                "min" => values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN),
                "max" => values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN),
                "stddev" => variance(&values).sqrt(),
                "stdvar" => variance(&values),
                _ => {
                    return Err(PromqlError::new(format!(
                        "PromQL aggregator {operation} is not enabled"
                    )));
                }
            };
            output.push(PromqlSample {
                labels,
                timestamp_ms: context.eval_ms,
                value,
            });
        }
        self.bound_vector(output).map(PromqlValue::Vector)
    }

    fn binary(
        &self,
        binary: &BinaryExpr,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let left = self.eval(&binary.lhs, context)?;
        let right = self.eval(&binary.rhs, context)?;
        let op = binary.op.to_string();
        match (left, right) {
            (
                PromqlValue::Scalar {
                    timestamp_ms,
                    value: left,
                },
                PromqlValue::Scalar { value: right, .. },
            ) => Ok(PromqlValue::Scalar {
                timestamp_ms,
                value: binary_float(&op, left, right, binary.return_bool())?.unwrap_or(f64::NAN),
            }),
            (PromqlValue::Vector(samples), PromqlValue::Scalar { value, .. }) => self
                .bound_vector(binary_vector_scalar(
                    samples,
                    value,
                    &op,
                    false,
                    binary.return_bool(),
                )?)
                .map(PromqlValue::Vector),
            (PromqlValue::Scalar { value, .. }, PromqlValue::Vector(samples)) => self
                .bound_vector(binary_vector_scalar(
                    samples,
                    value,
                    &op,
                    true,
                    binary.return_bool(),
                )?)
                .map(PromqlValue::Vector),
            (PromqlValue::Vector(left), PromqlValue::Vector(right)) => self
                .bound_vector(binary_vectors(left, right, binary, &op)?)
                .map(PromqlValue::Vector),
            _ => Err(PromqlError::new("unsupported PromQL binary operand types")),
        }
    }

    fn call(&self, call: &Call, context: &EvalContext) -> Result<PromqlValue, PromqlError> {
        match call.func.name {
            "time" => Ok(PromqlValue::Scalar {
                timestamp_ms: context.eval_ms,
                value: context.eval_ms as f64 / 1_000.0,
            }),
            "vector" => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                let PromqlValue::Scalar { value, .. } = value else {
                    return Err(PromqlError::new("vector() requires a scalar"));
                };
                Ok(PromqlValue::Vector(vec![PromqlSample {
                    labels: BTreeMap::new(),
                    timestamp_ms: context.eval_ms,
                    value,
                }]))
            }
            "scalar" => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                let PromqlValue::Vector(samples) = value else {
                    return Err(PromqlError::new("scalar() requires an instant vector"));
                };
                Ok(PromqlValue::Scalar {
                    timestamp_ms: context.eval_ms,
                    value: if samples.len() == 1 {
                        samples[0].value
                    } else {
                        f64::NAN
                    },
                })
            }
            "rate" | "irate" | "increase" | "delta" | "idelta" | "changes" | "resets"
            | "sum_over_time" | "avg_over_time" | "min_over_time" | "max_over_time"
            | "count_over_time" | "last_over_time" | "present_over_time" => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                let PromqlValue::Matrix(series) = value else {
                    return Err(PromqlError::new(format!(
                        "{}() requires a range vector",
                        call.func.name
                    )));
                };
                self.range_function(call.func.name, series, context)
            }
            name if is_unary_math(name) => {
                let value = self.eval(call_arg(call, 0)?, context)?;
                map_vector(value, context.eval_ms, |value| unary_math(name, value))
            }
            name => Err(PromqlError::new(format!(
                "PromQL function {name} is not enabled"
            ))),
        }
    }

    fn range_function(
        &self,
        function: &str,
        series: Vec<PromqlSeries>,
        context: &EvalContext,
    ) -> Result<PromqlValue, PromqlError> {
        let mut output = Vec::new();
        for series in series {
            let values = series
                .samples
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            let value = match function {
                "sum_over_time" => values.iter().sum(),
                "avg_over_time" => values.iter().sum::<f64>() / values.len() as f64,
                "min_over_time" => values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN),
                "max_over_time" => values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN),
                "count_over_time" => values.len() as f64,
                "last_over_time" => *values.last().unwrap_or(&f64::NAN),
                "present_over_time" => f64::from(!values.is_empty()),
                "changes" => values.windows(2).filter(|pair| pair[0] != pair[1]).count() as f64,
                "resets" => values.windows(2).filter(|pair| pair[1] < pair[0]).count() as f64,
                "delta" => delta(&series.samples, false),
                "idelta" => delta(&series.samples, true),
                "rate" => counter_rate(&series.samples, false),
                "irate" => counter_rate(&series.samples, true),
                "increase" => {
                    let duration = series
                        .samples
                        .last()
                        .zip(series.samples.first())
                        .map_or(0.0, |(last, first)| (last.0 - first.0) as f64 / 1_000.0);
                    counter_rate(&series.samples, false) * duration
                }
                _ => unreachable!("range function was matched by caller"),
            };
            output.push(PromqlSample {
                labels: series.labels,
                timestamp_ms: context.eval_ms,
                value,
            });
        }
        self.bound_vector(output).map(PromqlValue::Vector)
    }

    fn bound_vector(&self, samples: Vec<PromqlSample>) -> Result<Vec<PromqlSample>, PromqlError> {
        if samples.len() > self.limits.max_series {
            Err(PromqlError::new("PromQL series limit exceeded"))
        } else {
            Ok(samples)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EvalContext {
    eval_ms: i64,
    start_ms: i64,
    end_ms: i64,
    lookback: Duration,
}

fn selector_name(selector: &VectorSelector) -> Option<&str> {
    selector.name.as_deref().or_else(|| {
        selector
            .matchers
            .matchers
            .iter()
            .find(|matcher| matcher.name == "__name__" && matches!(matcher.op, MatchOp::Equal))
            .map(|matcher| matcher.value.as_str())
    })
}

fn selector_matches(selector: &VectorSelector, labels: &BTreeMap<String, String>) -> bool {
    let base = matcher_group_matches(&selector.matchers.matchers, labels);
    if selector.matchers.or_matchers.is_empty() {
        base
    } else {
        base && selector
            .matchers
            .or_matchers
            .iter()
            .any(|group| matcher_group_matches(group, labels))
    }
}

fn matcher_group_matches(matchers: &[Matcher], labels: &BTreeMap<String, String>) -> bool {
    matchers
        .iter()
        .all(|matcher| matcher.is_match(labels.get(&matcher.name).map_or("", String::as_str)))
}

fn selector_time(selector: &VectorSelector, context: &EvalContext) -> Result<i64, PromqlError> {
    let at = match selector.at.as_ref() {
        None => context.eval_ms,
        Some(AtModifier::Start) => context.start_ms,
        Some(AtModifier::End) => context.end_ms,
        Some(AtModifier::At(time)) => system_time_millis(*time)?,
    };
    let offset = match selector.offset.as_ref() {
        None => 0,
        Some(Offset::Pos(duration)) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Some(Offset::Neg(duration)) => -i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
    };
    at.checked_sub(offset)
        .ok_or_else(|| PromqlError::new("PromQL selector timestamp overflow"))
}

fn subquery_time(subquery: &SubqueryExpr, context: &EvalContext) -> Result<i64, PromqlError> {
    let at = match subquery.at.as_ref() {
        None => context.eval_ms,
        Some(AtModifier::Start) => context.start_ms,
        Some(AtModifier::End) => context.end_ms,
        Some(AtModifier::At(time)) => system_time_millis(*time)?,
    };
    let offset = match subquery.offset.as_ref() {
        None => 0,
        Some(Offset::Pos(duration)) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Some(Offset::Neg(duration)) => -i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
    };
    at.checked_sub(offset)
        .ok_or_else(|| PromqlError::new("PromQL subquery timestamp overflow"))
}

fn system_time_millis(time: SystemTime) -> Result<i64, PromqlError> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| PromqlError::new("PromQL @ timestamp exceeds i64")),
        Err(error) => i64::try_from(error.duration().as_millis())
            .map(|value| -value)
            .map_err(|_| PromqlError::new("PromQL @ timestamp precedes i64")),
    }
}

fn point_labels(point: &DurableMetricPoint) -> BTreeMap<String, String> {
    let mut labels = prometheus_string_labels(&point.identity)
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("__name__".into(), point.identity.name.to_string());
    labels
}

fn point_float(point: &DurableMetricPoint) -> Option<f64> {
    let value = match point.value {
        MetricValue::Gauge(value) | MetricValue::Sum(value) => value,
        MetricValue::ExplicitHistogram(_)
        | MetricValue::ExponentialHistogram(_)
        | MetricValue::Summary(_) => return None,
    };
    Some(match value {
        NumberValue::Integer(value) => value as f64,
        NumberValue::DoubleBits(bits) => f64::from_bits(bits),
    })
}

fn grouped_labels(
    mut labels: BTreeMap<String, String>,
    modifier: Option<&LabelModifier>,
) -> BTreeMap<String, String> {
    labels.remove("__name__");
    match modifier {
        None => BTreeMap::new(),
        Some(LabelModifier::Include(included)) => labels
            .into_iter()
            .filter(|(name, _)| included.labels.contains(name))
            .collect(),
        Some(LabelModifier::Exclude(excluded)) => labels
            .into_iter()
            .filter(|(name, _)| !excluded.labels.contains(name))
            .collect(),
    }
}

fn match_key(labels: &BTreeMap<String, String>, binary: &BinaryExpr) -> Vec<(String, String)> {
    let mut labels = labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__name__")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    if let Some(modifier) = &binary.modifier
        && let Some(matching) = &modifier.matching
    {
        match matching {
            LabelModifier::Include(included) => {
                labels.retain(|(name, _)| included.labels.contains(name));
            }
            LabelModifier::Exclude(excluded) => {
                labels.retain(|(name, _)| !excluded.labels.contains(name));
            }
        }
    }
    labels
}

fn binary_vectors(
    left: Vec<PromqlSample>,
    right: Vec<PromqlSample>,
    binary: &BinaryExpr,
    operation: &str,
) -> Result<Vec<PromqlSample>, PromqlError> {
    let mut left_by_key = HashMap::<Vec<(String, String)>, Vec<usize>>::new();
    let mut right_by_key = HashMap::<Vec<(String, String)>, Vec<usize>>::new();
    for (index, sample) in left.iter().enumerate() {
        left_by_key
            .entry(match_key(&sample.labels, binary))
            .or_default()
            .push(index);
    }
    for (index, sample) in right.iter().enumerate() {
        right_by_key
            .entry(match_key(&sample.labels, binary))
            .or_default()
            .push(index);
    }

    if binary.op.is_set_operator() {
        return match operation {
            "and" => Ok(left
                .into_iter()
                .filter(|sample| right_by_key.contains_key(&match_key(&sample.labels, binary)))
                .collect()),
            "unless" => Ok(left
                .into_iter()
                .filter(|sample| !right_by_key.contains_key(&match_key(&sample.labels, binary)))
                .collect()),
            "or" => {
                let mut output = left;
                output.extend(right.into_iter().filter(|sample| {
                    !left_by_key.contains_key(&match_key(&sample.labels, binary))
                }));
                Ok(output)
            }
            _ => Err(PromqlError::new(format!(
                "unsupported PromQL set operator {operation}"
            ))),
        };
    }

    let cardinality = binary
        .modifier
        .as_ref()
        .map_or(VectorMatchCardinality::OneToOne, |modifier| {
            modifier.card.clone()
        });
    match &cardinality {
        VectorMatchCardinality::OneToOne => {
            reject_duplicate_match_groups(&left_by_key, "left")?;
            reject_duplicate_match_groups(&right_by_key, "right")?;
            let mut output = Vec::new();
            for mut sample in left {
                let key = match_key(&sample.labels, binary);
                let Some(right_index) = right_by_key.get(&key).and_then(|group| group.first())
                else {
                    continue;
                };
                if let Some(value) = binary_float(
                    operation,
                    sample.value,
                    right[*right_index].value,
                    binary.return_bool(),
                )? {
                    sample.value = value;
                    normalize_binary_labels(&mut sample.labels, binary, None);
                    output.push(sample);
                }
            }
            Ok(output)
        }
        VectorMatchCardinality::ManyToOne(included) => {
            reject_duplicate_match_groups(&right_by_key, "right")?;
            let mut output = Vec::new();
            for mut sample in left {
                let key = match_key(&sample.labels, binary);
                let Some(right_index) = right_by_key.get(&key).and_then(|group| group.first())
                else {
                    continue;
                };
                let one = &right[*right_index];
                if let Some(value) =
                    binary_float(operation, sample.value, one.value, binary.return_bool())?
                {
                    sample.value = value;
                    normalize_binary_labels(
                        &mut sample.labels,
                        binary,
                        Some((&one.labels, included)),
                    );
                    output.push(sample);
                }
            }
            Ok(output)
        }
        VectorMatchCardinality::OneToMany(included) => {
            reject_duplicate_match_groups(&left_by_key, "left")?;
            let mut output = Vec::new();
            for mut sample in right {
                let key = match_key(&sample.labels, binary);
                let Some(left_index) = left_by_key.get(&key).and_then(|group| group.first()) else {
                    continue;
                };
                let one = &left[*left_index];
                if let Some(value) =
                    binary_float(operation, one.value, sample.value, binary.return_bool())?
                {
                    sample.value = value;
                    normalize_binary_labels(
                        &mut sample.labels,
                        binary,
                        Some((&one.labels, included)),
                    );
                    output.push(sample);
                }
            }
            Ok(output)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::new(
            "many-to-many matching is only valid for PromQL set operators",
        )),
    }
}

fn reject_duplicate_match_groups(
    groups: &HashMap<Vec<(String, String)>, Vec<usize>>,
    side: &str,
) -> Result<(), PromqlError> {
    if groups.values().any(|group| group.len() > 1) {
        Err(PromqlError::new(format!(
            "many-to-many matching: duplicate series on the {side} side"
        )))
    } else {
        Ok(())
    }
}

fn normalize_binary_labels(
    labels: &mut BTreeMap<String, String>,
    binary: &BinaryExpr,
    include_from_one: Option<(&BTreeMap<String, String>, &promql_parser::label::Labels)>,
) {
    if !binary.op.is_comparison_operator() || binary.return_bool() || binary.is_matching_on() {
        labels.remove("__name__");
    }
    if let Some((one, included)) = include_from_one {
        for name in &included.labels {
            if let Some(value) = one.get(name) {
                labels.insert(name.clone(), value.clone());
            } else {
                labels.remove(name);
            }
        }
    }
}

fn binary_vector_scalar(
    samples: Vec<PromqlSample>,
    scalar: f64,
    operation: &str,
    scalar_left: bool,
    return_bool: bool,
) -> Result<Vec<PromqlSample>, PromqlError> {
    let mut output = Vec::new();
    for mut sample in samples {
        let (left, right) = if scalar_left {
            (scalar, sample.value)
        } else {
            (sample.value, scalar)
        };
        if let Some(value) = binary_float(operation, left, right, return_bool)? {
            sample.value = value;
            output.push(sample);
        }
    }
    Ok(output)
}

fn binary_float(
    operation: &str,
    left: f64,
    right: f64,
    return_bool: bool,
) -> Result<Option<f64>, PromqlError> {
    let comparison = match operation {
        "==" => Some(left == right),
        "!=" => Some(left != right),
        ">" => Some(left > right),
        ">=" => Some(left >= right),
        "<" => Some(left < right),
        "<=" => Some(left <= right),
        _ => None,
    };
    if let Some(matches) = comparison {
        return Ok(if return_bool {
            Some(f64::from(matches))
        } else if matches {
            Some(left)
        } else {
            None
        });
    }
    Ok(Some(match operation {
        "+" => left + right,
        "-" => left - right,
        "*" => left * right,
        "/" => left / right,
        "%" => left % right,
        "^" => left.powf(right),
        "atan2" => left.atan2(right),
        _ => {
            return Err(PromqlError::new(format!(
                "unsupported binary operator {operation}"
            )));
        }
    }))
}

fn negate(value: PromqlValue) -> Result<PromqlValue, PromqlError> {
    map_vector(value, 0, |value| -value)
}

fn map_vector(
    value: PromqlValue,
    timestamp_ms: i64,
    function: impl Fn(f64) -> f64,
) -> Result<PromqlValue, PromqlError> {
    match value {
        PromqlValue::Scalar {
            timestamp_ms: observed,
            value,
        } => Ok(PromqlValue::Scalar {
            timestamp_ms: observed,
            value: function(value),
        }),
        PromqlValue::Vector(mut samples) => {
            for sample in &mut samples {
                sample.timestamp_ms = if timestamp_ms == 0 {
                    sample.timestamp_ms
                } else {
                    timestamp_ms
                };
                sample.value = function(sample.value);
            }
            Ok(PromqlValue::Vector(samples))
        }
        _ => Err(PromqlError::new(
            "function requires a scalar or instant vector",
        )),
    }
}

fn is_unary_math(name: &str) -> bool {
    matches!(
        name,
        "abs" | "ceil" | "floor" | "exp" | "ln" | "log2" | "log10" | "sqrt" | "sgn"
    )
}

fn unary_math(name: &str, value: f64) -> f64 {
    match name {
        "abs" => value.abs(),
        "ceil" => value.ceil(),
        "floor" => value.floor(),
        "exp" => value.exp(),
        "ln" => value.ln(),
        "log2" => value.log2(),
        "log10" => value.log10(),
        "sqrt" => value.sqrt(),
        "sgn" => value.signum(),
        _ => unreachable!("name was checked by is_unary_math"),
    }
}

fn call_arg(call: &Call, index: usize) -> Result<&Expr, PromqlError> {
    call.args.args.get(index).map(Box::as_ref).ok_or_else(|| {
        PromqlError::new(format!("{}() is missing argument {index}", call.func.name))
    })
}

fn variance(values: &[f64]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64
}

fn delta(samples: &[(i64, f64)], instant: bool) -> f64 {
    let samples = if instant && samples.len() > 2 {
        &samples[samples.len() - 2..]
    } else {
        samples
    };
    samples
        .last()
        .zip(samples.first())
        .map_or(f64::NAN, |(last, first)| last.1 - first.1)
}

fn counter_rate(samples: &[(i64, f64)], instant: bool) -> f64 {
    let samples = if instant && samples.len() > 2 {
        &samples[samples.len() - 2..]
    } else {
        samples
    };
    if samples.len() < 2 {
        return f64::NAN;
    }
    let mut increase = 0.0;
    for pair in samples.windows(2) {
        increase += if pair[1].1 < pair[0].1 {
            pair[1].1
        } else {
            pair[1].1 - pair[0].1
        };
    }
    let seconds = (samples.last().unwrap().0 - samples.first().unwrap().0) as f64 / 1_000.0;
    increase / seconds
}

fn millis_to_nanos(milliseconds: i64) -> Result<u64, PromqlError> {
    u64::try_from(milliseconds)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or_else(|| PromqlError::new("PromQL timestamp is outside the storage range"))
}

fn nanos_to_millis(nanoseconds: u64) -> Result<i64, PromqlError> {
    i64::try_from(nanoseconds / 1_000_000)
        .map_err(|_| PromqlError::new("metric timestamp exceeds PromQL range"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::{DurableTelemetryConfig, StripeConfig};

    fn sample(labels: &[(&str, &str)], value: f64) -> PromqlSample {
        PromqlSample {
            labels: labels
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            timestamp_ms: 1,
            value,
        }
    }

    fn binary(expression: &str) -> BinaryExpr {
        match parse(expression).unwrap() {
            Expr::Binary(binary) => binary,
            _ => panic!("expected binary expression"),
        }
    }

    #[test]
    fn comparisons_filter_unless_bool_is_requested() {
        assert_eq!(binary_float(">", 2.0, 1.0, false).unwrap(), Some(2.0));
        assert_eq!(binary_float(">", 0.0, 1.0, false).unwrap(), None);
        assert_eq!(binary_float(">", 0.0, 1.0, true).unwrap(), Some(0.0));
    }

    #[test]
    fn counter_rate_accounts_for_resets() {
        let samples = vec![(0, 8.0), (1_000, 10.0), (2_000, 2.0), (3_000, 4.0)];
        assert_eq!(counter_rate(&samples, false), 2.0);
        assert_eq!(counter_rate(&samples, true), 2.0);
    }

    #[test]
    fn subqueries_execute_through_the_bounded_evaluator() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-promql-subquery-{}-{nonce}",
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
        let engine = PromqlEngine::new(store, Arc::from("tenant"), PromqlLimits::default());
        assert_eq!(
            engine.query("up[5m:1m]", 600_000).unwrap(),
            PromqlValue::Matrix(Vec::new())
        );
        drop(engine);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn vector_matching_honors_group_left_and_included_labels() {
        let expression = binary("left * on(job) group_left(zone) right");
        let left = vec![
            sample(
                &[("__name__", "left"), ("job", "api"), ("instance", "a")],
                2.0,
            ),
            sample(
                &[("__name__", "left"), ("job", "api"), ("instance", "b")],
                3.0,
            ),
        ];
        let right = vec![sample(
            &[("__name__", "right"), ("job", "api"), ("zone", "west")],
            4.0,
        )];
        let output = binary_vectors(left, right, &expression, "*").unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].value, 8.0);
        assert_eq!(output[1].value, 12.0);
        assert_eq!(
            output[0].labels.get("zone").map(String::as_str),
            Some("west")
        );
        assert!(!output[0].labels.contains_key("__name__"));
    }

    #[test]
    fn vector_set_operators_keep_prometheus_side_semantics() {
        let left = vec![
            sample(&[("job", "api")], 1.0),
            sample(&[("job", "worker")], 2.0),
        ];
        let right = vec![
            sample(&[("job", "api")], 10.0),
            sample(&[("job", "db")], 30.0),
        ];
        let and = binary("left and on(job) right");
        assert_eq!(
            binary_vectors(left.clone(), right.clone(), &and, "and")
                .unwrap()
                .into_iter()
                .map(|sample| sample.value)
                .collect::<Vec<_>>(),
            vec![1.0]
        );
        let or = binary("left or on(job) right");
        assert_eq!(
            binary_vectors(left, right, &or, "or")
                .unwrap()
                .into_iter()
                .map(|sample| sample.value)
                .collect::<Vec<_>>(),
            vec![1.0, 2.0, 30.0]
        );
    }
}
