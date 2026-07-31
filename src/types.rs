use std::fmt;
use std::sync::Arc;

use regex::{Regex, RegexBuilder};
use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::{CompressionCohortId, LogDbError, LogDbResult};

/// Stable address of one log record in shard-stream's ordered append log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordRef {
    /// Logical topic and partition containing the record.
    pub topic_partition: TopicPartition,
    /// Durable logical offset of the record.
    pub offset: LogicalOffset,
}

impl RecordRef {
    /// Creates a record address.
    #[must_use]
    pub const fn new(topic_partition: TopicPartition, offset: LogicalOffset) -> Self {
        Self {
            topic_partition,
            offset,
        }
    }
}

/// A normalized metadata field stored alongside a log message.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataField {
    /// Metadata field name, for example `service` or `level`.
    pub key: Arc<str>,
    /// Exact field value.
    pub value: Arc<str>,
}

impl MetadataField {
    /// Creates a metadata field from owned or borrowed string-like values.
    #[must_use]
    pub fn new(key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// One log event accepted after the corresponding shard-stream append is durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLogRecord {
    /// The physical shard-stream worker that owns this record.
    pub stream_shard_id: ShardId,
    /// Ordered log address assigned by shard-stream.
    pub record_ref: RecordRef,
    /// Event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Original unstructured message body.
    pub message: Arc<str>,
    /// Extracted exact-match metadata.
    pub fields: Arc<Vec<MetadataField>>,
    /// Compression cohort selected by the producer or parser.
    pub compression_cohort: CompressionCohortId,
}

impl DurableLogRecord {
    /// Constructs a durable log record with no extracted metadata.
    #[must_use]
    pub fn new(
        stream_shard_id: ShardId,
        topic_partition: TopicPartition,
        offset: LogicalOffset,
        timestamp_unix_nanos: u64,
        message: impl Into<Arc<str>>,
        compression_cohort: CompressionCohortId,
    ) -> Self {
        Self {
            stream_shard_id,
            record_ref: RecordRef::new(topic_partition, offset),
            timestamp_unix_nanos,
            message: message.into(),
            fields: Arc::new(Vec::new()),
            compression_cohort,
        }
    }

    /// Adds a normalized metadata field.
    #[must_use]
    pub fn with_field(mut self, key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        Arc::make_mut(&mut self.fields).push(MetadataField::new(key, value));
        self
    }
}

/// Ordering applied to matching log records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryOrder {
    /// Return the lowest durable offsets first.
    #[default]
    OldestFirst,
    /// Return the highest durable offsets first.
    NewestFirst,
}

/// Case handling for message and metadata text predicates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CaseSensitivity {
    /// Compare Unicode text exactly.
    Sensitive,
    /// Compare Unicode text case-insensitively.
    #[default]
    Insensitive,
}

/// Non-regex text operation used by message and metadata predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextMatchKind {
    /// The complete value must match.
    Exact,
    /// The observed value must contain the requested text.
    Contains,
    /// The observed value must start with the requested text.
    Prefix,
    /// The observed value must end with the requested text.
    Suffix,
}

/// Reusable literal text matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextMatcher {
    /// Requested literal text.
    pub value: Arc<str>,
    /// Literal operation to perform.
    pub kind: TextMatchKind,
    /// Case handling for the comparison.
    pub case_sensitivity: CaseSensitivity,
}

impl TextMatcher {
    /// Creates a literal text matcher.
    #[must_use]
    pub fn new(
        value: impl Into<Arc<str>>,
        kind: TextMatchKind,
        case_sensitivity: CaseSensitivity,
    ) -> Self {
        Self {
            value: value.into(),
            kind,
            case_sensitivity,
        }
    }
}

/// Validated regular expression used by a log predicate.
#[derive(Clone)]
pub struct LogRegex {
    pattern: Arc<str>,
    case_sensitivity: CaseSensitivity,
    compiled: Arc<Regex>,
}

impl LogRegex {
    /// Compiles one Unicode-aware regular expression.
    pub fn new(
        pattern: impl Into<Arc<str>>,
        case_sensitivity: CaseSensitivity,
    ) -> LogDbResult<Self> {
        let pattern = pattern.into();
        let compiled = RegexBuilder::new(&pattern)
            .case_insensitive(case_sensitivity == CaseSensitivity::Insensitive)
            .build()
            .map_err(|error| LogDbError::InvalidQuery(error.to_string()))?;
        Ok(Self {
            pattern,
            case_sensitivity,
            compiled: Arc::new(compiled),
        })
    }

    /// Returns the original regular-expression source.
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Returns the expression's case handling.
    #[must_use]
    pub const fn case_sensitivity(&self) -> CaseSensitivity {
        self.case_sensitivity
    }

    pub(crate) fn is_match(&self, observed: &str) -> bool {
        self.compiled.is_match(observed)
    }
}

impl fmt::Debug for LogRegex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogRegex")
            .field("pattern", &self.pattern)
            .field("case_sensitivity", &self.case_sensitivity)
            .finish_non_exhaustive()
    }
}

impl PartialEq for LogRegex {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.case_sensitivity == other.case_sensitivity
    }
}

impl Eq for LogRegex {}

/// Integer comparison for normalized numeric metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericComparison {
    /// Observed value is equal to the requested value after integer parsing.
    Equal,
    /// Observed value is not equal to the requested value after integer parsing.
    NotEqual,
    /// Observed value is less than the requested value.
    LessThan,
    /// Observed value is less than or equal to the requested value.
    LessThanOrEqual,
    /// Observed value is greater than the requested value.
    GreaterThan,
    /// Observed value is greater than or equal to the requested value.
    GreaterThanOrEqual,
}

/// Exact Boolean predicate evaluated against one normalized log record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LogPredicate {
    /// Matches every record.
    #[default]
    MatchAll,
    /// Matches no records.
    MatchNone,
    /// Matches one case-insensitive token produced by ShardLog's scanner.
    Term(Arc<str>),
    /// Matches the complete message with a literal text operation.
    Message(TextMatcher),
    /// Matches the complete message with a validated regular expression.
    MessageRegex(LogRegex),
    /// Matches records containing at least one field with this key.
    FieldExists(Arc<str>),
    /// Matches a field key against one literal text matcher.
    Field {
        /// Exact normalized field key.
        key: Arc<str>,
        /// Matcher applied to every value under the key.
        matcher: TextMatcher,
    },
    /// Matches when a field equals any value in the set.
    FieldIn {
        /// Exact normalized field key.
        key: Arc<str>,
        /// Exact case-sensitive values.
        values: Vec<Arc<str>>,
    },
    /// Matches a field value with a validated regular expression.
    FieldRegex {
        /// Exact normalized field key.
        key: Arc<str>,
        /// Expression applied to every value under the key.
        regex: LogRegex,
    },
    /// Parses field values as signed 128-bit integers and compares them.
    FieldNumeric {
        /// Exact normalized field key.
        key: Arc<str>,
        /// Integer comparison.
        comparison: NumericComparison,
        /// Requested comparison value.
        value: i128,
    },
    /// Every child predicate must match.
    And(Vec<LogPredicate>),
    /// At least one child predicate must match.
    Or(Vec<LogPredicate>),
    /// Inverts one child predicate.
    Not(Box<LogPredicate>),
}

impl LogPredicate {
    /// Creates an exact case-insensitive token predicate.
    #[must_use]
    pub fn term(term: impl Into<Arc<str>>) -> Self {
        Self::Term(term.into())
    }

    /// Creates a literal message predicate.
    #[must_use]
    pub fn message(matcher: TextMatcher) -> Self {
        Self::Message(matcher)
    }

    /// Creates a case-insensitive message substring predicate.
    #[must_use]
    pub fn message_contains(value: impl Into<Arc<str>>) -> Self {
        Self::Message(TextMatcher::new(
            value,
            TextMatchKind::Contains,
            CaseSensitivity::Insensitive,
        ))
    }

    /// Creates a validated message regular-expression predicate.
    pub fn message_regex(
        pattern: impl Into<Arc<str>>,
        case_sensitivity: CaseSensitivity,
    ) -> LogDbResult<Self> {
        Ok(Self::MessageRegex(LogRegex::new(
            pattern,
            case_sensitivity,
        )?))
    }

    /// Creates an exact metadata-key existence predicate.
    #[must_use]
    pub fn field_exists(key: impl Into<Arc<str>>) -> Self {
        Self::FieldExists(key.into())
    }

    /// Creates an exact case-sensitive metadata equality predicate.
    #[must_use]
    pub fn field_equals(key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        Self::Field {
            key: key.into(),
            matcher: TextMatcher::new(value, TextMatchKind::Exact, CaseSensitivity::Sensitive),
        }
    }

    /// Creates a literal metadata predicate.
    #[must_use]
    pub fn field(key: impl Into<Arc<str>>, matcher: TextMatcher) -> Self {
        Self::Field {
            key: key.into(),
            matcher,
        }
    }

    /// Creates an exact case-sensitive metadata set-membership predicate.
    #[must_use]
    pub fn field_in(
        key: impl Into<Arc<str>>,
        values: impl IntoIterator<Item = impl Into<Arc<str>>>,
    ) -> Self {
        Self::FieldIn {
            key: key.into(),
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Creates a validated metadata regular-expression predicate.
    pub fn field_regex(
        key: impl Into<Arc<str>>,
        pattern: impl Into<Arc<str>>,
        case_sensitivity: CaseSensitivity,
    ) -> LogDbResult<Self> {
        Ok(Self::FieldRegex {
            key: key.into(),
            regex: LogRegex::new(pattern, case_sensitivity)?,
        })
    }

    /// Creates an integer metadata comparison.
    #[must_use]
    pub fn field_numeric(
        key: impl Into<Arc<str>>,
        comparison: NumericComparison,
        value: i128,
    ) -> Self {
        Self::FieldNumeric {
            key: key.into(),
            comparison,
            value,
        }
    }

    /// Creates a conjunction. An empty conjunction matches every record.
    #[must_use]
    pub fn and(predicates: Vec<Self>) -> Self {
        if predicates.is_empty() {
            Self::MatchAll
        } else {
            Self::And(predicates)
        }
    }

    /// Creates a disjunction. An empty disjunction matches no records.
    #[must_use]
    pub fn or(predicates: Vec<Self>) -> Self {
        if predicates.is_empty() {
            Self::MatchNone
        } else {
            Self::Or(predicates)
        }
    }

    /// Creates a negated predicate.
    #[must_use]
    pub fn negate(predicate: Self) -> Self {
        Self::Not(Box::new(predicate))
    }
}

/// Stable sort key used by a lookup and its cursor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QuerySort {
    /// Sort by durable logical offset.
    #[default]
    Offset,
    /// Sort by event timestamp, breaking ties with durable offset.
    Timestamp,
}

/// Exclusive continuation point for deterministic lookup pagination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCursor {
    /// Event timestamp of the final result on the previous page.
    pub timestamp_unix_nanos: u64,
    /// Durable offset of the final result on the previous page.
    pub offset: LogicalOffset,
}

impl QueryCursor {
    /// Creates a continuation point from a normalized record position.
    #[must_use]
    pub const fn new(timestamp_unix_nanos: u64, offset: LogicalOffset) -> Self {
        Self {
            timestamp_unix_nanos,
            offset,
        }
    }
}

/// Boolean log lookup against one logical partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogQuery {
    /// Partition to search. Query fan-out is intentionally an explicit higher layer.
    pub topic_partition: TopicPartition,
    /// Case-insensitive message terms, combined with AND semantics.
    pub terms: Vec<Arc<str>>,
    /// Exact metadata fields, combined with AND semantics.
    pub exact_fields: Vec<MetadataField>,
    /// Additional exact Boolean predicate.
    pub predicate: LogPredicate,
    /// Inclusive lowest durable offset.
    pub start_offset: Option<LogicalOffset>,
    /// Exclusive highest durable offset.
    pub end_offset: Option<LogicalOffset>,
    /// Inclusive lowest event timestamp in Unix nanoseconds.
    pub start_timestamp_unix_nanos: Option<u64>,
    /// Exclusive highest event timestamp in Unix nanoseconds.
    pub end_timestamp_unix_nanos: Option<u64>,
    /// Maximum number of records to return.
    pub limit: Option<usize>,
    /// Durable-offset result order.
    pub order: QueryOrder,
    /// Stable record sort key.
    pub sort: QuerySort,
    /// Exclusive continuation point from the previous page.
    pub after: Option<QueryCursor>,
}

impl LogQuery {
    /// Creates an empty query for a logical partition.
    #[must_use]
    pub fn new(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            terms: Vec::new(),
            exact_fields: Vec::new(),
            predicate: LogPredicate::MatchAll,
            start_offset: None,
            end_offset: None,
            start_timestamp_unix_nanos: None,
            end_timestamp_unix_nanos: None,
            limit: None,
            order: QueryOrder::OldestFirst,
            sort: QuerySort::Offset,
            after: None,
        }
    }

    /// Adds a case-insensitive exact token constraint.
    #[must_use]
    pub fn with_term(mut self, term: impl Into<Arc<str>>) -> Self {
        self.terms.push(term.into());
        self
    }

    /// Adds an exact metadata constraint.
    #[must_use]
    pub fn with_field(mut self, key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        self.exact_fields.push(MetadataField::new(key, value));
        self
    }

    /// Adds an arbitrary predicate with AND semantics.
    #[must_use]
    pub fn with_predicate(mut self, predicate: LogPredicate) -> Self {
        self.predicate = match self.predicate {
            LogPredicate::MatchAll => predicate,
            current => LogPredicate::And(vec![current, predicate]),
        };
        self
    }

    /// Replaces the additional Boolean predicate.
    #[must_use]
    pub fn where_predicate(mut self, predicate: LogPredicate) -> Self {
        self.predicate = predicate;
        self
    }

    /// Restricts results to `start..end` durable offsets.
    #[must_use]
    pub fn with_offset_range(mut self, start: LogicalOffset, end: LogicalOffset) -> Self {
        self.start_offset = Some(start);
        self.end_offset = Some(end);
        self
    }

    /// Restricts results to `start..end` event timestamps.
    #[must_use]
    pub fn with_timestamp_range(mut self, start: u64, end: u64) -> Self {
        self.start_timestamp_unix_nanos = Some(start);
        self.end_timestamp_unix_nanos = Some(end);
        self
    }

    /// Caps the number of materialized results.
    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Returns the highest durable offsets first.
    #[must_use]
    pub fn newest_first(mut self) -> Self {
        self.order = QueryOrder::NewestFirst;
        self
    }

    /// Returns the lowest sort keys first.
    #[must_use]
    pub fn oldest_first(mut self) -> Self {
        self.order = QueryOrder::OldestFirst;
        self
    }

    /// Sorts by event timestamp with durable offset as the tie-breaker.
    #[must_use]
    pub fn sort_by_timestamp(mut self) -> Self {
        self.sort = QuerySort::Timestamp;
        self
    }

    /// Sorts by durable logical offset.
    #[must_use]
    pub fn sort_by_offset(mut self) -> Self {
        self.sort = QuerySort::Offset;
        self
    }

    /// Continues strictly after a previous page's final record in query order.
    #[must_use]
    pub fn after(mut self, cursor: QueryCursor) -> Self {
        self.after = Some(cursor);
        self
    }
}

/// A visible log record returned by a [`LogQuery`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogMatch {
    /// The durable indexed record.
    pub record: DurableLogRecord,
}
