use std::cmp::Ordering;

use crate::{
    CaseSensitivity, LogPredicate, LogQuery, MetadataField, NumericComparison, QueryCursor,
    QueryOrder, QuerySort, StructuralRecordView, TextMatchKind, TextMatcher, analyze_message,
};

pub(crate) struct RequiredIndexConstraints<'a> {
    pub(crate) terms: Vec<&'a str>,
    pub(crate) fields: Vec<(&'a str, &'a str)>,
    pub(crate) message_literals: Vec<&'a str>,
    pub(crate) impossible: bool,
}

impl LogQuery {
    /// Returns whether a normalized structural record satisfies every lookup
    /// range, legacy exact constraint, Boolean predicate, and page cursor.
    #[must_use]
    pub fn matches<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.offset_matches(record.structural_offset())
            && self.timestamp_matches(record.structural_timestamp_unix_nanos())
            && self.cursor_matches(record)
            && self.legacy_terms_match(record)
            && self.legacy_fields_match(record)
            && predicate_matches(&self.predicate, record)
    }

    /// Compares two matching records in the query's deterministic result order.
    #[must_use]
    pub fn compare<R: StructuralRecordView>(&self, left: &R, right: &R) -> Ordering {
        let ordering = match self.sort {
            QuerySort::Offset => left.structural_offset().cmp(&right.structural_offset()),
            QuerySort::Timestamp => (
                left.structural_timestamp_unix_nanos(),
                left.structural_offset(),
            )
                .cmp(&(
                    right.structural_timestamp_unix_nanos(),
                    right.structural_offset(),
                )),
        };
        if self.order == QueryOrder::NewestFirst {
            ordering.reverse()
        } else {
            ordering
        }
    }

    /// Creates the stable continuation point represented by one result.
    #[must_use]
    pub fn cursor_for<R: StructuralRecordView>(&self, record: &R) -> QueryCursor {
        QueryCursor::new(
            record.structural_timestamp_unix_nanos(),
            record.structural_offset(),
        )
    }

    /// Applies exact residual filtering, deterministic ordering, and the result
    /// limit to decoded candidate records from a sealed-block lookup.
    #[must_use]
    pub fn select<R: StructuralRecordView>(&self, records: impl IntoIterator<Item = R>) -> Vec<R> {
        let mut selected = records
            .into_iter()
            .filter(|record| self.matches(record))
            .collect::<Vec<_>>();
        selected.sort_unstable_by(|left, right| self.compare(left, right));
        if let Some(limit) = self.limit {
            selected.truncate(limit);
        }
        selected
    }

    /// Returns whether sealed candidates require record decoding before the
    /// query can be limited safely.
    ///
    /// Callers serving sealed blocks should stream these queries block by
    /// block instead of materializing the complete candidate set.
    #[must_use]
    pub fn requires_post_decode(&self) -> bool {
        self.start_offset.is_some()
            || self.end_offset.is_some()
            || self.start_timestamp_unix_nanos.is_some()
            || self.end_timestamp_unix_nanos.is_some()
            || self.after.is_some()
            || self.sort == QuerySort::Timestamp
            || !predicate_is_index_only_conjunction(&self.predicate)
    }

    pub(crate) fn required_index_constraints(&self) -> RequiredIndexConstraints<'_> {
        let mut constraints = RequiredIndexConstraints {
            terms: self.terms.iter().map(AsRef::as_ref).collect(),
            fields: self
                .exact_fields
                .iter()
                .map(|field| (field.key.as_ref(), field.value.as_ref()))
                .collect(),
            message_literals: Vec::new(),
            impossible: false,
        };
        collect_required_constraints(&self.predicate, &mut constraints);
        constraints
    }

    pub(crate) fn can_apply_index_limit(&self) -> bool {
        self.start_offset.is_none()
            && self.end_offset.is_none()
            && self.start_timestamp_unix_nanos.is_none()
            && self.end_timestamp_unix_nanos.is_none()
            && self.after.is_none()
            && self.sort == QuerySort::Offset
            && predicate_is_index_only_conjunction(&self.predicate)
    }

    pub(crate) fn needs_record_filter(&self) -> bool {
        self.start_timestamp_unix_nanos.is_some()
            || self.end_timestamp_unix_nanos.is_some()
            || self.after.is_some()
            || self.sort == QuerySort::Timestamp
            || !predicate_is_index_only_conjunction(&self.predicate)
    }

    pub(crate) fn matches_index_candidate<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.offset_matches(record.structural_offset())
            && self.timestamp_matches(record.structural_timestamp_unix_nanos())
            && self.cursor_matches(record)
            && predicate_matches(&self.predicate, record)
    }

    pub(crate) fn has_invalid_range(&self) -> bool {
        self.start_offset
            .zip(self.end_offset)
            .is_some_and(|(start, end)| start >= end)
            || self
                .start_timestamp_unix_nanos
                .zip(self.end_timestamp_unix_nanos)
                .is_some_and(|(start, end)| start >= end)
    }

    fn offset_matches(&self, offset: shard_stream_core::LogicalOffset) -> bool {
        self.start_offset.is_none_or(|start| offset >= start)
            && self.end_offset.is_none_or(|end| offset < end)
    }

    fn timestamp_matches(&self, timestamp_unix_nanos: u64) -> bool {
        self.start_timestamp_unix_nanos
            .is_none_or(|start| timestamp_unix_nanos >= start)
            && self
                .end_timestamp_unix_nanos
                .is_none_or(|end| timestamp_unix_nanos < end)
    }

    fn cursor_matches<R: StructuralRecordView>(&self, record: &R) -> bool {
        let Some(cursor) = self.after else {
            return true;
        };
        let ordering = match self.sort {
            QuerySort::Offset => record.structural_offset().cmp(&cursor.offset),
            QuerySort::Timestamp => (
                record.structural_timestamp_unix_nanos(),
                record.structural_offset(),
            )
                .cmp(&(cursor.timestamp_unix_nanos, cursor.offset)),
        };
        if self.order == QueryOrder::NewestFirst {
            ordering == Ordering::Less
        } else {
            ordering == Ordering::Greater
        }
    }

    fn legacy_terms_match<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.terms
            .iter()
            .all(|expected| message_has_term(record.structural_message(), expected))
    }

    fn legacy_fields_match<R: StructuralRecordView>(&self, record: &R) -> bool {
        self.exact_fields
            .iter()
            .all(|expected| field_equals(record, expected))
    }
}

fn collect_required_constraints<'a>(
    predicate: &'a LogPredicate,
    constraints: &mut RequiredIndexConstraints<'a>,
) {
    match predicate {
        LogPredicate::MatchAll => {}
        LogPredicate::MatchNone => constraints.impossible = true,
        LogPredicate::Term(term) => constraints.terms.push(term),
        LogPredicate::Field { key, matcher }
            if matcher.kind == TextMatchKind::Exact
                && matcher.case_sensitivity == CaseSensitivity::Sensitive =>
        {
            constraints.fields.push((key, &matcher.value));
        }
        LogPredicate::And(predicates) => {
            for predicate in predicates {
                collect_required_constraints(predicate, constraints);
            }
        }
        LogPredicate::Message(matcher) => {
            constraints.message_literals.push(matcher.value.as_ref());
        }
        LogPredicate::MessageRegex(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::Or(_)
        | LogPredicate::Not(_) => {}
    }
}

fn predicate_is_index_only_conjunction(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Term(_)
        | LogPredicate::Field {
            matcher:
                TextMatcher {
                    kind: TextMatchKind::Exact,
                    case_sensitivity: CaseSensitivity::Sensitive,
                    ..
                },
            ..
        } => true,
        LogPredicate::And(predicates) => predicates.iter().all(predicate_is_index_only_conjunction),
        LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::Or(_)
        | LogPredicate::Not(_) => false,
    }
}

fn predicate_matches<R: StructuralRecordView>(predicate: &LogPredicate, record: &R) -> bool {
    match predicate {
        LogPredicate::MatchAll => true,
        LogPredicate::MatchNone => false,
        LogPredicate::Term(term) => message_has_term(record.structural_message(), term),
        LogPredicate::Message(matcher) => text_matches(record.structural_message(), matcher),
        LogPredicate::MessageRegex(regex) => regex.is_match(record.structural_message()),
        LogPredicate::FieldExists(key) => {
            fields(record).any(|(observed, _)| observed == key.as_ref())
        }
        LogPredicate::Field { key, matcher } => fields(record).any(|(observed_key, value)| {
            observed_key == key.as_ref() && text_matches(value, matcher)
        }),
        LogPredicate::FieldIn { key, values } => fields(record).any(|(observed_key, value)| {
            observed_key == key.as_ref() && values.iter().any(|expected| value == expected.as_ref())
        }),
        LogPredicate::FieldRegex { key, regex } => fields(record)
            .any(|(observed_key, value)| observed_key == key.as_ref() && regex.is_match(value)),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => fields(record).any(|(observed_key, observed)| {
            observed_key == key.as_ref()
                && observed
                    .parse::<i128>()
                    .is_ok_and(|observed| compare_numeric(observed, *comparison, *value))
        }),
        LogPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches(predicate, record)),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| predicate_matches(predicate, record)),
        LogPredicate::Not(predicate) => !predicate_matches(predicate, record),
    }
}

fn fields<R: StructuralRecordView>(record: &R) -> impl Iterator<Item = (&str, &str)> {
    (0..record.structural_field_count()).filter_map(|index| record.structural_field(index))
}

fn field_equals<R: StructuralRecordView>(record: &R, expected: &MetadataField) -> bool {
    fields(record)
        .any(|(key, value)| key == expected.key.as_ref() && value == expected.value.as_ref())
}

pub(crate) fn message_has_term(message: &str, expected: &str) -> bool {
    let mut matched = false;
    let _ = analyze_message(message, &[], |term| {
        matched |= text_equal(term, expected, CaseSensitivity::Insensitive);
    });
    matched
}

fn text_matches(observed: &str, matcher: &TextMatcher) -> bool {
    match matcher.kind {
        TextMatchKind::Exact => text_equal(observed, &matcher.value, matcher.case_sensitivity),
        TextMatchKind::Contains => {
            text_contains(observed, &matcher.value, matcher.case_sensitivity)
        }
        TextMatchKind::Prefix => text_prefix(observed, &matcher.value, matcher.case_sensitivity),
        TextMatchKind::Suffix => text_suffix(observed, &matcher.value, matcher.case_sensitivity),
    }
}

fn text_equal(left: &str, right: &str, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Sensitive => left == right,
        CaseSensitivity::Insensitive if left.is_ascii() && right.is_ascii() => {
            left.eq_ignore_ascii_case(right)
        }
        CaseSensitivity::Insensitive => left.to_lowercase() == right.to_lowercase(),
    }
}

fn text_contains(haystack: &str, needle: &str, case_sensitivity: CaseSensitivity) -> bool {
    if needle.is_empty() {
        return true;
    }
    match case_sensitivity {
        CaseSensitivity::Sensitive => haystack.contains(needle),
        CaseSensitivity::Insensitive if haystack.is_ascii() && needle.is_ascii() => haystack
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle.as_bytes())),
        CaseSensitivity::Insensitive => haystack.to_lowercase().contains(&needle.to_lowercase()),
    }
}

fn text_prefix(observed: &str, prefix: &str, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Sensitive => observed.starts_with(prefix),
        CaseSensitivity::Insensitive if observed.is_ascii() && prefix.is_ascii() => observed
            .get(..prefix.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix)),
        CaseSensitivity::Insensitive => observed.to_lowercase().starts_with(&prefix.to_lowercase()),
    }
}

fn text_suffix(observed: &str, suffix: &str, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Sensitive => observed.ends_with(suffix),
        CaseSensitivity::Insensitive if observed.is_ascii() && suffix.is_ascii() => observed
            .len()
            .checked_sub(suffix.len())
            .and_then(|start| observed.get(start..))
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(suffix)),
        CaseSensitivity::Insensitive => observed.to_lowercase().ends_with(&suffix.to_lowercase()),
    }
}

const fn compare_numeric(observed: i128, comparison: NumericComparison, expected: i128) -> bool {
    match comparison {
        NumericComparison::Equal => observed == expected,
        NumericComparison::NotEqual => observed != expected,
        NumericComparison::LessThan => observed < expected,
        NumericComparison::LessThanOrEqual => observed <= expected,
        NumericComparison::GreaterThan => observed > expected,
        NumericComparison::GreaterThanOrEqual => observed >= expected,
    }
}

#[cfg(test)]
mod tests {
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use super::*;
    use crate::{CompressionCohortId, DurableLogRecord, LogRegex};

    fn record(message: &str) -> DurableLogRecord {
        DurableLogRecord::new(
            ShardId::new(1),
            TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(2)),
            LogicalOffset::new(7),
            99,
            message,
            CompressionCohortId::new(1),
        )
        .with_field("service", "Paiements")
        .with_field("status", "503")
    }

    #[test]
    fn literal_regex_numeric_and_boolean_predicates_are_exact() {
        let record = record("ÉCHEC checkout timed out");
        let predicate = LogPredicate::and(vec![
            LogPredicate::message_contains("échec checkout"),
            LogPredicate::message_regex("timed\\s+out$", CaseSensitivity::Insensitive)
                .expect("regex compiles"),
            LogPredicate::field(
                "service",
                TextMatcher::new("paie", TextMatchKind::Prefix, CaseSensitivity::Insensitive),
            ),
            LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
            LogPredicate::negate(LogPredicate::field_equals("service", "worker")),
        ]);
        assert!(predicate_matches(&predicate, &record));
        assert!(!predicate_matches(
            &LogPredicate::message_contains("success"),
            &record
        ));
        assert!(predicate_matches(
            &LogPredicate::field_numeric("status", NumericComparison::Equal, 503),
            &record
        ));
        assert!(predicate_matches(
            &LogPredicate::field_numeric("status", NumericComparison::NotEqual, 200),
            &record
        ));
    }

    #[test]
    fn invalid_regular_expressions_are_rejected_at_query_construction() {
        let error =
            LogRegex::new("(", CaseSensitivity::Sensitive).expect_err("invalid regex is rejected");
        assert!(matches!(error, crate::LogDbError::InvalidQuery(_)));
    }

    #[test]
    fn empty_literal_contains_matches_without_panicking() {
        assert!(text_contains("anything", "", CaseSensitivity::Insensitive));
    }
}
