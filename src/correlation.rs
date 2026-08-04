//! Bounded stripe-local links between logs, traces, and metrics.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    AttributeFingerprint, DurableLog, DurableMetricPoint, DurableSpan, ResourceContextId,
    ScopeContextId, TelemetryAttribute, TelemetryRecordRef, TelemetrySignal, TraceId,
};

/// Hard bounds for one owner stripe's cross-signal postings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrelationConfig {
    /// Maximum distinct trace, resource, scope, and attribute keys combined.
    pub max_keys: usize,
    /// Maximum record references retained for one correlation key.
    pub max_refs_per_key: usize,
    /// Maximum references retained across all postings.
    pub max_total_refs: usize,
}

impl Default for CorrelationConfig {
    fn default() -> Self {
        Self {
            max_keys: 65_536,
            max_refs_per_key: 4_096,
            max_total_refs: 1_048_576,
        }
    }
}

/// Snapshot of bounded correlation-index state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CorrelationStats {
    /// Distinct admitted correlation keys.
    pub keys: usize,
    /// Record references retained across all postings.
    pub refs: usize,
    /// Postings omitted because a configured bound was reached.
    pub dropped_postings: u64,
}

/// Cross-signal lookup constraints combined with AND semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationQuery {
    /// Authenticated tenant.
    pub tenant: Arc<str>,
    /// Optional trace link shared by spans, logs, and metric exemplars.
    pub trace_id: Option<TraceId>,
    /// Optional exact resource context.
    pub resource_id: Option<ResourceContextId>,
    /// Optional exact instrumentation scope.
    pub scope_id: Option<ScopeContextId>,
    /// Exact typed metadata key/value identities.
    pub attributes: Arc<Vec<AttributeFingerprint>>,
    /// String labels rendered by compatibility APIs and matched across signal scopes.
    pub labels: Arc<Vec<(Arc<str>, Arc<str>)>>,
    /// Optional signal restriction.
    pub signal: Option<TelemetrySignal>,
    /// Exclusive stable continuation point.
    pub after: Option<TelemetryRecordRef>,
    /// Maximum record references returned.
    pub limit: usize,
}

impl CorrelationQuery {
    /// Creates an unconstrained, bounded query for one tenant.
    #[must_use]
    pub fn new(tenant: impl Into<Arc<str>>) -> Self {
        Self {
            tenant: tenant.into(),
            trace_id: None,
            resource_id: None,
            scope_id: None,
            attributes: Arc::new(Vec::new()),
            labels: Arc::new(Vec::new()),
            signal: None,
            after: None,
            limit: 1_000,
        }
    }

    /// Requires an exact trace link.
    #[must_use]
    pub const fn with_trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    /// Requires an exact resource context.
    #[must_use]
    pub const fn with_resource_id(mut self, resource_id: ResourceContextId) -> Self {
        self.resource_id = Some(resource_id);
        self
    }

    /// Requires an exact instrumentation scope.
    #[must_use]
    pub const fn with_scope_id(mut self, scope_id: ScopeContextId) -> Self {
        self.scope_id = Some(scope_id);
        self
    }

    /// Requires one exact typed metadata key/value.
    #[must_use]
    pub fn with_attribute(mut self, attribute: &TelemetryAttribute) -> Self {
        Arc::make_mut(&mut self.attributes).push(attribute.fingerprint());
        self
    }

    /// Requires one exact string label wherever that key/value is attached.
    #[must_use]
    pub fn with_label(mut self, key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        let key = key.into();
        let value = value.into();
        Arc::make_mut(&mut self.attributes).push(
            TelemetryAttribute::new(
                Arc::clone(&key),
                crate::TelemetryValue::String(Arc::clone(&value)),
            )
            .fingerprint(),
        );
        Arc::make_mut(&mut self.labels).push((key, value));
        self
    }

    /// Restricts results to one signal.
    #[must_use]
    pub const fn for_signal(mut self, signal: TelemetrySignal) -> Self {
        self.signal = Some(signal);
        self
    }

    /// Continues strictly after a record reference returned by a prior page.
    #[must_use]
    pub const fn after(mut self, record_ref: TelemetryRecordRef) -> Self {
        self.after = Some(record_ref);
        self
    }

    /// Sets the bounded result count.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CorrelationKey {
    Trace(TraceId),
    Resource(ResourceContextId),
    Scope(ScopeContextId),
    Attribute(AttributeFingerprint),
}

const CORRELATION_FILTER_WORDS: usize = 16;
const CORRELATION_FILTER_HASHES: usize = 4;

/// Compact immutable filter used to prune cold signal blocks by shared
/// telemetry identity. False positives are possible; false negatives are not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelationBlockFilter {
    bits: [u64; CORRELATION_FILTER_WORDS],
}

impl Default for CorrelationBlockFilter {
    fn default() -> Self {
        Self {
            bits: [0; CORRELATION_FILTER_WORDS],
        }
    }
}

impl CorrelationBlockFilter {
    /// Builds a filter for one immutable trace block.
    #[must_use]
    pub fn for_spans(spans: &[DurableSpan]) -> Self {
        let mut filter = Self::default();
        let mut last_resource = None;
        let mut last_scope = None;
        let mut last_attributes = None;
        for span in spans {
            let tenant = Arc::clone(&span.tenant);
            let resource_key = (Arc::clone(&tenant), Arc::as_ptr(&span.resource));
            if last_resource.as_ref() != Some(&resource_key) {
                filter.insert(&tenant, CorrelationKey::Resource(span.resource_id()));
                filter.insert_attributes(&tenant, &span.resource.attributes);
                last_resource = Some(resource_key);
            }
            let scope_key = (Arc::clone(&tenant), Arc::as_ptr(&span.scope));
            if last_scope.as_ref() != Some(&scope_key) {
                filter.insert(&tenant, CorrelationKey::Scope(span.scope_id()));
                filter.insert_attributes(&tenant, &span.scope.attributes);
                last_scope = Some(scope_key);
            }
            let attribute_key = (Arc::clone(&tenant), Arc::as_ptr(&span.attributes));
            if last_attributes.as_ref() != Some(&attribute_key) {
                filter.insert_attributes(&tenant, &span.attributes);
                last_attributes = Some(attribute_key);
            }
            for event in span.events.iter() {
                filter.insert_attributes(&tenant, &event.attributes);
            }
            for link in span.links.iter() {
                filter.insert(&tenant, CorrelationKey::Trace(link.trace_id));
                filter.insert_attributes(&tenant, &link.attributes);
            }
        }
        filter
    }

    /// Builds a filter for one immutable metric chunk.
    #[must_use]
    pub fn for_metrics(points: &[DurableMetricPoint]) -> Self {
        let mut filter = Self::default();
        let mut traces = HashSet::new();
        let mut attribute_sets = HashSet::new();
        if let Some(point) = points.first() {
            let tenant = point.identity.tenant.as_ref();
            filter.insert(
                tenant,
                CorrelationKey::Resource(point.identity.resource_id()),
            );
            filter.insert(tenant, CorrelationKey::Scope(point.identity.scope_id()));
            filter.insert_attributes(tenant, &point.identity.resource.attributes);
            filter.insert_attributes(tenant, &point.identity.scope.attributes);
            filter.insert_attributes(tenant, &point.identity.point_attributes);
        }
        for point in points {
            let tenant = Arc::clone(&point.identity.tenant);
            if !point.metadata.is_empty() {
                filter.insert_attribute_set_once(&tenant, &point.metadata, &mut attribute_sets);
            }
            for exemplar in point.exemplars.iter() {
                if let Some(trace_id) = exemplar.trace_id
                    && traces.insert((Arc::clone(&tenant), trace_id))
                {
                    filter.insert(&tenant, CorrelationKey::Trace(trace_id));
                }
                filter.insert_attribute_set_once(
                    &tenant,
                    &exemplar.filtered_attributes,
                    &mut attribute_sets,
                );
            }
        }
        filter
    }

    /// Returns whether this block can satisfy all exact identities in a query.
    #[must_use]
    pub fn may_match(&self, query: &CorrelationQuery) -> bool {
        query_keys(query).all(|key| self.contains(&query.tenant, key))
    }

    /// Tests a trace block whose primary trace IDs are represented exactly by
    /// the catalog range. The Bloom filter only needs linked trace IDs.
    #[must_use]
    pub fn may_match_trace_block(
        &self,
        query: &CorrelationQuery,
        min_trace_id: u128,
        max_trace_id: u128,
    ) -> bool {
        query_keys(query).all(|key| match key {
            CorrelationKey::Trace(trace_id) => {
                let value = u128::from_be_bytes(*trace_id.as_bytes());
                (value >= min_trace_id && value <= max_trace_id)
                    || self.contains(&query.tenant, key)
            }
            _ => self.contains(&query.tenant, key),
        })
    }

    fn insert(&mut self, tenant: &str, key: CorrelationKey) {
        for bit in correlation_filter_bits(tenant, key) {
            self.bits[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    fn contains(&self, tenant: &str, key: CorrelationKey) -> bool {
        correlation_filter_bits(tenant, key)
            .into_iter()
            .all(|bit| self.bits[bit / 64] & (1_u64 << (bit % 64)) != 0)
    }

    fn insert_attributes(&mut self, tenant: &str, attributes: &[TelemetryAttribute]) {
        for attribute in attributes {
            self.insert(tenant, CorrelationKey::Attribute(attribute.fingerprint()));
        }
    }

    fn insert_attribute_set_once(
        &mut self,
        tenant: &Arc<str>,
        attributes: &Arc<Vec<TelemetryAttribute>>,
        seen: &mut HashSet<(Arc<str>, *const Vec<TelemetryAttribute>)>,
    ) {
        if seen.insert((Arc::clone(tenant), Arc::as_ptr(attributes))) {
            self.insert_attributes(tenant, attributes);
        }
    }
}

fn query_keys(query: &CorrelationQuery) -> impl Iterator<Item = CorrelationKey> + '_ {
    query
        .trace_id
        .map(CorrelationKey::Trace)
        .into_iter()
        .chain(query.resource_id.map(CorrelationKey::Resource))
        .chain(query.scope_id.map(CorrelationKey::Scope))
        .chain(
            query
                .attributes
                .iter()
                .copied()
                .map(CorrelationKey::Attribute),
        )
}

fn correlation_filter_bits(tenant: &str, key: CorrelationKey) -> [usize; 4] {
    let (tag, value) = match key {
        CorrelationKey::Trace(value) => (0_u64, u128::from_be_bytes(*value.as_bytes())),
        CorrelationKey::Resource(value) => (1, value.get()),
        CorrelationKey::Scope(value) => (2, value.get()),
        CorrelationKey::Attribute(value) => (3, value.get()),
    };
    let tenant_hash = tenant
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
        });
    let low = value as u64;
    let high = (value >> 64) as u64;
    let first = mix_filter_hash(low ^ tenant_hash ^ tag.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let stride = mix_filter_hash(
        high ^ tenant_hash.rotate_left(29) ^ tag.wrapping_mul(0xd6e8_feb8_6659_fd93),
    ) | 1;
    let bit_count = CORRELATION_FILTER_WORDS * 64;
    let mut bits = [0; CORRELATION_FILTER_HASHES];
    for (index, bit) in bits.iter_mut().enumerate() {
        *bit = first.wrapping_add((index as u64).wrapping_mul(stride)) as usize % bit_count;
    }
    bits
}

const fn mix_filter_hash(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn span_matches_correlation(query: &CorrelationQuery, span: &DurableSpan) -> bool {
    if span.tenant != query.tenant {
        return false;
    }
    let mut observed = Vec::new();
    visit_span_keys(span, |_, key| observed.push(key));
    query_keys(query).all(|key| observed.contains(&key))
}

pub(crate) fn metric_matches_correlation(
    query: &CorrelationQuery,
    point: &DurableMetricPoint,
) -> bool {
    if point.identity.tenant != query.tenant {
        return false;
    }
    let mut observed = Vec::new();
    visit_metric_keys(point, |_, key| observed.push(key));
    query_keys(query).all(|key| observed.contains(&key))
}

fn visit_span_keys(span: &DurableSpan, mut visit: impl FnMut(&str, CorrelationKey)) {
    let tenant = span.tenant.as_ref();
    visit(tenant, CorrelationKey::Trace(span.trace_id));
    visit(tenant, CorrelationKey::Resource(span.resource_id()));
    visit(tenant, CorrelationKey::Scope(span.scope_id()));
    visit_attributes(tenant, span.resource.attributes.iter(), &mut visit);
    visit_attributes(tenant, span.scope.attributes.iter(), &mut visit);
    visit_attributes(tenant, span.attributes.iter(), &mut visit);
    for event in span.events.iter() {
        visit_attributes(tenant, event.attributes.iter(), &mut visit);
    }
    for link in span.links.iter() {
        visit(tenant, CorrelationKey::Trace(link.trace_id));
        visit_attributes(tenant, link.attributes.iter(), &mut visit);
    }
}

fn visit_metric_keys(point: &DurableMetricPoint, mut visit: impl FnMut(&str, CorrelationKey)) {
    let tenant = point.identity.tenant.as_ref();
    visit(
        tenant,
        CorrelationKey::Resource(point.identity.resource_id()),
    );
    visit(tenant, CorrelationKey::Scope(point.identity.scope_id()));
    visit_attributes(
        tenant,
        point.identity.resource.attributes.iter(),
        &mut visit,
    );
    visit_attributes(tenant, point.identity.scope.attributes.iter(), &mut visit);
    visit_attributes(tenant, point.identity.point_attributes.iter(), &mut visit);
    visit_attributes(tenant, point.metadata.iter(), &mut visit);
    for exemplar in point.exemplars.iter() {
        if let Some(trace_id) = exemplar.trace_id {
            visit(tenant, CorrelationKey::Trace(trace_id));
        }
        visit_attributes(tenant, exemplar.filtered_attributes.iter(), &mut visit);
    }
}

fn visit_attributes<'a>(
    tenant: &str,
    attributes: impl Iterator<Item = &'a TelemetryAttribute>,
    visit: &mut impl FnMut(&str, CorrelationKey),
) {
    for attribute in attributes {
        visit(tenant, CorrelationKey::Attribute(attribute.fingerprint()));
    }
}

/// Preallocated, single-writer correlation postings owned by one stripe.
///
/// Reaching a bound drops only an optional navigation posting; durable signal
/// storage and each signal's exact native indexes remain authoritative.
#[derive(Debug)]
pub struct CorrelationIndex {
    config: CorrelationConfig,
    postings: HashMap<(Arc<str>, CorrelationKey), Vec<TelemetryRecordRef>>,
    refs: usize,
    dropped_postings: u64,
}

impl CorrelationIndex {
    /// Creates a bounded stripe-local index.
    #[must_use]
    pub fn new(config: CorrelationConfig) -> Self {
        Self {
            config,
            postings: HashMap::with_capacity(config.max_keys.min(4_096)),
            refs: 0,
            dropped_postings: 0,
        }
    }

    /// Indexes one durable log without retaining its body or metadata values.
    pub fn index_log(&mut self, tenant: &str, log: &DurableLog) {
        if let Some(trace_id) = log.trace_id {
            self.insert(tenant, CorrelationKey::Trace(trace_id), log.record_ref);
        }
        self.index_contexts(
            tenant,
            log.record_ref,
            log.resource_id(),
            log.scope_id(),
            log.resource.attributes.iter(),
            log.scope.attributes.iter(),
        );
        self.index_attributes(tenant, log.record_ref, log.attributes.iter());
    }

    /// Indexes one durable span, including event and link metadata.
    pub fn index_span(&mut self, span: &DurableSpan) {
        let tenant = span.tenant.as_ref();
        self.insert(
            tenant,
            CorrelationKey::Trace(span.trace_id),
            span.record_ref,
        );
        for link in span.links.iter() {
            self.insert(
                tenant,
                CorrelationKey::Trace(link.trace_id),
                span.record_ref,
            );
            self.index_attributes(tenant, span.record_ref, link.attributes.iter());
        }
        self.index_contexts(
            tenant,
            span.record_ref,
            span.resource_id(),
            span.scope_id(),
            span.resource.attributes.iter(),
            span.scope.attributes.iter(),
        );
        self.index_attributes(tenant, span.record_ref, span.attributes.iter());
        for event in span.events.iter() {
            self.index_attributes(tenant, span.record_ref, event.attributes.iter());
        }
    }

    /// Indexes one metric point and connects exemplar trace IDs directly.
    pub fn index_metric(&mut self, point: &DurableMetricPoint) {
        let tenant = point.identity.tenant.as_ref();
        for exemplar in point.exemplars.iter() {
            if let Some(trace_id) = exemplar.trace_id {
                self.insert(tenant, CorrelationKey::Trace(trace_id), point.record_ref);
            }
            self.index_attributes(
                tenant,
                point.record_ref,
                exemplar.filtered_attributes.iter(),
            );
        }
        self.index_contexts(
            tenant,
            point.record_ref,
            point.identity.resource_id(),
            point.identity.scope_id(),
            point.identity.resource.attributes.iter(),
            point.identity.scope.attributes.iter(),
        );
        self.index_attributes(
            tenant,
            point.record_ref,
            point.identity.point_attributes.iter(),
        );
        self.index_attributes(tenant, point.record_ref, point.metadata.iter());
    }

    /// Returns the deterministic intersection of every requested posting.
    #[must_use]
    pub fn query(&self, query: &CorrelationQuery) -> Vec<TelemetryRecordRef> {
        let mut keys = Vec::with_capacity(3 + query.attributes.len());
        if let Some(trace_id) = query.trace_id {
            keys.push(CorrelationKey::Trace(trace_id));
        }
        if let Some(resource_id) = query.resource_id {
            keys.push(CorrelationKey::Resource(resource_id));
        }
        if let Some(scope_id) = query.scope_id {
            keys.push(CorrelationKey::Scope(scope_id));
        }
        keys.extend(
            query
                .attributes
                .iter()
                .copied()
                .map(CorrelationKey::Attribute),
        );
        if keys.is_empty() || query.limit == 0 {
            return Vec::new();
        }
        let mut lists = keys
            .into_iter()
            .map(|key| {
                self.postings
                    .get(&(Arc::clone(&query.tenant), key))
                    .map(Vec::as_slice)
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        lists.sort_unstable_by_key(|list| list.len());
        let Some(first) = lists.first() else {
            return Vec::new();
        };
        let start = query
            .after
            .map_or(0, |after| first.partition_point(|record| *record <= after));
        let mut selected = Vec::with_capacity(query.limit.min(first.len().saturating_sub(start)));
        let mut cursors = lists[1..]
            .iter()
            .map(|incoming| {
                query.after.map_or(0, |after| {
                    incoming.partition_point(|record| *record <= after)
                })
            })
            .collect::<Vec<_>>();
        'candidate: for &record in &first[start..] {
            if query.signal.is_some_and(|signal| record.signal != signal) {
                continue;
            }
            for (incoming, cursor) in lists[1..].iter().zip(&mut cursors) {
                while incoming
                    .get(*cursor)
                    .is_some_and(|current| *current < record)
                {
                    *cursor += 1;
                }
                let Some(current) = incoming.get(*cursor) else {
                    return selected;
                };
                if *current != record {
                    continue 'candidate;
                }
            }
            selected.push(record);
            if selected.len() == query.limit {
                break;
            }
        }
        selected
    }

    /// Returns current bounds and drop diagnostics.
    #[must_use]
    pub fn stats(&self) -> CorrelationStats {
        CorrelationStats {
            keys: self.postings.len(),
            refs: self.refs,
            dropped_postings: self.dropped_postings,
        }
    }

    fn index_contexts<'a>(
        &mut self,
        tenant: &str,
        record_ref: TelemetryRecordRef,
        resource_id: ResourceContextId,
        scope_id: ScopeContextId,
        resource_attributes: impl Iterator<Item = &'a TelemetryAttribute>,
        scope_attributes: impl Iterator<Item = &'a TelemetryAttribute>,
    ) {
        self.insert(tenant, CorrelationKey::Resource(resource_id), record_ref);
        self.insert(tenant, CorrelationKey::Scope(scope_id), record_ref);
        self.index_attributes(tenant, record_ref, resource_attributes);
        self.index_attributes(tenant, record_ref, scope_attributes);
    }

    fn index_attributes<'a>(
        &mut self,
        tenant: &str,
        record_ref: TelemetryRecordRef,
        attributes: impl Iterator<Item = &'a TelemetryAttribute>,
    ) {
        for attribute in attributes {
            self.insert(
                tenant,
                CorrelationKey::Attribute(attribute.fingerprint()),
                record_ref,
            );
        }
    }

    fn insert(&mut self, tenant: &str, key: CorrelationKey, record_ref: TelemetryRecordRef) {
        let lookup = (Arc::<str>::from(tenant), key);
        if !self.postings.contains_key(&lookup) && self.postings.len() >= self.config.max_keys {
            self.dropped_postings = self.dropped_postings.saturating_add(1);
            return;
        }
        let refs = self.postings.entry(lookup).or_default();
        if refs.last().copied() == Some(record_ref) {
            return;
        }
        if refs.len() >= self.config.max_refs_per_key || self.refs >= self.config.max_total_refs {
            self.dropped_postings = self.dropped_postings.saturating_add(1);
            return;
        }
        match refs.binary_search(&record_ref) {
            Ok(_) => return,
            Err(position) => refs.insert(position, record_ref),
        }
        self.refs += 1;
    }
}

#[cfg(test)]
mod tests {
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

    use super::*;
    use crate::{
        CompressionCohortId, DurableLog, LOGS_TOPIC_ID, ResourceContext, ScopeContext,
        TelemetryValue,
    };

    #[test]
    fn shared_typed_metadata_connects_signals_without_retaining_values() {
        let service = TelemetryAttribute::new(
            "service.name",
            TelemetryValue::String(Arc::from("checkout")),
        );
        let resource = Arc::new(ResourceContext {
            attributes: Arc::new(vec![service.clone()]),
            ..ResourceContext::default()
        });
        let scope = Arc::new(ScopeContext::default());
        let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
        let mut log = DurableLog::new(
            ShardId::new(0),
            partition,
            LogicalOffset::new(7),
            42,
            "request complete",
            CompressionCohortId::new(1),
        );
        log.resource = resource;
        log.scope = scope;
        let mut index = CorrelationIndex::new(CorrelationConfig::default());
        index.index_log("tenant-a", &log);

        let refs = index.query(
            &CorrelationQuery::new("tenant-a")
                .with_resource_id(log.resource_id())
                .with_attribute(&service),
        );
        assert_eq!(refs, vec![log.record_ref]);
        assert_eq!(index.stats().refs, 3);
    }

    #[test]
    fn bounds_drop_only_optional_postings() {
        let mut index = CorrelationIndex::new(CorrelationConfig {
            max_keys: 1,
            max_refs_per_key: 1,
            max_total_refs: 1,
        });
        let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
        for offset in 0..2 {
            let log = DurableLog::new(
                ShardId::new(0),
                partition,
                LogicalOffset::new(offset),
                offset,
                "message",
                CompressionCohortId::new(1),
            );
            index.index_log("tenant-a", &log);
        }
        assert_eq!(index.stats().refs, 1);
        assert!(index.stats().dropped_postings > 0);
    }

    #[test]
    fn bounded_intersection_preserves_order_filters_and_pagination() {
        let first = TelemetryAttribute::new("shared", TelemetryValue::String(Arc::from("yes")));
        let second = TelemetryAttribute::new("region", TelemetryValue::String(Arc::from("east")));
        let partition = TopicPartition::new(LOGS_TOPIC_ID, LogicalPartitionId::new(1));
        let mut index = CorrelationIndex::new(CorrelationConfig {
            max_keys: 2,
            max_refs_per_key: 20_000,
            max_total_refs: 40_000,
        });
        for offset in 0..10_000 {
            let record = TelemetryRecordRef::for_signal(
                TelemetrySignal::Logs,
                partition,
                LogicalOffset::new(offset),
            );
            index.insert(
                "tenant-a",
                CorrelationKey::Attribute(first.fingerprint()),
                record,
            );
            if offset % 2 == 0 {
                index.insert(
                    "tenant-a",
                    CorrelationKey::Attribute(second.fingerprint()),
                    record,
                );
            }
        }
        let after = TelemetryRecordRef::for_signal(
            TelemetrySignal::Logs,
            partition,
            LogicalOffset::new(4_990),
        );
        let result = index.query(
            &CorrelationQuery::new("tenant-a")
                .with_attribute(&first)
                .with_attribute(&second)
                .for_signal(TelemetrySignal::Logs)
                .after(after)
                .with_limit(3),
        );
        assert_eq!(
            result
                .iter()
                .map(|record| record.offset.get())
                .collect::<Vec<_>>(),
            vec![4_992, 4_994, 4_996]
        );
    }
}
