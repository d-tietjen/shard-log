use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use pco::standalone::{simple_compress, simple_decompress_into};
use pco::{ChunkConfig, DeltaSpec, ModeSpec};
use shard_stream_core::LogicalOffset;

use crate::{DurableLogRecord, LogDbError, LogDbResult, MetadataField};

const STRUCTURAL_BLOCK_MAGIC: &[u8; 4] = b"SLOG";
const EMBEDDED_INDEX_MAGIC: &[u8; 4] = b"SLI1";
const RAW_BODY: u8 = 0;
const TEMPLATE_BODY: u8 = 1;
const DIRECT_ATTRIBUTE_VALUE: u8 = 0;
const DICTIONARY_ATTRIBUTE_VALUE: u8 = 1;
const TIMESTAMP_PCO_LEVEL: usize = 8;
const MESSAGE_LAYOUT_CACHE_ENTRIES: usize = 1_024;
const FIELD_SET_CACHE_ENTRIES: usize = 1_024;
const EMPTY_MESSAGE_LAYOUT: u32 = u32::MAX;
const EMPTY_FIELD_SET: u32 = u32::MAX;
const EMPTY_ATTRIBUTE_KEY: u32 = u32::MAX;
const LINEAR_ATTRIBUTE_DICTIONARY_LIMIT: usize = 16;
const ATTRIBUTE_KEY_CACHE_ENTRIES: usize = 32;
const SEEK_CHECKPOINT_INTERVAL: usize = 256;
const EMBEDDED_MEMBERSHIP_FILTER_WORDS: usize = 128;

type DecodedAttributeTables = (Vec<Arc<str>>, Vec<Vec<Arc<str>>>);

struct SeekableRecordLane<'a> {
    interval: usize,
    checkpoints: Vec<usize>,
    payload: &'a [u8],
}

impl SeekableRecordLane<'_> {
    fn checkpoint_payload(&self, checkpoint: usize) -> LogDbResult<&[u8]> {
        let start = *self
            .checkpoints
            .get(checkpoint)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "record lane checkpoint is missing",
            ))?;
        let end = self
            .checkpoints
            .get(checkpoint + 1)
            .copied()
            .unwrap_or(self.payload.len());
        self.payload
            .get(start..end)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "record lane checkpoint is invalid",
            ))
    }
}

/// A record reconstructed from the single current structural block layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedStructuralRecord {
    /// Durable logical offset inside the descriptor's topic partition.
    pub offset: LogicalOffset,
    /// Original event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Exact UTF-8 body reconstructed from the body lane.
    pub message: Arc<str>,
    /// Exact metadata fields reconstructed from the attribute lanes.
    pub fields: Arc<Vec<MetadataField>>,
}

#[derive(Debug)]
struct ParsedMessage<'a> {
    message: &'a [u8],
    signature_hash: u64,
    literals: Vec<Range<usize>>,
    values: Vec<Range<usize>>,
    terms: Vec<Range<usize>>,
}

#[derive(Debug)]
struct ParsedMessages<'a> {
    layouts: Vec<ParsedMessage<'a>>,
    layout_ids: Vec<u32>,
    layout_counts: Vec<usize>,
}

#[derive(Debug)]
struct TemplateEntry {
    literals: Vec<Vec<u8>>,
}

#[derive(Debug)]
struct TemplateGroup {
    representative: usize,
    count: usize,
    template_id: Option<usize>,
}

#[derive(Debug)]
struct AttributeTables {
    keys: Vec<Vec<u8>>,
    values: Vec<AttributeValueTable>,
}

#[derive(Debug)]
struct AttributeValueTable {
    entries: Vec<Arc<[u8]>>,
    resolved_entry_ids: Vec<u32>,
    dictionary_len: usize,
}

impl AttributeValueTable {
    fn dictionary(&self) -> &[Arc<[u8]>] {
        &self.entries[..self.dictionary_len]
    }

    fn resolve(&self, unresolved_id: u32) -> LogDbResult<(usize, &[u8])> {
        let entry_id = *self.resolved_entry_ids.get(unresolved_id as usize).ok_or(
            LogDbError::InvalidBlockEncoding("attribute value ID is out of range"),
        )? as usize;
        let value = self
            .entries
            .get(entry_id)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "resolved attribute value ID is out of range",
            ))?;
        Ok((entry_id, value))
    }
}

#[derive(Debug)]
struct ResolvedField {
    key_id: u32,
    value_id: u32,
}

#[derive(Debug)]
struct ResolvedFields {
    entries: Vec<ResolvedField>,
    record_ends: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct CachedAttributeKey {
    address: usize,
    length: usize,
    key_id: u32,
}

const EMPTY_CACHED_ATTRIBUTE_KEY: CachedAttributeKey = CachedAttributeKey {
    address: 0,
    length: 0,
    key_id: EMPTY_ATTRIBUTE_KEY,
};

#[derive(Debug)]
struct ParsedFieldSets {
    sets: Vec<Vec<(u32, u32)>>,
    set_ids: Vec<u32>,
    membership_filter: MembershipFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PackedIdColumn {
    bits_per_id: u8,
    values: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddedTermLocator {
    term: Arc<str>,
    layout_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddedFieldLocator {
    key: Arc<str>,
    value: Arc<str>,
    field_set_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MembershipFilter {
    words: Box<[u64]>,
}

impl MembershipFilter {
    fn new() -> Self {
        Self {
            words: vec![0; EMBEDDED_MEMBERSHIP_FILTER_WORDS].into_boxed_slice(),
        }
    }

    fn insert(&mut self, value: &[u8]) {
        self.insert_hash(membership_hash(value));
    }

    fn insert_pair(&mut self, key: &[u8], value: &[u8]) {
        self.insert_hash(membership_pair_hash(key, value));
    }

    fn insert_hash(&mut self, hash: u64) {
        let second = hash.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15;
        for candidate in [hash, second] {
            let bit =
                candidate as usize & (EMBEDDED_MEMBERSHIP_FILTER_WORDS * u64::BITS as usize - 1);
            self.words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
        }
    }

    fn might_contain(&self, value: &[u8]) -> bool {
        self.might_contain_hash(membership_hash(value))
    }

    fn might_contain_pair(&self, key: &[u8], value: &[u8]) -> bool {
        self.might_contain_hash(membership_pair_hash(key, value))
    }

    fn might_contain_hash(&self, hash: u64) -> bool {
        let second = hash.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15;
        [hash, second].into_iter().all(|candidate| {
            let bit =
                candidate as usize & (EMBEDDED_MEMBERSHIP_FILTER_WORDS * u64::BITS as usize - 1);
            self.words[bit / u64::BITS as usize] & (1u64 << (bit % u64::BITS as usize)) != 0
        })
    }
}

/// Lossless compressed-domain candidate index embedded in one structural frame.
///
/// Repeated static message terms reference compressor template IDs and repeated
/// metadata values reference field-set IDs. High-cardinality values fail open
/// through bounded membership filters and are checked after selective decode.
/// The index can produce extra candidates but cannot suppress an exact match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedFrameIndex {
    record_count: u32,
    layout_count: u32,
    layout_ids: PackedIdColumn,
    residual_layout_ids: Vec<u32>,
    term_membership: MembershipFilter,
    terms: Vec<EmbeddedTermLocator>,
    field_set_count: u32,
    field_set_ids: PackedIdColumn,
    field_membership: MembershipFilter,
    fields: Vec<EmbeddedFieldLocator>,
}

/// Structural bytes and their already-built compressed-domain index.
///
/// Live ingestion can retain `index` for immediate query visibility while
/// persisting `structural` as the authoritative compressed frame. Recovery
/// reconstructs the same index from the embedded structural section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedStructuralBlock {
    /// Exact structural bytes, including the embedded index section.
    pub structural: Vec<u8>,
    /// In-memory view produced by the same dictionary-building pass.
    pub index: EmbeddedFrameIndex,
    /// Structural bytes occupied by the embedded index section before outer compression.
    pub embedded_index_bytes: usize,
}

impl EmbeddedFrameIndex {
    /// Number of records addressed by this frame index.
    #[must_use]
    pub const fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Returns a lossless candidate superset for a case-insensitive token.
    #[must_use]
    pub fn term_candidate_ordinals(&self, term: &str) -> Vec<u32> {
        let normalized = normalize_index_term(term);
        if !self.term_membership.might_contain(normalized.as_bytes()) {
            return Vec::new();
        }
        let mut selected = self.residual_layout_ids.clone();
        if let Ok(position) = self
            .terms
            .binary_search_by(|locator| locator.term.as_ref().cmp(normalized.as_ref()))
        {
            selected.extend_from_slice(&self.terms[position].layout_ids);
        }
        selected.sort_unstable();
        selected.dedup();
        matching_packed_ids(
            &self.layout_ids,
            self.record_count,
            self.layout_count,
            &selected,
        )
        .expect("validated embedded layout column")
    }

    /// Returns a lossless candidate superset for a metadata key/value pair.
    #[must_use]
    pub fn field_candidate_ordinals(&self, key: &str, value: &str) -> Vec<u32> {
        if !self
            .field_membership
            .might_contain_pair(key.as_bytes(), value.as_bytes())
        {
            return Vec::new();
        }
        let Ok(position) = self.fields.binary_search_by(|locator| {
            locator
                .key
                .as_ref()
                .cmp(key)
                .then_with(|| locator.value.as_ref().cmp(value))
        }) else {
            return (0..self.record_count).collect();
        };
        matching_packed_ids(
            &self.field_set_ids,
            self.record_count,
            self.field_set_count,
            &self.fields[position].field_set_ids,
        )
        .expect("validated embedded field-set column")
    }

    /// Encoded bytes occupied by the embedded frame-index section.
    pub fn encoded_bytes(&self) -> LogDbResult<Vec<u8>> {
        self.encode()
    }

    fn build(
        messages: &ParsedMessages<'_>,
        template_ids: &[Option<usize>],
        attributes: &AttributeTables,
        fields: &ParsedFieldSets,
    ) -> LogDbResult<Self> {
        let record_count =
            u32::try_from(messages.layout_ids.len()).map_err(|_| LogDbError::RecordTooLarge)?;
        if template_ids.len() != messages.layouts.len()
            || fields.set_ids.len() != messages.layout_ids.len()
        {
            return Err(LogDbError::InvalidBlockEncoding(
                "embedded index column count mismatch",
            ));
        }
        let template_count = template_ids
            .iter()
            .flatten()
            .copied()
            .max()
            .map_or(0usize, |maximum| maximum.saturating_add(1));
        let fallback_layout_id =
            u32::try_from(template_count).map_err(|_| LogDbError::RecordTooLarge)?;
        let layout_count = if record_count == 0 {
            0
        } else {
            fallback_layout_id
                .checked_add(1)
                .ok_or(LogDbError::RecordTooLarge)?
        };
        let field_set_count =
            u32::try_from(fields.sets.len()).map_err(|_| LogDbError::RecordTooLarge)?;

        let mut term_layouts = HashMap::<String, Vec<u32>>::new();
        let mut term_membership = MembershipFilter::new();
        let mut residual_layout_ids = if record_count == 0 {
            Vec::new()
        } else {
            vec![fallback_layout_id]
        };
        for (layout_id, message) in messages.layouts.iter().enumerate() {
            let Some(template_id) = template_ids[layout_id] else {
                for term in &message.terms {
                    let term =
                        std::str::from_utf8(&message.message[term.clone()]).map_err(|_| {
                            LogDbError::InvalidBlockEncoding("message term is invalid UTF-8")
                        })?;
                    let normalized = normalize_index_term(term);
                    term_membership.insert(normalized.as_bytes());
                }
                continue;
            };
            let template_id = u32::try_from(template_id).map_err(|_| LogDbError::RecordTooLarge)?;
            if !message.values.is_empty() {
                residual_layout_ids.push(template_id);
            }
            for term_range in &message.terms {
                let is_dynamic = message
                    .values
                    .iter()
                    .any(|value| term_range.start < value.end && value.start < term_range.end);
                let term =
                    std::str::from_utf8(&message.message[term_range.clone()]).map_err(|_| {
                        LogDbError::InvalidBlockEncoding("message term is invalid UTF-8")
                    })?;
                let normalized = normalize_index_term(term);
                term_membership.insert(normalized.as_bytes());
                if is_dynamic {
                    continue;
                }
                let layouts = term_layouts.entry(normalized.into_owned()).or_default();
                if layouts.last().copied() != Some(template_id) {
                    layouts.push(template_id);
                }
            }
        }
        residual_layout_ids.sort_unstable();
        residual_layout_ids.dedup();
        let mut terms = term_layouts
            .into_iter()
            .map(|(term, mut layout_ids)| {
                layout_ids.sort_unstable();
                layout_ids.dedup();
                EmbeddedTermLocator {
                    term: Arc::from(term),
                    layout_ids,
                }
            })
            .collect::<Vec<_>>();
        terms.sort_unstable_by(|left, right| left.term.cmp(&right.term));

        let mut field_sets = HashMap::<(String, String), Vec<u32>>::new();
        for (field_set_id, field_set) in fields.sets.iter().enumerate() {
            let field_set_id =
                u32::try_from(field_set_id).map_err(|_| LogDbError::RecordTooLarge)?;
            for (key_id, value_id) in field_set {
                let key = attributes.keys.get(*key_id as usize).ok_or(
                    LogDbError::InvalidBlockEncoding("embedded field key ID is out of range"),
                )?;
                let value = attributes
                    .values
                    .get(*key_id as usize)
                    .and_then(|values| values.dictionary().get(*value_id as usize))
                    .ok_or(LogDbError::InvalidBlockEncoding(
                        "embedded field value ID is out of range",
                    ))?;
                let key = std::str::from_utf8(key)
                    .map_err(|_| LogDbError::InvalidBlockEncoding("field key is invalid UTF-8"))?;
                let value = std::str::from_utf8(value).map_err(|_| {
                    LogDbError::InvalidBlockEncoding("field value is invalid UTF-8")
                })?;
                let set_ids = field_sets
                    .entry((key.to_owned(), value.to_owned()))
                    .or_default();
                if set_ids.last().copied() != Some(field_set_id) {
                    set_ids.push(field_set_id);
                }
            }
        }
        let mut field_locators = field_sets
            .into_iter()
            .map(|((key, value), field_set_ids)| EmbeddedFieldLocator {
                key: Arc::from(key),
                value: Arc::from(value),
                field_set_ids,
            })
            .collect::<Vec<_>>();
        field_locators.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.value.cmp(&right.value))
        });

        Ok(Self {
            record_count,
            layout_count,
            layout_ids: pack_ids(
                &messages
                    .layout_ids
                    .iter()
                    .map(|layout_id| {
                        template_ids[*layout_id as usize]
                            .and_then(|template_id| u32::try_from(template_id).ok())
                            .unwrap_or(fallback_layout_id)
                    })
                    .collect::<Vec<_>>(),
                layout_count,
            )?,
            residual_layout_ids,
            term_membership,
            terms,
            field_set_count,
            field_set_ids: pack_ids(&fields.set_ids, field_set_count)?,
            field_membership: fields.membership_filter.clone(),
            fields: field_locators,
        })
    }

    fn encode(&self) -> LogDbResult<Vec<u8>> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(EMBEDDED_INDEX_MAGIC);
        write_varint(u64::from(self.record_count), &mut encoded);
        encode_packed_column(&self.layout_ids, self.layout_count, &mut encoded)?;
        encode_optional_sorted_ids(&self.residual_layout_ids, self.layout_count, &mut encoded)?;
        encode_membership_filter(&self.term_membership, &mut encoded);
        write_varint(
            u64::try_from(self.terms.len()).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut encoded,
        );
        for locator in &self.terms {
            append_bytes(&mut encoded, locator.term.as_bytes())?;
            encode_sorted_ids(&locator.layout_ids, self.layout_count, &mut encoded)?;
        }
        encode_packed_column(&self.field_set_ids, self.field_set_count, &mut encoded)?;
        encode_membership_filter(&self.field_membership, &mut encoded);
        write_varint(
            u64::try_from(self.fields.len()).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut encoded,
        );
        for locator in &self.fields {
            append_bytes(&mut encoded, locator.key.as_bytes())?;
            append_bytes(&mut encoded, locator.value.as_bytes())?;
            encode_sorted_ids(&locator.field_set_ids, self.field_set_count, &mut encoded)?;
        }
        Ok(encoded)
    }

    fn decode(encoded: &[u8]) -> LogDbResult<Self> {
        if encoded.get(..EMBEDDED_INDEX_MAGIC.len()) != Some(EMBEDDED_INDEX_MAGIC) {
            return Err(LogDbError::InvalidBlockEncoding(
                "missing embedded index magic",
            ));
        }
        let mut cursor = EMBEDDED_INDEX_MAGIC.len();
        let record_count = read_u32(encoded, &mut cursor)?;
        let (layout_count, layout_ids) = decode_packed_column(encoded, &mut cursor, record_count)?;
        let residual_layout_ids = decode_optional_sorted_ids(encoded, &mut cursor, layout_count)?;
        let term_membership = decode_membership_filter(encoded, &mut cursor)?;
        let term_count = read_usize(encoded, &mut cursor)?;
        ensure_count_within(
            term_count,
            encoded.len().saturating_sub(cursor),
            "embedded term count",
        )?;
        let mut terms = Vec::with_capacity(term_count);
        for _ in 0..term_count {
            let term = read_arc_str(encoded, &mut cursor)?;
            let layout_ids = decode_sorted_ids(encoded, &mut cursor, layout_count)?;
            if terms
                .last()
                .is_some_and(|previous: &EmbeddedTermLocator| previous.term >= term)
            {
                return Err(LogDbError::InvalidBlockEncoding(
                    "embedded terms are not ordered",
                ));
            }
            terms.push(EmbeddedTermLocator { term, layout_ids });
        }
        let (field_set_count, field_set_ids) =
            decode_packed_column(encoded, &mut cursor, record_count)?;
        let field_membership = decode_membership_filter(encoded, &mut cursor)?;
        let field_count = read_usize(encoded, &mut cursor)?;
        ensure_count_within(
            field_count,
            encoded.len().saturating_sub(cursor),
            "embedded field count",
        )?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let key = read_arc_str(encoded, &mut cursor)?;
            let value = read_arc_str(encoded, &mut cursor)?;
            let field_set_ids = decode_sorted_ids(encoded, &mut cursor, field_set_count)?;
            if fields
                .last()
                .is_some_and(|previous: &EmbeddedFieldLocator| {
                    (previous.key.as_ref(), previous.value.as_ref())
                        >= (key.as_ref(), value.as_ref())
                })
            {
                return Err(LogDbError::InvalidBlockEncoding(
                    "embedded fields are not ordered",
                ));
            }
            fields.push(EmbeddedFieldLocator {
                key,
                value,
                field_set_ids,
            });
        }
        require_consumed(encoded, cursor)?;
        Ok(Self {
            record_count,
            layout_count,
            layout_ids,
            residual_layout_ids,
            term_membership,
            terms,
            field_set_count,
            field_set_ids,
            field_membership,
            fields,
        })
    }
}

#[derive(Debug)]
struct AttributeValueCounts {
    entries: Vec<(Arc<[u8]>, usize)>,
    ids: Option<HashMap<Arc<[u8]>, usize>>,
    last_address: usize,
    last_length: usize,
    last_entry_id: u32,
}

impl Default for AttributeValueCounts {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            ids: None,
            last_address: 0,
            last_length: 0,
            last_entry_id: u32::MAX,
        }
    }
}

impl AttributeValueCounts {
    #[inline(always)]
    fn increment(&mut self, value: &[u8]) -> LogDbResult<(u32, bool)> {
        let address = value.as_ptr() as usize;
        if self.last_entry_id != u32::MAX
            && self.last_address == address
            && self.last_length == value.len()
        {
            let entry_id = self.last_entry_id as usize;
            self.entries[entry_id].1 = self.entries[entry_id]
                .1
                .checked_add(1)
                .ok_or(LogDbError::RecordTooLarge)?;
            return Ok((self.last_entry_id, false));
        }
        self.increment_slow(value, address)
    }

    #[inline(never)]
    fn increment_slow(&mut self, value: &[u8], address: usize) -> LogDbResult<(u32, bool)> {
        let entry_id = if let Some(ids) = &self.ids {
            ids.get(value).copied()
        } else {
            self.entries
                .iter()
                .position(|(candidate, _)| candidate.as_ref() == value)
        };
        if let Some(entry_id) = entry_id {
            self.entries[entry_id].1 = self.entries[entry_id]
                .1
                .checked_add(1)
                .ok_or(LogDbError::RecordTooLarge)?;
            let entry_id = u32::try_from(entry_id).map_err(|_| LogDbError::RecordTooLarge)?;
            self.last_address = address;
            self.last_length = value.len();
            self.last_entry_id = entry_id;
            return Ok((entry_id, false));
        }

        if self.entries.len() == LINEAR_ATTRIBUTE_DICTIONARY_LIMIT {
            self.ids = Some(
                self.entries
                    .iter()
                    .enumerate()
                    .map(|(id, (entry, _))| (entry.clone(), id))
                    .collect(),
            );
        }
        let entry_id = self.entries.len();
        let value = Arc::<[u8]>::from(value);
        self.entries.push((Arc::clone(&value), 1));
        if let Some(ids) = &mut self.ids {
            ids.insert(value, entry_id);
        }
        let entry_id = u32::try_from(entry_id).map_err(|_| LogDbError::RecordTooLarge)?;
        self.last_address = address;
        self.last_length = self.entries[entry_id as usize].0.len();
        self.last_entry_id = entry_id;
        Ok((entry_id, true))
    }

    fn into_table(self) -> LogDbResult<AttributeValueTable> {
        let entry_count = self.entries.len();
        let mut dictionary = Vec::new();
        let mut direct = Vec::new();
        for (entry_id, (value, count)) in self.entries.into_iter().enumerate() {
            if count >= 2 {
                dictionary.push((entry_id, value));
            } else {
                direct.push((entry_id, value));
            }
        }
        dictionary.sort_unstable_by(|left, right| left.1.cmp(&right.1));
        let dictionary_len = dictionary.len();
        let mut entries = Vec::with_capacity(entry_count);
        let mut resolved_entry_ids = vec![0_u32; entry_count];
        for (unresolved_id, value) in dictionary.into_iter().chain(direct) {
            let entry_id = u32::try_from(entries.len()).map_err(|_| LogDbError::RecordTooLarge)?;
            resolved_entry_ids[unresolved_id] = entry_id;
            entries.push(value);
        }
        Ok(AttributeValueTable {
            entries,
            resolved_entry_ids,
            dictionary_len,
        })
    }
}

/// Read-only normalized record fields consumed by the structural encoder.
///
/// Implementations can expose thread-local parser output directly, avoiding
/// transient [`DurableLogRecord`] and [`Arc`] allocation before a block seals.
pub trait StructuralRecordView {
    /// Durable logical offset inside the record's topic partition.
    fn structural_offset(&self) -> LogicalOffset;

    /// Event timestamp in Unix nanoseconds.
    fn structural_timestamp_unix_nanos(&self) -> u64;

    /// Exact UTF-8 log body.
    fn structural_message(&self) -> &str;

    /// Number of normalized metadata fields.
    fn structural_field_count(&self) -> usize;

    /// Metadata field at `index`, if present.
    fn structural_field(&self, index: usize) -> Option<(&str, &str)>;

    /// Visits normalized metadata fields in their durable order.
    ///
    /// Implementations with segmented storage can override this method to
    /// avoid repeatedly resolving an indexed field accessor.
    #[inline]
    fn try_for_each_structural_field<F>(&self, mut visitor: F) -> LogDbResult<()>
    where
        F: FnMut(&str, &str) -> LogDbResult<()>,
    {
        for field_index in 0..self.structural_field_count() {
            let (key, value) =
                self.structural_field(field_index)
                    .ok_or(LogDbError::InvalidBlockEncoding(
                        "record field count changed while encoding",
                    ))?;
            visitor(key, value)?;
        }
        Ok(())
    }
}

impl StructuralRecordView for DurableLogRecord {
    fn structural_offset(&self) -> LogicalOffset {
        self.record_ref.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        self.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }
}

impl StructuralRecordView for DecodedStructuralRecord {
    fn structural_offset(&self) -> LogicalOffset {
        self.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        self.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }
}

/// Returns the legacy row-byte accounting used for block sealing and storage
/// ratio reporting. The structural wire layout may be smaller or larger before
/// compression, but this byte count continues to represent the logical record
/// payload that the block stores.
pub(crate) fn row_source_bytes(record: &DurableLogRecord) -> LogDbResult<u64> {
    validate_u32_length(record.message.len())?;
    validate_u32_length(record.fields.len())?;
    let mut total = 24u64
        .checked_add(u64::try_from(record.message.len()).map_err(|_| LogDbError::RecordTooLarge)?)
        .ok_or(LogDbError::RecordTooLarge)?;
    for field in record.fields.iter() {
        validate_u32_length(field.key.len())?;
        validate_u32_length(field.value.len())?;
        total = total
            .checked_add(8)
            .and_then(|value| value.checked_add(u64::try_from(field.key.len()).ok()?))
            .and_then(|value| value.checked_add(u64::try_from(field.value.len()).ok()?))
            .ok_or(LogDbError::RecordTooLarge)?;
    }
    Ok(total)
}

/// Encodes records into the one pre-release structural block layout.
///
/// The resulting bytes must be compressed with the descriptor's codec before
/// storage and can be reconstructed with [`decode_structural_block`].
pub fn encode_structural_block(records: &[DurableLogRecord]) -> LogDbResult<Vec<u8>> {
    encode_structural_records(records)
}

/// Encodes any zero-copy normalized record view into the current structural
/// block layout.
pub fn encode_structural_records<R: StructuralRecordView>(records: &[R]) -> LogDbResult<Vec<u8>> {
    Ok(encode_indexed_structural_records(records)?.structural)
}

/// Encodes structural data and builds its compressed-domain index in the same
/// template and metadata dictionary pass.
pub fn encode_indexed_structural_records<R: StructuralRecordView>(
    records: &[R],
) -> LogDbResult<IndexedStructuralBlock> {
    let parsed_messages = parse_messages(records)?;
    let (templates, template_ids) = select_templates(&parsed_messages)?;
    let (attributes, resolved_fields, field_membership) = build_attribute_tables(records)?;

    let offsets = encode_offsets(records)?;
    let timestamps = encode_timestamps(records)?;
    let template_bytes = encode_templates(&templates)?;
    let bodies = encode_bodies(&parsed_messages, &template_ids)?;
    let attribute_tables = encode_attribute_tables(&attributes)?;
    let (fields, parsed_fields) = encode_fields(&resolved_fields, &attributes, field_membership)?;
    let index =
        EmbeddedFrameIndex::build(&parsed_messages, &template_ids, &attributes, &parsed_fields)?;
    let embedded_index = index.encode()?;
    let embedded_index_bytes = embedded_index.len();

    let mut encoded = Vec::new();
    encoded.extend_from_slice(STRUCTURAL_BLOCK_MAGIC);
    write_varint(
        u64::try_from(records.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        &mut encoded,
    );
    for section in [
        offsets,
        timestamps,
        template_bytes,
        bodies,
        attribute_tables,
        fields,
        embedded_index,
    ] {
        append_bytes(&mut encoded, &section)?;
    }
    Ok(IndexedStructuralBlock {
        structural: encoded,
        index,
        embedded_index_bytes,
    })
}

/// Opens the exact embedded index without reconstructing record bodies or
/// metadata values.
pub fn decode_embedded_frame_index(encoded: &[u8]) -> LogDbResult<EmbeddedFrameIndex> {
    let (record_count, embedded_index) = structural_sections(encoded)?;
    decode_embedded_frame_index_section(embedded_index, record_count)
}

pub(crate) fn decode_embedded_frame_index_section(
    encoded: &[u8],
    expected_record_count: usize,
) -> LogDbResult<EmbeddedFrameIndex> {
    let index = EmbeddedFrameIndex::decode(encoded)?;
    if index.record_count as usize != expected_record_count {
        return Err(LogDbError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    Ok(index)
}

/// Reconstructs exact record data from one decompressed structural block.
///
/// The caller supplies the descriptor's partition, shard, and compression
/// cohort when rebuilding a complete [`DurableLogRecord`].
pub fn decode_structural_block(encoded: &[u8]) -> LogDbResult<Vec<DecodedStructuralRecord>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(LogDbError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    let offsets = decode_offsets(read_section(encoded, &mut cursor)?, record_count)?;
    let timestamps = decode_timestamps(read_section(encoded, &mut cursor)?, record_count)?;
    let templates = decode_templates(read_section(encoded, &mut cursor)?)?;
    let messages = decode_bodies(
        read_section(encoded, &mut cursor)?,
        &templates,
        record_count,
    )?;
    let attributes = decode_attribute_tables(read_section(encoded, &mut cursor)?)?;
    let fields = decode_fields(
        read_section(encoded, &mut cursor)?,
        &attributes,
        record_count,
    )?;
    let embedded_index = EmbeddedFrameIndex::decode(read_section(encoded, &mut cursor)?)?;
    if embedded_index.record_count as usize != record_count {
        return Err(LogDbError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    if cursor != encoded.len() {
        return Err(LogDbError::InvalidBlockEncoding("trailing bytes"));
    }
    Ok(offsets
        .into_iter()
        .zip(timestamps)
        .zip(messages)
        .zip(fields)
        .map(
            |(((offset, timestamp_unix_nanos), message), fields)| DecodedStructuralRecord {
                offset,
                timestamp_unix_nanos,
                message,
                fields,
            },
        )
        .collect())
}

/// Reconstructs only selected record ordinals from one structural block.
///
/// `record_ordinals` must be strictly increasing. The enclosing zstd frame and
/// Pco timestamp page are still decoded as a unit. Body and field lanes use
/// record checkpoints, so only the checkpoint neighborhoods containing selected
/// records are scanned.
pub fn decode_structural_records(
    encoded: &[u8],
    record_ordinals: &[u32],
) -> LogDbResult<Vec<DecodedStructuralRecord>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(LogDbError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    validate_selected_ordinals(record_ordinals, record_count)?;
    let offsets = decode_offsets(read_section(encoded, &mut cursor)?, record_count)?;
    let timestamps = decode_timestamps(read_section(encoded, &mut cursor)?, record_count)?;
    let templates = decode_templates(read_section(encoded, &mut cursor)?)?;
    let messages = decode_selected_bodies(
        read_section(encoded, &mut cursor)?,
        &templates,
        record_count,
        record_ordinals,
    )?;
    let attributes = decode_attribute_tables(read_section(encoded, &mut cursor)?)?;
    let fields = decode_selected_fields(
        read_section(encoded, &mut cursor)?,
        &attributes,
        record_count,
        record_ordinals,
    )?;
    let embedded_index = EmbeddedFrameIndex::decode(read_section(encoded, &mut cursor)?)?;
    if embedded_index.record_count as usize != record_count {
        return Err(LogDbError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    if cursor != encoded.len() {
        return Err(LogDbError::InvalidBlockEncoding("trailing bytes"));
    }
    let mut decoded = Vec::with_capacity(record_ordinals.len());
    for ((record_ordinal, message), fields) in
        record_ordinals.iter().copied().zip(messages).zip(fields)
    {
        let index = usize::try_from(record_ordinal)
            .map_err(|_| LogDbError::InvalidBlockEncoding("record ordinal does not fit usize"))?;
        decoded.push(DecodedStructuralRecord {
            offset: offsets[index],
            timestamp_unix_nanos: timestamps[index],
            message,
            fields,
        });
    }
    Ok(decoded)
}

/// Returns the structural lanes whose byte vocabulary can benefit from a
/// reusable Zstandard dictionary.
///
/// Offsets and Pco-compressed timestamps are intentionally excluded: their
/// numeric encodings change from block to block and contribute little stable
/// byte vocabulary. The returned slices point at the exact bytes later seen by
/// the enclosing Zstandard frame.
pub(crate) fn dictionary_training_sections(encoded: &[u8]) -> LogDbResult<[&[u8]; 4]> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(LogDbError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let _record_count = read_usize(encoded, &mut cursor)?;
    let _offsets = read_section(encoded, &mut cursor)?;
    let _timestamps = read_section(encoded, &mut cursor)?;
    let templates = read_section(encoded, &mut cursor)?;
    let bodies = read_section(encoded, &mut cursor)?;
    let attribute_tables = read_section(encoded, &mut cursor)?;
    let fields = read_section(encoded, &mut cursor)?;
    let _embedded_index = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(LogDbError::InvalidBlockEncoding("trailing bytes"));
    }
    Ok([templates, bodies, attribute_tables, fields])
}

fn select_templates(
    messages: &ParsedMessages<'_>,
) -> LogDbResult<(Vec<TemplateEntry>, Vec<Option<usize>>)> {
    let mut hash_groups = HashMap::<u64, Vec<usize>>::new();
    let mut groups = Vec::<TemplateGroup>::new();
    let mut layout_groups = vec![None; messages.layouts.len()];
    for (layout_id, layout_group) in layout_groups.iter_mut().enumerate() {
        let message = &messages.layouts[layout_id];
        let group_id = if let Some(group_id) = *layout_group {
            group_id
        } else {
            let candidates = hash_groups.entry(message.signature_hash).or_default();
            let group_id = candidates
                .iter()
                .copied()
                .find(|group_id| {
                    same_template(&messages.layouts[groups[*group_id].representative], message)
                })
                .unwrap_or_else(|| {
                    let group_id = groups.len();
                    groups.push(TemplateGroup {
                        representative: layout_id,
                        count: 0,
                        template_id: None,
                    });
                    candidates.push(group_id);
                    group_id
                });
            *layout_group = Some(group_id);
            group_id
        };
        groups[group_id].count = groups[group_id]
            .count
            .saturating_add(messages.layout_counts[layout_id]);
    }

    let mut entries = Vec::new();
    for group in &mut groups {
        if group.count < 2 {
            continue;
        }
        let message = &messages.layouts[group.representative];
        let id = entries.len();
        entries.push(TemplateEntry {
            literals: message
                .literals
                .iter()
                .map(|range| message.message[range.clone()].to_vec())
                .collect(),
        });
        group.template_id = Some(id);
    }
    if entries.len() > usize::try_from(u32::MAX).expect("u32 fits usize") {
        return Err(LogDbError::RecordTooLarge);
    }
    let template_ids = layout_groups
        .into_iter()
        .map(|group_id| group_id.and_then(|group_id| groups[group_id].template_id))
        .collect();
    Ok((entries, template_ids))
}

fn same_template(left: &ParsedMessage<'_>, right: &ParsedMessage<'_>) -> bool {
    left.literals.len() == right.literals.len()
        && left
            .literals
            .iter()
            .zip(&right.literals)
            .all(|(left_range, right_range)| {
                left.message[left_range.clone()] == right.message[right_range.clone()]
            })
}

fn build_attribute_tables<R: StructuralRecordView>(
    records: &[R],
) -> LogDbResult<(AttributeTables, ResolvedFields, MembershipFilter)> {
    let mut keys = Vec::<Vec<u8>>::new();
    let mut key_ids = HashMap::<Vec<u8>, usize>::new();
    let mut value_counts = Vec::<AttributeValueCounts>::new();
    let mut resolved_fields = ResolvedFields {
        entries: Vec::new(),
        record_ends: Vec::with_capacity(records.len()),
    };
    let mut key_cache = [EMPTY_CACHED_ATTRIBUTE_KEY; ATTRIBUTE_KEY_CACHE_ENTRIES];
    let mut field_membership = MembershipFilter::new();
    for record in records {
        record.try_for_each_structural_field(|field_key, field_value| {
            let key = field_key.as_bytes();
            let address = key.as_ptr() as usize;
            let cache_slot =
                ((address >> 4) ^ address ^ key.len()) & (ATTRIBUTE_KEY_CACHE_ENTRIES - 1);
            let cached = key_cache[cache_slot];
            let key_id = if cached.key_id != EMPTY_ATTRIBUTE_KEY
                && cached.address == address
                && cached.length == key.len()
            {
                cached.key_id as usize
            } else {
                let key_id = match if keys.len() <= LINEAR_ATTRIBUTE_DICTIONARY_LIMIT {
                    keys.iter()
                        .position(|candidate| candidate.as_slice() == key)
                } else {
                    key_ids.get(key).copied()
                } {
                    Some(key_id) => key_id,
                    None => {
                        let key_id = keys.len();
                        let key = key.to_vec();
                        keys.push(key.clone());
                        key_ids.insert(key, key_id);
                        value_counts.push(AttributeValueCounts::default());
                        key_id
                    }
                };
                key_cache[cache_slot] = CachedAttributeKey {
                    address,
                    length: key.len(),
                    key_id: u32::try_from(key_id).map_err(|_| LogDbError::RecordTooLarge)?,
                };
                key_id
            };
            let (value_id, inserted) = value_counts[key_id].increment(field_value.as_bytes())?;
            if inserted {
                field_membership.insert_pair(key, field_value.as_bytes());
            }
            resolved_fields.entries.push(ResolvedField {
                key_id: u32::try_from(key_id).map_err(|_| LogDbError::RecordTooLarge)?,
                value_id,
            });
            Ok(())
        })?;
        resolved_fields.record_ends.push(
            u32::try_from(resolved_fields.entries.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        );
    }
    let mut values = Vec::with_capacity(keys.len());
    for counts in value_counts {
        values.push(counts.into_table()?);
    }
    Ok((
        AttributeTables { keys, values },
        resolved_fields,
        field_membership,
    ))
}

fn encode_offsets<R: StructuralRecordView>(records: &[R]) -> LogDbResult<Vec<u8>> {
    let mut encoded = Vec::new();
    let Some(first) = records.first() else {
        return Ok(encoded);
    };
    let mut previous = first.structural_offset().get();
    write_varint(previous, &mut encoded);
    for record in &records[1..] {
        let offset = record.structural_offset().get();
        let delta = offset
            .checked_sub(previous)
            .ok_or(LogDbError::InvalidBlockEncoding("offsets must increase"))?;
        write_varint(delta, &mut encoded);
        previous = offset;
    }
    Ok(encoded)
}

fn encode_timestamps<R: StructuralRecordView>(records: &[R]) -> LogDbResult<Vec<u8>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let timestamps = records
        .iter()
        .map(StructuralRecordView::structural_timestamp_unix_nanos)
        .collect::<Vec<_>>();
    simple_compress(
        &timestamps,
        &ChunkConfig::default()
            .with_compression_level(TIMESTAMP_PCO_LEVEL)
            .with_mode_spec(ModeSpec::Classic)
            .with_delta_spec(DeltaSpec::TryConsecutive(1)),
    )
    .map_err(|error| {
        LogDbError::CompressionFailed(format!("Pco timestamp encoding failed: {error}"))
    })
}

fn encode_templates(templates: &[TemplateEntry]) -> LogDbResult<Vec<u8>> {
    let mut encoded = Vec::new();
    write_varint(
        u64::try_from(templates.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        &mut encoded,
    );
    for template in templates {
        write_varint(
            u64::try_from(template.literals.len()).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut encoded,
        );
        for literal in &template.literals {
            append_bytes(&mut encoded, literal)?;
        }
    }
    Ok(encoded)
}

fn encode_bodies(
    messages: &ParsedMessages<'_>,
    template_ids: &[Option<usize>],
) -> LogDbResult<Vec<u8>> {
    let mut cached_bodies = Vec::with_capacity(messages.layouts.len());
    for (layout_id, message) in messages.layouts.iter().enumerate() {
        if messages.layout_counts[layout_id] < 2 {
            cached_bodies.push(None);
            continue;
        }
        let mut encoded = Vec::new();
        encode_body(message, &template_ids[layout_id], &mut encoded)?;
        cached_bodies.push(Some(encoded));
    }
    let mut payload = Vec::new();
    let mut checkpoints =
        Vec::with_capacity(messages.layout_ids.len().div_ceil(SEEK_CHECKPOINT_INTERVAL));
    for (record_ordinal, &layout_id) in messages.layout_ids.iter().enumerate() {
        if record_ordinal % SEEK_CHECKPOINT_INTERVAL == 0 {
            checkpoints.push(payload.len());
        }
        let layout_id = layout_id as usize;
        if let Some(cached) = &cached_bodies[layout_id] {
            payload.extend_from_slice(cached);
        } else {
            encode_body(
                &messages.layouts[layout_id],
                &template_ids[layout_id],
                &mut payload,
            )?;
        }
    }
    encode_seekable_record_lane(&payload, &checkpoints)
}

fn encode_body(
    message: &ParsedMessage<'_>,
    template_id: &Option<usize>,
    encoded: &mut Vec<u8>,
) -> LogDbResult<()> {
    match template_id {
        Some(template_id) => {
            encoded.push(TEMPLATE_BODY);
            write_varint(
                u64::try_from(*template_id).map_err(|_| LogDbError::RecordTooLarge)?,
                encoded,
            );
            for value in &message.values {
                append_bytes(encoded, &message.message[value.clone()])?;
            }
        }
        None => {
            encoded.push(RAW_BODY);
            append_bytes(encoded, message.message)?;
        }
    }
    Ok(())
}

fn encode_attribute_tables(tables: &AttributeTables) -> LogDbResult<Vec<u8>> {
    let mut encoded = Vec::new();
    write_varint(
        u64::try_from(tables.keys.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        &mut encoded,
    );
    for (key, values) in tables.keys.iter().zip(&tables.values) {
        append_bytes(&mut encoded, key)?;
        write_varint(
            u64::try_from(values.dictionary_len).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut encoded,
        );
        for value in values.dictionary() {
            append_bytes(&mut encoded, value)?;
        }
    }
    Ok(encoded)
}

fn encode_fields(
    resolved: &ResolvedFields,
    tables: &AttributeTables,
    membership_filter: MembershipFilter,
) -> LogDbResult<(Vec<u8>, ParsedFieldSets)> {
    let mut payload = Vec::new();
    let mut checkpoints = Vec::with_capacity(
        resolved
            .record_ends
            .len()
            .div_ceil(SEEK_CHECKPOINT_INTERVAL),
    );
    let mut indexed_pairs = Vec::<(u32, u32)>::new();
    let mut field_sets = Vec::<Vec<(u32, u32)>>::new();
    let mut field_set_ids = Vec::with_capacity(resolved.record_ends.len());
    let mut field_set_cache = [EMPTY_FIELD_SET; FIELD_SET_CACHE_ENTRIES];
    let mut field_start = 0_usize;
    for (record_ordinal, field_end) in resolved.record_ends.iter().copied().enumerate() {
        let field_end = field_end as usize;
        let record_fields = resolved.entries.get(field_start..field_end).ok_or(
            LogDbError::InvalidBlockEncoding("resolved record field range is invalid"),
        )?;
        indexed_pairs.clear();
        if record_ordinal % SEEK_CHECKPOINT_INTERVAL == 0 {
            checkpoints.push(payload.len());
        }
        write_varint(
            u64::try_from(record_fields.len()).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut payload,
        );
        for field in record_fields {
            let key_id = field.key_id as usize;
            write_varint(u64::from(field.key_id), &mut payload);
            let values = tables
                .values
                .get(key_id)
                .ok_or(LogDbError::InvalidBlockEncoding(
                    "resolved attribute key ID is out of range",
                ))?;
            let (value_id, field_value) = values.resolve(field.value_id)?;
            if value_id < values.dictionary_len {
                let value_id = u32::try_from(value_id).map_err(|_| LogDbError::RecordTooLarge)?;
                indexed_pairs.push((field.key_id, value_id));
                payload.push(DICTIONARY_ATTRIBUTE_VALUE);
                write_varint(u64::from(value_id), &mut payload);
            } else {
                payload.push(DIRECT_ATTRIBUTE_VALUE);
                append_bytes(&mut payload, field_value)?;
            }
        }
        field_start = field_end;
        let field_set_hash = hash_field_id_pairs(&indexed_pairs);
        let cache_slot = field_set_hash as usize & (FIELD_SET_CACHE_ENTRIES - 1);
        let cached = field_set_cache[cache_slot];
        let field_set_id =
            if cached != EMPTY_FIELD_SET && field_sets[cached as usize] == indexed_pairs {
                cached
            } else {
                let field_set_id =
                    u32::try_from(field_sets.len()).map_err(|_| LogDbError::RecordTooLarge)?;
                field_sets.push(indexed_pairs.clone());
                field_set_cache[cache_slot] = field_set_id;
                field_set_id
            };
        field_set_ids.push(field_set_id);
    }
    Ok((
        encode_seekable_record_lane(&payload, &checkpoints)?,
        ParsedFieldSets {
            sets: field_sets,
            set_ids: field_set_ids,
            membership_filter,
        },
    ))
}

fn hash_field_id_pairs(pairs: &[(u32, u32)]) -> u64 {
    let mut hash = (pairs.len() as u64).wrapping_mul(0x9e37_79b1_85eb_ca87);
    for (key_id, value_id) in pairs {
        let pair = (u64::from(*key_id) << u32::BITS) | u64::from(*value_id);
        hash ^= pair.wrapping_mul(0x517c_c1b7_2722_0a95);
        hash = hash.rotate_left(23).wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
    hash ^ (hash >> 31)
}

fn decode_offsets(encoded: &[u8], record_count: usize) -> LogDbResult<Vec<LogicalOffset>> {
    if record_count == 0 {
        if !encoded.is_empty() {
            return Err(LogDbError::InvalidBlockEncoding("offsets for empty block"));
        }
        return Ok(Vec::new());
    }
    let mut cursor = 0usize;
    let mut previous = read_varint(encoded, &mut cursor)?;
    let mut offsets = Vec::with_capacity(record_count);
    offsets.push(LogicalOffset::new(previous));
    for _ in 1..record_count {
        let delta = read_varint(encoded, &mut cursor)?;
        previous = previous
            .checked_add(delta)
            .ok_or(LogDbError::InvalidBlockEncoding("offset delta overflow"))?;
        offsets.push(LogicalOffset::new(previous));
    }
    require_consumed(encoded, cursor)?;
    Ok(offsets)
}

fn decode_timestamps(encoded: &[u8], record_count: usize) -> LogDbResult<Vec<u64>> {
    if record_count == 0 {
        if !encoded.is_empty() {
            return Err(LogDbError::InvalidBlockEncoding(
                "timestamps for empty block",
            ));
        }
        return Ok(Vec::new());
    }
    let mut timestamps = vec![0; record_count];
    let progress = simple_decompress_into(encoded, &mut timestamps)
        .map_err(|_| LogDbError::InvalidBlockEncoding("invalid Pco timestamp section"))?;
    if progress.n_processed != record_count || !progress.finished {
        return Err(LogDbError::InvalidBlockEncoding(
            "Pco timestamp count mismatch",
        ));
    }
    Ok(timestamps)
}

fn decode_templates(encoded: &[u8]) -> LogDbResult<Vec<Vec<Vec<u8>>>> {
    let mut cursor = 0usize;
    let count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        count,
        encoded.len().saturating_sub(cursor),
        "template count",
    )?;
    let mut templates = Vec::with_capacity(count);
    for _ in 0..count {
        let literal_count = read_usize(encoded, &mut cursor)?;
        if literal_count == 0 {
            return Err(LogDbError::InvalidBlockEncoding("template has no literals"));
        }
        ensure_count_within(
            literal_count,
            encoded.len().saturating_sub(cursor),
            "template literal count",
        )?;
        let mut literals = Vec::with_capacity(literal_count);
        for _ in 0..literal_count {
            literals.push(read_bytes(encoded, &mut cursor)?.to_vec());
        }
        templates.push(literals);
    }
    require_consumed(encoded, cursor)?;
    Ok(templates)
}

fn decode_bodies(
    encoded: &[u8],
    templates: &[Vec<Vec<u8>>],
    record_count: usize,
) -> LogDbResult<Vec<Arc<str>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut cursor = 0usize;
    let mut messages = Vec::with_capacity(record_count);
    let mut exact_templates = vec![None::<Arc<str>>; templates.len()];
    let mut previous = None::<Arc<str>>;
    let mut previous_encoded = None::<Range<usize>>;
    for record_ordinal in 0..record_count {
        validate_checkpoint_cursor(&lane, record_ordinal, cursor)?;
        let record_start = cursor;
        let body_kind = read_byte(lane.payload, &mut cursor)?;
        let message =
            match body_kind {
                RAW_BODY => {
                    let bytes = read_bytes(lane.payload, &mut cursor)?;
                    if let Some(previous) = &previous
                        && previous.as_bytes() == bytes
                    {
                        Arc::clone(previous)
                    } else {
                        decode_text(bytes.to_vec())?
                    }
                }
                TEMPLATE_BODY => {
                    let template_id = read_usize(lane.payload, &mut cursor)?;
                    let literals = templates
                        .get(template_id)
                        .ok_or(LogDbError::InvalidBlockEncoding("unknown template ID"))?;
                    if literals.len() == 1 {
                        if let Some(message) = &exact_templates[template_id] {
                            Arc::clone(message)
                        } else {
                            let message = decode_text(literals.first().cloned().ok_or(
                                LogDbError::InvalidBlockEncoding("template has no first literal"),
                            )?)?;
                            exact_templates[template_id] = Some(Arc::clone(&message));
                            message
                        }
                    } else {
                        for _ in &literals[1..] {
                            let _ = read_bytes(lane.payload, &mut cursor)?;
                        }
                        let record_end = cursor;
                        if let (Some(previous), Some(previous_encoded)) =
                            (&previous, &previous_encoded)
                            && lane.payload[previous_encoded.clone()]
                                == lane.payload[record_start..record_end]
                        {
                            Arc::clone(previous)
                        } else {
                            let mut replay = record_start;
                            let _ = read_byte(lane.payload, &mut replay)?;
                            let replay_template_id = read_usize(lane.payload, &mut replay)?;
                            debug_assert_eq!(replay_template_id, template_id);
                            let mut reconstructed = literals.first().cloned().ok_or(
                                LogDbError::InvalidBlockEncoding("template has no first literal"),
                            )?;
                            for literal in &literals[1..] {
                                reconstructed
                                    .extend_from_slice(read_bytes(lane.payload, &mut replay)?);
                                reconstructed.extend_from_slice(literal);
                            }
                            debug_assert_eq!(replay, record_end);
                            decode_text(reconstructed)?
                        }
                    }
                }
                _ => return Err(LogDbError::InvalidBlockEncoding("invalid body kind")),
            };
        previous = Some(Arc::clone(&message));
        previous_encoded = Some(record_start..cursor);
        messages.push(message);
    }
    require_consumed(lane.payload, cursor)?;
    Ok(messages)
}

fn decode_selected_bodies(
    encoded: &[u8],
    templates: &[Vec<Vec<u8>>],
    record_count: usize,
    selected: &[u32],
) -> LogDbResult<Vec<Arc<str>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut selected_index = 0usize;
    let mut messages = Vec::with_capacity(selected.len());
    while selected_index < selected.len() {
        let first_ordinal = usize::try_from(selected[selected_index])
            .map_err(|_| LogDbError::InvalidBlockEncoding("record ordinal does not fit usize"))?;
        let checkpoint = first_ordinal / lane.interval;
        let checkpoint_end = (checkpoint + 1)
            .saturating_mul(lane.interval)
            .min(record_count);
        let mut group_end = selected_index + 1;
        while group_end < selected.len()
            && usize::try_from(selected[group_end])
                .ok()
                .is_some_and(|ordinal| ordinal < checkpoint_end)
        {
            group_end += 1;
        }
        let final_ordinal = usize::try_from(selected[group_end - 1])
            .map_err(|_| LogDbError::InvalidBlockEncoding("record ordinal does not fit usize"))?;
        let checkpoint_payload = lane.checkpoint_payload(checkpoint)?;
        let mut cursor = 0usize;
        let mut retained = selected_index;
        for record_ordinal in checkpoint * lane.interval..=final_ordinal {
            let retain = usize::try_from(selected[retained])
                .ok()
                .is_some_and(|selected| selected == record_ordinal);
            let body_kind = read_byte(checkpoint_payload, &mut cursor)?;
            match body_kind {
                RAW_BODY => {
                    let bytes = read_bytes(checkpoint_payload, &mut cursor)?;
                    if retain {
                        messages.push(decode_text(bytes.to_vec())?);
                    }
                }
                TEMPLATE_BODY => {
                    let template_id = read_usize(checkpoint_payload, &mut cursor)?;
                    let literals = templates
                        .get(template_id)
                        .ok_or(LogDbError::InvalidBlockEncoding("unknown template ID"))?;
                    let first = literals.first().ok_or(LogDbError::InvalidBlockEncoding(
                        "template has no first literal",
                    ))?;
                    if retain {
                        let mut reconstructed = first.clone();
                        for literal in &literals[1..] {
                            reconstructed
                                .extend_from_slice(read_bytes(checkpoint_payload, &mut cursor)?);
                            reconstructed.extend_from_slice(literal);
                        }
                        messages.push(decode_text(reconstructed)?);
                    } else {
                        for _ in &literals[1..] {
                            let _ = read_bytes(checkpoint_payload, &mut cursor)?;
                        }
                    }
                }
                _ => return Err(LogDbError::InvalidBlockEncoding("invalid body kind")),
            }
            if retain {
                retained += 1;
            }
        }
        if final_ordinal + 1 == checkpoint_end {
            require_consumed(checkpoint_payload, cursor)?;
        }
        selected_index = group_end;
    }
    Ok(messages)
}

fn decode_attribute_tables(encoded: &[u8]) -> LogDbResult<DecodedAttributeTables> {
    let mut cursor = 0usize;
    let key_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        key_count,
        encoded.len().saturating_sub(cursor),
        "attribute key count",
    )?;
    let mut keys = Vec::with_capacity(key_count);
    let mut values = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        keys.push(decode_text(read_bytes(encoded, &mut cursor)?.to_vec())?);
        let value_count = read_usize(encoded, &mut cursor)?;
        ensure_count_within(
            value_count,
            encoded.len().saturating_sub(cursor),
            "attribute value count",
        )?;
        let mut dictionary = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            dictionary.push(decode_text(read_bytes(encoded, &mut cursor)?.to_vec())?);
        }
        values.push(dictionary);
    }
    require_consumed(encoded, cursor)?;
    Ok((keys, values))
}

fn decode_fields(
    encoded: &[u8],
    tables: &DecodedAttributeTables,
    record_count: usize,
) -> LogDbResult<Vec<Arc<Vec<MetadataField>>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut cursor = 0usize;
    let mut records = Vec::with_capacity(record_count);
    let mut previous = None::<Arc<Vec<MetadataField>>>;
    for record_ordinal in 0..record_count {
        validate_checkpoint_cursor(&lane, record_ordinal, cursor)?;
        let field_count = read_usize(lane.payload, &mut cursor)?;
        ensure_count_within(
            field_count,
            lane.payload.len().saturating_sub(cursor),
            "field count",
        )?;
        let mut fields = if previous
            .as_ref()
            .is_some_and(|previous| previous.len() == field_count)
        {
            None
        } else {
            Some(Vec::with_capacity(field_count))
        };
        for field_index in 0..field_count {
            let key_id = read_usize(lane.payload, &mut cursor)?;
            let key = tables
                .0
                .get(key_id)
                .ok_or(LogDbError::InvalidBlockEncoding("unknown attribute key ID"))?;
            match read_byte(lane.payload, &mut cursor)? {
                DIRECT_ATTRIBUTE_VALUE => {
                    let bytes = read_bytes(lane.payload, &mut cursor)?;
                    let value = std::str::from_utf8(bytes)
                        .map_err(|_| LogDbError::InvalidBlockEncoding("invalid UTF-8 text"))?;
                    if fields.is_none()
                        && previous.as_ref().is_some_and(|previous| {
                            previous[field_index].key.as_ref() == key.as_ref()
                                && previous[field_index].value.as_ref() == value
                        })
                    {
                        continue;
                    }
                    if fields.is_none() {
                        let mut changed = Vec::with_capacity(field_count);
                        changed.extend_from_slice(
                            &previous.as_ref().expect("equal field count")[..field_index],
                        );
                        fields = Some(changed);
                    }
                    fields
                        .as_mut()
                        .expect("changed fields are materialized")
                        .push(MetadataField {
                            key: Arc::clone(key),
                            value: Arc::from(value),
                        });
                }
                DICTIONARY_ATTRIBUTE_VALUE => {
                    let value_id = read_usize(lane.payload, &mut cursor)?;
                    let value = tables
                        .1
                        .get(key_id)
                        .and_then(|values| values.get(value_id))
                        .ok_or(LogDbError::InvalidBlockEncoding(
                            "unknown attribute value dictionary ID",
                        ))?;
                    if fields.is_none()
                        && previous.as_ref().is_some_and(|previous| {
                            previous[field_index].key.as_ref() == key.as_ref()
                                && previous[field_index].value.as_ref() == value.as_ref()
                        })
                    {
                        continue;
                    }
                    if fields.is_none() {
                        let mut changed = Vec::with_capacity(field_count);
                        changed.extend_from_slice(
                            &previous.as_ref().expect("equal field count")[..field_index],
                        );
                        fields = Some(changed);
                    }
                    fields
                        .as_mut()
                        .expect("changed fields are materialized")
                        .push(MetadataField {
                            key: Arc::clone(key),
                            value: Arc::clone(value),
                        });
                }
                _ => {
                    return Err(LogDbError::InvalidBlockEncoding(
                        "invalid attribute value kind",
                    ));
                }
            }
        }
        let fields = match fields {
            Some(fields) => Arc::new(fields),
            None => Arc::clone(
                previous
                    .as_ref()
                    .expect("equal field count has a prior record"),
            ),
        };
        previous = Some(Arc::clone(&fields));
        records.push(fields);
    }
    require_consumed(lane.payload, cursor)?;
    Ok(records)
}

fn decode_selected_fields(
    encoded: &[u8],
    tables: &DecodedAttributeTables,
    record_count: usize,
    selected: &[u32],
) -> LogDbResult<Vec<Arc<Vec<MetadataField>>>> {
    let lane = decode_seekable_record_lane(encoded, record_count)?;
    let mut selected_index = 0usize;
    let mut records = Vec::with_capacity(selected.len());
    while selected_index < selected.len() {
        let first_ordinal = usize::try_from(selected[selected_index])
            .map_err(|_| LogDbError::InvalidBlockEncoding("record ordinal does not fit usize"))?;
        let checkpoint = first_ordinal / lane.interval;
        let checkpoint_end = (checkpoint + 1)
            .saturating_mul(lane.interval)
            .min(record_count);
        let mut group_end = selected_index + 1;
        while group_end < selected.len()
            && usize::try_from(selected[group_end])
                .ok()
                .is_some_and(|ordinal| ordinal < checkpoint_end)
        {
            group_end += 1;
        }
        let final_ordinal = usize::try_from(selected[group_end - 1])
            .map_err(|_| LogDbError::InvalidBlockEncoding("record ordinal does not fit usize"))?;
        let checkpoint_payload = lane.checkpoint_payload(checkpoint)?;
        let mut cursor = 0usize;
        let mut retained = selected_index;
        for record_ordinal in checkpoint * lane.interval..=final_ordinal {
            let retain = usize::try_from(selected[retained])
                .ok()
                .is_some_and(|selected| selected == record_ordinal);
            let field_count = read_usize(checkpoint_payload, &mut cursor)?;
            ensure_count_within(
                field_count,
                checkpoint_payload.len().saturating_sub(cursor),
                "field count",
            )?;
            let mut fields = retain.then(|| Vec::with_capacity(field_count));
            for _ in 0..field_count {
                let key_id = read_usize(checkpoint_payload, &mut cursor)?;
                let key = tables
                    .0
                    .get(key_id)
                    .ok_or(LogDbError::InvalidBlockEncoding("unknown attribute key ID"))?;
                let value = match read_byte(checkpoint_payload, &mut cursor)? {
                    DIRECT_ATTRIBUTE_VALUE => {
                        let bytes = read_bytes(checkpoint_payload, &mut cursor)?;
                        retain.then(|| decode_text(bytes.to_vec())).transpose()?
                    }
                    DICTIONARY_ATTRIBUTE_VALUE => {
                        let value_id = read_usize(checkpoint_payload, &mut cursor)?;
                        let value = tables
                            .1
                            .get(key_id)
                            .and_then(|values| values.get(value_id))
                            .ok_or(LogDbError::InvalidBlockEncoding(
                                "unknown attribute value dictionary ID",
                            ))?;
                        retain.then(|| Arc::clone(value))
                    }
                    _ => {
                        return Err(LogDbError::InvalidBlockEncoding(
                            "invalid attribute value kind",
                        ));
                    }
                };
                if let Some(fields) = &mut fields {
                    fields.push(MetadataField {
                        key: Arc::clone(key),
                        value: value.expect("retained field has a decoded value"),
                    });
                }
            }
            if let Some(fields) = fields {
                records.push(Arc::new(fields));
                retained += 1;
            }
        }
        if final_ordinal + 1 == checkpoint_end {
            require_consumed(checkpoint_payload, cursor)?;
        }
        selected_index = group_end;
    }
    Ok(records)
}

fn encode_seekable_record_lane(payload: &[u8], checkpoints: &[usize]) -> LogDbResult<Vec<u8>> {
    let mut encoded = Vec::with_capacity(payload.len().saturating_add(16 + checkpoints.len() * 3));
    encoded.extend_from_slice(payload);
    let directory_start = u32::try_from(encoded.len()).map_err(|_| LogDbError::RecordTooLarge)?;
    write_varint(
        u64::try_from(SEEK_CHECKPOINT_INTERVAL).map_err(|_| LogDbError::RecordTooLarge)?,
        &mut encoded,
    );
    write_varint(
        u64::try_from(checkpoints.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        &mut encoded,
    );
    let mut previous = 0usize;
    for (index, checkpoint) in checkpoints.iter().copied().enumerate() {
        if (index == 0 && checkpoint != 0) || (index > 0 && checkpoint <= previous) {
            return Err(LogDbError::InvalidBlockEncoding(
                "record lane checkpoints are not ordered",
            ));
        }
        write_varint(
            u64::try_from(checkpoint - previous).map_err(|_| LogDbError::RecordTooLarge)?,
            &mut encoded,
        );
        previous = checkpoint;
    }
    encoded.extend_from_slice(&directory_start.to_le_bytes());
    Ok(encoded)
}

fn decode_seekable_record_lane(
    encoded: &[u8],
    record_count: usize,
) -> LogDbResult<SeekableRecordLane<'_>> {
    let footer_start =
        encoded
            .len()
            .checked_sub(size_of::<u32>())
            .ok_or(LogDbError::InvalidBlockEncoding(
                "record lane footer is truncated",
            ))?;
    let directory_start = usize::try_from(u32::from_le_bytes(
        encoded[footer_start..]
            .try_into()
            .map_err(|_| LogDbError::InvalidBlockEncoding("record lane footer is invalid"))?,
    ))
    .map_err(|_| LogDbError::InvalidBlockEncoding("record lane footer does not fit usize"))?;
    let directory =
        encoded
            .get(directory_start..footer_start)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "record lane directory is invalid",
            ))?;
    let payload = encoded
        .get(..directory_start)
        .ok_or(LogDbError::InvalidBlockEncoding(
            "record lane payload is truncated",
        ))?;
    let mut cursor = 0usize;
    let interval = read_usize(directory, &mut cursor)?;
    if interval == 0 {
        return Err(LogDbError::InvalidBlockEncoding(
            "record lane checkpoint interval is zero",
        ));
    }
    let checkpoint_count = read_usize(directory, &mut cursor)?;
    if checkpoint_count != record_count.div_ceil(interval) {
        return Err(LogDbError::InvalidBlockEncoding(
            "record lane checkpoint count mismatch",
        ));
    }
    let mut checkpoints = Vec::with_capacity(checkpoint_count);
    let mut previous = 0usize;
    for index in 0..checkpoint_count {
        let delta = read_usize(directory, &mut cursor)?;
        let checkpoint = previous
            .checked_add(delta)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "record lane checkpoint overflow",
            ))?;
        if (index == 0 && checkpoint != 0) || (index > 0 && checkpoint <= previous) {
            return Err(LogDbError::InvalidBlockEncoding(
                "record lane checkpoints are not ordered",
            ));
        }
        checkpoints.push(checkpoint);
        previous = checkpoint;
    }
    require_consumed(directory, cursor)?;
    if checkpoints
        .last()
        .is_some_and(|checkpoint| *checkpoint >= payload.len())
        || (record_count == 0 && !payload.is_empty())
    {
        return Err(LogDbError::InvalidBlockEncoding(
            "record lane checkpoint exceeds payload",
        ));
    }
    Ok(SeekableRecordLane {
        interval,
        checkpoints,
        payload,
    })
}

fn validate_checkpoint_cursor(
    lane: &SeekableRecordLane<'_>,
    record_ordinal: usize,
    cursor: usize,
) -> LogDbResult<()> {
    if record_ordinal.is_multiple_of(lane.interval)
        && lane
            .checkpoints
            .get(record_ordinal / lane.interval)
            .copied()
            != Some(cursor)
    {
        return Err(LogDbError::InvalidBlockEncoding(
            "record lane checkpoint does not point to a record",
        ));
    }
    Ok(())
}

fn parse_message(message: &[u8]) -> ParsedMessage<'_> {
    let mut literals = Vec::new();
    let mut values = Vec::new();
    let mut terms = Vec::new();
    let mut literal_start = 0usize;
    let mut term_start = None;
    let mut cursor = 0usize;
    while cursor < message.len() {
        let start = cursor;
        let token = is_template_token_byte(message[cursor]);
        while cursor < message.len() && is_template_token_byte(message[cursor]) == token {
            if message.is_ascii() {
                match (term_start, message[cursor].is_ascii_alphanumeric()) {
                    (None, true) => term_start = Some(cursor),
                    (Some(start), false) => {
                        terms.push(start..cursor);
                        term_start = None;
                    }
                    _ => {}
                }
            }
            cursor += 1;
        }
        if token && is_variable_token(&message[start..cursor]) {
            literals.push(literal_start..start);
            values.push(start..cursor);
            literal_start = cursor;
        }
    }
    if let Some(start) = term_start {
        terms.push(start..message.len());
    }
    if !message.is_ascii() {
        terms = unicode_term_ranges(message);
    }
    literals.push(literal_start..message.len());
    let signature_hash = template_hash(message, &literals);
    ParsedMessage {
        message,
        signature_hash,
        literals,
        values,
        terms,
    }
}

/// Reuses the structural encoder's lossless token classifier to render a
/// Loki-compatible pattern with dynamic values replaced by `<_>`.
#[must_use]
pub fn message_pattern(message: &str) -> String {
    let parsed = parse_message(message.as_bytes());
    if parsed.values.is_empty() {
        return message.to_owned();
    }
    let mut pattern = String::with_capacity(message.len());
    for (index, literal) in parsed.literals.iter().enumerate() {
        pattern.push_str(&message[literal.clone()]);
        if index < parsed.values.len() {
            pattern.push_str("<_>");
        }
    }
    pattern
}

fn unicode_term_ranges(message: &[u8]) -> Vec<Range<usize>> {
    let message = std::str::from_utf8(message).expect("structural messages originate as UTF-8");
    let mut terms = Vec::new();
    let mut start = None;
    for (index, character) in message.char_indices() {
        match (start, character.is_alphanumeric()) {
            (None, true) => start = Some(index),
            (Some(term_start), false) => {
                terms.push(term_start..index);
                start = None;
            }
            _ => {}
        }
    }
    if let Some(term_start) = start {
        terms.push(term_start..message.len());
    }
    terms
}

fn parse_messages<R: StructuralRecordView>(records: &[R]) -> LogDbResult<ParsedMessages<'_>> {
    let mut layouts = Vec::<ParsedMessage<'_>>::new();
    let mut layout_ids = Vec::with_capacity(records.len());
    let mut layout_counts = Vec::<usize>::new();
    let mut cache = [EMPTY_MESSAGE_LAYOUT; MESSAGE_LAYOUT_CACHE_ENTRIES];
    for record in records {
        let message = record.structural_message().as_bytes();
        let cache_slot = message_layout_cache_slot(message);
        let cached_layout = cache[cache_slot];
        let cached_layout_id = cached_layout as usize;
        let layout_id = if cached_layout != EMPTY_MESSAGE_LAYOUT
            && same_message_bytes(layouts[cached_layout_id].message, message)
        {
            cached_layout_id
        } else {
            let layout_id = layouts.len();
            let cached_layout = u32::try_from(layout_id).map_err(|_| LogDbError::RecordTooLarge)?;
            layouts.push(parse_message(message));
            layout_counts.push(0);
            cache[cache_slot] = cached_layout;
            layout_id
        };
        layout_counts[layout_id] = layout_counts[layout_id].saturating_add(1);
        layout_ids.push(u32::try_from(layout_id).map_err(|_| LogDbError::RecordTooLarge)?);
    }
    Ok(ParsedMessages {
        layouts,
        layout_ids,
        layout_counts,
    })
}

#[inline]
fn same_message_bytes(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && (left.as_ptr() == right.as_ptr() || left == right)
}

#[inline]
fn message_layout_cache_slot(message: &[u8]) -> usize {
    let length = message.len();
    if length >= 8 {
        let first = u64::from_le_bytes(message[..8].try_into().expect("length checked"));
        let last = u64::from_le_bytes(message[length - 8..].try_into().expect("length checked"));
        let mut hash =
            first ^ last.rotate_left(29) ^ (length as u64).wrapping_mul(0x9e37_79b1_85eb_ca87);
        hash ^= hash >> 32;
        return hash as usize & (MESSAGE_LAYOUT_CACHE_ENTRIES - 1);
    }
    let mut hash = (length as u64).wrapping_mul(0x9e37_79b1_85eb_ca87);
    for &byte in message {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as usize & (MESSAGE_LAYOUT_CACHE_ENTRIES - 1)
}

fn is_template_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':')
}

fn is_variable_token(token: &[u8]) -> bool {
    token.iter().any(|byte| byte.is_ascii_digit())
}

fn template_hash(message: &[u8], literals: &[Range<usize>]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in u64::try_from(literals.len())
        .expect("literal count fits u64")
        .to_le_bytes()
    {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    for literal in literals {
        for byte in u64::try_from(literal.len())
            .expect("literal length fits u64")
            .to_le_bytes()
        {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        for byte in &message[literal.clone()] {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

fn append_bytes(encoded: &mut Vec<u8>, value: &[u8]) -> LogDbResult<()> {
    write_varint(
        u64::try_from(value.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        encoded,
    );
    encoded.extend_from_slice(value);
    Ok(())
}

fn structural_sections(encoded: &[u8]) -> LogDbResult<(usize, &[u8])> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(LogDbError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    for _ in 0..6 {
        let _ = read_section(encoded, &mut cursor)?;
    }
    let embedded_index = read_section(encoded, &mut cursor)?;
    require_consumed(encoded, cursor)?;
    Ok((record_count, embedded_index))
}

fn normalize_index_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

fn membership_hash(value: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn membership_pair_hash(key: &[u8], value: &[u8]) -> u64 {
    let mut hash = membership_hash(key);
    hash = (hash ^ 0xff).wrapping_mul(0x0000_0100_0000_01b3);
    for byte in value {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn encode_membership_filter(filter: &MembershipFilter, encoded: &mut Vec<u8>) {
    for word in filter.words.iter() {
        encoded.extend_from_slice(&word.to_le_bytes());
    }
}

fn decode_membership_filter(encoded: &[u8], cursor: &mut usize) -> LogDbResult<MembershipFilter> {
    let byte_count = EMBEDDED_MEMBERSHIP_FILTER_WORDS
        .checked_mul(size_of::<u64>())
        .ok_or(LogDbError::RecordTooLarge)?;
    let end = cursor
        .checked_add(byte_count)
        .ok_or(LogDbError::InvalidBlockEncoding(
            "membership filter length overflow",
        ))?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(LogDbError::InvalidBlockEncoding(
            "truncated membership filter",
        ))?;
    *cursor = end;
    let words = bytes
        .chunks_exact(size_of::<u64>())
        .map(|word| u64::from_le_bytes(word.try_into().expect("fixed word width")))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(MembershipFilter { words })
}

fn bits_for_dictionary(dictionary_count: u32) -> u8 {
    if dictionary_count <= 1 {
        0
    } else {
        u8::try_from(u32::BITS - (dictionary_count - 1).leading_zeros())
            .expect("u32 dictionary width fits u8")
    }
}

fn pack_ids(ids: &[u32], dictionary_count: u32) -> LogDbResult<PackedIdColumn> {
    if (ids.is_empty() && dictionary_count != 0)
        || (!ids.is_empty() && (dictionary_count == 0 || dictionary_count as usize > ids.len()))
        || ids.iter().any(|id| *id >= dictionary_count)
    {
        return Err(LogDbError::InvalidBlockEncoding(
            "invalid packed ID dictionary",
        ));
    }
    let bits_per_id = bits_for_dictionary(dictionary_count);
    if bits_per_id == 0 {
        return Ok(PackedIdColumn {
            bits_per_id,
            values: Vec::new(),
        });
    }
    let bit_count = ids
        .len()
        .checked_mul(usize::from(bits_per_id))
        .ok_or(LogDbError::RecordTooLarge)?;
    let mut values = Vec::with_capacity(bit_count.div_ceil(u8::BITS as usize));
    let mut buffered = 0u64;
    let mut buffered_bits = 0u8;
    for id in ids {
        buffered |= u64::from(*id) << buffered_bits;
        buffered_bits += bits_per_id;
        while buffered_bits >= u8::BITS as u8 {
            values.push(buffered as u8);
            buffered >>= u8::BITS;
            buffered_bits -= u8::BITS as u8;
        }
    }
    if buffered_bits > 0 {
        values.push(buffered as u8);
    }
    Ok(PackedIdColumn {
        bits_per_id,
        values,
    })
}

fn packed_id(column: &PackedIdColumn, ordinal: u32) -> u32 {
    if column.bits_per_id == 0 {
        return 0;
    }
    let bit = ordinal as usize * usize::from(column.bits_per_id);
    let byte = bit / u8::BITS as usize;
    let shift = bit % u8::BITS as usize;
    let mut window = 0u64;
    for index in 0..5 {
        if let Some(value) = column.values.get(byte + index) {
            window |= u64::from(*value) << (index * u8::BITS as usize);
        }
    }
    let mask = (1u64 << column.bits_per_id) - 1;
    u32::try_from((window >> shift) & mask).expect("packed ID is at most u32")
}

fn matching_packed_ids(
    column: &PackedIdColumn,
    record_count: u32,
    dictionary_count: u32,
    selected: &[u32],
) -> LogDbResult<Vec<u32>> {
    if selected.is_empty() || record_count == 0 {
        return Ok(Vec::new());
    }
    let mut selected_ids =
        vec![false; usize::try_from(dictionary_count).map_err(|_| LogDbError::RecordTooLarge)?];
    for id in selected {
        let slot = selected_ids
            .get_mut(*id as usize)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "embedded locator ID is out of range",
            ))?;
        *slot = true;
    }
    let mut ordinals = Vec::new();
    for ordinal in 0..record_count {
        if selected_ids[packed_id(column, ordinal) as usize] {
            ordinals.push(ordinal);
        }
    }
    Ok(ordinals)
}

fn encode_packed_column(
    column: &PackedIdColumn,
    dictionary_count: u32,
    encoded: &mut Vec<u8>,
) -> LogDbResult<()> {
    write_varint(u64::from(dictionary_count), encoded);
    encoded.push(column.bits_per_id);
    append_bytes(encoded, &column.values)
}

fn decode_packed_column(
    encoded: &[u8],
    cursor: &mut usize,
    record_count: u32,
) -> LogDbResult<(u32, PackedIdColumn)> {
    let dictionary_count = read_u32(encoded, cursor)?;
    if (record_count == 0 && dictionary_count != 0)
        || (record_count != 0 && (dictionary_count == 0 || dictionary_count > record_count))
    {
        return Err(LogDbError::InvalidBlockEncoding(
            "invalid embedded dictionary count",
        ));
    }
    let bits_per_id = read_byte(encoded, cursor)?;
    if bits_per_id != bits_for_dictionary(dictionary_count) {
        return Err(LogDbError::InvalidBlockEncoding(
            "noncanonical packed ID width",
        ));
    }
    let values = read_bytes(encoded, cursor)?.to_vec();
    let expected_bytes = usize::try_from(record_count)
        .map_err(|_| LogDbError::RecordTooLarge)?
        .checked_mul(usize::from(bits_per_id))
        .ok_or(LogDbError::RecordTooLarge)?
        .div_ceil(u8::BITS as usize);
    if values.len() != expected_bytes {
        return Err(LogDbError::InvalidBlockEncoding(
            "packed ID column length mismatch",
        ));
    }
    let column = PackedIdColumn {
        bits_per_id,
        values,
    };
    for ordinal in 0..record_count {
        if packed_id(&column, ordinal) >= dictionary_count {
            return Err(LogDbError::InvalidBlockEncoding(
                "packed ID exceeds its dictionary",
            ));
        }
    }
    if let Some(last) = column.values.last()
        && expected_bytes > 0
    {
        let used_bits = usize::try_from(record_count)
            .expect("u32 record count fits usize")
            .saturating_mul(usize::from(bits_per_id))
            % u8::BITS as usize;
        if used_bits != 0 && *last >> used_bits != 0 {
            return Err(LogDbError::InvalidBlockEncoding(
                "packed ID padding is nonzero",
            ));
        }
    }
    Ok((dictionary_count, column))
}

fn encode_sorted_ids(ids: &[u32], upper_bound: u32, encoded: &mut Vec<u8>) -> LogDbResult<()> {
    if ids.is_empty()
        || ids.windows(2).any(|adjacent| adjacent[0] >= adjacent[1])
        || ids.last().is_some_and(|id| *id >= upper_bound)
    {
        return Err(LogDbError::InvalidBlockEncoding(
            "embedded locator IDs are invalid",
        ));
    }
    write_varint(
        u64::try_from(ids.len()).map_err(|_| LogDbError::RecordTooLarge)?,
        encoded,
    );
    write_varint(u64::from(ids[0]), encoded);
    for adjacent in ids.windows(2) {
        write_varint(u64::from(adjacent[1] - adjacent[0]), encoded);
    }
    Ok(())
}

fn encode_optional_sorted_ids(
    ids: &[u32],
    upper_bound: u32,
    encoded: &mut Vec<u8>,
) -> LogDbResult<()> {
    if ids.is_empty() {
        write_varint(0, encoded);
        Ok(())
    } else {
        encode_sorted_ids(ids, upper_bound, encoded)
    }
}

fn decode_optional_sorted_ids(
    encoded: &[u8],
    cursor: &mut usize,
    upper_bound: u32,
) -> LogDbResult<Vec<u32>> {
    let count = read_usize(encoded, cursor)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    decode_sorted_ids_with_count(encoded, cursor, upper_bound, count)
}

fn decode_sorted_ids(
    encoded: &[u8],
    cursor: &mut usize,
    upper_bound: u32,
) -> LogDbResult<Vec<u32>> {
    let count = read_usize(encoded, cursor)?;
    decode_sorted_ids_with_count(encoded, cursor, upper_bound, count)
}

fn decode_sorted_ids_with_count(
    encoded: &[u8],
    cursor: &mut usize,
    upper_bound: u32,
    count: usize,
) -> LogDbResult<Vec<u32>> {
    if count == 0 || count > encoded.len().saturating_sub(*cursor) {
        return Err(LogDbError::InvalidBlockEncoding(
            "invalid embedded locator count",
        ));
    }
    let mut ids = Vec::with_capacity(count);
    let first = read_u32(encoded, cursor)?;
    if first >= upper_bound {
        return Err(LogDbError::InvalidBlockEncoding(
            "embedded locator ID is out of range",
        ));
    }
    ids.push(first);
    for _ in 1..count {
        let delta = read_u32(encoded, cursor)?;
        if delta == 0 {
            return Err(LogDbError::InvalidBlockEncoding(
                "embedded locator IDs are not ordered",
            ));
        }
        let next = ids
            .last()
            .copied()
            .and_then(|previous| previous.checked_add(delta))
            .filter(|next| *next < upper_bound)
            .ok_or(LogDbError::InvalidBlockEncoding(
                "embedded locator ID is out of range",
            ))?;
        ids.push(next);
    }
    Ok(ids)
}

fn read_u32(encoded: &[u8], cursor: &mut usize) -> LogDbResult<u32> {
    u32::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| LogDbError::InvalidBlockEncoding("value does not fit u32"))
}

fn read_arc_str(encoded: &[u8], cursor: &mut usize) -> LogDbResult<Arc<str>> {
    let value = std::str::from_utf8(read_bytes(encoded, cursor)?)
        .map_err(|_| LogDbError::InvalidBlockEncoding("embedded string is invalid UTF-8"))?;
    Ok(Arc::from(value))
}

#[inline]
fn write_varint(value: u64, encoded: &mut Vec<u8>) {
    if value < 0x80 {
        encoded.push(value as u8);
    } else {
        write_multibyte_varint(value, encoded);
    }
}

#[inline(never)]
fn write_multibyte_varint(mut value: u64, encoded: &mut Vec<u8>) {
    debug_assert!(value >= 0x80);
    while value >= 0x80 {
        encoded.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    encoded.push(value as u8);
}

fn read_section<'a>(encoded: &'a [u8], cursor: &mut usize) -> LogDbResult<&'a [u8]> {
    read_bytes(encoded, cursor)
}

fn read_bytes<'a>(encoded: &'a [u8], cursor: &mut usize) -> LogDbResult<&'a [u8]> {
    let length = read_usize(encoded, cursor)?;
    let end = cursor
        .checked_add(length)
        .ok_or(LogDbError::InvalidBlockEncoding("section length overflow"))?;
    let value = encoded
        .get(*cursor..end)
        .ok_or(LogDbError::InvalidBlockEncoding("truncated section"))?;
    *cursor = end;
    Ok(value)
}

fn read_usize(encoded: &[u8], cursor: &mut usize) -> LogDbResult<usize> {
    usize::try_from(read_varint(encoded, cursor)?)
        .map_err(|_| LogDbError::InvalidBlockEncoding("length does not fit usize"))
}

fn ensure_count_within(count: usize, remaining: usize, label: &'static str) -> LogDbResult<()> {
    if count <= remaining {
        Ok(())
    } else {
        Err(LogDbError::InvalidBlockEncoding(label))
    }
}

fn validate_selected_ordinals(selected: &[u32], record_count: usize) -> LogDbResult<()> {
    let mut previous = None;
    for ordinal in selected.iter().copied() {
        let ordinal = usize::try_from(ordinal)
            .map_err(|_| LogDbError::InvalidBlockEncoding("record ordinal does not fit usize"))?;
        if ordinal >= record_count || previous.is_some_and(|previous| previous >= ordinal) {
            return Err(LogDbError::InvalidBlockEncoding(
                "selected record ordinals are not strictly increasing",
            ));
        }
        previous = Some(ordinal);
    }
    Ok(())
}

fn read_varint(encoded: &[u8], cursor: &mut usize) -> LogDbResult<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_byte(encoded, cursor)?;
        let payload = u64::from(byte & 0x7f);
        if shift > 63 || (shift == 63 && payload > 1) {
            return Err(LogDbError::InvalidBlockEncoding("varint overflow"));
        }
        value |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift = shift.saturating_add(7);
        if shift > 63 {
            return Err(LogDbError::InvalidBlockEncoding("varint is too long"));
        }
    }
}

fn read_byte(encoded: &[u8], cursor: &mut usize) -> LogDbResult<u8> {
    let byte = *encoded
        .get(*cursor)
        .ok_or(LogDbError::InvalidBlockEncoding("truncated block"))?;
    *cursor = cursor
        .checked_add(1)
        .ok_or(LogDbError::InvalidBlockEncoding("cursor overflow"))?;
    Ok(byte)
}

fn decode_text(bytes: Vec<u8>) -> LogDbResult<Arc<str>> {
    String::from_utf8(bytes)
        .map(Arc::<str>::from)
        .map_err(|_| LogDbError::InvalidBlockEncoding("invalid UTF-8 text"))
}

fn require_consumed(encoded: &[u8], cursor: usize) -> LogDbResult<()> {
    if cursor == encoded.len() {
        Ok(())
    } else {
        Err(LogDbError::InvalidBlockEncoding("trailing component bytes"))
    }
}

fn validate_u32_length(length: usize) -> LogDbResult<()> {
    u32::try_from(length)
        .map(|_| ())
        .map_err(|_| LogDbError::RecordTooLarge)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use super::*;
    use crate::{CompressionCohortId, LogQuery};

    fn record(offset: u64, message: &str) -> DurableLogRecord {
        DurableLogRecord::new(
            ShardId::new(7),
            TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3)),
            LogicalOffset::new(offset),
            10_000 + offset,
            message,
            CompressionCohortId::new(4),
        )
        .with_field("service.name", "billing")
        .with_field("severity", "ERROR")
    }

    struct CountingRecord<'a> {
        record: &'a DurableLogRecord,
        field_reads: &'a Cell<usize>,
    }

    impl StructuralRecordView for CountingRecord<'_> {
        fn structural_offset(&self) -> LogicalOffset {
            self.record.record_ref.offset
        }

        fn structural_timestamp_unix_nanos(&self) -> u64 {
            self.record.timestamp_unix_nanos
        }

        fn structural_message(&self) -> &str {
            &self.record.message
        }

        fn structural_field_count(&self) -> usize {
            self.record.fields.len()
        }

        fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
            self.field_reads
                .set(self.field_reads.get().saturating_add(1));
            self.record
                .fields
                .get(index)
                .map(|field| (field.key.as_ref(), field.value.as_ref()))
        }
    }

    fn legacy_rows(records: &[DurableLogRecord]) -> Vec<u8> {
        let capacity = records
            .iter()
            .map(row_source_bytes)
            .collect::<LogDbResult<Vec<_>>>()
            .expect("logical source sizes fit")
            .into_iter()
            .sum::<u64>();
        let mut encoded = Vec::with_capacity(usize::try_from(capacity).expect("test fits"));
        for record in records {
            encoded.extend_from_slice(&record.record_ref.offset.get().to_le_bytes());
            encoded.extend_from_slice(&record.timestamp_unix_nanos.to_le_bytes());
            append_legacy_bytes(&mut encoded, record.message.as_bytes());
            encoded.extend_from_slice(
                &u32::try_from(record.fields.len())
                    .expect("field count fits")
                    .to_le_bytes(),
            );
            for field in record.fields.iter() {
                append_legacy_bytes(&mut encoded, field.key.as_bytes());
                append_legacy_bytes(&mut encoded, field.value.as_bytes());
            }
        }
        encoded
    }

    fn append_legacy_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
        encoded.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("test value fits")
                .to_le_bytes(),
        );
        encoded.extend_from_slice(bytes);
    }

    fn encode_timestamp_values(values: &[u64]) -> Vec<u8> {
        let records = values
            .iter()
            .copied()
            .enumerate()
            .map(|(offset, timestamp)| {
                let mut record = record(
                    u64::try_from(offset).expect("test offset fits"),
                    "timestamp test",
                );
                record.timestamp_unix_nanos = timestamp;
                record
            })
            .collect::<Vec<_>>();
        encode_timestamps(&records).expect("timestamps encode")
    }

    #[test]
    fn structural_block_round_trips_exact_human_readable_records() {
        let records = vec![
            record(4, "ERROR request_id=req-1001 retry=0 card declined"),
            record(5, "ERROR request_id=req-1002 retry=1 card declined"),
            record(7, "ERROR request_id=req-1003 retry=2 card declined"),
        ];
        let encoded = encode_structural_block(&records).expect("block encodes");
        let decoded = decode_structural_block(&encoded).expect("block decodes");
        assert_eq!(decoded.len(), records.len());
        for (decoded, record) in decoded.iter().zip(&records) {
            assert_eq!(decoded.offset, record.record_ref.offset);
            assert_eq!(decoded.timestamp_unix_nanos, record.timestamp_unix_nanos);
            assert_eq!(decoded.message, record.message);
            assert_eq!(decoded.fields.as_ref(), record.fields.as_ref());
        }
    }

    #[test]
    fn structural_field_plan_reads_each_input_field_once() {
        let records = vec![
            record(4, "ERROR request_id=req-1001 retry=0 card declined")
                .with_field("trace.id", "aaa"),
            record(5, "ERROR request_id=req-1002 retry=1 card declined")
                .with_field("trace.id", "bbb"),
            record(7, "ERROR request_id=req-1003 retry=2 card declined")
                .with_field("trace.id", "aaa"),
        ];
        let expected = encode_indexed_structural_records(&records)
            .expect("owned records encode")
            .structural;
        let field_reads = Cell::new(0);
        let counting = records
            .iter()
            .map(|record| CountingRecord {
                record,
                field_reads: &field_reads,
            })
            .collect::<Vec<_>>();
        let encoded = encode_indexed_structural_records(&counting)
            .expect("counting records encode")
            .structural;
        assert_eq!(encoded, expected);
        assert_eq!(
            field_reads.get(),
            records
                .iter()
                .map(|record| record.fields.len())
                .sum::<usize>()
        );
    }

    #[test]
    fn embedded_index_candidates_select_exact_static_dynamic_and_field_matches() {
        let records = vec![
            record(0, "ERROR request_id=req-1001 card declined").with_field("trace.id", "aaa"),
            record(1, "INFO request_id=req-1002 card accepted").with_field("trace.id", "bbb"),
            record(2, "ERROR request_id=req-1003 card declined").with_field("trace.id", "ccc"),
            record(3, "ERROR request_id=req-1004 card declined")
                .with_field("service.name", "worker")
                .with_field("trace.id", "ddd"),
        ];
        let indexed =
            encode_indexed_structural_records(&records).expect("indexed structural block encodes");
        let recovered =
            decode_embedded_frame_index(&indexed.structural).expect("embedded index recovers");
        assert_eq!(recovered, indexed.index);

        let assert_query = |candidate_ordinals: Vec<u32>, query: LogQuery| {
            let candidates = decode_structural_records(&indexed.structural, &candidate_ordinals)
                .expect("candidates selectively decode");
            let selected = query
                .select(candidates)
                .into_iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>();
            let expected = query
                .select(decode_structural_block(&indexed.structural).expect("full block decodes"))
                .into_iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>();
            assert_eq!(selected, expected);
        };

        assert_query(
            indexed.index.term_candidate_ordinals("error"),
            LogQuery::new(records[0].record_ref.topic_partition).with_term("error"),
        );
        assert_query(
            indexed.index.term_candidate_ordinals("req"),
            LogQuery::new(records[0].record_ref.topic_partition).with_term("req"),
        );
        assert_query(
            indexed.index.term_candidate_ordinals("1002"),
            LogQuery::new(records[0].record_ref.topic_partition).with_term("1002"),
        );
        assert_query(
            indexed
                .index
                .field_candidate_ordinals("service.name", "billing"),
            LogQuery::new(records[0].record_ref.topic_partition)
                .with_field("service.name", "billing"),
        );
        assert_query(
            indexed.index.field_candidate_ordinals("trace.id", "bbb"),
            LogQuery::new(records[0].record_ref.topic_partition).with_field("trace.id", "bbb"),
        );
        assert!(
            indexed
                .index
                .term_candidate_ordinals("definitely_absent_987654321")
                .is_empty()
        );
    }

    #[test]
    fn selective_decode_matches_full_decode_without_allocating_other_records() {
        let records = (0..1_000u64)
            .map(|offset| {
                record(
                    offset,
                    &format!(
                        "ERROR request_id=req-{offset:08} retry={} card declined",
                        offset % 4
                    ),
                )
                .with_field("request.id", format!("req-{offset:08}"))
            })
            .collect::<Vec<_>>();
        let encoded = encode_structural_block(&records).expect("block encodes");
        let full = decode_structural_block(&encoded).expect("full block decodes");
        let selected_ordinals = [0, 7, 500, 999];
        let selected =
            decode_structural_records(&encoded, &selected_ordinals).expect("selection decodes");
        assert_eq!(
            selected,
            selected_ordinals
                .iter()
                .map(|ordinal| full[usize::try_from(*ordinal).expect("ordinal fits")].clone())
                .collect::<Vec<_>>()
        );
        assert!(decode_structural_records(&encoded, &[7, 7]).is_err());
        assert!(decode_structural_records(&encoded, &[1_000]).is_err());
    }

    #[test]
    fn dictionary_training_sections_ignore_offsets_and_pco_timestamps() {
        let first = vec![
            record(0, "ERROR request_id=req-1001 card declined"),
            record(1, "ERROR request_id=req-1002 card declined"),
        ];
        let mut second = first.clone();
        second[0].record_ref.offset = LogicalOffset::new(100);
        second[1].record_ref.offset = LogicalOffset::new(200);
        second[0].timestamp_unix_nanos = u64::MAX - 1;
        second[1].timestamp_unix_nanos = 17;

        let first_encoded = encode_structural_block(&first).expect("first block encodes");
        let second_encoded = encode_structural_block(&second).expect("second block encodes");
        assert_ne!(first_encoded, second_encoded);
        assert_eq!(
            dictionary_training_sections(&first_encoded).expect("first sections parse"),
            dictionary_training_sections(&second_encoded).expect("second sections parse")
        );
    }

    #[test]
    fn varied_utf8_messages_round_trip_byte_identically() {
        let fragments = [
            "東京",
            "Échec",
            "🚀",
            "ключ",
            "مرحبا",
            "line\nbreak",
            "\0",
            "/api/v1",
            "42",
            "punct:=,[]",
        ];
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        let records = (0u64..256)
            .map(|offset| {
                let mut message = String::new();
                for _ in 0..24 {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let index = usize::try_from(
                        state % u64::try_from(fragments.len()).expect("fragment count fits"),
                    )
                    .expect("fragment index fits");
                    message.push_str(fragments[index]);
                    message.push(' ');
                }
                record(offset, &message)
                    .with_field("unicode.tenant", format!("顧客-{}", offset % 7))
            })
            .collect::<Vec<_>>();

        let encoded = encode_structural_block(&records).expect("UTF-8 block encodes");
        let decoded = decode_structural_block(&encoded).expect("UTF-8 block decodes");
        assert_eq!(decoded.len(), records.len());
        for (decoded, record) in decoded.iter().zip(records) {
            assert_eq!(decoded.offset, record.record_ref.offset);
            assert_eq!(decoded.timestamp_unix_nanos, record.timestamp_unix_nanos);
            assert_eq!(decoded.message, record.message);
            assert_eq!(decoded.fields.as_ref(), record.fields.as_ref());
        }
    }

    #[test]
    fn token_templates_retain_static_terms_and_only_extract_dynamic_tokens() {
        let message = parse_message(b"ERROR request_id=req-1001 retry=2 card declined");
        let values = message
            .values
            .iter()
            .map(|range| message.message[range.clone()].to_vec())
            .collect::<Vec<_>>();
        let literals = message
            .literals
            .iter()
            .map(|range| message.message[range.clone()].to_vec())
            .collect::<Vec<_>>();
        assert_eq!(values, vec![b"req-1001".to_vec(), b"2".to_vec()]);
        assert_eq!(
            literals,
            vec![
                b"ERROR request_id=".to_vec(),
                b" retry=".to_vec(),
                b" card declined".to_vec(),
            ]
        );
    }

    #[test]
    fn rendered_patterns_use_the_structural_dynamic_value_classifier() {
        assert_eq!(
            message_pattern("request id=123456 duration=42ms complete"),
            "request id=<_> duration=<_> complete"
        );
        assert_eq!(message_pattern("static message"), "static message");
    }

    #[test]
    fn tokenized_structural_layout_beats_row_blob_with_zstd() {
        let records = (0u64..4_096)
            .map(|offset| {
                let request_id = offset.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let trace_id = request_id ^ 0xd1b5_4a32_d192_ed03;
                record(
                    offset,
                    &format!(
                        "ERROR checkout request_id=req-{request_id:016x} trace_id={trace_id:016x} card declined"
                    ),
                )
                .with_field("request.id", format!("req-{request_id:016x}"))
            })
            .collect::<Vec<_>>();
        let legacy = legacy_rows(&records);
        let structural = encode_structural_block(&records).expect("structural block encodes");
        let legacy_compressed = zstd::bulk::compress(&legacy, 1).expect("legacy compresses");
        let structural_compressed =
            zstd::bulk::compress(&structural, 1).expect("structural block compresses");

        assert!(
            structural_compressed.len() < legacy_compressed.len(),
            "structural={} legacy={}",
            structural_compressed.len(),
            legacy_compressed.len()
        );
    }

    #[test]
    fn pco_timestamp_column_round_trips_regular_values_and_extremes() {
        let mut values = Vec::with_capacity(1_025);
        let mut timestamp = 1_700_000_000_000_000_000u64;
        for index in 0..1_025u64 {
            timestamp = timestamp.saturating_add(2_250 + (index % 17) * 10);
            values.push(timestamp);
        }
        values[255] = values[254].saturating_add(3_596_626);
        values[256] = values[255].saturating_add(2_250);
        values[511] = u64::MAX;
        values[512] = 0;
        values[513] = 2_250;

        let encoded = encode_timestamp_values(&values);
        let decoded = decode_timestamps(&encoded, values.len()).expect("timestamps decode");
        assert_eq!(decoded, values);
    }

    #[test]
    fn pco_timestamp_column_compacts_regular_input_before_zstd() {
        let mut values = Vec::with_capacity(4_096);
        let mut timestamp = 1_700_000_000_000_000_000u64;
        for index in 0..4_096u64 {
            timestamp += 2_250 + (index % 29) * 10;
            values.push(timestamp);
        }
        let pco = encode_timestamp_values(&values);
        let mut legacy = Vec::new();
        write_varint(values[0], &mut legacy);
        for pair in values.windows(2) {
            legacy.push(0);
            write_varint(pair[1] - pair[0], &mut legacy);
        }
        assert!(
            pco.len() < legacy.len(),
            "pco={} legacy={}",
            pco.len(),
            legacy.len()
        );
    }

    #[test]
    fn arbitrary_timestamp_bit_patterns_round_trip_exactly() {
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        let mut values = Vec::with_capacity(4_097);
        for index in 0..4_097 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.push(match index % 257 {
                0 => 0,
                1 => u64::MAX,
                _ => state,
            });
        }
        let encoded = encode_timestamp_values(&values);
        assert_eq!(
            decode_timestamps(&encoded, values.len()).expect("timestamps decode"),
            values
        );
    }

    #[test]
    fn malformed_pco_timestamps_and_count_mismatches_are_rejected() {
        let values = [10_000, 12_250, 14_500];
        let mut corrupted = encode_timestamp_values(&values);
        corrupted[0] ^= 0xff;
        assert_eq!(
            decode_timestamps(&corrupted, values.len()),
            Err(LogDbError::InvalidBlockEncoding(
                "invalid Pco timestamp section"
            ))
        );

        let mut truncated = encode_timestamp_values(&values);
        truncated.pop();
        assert!(decode_timestamps(&truncated, values.len()).is_err());

        let encoded = encode_timestamp_values(&values);
        assert_eq!(
            decode_timestamps(&encoded, values.len() - 1),
            Err(LogDbError::InvalidBlockEncoding(
                "Pco timestamp count mismatch"
            ))
        );
    }

    #[test]
    fn malformed_structural_block_rejects_unbounded_record_count() {
        let error = decode_structural_block(b"SLOG\xff\xff\xff\xff\x0f")
            .expect_err("truncated block cannot allocate from its record count");
        assert_eq!(error, LogDbError::InvalidBlockEncoding("record count"));
    }
}
