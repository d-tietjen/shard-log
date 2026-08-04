use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{CompressionPlacementId, TelemetryError, TelemetryResult};

/// Stable identifier for a compression cohort, such as a service and schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressionCohortId(u64);

impl CompressionCohortId {
    /// Cohort used when a producer has not classified its log line.
    pub const UNCLASSIFIED: Self = Self(0);

    /// Creates a cohort identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw cohort identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable identifier for an immutable compression dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DictionaryId(u128);

impl DictionaryId {
    /// Creates a dictionary identifier.
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// Returns the raw dictionary identifier.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }
}

/// Immutable control-plane view of published compression dictionaries.
///
/// A stripe copies an [`Arc`] to this snapshot at batch boundaries and then
/// performs all record-level dictionary selection from its owned state. The
/// snapshot is never mutated after publication.
#[derive(Debug, Clone, Default)]
pub struct DictionaryCatalogSnapshot {
    generation: u64,
    dictionaries: HashMap<DictionaryId, Arc<[u8]>>,
    placement_dictionaries: HashMap<CompressionPlacementId, DictionaryId>,
}

impl DictionaryCatalogSnapshot {
    /// Returns the monotonically increasing publication generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the immutable dictionary assigned to a final placement.
    #[must_use]
    pub fn dictionary_for(
        &self,
        placement_id: CompressionPlacementId,
    ) -> Option<(DictionaryId, Arc<[u8]>)> {
        let dictionary_id = *self.placement_dictionaries.get(&placement_id)?;
        let payload = Arc::clone(self.dictionaries.get(&dictionary_id)?);
        Some((dictionary_id, payload))
    }

    /// Returns an immutable dictionary by identifier.
    #[must_use]
    pub fn dictionary(&self, dictionary_id: DictionaryId) -> Option<Arc<[u8]>> {
        self.dictionaries.get(&dictionary_id).cloned()
    }

    /// Iterates immutable placement-to-dictionary assignments in this snapshot.
    pub fn assignments(&self) -> impl Iterator<Item = (CompressionPlacementId, DictionaryId)> + '_ {
        self.placement_dictionaries
            .iter()
            .map(|(&placement_id, &dictionary_id)| (placement_id, dictionary_id))
    }

    /// Iterates all immutable dictionary payloads retained by this snapshot.
    pub fn dictionaries(&self) -> impl Iterator<Item = (DictionaryId, Arc<[u8]>)> + '_ {
        self.dictionaries
            .iter()
            .map(|(&dictionary_id, payload)| (dictionary_id, Arc::clone(payload)))
    }
}

/// Result of atomically publishing an immutable dictionary assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryPublication {
    /// Generation containing the published assignment.
    pub generation: u64,
    /// Final placement now assigned to the dictionary.
    pub placement_id: CompressionPlacementId,
    /// Immutable dictionary selected for future blocks.
    pub dictionary_id: DictionaryId,
}

/// Control-plane catalog for immutable dictionaries and placement assignments.
///
/// Publication clones a small metadata map and replaces the complete snapshot.
/// Workers obtain that snapshot once per append batch, then retain it locally;
/// no record-level operation takes this lock or shares mutable compression
/// state with another stripe.
#[derive(Debug, Clone)]
pub struct DictionaryCatalog {
    snapshot: Arc<RwLock<Arc<DictionaryCatalogSnapshot>>>,
}

impl Default for DictionaryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl DictionaryCatalog {
    /// Creates an empty dictionary catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(Arc::new(DictionaryCatalogSnapshot::default()))),
        }
    }

    /// Returns the current immutable publication snapshot.
    pub fn snapshot(&self) -> TelemetryResult<Arc<DictionaryCatalogSnapshot>> {
        self.snapshot
            .read()
            .map(|snapshot| Arc::clone(&snapshot))
            .map_err(|_| TelemetryError::DictionaryCatalogUnavailable)
    }

    /// Publishes a dictionary and atomically assigns it to a placement.
    ///
    /// Reusing a dictionary ID with different bytes is rejected: a sealed block
    /// only stores the ID, so changing its meaning would make old blocks
    /// undecodable after an object-tier cache miss.
    pub fn publish(
        &self,
        placement_id: CompressionPlacementId,
        dictionary_id: DictionaryId,
        payload: Arc<[u8]>,
    ) -> TelemetryResult<DictionaryPublication> {
        if payload.is_empty() {
            return Err(TelemetryError::EmptyDictionary);
        }

        let mut snapshot = self
            .snapshot
            .write()
            .map_err(|_| TelemetryError::DictionaryCatalogUnavailable)?;
        if let Some(existing) = snapshot.dictionaries.get(&dictionary_id)
            && existing.as_ref() != payload.as_ref()
        {
            return Err(TelemetryError::DictionaryIdConflict(dictionary_id));
        }

        let mut next = (**snapshot).clone();
        next.generation = next.generation.wrapping_add(1);
        next.dictionaries.entry(dictionary_id).or_insert(payload);
        next.placement_dictionaries
            .insert(placement_id, dictionary_id);
        let publication = DictionaryPublication {
            generation: next.generation,
            placement_id,
            dictionary_id,
        };
        *snapshot = Arc::new(next);
        Ok(publication)
    }
}

#[derive(Debug, Clone)]
struct CachedDictionary {
    payload: Arc<[u8]>,
    last_used: u64,
}

/// Outcome of inserting an immutable compression dictionary into a stripe cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryInsert {
    /// Dictionary now cached by the stripe.
    pub dictionary_id: DictionaryId,
    /// Dictionaries evicted to maintain the byte capacity.
    pub evicted: Vec<DictionaryId>,
}

/// Byte-bounded LRU for immutable compression dictionaries.
///
/// Every sealed log block stores its [`DictionaryId`]. A cache miss is therefore
/// recoverable by loading that immutable dictionary from the durable dictionary
/// tier; this first implementation exposes the cache but leaves object-tier
/// fetching to a later storage adapter.
#[derive(Debug)]
pub struct DictionaryCache {
    capacity_bytes: usize,
    stored_bytes: usize,
    clock: u64,
    entries: HashMap<DictionaryId, CachedDictionary>,
}

impl DictionaryCache {
    /// Creates an empty LRU cache with a nonzero byte capacity.
    pub fn new(capacity_bytes: usize) -> TelemetryResult<Self> {
        if capacity_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "dictionary_cache_bytes must be nonzero",
            ));
        }
        Ok(Self {
            capacity_bytes,
            stored_bytes: 0,
            clock: 0,
            entries: HashMap::new(),
        })
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub const fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    /// Returns the bytes currently resident in the cache.
    #[must_use]
    pub const fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    /// Returns the number of resident dictionaries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the cache has no resident dictionaries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Inserts or replaces a dictionary and evicts least-recently-used entries.
    pub fn insert(
        &mut self,
        dictionary_id: DictionaryId,
        payload: Arc<[u8]>,
    ) -> TelemetryResult<DictionaryInsert> {
        if payload.is_empty() {
            return Err(TelemetryError::EmptyDictionary);
        }
        if payload.len() > self.capacity_bytes {
            return Err(TelemetryError::DictionaryTooLarge {
                bytes: payload.len(),
                capacity: self.capacity_bytes,
            });
        }

        if let Some(existing) = self.entries.get(&dictionary_id)
            && existing.payload.as_ref() != payload.as_ref()
        {
            return Err(TelemetryError::DictionaryIdConflict(dictionary_id));
        }

        if let Some(previous) = self.entries.remove(&dictionary_id) {
            self.stored_bytes -= previous.payload.len();
        }
        self.clock = self.clock.wrapping_add(1);
        self.stored_bytes += payload.len();
        self.entries.insert(
            dictionary_id,
            CachedDictionary {
                payload,
                last_used: self.clock,
            },
        );

        let mut evicted = Vec::new();
        while self.stored_bytes > self.capacity_bytes {
            let candidate = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(dictionary_id, _)| *dictionary_id)
                .expect("a cache over capacity has an entry");
            let entry = self
                .entries
                .remove(&candidate)
                .expect("selected dictionary remains resident");
            self.stored_bytes -= entry.payload.len();
            evicted.push(candidate);
        }
        Ok(DictionaryInsert {
            dictionary_id,
            evicted,
        })
    }

    /// Returns a dictionary payload and refreshes its LRU position.
    pub fn get(&mut self, dictionary_id: DictionaryId) -> Option<Arc<[u8]>> {
        self.clock = self.clock.wrapping_add(1);
        let entry = self.entries.get_mut(&dictionary_id)?;
        entry.last_used = self.clock;
        Some(Arc::clone(&entry.payload))
    }

    /// Returns whether a dictionary is resident without changing recency.
    #[must_use]
    pub fn contains(&self, dictionary_id: DictionaryId) -> bool {
        self.entries.contains_key(&dictionary_id)
    }
}
