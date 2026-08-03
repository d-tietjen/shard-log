# Query architecture

ShardLog has two exact query tiers that share one `LogQuery` contract:

- the mutable stripe-local hot index for records still resident on the ingest
  shard; and
- an immutable persistent index that selects records from independently
  compressed sealed blocks.

The lookup contract includes:

| Area | Exact behavior |
| --- | --- |
| Boolean | Nested `AND`, `OR`, and `NOT`, plus match-all and match-none |
| Message | Scanner token, literal exact/contains/prefix/suffix, and validated Unicode regex |
| Metadata | Key existence, literal exact/contains/prefix/suffix, exact set membership, regex, and signed 128-bit integer `=`, `!=`, `<`, `<=`, `>`, `>=` |
| Bounds | Half-open durable-offset and event-timestamp ranges |
| Ordering | Offset or `(timestamp, offset)`, oldest or newest first |
| Pagination | Exclusive stable cursor plus limit |
| Fan-out | One physical stripe, selected stripes, or every physical stripe for one logical partition |

Positive metadata predicates use existential semantics when a record contains
the same key more than once. `NOT` negates the complete child predicate, so
negated field equality also matches a record where the field is absent.
Regular expressions compile when the query is constructed and invalid
expressions return `LogDbError::InvalidQuery`.

Queries are scoped to one `TopicPartition`. Cross-partition query language,
aggregations, grouping, and faceting are coordinator/analytics features rather
than record-lookup semantics and are outside this API.

## Hot stripe

Each stripe owns ordered postings per partition. A hot lookup:

1. resolves borrowed term and metadata keys against partition-local interners
   without allocating when the query term is already lowercase;
2. narrows each posting to the requested offset range with binary partition
   points;
3. starts from the shortest posting, independently of constraint order;
4. uses a linear two-pointer intersection for similarly sized postings and an
   adaptive galloping search for highly skewed postings; and
5. evaluates exact residual predicates when needed;
6. applies deterministic ordering and limits before cloning complete records.

`LogStripe::query_refs` returns only durable `RecordRef`s. `LogStripe::query`
uses those references to clone complete visible records. Empty-filter queries
use the partition's ordered record vector and materialize only the requested
limit rather than allocating a full-range ordinal vector.
`ShardLogDb::query_all` and `query_stripes` merge per-stripe top results and
apply the global limit in the same deterministic order.

All hot query state remains stripe-local. A lookup takes no cross-stripe lock,
channel, or global mutable counter.

The ingest representation is ordinal-based. Each partition keeps append-only
records and dense vectors of posting slots; postings contain `u32` record
ordinals instead of full `RecordRef` values. Hash tables are touched only when
a term or field pair is first interned and when a query resolves its borrowed
constraint. Repeated messages reuse a bounded direct-mapped term-ID vector.
Repeated immutable metadata vectors use a second bounded exact cache. The
per-record path consequently appends directly to known vectors without one
hash lookup per term or metadata field.

## Persistent index

Every independently compressed block produces a `BlockQueryIndex` containing:

- exact term to record-ordinal postings;
- exact metadata key/value to record-ordinal postings; and
- an 8 KiB message-trigram rejection filter; and
- block offset, timestamp, partition, and record-count bounds.

`PersistentQueryIndex` first intersects block directories, prunes blocks by
offset and timestamp bounds, and only then intersects record ordinals inside
the surviving blocks. Missing terms and fields terminate without reading a
payload.

The planner extracts only mandatory positive constraints. Legacy terms and
fields, plus token and exact case-sensitive field leaves under top-level
conjunctions, use postings. Literal message exact, contains, prefix, and suffix
leaves under those same conjunctions use the trigram rejection filters. A
message literal under OR or NOT is not required and therefore cannot prune a
block. Regex, existence, set, and numeric predicates receive a safe superset
of candidates. Decoded candidates then pass through `LogQuery::select`, which
applies every range, cursor, legacy constraint, and Boolean residual before
sorting and limiting.
The persistent planner applies an early limit only for a posting-only offset
query with no residual or boundary check. This rule is what prevents false
negatives for expressions such as `(error OR status >= 500) AND NOT env=dev`.

Residual serving is block-streamed. `candidate_blocks` returns only block
ordinals in deterministic offset order, and `candidate_hits_in_block` produces
an unbounded candidate set for one block. The serving loop decodes and filters
that block immediately. Offset-ordered lookups stop as soon as the requested
page is complete, so a positive substring or regex lookup does not allocate a
corpus-wide candidate vector. Full misses may scan blocks concurrently; exact
ordering and the global limit are applied after the worker results are merged.

Postings use one of two resident representations:

- sorted `u32` ordinals for sparse matches; or
- `(start, length)` runs for dense matches.

The same delta/run distinction is retained in the deterministic `SLOGQIX2`
wire format, wrapped in a `SLOGQIZ2` zstd frame. Dense postings are decoded
directly into runs rather than expanded. `posting_cardinality` and
`posting_storage_bytes` expose the logical and resident posting sizes for
operational diagnostics.

Each message filter hashes every UTF-8 byte trigram after applying the same
Unicode lowercase transformation as case-insensitive literal matching. ASCII
messages use an allocation-free lowercase path. The persistent directory
stores all block filters in one contiguous allocation and derives two 8 KiB
aggregate masks:

- if a required bit is absent from the union, no block can match;
- if every required bit is in the intersection, every block survives without
  touching the 80 MiB block-filter directory; and
- otherwise, only candidate blocks whose filter contains every required bit
  proceed to payload materialization.

The aggregate masks are derived when the index loads and are not persisted.
Literal strings shorter than three normalized UTF-8 bytes simply receive no
trigram pruning.

Index construction has two bounded direct-mapped accelerators per block:

- 1,024 whole-message entries reuse term IDs for byte-identical messages; and
- 4,096 term entries bypass repeated case normalization and hash-table lookup.

Whole-message term-ID vectors are recycled on cache replacement. Duplicate
terms and duplicate metadata fields remain exact because the posting append
checks the last record ordinal. Trigrams are inserted only on a whole-message
cache miss because reinserting an identical message cannot change the filter.

## Selective materialization

The outer zstd frame and the Pco timestamp page are block units. The body and
metadata lanes, however, contain a seek footer every 256 records. For each
lane the original record payload remains first and byte-identical; the footer
contains delta-coded byte positions and a fixed-width directory pointer.
Keeping the directory after the payload avoids perturbing zstd's parsing of
the highly repetitive record stream.

`decode_structural_records` groups selected ordinals by checkpoint and scans
only those neighborhoods. It validates selected UTF-8 values, framing,
template IDs, attribute IDs, and every directory bound it touches. The full
`decode_structural_block` path remains the strict integrity verifier: it walks
every record and proves that every checkpoint points to an actual boundary.
Pack queries additionally verify the stored payload checksum before selective
decoding.

## Exactness and filtering

Term and field postings are exact, not Bloom filters, so they do not introduce
false positives. The trigram directory is a necessary-condition filter: a
missing bit proves absence, while a hash collision only allows an extra block
read. It never returns records and cannot suppress an exact match. Structural
records still pass through byte-exact literal matching after decode. Residual
planning may therefore produce false-positive candidates but cannot produce a
false-negative result. Hash collisions in either the filter or Rust maps
affect performance only. Offset and timestamp bounds initially prune whole
blocks; records in a boundary block are checked after materialization before
the caller returns the final result. Query hits are stable
`(block_ordinal, record_ordinal)` locations and do not affect reconstruction.

Offset ordering assumes durable offsets are unique inside a logical partition,
as required by shard-stream. Timestamp ordering always uses the durable offset
as a deterministic tie-breaker. A cursor is valid only with the same
partition, sort, direction, bounds, and predicate that produced it.

Compatibility tests run the operator matrix against the mutable hot stripe,
the persistent candidate planner, and records actually round-tripped through
the structural block codec. They also check non-monotonic timestamps, stable
two-page cursors, duplicate metadata indexing, invalid regex rejection, and
the rule that a residual query cannot consume its limit before exact
filtering.

On a local arm64 release build with 100,000 hot records, posting-only queries
returning 100 records took 4.88-22.52 microseconds. A missing token took 69
nanoseconds. Predicates without a posting path scanned the 100,000-record
stripe in 3.32-9.51 milliseconds before or while materializing results.
Existence and broad Boolean queries cost more when they return and clone tens
of thousands of complete records. These are compatibility fallbacks, not a
claim that every operator has the same acceleration.

## Sealed cold-payload behavior

The 2026-07-30 Adam comparison kept both engines' search indexes resident and
evicted immutable record payload pages with `POSIX_FADV_DONTNEED` before every
cold sample. This models a long-lived query process reading sealed data from
local SSD; it does not include remote object-store request latency.

With the trigram directory, indexed ShardLog lookups returning 100 records
measured 1.34-2.18 ms warm and 1.78-2.62 ms cold at p50. Positive substring
and regex lookups filtered one 59,299-record block in 79-86 ms. The
cache-temperature penalty is small because checksum, decompression, and
reconstruction dominate a roughly 60 KiB pack read.

The former adversarial missing-substring query scanned all 10,240 blocks and
607,363,459 records in 35.92 seconds. It now finds an absent required trigram
in the global union and returns with zero candidate blocks in 0.91 microseconds
at cold p50. ClickHouse took 1.794 seconds in the same sequential harness.
Regex does not yet extract safe literal fragments, so the next residual-query
optimizations are body-lane-only decode, regex literal-prefix extraction, and
allocation-free vectorized evaluation inside surviving blocks.

The monolithic benchmark index remains a useful cold-start baseline. Loading it
takes about 23.1 seconds and expands 114,420,962 compressed bytes into 15.90
GiB of posting arrays plus 80 MiB of trigram filters. The production path now
uses independent object-tier query-index segments and loads only catalog groups
selected by coarse bounds, avoiding that mandatory whole-corpus expansion.

The persistent index is derived data. Its durable offset watermark must not
advance until the corresponding block and index segment are durable. Recovery
may rebuild it from exact structural records without changing record identity.

## Current measured behavior

The pre-checkpoint Adam profile of a 100-hit query against a 1 GiB real
ClickHouse pack attributed 27.24% of cycles to UTF-8 validation of skipped
values, 39.86% to walking body and field streams, and 13.35% to varint reads.
Only 2.27% was zstd and 1.14% was the pack read.

On the deterministic 60,000-record development block, the checkpoint footer
changed zstd-1 size from 148,554 to 148,562 bytes while reducing a newest
contiguous 100-record decode from 2,683.55 to roughly 175 microseconds. One
hundred hits spaced every 100 records took roughly 460 microseconds.

The 2026-07-30 Adam head-to-head loaded the full indexed 80 GiB corpus:
607,363,459 accepted records in 10,240 blocks. A warm 100-hit sealed-pack
query, including planning, pack read, checksum, decompression, and selective
decode, measured 1.06-1.15 ms p50 across latest, exact-field, one-term, and
five-term queries. An exact missing term returned in 0.15 microseconds p50
without reading a payload. Canonical results matched ClickHouse byte-for-byte.

The same run identified a more important scaling cost than selective decode.
The 114,420,962-byte compressed index expands to 17,071,691,984 bytes of
resident postings, 83,886,080 bytes of block filters, and two derived 8 KiB
aggregate masks, and takes about 23 seconds to load. Full indexed ingest peaked
at 97.65 GiB RSS. Query latency is already competitive; reducing posting
expansion and startup cost is now the primary query-architecture priority.

See [BENCHMARKS.md](BENCHMARKS.md) for the hot-index and real-pack tables,
provenance, and evidence locations.

## Remaining optimization work

The production worker constructs and publishes one immutable query-index
artifact per bounded append-aligned group. Catalog pages and groups are pruned
before that artifact is loaded, and candidate frames are range-read through the
SSD cache. A future optimization can intersect compressed run/delta postings
without materializing each selected segment's ordinal arrays. The monolithic
benchmark file remains useful only for reproducible single-host comparisons.

See [TIERED_STORAGE_ARCHITECTURE.md](TIERED_STORAGE_ARCHITECTURE.md) for the
object hierarchy, publication order, SSD range cache, recovery, and
petabyte-scale capacity model.
