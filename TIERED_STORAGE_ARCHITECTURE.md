# Tiered storage architecture

ShardTelemetry's durable tier is designed so data volume can grow to at least one
pebibyte without requiring a process to load a corpus-wide block catalog,
enumerate an object-store bucket, or validate every payload at startup.

The implementation follows shard-stream's proven lifecycle:

- data objects are immutable;
- publication is ordered and idempotent;
- a small `CURRENT` object selects an immutable metadata generation;
- the final publication is conditional, fencing stale writers;
- local data is released only after object durability is visible; and
- retention publishes new metadata before deleting unreachable bytes.

ShardTelemetry changes the manifest shape. shard-stream's per-shard pack list is
small enough to keep in one manifest. A petabyte-scale log database needs a
partition-scoped hierarchy of roots, catalog pages, block groups, query-index
segments, dictionaries, and payload ranges.

## Storage units

| Unit | Recommended production target | Purpose |
| --- | ---: | --- |
| Structural block | Existing 8 MiB source target | Independent compression, checksum, and selective decode boundary |
| Block group | 1 GiB compressed target, 2 GiB hard limit | Amortizes object PUT/GET cost across ordered blocks |
| Blocks per group | 4,096 hard limit | Bounds group manifests even when blocks compress extremely well |
| Catalog page | 1,024 groups | Bounds metadata fetched for one range lookup |
| Catalog root | References pages only | Keeps startup independent of block count |
| SSD cache chunk | 4 MiB | Avoids downloading an entire group for one matching block |
| Control-object read | 64 MiB hard limit | Prevents corrupt metadata from causing unbounded allocation |

The defaults are represented by `ObjectTierConfig` and `SsdCacheConfig`.
Deployments may close a group before its byte target for age, durability, or
partition-idleness reasons, but may not cross the configured hard limits.

At the worst case of one PiB of already-compressed payload and 1 GiB groups,
there are 1,048,576 groups and 1,024 catalog pages. A root therefore has about
one thousand references, not one million block entries. With highly
compressible 8 MiB source blocks, the 4,096-block cap closes groups first and
keeps every group manifest bounded. Physical shards and logical partitions
split those totals further.

These are metadata-scale guarantees, not an assumption that arbitrary logs
will match the 136.68x ratio measured on the repetitive ClickHouse corpus.
Capacity planning must use measured stored bytes for each production source.

## Namespace and artifacts

Every catalog is scoped to one physical shard and one logical partition:

```text
catalog/
  shard-<physical-shard>/
    topic-<32-hex-digit-topic-id>/
      partition-<logical-partition>/
        CURRENT
        roots/root-<generation>-<checksum>.json
        pages/page-<sequence>-<checksum>.json
        groups/<group-sequence>/
          manifest-<checksum>.json
          payload-<name>-<checksum>
          query-index-<name>-<checksum>
          dictionary-<name>-<checksum>
          dictionary-catalog-<name>-<checksum>
```

All keys except `CURRENT` are immutable and include a BLAKE3 content checksum.
Readers never use object listing. They start from a known namespace and follow
authenticated references.

A block-group manifest records:

- physical shard and logical partition identity;
- source cohort, final compression placement, and dictionary ID per block;
- offset, timestamp, record-count, source, structural, and stored byte
  accounting;
- compression temperature and variance diagnostics;
- exact payload offset and length per block;
- a BLAKE3 checksum for each compressed block; and
- object key, size, and BLAKE3 checksum for every group artifact.

The query index is an independent artifact per group. This is the persistent
query architecture's segmentation boundary: a cold lookup loads postings only
for candidate groups instead of expanding the measured 15.90 GiB global index.

## Publication protocol

The owning shard worker is the only normal publisher for a
`(physical shard, topic, partition)` namespace. Publication is:

1. Seal a bounded set of compressed blocks and its independent query index.
2. Write and synchronize a local payload pack. `write_staged_payload_pack`
   records every block's exact range and checksum without buffering the whole
   pack a second time.
3. Put payload, query index, required dictionaries, and assignment metadata
   with immutable put-if-absent semantics.
4. Put the immutable group manifest.
5. Append the group entry to an immutable catalog page.
6. Put a new immutable catalog root that references the new page generation.
7. Compare-and-swap `CURRENT` from the writer's observed object version token to the new root
   pointer.
8. Call `mark_group_offloaded` only after step 7 succeeds. This records object
   ranges and releases staged block payloads.

Retries with identical content are accepted. Reusing a sequence with different
content is corruption. Two writers may upload the same immutable objects, but
only one can advance `CURRENT`; the other receives `TelemetryError::StaleCatalog`
and must reopen the authoritative root before retrying.

Crashes before step 7 leave invisible immutable objects. They are harmless and
can be removed later by an orphan collector after a grace period. Crashes
after step 7 are recoverable from `CURRENT`, even if local staged-data cleanup
did not run.

`LocalObjectStore` implements these rules with synchronized temporary writes,
atomic rename, parent-directory synchronization, BLAKE3 verification, and a
filesystem update lock. A production S3-compatible adapter implements the same
`TelemetryObjectStore` contract with immutable create, bounded GET, range GET, HEAD,
and conditional replacement. If an object service cannot conditionally replace
`CURRENT`, the adapter must provide an equivalent external fencing primitive;
unconditional last-writer-wins publication is not safe.

## Cold query path

A lookup over sealed data performs:

1. Read `CURRENT` and the selected root from the metadata SSD cache, refreshing
   only when its generation changes.
2. Prune root page references by offset and event-time bounds.
3. Read only candidate catalog pages.
4. Prune their group entries by the same coarse bounds.
5. Load each surviving group's `SLOGQIX2`/`SLOGQIZ2` query-index segment.
6. Intersect exact term and metadata postings and apply trigram rejection.
7. Range-read only selected compressed block extents through the payload SSD
   cache.
8. Verify the per-block checksum, decompress, selectively reconstruct
   candidate records, and run every exact residual predicate.

This preserves the existing hot/cold query compatibility contract. Catalog
and trigram collisions can only create extra reads; reconstruction and exact
filtering remain authoritative.

`TelemetryObjectTier::open` deliberately validates only `CURRENT` and the immutable
root. A page is verified when its bounds are touched, a group manifest when
selected, and a full artifact when read. Verifying every referenced payload on
startup would turn process recovery into a petabyte scan. A separate
background auditor should continuously sample or sweep immutable objects
without blocking availability.

## SSD tier

Object storage remains the durable authority after publication. Local NVMe has
two roles:

- an unpublished write spool for newly sealed groups; and
- a recoverable range cache for already published objects.

`SsdObjectCache` is byte bounded and uses fixed-size chunks. Its cache identity
is the BLAKE3 hash of object key, immutable version token, and chunk index. Each local
chunk has its own length and BLAKE3 integrity header. A corrupt chunk is
discarded and fetched again. Startup reconstructs the cache directory and
evicts least-recently-used entries until it fits the configured budget.

Production should create at least two cache instances:

| Cache | Suggested policy |
| --- | --- |
| Metadata/index | Smaller chunks, protected capacity, long residency |
| Payload | 4 MiB chunks, large capacity, scan-resistant admission |

`read_range_with_metadata` accepts object size, version token, and BLAKE3
content digest already authenticated
by a group manifest, avoiding one remote HEAD request per block lookup.
`read_range` remains available when the caller has only an object key.

The current cache uses exact LRU for its bounded local directory. A later
high-concurrency implementation may replace only the admission/eviction data
structure with stripe-local TinyLFU; it must retain the same immutable cache
identity and integrity framing.

## Recovery and durability

There are three explicit durability states:

| State | Meaning |
| --- | --- |
| Stream durable | shard-stream has synchronized the source append |
| SSD staged | ShardTelemetry block, query index, and group files can be retried locally |
| Object durable | `CURRENT` selects a root that reaches every required immutable artifact |

An object-durable acknowledgement, when requested, must wait through the
`CURRENT` compare-and-swap. A local-durable acknowledgement may return after
the write spool is synchronized, with offload continuing in the owning worker.
The indexed watermark must never advance beyond the selected durability mode's
data and query index.

On restart:

1. Recover shard-stream and replay any source offsets beyond ShardTelemetry's durable
   index checkpoint.
2. Open each known catalog directly; do not list the bucket.
3. Reconcile synchronized local spool groups against the selected root.
4. Retry unpublished groups idempotently.
5. Remove a local spool group only after its catalog generation is selected.

Historical physical-shard ownership is part of the query coordinator's routing
metadata. A logical partition that moved between physical shards may have
catalogs in more than one shard namespace; the coordinator merges them by the
same durable offset and timestamp order used by hot queries.

## Retention and garbage collection

Retention is metadata first:

1. Build new immutable pages excluding groups wholly below the retention
   boundary.
2. Publish a new root and conditionally advance `CURRENT`.
3. Wait for the configured reader/version grace period.
4. Delete unreachable group artifacts, manifests, superseded pages, and roots
   asynchronously.
5. Evict matching SSD cache chunks opportunistically; correctness does not
   depend on immediate eviction.

Boundary groups remain intact until every block in them expires. Optional
compaction may rewrite a partially expired group under a new sequence, but it
must publish the replacement before removing the original. Legal hold is a
root-selection policy: held groups remain reachable regardless of the normal
time cutoff.

Deletion is intentionally outside `TelemetryObjectStore`'s online query/publication
contract. The garbage collector should use a separately authorized object
client so an ingest or query process cannot erase durable data.

## Worker integration

The worker-level integration mirrors shard-stream's pack offloader:

- one mutable group builder belongs to each shard worker;
- block compression, query-index construction, and local spool writes remain
  worker local;
- a group closes on target bytes, block count, explicit flush, or shutdown;
- publication runs in sequence order for each shard/partition namespace;
- backpressure is based on unpublished SSD spool bytes, never total retained
  object bytes; and
- local spool retention advances only from authoritative catalog generations.

`LogStripe::offload_indexed_groups` constructs append-aligned payload and query
artifacts, publishes them through `TelemetryObjectTier`, and releases resident frames
only after the new `CURRENT` generation is selected. On restart, catalog
checkpoints skip already-published recovery transactions. Queries load a group
index before payload and verify each selected frame checksum after range read.

The standalone binary ships `LocalObjectStore`. The public `TelemetryObjectStore`
trait is the stable integration point for S3-compatible and other cloud
adapters; adapters must preserve immutable create and conditional `CURRENT`
replacement semantics.

## Required operational metrics

At minimum, report these per shard and partition:

- staged and object-durable group sequence;
- unpublished SSD spool bytes and oldest spool age;
- group payload bytes, block count, and close reason;
- `CURRENT` generation and conditional-publication failures;
- catalog root/page/group cache hit rates;
- payload-cache hit bytes, miss bytes, evictions, and integrity failures;
- object PUT, GET, range-GET, HEAD, bytes, latency, and retry counts;
- query pages and groups pruned before index fetch;
- index bytes fetched and blocks range-read per query;
- checksum, decompression, and reconstruction failures; and
- retention watermark, unreachable bytes, and garbage-collection lag.

Alerts should fire on a non-advancing object-durable sequence, spool growth
approaching its budget, repeated stale-writer failures, or any immutable object
checksum mismatch.
