# ShardTelemetry: A Signal-Native Observability Engine

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`shard-telemetry` is a signal-native observability database built around
shard-stream's ordered, durable ingestion stripes. It stores logs, traces, and
metrics in separate signal-native layouts behind one checksummed `STEL`
envelope and assigns each physical shard to a single-writer owner stripe.

The initial vertical slice implements:

- post-durability, offset-ordered publication into term and exact-metadata
  postings;
- a checksummed, signal-aware native TCP protocol with partitioned `STEL`
  appends, indexed query, and 128-bit request correlation;
- selectivity-ordered hot queries with ranges, newest/oldest ordering, bounded
  results, and a record-reference-only path;
- an immutable block/record query index with delta/run postings, lossless
  block-level substring rejection, and seekable selective structural decoding;
- a per-partition `indexed_through` watermark for read-your-write coordination;
- immutable dictionary generations plus a byte-bounded LRU in each
  stripe;
- an opt-in bounded real-time Zstandard dictionary learner with measured
  held-out admission;
- opt-in bounded block-level compression collation with deterministic
  temperatures, variance-driven splitting, and fail-open OTLP cohorts;
- owner-local Zstd contexts that seal dictionary-aware compressed blocks; and
- immutable block-group publication, paged partition catalogs, exact object
  ranges, and a byte-bounded integrity-checked SSD cache.

## OTLP telemetry

OTLP/gRPC and OTLP/HTTP accept logs, traces, and metrics. Complete requests are
decoded and validated before records are split into deterministic logical
partitions. Typed values, resource and scope context, identifiers, dropped
counts, NaN payload bits, span events and links, and metric exemplars are
preserved by their signal-native records.

For live ingestion, install `TelemetrySinkFactory` with
`StreamEngine::open_with_durable_sink`. Every durable append is a checksummed
`STEL` envelope containing exactly the requested item count. Once shard-stream
has synchronized the append, the owning stripe applies the signal payload at
its durable offsets. One offset identifies one log record, span, or metric
point; nested events, links, and exemplars remain part of their parent item.

## Native protocol

The standalone server exposes native protocol v2 on TCP port `3101`. An
`STB2` request carries one checksummed `STEL` envelope per resulting partition,
and the response returns one durable acknowledgement per partition. The server
validates every envelope before any append and executes independent partitions
in parallel. Multiple frames may be in flight; responses retain the caller's
128-bit request ID and may complete out of order.

This pre-release codebase has one durable format and one native append format.
Historical grouped-log append payloads are not decoded. The grouped `STR2`
encoding is response-only for native log queries.

See [NATIVE_PROTOCOL.md](NATIVE_PROTOCOL.md) for the wire layout, bounds,
acknowledgement semantics, query primitive, and initial Adam ablation.

## Stripe ownership

`TelemetrySinkFactory` creates one bounded, single-writer index worker for each
physical shard. shard-stream invokes its corresponding durable sink only after
the shard append has synchronized; the sink waits for that worker to apply the
batch before the append acknowledgement is returned. The worker uses the
durable offset range as record identities and accepts byte-identical replay,
which makes recovery/retry safe.

The index watermark advances only after term and metadata postings have been
updated. A caller can therefore wait for `indexed_through(partition)` to reach
the offset returned by shard-stream before issuing a read-your-write query.

## Querying and log lookup

`LogQuery` provides the same exact lookup semantics for hot records and sealed
blocks. Predicates support nested AND/OR/NOT, token search, literal
exact/contains/prefix/suffix message and metadata matching, validated regular
expressions, metadata existence and set membership, and signed-integer
metadata comparisons. Queries also support half-open offset and event-time
ranges, offset or `(timestamp, offset)` sorting, oldest/newest order, stable
exclusive cursors, and limits.

`LogStripe::query_refs` avoids cloning records when a coordinator only needs
durable locations. `ShardTelemetry::query_all` and `query_stripes` merge physical
stripes deterministically for one logical partition. The legacy `with_term`
and `with_field` builders remain the shortest indexed path.

Sealed blocks use a persistent two-level index: block postings prune object
reads, then record-ordinal postings select exact rows inside the surviving
blocks. Predicates that cannot be answered by postings receive a safe candidate
superset and are evaluated exactly after selective decode; limits are never
applied before this residual filter. Dense postings remain run encoded in
memory. Body and field lanes have 256-record seek footers, allowing selective
decode without scanning an entire 8 MiB structural block. The full decoder
remains the strict block-integrity path.

Sealed residual queries stream candidates one block at a time and stop after
an offset-ordered page is complete. Full misses can scan blocks across the
configured worker set without creating a 607-million-entry candidate vector.
Each block also carries a fixed 8 KiB lowercase trigram filter. A global union
rejects corpus-wide literal misses before any block or payload read, while a
global intersection bypasses block-filter scans for literals common to every
block. Hash collisions can only admit extra exact residual work. The retained
80 GiB cold-payload benchmark shows indexed pages at roughly 1.8-2.6 ms p50,
positive substring/regex pages at roughly 79-86 ms, and an absent substring at
0.91 microseconds with zero candidate blocks.

See [QUERY_ARCHITECTURE.md](QUERY_ARCHITECTURE.md) for the query plan,
correctness boundary, persistent format, and measured lookup costs.

## ClickHouse analytical compatibility

ShardTelemetry exposes an opt-in authenticated Arrow IPC scan boundary for the
pinned ClickHouse 26.3.17.56 LTS query evaluator. A stock ClickHouse binary can
use the generic URL source with explicit constraints. The narrow
`StorageShardTelemetry` adapter in `clickhouse/adapter` additionally converts
analyzed projections, timestamp ranges, and exact map predicates into
automatic storage pushdown. ShardTelemetry streams bounded columnar batches while
ClickHouse remains responsible for expressions, aggregates, joins, windows,
JSON functions, subqueries, materialized views, protocols, and formats.

The scan route is absent unless `--clickhouse-token-file` is supplied. See
[CLICKHOUSE_COMPATIBILITY.md](CLICKHOUSE_COMPATIBILITY.md) for the schema,
security requirements, adapter installation, differential harness, and
remaining production acceptance gates.

## Thread-local compression and dictionary reuse

Compression runs inside the worker that owns the corresponding shard-stream
physical shard. Each `LogStripe` owns its mutable Zstd context, block
collator, active compression-shard buffers, placement assignment map, and dictionary LRU;
these structures are never shared or locked across stripes.

`DictionaryCatalog` is the shared control plane. It atomically publishes an
immutable snapshot mapping final compression placement IDs to dictionary IDs.
A sink worker adopts that snapshot once at the start of a durable append
batch, never per log record. The stripe then loads selected dictionary bytes
into its local LRU.
An active block retains an `Arc` to the exact dictionary used at its creation,
so cache eviction and later dictionary rotation cannot change its encoding.

Every sealed descriptor records the codec, compression level, logical source
size, structural-payload size, stored size, and optional `dictionary_id`. The
ID must name immutable bytes in the durable dictionary tier; an in-memory LRU
is only a local acceleration layer. The block payload stays staged locally
until an object-tier writer publishes its complete block group and calls
`mark_group_offloaded`, which records the payload object's exact byte ranges
and releases the staged copies.

For a catalog-backed deployment, create the factory with
`TelemetrySinkFactory::with_dictionary_catalog`. Publish dictionaries from a
background trainer or control-plane service; the worker sees the publication at
its next append-batch boundary. This preserves stripe-local ingestion while
allowing the same immutable dictionary generation to be reused across all
matching shards.

`RealtimeDictionaryTrainer` implements that control plane when online learning
is desired. Start one trainer with a shared `DictionaryCatalog`, then construct
the sink with `TelemetrySinkFactory::with_realtime_dictionary`. A stripe offers
one bounded sample only after a block seals; a full queue drops that observation
without delaying or failing ingestion.

Training uses the exact structural template, body, attribute-table, and field
bytes seen by the outer Zstd frame. It deliberately excludes offset deltas and
the Pco timestamp stream. Candidates are shadow-compressed against later
blocks in bounded batches. A generation is published only after cumulative
measured savings repay the complete immutable dictionary plus the configured
minimum gain; a losing candidate or one that cannot repay itself within the
observation cap is discarded. Dictionary bytes are content-addressed with a
128-bit BLAKE3 prefix and remain immutable after publication.

The learner is opt-in. On a 1 GiB stationary real-log workload with 512 KiB
blocks it reduced durable storage by 4.80% while sustaining a median 1,358.87
MiB/s. At 8 MiB blocks the same candidate did not repay its 64 KiB footprint,
so no dictionary or assignment sidecar was written and durable bytes were
identical to the disabled run. See [BENCHMARKS.md](BENCHMARKS.md) for the
corpus construction, accounting, and limitations.

## Algorithmic compression locality

The optional collator uses no model, trainer, embedding, global counter,
floating-point score, or cross-stripe mutable state. An allocation-free
scanner derives a stable template-shape hash and a 16-bit integer SimHash while
exposing search terms to the index. That SimHash is the record's compression
temperature; XOR Hamming distance, not numeric subtraction, measures locality.
Following Eden's exact signature grouping, a mismatched template-shape hash
adds a fixed distance penalty, so exact recurring structures are preferred
without preventing compact neighboring structures from sharing a shard.

Records enter the nearest tentative compression shard, but placement is not
final until a block fills. The collator calculates a byte-weighted block
temperature, mean squared internal deviation, and maximum deviation. A block
with high variance—or one closer to another shard—is recursively partitioned
with farthest-point seeds and nearest-seed assignment, for at most two split
levels. Matching sub-blocks remain in the current shard; deviating sub-blocks
move into bounded destination buffers and are replaced by later matching
records before seal.

Unsplit blocks use an implicit contiguous membership range. Actual split
leaves use packed `u32` record indices; `bytes-handoff` owns that packed buffer
and splits a complete left prefix from the right tail without cloning log
payloads or creating per-record channels. At most 16 compression-shard
profiles are retained per stripe. A new lane requires at least 4 MiB by
default. After two unproductive split attempts for a cohort, a bounded
stripe-local table keeps intervening blocks whole and probes again every 64
blocks. Sparse, highly variant, or over-capacity leaves use the original OTLP
cohort.

Locality routing is disabled by default. On the full 80 GiB acceptance corpus
it changed no stored bytes while reducing Pco-enabled throughput from 1,104.68
to 1,005.02 MiB/s. It remains an explicit opt-in for heterogeneous-corpus
experiments until the current block collator demonstrates a durable storage
gain that repays that cost.

## Compression policy and structural roadmap

[Benchmark results and priority-based codec choices](BENCHMARKS.md) record the
complete Apache-compatible 24-engine comparison on the Adam corpus. The implemented online
default is Pco level 8 for the timestamp column and zstd level 1 for the
enclosing structural block. LZ4 is a planned low-latency option, and zstd
level 9 is a future compaction-only cold format. The current pre-release block
format contains delta-coded offsets, a lossless Pco numeric timestamp stream,
lossless token templates with raw fallback, and dictionary-aware metadata
columns. The enclosing payload is compressed by the stripe-local zstd
context. The
[structural compression architecture](COMPRESSION_ARCHITECTURE.md) documents
the layout, its exact reconstruction rule, and the next measurements needed to
refine it without introducing a second stored format.

## Durable object tier

The durable object-tier boundary is implemented: immutable artifact upload,
block-group manifests, 1,024-group catalog pages, conditionally selected roots,
shallow recovery, exact block ranges, and a recoverable SSD range cache. See
[TIERED_STORAGE_ARCHITECTURE.md](TIERED_STORAGE_ARCHITECTURE.md) for the scale
model, durability states, cold-read path, retention protocol, and capacity
defaults.

The stripe worker now closes complete durable append boundaries, synchronizes
their compressed payload and independent query-index artifacts to the local
spool, uploads immutable objects, and conditionally publishes the catalog
root. Resident payload is released only after that catalog commit. Cold
queries prune catalog pages and groups, load only the selected index segment,
range-read candidate frames through the bounded SSD cache, verify BLAKE3, and
reconstruct exact records. Restart begins from the catalog checkpoint without
loading a corpus-wide posting map.

`LocalObjectStore` is the production adapter shipped by the standalone binary.
Cloud deployments implement the public `TelemetryObjectStore` contract with
put-if-absent, bounded reads, range reads, HEAD, and conditional replacement;
the public core deliberately does not tie storage correctness to one cloud SDK.

The durable sink also supports an optional stripe-local recovery journal for
deployments without the object tier. Each synchronized frame contains exact
indexed native or OTLP appends and its checkpoint chain. Startup repairs an
incomplete final frame and fails closed on committed corruption. Object-tier
deployments recover from immutable catalog checkpoints and do not need this
duplicate raw journal.

## Real-log compression benchmark

The reproducible benchmark compares independent zstd blocks with a trained
dictionary and a prototype template column plus its compressed static-term
block index. It processes up to 1 GiB by default:

```text
cargo run --release --bin shard-telemetry-compress-bench -- /path/to/raw.log
```

The durable structural benchmark can enable the bounded online learner:

```text
cargo run --release --bin shard-telemetry-structural-bench -- \
  /path/to/docker-json.log \
  --block-bytes 512KiB \
  --workers 16 \
  --locality disabled \
  --dictionary realtime \
  --output-dir /new/output/directory
```

Use `--dictionary disabled` for the sequential control leg. The output
accounts for pack payloads, the manifest, immutable dictionary objects, and
run-length encoded per-block dictionary assignments; it then checks every
payload checksum and exactly reconstructs sampled blocks.

For a remote or otherwise stream-only source, use `-` and a new temporary
destination; the benchmark spools at most the requested limit once so every
layout receives exactly the same bytes:

```text
remote-log-command | cargo run --release --bin shard-telemetry-compress-bench -- - --spool-stdin-to /tmp/input.log
```

Use `--report PATH` to retain the final metrics in a new file; the benchmark
refuses to overwrite an existing report.

To compare codec families with independent blocks and first-block round-trip
verification, run:

```text
cargo run --release --bin shard-telemetry-codec-bench -- /path/to/raw.log --limit-bytes 80GiB
```

The default `screen` profile is designed for a practical 80 GiB throughput
campaign. It covers LZ4 (native, pure Rust, and high-compression native),
Snappy, S2, MinLZ, five Deflate implementations, Brotli, native and pure-Rust
LZFSE, zRip, and zstd levels 1, 3, and 9:

```text
cargo run --release --bin shard-telemetry-codec-bench -- /path/to/raw.log --limit-bytes 80GiB --codecs screen --report /path/to/codec-screen-80g.csv
```

Use `--codecs archive` for the slower ratio-focused sweep: S2-best, Zopfli
(five iterations), bzip2-9, native XZ-6, pure-Rust XZ-6, Brotli-5, zRip-4,
and zstd-9. Use `--codecs all` for the complete 24-engine matrix, preferably
first at the default 1 GiB limit; Zopfli and the XZ encoders make a full 80 GiB
all-codec run intentionally long.

Every named implementation can also be selected directly, for example:

```text
cargo run --release --bin shard-telemetry-codec-bench -- /path/to/raw.log --limit-bytes 80GiB --codecs lz4_flex,lz4_native,s2,minlz_balanced,libdeflate-6,zlib_rs-6,lzfse,lzfse_rust,zrip-1,zstd-1
```

The full [1 GiB codec screen and 80 GiB validation](BENCHMARKS.md) identify
zstd-1 as the hot-tier default and zstd-9 as the cold-tier ratio option for the
tested ClickHouse error-loop corpus. The document also records the complete
24-codec comparison and its decision guide. Codecs with GPL, AGPL, or missing
license metadata are deliberately excluded from the Apache-2.0 distribution.

The final template total includes the template table and block-level static
term index. It intentionally excludes high-cardinality value postings; those
are a separate metadata-index policy, not free compression.

To measure the implemented structural layout rather than a line-template
prototype, normalize a Docker `json-file` corpus into durable worker packs and
a manifest:

```text
cargo run --release --bin shard-telemetry-structural-bench -- \
  /path/to/docker-json.log \
  --limit-bytes 80GiB \
  --workers 16 \
  --locality disabled \
  --output-dir /new/path/shard-telemetry-packs \
  --report /new/path/structural-report.txt
```

The benchmark preserves each Docker record's body, RFC3339 nanosecond
timestamp, and `stream` as `docker.stream`. It verifies exact reconstruction
of the first structural block, skips one possible leading partial line from a
tail snapshot, counts and skips malformed complete records, synchronizes packs
and the manifest, and refuses to overwrite existing output. Its reported ratio
is against accepted Docker source bytes, so it includes the gain from replacing
JSON syntax with typed log fields; it is not a transparent byte-for-byte JSON
archive metric.

On Adam, the sequential head-to-head harness gives ShardTelemetry variants and a
typed ClickHouse `MergeTree` the same immutable corpus, 16 physical cores, and
source prewarm. By default it runs locality-disabled and locality-enabled legs
for `SHARD_TELEMETRY_BIN` before ClickHouse:

```text
scripts/run-head-to-head.sh
```

For a cross-version comparison, set `SHARD_TELEMETRY_VARIANTS_FILE` to a
tab-separated manifest with `label`, filesystem-safe `slug`, `binary`, and
`enabled|disabled` columns. The harness runs every row sequentially and records
each binary's SHA-256:

```text
TinyLFU-disabled	tinylfu-disabled	/path/to/old-bench	disabled
TinyLFU-enabled	tinylfu-enabled	/path/to/old-bench	enabled
BlockCollator-disabled	block-disabled	/path/to/new-bench	disabled
BlockCollator-enabled	block-enabled	/path/to/new-bench	enabled
```

```text
SHARD_TELEMETRY_VARIANTS_FILE=/path/to/variants.tsv scripts/run-head-to-head.sh
```

It verifies the 80 GiB source checksum, pins both engines to CPUs `0-15`,
persists each engine into an isolated new result directory, requires equal
accepted row counts, and records provenance plus a TSV summary.
The current [80 GiB timestamp-codec and locality acceptance
result](BENCHMARKS.md) measured the default Pco/zstd layout at 1,104.68 MiB/s
and 136.68x versus 932.71 MiB/s and 73.09x for ClickHouse. It stored
628,473,667 bytes, 23.95% less than the previous ShardTelemetry layout. The
homogeneous corpus remained in base placement; enabling the block collator
changed no bytes and fell below the strict 1 GiB/s gate, which is why routing
is now opt-in.

Run component-level fingerprint, tentative-shard probe, block
score/split/assignment, handoff, throughput, and seal-latency measurements
with:

```text
cargo run --release --bin shard-telemetry-locality-bench
```

To construct a deterministic round-robin corpus from multiple real Docker JSON
logs or OTLP-to-Docker adapter outputs and run the locality ablation:

```text
scripts/run-locality-interleaving.sh /new/result/dir input1.log input2.log
```

The first bounded Adam run interleaved messages from Pluribus, Eden, and the
OpenTelemetry Collector. The superseded record router used seven placements
and reduced stored size by 4.05%. This is retained as a historical baseline;
the same corpus must be rerun for block collation.

The measured 1 GiB baselines are recorded in [BENCHMARKS.md](BENCHMARKS.md):
10.67x for the public HDFS corpus and 22.92x for a live Docker `json-file`
corpus, both including the template payload and static-term index. The report
keeps their storage-accounting caveats explicit.

## Development

The repository follows shard-stream's Rust 1.93 toolchain.

```text
cargo fmt --check
cargo test
cargo clippy -- -D warnings
```
## Standalone Loki-compatible server

The standalone server has a fail-closed single-tenant production mode. Loki,
OTLP, ClickHouse scans, and the native protocol share one immutable tenant,
global bearer authentication, bounded concurrency, ingest byte-rate admission,
query deadlines, real readiness and metrics, and one drain/flush/shutdown
lifecycle. HA, replication, and automatic failover are reserved for the
licensed distribution; the open server always uses one durable replica.

Create a root-readable token file containing at least 16 bytes, then start the
server behind a TLS-terminating reverse proxy. Both listeners bind to loopback
unless explicitly changed:

```bash
cargo run --release --bin shard-telemetry-server -- \
  --auth-token-file /run/secrets/shard-telemetry-token \
  --default-tenant production \
  --data-directory /var/lib/shard-telemetry \
  --object-store-directory /var/lib/shard-telemetry-objects \
  --shards 16 \
  --tenant-partitions 256 \
  --append-linger-micros 250
```

HTTP callers send `Authorization: Bearer <token>`. `X-Scope-OrgID` may be
omitted; if supplied, it must equal `--default-tenant`. A native connection
must send opcode `4` with the token before ping, append, or query. Starting
without authentication requires the explicit `--insecure-development-mode`
flag.

Production mode validates an on-disk format marker, exclusively locks the data
directory, keeps CPU and durable storage work off asynchronous I/O workers,
persists checksummed deletion requests atomically, and applies deletions to
Loki, native, tail, and Arrow reads. `--retention-seconds` enforces an immediate
query cutoff and periodically advances shard-stream's durable log start only
across fully expired append batches. OS signals and Loki shutdown endpoints
stop admission, flush the source log and index checkpoint, close tail/native
connections, and then stop listeners.

Production mode requires `--object-store-directory`; this is where compressed
checkpointed groups and their segmented indexes become restart-authoritative.
The raw recovery journal is opt-in and is intended only as a migration or
diagnostic fallback.

To opt into the administrative ClickHouse scan route, create a protected token
file and add `--clickhouse-token-file /run/secrets/shard-telemetry-clickhouse`. Keep
the listener on loopback or behind TLS/mTLS; the route is intentionally not
registered without that option.

The authoritative shard-stream packs are sufficient for recovery in explicit
development mode. With `--object-store-directory`, production flushes publish
compressed frames and segmented indexes to the immutable ShardTelemetry catalog;
restarts use its durable checkpoint and cold reads do not depend on resident
payload.

See [LOKI_COMPATIBILITY.md](LOKI_COMPATIBILITY.md) for the executable API
surface, differential target, known differences, and wire-path benchmarks. The
HTTP surface includes lossless stream filters, parser and formatting pipelines,
unwrapped metric ranges, vector aggregation and matching, labels, series,
stats, volume, structural patterns, detected JSON/logfmt fields, tailing, and
durable logical deletion. Ruler and multi-tenant control-plane APIs remain
outside the single-tenant storage engine.

## License

ShardTelemetry is licensed under the [Apache License 2.0](LICENSE). Required
third-party attributions are retained in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
