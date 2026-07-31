# Compression benchmark results

## Native fully indexed ingest optimization series — 1 GiB

Date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Corpus: first complete-record span covering 1,073,741,933 source bytes and
7,592,023 records from the immutable 80 GiB ClickHouse Docker JSON corpus  
Protocol: native grouped batches, 1 MiB target, 16 ShardLog owner stripes,
leader-durable shard-stream append, and indexed acknowledgement  
Storage: one authoritative shard-stream copy; the optional recovery journal
was disabled

The profile-guided series kept the corpus, durability, record count, and
server shard count fixed:

| Stage | Source throughput | Peak server RSS | Main change |
| --- | ---: | ---: | --- |
| v2 baseline | 65.95 MiB/s | not retained | Original native durable/indexed path |
| v3 | 85.84 MiB/s | not retained | Foldhash, allocation-free term scanner, consolidated record state |
| v4 | 112.42 MiB/s | 12,294,732 KiB | Removed duplicate raw recovery journal |
| v5 | 121.06 MiB/s | not retained | Bounded exact-message term cache |
| v7 | 167.49 MiB/s | 5,991,060 KiB | Partition-local record ordinals and `u32` postings |
| v9 | 289.76 MiB/s | 3,293,284 KiB | Shared immutable metadata, repeated-body reuse, non-materializing pre-validation, append fast path |
| v11 | 380.29 MiB/s | 3,312,004 KiB | 32 connections instead of 16 |
| v14 best | **423.37 MiB/s** | 3,321,092 KiB | Partition-local dense posting slots and exact field cache |

The v9 change improved the identical 16-connection run from 169.44 to
289.76 MiB/s, reduced sampled server cycles from approximately 58.9 billion to
23.0 billion, and reduced peak RSS by 45.0%. The complete series improved the
best observed end-to-end result by 6.42x.

The 32-connection result is not yet a stable production rate. Three repetitions
of the current binary at 1 MiB produced 219.24, 240.78, and 425.27 MiB/s
(median 240.78 MiB/s). Server CPU work and stored bytes were nearly constant;
wall time changed according to whether adjacent shard-stream appends joined
the same fsync group. Raising append linger to 1 ms retained the same
approximately 219/424 MiB/s bimodality. A 2 MiB batch regressed to a
210.29 MiB/s median and increased kernel time.

Adam exposes 32 logical CPUs as 16 SMT sibling pairs: `0/16`, `1/17`, through
`15/31`. The early `0-15` server and `16-31` producer split therefore shared
physical cores. Repeating with both processes constrained to `0-15` retained
the same 216-423 MiB/s range, which rules out the affinity split as the primary
cause but means none of these rows is a server-only 16-core measurement.

Every 1 GiB run wrote 832,404,552 native payload bytes and occupied about
795 MiB on disk. That raw authoritative WAL write is now the throughput
ceiling: ShardLog's structural block compressor runs above 2 GiB/s on this
corpus, but compression currently occurs after the raw shard-stream append.
Reaching sustainable multi-GiB/s durable ingestion requires an explicitly
approved recovery-format change that makes a checksummed structural-compressed
ingest pack authoritative before fsync. Asynchronous acknowledgement alone
would only hide index/WAL lag and is not counted as a throughput result.

Retained Adam evidence:

```text
/home/dtietjen/shard-log-native-profile-v2
/home/dtietjen/shard-log-native-profile-v3
/home/dtietjen/shard-log-native-profile-v4-single-copy
/home/dtietjen/shard-log-native-profile-v5-term-cache
/home/dtietjen/shard-log-native-profile-v7-ordinals
/home/dtietjen/shard-log-native-profile-v9-shared-metadata
/home/dtietjen/shard-log-native-profile-v10-shared-metadata-perf
/home/dtietjen/shard-log-native-profile-v11-workers32
/home/dtietjen/shard-log-native-profile-v14-dense-postings-w32
/home/dtietjen/shard-log-native-profile-v15-dense-postings-perf-w32
/home/dtietjen/shard-log-native-profile-v19-notify-repeat-{a,b,c}
/home/dtietjen/shard-log-native-profile-v20-linger1000-{a,b,c}
/home/dtietjen/shard-log-native-profile-v21-batch2m-{a,b,c}
/home/dtietjen/shard-log-native-profile-v22-shared-cpuset-{a,b,c}
```

## Loki-wire durability ablation — 128 MiB

Date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
CPUs: physical cores `0-15` for each sequential engine run  
Source: first 128 MiB of the immutable 80 GiB ClickHouse Docker JSON corpus  
Client: `shard-log-loki-load`, 16 deterministic file spans, 16 persistent HTTP
connections, and 1 MiB Loki JSON push batches  
Records accepted by both engines: 949,018

| Engine | Source throughput | Total disk bytes after run |
| --- | ---: | ---: |
| ShardLog durable API | 43.16 MiB/s | 241,490,286 |
| Grafana Loki 3.7.2 | 57.63 MiB/s | 120,076,424 |

Loki used image
`sha256:191d4fdfb7264f16989f0a57f320872620a5a7c2ceeec6229212c4190ec49b86`
with its ingestion rate and burst limits raised so the database, rather than a
4 MiB/s tenant throttle, determined throughput.

This is a failed ShardLog ablation, not an accepted full result. The direct
structural benchmark did not include the production durability path. Here
ShardLog retained the authoritative shard-stream source packs and a second raw
sink recovery journal, while sealed compressed block payloads remained
memory-owned. The full 80 GiB ShardLog result is therefore gated on durable
compressed-block publication, cold reads, and reclaiming raw source ranges
covered by published blocks. See `LOKI_COMPATIBILITY.md`.

## Loki 3.7.2 — full 80 GiB Loki-wire ingest

Date: 2026-07-30  
Host and CPUs: Adam, physical cores `0-15`  
Corpus: the same immutable 80 GiB file and SHA-256 used by the ShardLog and
ClickHouse runs below  
Image:
`sha256:191d4fdfb7264f16989f0a57f320872620a5a7c2ceeec6229212c4190ec49b86`

The same `shard-log-loki-load` client used for the bounded ablation sent 1 MiB
Loki JSON pushes over 16 persistent connections. Every submitted batch
received HTTP 200/204. The client reached the known truncated final physical
line after sending all complete records and exited nonzero while parsing that
tail; the fixed client now skips a non-newline-terminated final fragment just
as the established ShardLog/ClickHouse harness skips the leading fragment.

| Source bytes represented | Settled disk bytes | Ratio | Wall time | Throughput |
| ---: | ---: | ---: | ---: | ---: |
| 85,899,345,826 | 3,782,890,631 | 22.71x | 976.70 s | 83.87 MiB/s |

Settled components after `/flush` and WAL checkpoint:

| Component | Bytes |
| --- | ---: |
| Chunks | 3,782,791,668 |
| WAL | 0 |
| Active TSDB shipper | 69,748 |
| TSDB shipper cache | 29,215 |
| Total | 3,782,890,631 |

The WAL-inclusive footprint was observed between roughly 30 and 40 GiB during
ingest before checkpoint reclamation. Both peak and settled storage matter for
capacity planning; the compression ratio uses settled durable storage.

Evidence is retained on Adam at:

```text
/home/dtietjen/shard-log-loki-full-80g-20260730-v1
```

### Provisional same-corpus comparison

The rows below use the same immutable source and the same Adam physical CPU set,
but are not yet a single-interface campaign: ShardLog and ClickHouse are the
previous sequential indexed native-engine run, while Loki includes Loki HTTP
JSON framing and the shared wire client.

| Engine | Settled stored bytes | Raw-source ratio | Throughput |
| --- | ---: | ---: | ---: |
| ShardLog + exact index | **743,581,572** | **115.52x** | **1,214.17 MiB/s** |
| Loki 3.7.2 | 3,782,890,631 | 22.71x | 83.87 MiB/s |
| ClickHouse + text index | 6,093,155,990 | 14.10x | 353.33 MiB/s |

This table does not satisfy the final drop-in service acceptance gate.
ShardLog's Loki-wire durable path currently fails the bounded comparison and
must publish cold blocks and reclaim source data before a three-service
sequential rerun is meaningful.

## LogHub HDFS — 1 GiB run

Run date: 2026-07-29  
Corpus: [LogHub HDFS_1](https://zenodo.org/records/3227177), a public HDFS
machine-log dataset. Archive checksum: `b0d0a8bed97530bccf0babdf3a905572`.

Command:

```text
cargo run --release --bin shard-log-compress-bench -- /path/to/HDFS.log --limit-bytes 1GiB
```

The line-oriented input boundary made the measured source `1,073,741,791`
bytes: the largest complete-line prefix not exceeding 1 GiB. Blocks target 8
MiB; zstd level is 3; the trained dictionary is 112 KiB and is included in the
dictionary result.

| Layout | Stored bytes | Ratio | Source retained |
| --- | ---: | ---: | ---: |
| Independent zstd blocks | 105,156,269 | 10.21x | 9.79% |
| zstd + trained dictionary | 105,248,502 | 10.20x | 9.80% |
| Prototype template column | 100,640,713 | 10.67x | 9.37% |
| Template column + static-term block index | 100,641,996 | 10.67x | 9.37% |

The template run found 52 templates, 124 static terms, and 70 encoded blocks.
Its final total includes compressed records (100,639,419 bytes), the template
table (1,294 bytes), and the static-term index (1,283 bytes).

This is deliberately an honest lower bound for the prototype’s indexed layout:
it excludes per-record postings for high-cardinality variable values and does
not include OTLP envelope or metadata-column storage. The result therefore
does not support assuming 100x compression for transparent raw-log retention.
Reaching that territory would require a semantic representation with a
different value-storage/index policy, validated separately for fidelity and
query cost.

## Adam Docker json-file — 1 GiB run

Run date: 2026-07-29  
Corpus: a live ClickHouse container's Docker `json-file` log on Adam. The
benchmark executed on Adam against the protected file itself: a static
benchmark binary and that one log file were mounted into an ephemeral,
network-disabled, read-only helper container. The helper had a RAM-backed
directory writable only for its text report; no log data was copied off host.

The measured input is `1,073,741,765` bytes, the largest complete-line prefix
not exceeding 1 GiB. It preserves Docker's JSON wrapper, including timestamps,
stream metadata, and escaped message body.

| Layout | Stored bytes | Ratio | Source retained |
| --- | ---: | ---: | ---: |
| Independent zstd blocks | 46,961,071 | 22.86x | 4.37% |
| zstd + trained dictionary | 47,183,439 | 22.76x | 4.39% |
| Prototype template column | 46,840,158 | 22.92x | 4.36% |
| Template column + static-term block index | 46,840,543 | **22.92x** | **4.36%** |

This run found 22 templates, 45 static terms, and 84 encoded template blocks.
The final total comprises compressed records (46,839,651 bytes), the template
table (507 bytes), and the static-term index (385 bytes). Like the HDFS run,
it excludes high-cardinality per-record postings and any OTLP-normalized
columns, so it is a storage-layout baseline rather than a full serving-size
claim.

## Adam Docker json-file — implemented structural layout, 1 GiB run

Run date: 2026-07-29  
Corpus: the same immutable ClickHouse Docker `json-file` tail on Adam. The
tail begins mid-record, so the benchmark discards that one incomplete leading
line and measures `1,073,741,671` bytes of complete Docker JSON records. It
normalizes each record into exact body text, a nanosecond event timestamp, and
one `docker.stream` metadata field. The first structural block is decoded and
compared field-for-field before measurement continues.

Command:

```text
shard-log-structural-bench /path/to/docker-json.log --limit-bytes 1GiB
```

| Layout | Stored bytes | Ratio against raw Docker source | Source retained |
| --- | ---: | ---: | ---: |
| Raw Docker JSON, zstd-1 | 42,048,146 | 25.54x | 3.92% |
| Structural payload before zstd | 214,310,852 | 5.01x | 19.96% |
| Structural payload, zstd-1 | **10,463,133** | **102.62x** | **0.97%** |

The structural payload is 75.12% smaller than raw Docker JSON compressed with
the same zstd-1 level (a 31,585,013-byte reduction per GiB). Raw zstd encoded
at 1,489 MiB/s; zstd over the already-normalized structural payload encoded at
1,513 MiB/s. This ratio includes a semantic representation gain—the Docker
JSON wrapper is replaced with typed fields—while retaining the body, timestamp,
and stream. It must therefore not be read as a byte-for-byte archival JSON
claim or as a prediction for heterogeneous OTLP traffic.

## Codec matrix — immutable Adam ClickHouse JSON corpus

Run date: 2026-07-29  
Corpus: `clickhouse-docker-json-error-loop-tail-80g-20260729.log` on Adam.
The raw file is exactly 80 GiB and has SHA-256
`4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508`.

Every codec used independent 8 MiB blocks and passed a first-block
compress/decompress round-trip. Reported MiB/s is time spent compressing
already-read blocks, not filesystem read throughput. This corpus is a
high-repetition ClickHouse error-loop tail, so its ratios are not a proxy for
all production logs.

### Complete 1 GiB codec comparison

Command:

```text
cargo run --release --bin shard-log-codec-bench -- /path/to/raw.log --codecs all
```

The complete matrix uses a fresh reader and independent 8 MiB blocks for every
codec. The benchmark verifies a first-block round trip for every row and times
compression after the input block has been read. It does not use a trained
dictionary, so it is a baseline for choosing a byte codec rather than a claim
about the final normalized shard-log layout.

| Codec | Stored bytes | Ratio | Retained | Compression throughput |
| --- | ---: | ---: | ---: | ---: |
| Copy | 1,073,741,824 | 1.00x | 100.00% | 11,691 MiB/s |
| `lz4_flex` | 81,142,348 | 13.23x | 7.56% | 1,897 MiB/s |
| `lz4_native` | 81,110,632 | 13.24x | 7.55% | 2,261 MiB/s |
| `lz4_native_hc-9` | 62,630,026 | 17.14x | 5.83% | 133 MiB/s |
| `lz4_rust` | 81,110,120 | 13.24x | 7.55% | 2,263 MiB/s |
| `snap` | 119,801,022 | 8.96x | 11.16% | **2,410 MiB/s** |
| `s2` | 93,329,078 | 11.50x | 8.69% | 2,118 MiB/s |
| `s2_better` | 91,011,664 | 11.80x | 8.48% | 551 MiB/s |
| `s2_best` | 84,874,530 | 12.65x | 7.90% | 142 MiB/s |
| `minlz_balanced` | 72,829,036 | 14.74x | 6.78% | 479 MiB/s |
| DEFLATE level 6 | 42,822,645 | 25.07x | 3.99% | 128 MiB/s |
| `libdeflate-6` | 40,665,196 | 26.40x | 3.79% | 333 MiB/s |
| `zlib_rs-6` | 42,149,520 | 25.47x | 3.93% | 270 MiB/s |
| `flate3` | 52,393,082 | 20.49x | 4.88% | 76 MiB/s |
| `zenflate-7` | 40,469,558 | 26.53x | 3.77% | 235 MiB/s |
| `zopfli-5` | 36,168,767 | 29.69x | 3.37% | 0.50 MiB/s |
| `brotli-5` | 42,479,572 | 25.28x | 3.96% | 62 MiB/s |
| `bzip2-9` | **32,413,164** | **33.13x** | **3.02%** | 11.7 MiB/s |
| `xz2-6` | 40,033,340 | 26.82x | 3.73% | 18.5 MiB/s |
| `lzma_rust2_xz-6` | 40,018,544 | 26.83x | 3.73% | 9.6 MiB/s |
| `lzfse` | 50,841,869 | 21.12x | 4.74% | 161 MiB/s |
| `lzfse_rust` | 50,834,312 | 21.12x | 4.73% | 212 MiB/s |
| `zrip-1` | 48,505,369 | 22.14x | 4.52% | 1,053 MiB/s |
| `zrip-4` | 50,450,929 | 21.28x | 4.70% | 804 MiB/s |
| `zstd-1` | 42,045,476 | 25.54x | 3.92% | 1,374 MiB/s |
| `zstd-3` | 46,959,090 | 22.87x | 4.37% | 1,146 MiB/s |
| `zstd-9` | 38,958,957 | 27.56x | 3.63% | 151 MiB/s |

The practical Pareto choices are Snappy, LZ4, zstd-1, libdeflate-6,
zenflate-7, zstd-9, and bzip2-9. The small 0.13x ratio difference between
libdeflate-6 and zenflate-7 is not enough to treat the latter as a default: it
is 29% slower. Zopfli is dominated by bzip2-9 on this corpus: bzip2 is both
smaller and more than twenty times faster.

### Selection guide

| Priority | Recommended policy | Why | Cost |
| --- | --- | --- | --- |
| Lowest encode latency | LZ4 (`lz4_rust` or native LZ4) | 13.24x at about 2.26 GiB/s; substantially smaller than Snappy for only a small throughput cost. | Stores about twice as many bytes as zstd-1. |
| Default hot storage | zstd level 1 | 25.54x at 1.37 GiB/s; it dominates most middle-ground codecs. | Less peak throughput than LZ4. |
| Background compact storage | libdeflate-6 | 26.40x at 333 MiB/s; modestly smaller than zstd-1. | Requires a separate codec path and is about four times slower than zstd-1. |
| Cold searchable storage | zstd level 9 | 27.56x at 151 MiB/s; a good compact, queryable block format. | About nine times slower than zstd-1. |
| Offline deep archive | bzip2 level 9 | Best measured ratio, 33.13x. | Only 11.7 MiB/s; do not use on the ingest or query path. |

The initial implementation remains zstd level 1. A future compactor may
rewrite sealed, immutable blocks to zstd level 9. Do not put Zopfli, bzip2,
or XZ on the online query path; their one-shot ratios do not compensate for
their encode cost or their poor operational fit for independently retrievable
blocks.

### 80 GiB Pareto validation

| Codec | Stored bytes | Ratio | Retained | Compression time | Throughput |
| --- | ---: | ---: | ---: | ---: | ---: |
| `lz4_flex` | 6,485,916,300 | 13.24x | 7.55% | 43.25 s | 1,894 MiB/s |
| `snap` | 9,576,902,250 | 8.97x | 11.15% | 33.18 s | 2,469 MiB/s |
| zstd level 1 | 3,344,308,661 | 25.69x | 3.89% | 53.98 s | 1,518 MiB/s |
| zstd level 9 | **3,088,103,261** | **27.82x** | **3.60%** | 558.76 s | 147 MiB/s |

For this corpus, zstd-1 is the clear hot-tier default: it is 25.69x while
compressing at 1.52 GiB/s. zstd-9 saves a further 256,205,400 bytes (7.66%)
over zstd-1 but takes 10.35x as long, making it a cold-tier option. Snappy is
the pure speed winner; LZ4 is 32.3% smaller at 76.7% of Snappy's throughput.

## ShardLog versus ClickHouse — sequential 80 GiB ingest

Run date: 2026-07-29  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Corpus:
`/home/dtietjen/log-compression-samples/clickhouse-docker-json-error-loop-tail-80g-20260729.log`

The source is exactly 80 GiB with SHA-256
`4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508`.
It contains one 94-byte leading partial line and seven malformed complete
records totaling 1,973 bytes. Both engines rejected those same records and
accepted exactly 607,363,459 typed log records spanning 85,899,343,853 source
bytes.

The engines ran sequentially. Each received CPUs `0-15`, the first hardware
thread of Adam's 16 physical cores; SMT siblings `16-31` were not used. The
entire source was read immediately before each timed leg to give both the same
hot-cache treatment.

ShardLog used deterministic 8 MiB complete-line ranges, 16 owner-local parser,
structural-encoder, and zstd-1 contexts, 16 durable pack files, and one ordered
manifest. Pack files and the manifest were synchronized before the ingest
timer stopped. Every payload checksum was then verified and the first, middle,
and last blocks were decompressed and decoded outside the timed interval.

ClickHouse 26.5.1.882 ran in an isolated container pinned to the same CPU set.
Its typed `MergeTree` stored `time DateTime64(9)` with
`DoubleDelta, ZSTD(1)`, `stream LowCardinality(String)` with `ZSTD(1)`, and
`log String` with `ZSTD(1)`, ordered by `(stream, time)`. The table enabled
`fsync_after_insert`. A pinned in-container adapter skipped only the leading
partial line and streamed the remaining immutable file into `JSONEachRow`;
the server's error allowance was set to ShardLog's measured count of seven.
The final row-count equality was a mandatory harness gate.

| Engine | Accepted records | Durable stored bytes | Compression ratio | Ingest time | Throughput |
| --- | ---: | ---: | ---: | ---: | ---: |
| ShardLog | 607,363,459 | **826,364,011** | **103.95x** | **64.890 s** | **1,262.44 MiB/s** |
| ClickHouse | 607,363,459 | 1,175,227,112 | 73.09x | 86.850 s | 943.24 MiB/s |

ShardLog was 1.338x as fast and used 348,863,101 fewer durable bytes, a 29.68%
reduction from ClickHouse's active-part size. Its ratio was 1.422x higher.
The ShardLog total includes 825,544,794 bytes of pack payload and an 819,217
byte manifest. The ClickHouse total is `sum(bytes_on_disk)` for the 20 active
table parts, including their active data, marks, and part metadata. It does not
use the whole container data-directory size, which also contained system data
and transient inactive parts immediately after ingestion.

This result demonstrates greater than 100x compression for this particular,
highly repetitive error-loop corpus after replacing Docker JSON syntax with a
typed semantic representation. It is not a byte-identical JSON archival ratio
and is not a general prediction for heterogeneous OTLP logs.

The authoritative evidence is retained on Adam at:

```text
/home/dtietjen/shard-log-head-to-head/full-80g-20260729-v4
```

Key evidence checksums:

```text
summary.tsv          d07a33a31dccdea7e2f747ebf00a9da08242f5f78763b5025520b97292447ebf
shard-log-report.txt b27c688bae0ad01feaabc0c25f2eb0511253d985f4da0f588ec1083063e1b78d
clickhouse-parts.tsv 3fcfa78913d65e761194c2e93a6c18439ba7b98066bf80741e3a3c5c050c7b13
provenance.txt       95883ba16989d22d7b20437f6070448d6c27851941fd089351278a40a0f348ef
```

## ShardLog versus indexed ClickHouse — 80 GiB ingest and query

Run date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Execution tier: native Linux host-performance evidence; this is not a
logical-time or exact-replay simulation.

This comparison adds serving indexes to both engines. ShardLog used its
persistent exact term/field index and selective structural decoder. ClickHouse
26.5.1 used its generally available `text` index with
`splitByNonAlpha` tokenization and a lowercase preprocessor.

The source was the same immutable 80 GiB file with SHA-256
`4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508`.
The complete-line range contained seven malformed rows totaling 1,973 bytes.
Both engines received all 85,899,345,826 complete-line bytes, retained exactly
seven parse errors, and accepted exactly 607,363,459 records. The harness
compared canonical timestamp, stream, and body output byte-for-byte before
timing every query class.

Both legs ran sequentially on physical CPUs `0-15`, with the exact source
range prewarmed immediately before each ingest. ShardLog used 16 owner-local
workers, 8 MiB blocks, Pco timestamps, zstd level 1, disabled locality and
real-time dictionary experiments, synchronized packs and manifest, and its
persistent query index. ClickHouse used a `MergeTree` ordered by
`(stream, time)`, `DateTime64(9)` with `DoubleDelta, ZSTD(1)`,
`LowCardinality(String)` stream storage, `ZSTD(1)` bodies, the text index, and
`fsync_after_insert = 1`.

### Ingest and storage

| Engine | Stored bytes | Raw-source ratio | External wall time | External throughput |
| --- | ---: | ---: | ---: | ---: |
| ShardLog + exact index | **743,581,572** | **115.52x** | **67.47 s** | **1,214.17 MiB/s** |
| ClickHouse + text index | 6,093,155,990 | 14.10x | 231.85 s | 353.33 MiB/s |

The conservative process-level result makes ShardLog 3.44x faster and 8.19x
smaller, an 87.80% stored-byte reduction. ShardLog's external time includes
25.19 seconds of post-ingest checksum and sampled reconstruction verification.
Its durable stage alone took 35.43 seconds at 2,311.95 MiB/s, but that number
is not used in the headline because the comparison records the stricter
whole-process wall time.

| Component | ShardLog | ClickHouse |
| --- | ---: | ---: |
| Compressed structural/data bytes | 628,909,207 | 1,172,219,181 |
| Search index bytes | 113,853,148 | 4,918,354,227 |
| Manifest/marks/remaining active-part bytes | 819,217 | 2,582,582 |
| Total | **743,581,572** | 6,093,155,990 |

The compressed ShardLog index is 43.2x smaller than ClickHouse's text index.
The normalized data lane itself is 1.86x smaller than ClickHouse's compressed
typed columns. These ratios are against the raw Docker source and therefore
include replacing the JSON wrapper with typed fields; they are not
byte-identical JSON archival ratios.

### Warm query latency

Every row reports 200 sequential warm queries returning at most 100 records.
ShardLog latency includes planning, pack read, payload checksum, zstd
decompression, and selective record decode. ClickHouse latency is measured by
`clickhouse-benchmark` over its local native client/server path. The forced
scan disables skip indexes and is a reference, not the indexed head-to-head.

| Query | ShardLog p50 / p99 | ClickHouse indexed p50 / p99 | ClickHouse scan p50 | ShardLog p50 advantage |
| --- | ---: | ---: | ---: | ---: |
| Latest 100 | 1.055 / 1.076 ms | 497 / 521 ms | 497 ms | 471x |
| `docker.stream=stderr` | 1.073 / 1.110 ms | 4 / 5 ms | 4 ms | 3.7x |
| Term `cannot` | 1.085 / 1.117 ms | 524 / 547 ms | 579 ms | 483x |
| Five-term AND | 1.154 / 1.201 ms | 570 / 599 ms | 586 ms | 494x |
| Missing term | 0.00015 / 0.00019 ms | 11 / 12 ms | 1,571 ms | about 73,000x |

The corpus is a homogeneous error loop and nearly every row is `stderr`.
Positive terms such as `cannot` occur almost everywhere, so ClickHouse's text
index cannot prune those workloads. Conversely, `(stream, time)` ordering
makes the stream-filtered ClickHouse query efficient, and its indexed missing
term avoids the 1.57-second full scan. This matrix intentionally includes both
favorable and unfavorable selectivity cases.

### Current ShardLog cost

Query execution is fast, but the benchmark's monolithic index representation
is not production-ready at this scale:

- the 113,853,148-byte on-disk index expands to 17,071,691,984 bytes
  (15.90 GiB) of resident posting storage;
- loading it takes about 23.0 seconds; and
- indexed ingest peaks at 102,406,520 KiB (97.66 GiB) RSS.

The next optimization is therefore segmented, mmap-friendly postings that
retain run/delta encoding and load only selected block directories. These
figures also explain why the result is an engine-level comparison, not yet a
production service claim. ShardLog ran in-process without an RPC layer;
ClickHouse ran as a warm server over its local native protocol.

### Provenance and evidence

ShardLog source archive:
`af8bfe78a8788bd5c7fb837d4bf06477ed359e892f99a346517810df7613948a`.
The bundled `shard-stream` revision is
`13ee7903d42cabe9bd5c0df0fa8e4a4fdc660ea7`, with relevant source-tree hash
`ad9fdfd9b13fb9635d40a5df6308cdecca7de81a5e3ebfe45db8b5e47f229308`.
The ClickHouse image is pinned by digest
`sha256:770156c537ca9124046e138a3b5845c64ea58ce8722de7a2e05fd827f4976520`.
The deployment procedure was audited against local deterministic-simulation
revision `62f4e527284add129faf9d9d3bfd1ec99f65f26e` and Adam revision
`bd1c4ba7eff99a5f324a5434b1a9d23444e32582`; both framework worktrees were
dirty and neither framework binary was in the timed native-host path.

Authoritative evidence is retained on Adam at:

```text
/home/dtietjen/shard-log-query-head-to-head/80gib-v7-20260730T205000Z
```

Key evidence checksums:

```text
ingest-summary.tsv          a71d829413d3cf4a177a26bb6a34dfd32d488a4dc100841e44c2fa2a1399d27a
shard-log-report.txt        87481295ff6a1db549c2cfc2a32ea91bc6d740cb5c878afd0963e2e0b992b541
clickhouse-parts.tsv        1d3e1234175d1b14153d242f4f3d68d1b86decc22d979c6910d961fd025cebdb
clickhouse-index.tsv        90d39c7c2da5b8b540518f18bd02521041fb7ecc919049c6d0db555e26b88de5
clickhouse-parse-errors.csv f1395ea74680adf42e4d04d371a8aaff194fe2cd410ee68cb2912d8928353048
provenance.txt              fbb6764216bda6574e4e4bde33b6d8e85475791689e55c7162979bdd9d8f4207
harness.log                 6c16e89a3d5d30cb5de70a384fd964d8d1d7161b18361b89fa01eda7af916425
```

This native campaign is command-replayable on the same corpus and host class,
but timing is not exact-replay deterministic. The isolated comparison
container was stopped and removed by the harness; its data, query outputs,
checksums, and logs remain in the evidence directory.

## Historical algorithmic locality routing — microbenchmarks

> Superseded baseline: these results measure the former per-record
> TinyLFU/bucket router, not the current block-collation algorithm. They are
> retained to make the next Adam comparison explicit.

Run date: 2026-07-29  
Host: Adam, one Ryzen 9 3950X physical core pinned with `taskset -c 0`  
Command:

```text
target/release/shard-log-locality-bench \
  --iterations 1000000 \
  --seal-records 50000
```

| Operation | Result |
| --- | ---: |
| 64-byte fingerprint | 208.99 MiB/s, 292.05 ns/record |
| 256-byte fingerprint | 252.68 MiB/s, 966.22 ns/record |
| 1,024-byte fingerprint | 266.48 MiB/s, 3,664.66 ns/record |
| 4,096-byte fingerprint | 267.94 MiB/s, 14,578.88 ns/record |
| Route-cache hit | 38.31 Mrecords/s, 26.10 ns/record |
| Route-cache miss | 20.74 Mrecords/s, 48.21 ns/record |
| Radius-one probe miss | 17.18 Mrecords/s, 58.21 ns/record |
| Representative fingerprint + route | 139.06 MiB/s, 2.65 Mrecords/s |
| Stripe ingest, 64 KiB block target | 0.35 Mrecords/s |
| Block-seal latency | 318.51 µs p50, 368.83 µs p99 |

The route-cache hit rate was effectively 100% after power-of-two warmup. Each
stripe preallocated 387,840 bytes in five buffers; the cache-hit path performed
no heap allocation. The representative run placed 979,520 records in fine
buckets and used base placement for the first 20,480 observations.

These are single-core component measurements. The acceptance decision uses the
full 16-core durable corpus run below, because parsing, structural encoding,
compression, and pack synchronization determine end-to-end throughput.

## Historical real-service deterministic interleaving

Run date: 2026-07-29  
Host: Adam, one Ryzen 9 3950X physical core  
Evidence:
`/home/dtietjen/shard-log-locality-interleaving-20260729-v1`

The harness collected bounded tails from three independent live services:
Pluribus, Eden's telemetry hot path, and the OpenTelemetry Collector. A
lossless adapter put each original message line into a Docker JSON envelope
with a fixed valid timestamp, then `shard-log-interleave` emitted one complete
line from each source in round-robin order. The resulting 76,851-record corpus
was 26,408,726 bytes. Both modes used one worker and 1 MiB target blocks.

| Mode | Stored bytes | Ratio | Elapsed | Throughput |
| --- | ---: | ---: | ---: | ---: |
| Routing disabled | 1,443,611 | 18.29x | 0.2492 s | 101.06 MiB/s |
| Routing enabled | **1,385,107** | **19.07x** | 0.3302 s | 76.27 MiB/s |

The superseded record router saved 58,504 bytes, or 4.05% of the disabled stored size. It
admitted seven specialized placements and routed 60,398 records fine, 6,747
coarse, and 9,706 base, for a 12.63% fallback rate. The route-cache hit rate
was 98.74%.

The single-core enabled leg was 24.53% slower. This sample demonstrates a real
placement benefit but is too small to be a universal ratio claim; the separate
80 GiB run establishes multicore throughput and homogeneous no-regression.

Evidence checksums:

```text
summary.tsv           3222d4642456baa3c7b6a516ad121a01b0cdd9ced583a15cab90c7c045b12f52
disabled-report.txt   3183c95b39883f5bd21469e16b54af3e2b8b908925fb672684c82775982fd0f8
enabled-report.txt    561ea8e7b3207eb445875cf2fa691d6525f92fe367efec1cfeea150ca317a91a
provenance.txt        95e8f05cdca4c98e73341a22e258f4ae7a1e931c2f46fea6c4ab266b3b347ed3
interleave-report.txt 899c213e4e36f4bf6cdf45e56810c2a3e7896edaf0fa6136895e408e15c18e76
```

## Historical locality acceptance — sequential 80 GiB ingest

Run date: 2026-07-29  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Evidence:
`/home/dtietjen/shard-log-head-to-head/full-80g-locality-20260729-v1`

The finalized harness ran three sequential legs with identical source
prewarming and CPUs `0-15`: ShardLog with routing disabled, ShardLog with
routing enabled, and ClickHouse. All accepted exactly 607,363,459 records and
85,899,343,853 source bytes. Every ShardLog payload checksum passed, and the
first, middle, and last blocks decompressed and decoded successfully after
ingest.

| Engine | Stored bytes | Ratio | Elapsed | Throughput |
| --- | ---: | ---: | ---: | ---: |
| ShardLog, routing disabled | **826,364,011** | **103.95x** | **70.2466 s** | **1,166.18 MiB/s** |
| ShardLog, routing enabled | **826,364,011** | **103.95x** | 76.5802 s | 1,069.73 MiB/s |
| ClickHouse | 1,176,062,051 | 73.04x | 87.3700 s | 937.62 MiB/s |

The enabled router cleared the required 1 GiB/s floor and did not change
stored size by one byte. It was 8.27% slower than the disabled ablation, but
still 14.09% faster than ClickHouse. It stored 349,698,040 fewer bytes than
ClickHouse, a 29.74% reduction from ClickHouse's active-part size.

Storage accounting for both ShardLog legs:

| Component | Bytes |
| --- | ---: |
| Accepted Docker JSON source | 85,899,343,853 |
| Structural payload before zstd | 17,144,825,250 |
| zstd-1 pack payload | 825,544,794 |
| Manifest | 819,217 |
| Persistent term/field index | 0 in the direct corpus encoder |
| Compression dictionaries | 0 |
| Durable pack plus manifest | 826,364,011 |

Routing diagnostics for the enabled leg:

| Diagnostic | Result |
| --- | ---: |
| Route-cache hit rate | 99.9993% |
| Fine / coarse / base records | 0 / 0 / 607,363,459 |
| Fallback rate | 100% |
| Active specialized placements | 0 |
| Router state across 16 workers | 6,205,440 bytes |
| Process user CPU time | 1,100.90 s |
| Process system CPU time | 29.96 s |
| Maximum RSS | 599,888 KiB |

The corpus is one homogeneous ClickHouse error loop, but its individual
template buckets did not independently reach durable admission under the
bounded aging policy. All records therefore failed open to the source cohort.
This is the intended no-regression behavior, not a measured locality gain.

These numbers do not select the current pre-release default. The block
collator replaced this router and must independently tie or improve stored
size while sustaining at least 1 GiB/s on Adam.

Evidence checksums:

```text
summary.tsv                    5b67c4772211f1999cd22d898c32dbf71d69e5d6522d5a90096c1de547a19091
shard-log-disabled-report.txt d088b4d62399890b82f9998f53d82a0110b68dab25580af84039577474454b71
shard-log-enabled-report.txt  3440256346019492834c239f20aaf0e8068c57ab4be2f53cef796536e390c8be
clickhouse-parts.tsv           bb38748452501ef2211d8e831127f49fad9b66a9891c225ddc8cd16a0536cf5a
provenance.txt                 57a264c71f361a4c131f76f42a3af8eeefd9c9a4ff2ed7f53a606daf4b4f3234
```

## Block-collation implementation validation

Implementation date: 2026-07-29

The current code replaces per-record bucket routing with block-level
temperature scoring and bounded redistribution:

- 16-bit SimHash temperatures, XOR Hamming distance, and a fixed mismatch
  penalty for Eden-style exact template-shape hashes;
- byte-weighted block centroids, Q8 mean squared internal variance, and maximum
  deviation;
- farthest-point seeds, nearest-seed assignment, and at most two recursive
  split levels;
- at most 16 stripe-local compression-shard profiles;
- destination buffers that retain matching records and refill space left by
  moved deviations;
- implicit contiguous membership for unsplit blocks and packed `u32`
  memberships split into owned prefixes and tails through `bytes-handoff`;
- bounded split-exploration backoff after repeated fallback-only blocks; and
- fail-open base placement for sparse, highly variant, or over-capacity leaves.

Local unit and property-style tests pass for deterministic splitting,
membership conservation, bounded profiles and split depth, sparse/capacity
fallback, mixed-block filtering/refill, exact UTF-8 reconstruction, offset
preservation, and unchanged term/field queries. `cargo test --all-targets` and
`cargo clippy --all-targets -- -D warnings` are the implementation gates.

An early reduced release-mode smoke run on the Darwin arm64 development host
used 200,000 iterations and 20,000 stripe records:

| Operation | Development smoke result |
| --- | ---: |
| Tentative existing-shard lookup | 218.53 Mrecords/s |
| Tentative bounded-shard probe | 210.10 Mrecords/s |
| Block score/split/assignment | 7,988.70 MiB/s, 32.72 Mrecords/s |
| Persistent profile state | 3,072 bytes |
| Fingerprint + tentative placement | 378.36 MiB/s, 7.21 Mrecords/s |
| Stripe ingest, 64 KiB blocks | 0.91 Mrecords/s |
| Seal latency | 206.88 µs p50, 380.96 µs p99 |

This is a local implementation smoke test, not an acceptance result. In
particular, block score throughput operates on already fingerprinted compact
records and must not be interpreted as end-to-end log ingest throughput.

### Current 80 GiB cross-version matrix

Run date: 2026-07-29 local / 2026-07-30 UTC  
Host: Adam, Ryzen 9 3950X, Linux 6.8  
CPU set: physical cores `0-15`, sequential execution  
Block size: 8 MiB  
Source SHA-256:
`4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508`

The matrix used one immutable source and mandatory equality gates for
85,899,343,853 accepted source bytes and 607,363,459 records:

| Version and mode | Stored bytes | Ratio | Seconds | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| Historical TinyLFU, disabled | 826,364,011 | 103.95x | 70.060 | 1,169.28 |
| Historical TinyLFU, enabled | 826,364,011 | 103.95x | 76.604 | 1,069.40 |
| Block collator v5, disabled | 826,364,011 | 103.95x | 75.981 | 1,078.16 |
| Block collator v5, enabled | 826,364,011 | 103.95x | 80.042 | 1,023.46 |
| ClickHouse 26.5.1.882 | 1,175,260,664 | 73.09x | 87.830 | 932.71 |

The historical router cost 8.54% against its disabled control. Block
collation v5 cost 5.07% against its disabled control. This homogeneous corpus
admitted no specialized placement, so all ShardLog rows are byte-identical.

The matrix exposed an enabled-only offset-ordering defect during its first
attempt. The incomplete run is retained separately. The fix merges relocated
sub-blocks by logical offset and has a dedicated regression test.

### Final v6 acceptance

After the complete matrix, v6 replaced packed root membership with an implicit
contiguous range. Actual split leaves still use `bytes-handoff`. A separately
prewarmed, CPU-identical full pass produced:

| Version | Stored bytes | Ratio | Seconds | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| Block collator v6, enabled | **826,364,011** | **103.95x** | **79.177** | **1,034.64** |

This clears the 1 GiB/s gate by 10.64 MiB/s. It scored 11,296 blocks and
sub-blocks, performed 528 splits, suppressed 10,064 repeated unproductive
explorations, and transferred 83,512,504 packed membership bytes through
`bytes-handoff`. Every record remained in base placement, every one of 10,240
payload checksums passed, and sampled blocks decompressed and reconstructed
exactly.

Compared with ClickHouse from the sequential matrix, v6 was 1.109x as fast
and stored 348,896,653 fewer bytes, a 29.69% reduction. Compared with v5,
implicit root membership reduced handoff volume by 96.68% and improved
throughput by 1.09%.

Authoritative matrix evidence:

```text
/home/dtietjen/shard-log-head-to-head/full-80g-version-matrix-20260729-v2
summary.tsv      c17ae0b547efea213f29b2eb6684d6e884b12948e25433656767eb601686b94c
provenance.txt   ffe5509194ea75675f26c4569bb692c822c55eea66b332b4a05f3205188bf929
```

Final v6 evidence and source:

```text
/home/dtietjen/shard-log-head-to-head/block-collator-enabled-full-80g-v6
report.txt       11006550ea8fdc98f516e7a9565b26f4cf2f1b4f4e9a10884d741dcd43dc85ee
time.txt         9915ae87c45664aa022c0d7c2f42a536b90905f4e3ceb3b443605c30015458a5
source archive   c63ee9585327a18daabe348b29c05d84e5c50e83d93b4ecc83ee150829e17140
binary           d800a3e0028e505b34899fcb4a919537b7845bc854eea12ed4aad34cfab14bf7
```

The remaining acceptance work is the deterministic heterogeneous
Pluribus/Eden/OTEL interleaving matrix; this corpus validates throughput and
homogeneous no-regression, not placement gain.

## Current pre-release default — Pco timestamps, zstd payload

Run date: 2026-07-29 local / 2026-07-30 UTC  
Host: Adam, Ryzen 9 3950X, Linux 6.8  
CPU set: physical cores `0-15`, sequential execution  
Block size: 8 MiB  
Pco: 1.0.2, compression level 8  
Outer codec: zstd 1.5.7, level 1

The source remained the immutable 80 GiB corpus with SHA-256
`4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508`.
Both final legs accepted 607,363,459 records and 85,899,343,853 source
bytes, rejected the same seven malformed records totaling 1,973 bytes, wrote
and synchronized 16 pack files plus the ordered manifest, verified all 10,240
payload checksums, and exactly reconstructed sampled first, middle, and last
blocks.

### Timestamp representation ablation

The zstd library investigation showed that timestamps contributed 4,510,097
bytes—86.2% of the complete stored frames—on a 512 MiB, 3,796,013-record
slice. Bodies compressed to 241,005 bytes at 309.3x, so further text placement
could not address the dominant bytes.

| Timestamp representation | Raw bytes | zstd-1 / codec bytes | Change from prior timestamp lane |
| --- | ---: | ---: | ---: |
| Prior direction-tagged delta + zstd-1 | 11,388,962 | 4,510,097 | baseline |
| Monotonic delta varint + zstd-1 | 7,593,077 | 4,129,361 | -8.44% |
| Byte-shuffled delta + zstd-1 | 9,490,783 | 4,120,277 | -8.64% |
| Two byte-shuffled `i32` words + zstd-1 | 30,368,104 | 7,561,921 | +67.67% |
| Epoch hour/minute/second/millisecond/nanosecond + zstd-1 | 45,552,156 | 7,181,696 | +59.24% |
| Pco level 8 | 3,330,012 | 3,330,012 | **-26.16%** |

The measured timestamp deltas had a 6.32-bit Shannon lower bound on this
slice. Pco-8 used 7.02 bits/value, encoded 36.04 million values/s per core,
decoded 306.55 million values/s, and passed exact round trips for every frame.
Pco-12 reached 6.73 bits/value but encoded only 9.69 million values/s, so level
8 is the selected Pareto point.

The native 256-record checkpoint prototype used a sampled median predictor,
scaled byte-shuffled `i8`/`i16`/`i32` residual lanes, and full-timestamp
exceptions. It reduced the integrated 1 GiB durable total by only 2.37% and
was 1.46% slower in the repeated A/B. Replacing the median with the first
delta retained the raw reduction but made complete-frame storage 6.96% worse
than the median variant. Neither native prototype remains in the single
pre-release format.

### Integrated 1 GiB gate

| Variant | Durable bytes | Ratio | Seconds | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| Prior timestamp format | 10,472,712 | 102.53x | 1.0657 | 960.87 |
| Native checkpoint/predictor | 10,224,947 | 105.01x | 1.0739 | 953.58 |
| Pco-8 timestamp column | **7,946,802** | **135.12x** | **1.0590** | **966.99** |

Pco reduced the 1 GiB durable total by 24.12% without a throughput penalty.
Every leg used the same prewarm, CPUs, worker count, block spans, durable
writes, and post-ingest verification.

### Full 80 GiB acceptance

| Engine / mode | Durable bytes | Ratio | Seconds | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| Previous ShardLog v6, locality enabled | 826,364,011 | 103.95x | 79.177 | 1,034.64 |
| Pco-8 ShardLog, locality enabled | **628,473,667** | **136.68x** | 81.511 | 1,005.02 |
| Pco-8 ShardLog, locality disabled (initial default) | **628,473,667** | **136.68x** | **74.157** | **1,104.68** |
| Pco-8 ShardLog, first optimized hot path | **628,473,667** | **136.68x** | **42.344** | **1,934.63** |
| ClickHouse 26.5.1.882 | 1,175,260,664 | 73.09x | 87.830 | 932.71 |

Storage accounting for both Pco legs:

| Component | Bytes |
| --- | ---: |
| Accepted Docker JSON source | 85,899,343,853 |
| Structural payload before zstd | 15,847,519,672 |
| zstd-1 pack payload | 627,654,450 |
| Manifest | 819,217 |
| Persistent term/field index | 0 in the direct corpus encoder |
| Compression dictionaries | 0 |
| Durable pack plus manifest | **628,473,667** |

Pco saved 197,890,344 durable bytes, or 23.95%, from the previous ShardLog
format. It saved 546,786,997 bytes, or 46.53%, from ClickHouse's active-part
size. That first optimized path was 75.13% faster than the initial
locality-disabled Pco path and 107.42% faster than ClickHouse while preserving
every stored byte. The later path-by-path section records the current default.

Locality enabled and disabled produced byte-identical totals. The enabled leg
performed 528 splits, created 1,056 sub-blocks, suppressed 10,064 later split
explorations, transferred 83,512,504 membership bytes through `bytes-handoff`,
and reassigned zero records. It was 9.92% slower and missed the strict
throughput floor by 18.98 MiB/s. `CompressionLocalityConfig` therefore defaults
to disabled; the collator remains an explicit opt-in for heterogeneous
validation.

Authoritative evidence:

```text
/home/dtietjen/shard-log-head-to-head/timestamp-checkpoints-20260729-v1
pco-v4-full-80g-report.txt          0dccc8eb8c72af4f2063feb64e4c5a8b0917a15f739083089b24c29d41bbfc2c
pco-v4-disabled-full-80g-report.txt 28edca19c3d9a6cf06dde16c9f67e6260fafd27c1c9e9129c48c39861faa17ca
pco-v4-full-80g-time.txt            9df4c3a55ca4c739e0a709b966770b101395af90062e84ab70ff65165eeb1e08
pco-v4-disabled-full-80g-time.txt   f272336c3f07cb81c01a99ae126df1a099d3d974a533ff5e04b5193d9eb3b237
component-ablation.txt              ea00d01a52d766cabf0233e6edfd2270bbe5675b8f06c3ce78dea8924d61a33f
source archive                      ad759e63e709b1b72c9808664e836100b23386c3d815630881df332cca8e63f1
benchmark binary                    9d7eebc85fde95d10974436e83deb19d869b7999f96b3f43e4e66324c9c80702
```

The final source state was archived and rebuilt independently after making
locality-disabled the hardcoded default. A 1 GiB invocation with no
`--locality` argument reported `locality routing: disabled`, reproduced the
same 7,946,802 durable bytes and 135.12x ratio, and passed all 128 payload
checksums plus sampled exact reconstruction. Its measured ingest throughput
was 951.80 MiB/s; this smoke run verifies default selection and stored bytes,
while the prewarmed full 80 GiB run above remains the acceptance throughput
measurement.

```text
final source archive                 304cd50003b4f17667fa1e9495002e65e2e0dfcdfd112ac05333fb2dac08ec5f
pco-v5-default-binary                c5a5f432a08b5523ec3dc306ab33bca62a774637c4d2c5774b217767ef852efa
pco-v5-default-1g-report.txt         f06a0e185016a6106399e5af37cb207ddcbab8bb032fb8a6831701acac54f1d7
pco-v5-default-1g-time.txt           edbb5a49d03f313543e930b6f4d95fef4806cb650d86968493b364109a9bae38
```

These are native pinned-core performance runs, not deterministic task-schedule
replays. Adam passed the deterministic-simulation Linux doctor, but both the
framework checkout and ShardLog source state were dirty/uncommitted; the
retained source archive and binary hashes, rather than a Git revision, are the
reconstruction boundary.

## Pco-8 hot-path throughput optimization

Run date: 2026-07-29 America/New_York / 2026-07-30 UTC  
Host: Adam, Ryzen 9 3950X, Linux 6.8.0-111-generic  
CPUs: `0-15`, 16 workers  
Block target: 8 MiB  
Locality and real-time dictionary: disabled  
Evidence: `/home/dtietjen/shard-log-head-to-head/throughput-optimization-20260729-v1`

Profiling showed that Pco and Zstd were no longer the throughput limit. The
initial path spent most of its CPU in repeated message scanning, metadata
hashing and allocation, Docker JSON normalization, and locality bookkeeping
that could not affect a disabled-routing result.

The accepted implementation:

1. Bypasses locality membership, grouping, and redistribution when routing is
   disabled. `LogStripe` now takes the same direct seal path and records zero
   collator observations.
2. Uses borrowed probes when building attribute tables, allocating a key or
   value only on its first occurrence.
3. Keeps a 1,024-entry direct-mapped, block-local message-layout cache. A hit
   requires exact message-byte equality; collisions only trigger reparsing.
4. Uses linear lookup through 16 entries for low-cardinality metadata, then
   promotes to the existing hash-table path for wider blocks.
5. Parses the Docker adapter's common fixed 30-byte RFC3339-nanosecond
   timestamp directly, retaining the general parser as fallback. OTLP already
   supplies a numeric timestamp and does not need this adapter step.

One representative 8 MiB source block contained 59,313 valid records but only
19 exact messages. Reusing parsed layouts therefore removes almost all
token-range allocation and template rescanning on this corpus without
special-casing message text.

### Sequential 1 GiB ablations

Each comparison alternated the two binaries on the same warm source and CPUs.
Runs are shorter than one second after optimization, so the table reports the
median within each paired experiment rather than comparing medians from
different experiments.

| Paired experiment | Control MiB/s | Candidate MiB/s | Change | Decision |
| --- | ---: | ---: | ---: | --- |
| Disabled-locality and borrowed-attribute path | 963.06 | 1,007.48 | +4.61% | Keep |
| Exact-message layout cache | 1,020.76 | 1,615.83 | +58.30% | Keep |
| Low-cardinality metadata path | 1,606.19 | 1,668.87 | +3.90% | Keep |
| Fixed-width timestamp path | 1,646.44 | 1,697.75 | +3.12% | Keep |
| Canonical Docker JSON parser | 1,633.36 | 1,623.00 | -0.63% | Removed |
| Cheaper cache index plus buffer reservations | 1,711.52 | 1,696.48 | -0.88% | Removed |
| Buffer reservations with original cache index | 1,677.52 | 1,651.66 | -1.54% | Removed |

Every 1 GiB candidate produced exactly 7,946,802 durable bytes and passed all
128 block checksums plus sampled exact reconstruction.

### Final 80 GiB gate

| Metric | Initial Pco-8 default | Optimized default | Change |
| --- | ---: | ---: | ---: |
| Durable bytes | 628,473,667 | **628,473,667** | identical |
| Compression ratio | 136.68x | **136.68x** | identical |
| Ingest seconds | 74.157 | **42.344** | -42.90% |
| Ingest throughput | 1,104.68 MiB/s | **1,934.63 MiB/s** | +75.13% |

The optimized run consumed 85,899,345,826 complete-line bytes, accepted
85,899,343,853 bytes and 607,363,459 records, rejected the same seven malformed
records totaling 1,973 bytes, and emitted 10,240 blocks. It reproduced the
existing component totals exactly: 15,847,519,672 structural bytes,
627,654,450 pack bytes, and an 819,217-byte manifest. All 10,240 payload
checksums passed, and first/middle/last sampled records reconstructed exactly.
External wall time was 43.19 seconds, with 524.34 user-seconds, 92.88
system-seconds, and 370,788 KiB maximum RSS.

```text
corpus SHA-256       4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508
v5 source archive    4e43a39d7d4db018de810f87b9baa3084f01ef9baa09d7e17d78f54ca97f163a
v5 benchmark binary  83b6bf3bb6789fe9c556f510ffe642d0b124f202d504c1ea0c2458fc8698d87f
full report          final-v5-full-80g-report.txt
wall-time evidence   final-v5-full-80g-time.txt
```

Execution tier was native pinned multicore on Adam. This is host-level
performance evidence, not deterministic task-schedule replay. The
deterministic-simulation framework revisions were
`62f4e527284add129faf9d9d3bfd1ec99f65f26e` locally and
`bd1c4ba7eff99a5f324a5434b1a9d23444e32582` on Adam; both checkouts were dirty.
The ShardLog repository was unborn/uncommitted, so retained archives and
binary hashes are the replay boundary.

## Current default — path-by-path single-core optimization

Run date: 2026-07-30  
Host: Adam, Ryzen 9 3950X, Linux 6.8.0-111-generic  
Single-core gates: `taskset -c 0`, one worker, prewarmed source  
Multicore gate: CPUs `0-15`, one worker per physical core  
Block target: 8 MiB  
Locality and real-time dictionary: disabled  
Evidence:
`/home/dtietjen/shard-log-head-to-head/single-thread-optimization-20260730-v1`

The optimization pass treated ingestion as a sequence of independently
measurable costs:

```text
read immutable bytes
  -> discover complete lines
  -> parse Docker JSON and timestamps
  -> identify exact message layouts
  -> build structural columns
  -> encode Pco timestamps
  -> compress with zstd-1
  -> write/sync packs and manifest
```

Each candidate was built as a separate native binary and alternated with its
control on the same prewarmed 1 GiB source and CPU. Every accepted candidate
stored exactly 7,946,802 durable bytes in that gate and passed all 128 block
checksums plus sampled exact reconstruction. Percentages below are paired
measurements and must not be added together.

| Path component | Accepted change | Paired gain |
| --- | --- | ---: |
| Line discovery | Scan newlines with `memchr` and retain borrowed ranges | +9.91% |
| Docker timestamp | Bound parsing to the known timestamp tail | +15.29% |
| Timestamp calendar prefix | Cache the repeated RFC3339 date/hour prefix | +13.34% |
| Timestamp fraction | Unroll the common nanosecond fraction | +2.54% |
| Docker adapter reuse | Cache exact normalized records | +2.19% |
| Structural layout | Cache exact parsed body layouts | +2.16% |
| Integer serialization | Add the common one-byte varint path | +2.63% |
| Pco model selection | Fix classic mode and first-order consecutive delta after a real-block probe | +3.18% |
| Template selection | Count work once per distinct layout | +1.82% |
| Layout cache lookup | Index with length plus first/last eight-byte samples; retain exact equality gate | +2.73% |
| Shared messages | Accept pointer-identical body slices before byte comparison | +1.58% |
| Timestamp fraction | Validate and fold eight ASCII digits with checked SWAR arithmetic | +1.69% |
| Body serialization | Pre-encode exact fragments for repeated layouts | +3.40% |
| Input transfer | Read-only `mmap` in the corpus harness, exposing borrowed worker slices | +10.62% |
| Layout working set | Store bounded per-record layout IDs as `u32` | +0.36% |

The medians advanced from approximately 499 MiB/s before this sequence to
1,031.55 MiB/s in the final paired 1 GiB cohort, a 2.07x one-core gain.
Independent 8 GiB validation reached 1,122.91 MiB/s (1.10 GiB/s) on one
physical core. The `mmap` change belongs only to the benchmark adapter; the
storage library remains safe Rust and accepts stable borrowed input from any
ingestion source.

The independent full 80 GiB one-core gate then reached 1,157.26 MiB/s in
70.7876 timed seconds and produced the same 628,417,043 durable bytes as the
16-core run. External process wall time was 75.40 seconds, including
verification and teardown. Its reported 83,915,148 KiB maximum RSS is
file-backed mapped corpus pages already present in the page cache, not an
80 GiB private heap requirement.

### Rejected candidates

Negative results were restored rather than accumulating speculative branches:

| Candidate | Result | Decision |
| --- | --- | --- |
| Pco level 7 | 3.81% more stored bytes and 1.98% slower | Restore level 8 |
| Lighter Docker cache hash | 0.11% slower | Restore the stronger sampled hash |
| Canonical general Docker JSON parser | 0.63% slower | Keep specialized validated path plus fallback |
| Cheaper cache index plus broad reservations | 0.88% slower | Remove |
| Broad buffer reservations alone | 1.54% slower | Remove |

The fixed Pco model is not a corpus-assuming wire-format shortcut: Pco's
automatic mode probe selected the same classic/first-order model for every
sampled real block, and the fixed path retains lossless Pco framing. Exact
message comparisons similarly remain the authority behind all body cache hits.

### Current profile and remaining costs

An 8 GiB one-core `perf` run sustained 1,118.94 MiB/s. Approximate inclusive
samples in the timed hot path were:

| Cost center | Samples |
| --- | ---: |
| Structural encoding overall | 14.19% |
| Per-block processing/orchestration | 10.72% |
| Timestamp parsing | 6.43% |
| Newline search | 5.41% |
| Docker normalization cache | 5.09% |
| Pco histogram/model/binary/ANS/dissection work | 14.70% combined |
| zstd-1 | 3.50% |
| Attribute construction | 2.64% |
| Body encoding | 2.41% |
| Varints | 1.08% |

This changes the optimization priority. Pco is now the largest coherent CPU
consumer, but lowering its level failed both size and speed. The next useful
work is targeted internal Pco profiling or a purpose-built timestamp codec
that must beat its 7.02 bits/value result—not more message grouping on this
already repetitive corpus. The parser remains the next generic target:
fusing newline discovery with Docker framing, reducing timestamp fallback
branches, and accepting native OTLP timestamps without RFC3339 conversion.

### Physical-core scaling

All rows used the same prewarmed 8 GiB prefix, 8 MiB blocks, durable
pack/manifest output, and post-ingest verification:

| Physical cores | MiB/s | GiB/s | Scale from one core |
| ---: | ---: | ---: | ---: |
| 1 | 1,122.91 | 1.10 | 1.00x |
| 2 | 1,491.85 | 1.46 | 1.33x |
| 4 | 2,695.98 | 2.63 | 2.40x |
| 8 | 5,008.73 | 4.89 | 4.46x |
| 16 | 6,999.68 | 6.84 | 6.23x |

CPUs `0-15` are separate physical cores on Adam; SMT siblings are `16-31`.
Scaling is intentionally reported rather than called linear: shared memory
bandwidth, page cache, filesystem synchronization, and per-core turbo become
visible above one worker.

### Authoritative full 80 GiB head-to-head

The final sequential harness prewarmed the immutable source separately before
each engine. ShardLog and ClickHouse 26.5.1.882 each received CPUs `0-15`.
ShardLog synchronized 16 pack files and its manifest before stopping the
timer. ClickHouse enabled `fsync_after_insert`; the harness flushed the table
and measured active-part bytes. Both accepted 607,363,459 records spanning
85,899,343,853 source bytes.

| Engine | Durable bytes | Ratio | Seconds | MiB/s |
| --- | ---: | ---: | ---: | ---: |
| **ShardLog current default** | **628,417,043** | **136.69x** | **10.8206** | **7,570.75** |
| ClickHouse 26.5.1.882 | 1,175,169,126 | 73.10x | 88.3300 | 927.43 |

ShardLog was 8.16x as fast and used 546,752,083 fewer bytes, a 46.52%
reduction from ClickHouse's stored size. Its storage comprised 627,597,826
compressed payload bytes and an 819,217-byte manifest; structural data before
zstd was 15,847,462,930 bytes. The result is 56,624 bytes smaller than the
first Pco-8 optimized implementation and 3.91x its full-corpus throughput.
All 10,240 payload checksums passed and sampled first/middle/last records
decompressed and reconstructed exactly.

Current source and binary reconstruction boundary:

```text
source archive  80edd919e79fea5a64f09627002de84d7fcdd38ae065ee310eff936fdec81035
Adam binary     78e76a927f82937cc8ce9ce017ecb6063310a52a8c2aed8cc6719f84b386c7df
```

Key evidence checksums:

```text
head-to-head summary          07a74e830c19dc2a66af58726ca7617ce53d2900f7375e93a65f3d73acb54e15
16-core ShardLog report       d8fa443934f7057c084ddc14780a51eb979ec34a6757011ca5fa8c915a590a79
ClickHouse active parts       8dc59b4010d60824b7320d53009a570a5f68bb0a3e5e175a0494b728dd684054
head-to-head provenance       77a96bf9efd940ac3f40ecf01a44225344c57255f7baac4de15053623cf385b2
one-core ShardLog report      aba3310b3995f9bc0f97b4e7605235a74d168bd633a1c0f45d78158934fafd4f
one-core external time        10c7bf2b85c9fb9fd64241a993b0784a81883f037a8b8dee91cd5b320d94607c
one-core provenance           fcc702f1912e9b15716f7e68587cb0307c108655f2c0205c720e7125bd9ff748
```

This is native Linux host-performance evidence, not logical or exact
task-schedule replay. The deterministic-simulation framework revisions were
`62f4e527284add129faf9d9d3bfd1ec99f65f26e` locally and
`bd1c4ba7eff99a5f324a5434b1a9d23444e32582` on Adam; both framework
checkouts were dirty. The ShardLog repository was unborn/uncommitted, so the
retained source archive and binary hashes are the reconstruction boundary.

## Real-time dictionary learning

Run date: 2026-07-29 local  
Host: Apple silicon, macOS 26.5, Darwin 25.5.0  
Rust: 1.93.0  
Zstd crate: 0.13.3  
Workers: 8  
Locality: disabled  
Evidence: `/private/tmp/shard-log-realtime-dictionary-local-20260729-v1`

### What was tested

The learner samples only the exact structural lanes that can contribute
reusable Zstd vocabulary: templates, encoded bodies, attribute tables, and
fields. It excludes offset and Pco timestamp sections. One background worker
trains a 64 KiB candidate from 4 MiB of bounded samples. Later observations
are shadow-compressed in 16-block batches. Admission accumulates actual
held-out savings until they repay the dictionary plus a 16 KiB minimum net
gain; a losing candidate or one that cannot repay itself within 256 MiB of
observed structural data is rejected.

The source began with 26,393,298 bytes of genuine local install, Wi-Fi, and
launchd text logs. A lossless adapter emitted 175,169 Docker JSON records
totaling 38,672,775 bytes:

```text
4532294b8931bacd26692f4d5d52163e5dea7fa3937bf574a8eff5bbd78bf850
```

That adapted corpus was repeated 28 times, preserving its internal order, to
create a 1,082,837,700-byte stationary stream:

```text
2a92f487711c1515d35e1ebe5066c0a39ce67868f27bdcc73151f1f7277581ff
```

The benchmark consumed the largest complete-line prefix at 1 GiB:
1,073,741,627 bytes and 4,862,297 records. Repetition intentionally provides a
long stable lifetime in which a live dictionary can repay training; it does
not represent 1 GiB of independent production events.

### 512 KiB blocks

Both modes wrote 2,048 independent blocks, synchronized eight pack files and
the ordered manifest, verified all 2,048 payload checksums, and exactly
reconstructed sampled first, middle, and last blocks.

| Mode | Pack bytes | Manifest/assignments | Dictionary | Durable bytes | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 45,111,026 | 163,857 | 0 | 45,274,883 | 23.72x |
| Real-time | 42,871,929 | 163,970 | 65,536 | **43,101,435** | **24.91x** |

The online generation saved 2,173,448 durable bytes, or 4.80%, after charging
both the immutable dictionary and three sparse assignment runs. The final run
observed all 2,048 blocks, dropped 183 training observations from its bounded
queue without ingestion backpressure, trained two candidates, published one,
and rejected one. Shadow evaluation compressed 2,812,417 baseline bytes to
2,662,420 candidate bytes. Training and evaluation consumed 0.146 and 0.043
CPU-seconds respectively.

Three prewarmed disabled runs measured 1,448.59 MiB/s median. The corresponding
online implementation measured 1,358.87 MiB/s median, 6.19% lower but 334.87
MiB/s above the 1 GiB/s gate. The online publication boundary is asynchronous:
repeated runs differed slightly in how many already-sealed blocks adopted the
generation. This changes compression efficiency, never reconstruction or
dictionary identity. The final sparse-sidecar run above is the durable byte
accounting result.

### 8 MiB control

| Mode | Pack bytes | Manifest/assignments | Dictionary | Durable bytes | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 38,826,286 | 10,257 | 0 | **38,836,543** | **27.65x** |
| Real-time | 38,826,286 | 10,257 | 0 | **38,836,543** | **27.65x** |

The candidate reduced sampled held-out frames from 3,678,051 to 3,638,588
bytes, but that 39,463-byte gross gain did not repay a 65,536-byte dictionary
within the 256 MiB observation cap. The learner rejected it, emitted no
dictionary object or assignment file, and produced byte-identical durable
storage. This is the intended fail-open behavior.

The larger dictionary-free blocks still beat 512 KiB dictionary-backed blocks
on ratio (27.65x versus 24.91x). A real-time dictionary is useful when query
latency or bounded decode amplification requires small independent blocks; it
does not supersede larger blocks where those are acceptable.

These local runs are native parallel performance measurements, not
deterministic task-schedule replays and not substitutes for Adam acceptance.
Adam became unreachable because the client had no DNS configuration before
the final exact-lane archive could be deployed. The retained Adam 1 GiB
message-body and mixed-structural precursors both rejected their candidates
and left pack payload bytes unchanged. The remaining acceptance run is the
final exact-lane learner on the immutable 80 GiB corpus with CPUs `0-15`, plus
the non-repeated Pluribus/Eden/OTEL interleaving.

## Query and log-lookup optimization

Run date: 2026-07-30  
Adam host: `dtietjen@ssh.tryeden.dev`, AMD Ryzen 9 3950X  
Adam evidence:
`/home/dtietjen/shard-log-query-optimization-20260730-v1`  
Local comparison:
`/private/tmp/shard-log-v3-compare.8FPULe`

### Hot stripe

The original implementation converted the first posting to a hash set and
called linear `Vec::contains` for every later constraint. Query term order
therefore changed latency, and a three-term query became quadratic.

The optimized path uses borrowed nested partition maps, offset windows,
selectivity ordering, sorted merge/galloping intersections, ordered
partition postings, reference-only results, and early ordering/limits. Adam
used CPU 0, 100,000 records, and 20 measured iterations:

| Query | Original | Optimized v2 | Improvement |
| --- | ---: | ---: | ---: |
| common AND rare | 11.765 ms | 32.03 us | 367x |
| rare AND common | 4.299 ms | 31.90 us | 135x |
| common AND medium AND rare | 849.269 ms | 51.35 us | 16,540x |
| metadata AND rare | 398.11 us | 15.69 us | 25.4x |
| newest common, limit 100 | not available | 12.97 us | bounded |
| newest all, limit 100 | not available | 12.94 us | bounded |
| 100-offset window | not available | 13.00 us | bounded |

The two term orders now have equivalent cost. The miss path remained below
100 ns. Evidence files are `hot-baseline-100k.txt` and
`hot-sorted-postings-v2-100k.txt`.

### First persistent 1 GiB result

The first persistent implementation used 128 independently compressed 8 MiB
blocks and an exact term/metadata index:

| Component | Bytes |
| --- | ---: |
| Source | 1,073,741,671 |
| Structural | 198,142,239 |
| Pack payload | 7,936,545 |
| Manifest | 10,257 |
| Query index | 1,407,758 |
| Total durable | 9,354,560 |

This is 114.78x total compression. The query index added 17.7% over the tiny
pack payload but only 0.13% of raw source bytes. All 128 checksums and sampled
exact reconstruction checks passed. Index-enabled ingest sustained 762.12
MiB/s, below the 1 GiB/s acceptance floor and therefore not the final default
result.

Warm exact queries returned 100 records from one block:

| Query | Plan | Read + zstd + selective decode |
| --- | ---: | ---: |
| newest 100, no filter | 11.18 us | 2.717 ms |
| term `cannot` | 2.52 us | 2.537 ms |
| five-term error AND | 32.96 us | 2.560 ms |
| `docker.stream=stderr` | 12.22 us | 2.781 ms |
| missing term | 0.08 us | 0.10 us |

The corresponding cached-structural measurements were 2.31–2.61 ms, proving
that payload I/O and zstd were not the primary cost. `perf` attributed 27.24%
to UTF-8 validation, 24.13% to body walking, 15.73% to field walking, 13.35%
to varints, 5.80% to offset/Pco decoding, 2.27% to zstd, and 1.14% to pack
reads. Evidence includes `persistent-index-1g-v3-report.txt`,
`pack-*-v3.txt`, and `pack-query-v3-perf.data`.

### Checkpointed selective decoding

A 256-record seek footer now follows each structural body and field lane.
Directories are appended after the original record bytes so the compressible
payload remains byte-identical at the front. The strict full decoder verifies
every checkpoint against an actual record boundary.

The same deterministic synthetic block was built with the archived v3 source
and the current source:

| Metric | Scan-all v3 | Checkpoint footer |
| --- | ---: | ---: |
| Records | 60,000 | 60,000 |
| Structural bytes | 5,580,222 | 5,581,410 |
| zstd-1 bytes | 148,554 | 148,562 |
| Full decode | 15.25 ms | 15.24 ms |
| Newest contiguous 100 | 2.684 ms | about 0.175 ms |
| Every 100th record, 100 hits | not measured | about 0.460 ms |

The contiguous selective path improved about 15x while adding only eight
compressed bytes in this test. The current `shard-log-selective-decode-bench`
also measures query-index construction. Message/term direct maps, recycled
term-ID vectors, duplicate-safe last-ordinal checks, and removal of a
per-record metadata set increased that synthetic build rate from 742,463 to
approximately 1.14 million records/s per core.

Persistent postings now retain dense runs after decode instead of expanding
them into complete ordinal vectors. `shard-log-pack-query-bench` reports both
logical posting cardinality and resident posting-array/run bytes. On this
synthetic block, 1,736,000 logical ordinals occupied 1,452,352 resident bytes
instead of 6,944,000 bytes as a flat `u32` representation, a 79.1% reduction
for posting payload storage. A dense two-constraint newest-first plan with a
100-record limit took about 0.35 microseconds locally because it walks runs
backward and stops as soon as the limit is satisfied.

The checkpoint/index-memory candidate has passed the complete local test suite
and warning-denied Clippy. A final Adam result requires rebuilding the
1 GiB pack in the new single pre-release structural format; the older v3 pack
cannot be queried by the new decoder.

### Complete lookup-contract benchmark

Run date: 2026-07-30  
Command:
`cargo run --release --bin shard-log-query-bench -- --records 100000 --iterations 100`  
Environment: local arm64 release build, one `LogStripe`

This run exercises the complete lookup contract added after the original
posting optimization. Timings include result-reference collection and cloning
complete matching records.

| Query | Matches | Mean |
| --- | ---: | ---: |
| Rare token | 100 | 5.42 us |
| Newest common, limit 100 | 100 | 5.39 us |
| Newest all, limit 100 | 100 | 5.19 us |
| 100-offset window | 100 | 4.88 us |
| Common AND rare | 100 | 14.77 us |
| Rare AND common | 100 | 14.70 us |
| Common AND medium AND rare | 100 | 22.52 us |
| Exact field AND rare | 50 | 8.71 us |
| Missing token | 0 | 0.069 us |
| Common AND (error OR rare) | 25,000 | 32.95 ms |
| Message contains | 100 | 9.51 ms |
| Message regex | 100 | 5.35 ms |
| Field exists | 100,000 | 18.71 ms |
| Field set membership | 12,500 | 8.48 ms |
| Numeric field comparison | 10,000 | 9.11 ms |
| Newest timestamp, limit 100 | 100 | 6.33 ms |
| Offset cursor, next 100 | 100 | 3.32 ms |

The first nine rows are posting or ordered-window plans. The remaining rows
are exact residual scans over 100,000 records; broad rows also clone every
returned record. The planner deliberately returns a safe superset for those
operators and does not consume the query limit until after residual filtering.
Index construction for the same fixture sustained 868,818 records/s.

Compatibility is tested across nested Boolean expressions, every literal text
mode, message and field regex, field existence and set membership, all signed
integer comparisons, non-monotonic timestamp ranges and ordering, stable
cursors, and selected/all-stripe fan-out. The same expected offsets must be
returned by the hot stripe and by persistent-index candidates round-tripped
through the structural encoder and decoder.

## Historical sealed/cold baseline versus ClickHouse — 80 GiB

Run date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Evidence:
`/home/dtietjen/shard-log-query-head-to-head/cold-current-v2-20260730T221500Z`

This run answers the non-hot-record case. It reused the verified
607,363,459-row ShardLog pack and ClickHouse snapshot from the indexed 80 GiB
campaign. Both engines ran sequentially on physical CPUs `0-15`. Search
indexes remained resident; immediately before each cold sample the harness
used `POSIX_FADV_DONTNEED` on ShardLog pack files and ClickHouse's immutable
`time`, `stream`, and `log` payload files. ClickHouse's result and query caches
were disabled. This is a local-SSD payload-cold comparison, not a remote
object-store latency measurement.

Every query first emitted timestamp, stream, and message results. The harness
required byte-identical ShardLog and ClickHouse files before timing. Warm rows
use 20 iterations and cold rows use five, except the deliberately expensive
substring miss, which uses one.

| Lookup, limit 100 | ShardLog warm p50 | ClickHouse warm p50 | ShardLog cold p50 | ClickHouse cold p50 |
| --- | ---: | ---: | ---: | ---: |
| Latest | **1.055 ms** | 508 ms | **1.414 ms** | 666 ms |
| `docker.stream=stderr` | **1.054 ms** | 5 ms | **1.396 ms** | 21 ms |
| Token `cannot` | **1.237 ms** | 529 ms | **1.633 ms** | 689 ms |
| Five-token AND | **1.994 ms** | 568 ms | **2.355 ms** | 729 ms |
| Missing token | **0.00020 ms** | 11 ms | **0.00042 ms** | 11 ms |
| Message contains, positive | **75.613 ms** | 583 ms | **76.367 ms** | 764 ms |
| Message regex, positive | **72.193 ms** | 597 ms | **72.892 ms** | 788 ms |
| Message contains, missing | 35,930 ms | **1,672 ms** | 35,918 ms | **1,885 ms** |

Indexed ShardLog reads one block and remains within about 1.4-2.4 ms at cold
p50. Cold latest is 471x faster, the favorable ClickHouse stream lookup is
15x faster, positive contains is 10.0x faster, and positive regex is 10.8x
faster. A missing token is rejected by resident postings without a payload
read.

The missing substring reverses the result: ClickHouse is 19.1x faster cold.
Neither engine can reject it with the configured text index, but ClickHouse
scans its body column vectorially. ShardLog's 16-worker fallback currently
decodes complete structural records across all 10,240 blocks. It processes the
corpus in 35.92 seconds without constructing a corpus-wide candidate vector,
but body-only predicate pushdown and block-level n-gram rejection are now the
highest-value query optimizations.

Payload temperature is not ShardLog's largest indexed-query problem. The
current global index still takes 23.13-23.23 seconds to load and expands its
113,853,148-byte file into 17,071,691,984 bytes of resident posting storage.
The latency table excludes this one-time process startup for both long-lived
engines. Segmented mmap-friendly postings are required to remove that cold
start and make object-tier serving production-ready.

The source archive is identified by SHA-256
`41e96972ba58c86476edc05d7428047f4546669f6d0fcc69a0ec0b98dae772f2`;
the benchmark binary by
`eca589c1d944c4c2d21ef3d4d731574f85fc8e2071496f1a4cbe785517741328`;
and the harness by
`5f77f147bdfa7e259c451c207745964b046002772cf4904fdc113c3e5192c158`.
The ShardLog repository is still unborn, so those content hashes—not a commit
ID—identify the tested source. The ClickHouse image remained pinned to
`sha256:770156c537ca9124046e138a3b5845c64ea58ce8722de7a2e05fd827f4976520`.

## Block-trigram substring rejection — 80 GiB

Run date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Final query evidence:
`/home/dtietjen/shard-log-query-head-to-head/cold-trigram-v5-20260730T185500Z`  
Final ingest evidence:
`/home/dtietjen/shard-log-query-head-to-head/missing-substring-v5-build-20260730T185600Z`

The `SLOGQIX2` query directory adds one 65,536-bit lowercase message-trigram
filter per 8 MiB source block. Filters are necessary-condition metadata:
missing bits reject blocks, while present bits still require the existing
exact decoded-record predicate. Hash collisions can add work but cannot hide
records. Only message literals required by the query's conjunction are used;
OR and NOT branches remain fail-open.

The resident directory stores all filters in one contiguous 80 MiB allocation.
Two derived 8 KiB masks avoid touching it in the common extremes. A missing
bit in the global union rejects the entire corpus, while bits in the global
intersection prove that every block is a candidate. The latter recovered the
positive-query cache behavior that was lost in the first block-major
implementation.

### Implementation ablation

All rows used the same 10,240 blocks, 607,363,459 records, 16 physical CPUs,
and byte-identical positive/negative results.

| Version | Missing contains cold p50 | Positive contains warm p50 | Positive contains cold p50 |
| --- | ---: | ---: | ---: |
| No trigram metadata | 35,918 ms | 79.595 ms | 80.826 ms |
| Block filters, scan every block | 0.885 ms | 89.578 ms | 89.059 ms |
| Union/intersection, boxed block filters | 0.00091 ms | 87.400 ms | 86.934 ms |
| Union/intersection, contiguous block filters | **0.00091 ms** | **80.487 ms** | **81.770 ms** |

The final positive path is within 1.2% of the sequential no-filter control,
while the cold full miss is about 39.5 million times faster and reads no
payload. The intermediate boxed layout is retained only as benchmark evidence;
the pre-release code has one current format and one contiguous resident
layout.

### Final sequential ClickHouse comparison

Both engines ran sequentially on physical CPUs `0-15`. The harness kept search
indexes resident, evicted immutable payload pages before each cold sample,
disabled ClickHouse result/query/filesystem caches, and required byte-identical
timestamp, stream, and message output before timing. Warm rows use 20
iterations and cold rows use five.

| Lookup, limit 100 | ShardLog warm p50 | ClickHouse warm p50 | ShardLog cold p50 | ClickHouse cold p50 |
| --- | ---: | ---: | ---: | ---: |
| Latest | **1.336 ms** | 511 ms | **1.776 ms** | 663 ms |
| `docker.stream=stderr` | **1.346 ms** | 5 ms | **1.788 ms** | 21 ms |
| Token `cannot` | **1.511 ms** | 527 ms | **1.942 ms** | 694 ms |
| Five-token AND | **2.181 ms** | 567 ms | **2.617 ms** | 718 ms |
| Missing token | **0.00020 ms** | 11 ms | **0.00043 ms** | 12 ms |
| Message contains, positive | **79.131 ms** | 581 ms | **86.284 ms** | 753 ms |
| Message regex, positive | **79.259 ms** | 596 ms | **78.998 ms** | 775 ms |
| Message contains, missing | **0.00091 ms** | 1,624 ms | **0.00091 ms** | 1,794 ms |

Cold ShardLog is 8.7x faster for the positive substring and about 1.97 million
times faster for the missing substring. Regex receives no trigram pruning in
this version; its 9.8x cold advantage comes from stopping after the first
matching block.

Every one of the eight ShardLog result files has the same SHA-256 as its
ClickHouse counterpart. The missing result is the canonical empty-file digest
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`;
the positive substring pair is
`173edb4e10771f7d0ea9bdc54e02fb013a9ceaa9c00423aa979b2c28ff0dd128`.

### Ingest and storage cost

The final source rebuilt the complete immutable corpus and verified all 10,240
payload checksums plus first/middle/last record reconstruction in every block.

| Metric | No trigram metadata | Final trigram directory | Change |
| --- | ---: | ---: | ---: |
| End-to-end indexed ingest | 2,311.95 MiB/s | 2,147.10 MiB/s | -7.1% |
| Ingest elapsed | 35.433 s | 38.154 s | +7.7% |
| Compressed query index | 113,853,148 B | 114,420,962 B | +567,814 B |
| Pack + manifest + index | 743,581,572 B | 744,149,386 B | +567,814 B |
| Source-to-total ratio | 115.52x | 115.43x | -0.08% |
| Peak RSS | 102,406,520 KiB | 102,389,728 KiB | effectively unchanged |

The raw filters occupy 83,886,080 bytes resident but their repetitive bit
patterns add only 567,814 compressed bytes: 0.50% of the prior index and
0.00066% of the source corpus. Indexed ingest remains 2.10x above the 1 GiB/s
acceptance floor.

The Adam-tested implementation archive SHA-256 is
`4d26ba4bd28053d22f8d08738e368ef079ecaa6432b062353770ae49d6289a80`.
The query binary is
`5af88391e19efb85854c74fb1e9475e4f52b0a1234b4ef973362036669881db3`;
the ingest binary is
`22d4b6e31ab91809143a7a4aee348da843248f3407c7fb77a80be3fe3dd2532a`;
and the immutable `query-index.bin` is
`fa22f831534d6a429c019c52276da5591e8927d73480c77063b943156a789481`.

## Native protocol versus Loki JSON adapter — 1 GiB

Run date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Evidence: `/home/dtietjen/shard-log-native-ab-v7`

This sequential ablation compares ShardLog's native binary protocol with its
Loki-compatible JSON push boundary. It is not a ShardLog-versus-Loki-engine
comparison. Both legs used the same final release server and loader, CPUs
`0-15`, 16 physical/index stripes, 16 persistent client connections, 1 MiB
target batches, indexed acknowledgements, fresh directories, and the same
1,073,741,933 source bytes (7,592,023 records) from the immutable 80 GiB
corpus.

| Protocol | Elapsed | Source throughput | Client wire bytes |
| --- | ---: | ---: | ---: |
| Native grouped TCP | **14.959744 s** | **68.45 MiB/s** | **832,404,552** |
| Loki HTTP JSON | 17.000543 s | 60.23 MiB/s | 953,863,512 |

The native protocol improved end-to-end durable-and-indexed ingestion by
13.6% and reduced client-to-server bytes by 12.7%. It stores labels once per
stream, uses fixed-width timestamps and counts, and decodes directly into
normalized events with stream label fields shared through `Arc`. It avoids
HTTP headers, JSON parsing/escaping, `LokiEntry` maps, and OTLP protobuf
transcoding.

| Durable component | Native | Loki JSON |
| --- | ---: | ---: |
| shard-stream data | 832,597,320 B | 832,630,024 B |
| Sink recovery journal | 832,481,480 B | 832,502,536 B |
| Total | 1,665,078,800 B | 1,665,132,560 B |

The durable outputs differ by only 53,760 bytes because the Loki adapter
converts accepted entries into the same native grouped representation before
append. Both server logs were empty. Binary SHA-256 identifiers:

- server:
  `ba6ed14975058087792a1b54a7c5937fe6fbd202788055894d101d7c972cd8d5`
- loader:
  `0d4b3dbb049b8a5bc9f1133bda9d33f716304c1d461e1b3c83e78570177b0504`

Those hashes identify the timed binaries. The subsequent restart gate found
that one logical partition can migrate between physical owner shards, so
journal recovery now merges all stripe journals by
`(topic, partition, placement sequence)` before replay. The current server
binary
`88dbbb3bcc5f9861bc4cc17e2ada0394a7d09b7af7b1eecc8049757b342561cc`
reopened the retained 1 GiB native directory, replayed all 7,592,023 records,
and became ready in approximately 32 seconds with an empty recovery log. The
current loader binary is
`5e486ffe4e0f0e9aea729021369c070b214650872c70700ba4871acfff4b23d8`.
The replay remains single-threaded and is now an explicit startup-throughput
optimization target.

The run also exposed and fixed an offset invariant at the shard-stream
boundary. Offsets are lane-global and strictly increasing for a logical
partition, but may contain gaps occupied by sibling partitions. ShardLog now
accepts monotonic gaps while preserving exact IDs and rejecting duplicates or
regressions. A three-batch durable/restart test covers this behavior.

The complete 80 GiB protocol comparison is intentionally deferred. With
compressed block publication and WAL/journal reclamation still open, each
current durable leg retains approximately two copies of its native payload;
running both full legs would consume most of Adam's remaining space and would
measure a known temporary storage path rather than the production design.

## Compression-derived durable index — 1 GiB production path

Run date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
CPUs: physical cores `0-15`, shared by server and native loader  
Corpus: 1,073,741,933 source bytes and 7,592,023 complete records from the
immutable 80 GiB corpus  
Protocol: native grouped TCP, 16 owner stripes, 16 persistent connections,
1 MiB target batches, indexed acknowledgements  
Recovery journal: disabled; the checksummed compressed ingest pack is the
authoritative shard-stream payload

This run replaces the production path's decoded-record map and independent
term/field postings with the index built by structural compression itself.
Live handoff carries only the encoded index section. Restart recovers that
section from the compressed frame.

### Durable ingest

| Configuration | Elapsed | Source throughput |
| --- | ---: | ---: |
| Pipeline depth 1 | 1.961421 s | 522.07 MiB/s |
| Pipeline depth 4 | 1.641291 s | 623.90 MiB/s |
| Pipeline depth 8, initial frame descriptor | 1.619862 s | 632.15 MiB/s |
| Pipeline depth 16 | 1.619783 s | 632.18 MiB/s |
| 32 connections, pipeline depth 4 | 1.666580 s | 614.43 MiB/s |
| **Final timestamp-aware frame, pipeline depth 8** | **1.601187 s** | **639.53 MiB/s** |

The pipeline-depth plateau is between 8 and 16; adding connections did not
improve it. The final run is 9.34x the retained 68.45 MiB/s native service row
above. Even the depth-one result is 7.63x that row, so pipelining is not the
source of the architectural gain.

Final durable storage was 9,892,892 bytes, or **108.54x** source-to-disk
compression (0.9213% retained). This total includes shard-stream pack
envelopes, coordinator metadata, and manifests. For context, the matching
structural-only 1 GiB run stored 8,362,652 bytes at 128.40x; durable framing
therefore adds about 1.53 MB. Server RSS after the ingest and query series was
373,256 KiB. That is a settled observation, not a sampled peak.

### Exact native query latency

Query: label `source=clickhouse-docker`, token `runnableEntry`, a 60-second
timestamp range covering the loaded data, newest first, limit 10. Every
response contained the same 10 records and 1,080-byte native payload.

| Coordinator/index stage | Mean | p50 | p99 |
| --- | ---: | ---: | ---: |
| Per-partition fanout, scan all positive frames | 724.939 ms | 724.280 ms | 737.362 ms |
| One batched fanout per stripe | 568.122 ms | 567.767 ms | 586.321 ms |
| **Batched fanout + frame min/max top-K** | **12.682 ms** | **11.307 ms** | **16.708 ms** |
| Missing token, final path | **0.314 ms** | **0.313 ms** | **0.343 ms** |

Frame bounds make the positive limited lookup 44.8x faster than batching
alone. A missing token opens no compressed frame. A stop/reopen cycle rebuilt
the embedded indexes from the authoritative packs, reported ready, and
returned the same 10-record exact result.

The Loki-compatible substring query `|= "runnableEntry"` remains a residual
scan in this frame-index implementation. It took 19.26 seconds before the
top-K work because arbitrary substring literals cannot safely use whole-token
locators. Folding the existing bounded trigram rejection into the embedded
frame index is required before this path replaces the older sealed-query
directory for all LogQL predicates.

### Profile

A 4 GiB profiled run sustained 547.29 MiB/s under `perf` instrumentation.
Largest server self-costs were:

| Symbol/category | Self cycles |
| --- | ---: |
| Structural field encoding | 7.82% |
| UTF-8 validation | 6.87% |
| Structural/index orchestration | 6.78% |
| BLAKE3 hashing | 5.50% |
| Native event decode | 5.27% |
| Free + malloc | 4.76% |
| Attribute value counting | 2.49% |
| Zstandard block compression | 1.46% |

Compression itself is no longer the production bottleneck. The next write-path
optimization should parse native records into borrowed structural views,
eliminating owned-string construction and the corresponding UTF-8/allocation
work before changing codecs.

### Final full 80 GiB durable run

The final format and service path completed the complete immutable corpus with
indexed acknowledgements:

| Metric | Result |
| --- | ---: |
| Source bytes | 85,899,345,920 |
| Accepted records | 607,363,459 |
| Malformed Docker wrapper records skipped | 8 |
| Native wire bytes | 66,592,152,397 |
| Ingest elapsed | 122.369951 s |
| Source throughput | **669.45 MiB/s** |
| Total durable bytes | **780,387,953** |
| Source-to-disk ratio | **110.07x** |
| Source retained | 0.9085% |
| Durable files | 1,842 |
| Open descriptors after ingest | 44 |
| Peak server RSS | 2,240,432 KiB |
| Peak loader RSS | 273,828 KiB |

The first full attempt exposed an unbounded shard-stream `pack_readers` map:
every rolled pack retained a `File`, reaching Adam's 1,024 soft descriptor
limit after 991 files. A bounded per-shard cache fixed live ingest but still
multiplied its capacity by 16 shards during recovery. The final implementation
opens immutable pack readers on demand and closes them after each coalesced
read. A dedicated 81-pack append/reopen/fetch test and the full run verify the
fix. The complete run finished with 44 descriptors despite 1,842 durable
files.

Full-corpus native query results:

| Exact query, limit 10 | Mean | p50 | p99 |
| --- | ---: | ---: | ---: |
| `source=clickhouse-docker AND runnableEntry` | **12.890 ms** | **12.978 ms** | **16.485 ms** |
| Missing token | **1.658 ms** | **1.626 ms** | **2.296 ms** |

Cold reopen reconstructed all frame indexes, became ready within a measured
25.1-second upper bound, held 44 descriptors, and returned the same exact
10-record result. Recovered RSS was 1,869,216 KiB before that query.

Using the retained same-corpus, same-host, same-CPU ClickHouse and Loki rows
above, the now-production-shaped comparison is:

| Engine | Durable/settled bytes | Ratio | Throughput |
| --- | ---: | ---: | ---: |
| **ShardLog compressed-frame index** | **780,387,953** | **110.07x** | **669.45 MiB/s** |
| Loki 3.7.2 | 3,782,890,631 | 22.71x | 83.87 MiB/s |
| ClickHouse + text index | 6,093,155,990 | 14.10x | 353.33 MiB/s |

ShardLog stored 4.85x fewer bytes than Loki and 7.81x fewer than ClickHouse.
It ingested 7.98x faster than Loki and 1.89x faster than ClickHouse. These
comparison rows share corpus, Adam host, and physical CPU set, but were
executed as retained sequential campaigns rather than one newly rerun
three-engine script.

Retained Adam evidence:

```text
/home/dtietjen/shard-log-embedded-durable-v1-s1
/home/dtietjen/shard-log-embedded-durable-v2-perf
/home/dtietjen/shard-log-embedded-durable-v3-pipeline4
/home/dtietjen/shard-log-embedded-durable-v4-workers32
/home/dtietjen/shard-log-embedded-durable-v5-pipeline8
/home/dtietjen/shard-log-embedded-durable-v6-pipeline16
/home/dtietjen/shard-log-embedded-durable-v7-topk
/home/dtietjen/shard-log-embedded-durable-v8-full80
/home/dtietjen/shard-log-embedded-durable-v9-full80-reader-cache
/home/dtietjen/shard-log-embedded-durable-v10-full80-final
```

## Borrowed native decode — single-core durable path

Run date: 2026-07-30  
Host: Adam, AMD Ryzen 9 3950X, Linux 6.8  
Server affinity: CPU `0` only; all server threads inherited that affinity  
Loader affinity: physical CPUs `1-15`, outside the measured server core  
Corpus: the complete immutable 80 GiB ClickHouse Docker JSON corpus, SHA-256
`4fd6379bd89fcb44688a3ebd611729416c82f110fbf49ffef905d9df0ebf0508`  
Protocol: native grouped TCP, one owner stripe, 16 persistent loader
connections, 1 MiB batches, pipeline depth 8, indexed acknowledgements

The production native path no longer materializes a `Vec<OtlpLogEvent>` before
structural encoding. A bounded parser retains bodies and values as slices of
the native frame, stores compact record/metadata descriptors, allocates
normalized label keys once per stream and metadata keys once per batch, and
feeds those records directly into the existing structural encoder. The `SLW1`
storage format did not change.

A unit ablation requires the borrowed and old owned paths to produce
byte-identical durable packs and transient embedded indexes for multiple
streams, Unicode, zero/one/multiple metadata fields, and different cohorts.
All 99 library tests and every binary test passed on macOS and Linux; Clippy
also passed with warnings denied.

### Full 80 GiB result

| Metric | Owned events | Borrowed native | Change |
| --- | ---: | ---: | ---: |
| Source bytes | 85,899,345,920 | 85,899,345,920 | identical |
| Accepted records | 607,363,459 | 607,363,459 | identical |
| Native wire bytes | 66,592,152,397 | 66,592,152,397 | identical |
| Ingest elapsed | 275.756479 s | **229.309520 s** | 16.84% lower |
| Source throughput | 297.07 MiB/s | **357.25 MiB/s** | **20.26% higher** |
| Durable bytes | 780,372,593 | 780,372,593 | byte-identical total |
| Compression ratio | 110.0748x | 110.0748x | identical |
| Peak server RSS | 2,312,628 KiB | **2,205,552 KiB** | 4.63% lower |
| Server threads after ingest | 151 | 136 | 15 fewer |

Both full runs kept the entire server on one physical core and reported no
server, loader, or query errors. The loader remained outside CPU 0 and was not
the measured server-core budget. The 1 GiB gate improved from 284.12 to
333.58 MiB/s while producing the same 9,877,532 durable bytes.

Cold reopen of the borrowed result became ready within a measured 9.848-second
upper bound, held 14 descriptors, and returned the same exact 10 records.
Across 100 recovered queries, `source=clickhouse-docker AND runnableEntry`,
newest first, limit 10, measured 12.211 ms mean, 12.182 ms p50, and 12.925 ms
p99.

### Single-core profile

The 4 GiB `perf` run collected 6,328 cycle samples with zero lost and sustained
329.32 MiB/s under sampling, versus 284.41 MiB/s for the owned profile.

| Profile category | Owned path | Borrowed path |
| --- | ---: | ---: |
| Native decode, inclusive cycles | 30.26% | **20.58%** |
| Native decoder entry, self cycles | 7.61% | **3.42%** |
| Temporary `Vec` destruction, self cycles | 2.72% | below 0.20% |
| `malloc`, self cycles | 2.66% | **0.47%** |

Inclusive native decode fell 31.99%. The remaining borrowed decode total is
mostly required UTF-8 validation and cursor traversal: `from_utf8` was 7.86%
self, `Cursor::string` 6.23%, and `Cursor::string16` 0.77%. The generic
borrowed `record_field` accessor is now visible at 6.01% self because the
multi-pass structural encoder repeatedly resolves batch, stream, and metadata
indexes. BLAKE3 transport verification was 8.82% self. Structural encoding is
now the dominant inclusive stage at 57.42%; Zstandard was 4.32% inclusive.

An independent 8 GiB confirmation collected 12,764 cycle samples with zero
lost and sustained 325.34 MiB/s under DWARF call-graph sampling. A separate
counter-only run sustained 324.16 MiB/s. Both runs pinned the complete server
to CPU 0 and kept the loader on CPUs 1-15.

| Current borrowed-path category | Inclusive cycles | Self cycles |
| --- | ---: | ---: |
| Native pack preparation | 58.33% | 2.30% |
| Indexed structural encoding | 56.86% | 6.54% |
| Field lane encoding | 19.49% | 8.56% |
| Native batch decode and validation | 21.18% | 3.88% |
| `Cursor::string` | 13.94% | 6.54% |
| UTF-8 validation | 7.62% | 7.61% |
| Borrowed `record_field` lookup | 6.43% | 6.42% |
| BLAKE3 transport verification | 8.62% | 8.61% |
| Pco timestamp compression | 8.44% | distributed |
| Zstandard structural compression | 3.79% | distributed |

Inclusive rows overlap and must not be added. The counter run measured
86,168,709,058 cycles and 203,464,785,768 instructions for 8,589,934,670 source
bytes: 10.031 cycles/source-byte, 23.686 instructions/source-byte, 2.36 IPC,
0.89% branch misses, and 11.04% reported cache misses. At the measured
3.475 GHz clock, 1 GiB/s permits 3.236 cycles/source-byte, so the current path
still needs about a 3.10x reduction in CPU work to reach that target on this
core. The run had no CPU migrations, but its one-request-per-`spawn_blocking`
execution model reached 153 threads and 12,375 context switches/second.

The next write-path priorities are therefore:

1. Make normalized field IDs part of the validated native view and carry them
   through the attribute-table and field-lane passes. This removes repeated
   `record_field` resolution and repeated byte comparisons from the largest
   structural substage.
2. Fuse UTF-8 validation with the message/term scan, or adopt a measured SIMD
   validator for bodies while keeping malformed input fail-closed.
3. Evaluate hardware CRC32C for native transport corruption detection while
   retaining strong checksums for authoritative durable bytes.
4. Reuse a bounded Pco timestamp plan, or ablate a delta/bit-packed timestamp
   lane, without changing exact timestamp reconstruction or stored size.
5. Replace one `spawn_blocking` operation per pipelined request with one
   bounded worker per stripe to remove scheduler and thread-pool churn.
6. Leave Zstandard tuning until the larger field, validation, checksum, and
   timestamp costs have been reduced.

Retained Adam evidence:

```text
/home/dtietjen/deterministic-sim-runs/shard-log/borrowed-native-single-core-20260730-v1-sanity-1g
/home/dtietjen/deterministic-sim-runs/shard-log/borrowed-native-single-core-20260730-v2-full80
/home/dtietjen/deterministic-sim-runs/shard-log/borrowed-native-single-core-profile-20260730-v1-4g
/home/dtietjen/deterministic-sim-runs/shard-log/borrowed-native-single-core-profile-20260730-v2-8g
/home/dtietjen/deterministic-sim-runs/shard-log/borrowed-native-single-core-stat-20260730-v2-8g
```

The synchronized executable-source digest over `Cargo.lock`, `Cargo.toml`,
`rust-toolchain.toml`, and `src/` is
`c29cc9896ea48c70780ee4e132eed352d4b700ec9ffc0fdbde90a18010f79cc3`.
The full pre-run non-Git tree digest was
`a550a38c4b7239d0f47220edc538ff90dd2d160c59f6008dc5f5cc51d6aceec0`.
Because the pre-release repository still has no commit, these digests and the
binary hashes retained in each run are the reconstruction identities; the
run must not be described as reconstructable from a Git revision alone.

## Single-pass structural field encoding

The 2026-07-31 structural optimization resolves every input field once and
passes compact key/value IDs to the field encoder. The native view walks its
segmented tenant, label, and metadata storage directly. A fixed pointer-identity
cache handles immutable repeated keys and values, while exact byte lookup
remains the miss path. The large miss path is kept out of line and the repeated
value path is inlined.

The serialized structural format did not change. A focused test asserts that
the generic encoder reads each field exactly once. The existing native/owned
equivalence test still produces byte-identical ingest packs. A deterministic
one-worker Adam ablation over 1 GiB emitted 128 compressed blocks that were
byte-identical between the old and new encoders; every block checksum passed,
and sampled first/middle/last blocks reconstructed exactly.

### 8 GiB ablations

All production-path trials pinned the complete server to CPU 0, kept the
loader on CPUs 1-15, used one shard, 16 loader workers, pipeline depth 8, and
1 MiB native batches. Short 1 GiB trials were rejected as too noisy.

| Implementation | Runs | Mean MiB/s | Change from baseline |
| --- | ---: | ---: | ---: |
| Borrowed native baseline | 4 | 353.95 | baseline |
| Pre-resolved field plan | 5 | 367.11 | +3.72% |
| Pointer-identity key/value cache | 3 | 400.29 | +13.09% |
| Split and inline repeated-value fast path | 2 | **421.65** | **+19.13%** |

Adjacent two-run ablations measured the pointer cache 9.14% above the field
plan and the final inline split 7.37% above the pointer-cache version. Every
run accepted 60,736,279 records, skipped the same malformed record, and wrote
78,686,129 durable bytes.

### Full 80 GiB result

| Metric | Borrowed baseline | Structural optimized | Change |
| --- | ---: | ---: | ---: |
| Source bytes | 85,899,345,920 | 85,899,345,920 | identical |
| Accepted records | 607,363,459 | 607,363,459 | identical |
| Native wire bytes | 66,592,152,397 | 66,592,152,397 | identical |
| Ingest elapsed | 229.309520 s | **192.491413 s** | **16.06% lower** |
| Source throughput | 357.25 MiB/s | **425.58 MiB/s** | **19.13% higher** |
| Durable bytes | 780,372,593 | 780,372,593 | identical total |
| Compression ratio | 110.0748x | 110.0748x | identical |
| Peak server RSS | 2,205,552 KiB | 2,226,716 KiB | 0.96% higher |
| Cold-ready upper bound | 9.848 s | **9.730 s** | 1.20% lower |
| Recovered query mean | 12.211 ms | 12.214 ms | effectively unchanged |
| Recovered query p99 | 12.925 ms | **12.500 ms** | 3.29% lower |

The full storage trees have different pack boundaries because the native
multi-request run is host-scheduled and the faster consumer changes durable
pack filling. The authoritative total is identical, cold recovery validated
the store without any checksum error, and exact queries returned the same ten
records.
The deterministic single-worker structural ablation above is the byte-level
wire-format equivalence check.

### Final single-core profile

The final 8 GiB DWARF profile collected 10,538 cycle samples with zero lost and
sustained 401.30 MiB/s under sampling, 23.35% above the retained baseline
profile. The separate counter run sustained 385.06 MiB/s.

| Counter/category | Borrowed baseline | Structural optimized | Change |
| --- | ---: | ---: | ---: |
| Cycles/source-byte | 10.031 | **8.422** | **-16.04%** |
| Instructions/source-byte | 23.686 | **19.357** | **-18.28%** |
| IPC | 2.36 | 2.30 | -2.54% |
| Context switches/second | 12,375 | **12,026** | -2.82% |
| Indexed structural encoding, inclusive | 56.86% | **50.27%** | lower share of a faster run |
| Field lane, inclusive | 19.49% | **9.29%** | -52.33% relative |
| Generic `record_field`, self | 6.42% | absent | removed |
| Repeated-value slow path, inclusive | 5.08% old counter | **3.40%** | hot hits are inlined |

At the measured 3.473 GHz clock, the optimized counter run still uses about
2.60 times the CPU budget available at 1 GiB/s. The next targets are native
UTF-8/cursor validation, BLAKE3 transport verification, Pco timestamps, and
the one-`spawn_blocking`-task-per-request execution model. Zstandard remains a
smaller target.

All 100 library tests, every binary test, formatting, and Clippy with warnings
denied passed on macOS and Linux.

Retained Adam evidence:

```text
/home/dtietjen/deterministic-sim-runs/shard-log/structural-inline-ab-inline-20260731-v1-8g
/home/dtietjen/deterministic-sim-runs/shard-log/structural-inline-ab-inline-20260731-v2-8g
/home/dtietjen/deterministic-sim-runs/shard-log/structural-inline-profile-20260731-v1-8g
/home/dtietjen/deterministic-sim-runs/shard-log/structural-inline-stat-20260731-v1-8g
/home/dtietjen/deterministic-sim-runs/shard-log/structural-byte-equivalence-baseline-20260731-v3-1g
/home/dtietjen/deterministic-sim-runs/shard-log/structural-byte-equivalence-optimized-20260731-v3-1g
/home/dtietjen/deterministic-sim-runs/shard-log/structural-inline-single-core-20260731-v1-full80
```

The final executable-source digest is
`0773d0009692bc21814c377f4b11791414647cfc9d76814d970305b485036869`.
The retained deterministic source archive has SHA-256
`29fb85e8d72ea8274d0c602120bd0efab45d526b96df6be2948a3107707e07e3`.
Adam used framework revision
`bd1c4ba7eff99a5f324a5434b1a9d23444e32582`; its framework checkout was
dirty, and ShardLog still has an unborn revision. This is native
host-scheduled benchmark evidence, not exact task-schedule replay.
