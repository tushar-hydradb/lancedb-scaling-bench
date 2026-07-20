# lancedb-scaling-bench

Characterizes the scaling envelope of **LanceDB embedded (OSS)** the way MOVEIT runs
it in production — S3-backed (MinIO), pinned to `lancedb==0.33.0`, inside a container
**capped to the MOVEIT prod pod limits (2 vCPU / 4 GiB, disk uncapped)**.

## Run

```sh
./run_all.sh              # primary pass @ 2 vCPU / 4 GiB (MOVEIT prod cap)
CAP=1cpu2g ./run_all.sh   # optional staging-cap pass @ 1 vCPU / 2 GiB
docker compose down -v    # tear down (removes the MinIO volume)
```

Output: `results/REPORT.md` (rendered answers to the seven questions) + `results/*.json` (raw).

## Large-scale (>1 TB) run — EC2 + real AWS S3

The capped local suite above answers the *small-scale* envelope. The question of how a single
connector table scales **to and past 1 TB**, how fast we can ingest into many such tables **in
parallel**, and how **queries tank** (as the table grows, as fragments grow vs one compaction, as the
table count grows, and under **busy neighbors**) needs real object storage — MinIO on a laptop can't
hold multiple terabytes. That suite runs **uncapped on an EC2 box against real AWS S3**:

```sh
# on the EC2 box (IAM instance role attached), after scp/clone of this repo.
# Bucket defaults to lancedb-temp-tprf500-bucket; region is taken from the creds'
# default (aws config / IMDS). Override with BENCH_BUCKET / AWS_REGION if needed.
BENCH_SMOKE=1 ./run_ec2.sh    # dry run: 2x1 GB, validates IAM + measures real EC2->S3 MB/s
./run_ec2.sh                  # full: 4 tables x ~1 TB (~4 TB in S3)
```

The run publishes the report to the bucket root (`BENCH_UPLOAD_REPORT=0` to skip). Grab it from anywhere:

```sh
aws s3 cp    s3://lancedb-temp-tprf500-bucket/REPORT.md .        # the report
aws s3 sync  s3://lancedb-temp-tprf500-bucket/graphs ./graphs    # its images
aws s3 sync  s3://lancedb-temp-tprf500-bucket/results ./results  # raw JSON
aws s3 rm --recursive "s3://lancedb-temp-tprf500-bucket/bigscale"  # teardown the ~4 TB of table data
```

`run_ec2.sh` uses a bare venv (not Docker — avoids the IMDSv2 hop-limit gotcha that blocks containers
from reading instance-role creds), sets `BENCH_S3_REAL=1` (region-only `storage_options`, creds from the
default AWS chain, plain `s3://` since real S3 has native atomic conditional-PUT), and runs
`bench_parallel_ingest.py` → `bench_query_degradation.py` → `plots.py` → `report.py`. Knobs:
`BENCH_N_WRITERS`, `BENCH_PER_TABLE_TARGET_GB`, `BENCH_CHECKPOINTS_GB`, `BENCH_QUERY_SAMPLES`. Compaction
`trpf` is **hardcoded to 500** (~2.5 GB/output fragment; 2000 OOM'd an 8-core box). These numbers reflect
the **uncapped box, not the 2 vCPU/4 GiB pod** — a ceiling, not the prod floor. Version cleanup is
intentionally never called (one fewer variable); on-disk size is not a reported metric.

**Resilience (built for an unattended overnight run):** every stage snapshots its consolidated `*.json`
after each unit of work (each ingest checkpoint, each query axis) — written atomically (temp+rename) — and
append-logs fine detail to crash-safe `*.jsonl` (`ingest_events`, `ingest_samples`, `sweep_results`,
`query_cells` with **raw per-query latencies**, `compaction_heartbeat`, `compaction_probe`, `run_meta` with
host/instance info). A background job syncs `results/` to S3 every 5 min. So an OOM / spot-reclaim / SSH
drop loses at most the last in-flight unit. Resume the query stage without re-ingesting via
`BENCH_SKIP_INGEST=1 ./run_ec2.sh` (add `BENCH_SKIP_COMPACTION=1` to skip the memory-risky compaction).

## What it measures

| Bench | Question answered |
| --- | --- |
| `bench_write.py` | How much can we write? (blob append vs merge_insert × payload; vector append + index build) |
| `bench_read.py` | How much can we read? (projected/filtered scan, keyset paging; vector ANN QPS & latency) |
| `bench_concurrency.py` | Multiple instances on one S3 table — correctness (lost keys) + latency, `s3+ddb` vs plain `s3` |
| `bench_2gb_column.py` | The 2GB var-width column cap; lifting it with `large_binary`; growth past 2GB via chunked appends |
| `bench_optimize.py` | `optimize()`/compaction cost vs fragment count (CPU-s, peak RSS, S3 rewrite amplification) |
| `bench_timeseries.py` | **Graph:** container CPU/mem vs cumulative query count, per read/write pattern |
| `bench_optimize_intervals.py` | **Graph:** row count vs on-disk storage, with `optimize()` at different cadences (storage sawtooth) |
| `bench_scaling.py` | **Graph:** throughput + CPU/mem while instances are added live (vertical-scaling ceiling) |
| `bench_parallel_ingest.py` | **(EC2/S3)** parallel ingest to 4×~1 TB connector tables; writer-scaling sweep; interval throughput at size checkpoints (records dataset versions) |
| `bench_query_degradation.py` | **(EC2/S3)** query latency vs table size, same/different tables, one compaction, table count, and busy neighbors |
| `plots.py` | Renders the above JSON into PNGs under `results/graphs/` |

Graphs use a container-level cgroup v2 sampler (`CgroupSampler` in `common.py`) reading `cpu.stat`/`memory.current`, so they capture **all** instances, not just one process.

## Layout

```
docker-compose.yml   minio + createbucket + dynamodb-local + capped bench runner
Dockerfile           python:3.13 + lancedb==0.33.0 + pylance==0.33.0 + pyarrow==24.0.0
bench/common.py      connect modes (safe s3+ddb / plain s3), schemas, metrics, cgroup sampler, S3 footprint
bench/bench_*.py      the eight benches (write/read/concurrency/2gb/optimize + timeseries/optimize_intervals/scaling)
bench/plots.py       time-series JSON -> results/graphs/*.png
bench/report.py      results/*.json -> results/REPORT.md (embeds the graphs)
run_all.sh           boot, run each bench in the capped container, render graphs + report
```

## Headline findings (from a 2 vCPU / 4 GiB run)

- **Writes:** append peaks ~350k rows/s (1KB) → 14k rows/s (100KB); `merge_insert` runs 2–3.5× slower
  than append (read-modify-write). Larger batches → higher MB/s until memory-bound.
- **Reads:** projected full scans are effectively memory-bandwidth bound; `cursor` keyset windows are
  single-digit-ms p50. Vector ANN with IVF_PQ is ~175 QPS vs ~2 QPS brute-force.
- **Concurrency:** multiple processes CAN share one S3 dataset. On MinIO 2025-09 **both** commit paths
  were loss-free; contention shows up as **tail-latency growth** (p95 ~490ms at 4 overlapping writers
  vs ~24ms baseline), not data loss. The `s3+ddb` (DynamoDB commit store) path is the portable one.
- **2GB cap:** real and *per-write-array per-column* (int32 offsets). `large_binary`/`large_string`
  (64-bit offsets) lifts it; the practical MOVEIT path is keeping each write batch < 2GB — the column
  total grows unbounded across fragments regardless.
- **optimize():** CPU/RSS scale with rows+fragments; ~2× on-disk rewrite amplification until
  `cleanup_old_versions`. Budget memory carefully at scale (public report: 60M×768-dim hit 250GB RAM).
- **Compaction ~halves steady-state storage:** never-compacting held 500k rows in **~1071 MB / 200
  fragments** vs **~535 MB / 21 fragments** compacting every 20 appends — small append-fragments are
  ~2× less space-efficient. See `graphs/optimize_storage.png`.
- **Adding instances scales reads — up to the binding resource (not a LanceDB wall).** Reads are
  lock-free MVCC snapshots (no coordination), so concurrent readers scale until CPU or IO/bandwidth
  saturates. The plateau **tracks the CPU cap**, proving it's resource saturation, not contention:

  | Cap | Read throughput knee | Peak ops/s | Binds on |
  | --- | --- | --- | --- |
  | 2 cores | ~2 instances | ~200 | CPU (pinned at cap) |
  | 4 cores | ~4 instances | ~231 | CPU (pinned at cap) |
  | 8 cores | ~3–4 instances | ~229 | IO / memory bandwidth (CPU only ~5/8 used) |

  To scale reads further: add cores (vertical) or add stateless nodes on the same object store
  (horizontal) — LanceDB docs report near-linear QPS scaling per node. Writes are the exception: they
  serialise commits (optimistic concurrency, single-writer-ideal), so they're commit/IO-bound and don't
  scale with instances. See `graphs/scaling_*.png`.
- ⚠️ **lance 0.33 bug (fixed in lance 8.0.0):** filtering on a **low-cardinality/constant string column**
  panics (`lance-encoding buffer.rs` slice overflow) — a known dictionary-encoding decode bug
  ([lance#2828](https://github.com/lancedb/lance/issues/2828), [#2939](https://github.com/lance-format/lance/issues/2939)).
  Verified: PANICS on lancedb/lance 0.33.0, returns all rows on lancedb 0.34.0 / lance 8.0.0.
  **MOVEIT's Rust crate pins `lance = "8.0.0"` (the fixed version), so its read path is most likely NOT
  affected;** the bench hit it only via the pinned Python `lancedb==0.33.0`. Any Python `lancedb ≤0.33`
  reader would be. `cursor` (numeric) and `id` (unique) filters are unaffected in all versions.

## The 2GB variable-width column cap — what it actually means

A common misreading is that "a column caps at 2GB" means a single cell (or the whole dataset) is
capped. **Neither is quite right.** The limit is on the **cumulative bytes of a column within one write
array (batch)**, and it comes from Arrow's offset encoding — Lance inherits it.

An Arrow `string`/`binary` column is two buffers:

```
values:  [ cell0 bytes ][ cell1 bytes ][ cell2 bytes ] ...   ← all cells concatenated
offsets: [ 0, len0, len0+len1, len0+len1+len2, ... ]         ← N+1 integers, INT32 for plain string/binary
```

The **last offset = the sum of every cell's bytes in that array**, and for plain `string`/`binary` it's
a signed **int32**, so:

```
int32 max = 2,147,483,647 bytes  ≈ 2 GiB
sum(len(cell) for every cell in ONE write array) < 2 GiB
```

So the 2GB ceiling is **per write-array (per `add()`/`write_dataset` call), per column** — not per cell,
not per dataset:

| Scenario | plain `binary`/`string` (int32) |
| --- | --- |
| One 3 GB value, by itself | ❌ overflows (a single item >2 GB also breaks it) |
| 22,000 rows × 100 KB in **one** write batch = 2.2 GB | ❌ overflows — even though every cell is tiny |
| Same 2.2 GB split across **many** batches (each <2 GB) | ✅ fine — each batch is its own array/offsets |
| Column growing to 100 GB across fragments over time | ✅ fine — no per-dataset limit |

`large_string` / `large_binary` switch the offsets to **int64**, lifting both: a single cell can exceed
2 GB *and* a batch's cumulative bytes become effectively unbounded (~8 EiB).

**Proven by `bench_2gb_column.py` (real run):**
- 2,200 rows × 1 MB = **2.2 GB in one array → FAILS** the int32 cast (`input array too large`). Each cell
  is only 1 MB, so it's the *cumulative* total that breaks it — confirming per-array, not per-cell.
- 24 batches × 100 MB = **2.4 GB column total, each batch <2 GB, plain `binary` → SUCCEEDS**. Same bytes,
  chunked, so no single array crosses int32.

**MOVEIT takeaway:** you do **not** need `large_binary` just because the `data` column grows past 2 GB
overall (it grows across fragments). You'd only need it if a **single write batch** packs >2 GB of `data`,
or a **single record's `data`** could exceed 2 GB (unlikely for SaaS JSON). Keeping write batches under
~2 GB — which per-stream syncing already does — is the whole mitigation.
