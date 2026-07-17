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
