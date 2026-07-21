# LanceDB knowledge base (for MOVEIT)

Durable, mechanism-level notes on how LanceDB / the Lance format actually work, distilled from
this repo's benchmarks + a read of the **`lance = "8.0.0"`** crate source (the exact version MOVEIT
links). Two kinds of claim are cited differently:

- **Mechanism** claims cite lance 8.0.0 source as `crate/path.rs:line` (paths under the vendored
  crate, e.g. `lance-table-8.0.0/src/format/fragment.rs`).
- **Empirical** claims (numbers, latencies, RSS) link to the benchmark that produced them:
  `results/INTERPRETATION.md` (4×1 TB + cadence run) and `results/rust-knobs/REPORT.md` (compaction knobs).

MOVEIT context: LanceDB is an S3-backed **drain buffer** — one `.lance` table per `connector_id`,
8-column schema (`id, connector_id, object_type, op, content_hash, ingested_at, cursor, data`),
single-writer-per-table, upsert (`merge_insert`) on drain.

---

## 1. Data model: dataset → fragment → data file → column

```
Dataset  (an S3 prefix / directory)
├─ data/        <fragment>.lance data files   (the actual column data)
├─ _versions/   <n>.manifest                  (one immutable manifest per commit/version)
├─ _transactions/                             (transaction records for conflict resolution)
├─ _indices/    <uuid>/                        (scalar/vector index files, if any)
└─ _deletions/  <fragment>-<n>.{arrow,bin}     (deletion vectors / tombstones)
```

- **Fragment** (`lance-table/src/format/fragment.rs:479`) = a logical group of rows:
  `{ id, files: Vec<DataFile>, deletion_file: Option<…>, physical_rows, … }`.
- **DataFile** (`fragment.rs:28`) = **one `.lance` file** — *"one piece of file storing data."* It carries
  `fields: Arc<[i32]>`: the **set of column (field) ids stored in this file**.

**The key relationships (correcting common misconceptions):**

| Myth | Reality |
| --- | --- |
| "each `.lance` file is a column" | A `.lance` file holds a **set of columns** (usually *all* of them), stored columnar *within* the file. |
| "a column = its own `.lance` files" | A column's data = that column's **chunk inside each fragment's file**, one chunk per fragment, summed across fragments. Files are shared by all columns. |
| "#files = #fragments × #columns" | **#data files ≈ #fragments.** One fragment → one `.lance` file in the common case. |

- **1 fragment ⟷ 1 `.lance` file** on a normal write. A fragment gets **>1** file only when columns are
  **added later** (`add_columns`, a merge introducing a new column, blob columns): Lance writes a *new*
  `.lance` file holding just the new field-ids and appends it to the fragment's `files` — it never
  rewrites the existing file to add a column.
- **S3 object count for data ≈ fragment count** (+ one manifest per version, + deletion/index files).
  This is why fragment count — not table bytes — drives manifest size, list cost, and query planning.

**Worked sizing example** (a 1 TB, 10-column table at 10 GB per fragment):
> ~100 fragments = **~100 `.lance` data files total**, each 10 GB holding ~1 GB of *every* column.
> A single column's 100 GB = 1 GB × 100 files — but those are the *same* 100 files all columns live in,
> **not** 100 files per column (so **not** 1000 files).

## 2. Fragment / file sizing knobs

On write, the row stream is cut into files — a new file starts when **either** limit trips
(`lance/src/dataset/write.rs:646-647`), and **each file becomes one fragment**:

| Knob | Where | Default | Meaning |
| --- | --- | --- | --- |
| `max_rows_per_file` | `WriteParams` (`write.rs:269,397`) | 1,048,576 rows | Row cap per file/fragment. |
| `max_bytes_per_file` | `WriteParams` (`write.rs:285,401`) | **90 GB** | Byte cap per file/fragment (this is the "`max_bytes_per_fragment`" people look for). |
| `max_rows_per_group` | `WriteParams` | 1024 | In-file page/group size (v2 largely ignores it). |
| `target_rows_per_fragment` | `CompactionOptions` (`optimize.rs:82`) | 1,048,576 rows | Compaction **output** fragment size (the `trpf` we tune to 500). |
| `max_bytes_per_file` | `CompactionOptions` (`optimize.rs:19`, `Option`) | none | Compaction output **byte** cap, alongside `trpf`. |

So you can size fragments by **rows or bytes**, on both ingest and compaction.

## 3. Writes & `merge_insert` (upsert) — what happens to fragments

`merge_insert` (`lance/src/dataset/write/merge_insert.rs`) splits its output into two buckets
(`merge_insert.rs:1022-1023`):

- **Unmatched inserts** ("when not matched → insert") → **`new_fragments`**, written via
  `write_fragments_internal` (`merge_insert.rs:1265`). These **append new fragments** exactly like a
  plain append → **fragment count grows** with insert volume.
- **Matched updates** → **`updated_fragments`**: the touched fragment is **rewritten in place** (same
  fragment id, a fresh `.lance` file pushed onto its `files`, `merge_insert.rs:1106-1116`). Count does
  **not** grow, but this is **write amplification** — even a partial update reads-merges-rewrites the
  whole affected fragment; matched-old rows get **deletion vectors** that accumulate.

**MOVEIT implication:** a re-drain that *changes* existing records rewrites their home fragments (bounded
churn, flat count); *new* records append fragments (count grows). Fragment growth tracks the **insert**
share; compaction is still needed to (a) consolidate appended-insert fragments and (b) reclaim
update-rewrite + tombstone churn (`materialize_deletions`). Empirically `merge_insert` runs **2–3.5×
slower than a plain append** (read-modify-write) — see README headline / `bench_write.py`.

## 4. Compaction

`compact_files(dataset, CompactionOptions, …)` (`lance/src/dataset/optimize.rs:766`) rewrites small/updated
fragments into fewer, larger ones. It **plans** candidate bins of fragments smaller than
`target_rows_per_fragment` (`optimize.rs:657-733`), then **executes** them concurrently.

### Knobs — what moves RAM & wall (measured: `results/rust-knobs/REPORT.md`, 250 GB, real S3)

| Knob | Effect | Evidence |
| --- | --- | --- |
| **`num_threads`** | **The peak-RSS dial**, near-linear: `RSS ≈ 2.0 GiB + 2.45 GiB × threads` (1→4.3, 2→6.7, 4→12.5, 8→21.4 GiB). Also the wall dial, but speedup flattens past ~4 threads (IO/S3-bound). | `buffer_unordered(num_threads.unwrap_or(get_num_compute_intensive_cpus()))` (`optimize.rs:790`) — N concurrent tasks, each holding one ~`trpf`-sized decode buffer. |
| **`target_rows_per_fragment` (`trpf`)** | Sets **per-task size** → per-task RSS, and output fragment count → query latency. Consolidation knob, **not** a memory cap on its own. | §1 (1 TB): trpf=2000 (~10 GB output) OOM'd; trpf=500 (~2.5 GB) completed. |
| **`max_source_fragments`** | **NOT a memory knob** — a per-**call** work throttle. Foot-gun: it's a *cumulative* frag budget applied with `take_while`, which **drops the first task over budget AND everything after**; with a large `trpf` (all frags in one task) any value below the frag count **silently no-ops** the whole compaction (0 merged). | `optimize.rs:735-746`; measured no-op in `results/rust-knobs/`. |
| **`io_buffer_size`** | Immaterial at MOVEIT fragment sizes (<2.5% RSS, <2% wall for 32/128 MB). | `results/rust-knobs/REPORT.md`. |
| **`materialize_deletions` / `_threshold`** | Whether/when compaction also rewrites fragments to physically drop tombstoned rows (default threshold 0.10). | `optimize.rs` CompactionOptions. |

**Peak-RSS model:** `RSS ≈ base + num_threads × (per-task decode buffer)`, and per-task buffer ≈ the
`trpf`-sized working set — **independent of table size and of output fragment count**. So the two real
memory levers are `num_threads` (how many tasks at once) and `trpf` (how big each task is). The default
`num_threads` = ~all cores, which is why an unconfigured compaction spikes to ~20 GB.

**To run compaction in a bounded pod:** set `num_threads` explicitly — `2` → ~8 GiB pod, `1` → ~5 GiB —
keep `trpf` as the query-latency/per-task-size trade, use `max_source_fragments` only to *chunk* a big
backlog across several calls (never below a task's frag count), leave `io_buffer_size` default.

### Cadence — compact often, never let a backlog build (measured: `INTERPRETATION.md` §5)

Paying off a 500 GB uncompacted backlog in one shot cost **17.6 min + 20.3 GB RAM** (turn 1); compacting
after every append held the same layout goal at **~6.5 s + ~2.5 GB per turn, flat regardless of table
size** (8.1× RSS / 162× wall drop), because each turn only ever touches the fresh delta. **Compacting
during ingest removes the table-size term from both RAM and wall** — the terminal big-bang compaction
never has to happen. Distributed compaction was considered but **dropped** (vertical scaling is fine;
`num_threads` + cadence bound the single-box cost).

### On-disk churn

Compaction is a rewrite: expect **~2× transient on-disk amplification** until old versions are cleaned
(`cleanup_old_versions`, intentionally **not** used in these benches — one fewer variable). Steady-state,
compacting **~halves** storage vs never-compacting (small append fragments are ~2× less space-efficient;
`bench_optimize_intervals.py`).

## 5. Querying & latency

- **Fragment count is the only query-latency dial** — table *bytes* are irrelevant.
  `latency ≈ fixed payload-IO floor + 0.666 ms × fragment_count` (R²=0.994, `INTERPRETATION.md` §1-2).
- Compaction shrinks only the *metadata* term: 33% fewer fragments → only ~15% faster full-payload
  queries (metadata is ~45% of latency; the rest is the payload-IO floor). To beat the floor you need
  ~10× fewer fragments (→ big output fragments → the OOM regime) — so treat **fragment count as the
  health metric** and target a fragment budget, not a size budget.
- Reads are **lock-free MVCC snapshots** — concurrent readers scale until CPU or IO/bandwidth saturates
  (not a LanceDB wall). Keyset windows on `cursor` are single-digit-ms p50; projected full scans are
  memory-bandwidth-bound (README headline table).

## 6. Indexing

Scalar index types a user can build (`lance-index/src/scalar.rs:64-71`,
plugins registered in `registry.rs`):

| Index | Best for | Notes |
| --- | --- | --- |
| **BTree** | Range + equality on ordered/high-cardinality cols | Good for `cursor >= … AND cursor < …`. |
| **Bitmap** | **Low-cardinality** equality (`connector_id`, `object_type`) | Exact, tiny — the right pick for MOVEIT's constant-ish string columns. |
| **LabelList** | Multi-value / list membership | |
| **NGram** | Substring `LIKE` | |
| **ZoneMap** | Skip row-zones by min/max | Coarse pushdown for sorted-ish columns. |
| **BloomFilter** | **High-cardinality** equality / `IN` (e.g. `id = …`) | See below. |
| **Inverted** | Full-text search | Tokenized. |

**Bloom filters — exist & granularity.** lance 8.0.0 has a real, creatable `BloomFilter` scalar index
(`ScalarIndexType::BloomFilter` `scalar.rs:69`; `IndexType::BloomFilter = 9` `lance-index/src/lib.rs:131`;
`BloomFilterIndexPlugin` `registry.rs:82`; module `scalar/bloomfilter.rs`; SBBF algorithm, same family as
Parquet). **Granularity is per-*zone*, not per-`.lance`-file**: `BloomFilterStatistics` stores *"bound of
this zone within the fragment… (fragment_id, zone_start, zone_length)"* (`bloomfilter.rs:50-52`) — it's a
dataset-level index under `_indices/`, partitioned into zones within fragments (one filter per zone),
letting a scan skip zones that definitely lack a value (probabilistic: false positives rechecked, no
false negatives).

- **For MOVEIT, bloom is usually the wrong pick.** It's for high-cardinality equality (`id`). For the
  low-cardinality strings (`connector_id`, `object_type`) use **Bitmap**; for `cursor` ranges use
  **BTree/ZoneMap**.

**Who builds the bloom filter — user vs engine?** There are *three* separate bloom uses in lance 8.0.0;
only one is a query index, and it is **user-created**:

| Use | Automatic? | Detail |
| --- | --- | --- |
| **Scalar `BloomFilter` index** (query acceleration) | **No — user opt-in.** | Like every scalar/vector index, you must explicitly call `create_index(…, ScalarIndexType::BloomFilter)` (`lance/src/index.rs:933`, `DatasetIndexExt::create_index`). Lance **never** auto-builds it on write/append/compaction, and it **does not auto-maintain** it — fragments appended after creation are *unindexed* until you retrain/optimize the index (standard Lance index-staleness behavior). |
| **`merge_insert` conflict-detection bloom** | **Yes — internal.** | Every upsert on a PK automatically builds a bloom of the affected keys (`KeyExistenceFilter::from_bloom_filter`, `merge_insert.rs:6111`; *"partial-schema upsert on a PK must emit a bloom filter"* `:4836`) and stores it in the transaction, so concurrent merge_inserts touching the same keys detect the write-write conflict (→ `TooMuchWriteContention`). Not a query index; not user-controlled. |
| **MemTable/WAL point-lookup bloom** | **Yes — if MemWAL enabled.** | Per-memtable-generation `bloom_filter.bin` (`mem_wal/memtable/flush.rs`) for the WAL point-lookup path. Not the scalar index, not MOVEIT's read path. |

**The "free" out-of-the-box acceleration is NOT a bloom filter — it's per-page zone statistics.** The v2
file writer automatically records column stats (min/max, `NullCount`, …) per page (`lance-encoding/src/statistics.rs`),
giving the scan **filter pushdown to skip pages with no user index at all**. So: automatic pruning = zone
stats (always on); a bloom/bitmap/btree scalar index = an explicit thing you build and keep fresh.

## 7. Concurrency, consistency, storage

- **Writes are commit-serialized** (optimistic concurrency): each commit is one new immutable manifest;
  conflicts retry. Single-writer-per-table (MOVEIT's model) never conflicts. Concurrent multi-writer to
  one dataset was **loss-free** on MinIO 2025-09 via both `s3+ddb://` (DynamoDB commit store) and plain
  `s3://` (native atomic conditional-PUT); contention shows as tail-latency growth, not data loss
  (`bench_concurrency.py`).
- **`UnsafeCommitHandler`** in MOVEIT's Rust path was a workaround for *older* S3/MinIO lacking
  conditional-PUT; on modern S3 the native conditional PUT makes it revisitable.
- **1 commit → 1 fragment** held exactly in tests (1000 commits → 1000 fragments) — the realistic drain
  cadence, and why appends need periodic compaction.

## 8. Gotchas & version notes

- **2 GB var-width column cap** is real and *per-write-array per-column* (int32 offsets). Lift with
  `large_binary`/`large_string` (64-bit offsets); practically, keep each write batch < 2 GB — the column
  total grows unbounded across fragments regardless (`bench_2gb_column.py`).
- **lance 0.33 panics filtering a low-cardinality/constant string column** (dictionary-decode buffer bug,
  `lance-encoding buffer.rs:372`) — **fixed in lance 8.0.0**. MOVEIT's Rust crate pins 8.0.0 so its read
  path is safe; any Python `lancedb ≤ 0.33` reader is affected. A **Bitmap index** also sidesteps it.
- **Compressible filler caveat** (applies to these benches): the `'x'` payload compresses hugely, so
  on-disk/S3 bytes are tiny and reported MB/s is *logical* decode throughput — 250 GB logical was only
  115 MB on disk. **Peak RSS (decompressed working set) and wall are the honest metrics**; on
  incompressible prod JSON, IO-bound wall would be higher.
- **Version accumulation**: cleanup is intentionally never called in these benches; ~1000 versions/table
  added no query cliff (manifest-load stayed flat), but real deployments should schedule
  `cleanup_old_versions` to cap manifest/storage growth.
- Bench tooling: `.venv` must be Python 3.11–3.13 (3.14 segfaults matplotlib).

## 9. Where the evidence lives

| Topic | Doc |
| --- | --- |
| 4×1 TB ingest + query-degradation + compaction memory/time model | `results/INTERPRETATION.md` §1–4 |
| Compaction cadence (backlog-vs-steady, within-run) | `results/INTERPRETATION.md` §5 |
| Rust `CompactionOptions` knob sweep (num_threads / max_source_fragments / io_buffer) | `results/INTERPRETATION.md` §6, `results/rust-knobs/REPORT.md` |
| Small-scale envelope (write/read/concurrency/2GB/optimize) | `results/REPORT.md`, README headline findings |
| Compaction-knob bench harness (Rust, lance 8.0.0) | `rust-compaction-knobs/` |

_Source line numbers reference the vendored `lance-*-8.0.0` crates (`~/.cargo/registry/.../lance-8.0.0`
etc.). Confirm against the same version if upgrading — the layout/knob semantics can shift between majors._
