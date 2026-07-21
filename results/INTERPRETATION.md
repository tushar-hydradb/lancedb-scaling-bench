# LanceDB >1 TB run — interpretation

**Run:** 2026-07-21, `t4g.2xlarge` (8 vCPU Graviton, 32.4 GB RAM), real AWS S3
(`lancedb-temp-tprf500-bucket`, us-east-1), `lancedb==0.33.0`, uncapped, version
cleanup off. 4 connector tables built to **1 TB each (4.00 TB total)**, 1000
fragments/table (~1 GB each, 200 rows × ~5 MB gaussian blobs). `trpf=500` for the
single compaction. Numbers are from the real run; raw data in `query_degradation.json`,
`parallel_ingest.json`, and the `*.jsonl` logs.

---

## TL;DR — both of your hunches were right

1. **The RAM and time models for compaction were off — in opposite directions.**
   We reasoned about compaction memory from the *output* fragment size (`trpf=500` ≈
   2.5 GB/fragment → "should be safe"). Reality: **peak RSS 23.5 GB on a 32.4 GB box
   (73%)**, sustained mean ~17 GB for the whole run. It did **not** OOM, but it was a
   near-miss, not the comfortable headroom the model implied. Time went the other way:
   we said "~1–2 h to rewrite a terabyte"; it took **34.7 min**. So the mental model was
   wrong on *both* axes — too optimistic on memory, too pessimistic on time.

2. **33% fewer fragments buys only ~15% faster queries — and that's arithmetic, not
   noise.** Query latency splits into a *fixed payload-IO floor* + a *metadata term that
   is linear in fragment count*. Compaction only shrinks the metadata term, which is
   ~45% of the total. Cut fragments 33% → cut 45%×33% ≈ **15% of total latency**. The
   model predicts 15.0%; we observed **15.4%**.

---

## 1. Compaction memory & time — why the model was off

| metric | our prior | measured | verdict |
|---|---|---|---|
| peak RSS | "single-digit GB, safe" | **23.5 GB (73% of 32.4)** | **way under-modeled** |
| mean RSS | — | ~17 GB (oscillates 12–23 GB the whole run) | holds a large working set start→finish |
| wall time | ~1–2 h | **34.7 min** (2081 s) | over-modeled ~2–3× |
| CPU | (assumed CPU-bound) | 3.65 / 8 cores avg | **IO-bound, not CPU-bound** |
| S3 IO rate | — | ~961 MB/s (≈2 TB read+write / 2081 s) | in-region Graviton↔S3 is fast |
| fragments | 1000 → ~400 (expected 2.5×) | **1000 → 666** (1.5×) | weaker consolidation than trpf implies |

**Why memory was under-modeled.** Compaction RSS is *not* one output fragment. It's
`(concurrent merge groups) × (input-fragment size + write buffering)`. It streams many
~1 GB *input* fragments in parallel and buffers the rewrite, so it sits at 15–23 GB the
entire time regardless of the 2.5 GB output target. `trpf` controls how many inputs get
coalesced into each output (and thus how *few* fragments you end up with) — it is **not**
a memory knob. This is also why `trpf=2000` OOM'd the earlier box: bigger output targets
mean bigger in-flight buffers on top of the already-large input working set.

**Why time was over-modeled.** The rewrite is S3-bandwidth-bound, not CPU-bound (only
3.65/8 cores used). In-region Graviton↔S3 sustained ~961 MB/s, so a ~2 TB read+write
round-trip finished in 35 min. My "1–2 h" over-weighted serial CPU rewrite cost.

**Cost/benefit at trpf=500:** 35 min wall, 23.5 GB peak RAM, 7600 CPU-s, ~2 TB of S3
IO — to make full-payload queries **15%** faster. That's a poor trade. The lever
(fragment count) is correct; the *dose* was too small to matter, and a bigger dose costs
proportionally more RAM and OOM risk.

## 2. Why 33% fewer fragments → only 15% faster queries

The whole story is one decomposition, and it holds across every size checkpoint
(`metadata_only` = plan + open fragments + read the `cursor` column to evaluate the
window filter; `windowed_full` = that **plus** materialize the ~100 MB payload):

```
query latency  =  payload-IO floor (~820 ms, constant)  +  0.666 ms × fragment_count
```

`metadata_only` latency vs fragment count is dead-linear: **0.666 ms/fragment + 3.4 ms,
R² = 0.994** (101, 251, 501, 1000 fragments → 80, 180, 307, 681 ms). The payload floor is
~820 ms and barely moves with table size, because every query returns the same 20-row /
~100 MB window no matter how big the table is.

At the 1 TB / 1000-fragment operating point:

| component | ms | share |
|---|---|---|
| payload IO (fixed) | 836 | **55%** |
| fragment metadata (0.666 × 1000) | 681 | **45%** |
| **total (p50)** | **1518** | 100% |

Compaction only attacks the 45% metadata slice. So:

| | fragments | metadata p50 | full-query p50 |
|---|---|---|---|
| before | 1000 | 628 ms | 1444 ms |
| after | 666 | 452 ms (**−28%**) | 1222 ms (**−15%**) |

Metadata dropped 28% (tracking the 33% fragment cut); the full query dropped only 15%
because the 55% payload floor didn't budge. **`0.45 × 0.33 = 15.0%` predicted, 15.4%
observed.** Your "barely 15%" intuition is exactly this arithmetic.

**Implication for MOVEIT.** To get a *real* query win you must drive fragments down by an
order of magnitude (e.g. 1000→100 would take metadata 681→~70 ms and the full query
~1518→~910 ms, ≈ −40%). But 100 fragments over 1 TB = ~10 GB output fragments = squarely
the `trpf=2000` OOM regime. So the payload-IO floor caps compaction's usefulness, and the
fragment count needed to beat it is exactly the count that's memory-dangerous. The
sustainable play is **incremental compaction that keeps fragment count bounded as you
ingest** (many small merges, low peak RSS), not one big terminal compaction — and even
then the ceiling on query improvement is the ~55% payload floor.

## 3. The other axes (all held at 500 GB, un-compacted, idle unless noted)

- **Table size → query latency.** Linear in fragments, as above. 100→1000 GB took p50
  from 817→1518 ms (windowed_full) — a 1.9× slowdown for 10× the data, entirely via the
  fragment-count term. Bytes-on-disk is irrelevant; **fragment count is the scaling
  variable.**
- **Number of tables (1/2/4/8) → no effect.** p50 flat at 83–85 ms (small padding
  tables). Catalog / `list_tables` overhead is negligible; a busy bucket by *count*
  doesn't tank queries. Answers your "do queries tank as tables grow" → **no.**
- **Same table vs 4 different tables → negligible.** p50 1159 vs 1226 ms (+6%). Querying
  across separate connector tables is essentially free vs hammering one.
- **Busy neighbors → mild, IO/CPU-contention only.** K=4 concurrent scanners on the
  *same* table: p50 1092→1196 ms (+10%); on *other* tables: 1093→1244 ms (+14%). CPU
  climbed to ~6.8/8 cores at K=4 (proof the neighbors were actually loaded). Contention
  shows up as modest tail growth, not a cliff — consistent with lock-free MVCC reads
  competing only for CPU/S3 bandwidth, not for locks.

## 4. Ingest (the part that went well)

- **4.00 TB in 3.98 h** across 4 parallel writers = **279 MB/s aggregate, ~70 MB/s per
  writer**, rock-steady (per-writer 69.9–70.3). Peak RSS per writer ~1.3 GB — cheap.
- **Writer-scaling sweep is sub-linear (S3/network-bound), not LanceDB-bound:** burst
  throughput per writer 279 → 240 → 134 MB/s at 1/2/4 writers; aggregate 279 → 479 → 535
  MB/s. Adding writers keeps raising aggregate but with diminishing per-writer returns —
  the box's S3 egress saturates around ~535 MB/s in burst, settling to ~279 MB/s
  sustained over 4 hours.
- **1 commit → 1 fragment held exactly** (1000 commits → 1000 fragments/table), so the
  fragment-count model above rests on a verified assumption.

---

## 5. Follow-up: does compacting *often* bound OOM risk + wall time? (500 GB, within-run)

Seeded one table to **500 GB** as 501 *small* fragments of 200 rows each — **under**
`trpf=500`, i.e. an **uncompacted drain backlog** (NOT pre-optimized). Then **50 turns of
read → append ~0.5 GB → `compact(trpf=500)`**, no version cleanup, each op isolated in its
own process for honest per-op peak RSS. The contrast that matters is **within-run**: turn 1
(pay off the whole backlog) vs turns 2–50 (steady state) — *same box, same ~500 GB, same
backend*, so nothing but the backlog differs. (`bench_compaction_cadence.py`, EC2 8-vCPU
Graviton / 30 GB RAM, real AWS S3, uncapped. Ran on S3 rather than local MinIO because the
box had only 99 GB free disk — a 500 GB local store wouldn't fit; the within-run contrast is
unaffected since network cost is constant across every turn.)

| compaction | peak RSS | wall | fragments |
|---|---|---|---|
| **turn 1 — full 500 GB backlog** | **20.3 GiB** (20741 MB) | **1054 s** (17.6 min) | 501 → 333 |
| turns 2–50 — steady state (mean) | **2.5 GiB** (2568 MB) | **6.5 s** | 334 → 333 (+1 delta/turn) |
| steady state (max) | 4.9 GiB (4925 MB) | 11.8 s | — |
| **drop, turn 1 → steady mean** | **8.1× less RSS** | **162× less wall** | — |

No OOM at any point (`any_oom: false`).

**Confirmed — exactly the shape you predicted, with a precise mechanism.** Turn 1 compacts
the whole uncompacted backlog (501 small fragments → 333), which *is* a terminal-scale
event: **20.3 GiB / 17.6 min**. From turn 2 on the body is already at target and is skipped,
so each compaction only re-reads the one fresh ~0.5 GB append fragment (334 → 333). That
collapses the cost to **2.5 GiB / 6.5 s mean and holds it flat for 49 turns** while the table
grows past 500 GB and the un-cleaned version count climbs to **798**. Per-turn work has **no
table-size term** — you pay the backlog once, then compacting often keeps every subsequent
compaction cheap.

**The refinement (don't overclaim):** the steady-state win is table-size *independence*, not
a tiny absolute. Compaction RSS is bounded by **`trpf` (output-fragment size), not the
table** — at `trpf=500` (~2.5 GB output fragments) the steady mean (2.5 GiB) fits a 4 GiB pod
but the **peak (4.9 GiB) just exceeds it** (see the sawtooth straddling the pod-cap line in
`cadence_rss_vs_turn.png`). So `trpf` is a three-way knob:

- `trpf` **up** → fewer fragments → **faster queries** (§2) but **higher compaction peak RSS**.
- `trpf` **down** → lower peak RSS (pod-safe) but **more fragments → slower queries**.
- **compacting often** is the free axis: it removes the *table-size* term from both RSS and
  wall, turning a 20 GiB / 18-min terminal event into a bounded ~2.5 GiB / ~6.5-s per-turn op.

Read p50 stayed in the **~700–960 ms** band across all 50 turns despite 129 extra un-cleaned
versions — version accumulation adds a little manifest-load latency but no cliff over this
range.

_Deliberately **not** compared to the 1 TB EC2 terminal number (§1: 23.6 GiB / 35 min): that
was a different scale (1 TB vs 500 GB) — the honest contrast is the within-run turn-1-vs-steady
one above, on one box at one scale. See `graphs/cadence_rss_vs_turn.png`,
`cadence_wall_vs_turn.png`, `cadence_fragments_vs_turn.png`._

## What I'd change next

- **Compact incrementally, never let a backlog build — now confirmed (§5).** Paying off a
  500 GB uncompacted backlog in one shot cost 17.6 min + 20.3 GB RAM (turn 1); compacting
  after every append holds the *same* layout goal at ~6.5 s + ~2.5 GB per turn, flat
  regardless of table size, because it only ever touches the fresh delta. Do this during
  ingest so the terminal event never has to happen. Pick `trpf` to trade compaction peak-RSS
  (bounded by output-fragment size) against query latency (fragment count) — and size the
  pod for the *peak*, not the mean (~4.9 GB at `trpf=500`).
- **Fragment count, not bytes, is the query-latency dial.** Track fragments/table as the
  health metric; target a fragment budget rather than a size budget.
- **The payload-IO floor (~820 ms for a 100 MB window) caps what compaction can do.** If
  read latency matters more, the bigger lever is the window size / projection, not
  fragment layout.
- **Distributed compaction stays attractive** precisely because a single box holds the
  whole 15–23 GB working set; spreading disjoint fragment groups across workers would cut
  both wall time and per-node RAM. (Deferred — the API in `pylance 0.33` supports it:
  `Compaction.plan/execute/commit` with serializable tasks.)
