"""Render results/*.json -> results/REPORT.md, answering the seven questions."""

from __future__ import annotations

import glob
import json
import os

RESULTS_DIR = os.environ.get("BENCH_RESULTS_DIR", "/results")
GRAPHS_DIR = os.path.join(RESULTS_DIR, "graphs")


def load() -> dict:
    out = {}
    for path in glob.glob(os.path.join(RESULTS_DIR, "*.json")):
        with open(path) as fh:
            d = json.load(fh)
        out[d.get("bench", os.path.basename(path))] = d
    return out


def table(rows: list[dict], cols: list[str]) -> str:
    if not rows:
        return "_(no data)_\n"
    head = "| " + " | ".join(cols) + " |\n"
    sep = "| " + " | ".join("---" for _ in cols) + " |\n"
    body = ""
    for r in rows:
        body += "| " + " | ".join(str(r.get(k, "")) for k in cols) + " |\n"
    return head + sep + body


def section_write(d: dict) -> str:
    rows = d.get("results", [])
    blob = [r for r in rows if r.get("workload") == "blob"]
    vec = [r for r in rows if r.get("workload") == "vector"]
    s = "### Q1 — How much can we write?\n\n**Blob (MOVEIT schema, `data` payload):**\n\n"
    s += table(blob, ["op", "payload_bytes", "rows_per_batch", "rows_per_s", "mb_per_s", "peak_rss_mb", "cpu_s"])
    s += "\n**Vector (768-dim):**\n\n"
    s += table(vec, ["op", "rows", "rows_per_s", "mb_per_s", "peak_rss_mb", "wall_s"])
    return s + "\n"


def section_read(d: dict) -> str:
    rows = d.get("results", [])
    blob = [r for r in rows if r.get("workload") == "blob"]
    vec = [r for r in rows if r.get("workload") == "vector"]
    probe = next((r for r in blob if r.get("op") == "lowcard_string_filter_probe"), None)
    blob_tbl = [r for r in blob if r.get("op") != "lowcard_string_filter_probe"]
    s = "### Q2 — How much can we read?\n\n**Blob scans:**\n\n"
    s += table(blob_tbl, ["op", "rows_per_s", "mb_per_s", "p50_ms", "p95_ms", "p99_ms", "pages_per_s"])
    s += "\n**Vector ANN search:**\n\n"
    s += table(vec, ["index", "nprobes", "qps", "p50_ms", "p95_ms", "p99_ms"])
    if probe:
        if probe.get("result") == "ok":
            s += f"\n> Low-cardinality string filter (`object_type = 'messages'`): OK ({probe.get('rows')} rows).\n"
        else:
            s += ("\n> ⚠️ **FINDING (known upstream bug — FIXED in newer Lance):** filtering on a "
                  "low-cardinality/constant string column (`object_type = 'messages'`) **panics** in "
                  "lance 0.33 (`lance-encoding buffer.rs` slice overflow). Lance adaptively "
                  "*dictionary-encodes* low-cardinality string columns and the v2/2.1 dictionary "
                  "**decode** path had buffer-slicing bugs — a documented cluster: "
                  "[lance#2828](https://github.com/lancedb/lance/issues/2828) (`Dictionary<_, "
                  "LargeString>`, same panic), [lance#2939](https://github.com/lance-format/lance/issues/2939), "
                  "[lance#4071](https://github.com/lancedb/lance/issues/4071). **Verified empirically:** "
                  "the identical probe PANICS on lancedb/lance 0.33.0 but returns all rows on "
                  "**lancedb 0.34.0 / lance 8.0.0** (latest) — fixed. **MOVEIT implication:** MOVEIT's "
                  "Rust crate pins `lance = \"8.0.0\"` (the fixed version), so its read path is most "
                  "likely NOT affected; this bench hit it only because it pinned Python `lancedb==0.33.0` "
                  "to mirror the documented stack. Any Python `lancedb ≤0.33` reader WOULD be affected. "
                  "Numeric `cursor` keyset and unique-`id` filters are unaffected in all versions. "
                  f"Error on 0.33: `{probe.get('error')}`\n")
    return s + "\n"


def section_concurrency(d: dict) -> str:
    rows = d.get("results", [])
    s = ("### Q3/Q4/Q5 — Multiple instances on one S3 table: correctness & latency\n\n"
         "Yes — multiple processes can open the same S3 dataset and write concurrently. "
         "`lost_keys > 0` would mean commits clobbered each other (data loss). Two commit paths:\n"
         "- **safe_ddb** — `s3+ddb://` external DynamoDB commit store (serialises commits everywhere).\n"
         "- **unsafe_s3** — plain `s3://` relying on the object store's own atomic conditional-PUT.\n\n"
         "Both use LanceDB's default MVCC/optimistic-concurrency commit; conflicts are retried "
         "internally (visible as tail-latency growth, not as errors). Note MOVEIT prod additionally "
         "runs a lower-level `UnsafeCommitHandler` (a Rust-side bypass) to work around *older* MinIO "
         "that lacked conditional-PUT — that is the only configuration that risks silent loss.\n\n")
    s += table(rows, ["mode", "shape", "writers", "expected_distinct_keys", "actual_distinct_keys",
                      "lost_keys", "data_loss", "total_conflicts", "wall_s",
                      "commit_p50_ms", "commit_p95_ms", "commit_p99_ms"])
    errs = [e for r in rows for e in (r.get("writer_errors") or [])]
    if errs:
        s += "\nWriter errors observed:\n\n" + "\n".join(f"- `{e[:200]}`" for e in errs[:10]) + "\n"
    return s + "\n"


def section_cap(d: dict) -> str:
    s = "### Q6 — 2GB column cap (and lifting it)\n\n"
    cc = d.get("construct_and_cap", {})
    w1 = d.get("write_one_2gb_batch", {})
    ag = d.get("append_growth_past_2gb", {})
    s += f"- int32 offset ceiling: **{d.get('int32_max_bytes'):,} bytes** (~2 GiB)\n"
    s += (f"- Construct >2GB `large_binary` array (64-bit offsets): "
          f"{'**succeeded — cap lifted**' if cc.get('large_binary_constructed') else '**failed** ' + str(cc.get('error'))}\n")
    s += (f"- Same array cast to int32 `binary`: "
          f"{'**FAILED — 2GB cap confirmed** — ' + str(cc.get('cap_error')) if cc.get('cap_confirmed') else 'cast ok (unexpected)'}\n")
    if w1.get("written"):
        s += f"- Write a single >2GB batch to Lance under the cap: **succeeded** ({w1.get('rows')} rows)\n"
    elif w1.get("oom"):
        s += "- Write a single >2GB batch to Lance under the cap: **OOM-killed** — must chunk writes\n"
    else:
        s += f"- Write a single >2GB batch to Lance under the cap: **failed** — {w1.get('error')}\n"
    s += (f"- Grow a column PAST 2GB via many <2GB `binary` batches: "
          f"logical **{ag.get('logical_column_bytes', 0):,} B** across {ag.get('batches')} batches, "
          f"exceeded_2gb={ag.get('exceeded_2gb')}, on-disk {ag.get('on_disk_bytes', 0):,} B "
          f"({ag.get('fragments')} fragments)"
          f"{' — error: ' + str(ag.get('error')) if ag.get('error') else ''}\n")
    s += ("\n**Verdict:** the ~2GB cap is real and is *per write array/batch per column* (int32 "
          "offsets), not a per-dataset limit. Lift a single large value/batch with Arrow "
          "`large_string`/`large_binary` (64-bit offsets); for MOVEIT's `data` column the practical "
          "path is keeping each write batch under 2GB — the column total grows unbounded across "
          "fragments regardless.\n\n")
    return s


def section_optimize(d: dict) -> str:
    rows = d.get("results", [])
    s = "### Q7 — optimize() / compaction overhead\n\n"
    s += table(rows, ["input_fragments", "rows", "fragments_before", "fragments_after",
                      "compact_wall_s", "compact_cpu_s", "compact_peak_rss_mb",
                      "rewrite_amplification", "bytes_reclaimed_by_cleanup", "cpu_s_per_1k_frags"])
    s += ("\nCPU/RSS scale with rows+fragments rewritten; S3 cost is the rewrite amplification "
          "(old+new files coexist until `cleanup_old_versions`). Public data point for scale: a "
          "60M-row table with a 768-dim vector exceeded **250GB RAM** during optimize — plan "
          "compaction cadence and memory accordingly ([lancedb#3201]"
          "(https://github.com/lancedb/lancedb/issues/3201)).\n\n")
    return s


def section_parallel_ingest(d: dict) -> str:
    cfg = d.get("config", {})
    agg = d.get("aggregate", {})
    sweep = d.get("sweep_writers", [])
    writers = d.get("writers", [])
    s = "### Large-scale (>1 TB) — parallel ingest\n\n"
    s += (f"**{cfg.get('n_writers')} writers × ~{cfg.get('per_table_target_gb')} GB** connector tables, "
          f"gaussian `data` (mean {(cfg.get('mean_bytes') or 0)/1e6:.1f} MB / std "
          f"{(cfg.get('std_bytes') or 0)/1e6:.1f} MB), {cfg.get('rows_per_array')} rows/array "
          f"(~1 GB, ~72σ under the 2 GB int32 offset cap), schema "
          f"`{'large_string' if cfg.get('use_large') else 'string'}`, seed {cfg.get('seed')}.\n\n")
    s += "**Writer-scaling sweep — aggregate throughput vs concurrent writers:**\n\n"
    s += table(sweep, ["n_writers", "per_table_target_gb", "agg_mb_per_s", "agg_rows_per_s",
                       "scaling_efficiency", "wall_s"])
    s += (f"\n**Primary build:** {agg.get('n_writers')} writers → aggregate **{agg.get('agg_mb_per_s')} MB/s** "
          f"/ {agg.get('agg_rows_per_s')} rows/s, {round((agg.get('total_data_bytes') or 0)/1e9,1)} GB in "
          f"{round((agg.get('wall_s') or 0)/60,1)} min. Per-writer MB/s: {agg.get('per_writer_mb_per_s')}.\n\n")
    if writers:
        te = writers[0]
        s += f"**Ingest vs size — `{te.get('table')}` checkpoints** (does write slow as the table grows?):\n\n"
        s += table(te.get("checkpoints", []), ["target_gb", "rows", "fragments", "version",
                                               "interval_mb_per_s", "interval_rows_per_s", "cum_wall_s", "rss_mb"])
    s += ("\nThe **version** column is the dataset version at each size — the query bench opens exactly "
          "these to read a table *as it was* at 100/250/500/1000 GB (versions are intact; cleanup is off).\n\n")
    return s


def section_query_degradation(d: dict) -> str:
    cfg = d.get("config", {})
    cells = d.get("cells", [])
    probe = d.get("compaction_probe", {})

    def sel(**kw):
        return [c for c in cells if all(c.get(k) == v for k, v in kw.items())]

    s = "### Large-scale (>1 TB) — query degradation\n\n"
    s += (f"Query unit: windowed `cursor` keyset scan, W={cfg.get('window')} rows, "
          f"{cfg.get('samples')} samples/cell ({cfg.get('warmup')} warm-up discarded), random offset "
          f"per query. Default probe size {cfg.get('default_gb')} GB.\n\n")
    s += "**Latency vs table size** (windowed_full — payload IO included):\n\n"
    s += table(sorted(sel(axis="size", variant="windowed_full"), key=lambda x: x["table_gb"]),
               ["table_gb", "fragments", "p50_ms", "p95_ms", "p99_ms", "qps", "mb_per_s"])
    s += "\n**Latency vs table size** (metadata_only — isolates fragment/plan cost):\n\n"
    s += table(sorted(sel(axis="size", variant="metadata_only"), key=lambda x: x["table_gb"]),
               ["table_gb", "fragments", "p50_ms", "p95_ms", "p99_ms", "qps"])
    s += "\n**Same table vs different tables:**\n\n"
    s += table(sel(axis="layout"), ["layout", "table_gb", "n_tables", "p50_ms", "p95_ms", "p99_ms", "qps"])
    s += "\n**Busy neighbors** — conn_0 latency while K workers hammer the same/other tables:\n\n"
    s += table(sorted(sel(axis="neighbor"), key=lambda x: (str(x.get("load")), x.get("k", 0))),
               ["load", "k", "p50_ms", "p95_ms", "p99_ms", "neighbor_qps", "peak_cpu_cores"])
    s += "\n**Table-count sweep** — single-table query latency vs catalog-listing latency:\n\n"
    s += table(sorted(sel(axis="table_count"), key=lambda x: x["n_tables"]),
               ["n_tables", "p50_ms", "p95_ms", "p99_ms", "list_p50_ms", "list_p95_ms"])
    s += "\n**One compaction — before vs after (identical table size):**\n\n"
    s += table([c for c in sel(axis="compaction") if c.get("variant") == "windowed_full"],
               ["state", "table_gb", "fragments", "p50_ms", "p95_ms", "p99_ms", "qps"])
    if probe.get("oom"):
        s += f"\n> ⚠️ **FINDING:** the single compaction **OOM-killed** — `{probe.get('error')}`.\n"
    elif probe.get("error"):
        s += f"\n> Compaction error: `{probe.get('error')}`\n"
    else:
        s += (f"\n> One compaction ({probe.get('fragments_before')} → {probe.get('fragments_after')} fragments, "
              f"target_rows_per_fragment={probe.get('target_rows_per_fragment')}) took **{probe.get('wall_s')} s**, "
              f"{probe.get('cpu_s')} CPU-s, peak RSS **{probe.get('peak_rss_mb')} MB**. No `cleanup_old_versions` "
              f"was run — superseded fragments remain (kept on purpose to remove that variable).\n")
    return s + "\n"


LEVERS = """### Scaling levers → how each moves the ceiling

| Lever | Effect on ceiling |
| --- | --- |
| Batch size | Larger batches amortise commit/manifest overhead → higher rows/s & MB/s, until memory-bound under the cap. |
| append vs merge_insert | `append` is cheap & commutative (auto-retries, rarely conflicts); `merge_insert` reads+rewrites matched fragments and conflicts under concurrency. |
| Commit store (s3 vs s3+ddb) | Plain `s3://` needs an object store with atomic put for safe concurrency; `s3+ddb://` serialises commits safely everywhere (incl. MinIO) at the cost of commit latency. |
| Vector index (IVF_PQ) | Turns brute-force O(N) scan into sub-linear ANN → large read QPS gain; `nprobes` trades recall for latency; index build is memory-heavy. |
| Fragment count / optimize cadence | Many small fragments slow reads & metadata; compaction restores it but is the most expensive write op — keep ≤~100 fragments/1B rows. |
| use_large_var_types (large_string/binary) | Lifts the per-batch 2GB cap on var-width columns (64-bit offsets) at a small size cost. |
| Horizontal instances / sharding | Object storage QPS is concurrency-bound; multiple reader instances scale reads linearly; writers must coordinate (single-writer-per-table or a commit store). |
"""

def section_graphs() -> str:
    """Embed any rendered PNGs (paths relative to results/ so links work from
    the repo)."""
    if not os.path.isdir(GRAPHS_DIR):
        return ""
    pngs = sorted(os.path.basename(p) for p in glob.glob(os.path.join(GRAPHS_DIR, "*.png")))
    if not pngs:
        return ""
    groups = [
        ("Time-series — CPU/mem vs query count (per pattern)", [p for p in pngs if p.startswith("ts_")]),
        ("Row count vs storage — by optimize() cadence", [p for p in pngs if p.startswith("optimize_")]),
        ("Vertical scaling — instances added while queries run", [p for p in pngs if p.startswith("scaling_")]),
        ("Large-scale (>1 TB) — parallel ingest", [p for p in pngs if p.startswith("ingest_")]),
        ("Large-scale (>1 TB) — query degradation", [p for p in pngs if p.startswith("query_")]),
    ]
    s = ("### Graphs\n\n_Rendered from real runs; see `results/graphs/`._\n\n"
         "**What the graphs show (measured, not modelled):**\n"
         "- **Vertical-scaling ceiling** (`scaling_read`): under the 2-core cap, read throughput climbs "
         "1→2 instances (~1→2 cores) then **plateaus and degrades** as instances 3–6 are added live — "
         "CPU is pinned at the cap and extra instances only add context-switch contention. Writes "
         "(`scaling_write`) never saturate CPU (~0.3 cores): they're commit/IO-bound, so they scale on a "
         "different axis (object-store round-trips), not cores.\n"
         "- **optimize() cadence vs storage** (`optimize_storage`): never-compacting grew to **~1071 MB / "
         "200 fragments** for 500k rows, ~**2× the ~535 MB / 21 fragments** of compact-every-20 — many "
         "small append-fragments store the same rows far less efficiently. ▲ marks the transient ~2× "
         "spike during compaction (old+new coexist) reclaimed by `cleanup_old_versions`.\n"
         "- **CPU/mem vs query count** (`ts_*`): scans hold <1 core with an RSS sawtooth per "
         "materialisation; merge_insert is the CPU-heaviest write pattern.\n"
         "- **Read-ops dips at each instance-add were a plot artifact, now fixed.** Throughput is "
         "`Δagg_ops/Δt`; spawning an instance briefly stretches the one sampler interval that straddles "
         "it, so a constant-rate counter dips then rebounds (the identical dip appeared in the write "
         "profile — proof it's harness-level, not LanceDB). `_throughput` now averages over a ~1 s "
         "trailing window, removing the aliasing while keeping the true ramp/plateau.\n"
         "- **Large-scale (`ingest_*`, `query_*`):** parallel ingest to ~1 TB/table on real S3, query "
         "latency vs table size, fragment count (one compaction), table count, and busy neighbors — see "
         "the two large-scale sections above for the numbers.\n\n")
    for title, items in groups:
        if not items:
            continue
        s += f"**{title}**\n\n"
        for p in items:
            s += f"![{p}](graphs/{p})\n\n"
    return s


CAVEAT = """### Caveats

- **MinIO ≠ AWS S3 for concurrency.** On this MinIO release (2025-09) both commit paths were
  loss-free, i.e. its atomic conditional-PUT works — the `safe_ddb` (DynamoDB) numbers are the
  ones that transfer to any object store. AWS S3 has native atomic conditional-PUT too; the only
  path that risks silent loss is MOVEIT's Rust-side `UnsafeCommitHandler` on *older* MinIO that
  lacked it (not reproduced here — pylance doesn't expose that bypass).
- **Blob payloads are highly compressible** (repeated filler), so blob MB/s is a logical-throughput
  upper bound; on-disk bytes are far smaller. rows/s and latency are unaffected.
- **On-disk bytes include superseded versions** until `cleanup_old_versions` runs (visible as
  rewrite_amplification in Q7 and the >logical on-disk size in Q6).
- All small-scale numbers are under the capped container (see cap label); they are a *floor* on real
  hardware, not a hardware benchmark. Vector index build (IVF_PQ) took ~147s on 200k×768 under 2 vCPU —
  index construction is the CPU-heaviest single op measured.
- **The large-scale (>1 TB) suite runs UNCAPPED on the full EC2 box against real AWS S3.** Those
  latencies/throughputs reflect that box, **not** the 2 vCPU / 4 GiB MOVEIT pod — they are a *ceiling*,
  the opposite end from the capped floor above. Real S3 (native atomic conditional-PUT, per-connector
  single writer) also means no DynamoDB commit store is used there.
- **Version count grows unbounded in the large-scale suite** because `cleanup_old_versions` is
  intentionally never called (one fewer variable). The per-checkpoint `version` values are recorded so
  any manifest-load slowdown is attributable, not hidden. Delete the tables from S3 after capturing
  `results/` to stop storage billing.
"""


def main() -> None:
    data = load()
    caps = sorted({d.get("cap_label", "?") for d in data.values()})
    md = ["# LanceDB Scaling Characterization — Report\n",
          f"_Resource cap(s): **{', '.join(caps)}** (disk uncapped). LanceDB pinned to prod "
          "version (lancedb 0.33.0). Backend: MinIO + DynamoDB-local for the capped local suite; "
          "real AWS S3 (uncapped EC2) for the large-scale (>1 TB) suite._\n"]

    if "write" in data:
        md.append(section_write(data["write"]))
    if "read" in data:
        md.append(section_read(data["read"]))
    if "concurrency" in data:
        md.append(section_concurrency(data["concurrency"]))
    if "cap2gb" in data:
        md.append(section_cap(data["cap2gb"]))
    if "optimize" in data:
        md.append(section_optimize(data["optimize"]))
    if "parallel_ingest" in data:
        md.append(section_parallel_ingest(data["parallel_ingest"]))
    if "query_degradation" in data:
        md.append(section_query_degradation(data["query_degradation"]))
    md.append(section_graphs())
    md.append(LEVERS)
    md.append(CAVEAT)

    out = os.path.join(RESULTS_DIR, "REPORT.md")
    with open(out, "w") as fh:
        fh.write("\n".join(md))
    print(f"[report] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
