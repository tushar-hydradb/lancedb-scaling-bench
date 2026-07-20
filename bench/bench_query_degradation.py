"""Q2 — how queries tank as data grows, fragments grow, neighbors get busy,
and the table count grows.

Reads `results/parallel_ingest.json` for the connector tables built by
`bench_parallel_ingest.py` and their per-checkpoint dataset **versions**, then
runs a windowed `cursor`-keyset scan (the MOVEIT drain read pattern) as the
query unit — W rows (~100 MB with `data`, or metadata-only) at a random offset
to spread across fragments and defeat caching. p50/p95/p99 via np.percentile.

Axes (each a 1-D sweep holding the rest at defaults, plus the two requested
2-D slices):
  * size        — conn_0 opened at each checkpoint version (100/250/500/1000 GB)
  * layout      — all queries on one table vs round-robin across tables
  * neighbor    — idle vs K workers hammering the SAME table vs OTHER tables
  * table_count — single-table latency + catalog-listing latency vs #tables
  * compaction  — conn_0 at full size, before vs after exactly ONE compaction
                  (isolated process; OOM classified as a finding). cleanup is
                  never called (constraint).

Uncapped on EC2 / real S3. On-disk size is not a metric; we read S3 only for
fragment counts (carried from the ingest checkpoints / get_fragments()).
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import threading
import time

import lance
import lancedb
import numpy as np
import psutil

import common as c

PREFIX = "bigscale"
TABLECOUNT_PREFIX = "tablecount"


# --- config -----------------------------------------------------------------
def _load_qcfg(tables: list) -> dict:
    smoke = os.environ.get("BENCH_SMOKE") == "1"
    cks = tables[0]["checkpoints"]
    max_gb = max(x["target_gb"] for x in cks)
    default_gb = min(float(os.environ.get("BENCH_QUERY_DEFAULT_GB", 500)), max_gb)
    cfg = {
        "window": int(os.environ.get("BENCH_WINDOW_ROWS", 20)),
        "samples": int(os.environ.get("BENCH_QUERY_SAMPLES", 300)),
        "warmup": 5,
        "seed": int(os.environ.get("BENCH_SEED", 11)),
        "default_gb": default_gb,
        "neighbor_ks": [1, 2, 4],
        "table_count_ns": [1, 2, 4, 8],
        # HARDCODED to 500 for this run: at ~5 MB/row that's ~2.5 GB/output
        # fragment (2000 OOM'd the box at ~10 GB), still collapsing ~1000 input
        # fragments to ~400. Distributed compaction is the path to going higher.
        "compact_trpf": 500,
        "smoke": smoke,
    }
    if smoke:
        cfg.update(samples=30, warmup=2, neighbor_ks=[1, 2], table_count_ns=[1, 2], compact_trpf=50)
    return cfg


def _load_ingest() -> dict | None:
    p = os.path.join(c.RESULTS_DIR, "parallel_ingest.json")
    if not os.path.exists(p):
        return None
    with open(p) as fh:
        return json.load(fh)


def _get_ckpt(table_entry: dict, gb: float) -> dict | None:
    cks = table_entry.get("checkpoints") or []
    if not cks:
        return None
    exact = [x for x in cks if abs(x["target_gb"] - gb) < 1e-6]
    return exact[0] if exact else max(cks, key=lambda x: x["target_gb"])


# --- stats + query suites ---------------------------------------------------
def _pct(xs: list, p: float) -> float:
    return round(float(np.percentile(xs, p)) * 1000, 2) if xs else 0.0


def _cell_stats(lats: list, bytes_read: int) -> dict:
    tot = sum(lats)
    a = np.array(lats) * 1000 if lats else np.array([0.0])
    return {
        "samples": len(lats),
        "p50_ms": _pct(lats, 50), "p90_ms": _pct(lats, 90), "p95_ms": _pct(lats, 95),
        "p99_ms": _pct(lats, 99), "p25_ms": _pct(lats, 25), "p75_ms": _pct(lats, 75),
        "min_ms": round(float(a.min()), 2), "max_ms": round(float(a.max()), 2),
        "mean_ms": round(float(a.mean()), 2), "stddev_ms": round(float(a.std()), 2),
        "qps": round(len(lats) / tot, 2) if tot > 0 else 0,
        "mb_per_s": round(bytes_read / 1e6 / tot, 1) if tot > 0 else 0,
        "bytes_read": bytes_read,
    }


def _emit(out: list, base: dict, stats: dict, lats: list) -> dict:
    """Append a completed cell to the in-memory list AND to query_cells.jsonl
    (with the raw per-query latencies, so any percentile can be recomputed and
    nothing is lost if the run dies later)."""
    cell = {**base, **stats}
    c.append_jsonl("query_cells", {**cell, "latencies_ms": [round(x * 1000, 3) for x in lats]})
    out.append(cell)
    return cell


def _suite(ds, n_rows: int, variant: str, cfg: dict, rng) -> tuple[dict, list]:
    cols = ["cursor"] if variant == "metadata_only" else ["id", "cursor", "data"]
    win, hi = cfg["window"], max(1, n_rows - cfg["window"])
    lats, br = [], 0
    for i in range(cfg["samples"] + cfg["warmup"]):
        lo = int(rng.integers(0, hi))
        t = time.perf_counter()
        tbl = ds.to_table(columns=cols, filter=f"cursor >= {lo} AND cursor < {lo + win}")
        dt = time.perf_counter() - t
        if i >= cfg["warmup"]:
            lats.append(dt)
            br += tbl.nbytes
    return _cell_stats(lats, br), lats


def _suite_multi(handles: list, cfg: dict, rng) -> tuple[dict, list]:
    """Round-robin the query across several (ds, n_rows) handles."""
    win = cfg["window"]
    lats, br = [], 0
    for i in range(cfg["samples"] + cfg["warmup"]):
        ds, n_rows = handles[i % len(handles)]
        hi = max(1, n_rows - win)
        lo = int(rng.integers(0, hi))
        t = time.perf_counter()
        tbl = ds.to_table(columns=["id", "cursor", "data"], filter=f"cursor >= {lo} AND cursor < {lo + win}")
        dt = time.perf_counter() - t
        if i >= cfg["warmup"]:
            lats.append(dt)
            br += tbl.nbytes
    return _cell_stats(lats, br), lats


def _open(name: str, version=None):
    return lance.dataset(c.dataset_uri(PREFIX, name, safe=False),
                         version=version, storage_options=c.storage_options(False))


# --- axis: size (conn_0 at each checkpoint version) -------------------------
def size_cells(conn0: dict, cfg: dict, rng) -> list:
    out = []
    for ck in conn0["checkpoints"]:
        ds = _open(conn0["table"], ck["version"])
        for variant in ("windowed_full", "metadata_only"):
            stats, lats = _suite(ds, ck["rows"], variant, cfg, rng)
            base = {"axis": "size", "table": conn0["table"], "table_gb": ck["target_gb"],
                    "version": ck["version"], "fragments": ck["fragments"], "variant": variant}
            _emit(out, base, stats, lats)
            print(f"[query_degradation] size {ck['target_gb']}GB {variant}: "
                  f"p50={stats['p50_ms']} p95={stats['p95_ms']} p99={stats['p99_ms']}ms", flush=True)
    return out


# --- axis: layout (same vs different tables at default size) -----------------
def layout_cells(tables: list, cfg: dict, rng) -> list:
    gb = cfg["default_gb"]
    conn0 = tables[0]
    dck = _get_ckpt(conn0, gb)
    s_stats, s_lats = _suite(_open(conn0["table"], dck["version"]), dck["rows"], "windowed_full", cfg, rng)
    handles = [(_open(te["table"], _get_ckpt(te, gb)["version"]), _get_ckpt(te, gb)["rows"]) for te in tables]
    d_stats, d_lats = _suite_multi(handles, cfg, rng)
    out: list = []
    _emit(out, {"axis": "layout", "layout": "same_table", "table_gb": gb, "n_tables": 1,
                "fragments": dck["fragments"], "variant": "windowed_full"}, s_stats, s_lats)
    _emit(out, {"axis": "layout", "layout": "different_tables", "table_gb": gb, "n_tables": len(tables),
                "variant": "windowed_full"}, d_stats, d_lats)
    return out


# --- axis: neighbor (busy neighbors, same vs other tables) ------------------
def _neighbor_worker(uri: str, opts: dict, window: int, n_rows: int, counter, stop) -> None:
    ds = lance.dataset(uri, storage_options=opts)
    rng = np.random.default_rng(os.getpid())
    hi = max(1, n_rows - window)
    while not stop.is_set():
        lo = int(rng.integers(0, hi))
        try:
            ds.to_table(columns=["id", "cursor", "data"], filter=f"cursor >= {lo} AND cursor < {lo + window}")
            with counter.get_lock():
                counter.value += 1
        except Exception:  # noqa: BLE001
            pass


def _neighbor_cell(conn0: dict, dck: dict, load: str, K: int, tables: list, cfg: dict, rng) -> dict:
    ctx = mp.get_context("spawn")
    opts = c.storage_options(False)
    stop = ctx.Event()
    counters = [ctx.Value("L", 0) for _ in range(K)]
    procs = []

    if load == "same":
        targets = [(c.dataset_uri(PREFIX, conn0["table"], safe=False), dck["rows"])] * K
    else:
        sibs = tables[1:] or tables
        targets = []
        for k in range(K):
            te = sibs[k % len(sibs)]
            ck = _get_ckpt(te, cfg["default_gb"])
            targets.append((c.dataset_uri(PREFIX, te["table"], safe=False), ck["rows"]))

    def extra():
        return {"neighbor_ops": sum(cnt.value for cnt in counters)}

    with c.CgroupSampler(interval=0.5, extra_fn=extra) as smp:
        for k in range(K):
            uri, nr = targets[k]
            p = ctx.Process(target=_neighbor_worker, args=(uri, opts, cfg["window"], nr, counters[k], stop))
            p.start()
            procs.append(p)
        time.sleep(1.0)  # let neighbors ramp before measuring
        ds0 = _open(conn0["table"], dck["version"])
        t0 = time.perf_counter()
        stats, lats = _suite(ds0, dck["rows"], "windowed_full", cfg, rng)
        elapsed = time.perf_counter() - t0
        stop.set()
        for p in procs:
            p.join(timeout=10)
    nops = sum(cnt.value for cnt in counters)
    peak_cpu = max((s["cpu_cores"] for s in smp.samples), default=0.0)
    mean_cpu = round(sum(s["cpu_cores"] for s in smp.samples) / len(smp.samples), 2) if smp.samples else 0.0
    print(f"[query_degradation] neighbor {load} K={K}: p50={stats['p50_ms']} p95={stats['p95_ms']}ms "
          f"(neighbor {round(nops/elapsed,1) if elapsed>0 else 0} qps, peak {round(peak_cpu,1)} cores)", flush=True)
    base = {"axis": "neighbor", "load": load, "k": K, "table_gb": dck["target_gb"],
            "neighbor_qps": round(nops / elapsed, 1) if elapsed > 0 else 0,
            "peak_cpu_cores": round(peak_cpu, 2), "mean_cpu_cores": mean_cpu, "variant": "windowed_full"}
    return base, stats, lats


def neighbor_cells(conn0: dict, tables: list, cfg: dict, rng) -> list:
    dck = _get_ckpt(conn0, cfg["default_gb"])
    out: list = []
    i_stats, i_lats = _suite(_open(conn0["table"], dck["version"]), dck["rows"], "windowed_full", cfg, rng)
    _emit(out, {"axis": "neighbor", "load": "idle", "k": 0, "table_gb": dck["target_gb"],
                "neighbor_qps": 0, "peak_cpu_cores": None, "variant": "windowed_full"}, i_stats, i_lats)
    for load in ("same", "other"):
        for K in cfg["neighbor_ks"]:
            base, stats, lats = _neighbor_cell(conn0, dck, load, K, tables, cfg, rng)
            _emit(out, base, stats, lats)
    return out


# --- axis: table count (single-table latency + catalog listing vs #tables) --
def table_count_cells(cfg: dict, rng) -> list:
    opts = c.storage_options(False)
    out = []
    created = 0
    for N in cfg["table_count_ns"]:
        while created < N:  # incremental: exactly N tables exist at this step
            name = f"t{created}"
            lance.write_dataset(c.make_blob_rows(5000, 1024, 0, name),
                                c.dataset_uri(TABLECOUNT_PREFIX, name, safe=False),
                                mode="overwrite", storage_options=opts)
            created += 1
        ds = lance.dataset(c.dataset_uri(TABLECOUNT_PREFIX, "t0", safe=False), storage_options=opts)
        stats, lats = _suite(ds, 5000, "windowed_full", cfg, rng)
        db = lancedb.connect(c.db_uri(TABLECOUNT_PREFIX), storage_options=opts)
        _list = getattr(db, "list_tables", None) or db.table_names  # list_tables() in newer lancedb
        lt = []
        for _ in range(20):
            t = time.perf_counter()
            _list()
            lt.append(time.perf_counter() - t)
        base = {"axis": "table_count", "n_tables": N, "variant": "windowed_full",
                "list_p50_ms": _pct(lt, 50), "list_p95_ms": _pct(lt, 95)}
        _emit(out, base, stats, lats)
        print(f"[query_degradation] table_count N={N}: query p50={stats['p50_ms']}ms, "
              f"list p50={_pct(lt,50)}ms", flush=True)
    return out


# --- axis: compaction (one compaction, isolated; OOM = finding) -------------
def _compact_worker(uri: str, opts: dict, trpf: int, q) -> None:
    ds = lance.dataset(uri, storage_options=opts)
    before = len(ds.get_fragments())
    proc = psutil.Process()
    stop = threading.Event()

    def _heartbeat() -> None:  # live progress: compacting 1 TB rewrites ~1 TB, can take hours
        t0 = time.perf_counter()
        while not stop.wait(30):
            el = round(time.perf_counter() - t0)
            rss = round(proc.memory_info().rss / 1e6)
            print(f"[query_degradation] compaction running… {el}s, rss={rss}MB", flush=True)
            try:
                c.append_jsonl("compaction_heartbeat", {"elapsed_s": el, "rss_mb": rss})
            except Exception:  # noqa: BLE001
                pass

    hb = threading.Thread(target=_heartbeat, daemon=True)
    hb.start()
    try:
        with c.Meter() as m:
            ds.optimize.compact_files(target_rows_per_fragment=trpf)  # NO cleanup_old_versions (constraint)
        ds2 = lance.dataset(uri, storage_options=opts)
        q.put({"oom": False, "error": None, "fragments_before": before,
               "fragments_after": len(ds2.get_fragments()), "version_after": ds2.version,
               "target_rows_per_fragment": trpf, "wall_s": round(m.metrics.wall_s, 2),
               "cpu_s": round(m.metrics.cpu_s, 2), "peak_rss_mb": m.metrics.peak_rss_mb})
    except Exception as exc:  # noqa: BLE001
        q.put({"oom": False, "error": str(exc)[:300], "fragments_before": before})
    finally:
        stop.set()


def _run_isolated(target, *args) -> dict:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    p = ctx.Process(target=target, args=(*args, q))
    t0 = time.perf_counter()
    p.start()
    p.join()
    elapsed = round(time.perf_counter() - t0, 1)  # wall even if the child OOM-dies before reporting
    if p.exitcode == 0 and not q.empty():
        r = q.get()
        r["elapsed_s"] = elapsed
        return r
    if p.exitcode == -9:
        return {"oom": True, "exitcode": -9, "elapsed_s": elapsed,
                "error": f"OOM-killed (SIGKILL) after {elapsed}s during compaction"}
    return {"error": f"compaction worker died exitcode={p.exitcode} after {elapsed}s",
            "exitcode": p.exitcode, "elapsed_s": elapsed}


def compaction_cells(conn0: dict, cfg: dict, rng) -> tuple[list, dict]:
    opts = c.storage_options(False)
    name = conn0["table"]
    uri = c.dataset_uri(PREFIX, name, safe=False)
    total = conn0.get("total") or {}
    n_rows = total.get("rows") or max(x["rows"] for x in conn0["checkpoints"])
    gb = round((total.get("data_bytes") or 0) / 1e9, 0) or cfg["default_gb"]

    ds = lance.dataset(uri, storage_options=opts)
    frags_before = len(ds.get_fragments())
    cells: list = []
    for variant in ("windowed_full", "metadata_only"):
        stats, lats = _suite(ds, n_rows, variant, cfg, rng)
        _emit(cells, {"axis": "compaction", "state": "uncompacted", "table_gb": gb,
                      "fragments": frags_before, "variant": variant}, stats, lats)
    print(f"[query_degradation] compacting {name} ({frags_before} frags, ~{gb}GB, trpf={cfg['compact_trpf']})...",
          flush=True)
    probe = _run_isolated(_compact_worker, uri, opts, cfg["compact_trpf"])
    c.append_jsonl("compaction_probe", probe)
    if not probe.get("error"):
        ds2 = lance.dataset(uri, storage_options=opts)
        after = probe.get("fragments_after")
        for variant in ("windowed_full", "metadata_only"):
            stats, lats = _suite(ds2, n_rows, variant, cfg, rng)
            _emit(cells, {"axis": "compaction", "state": "compacted", "table_gb": gb,
                          "fragments": after, "variant": variant}, stats, lats)
        print(f"[query_degradation] compaction done: {frags_before}→{after} frags, "
              f"wall={probe.get('wall_s')}s cpu={probe.get('cpu_s')}s peak_rss={probe.get('peak_rss_mb')}MB "
              f"elapsed={probe.get('elapsed_s')}s", flush=True)
    else:
        print(f"[query_degradation] compaction FINDING: {probe}", flush=True)
    return cells, probe


def main() -> None:
    ing = _load_ingest()
    tables = [t for t in (ing or {}).get("writers", []) if t.get("checkpoints")]
    if not tables:
        print("[query_degradation] no usable parallel_ingest.json — run bench_parallel_ingest first", flush=True)
        c.write_result("query_degradation", {"error": "no ingest data"})
        return
    cfg = _load_qcfg(tables)
    host = c.host_info()
    print(f"[query_degradation] host: {host}", flush=True)
    print(f"[query_degradation] config: {cfg}", flush=True)
    c.append_jsonl("run_meta", {"bench": "query_degradation", "config": cfg, "host": host})
    rng = np.random.default_rng(cfg["seed"])
    conn0 = tables[0]

    def _write(cell_list, probe, partial):
        c.write_result("query_degradation", {
            "config": cfg,
            "host": host,
            "cpu_limit_cores": c.cgroup_cpu_limit_cores(),
            "cells": cell_list,
            "compaction_probe": probe,
            "partial": partial,
        }, quiet=True)

    # Run each axis, snapshotting the consolidated JSON after every one so a
    # crash mid-suite still leaves a usable report source (each cell is also
    # append-logged to query_cells.jsonl with its raw latencies as it completes).
    cells: list = []
    for label, axis_fn in (("size", lambda: size_cells(conn0, cfg, rng)),
                           ("layout", lambda: layout_cells(tables, cfg, rng)),
                           ("neighbor", lambda: neighbor_cells(conn0, tables, cfg, rng)),
                           ("table_count", lambda: table_count_cells(cfg, rng))):
        cells += axis_fn()
        _write(cells, None, partial=True)
        print(f"[query_degradation] axis '{label}' done — {len(cells)} cells persisted", flush=True)

    # Compaction is the memory-risky part (it OOM-killed a real run and took the
    # tmux session with it), so everything above is already on disk + in S3.
    if os.environ.get("BENCH_SKIP_COMPACTION") == "1":
        print("[query_degradation] BENCH_SKIP_COMPACTION=1 — skipping compaction axis", flush=True)
        _write(cells, {"skipped": True}, partial=False)
        return

    comp_cells, probe = compaction_cells(conn0, cfg, rng)  # mutates conn_0 (old versions kept)
    cells += comp_cells
    _write(cells, probe, partial=False)
    print(f"[query_degradation] done — {len(cells)} cells", flush=True)


if __name__ == "__main__":
    main()
