"""Compaction-cadence bench (Q: does compacting *often* bound OOM risk + wall time?).

Motivation: the one-shot terminal compaction of a 1 TB table (bench_query_degradation)
peaked at **23.5 GB RSS over 34.7 min**. The hypothesis this bench confirms: if you
instead compact after every small append, each compaction only ever consolidates the
*fresh* delta — never re-reading the 500 GB body — so both peak RSS and wall time stay
low and FLAT across turns, regardless of how large the table (and its un-cleaned version
history) grows.

Design:
  * Seed one table to ~500 GB, laid out as fragments with MORE rows than `trpf` so
    compaction treats the seed body as already-optimal and never rewrites it (the
    realistic drain state: history already compacted, only recent appends are small).
  * Loop `turns` times: read -> append (~0.5 GB) -> compact(trpf). No version cleanup.
  * Every op runs in its OWN spawned process, so its peak RSS (Meter, fresh baseline) is
    honestly attributable to *that* op — a long-lived process's RSS high-water would smear
    a cheap op's cost with retained allocations from earlier ops.
  * Records wall_s / cpu_s / peak_rss_mb / fragments / version for every op, appended to
    JSONL after each turn and snapshotted to compaction_cadence.json (crash-safe).

Uses moveit_schema_large() (int64 offsets) so a single seed array can hold >2 GB, letting
seed fragments exceed `trpf` rows in one commit.
"""
from __future__ import annotations

import multiprocessing as mp
import os
import time

import lance
import numpy as np

import common as c

PREFIX = "cadence"
TABLE = "conn_cadence"
# Terminal-compaction reference points (from the 1 TB run, results/compaction_probe.jsonl)
# so the report/plots can draw the contrast line.
TERMINAL_RSS_MB = 23581.8
TERMINAL_WALL_S = 2081.69
POD_MEM_MB = 4096  # MOVEIT prod pod cap — the OOM line that matters in production


def _cfg() -> dict:
    smoke = os.environ.get("BENCH_SMOKE") == "1"
    mean = 5e6
    append_gb = float(os.environ.get("BENCH_APPEND_GB", 0.05 if smoke else 0.5))
    return {
        "seed_gb": float(os.environ.get("BENCH_SEED_GB", 5 if smoke else 500)),
        "turns": int(os.environ.get("BENCH_TURNS", 3 if smoke else 50)),
        "append_gb": append_gb,
        "append_rows": max(1, round(append_gb * 1e9 / mean)),
        # trpf < seed_rows_per_array => the seed body is above target, never recompacted.
        "trpf": int(os.environ.get("BENCH_TRPF", 50 if smoke else 500)),
        "seed_rows_per_array": int(os.environ.get("BENCH_SEED_RPA", 60 if smoke else 600)),
        "mean_bytes": mean,
        "std_bytes": 1e6,
        "min_bytes": 64000,
        "window": int(os.environ.get("BENCH_WINDOW", 20)),
        "read_samples": int(os.environ.get("BENCH_READ_SAMPLES", 15)),
        "seed_rng": 13,
        "smoke": smoke,
    }


def _uri() -> str:
    return c.dataset_uri(PREFIX, TABLE, safe=False)


# --- isolated op workers (module-level, spawn-picklable) --------------------
def _append_worker(uri, opts, key_start, rows, mean, std, minb, seed, q) -> None:
    rng = np.random.default_rng(seed)
    schema = c.moveit_schema_large()
    with c.Meter() as m:
        batch = c.make_blob_rows_gaussian(rows, mean, std, int(minb), key_start,
                                          "cadence", rng, schema=schema)
        data_bytes = batch.column("data").nbytes
        lance.write_dataset(batch, uri, mode="append", storage_options=opts,
                            schema=schema)
    ds = lance.dataset(uri, storage_options=opts)
    q.put({"op": "append", "rows": rows, "data_bytes": int(data_bytes),
           "fragments_after": len(ds.get_fragments()), "version": ds.version,
           "wall_s": round(m.metrics.wall_s, 3), "cpu_s": round(m.metrics.cpu_s, 3),
           "peak_rss_mb": m.metrics.peak_rss_mb})


def _compact_worker(uri, opts, trpf, q) -> None:
    ds = lance.dataset(uri, storage_options=opts)
    before = len(ds.get_fragments())
    ver_before = ds.version
    with c.Meter() as m:
        ds.optimize.compact_files(target_rows_per_fragment=trpf)  # NO cleanup_old_versions
    ds2 = lance.dataset(uri, storage_options=opts)
    q.put({"op": "compact", "trpf": trpf, "fragments_before": before,
           "fragments_after": len(ds2.get_fragments()), "version_before": ver_before,
           "version": ds2.version, "wall_s": round(m.metrics.wall_s, 3),
           "cpu_s": round(m.metrics.cpu_s, 3), "peak_rss_mb": m.metrics.peak_rss_mb})


def _read_worker(uri, opts, total_rows, window, samples, seed, q) -> None:
    rng = np.random.default_rng(seed)
    ds = lance.dataset(uri, storage_options=opts)
    n_frag = len(ds.get_fragments())
    lats: list[float] = []
    total_bytes = 0
    with c.Meter() as m:
        # a fresh reader's manifest-open cost is part of the read's wall (realistic;
        # and it's what surfaces any version-accumulation slowdown).
        for _ in range(samples):
            lo = int(rng.integers(0, max(1, total_rows - window)))
            t0 = time.perf_counter()
            tbl = ds.to_table(columns=["id", "cursor", "data"],
                              filter=f"cursor >= {lo} AND cursor < {lo + window}")
            lats.append((time.perf_counter() - t0) * 1000.0)
            total_bytes += tbl.get_total_buffer_size()
    arr = np.array(lats)
    q.put({"op": "read", "samples": samples, "window": window, "fragments": n_frag,
           "version": ds.version, "p50_ms": round(float(np.percentile(arr, 50)), 2),
           "p95_ms": round(float(np.percentile(arr, 95)), 2),
           "mean_ms": round(float(arr.mean()), 2), "bytes_read": int(total_bytes),
           "wall_s": round(m.metrics.wall_s, 3), "cpu_s": round(m.metrics.cpu_s, 3),
           "peak_rss_mb": m.metrics.peak_rss_mb})


def _isolated(target, *args) -> dict:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    p = ctx.Process(target=target, args=(*args, q))
    t0 = time.perf_counter()
    p.start()
    p.join()
    elapsed = round(time.perf_counter() - t0, 2)
    if p.exitcode == 0 and not q.empty():
        r = q.get()
        r["elapsed_s"] = elapsed
        return r
    if p.exitcode == -9:  # SIGKILL == OOM under a mem cap
        return {"oom": True, "exitcode": -9, "elapsed_s": elapsed,
                "error": f"OOM-killed (SIGKILL) after {elapsed}s"}
    return {"error": f"worker died exitcode={p.exitcode} after {elapsed}s",
            "exitcode": p.exitcode, "elapsed_s": elapsed}


# --- seeding ----------------------------------------------------------------
def _seed(cfg: dict) -> dict:
    """Build ~seed_gb into the table as fragments of seed_rows_per_array rows each
    (each > trpf, so compaction never rewrites them). Done in-process (setup, not a
    measured per-op cost) with progress logged to JSONL."""
    uri = _uri()
    opts = c.storage_options(False)
    schema = c.moveit_schema_large()
    rng = np.random.default_rng(cfg["seed_rng"])
    rpa = cfg["seed_rows_per_array"]
    target_rows = max(rpa, round(cfg["seed_gb"] * 1e9 / cfg["mean_bytes"]))
    print(f"[cadence] seeding ~{cfg['seed_gb']}GB = {target_rows} rows in {rpa}-row "
          f"fragments (trpf={cfg['trpf']}) ...", flush=True)

    written = 0
    key = 0
    mode = "create"  # first write creates the dataset
    t0 = time.perf_counter()
    next_log = 50e9
    data_bytes = 0
    while written < target_rows:
        n = min(rpa, target_rows - written)
        batch = c.make_blob_rows_gaussian(n, cfg["mean_bytes"], cfg["std_bytes"],
                                          cfg["min_bytes"], key, "cadence", rng,
                                          schema=schema)
        data_bytes += batch.column("data").nbytes
        # overwrite on the very first commit only, append thereafter
        lance.write_dataset(batch, uri, mode="overwrite" if mode == "create" else "append",
                            storage_options=opts, schema=schema)
        mode = "append"
        written += n
        key += n
        if data_bytes >= next_log:
            el = round(time.perf_counter() - t0)
            print(f"[cadence] seeded {data_bytes/1e9:.0f}GB / {written} rows ({el}s)", flush=True)
            c.append_jsonl("cadence_seed", {"data_gb": round(data_bytes / 1e9, 1),
                                            "rows": written, "elapsed_s": el})
            next_log += 50e9

    ds = lance.dataset(uri, storage_options=opts)
    info = {"rows": written, "data_bytes": int(data_bytes),
            "fragments": len(ds.get_fragments()), "version": ds.version,
            "wall_s": round(time.perf_counter() - t0, 1),
            "rows_per_array": rpa}
    print(f"[cadence] seed done: {written} rows, {info['fragments']} fragments, "
          f"{data_bytes/1e9:.0f}GB, {info['wall_s']}s", flush=True)
    c.append_jsonl("cadence_seed", {"done": True, **info})
    return info


# --- main loop --------------------------------------------------------------
def main() -> None:
    cfg = _cfg()
    host = c.host_info()
    c.append_jsonl("run_meta", {"bench": "compaction_cadence", "config": cfg, "host": host})
    opts = c.storage_options(False)
    uri = _uri()

    result = {"bench": "compaction_cadence", "cap_label": c.CAP_LABEL,
              "unix_time": int(time.time()), "config": cfg, "host": host,
              "reference": {"terminal_rss_mb": TERMINAL_RSS_MB,
                            "terminal_wall_s": TERMINAL_WALL_S, "pod_mem_mb": POD_MEM_MB},
              "seed": None, "turns": [], "partial": True}

    def snapshot() -> None:
        c.write_result("compaction_cadence", result, quiet=True)

    # Fresh table each run so fragment/version counts are deterministic.
    seed = _seed(cfg)
    result["seed"] = seed
    row_count = seed["rows"]
    key = seed["rows"]
    snapshot()

    for t in range(1, cfg["turns"] + 1):
        rec: dict = {"turn": t}
        # read (as the table currently stands)
        rec["read"] = _isolated(_read_worker, uri, opts, row_count, cfg["window"],
                                cfg["read_samples"], 1000 + t)
        # append ~append_gb
        rec["append"] = _isolated(_append_worker, uri, opts, key, cfg["append_rows"],
                                  cfg["mean_bytes"], cfg["std_bytes"], cfg["min_bytes"],
                                  2000 + t)
        key += cfg["append_rows"]
        row_count += cfg["append_rows"]
        # compact (only the small fresh delta should be touched)
        rec["compact"] = _isolated(_compact_worker, uri, opts, cfg["trpf"])
        # post-turn table state
        ds = lance.dataset(uri, storage_options=opts)
        rec["fragments"] = len(ds.get_fragments())
        rec["version"] = ds.version
        rec["table_gb"] = round((seed["data_bytes"] + t * cfg["append_rows"] * cfg["mean_bytes"]) / 1e9, 1)

        result["turns"].append(rec)
        c.append_jsonl("cadence_ops", rec)
        snapshot()
        comp = rec["compact"]
        print(f"[cadence] turn {t}/{cfg['turns']}: "
              f"read p50={rec['read'].get('p50_ms')}ms rss={rec['read'].get('peak_rss_mb')}MB | "
              f"append {rec['append'].get('wall_s')}s rss={rec['append'].get('peak_rss_mb')}MB | "
              f"compact {comp.get('wall_s')}s rss={comp.get('peak_rss_mb')}MB "
              f"frags {comp.get('fragments_before')}->{comp.get('fragments_after')} "
              f"ver={rec['version']}", flush=True)

    result["partial"] = False
    # convenience aggregates over the compaction ops
    comps = [t["compact"] for t in result["turns"] if not t["compact"].get("error")]
    if comps:
        rss = [x["peak_rss_mb"] for x in comps]
        wall = [x["wall_s"] for x in comps]
        result["compact_summary"] = {
            "n": len(comps),
            "peak_rss_mb_max": max(rss), "peak_rss_mb_mean": round(sum(rss) / len(rss), 1),
            "wall_s_max": max(wall), "wall_s_mean": round(sum(wall) / len(wall), 2),
            "any_oom": any(t["compact"].get("oom") for t in result["turns"]),
        }
    snapshot()
    print(f"[cadence] done -> {c.RESULTS_DIR}/compaction_cadence.json", flush=True)


if __name__ == "__main__":
    main()
