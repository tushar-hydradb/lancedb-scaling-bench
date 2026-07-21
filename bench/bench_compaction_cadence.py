"""Compaction-cadence bench (Q: does compacting *often* bound OOM risk + wall time?).

Hypothesis to test: the FIRST compaction of a large, uncompacted table (the accumulated
drain backlog) is expensive — long wall + high RSS, because it rewrites the whole body.
But once you're caught up, compacting after each small append is cheap and FLAT, because
each subsequent compaction only consolidates the fresh delta (the body is now at target
and is skipped). So the curve should be a big spike on turn 1, then a low plateau.

Design (all ONE local table, so the contrast is within-run — same box, same scale, no
cross-environment apples-to-oranges):
  * Seed one table to ~500 GB the way the drain actually lands it: many SMALL fragments
    (seed_rows_per_array < trpf), i.e. an uncompacted backlog. NOT pre-compacted.
  * Loop `turns` times: read -> append (~0.5 GB) -> compact(trpf). No version cleanup.
      - Turn 1's compaction hits the full 500 GB backlog  → the expensive one.
      - Turns 2..N only ever see the small fresh deltas   → cheap + flat.
  * Every op runs in its OWN spawned process, so its peak RSS (Meter, fresh baseline) is
    honestly attributable to *that* op — a long-lived process's RSS high-water would smear
    a cheap op's cost with retained allocations from earlier ops.
  * Records wall_s / cpu_s / peak_rss_mb / fragments / version for every op, appended to
    JSONL after each turn and snapshotted to compaction_cadence.json (crash-safe).

The headline comparison is turn-1 (backlog) vs the turns-2..N mean (steady state), both
measured here, locally, at 500 GB. The 1 TB EC2/real-S3 terminal number is NOT a valid
contrast (different scale AND network) and is only kept as a loosely-labelled aside.
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
# 1 TB EC2/real-S3 terminal compaction (results/compaction_probe.jsonl). Kept only as a
# loosely-labelled aside — NOT a valid contrast to the local 500 GB numbers (different
# scale AND no network). The real contrast is turn-1 vs steady-state, both measured here.
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
        # seed_rows_per_array < trpf => seed lands as small UNDER-target fragments (an
        # uncompacted backlog), so turn 1's compaction pays the full-table cost.
        "trpf": int(os.environ.get("BENCH_TRPF", 50 if smoke else 500)),
        "seed_rows_per_array": int(os.environ.get("BENCH_SEED_RPA", 20 if smoke else 200)),
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
    schema = c.moveit_schema()  # ~0.5 GB/array, well under the int32 offset cap
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
    schema = c.moveit_schema()  # ~1 GB/array at 200 rows — under the int32 offset cap
    rng = np.random.default_rng(cfg["seed_rng"])
    rpa = cfg["seed_rows_per_array"]
    target_rows = max(rpa, round(cfg["seed_gb"] * 1e9 / cfg["mean_bytes"]))
    print(f"[cadence] seeding ~{cfg['seed_gb']}GB = {target_rows} rows in {rpa}-row "
          f"fragments (UNDER trpf={cfg['trpf']} => uncompacted backlog for turn 1) ...", flush=True)

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
    # Headline contrast: turn-1 (full backlog) vs steady-state (turns 2..N) — both here.
    turns_ok = [t for t in result["turns"] if not t["compact"].get("error")]
    if turns_ok:
        backlog = turns_ok[0]["compact"]
        steady = [t["compact"] for t in turns_ok[1:]]

        def _agg(ops: list[dict]) -> dict:
            rss = [o["peak_rss_mb"] for o in ops]
            wall = [o["wall_s"] for o in ops]
            return {"n": len(ops), "peak_rss_mb_max": max(rss),
                    "peak_rss_mb_mean": round(sum(rss) / len(rss), 1),
                    "wall_s_max": max(wall), "wall_s_mean": round(sum(wall) / len(wall), 2)}

        result["compact_summary"] = {
            "backlog_turn1": {"peak_rss_mb": backlog["peak_rss_mb"], "wall_s": backlog["wall_s"],
                              "fragments_before": backlog["fragments_before"],
                              "fragments_after": backlog["fragments_after"]},
            "steady_state": _agg(steady) if steady else None,
            "any_oom": any(t["compact"].get("oom") for t in result["turns"]),
        }
    snapshot()
    print(f"[cadence] done -> {c.RESULTS_DIR}/compaction_cadence.json", flush=True)


if __name__ == "__main__":
    main()
