"""Q1 — parallel ingest ceiling + how a connector table scales to/past 1 TB.

Creates multiple connector-style tables (one `.lance` per writer, MOVEIT's
one-table-per-connector model) and appends ~5 MB rows (gaussian size spread)
into each until it crosses the per-table target (~1 TB). Two phases:

  * **Writer sweep** — ingest a small per-writer target with N ∈ {1,2,4}
    concurrent writers (each its own table) to measure how aggregate throughput
    scales with concurrency (and where it stops scaling: CPU vs NIC/S3).

  * **Primary build** — N writers each build to the full target, recording at
    every size checkpoint: the dataset **version** (so the query bench can read
    the table *as it was* at that size — versions are intact because cleanup is
    intentionally OFF), rows, fragment count, and the *interval* ingest MB/s
    (does write slow as fragments climb?).

A container-level CgroupSampler records CPU-cores + RSS + aggregate bytes over
the whole build. On-disk size is not a reported metric; we read S3 only for the
fragment *count* (needed to make the later compaction contrast meaningful).

Uncapped on EC2 against real S3 (BENCH_S3_REAL=1, IAM instance role). Batches are
kept at ~1 GB of `data` (200 rows × 5 MB) — ~72σ under the 2 GB int32 offset cap.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import queue
import time

import lance
import numpy as np
import psutil

import common as c

PREFIX = "bigscale"


# --- config (env-overridable; BENCH_SMOKE=1 shrinks everything for a dry run)--
def _load_cfg() -> dict:
    smoke = os.environ.get("BENCH_SMOKE") == "1"
    ck_env = os.environ.get("BENCH_CHECKPOINTS_GB")
    cfg = {
        "n_writers": int(os.environ.get("BENCH_N_WRITERS", 4)),
        "per_table_target_gb": float(os.environ.get("BENCH_PER_TABLE_TARGET_GB", 1000)),
        "checkpoints_gb": [100.0, 250.0, 500.0, 1000.0],
        "rows_per_array": int(os.environ.get("BENCH_ROWS_PER_ARRAY", 200)),
        "mean_bytes": float(os.environ.get("BENCH_MEAN_BYTES", 5_000_000)),
        "std_bytes": float(os.environ.get("BENCH_STD_BYTES", 1_000_000)),
        "min_bytes": int(os.environ.get("BENCH_MIN_BYTES", 64_000)),
        "seed": int(os.environ.get("BENCH_SEED", 7)),
        "use_large": os.environ.get("BENCH_USE_LARGE_STRING") == "1",
        "sweep_ns": [1, 2, 4],
        "sweep_target_gb": float(os.environ.get("BENCH_SWEEP_TARGET_GB", 10)),
        "smoke": smoke,
    }
    if smoke:
        cfg.update(n_writers=2, per_table_target_gb=1.0,
                   checkpoints_gb=[0.25, 0.5, 1.0], sweep_ns=[1, 2], sweep_target_gb=0.25)
    if ck_env:
        cfg["checkpoints_gb"] = [float(x) for x in ck_env.split(",")]
    return cfg


def _schema(use_large: bool):
    return c.moveit_schema_large() if use_large else c.moveit_schema()


# --- ingest worker (module-level so spawn can import + run it) ----------------
def _ingest_worker(uri: str, opts: dict, name: str, seed: int, target_bytes: int,
                   checkpoints_gb: list, cfg: dict, bytes_counter, q) -> None:
    """Append gaussian ~5 MB rows to `uri` until `target_bytes`, emitting a
    checkpoint message each time cumulative `data` bytes cross a checkpoint."""
    rng = np.random.default_rng(seed)
    schema = _schema(cfg["use_large"])
    proc = psutil.Process()
    ck_bytes = [gb * 1e9 for gb in checkpoints_gb]

    written = rows = commits = 0
    ck_idx = 0
    t0 = time.perf_counter()
    last_wall, last_bytes, last_rows = t0, 0, 0
    mode = "overwrite"  # first commit creates/replaces the table (idempotent re-run)

    while written < target_bytes:
        rb = c.make_blob_rows_gaussian(
            cfg["rows_per_array"], cfg["mean_bytes"], cfg["std_bytes"], cfg["min_bytes"],
            rows, name, rng, schema=schema)
        lance.write_dataset(rb, uri, mode=mode, storage_options=opts)
        mode = "append"
        written += rb.column("data").nbytes
        rows += cfg["rows_per_array"]
        commits += 1
        with bytes_counter.get_lock():
            bytes_counter.value = written

        while ck_idx < len(ck_bytes) and written >= ck_bytes[ck_idx]:
            now = time.perf_counter()
            ver = lance.dataset(uri, storage_options=opts).version
            fp = c.s3_footprint(PREFIX, name)
            iv = now - last_wall
            q.put({
                "type": "checkpoint", "table": name, "target_gb": checkpoints_gb[ck_idx],
                "version": ver, "rows": rows, "data_bytes": written,
                "fragments": fp.fragment_count, "objects": fp.object_count, "commits": commits,
                "cum_wall_s": round(now - t0, 2), "interval_wall_s": round(iv, 2),
                "interval_mb_per_s": round((written - last_bytes) / 1e6 / iv, 1) if iv > 0 else 0,
                "interval_rows_per_s": round((rows - last_rows) / iv, 1) if iv > 0 else 0,
                "rss_mb": round(proc.memory_info().rss / 1e6, 1),
            })
            last_wall, last_bytes, last_rows = now, written, rows
            ck_idx += 1

    q.put({"type": "done", "table": name, "rows": rows, "data_bytes": written,
           "commits": commits, "wall_s": round(time.perf_counter() - t0, 2)})


def _drain(procs, q) -> tuple[list, list]:
    """Collect checkpoint + done messages until all writers exit and q is empty."""
    checkpoints, dones = [], []
    while any(p.is_alive() for p in procs) or not q.empty():
        try:
            msg = q.get(timeout=0.5)
        except queue.Empty:
            continue
        (checkpoints if msg["type"] == "checkpoint" else dones).append(msg)
    for p in procs:
        p.join(timeout=30)
    return checkpoints, dones


# --- phase A: writer-scaling sweep ------------------------------------------
def run_writer_sweep(cfg: dict) -> list:
    opts = c.storage_options(False)
    target_bytes = int(cfg["sweep_target_gb"] * 1e9)
    out = []
    for nw in cfg["sweep_ns"]:
        ctx = mp.get_context("spawn")
        counters = [ctx.Value("L", 0) for _ in range(nw)]
        q: mp.Queue = ctx.Queue()
        procs = []
        t0 = time.perf_counter()
        for i in range(nw):
            name = f"sweep_n{nw}_w{i}"
            p = ctx.Process(target=_ingest_worker, args=(
                c.dataset_uri(PREFIX, name, safe=False), opts, name, cfg["seed"] + 100 * nw + i,
                target_bytes, [], cfg, counters[i], q))
            p.start()
            procs.append(p)
        _, dones = _drain(procs, q)
        wall = time.perf_counter() - t0
        total_b = sum(d["data_bytes"] for d in dones)
        total_r = sum(d["rows"] for d in dones)
        out.append({
            "n_writers": nw, "per_table_target_gb": cfg["sweep_target_gb"], "wall_s": round(wall, 2),
            "agg_mb_per_s": round(total_b / 1e6 / wall, 1) if wall > 0 else 0,
            "agg_rows_per_s": round(total_r / wall, 1) if wall > 0 else 0,
            "per_writer_mb_per_s": [round(d["data_bytes"] / 1e6 / d["wall_s"], 1) for d in dones if d["wall_s"] > 0],
        })
        print(f"[parallel_ingest] sweep N={nw}: agg {out[-1]['agg_mb_per_s']} MB/s "
              f"({out[-1]['wall_s']}s)", flush=True)
    # scaling efficiency vs the single-writer baseline
    if out:
        base = out[0]["agg_mb_per_s"] or 1.0
        for r in out:
            r["scaling_efficiency"] = round(r["agg_mb_per_s"] / (base * r["n_writers"]), 3)
    return out


# --- phase B: primary build to the full per-table target --------------------
def run_primary_build(cfg: dict) -> dict:
    ctx = mp.get_context("spawn")
    opts = c.storage_options(False)
    n = cfg["n_writers"]
    target_bytes = int(cfg["per_table_target_gb"] * 1e9)
    counters = [ctx.Value("L", 0) for _ in range(n)]
    q: mp.Queue = ctx.Queue()
    procs = []
    state = {"active": 0}

    def extra():
        return {"active_writers": state["active"],
                "agg_bytes": sum(counters[i].value for i in range(n))}

    with c.CgroupSampler(interval=0.5, extra_fn=extra) as smp:
        t0 = time.perf_counter()
        for i in range(n):
            name = f"conn_{i}"
            p = ctx.Process(target=_ingest_worker, args=(
                c.dataset_uri(PREFIX, name, safe=False), opts, name, cfg["seed"] + i,
                target_bytes, cfg["checkpoints_gb"], cfg, counters[i], q))
            p.start()
            procs.append(p)
            state["active"] = i + 1
            smp.mark("writer_started", writer=i)
        checkpoints, dones = _drain(procs, q)
        for ck in checkpoints:
            smp.mark("checkpoint", table=ck["table"], gb=ck["target_gb"])
        wall = time.perf_counter() - t0

    # regroup per table
    tables = []
    for i in range(n):
        name = f"conn_{i}"
        cks = sorted([ck for ck in checkpoints if ck["table"] == name], key=lambda x: x["target_gb"])
        done = next((d for d in dones if d["table"] == name), None)
        tables.append({"table": name, "checkpoints": cks, "total": done})

    total_b = sum(d["data_bytes"] for d in dones)
    total_r = sum(d["rows"] for d in dones)
    aggregate = {
        "n_writers": n, "wall_s": round(wall, 2),
        "agg_mb_per_s": round(total_b / 1e6 / wall, 1) if wall > 0 else 0,
        "agg_rows_per_s": round(total_r / wall, 1) if wall > 0 else 0,
        "per_writer_mb_per_s": [round(d["data_bytes"] / 1e6 / d["wall_s"], 1) for d in dones if d["wall_s"] > 0],
        "total_data_bytes": total_b,
    }
    print(f"[parallel_ingest] primary build: {n} writers, agg {aggregate['agg_mb_per_s']} MB/s, "
          f"{round(total_b/1e9,1)} GB in {round(wall/60,1)} min", flush=True)
    return {"tables": tables, "aggregate": aggregate, "samples": smp.samples, "events": smp.events}


def main() -> None:
    cfg = _load_cfg()
    print(f"[parallel_ingest] config: {cfg}", flush=True)
    sweep = run_writer_sweep(cfg)
    primary = run_primary_build(cfg)
    c.write_result("parallel_ingest", {
        "config": cfg,
        "cpu_limit_cores": c.cgroup_cpu_limit_cores(),
        "sweep_writers": sweep,
        "writers": primary["tables"],
        "aggregate": primary["aggregate"],
        "samples": primary["samples"],
        "events": primary["events"],
    })


if __name__ == "__main__":
    main()
