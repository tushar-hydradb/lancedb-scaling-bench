"""Time-series capture: container CPU/mem vs cumulative query count.

Runs each read/write pattern as a stream of operations, sampling container-level
CPU (cores) and RSS (MB) throughout and marking the cumulative query count at
each op. Output feeds plots.py to draw "CPU/mem vs query count" per pattern.

Patterns: write_append, write_merge, read_full_scan, read_filter_window,
read_ann_search.
"""

from __future__ import annotations

import time

import lancedb
import numpy as np
import pyarrow as pa

import common as c

PREFIX = "timeseries"
DURATION = 20.0        # run each pattern for this long -> enough samples to plot
SAMPLE_INTERVAL = 0.1


def _stream(op, unit: str) -> dict:
    """Run ``op(i)`` in a tight loop for DURATION seconds, sampling container
    CPU/mem and marking the cumulative query count."""
    queries = 0
    with c.CgroupSampler(interval=SAMPLE_INTERVAL) as s:
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < DURATION:
            op(queries)
            queries += 1
            s.mark("op", queries=queries)
    return {"samples": s.samples, "events": s.events, "unit": unit, "total_queries": queries}


def _build_blob(db, name: str, n: int, payload: int):
    try:
        db.drop_table(name)
    except Exception:
        pass
    chunk = 50_000
    tbl = db.create_table(name, data=c.make_blob_rows(min(chunk, n), payload), schema=c.moveit_schema(), mode="overwrite")
    for s in range(chunk, n, chunk):
        tbl.add(c.make_blob_rows(min(chunk, n - s), payload, key_start=s))
    return tbl


def _vector_table(db, name: str, n: int, dim: int):
    try:
        db.drop_table(name)
    except Exception:
        pass
    rng = np.random.default_rng(1)
    chunk = 50_000

    def mk(start, count):
        v = rng.standard_normal((count, dim), dtype=np.float32)
        return pa.RecordBatch.from_arrays(
            [
                pa.array([f"v{start + i}" for i in range(count)], pa.string()),
                pa.array(list(v), pa.list_(pa.float32(), dim)),
                pa.array([f"c{(start + i) % 10}" for i in range(count)], pa.string()),
                pa.array(rng.random(count), pa.float64()),
            ],
            schema=c.vector_schema(dim),
        )

    tbl = db.create_table(name, data=mk(0, chunk), schema=c.vector_schema(dim), mode="overwrite")
    for s in range(chunk, n, chunk):
        tbl.add(mk(s, min(chunk, n - s)))
    tbl.create_index(metric="l2", vector_column_name="vector", num_partitions=128, num_sub_vectors=96, replace=True)
    return tbl, rng


def pattern_write_append(db) -> dict:
    name = "ts_append"
    try:
        db.drop_table(name)
    except Exception:
        pass
    rows = 5000
    tbl = db.create_table(name, data=c.make_blob_rows(rows, 1024, 0), schema=c.moveit_schema(), mode="overwrite")
    return _stream(lambda i: tbl.add(c.make_blob_rows(rows, 1024, key_start=(i + 1) * rows)),
                   "append (5k rows @1KB)")


def pattern_write_merge(db) -> dict:
    rows = 5000
    tbl = _build_blob(db, "ts_merge", 50_000, 1024)  # seed 50k keys to update

    def op(i):
        key = (i % 10) * rows  # cycle over existing key ranges -> updates
        (
            tbl.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(c.make_blob_rows(rows, 1024, key_start=key))
        )

    return _stream(op, "merge_insert (5k rows @1KB)")


def pattern_read_full_scan(db) -> dict:
    ds = _build_blob(db, "ts_scan", 300_000, 1024).to_lance()
    return _stream(lambda i: ds.to_table(columns=["id", "cursor", "data"]),
                   "full projected scan (300k rows)")


def pattern_read_filter_window(db) -> dict:
    ds = _build_blob(db, "ts_filter", 300_000, 1024).to_lance()
    return _stream(
        lambda i: ds.to_table(columns=["id", "cursor", "data"],
                              filter=f"cursor >= {(i * 971) % 295_000} AND cursor < {(i * 971) % 295_000 + 5000}"),
        "cursor-window scan (5k rows)")


def pattern_read_ann(db) -> dict:
    tbl, rng = _vector_table(db, "ts_ann", 200_000, 768)
    qs = rng.standard_normal((512, 768), dtype=np.float32)
    return _stream(lambda i: tbl.search(qs[i % len(qs)]).metric("l2").nprobes(20).limit(10).to_arrow(),
                   "ANN search (IVF_PQ, nprobes=20)")


def main() -> None:
    db = lancedb.connect(c.db_uri(PREFIX), storage_options=c.storage_options(safe=False))
    patterns = {}
    for name, fn in [
        ("write_append", pattern_write_append),
        ("write_merge", pattern_write_merge),
        ("read_full_scan", pattern_read_full_scan),
        ("read_filter_window", pattern_read_filter_window),
        ("read_ann_search", pattern_read_ann),
    ]:
        try:
            patterns[name] = fn(db)
            n = len(patterns[name]["samples"])
            print(f"[timeseries] {name}: {n} samples, {len(patterns[name]['events'])} ops", flush=True)
        except Exception as exc:  # noqa: BLE001
            patterns[name] = {"error": str(exc)[:200]}
            print(f"[timeseries] {name} FAILED: {exc}", flush=True)
    c.write_result("timeseries", {"cpu_limit_cores": c.cgroup_cpu_limit_cores(), "patterns": patterns})


if __name__ == "__main__":
    main()
