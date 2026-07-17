"""Multi-instance writers against the SAME S3 table.

Answers: can we spawn multiple LanceDB instances on one S3 dataset, run
transactional merges on same/different rows without data loss, and what does
that do to latency?

Two commit modes:
  * UNSAFE — plain ``s3://`` on MinIO (MOVEIT prod semantics; relies on the
             object store's own atomic-put support).
  * SAFE   — ``s3+ddb://...?ddbTableName=`` external commit store on
             DynamoDB-local (the transferable, provably-serialised path).

Two contention shapes per mode/fan-out:
  * disjoint  — each writer owns a distinct key range. A correct system keeps
                every key; lost keys => clobbered commits.
  * overlap   — all writers merge the SAME keys (last-writer-wins). Row count
                must stay == key count; distinct value proves a merge landed.

Uses the low-level ``lance`` package so the commit-store URI is exact. Writers
run as separate processes (real multi-instance, not threads).
"""

from __future__ import annotations

import multiprocessing as mp
import time
from concurrent.futures import ProcessPoolExecutor

import lance
import pyarrow as pa

import common as c

PREFIX = "concurrency"
RETRIES = 8


def _batch(ids: list[int], writer_id: int, op: int) -> pa.RecordBatch:
    n = len(ids)
    now_us = int(time.time() * 1_000_000)
    tag = f'{{"w":{writer_id},"op":{op}}}'
    return pa.RecordBatch.from_arrays(
        [
            pa.array([f"k{i}" for i in ids], pa.string()),
            pa.array(["bench"] * n, pa.string()),
            pa.array(["messages"] * n, pa.string()),
            pa.array(["upsert"] * n, pa.string()),
            pa.array([f"w{writer_id}o{op}"] * n, pa.string()),
            pa.array([now_us] * n, pa.timestamp("us")),
            pa.array(ids, pa.int64()),
            pa.array([tag] * n, pa.string()),
        ],
        schema=c.moveit_schema(),
    )


def _writer(task: dict) -> dict:
    """One instance: perform ``n_ops`` merge_insert commits over its key set."""
    uri = task["uri"]
    opts = c.storage_options(task["safe"])
    ids = task["ids"]
    wid = task["writer_id"]
    conflicts = 0
    commits = 0
    latencies = []
    err = None
    for op in range(task["n_ops"]):
        batch = _batch(ids, wid, op)
        for attempt in range(RETRIES):
            try:
                t0 = time.perf_counter()
                ds = lance.dataset(uri, storage_options=opts)
                (
                    ds.merge_insert("id")
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute(batch)
                )
                latencies.append(time.perf_counter() - t0)
                commits += 1
                break
            except Exception as exc:  # noqa: BLE001 — classify by message
                msg = str(exc).lower()
                if any(k in msg for k in ("conflict", "commit", "concurrent", "version")):
                    conflicts += 1
                    time.sleep(0.05 * (attempt + 1))
                    continue
                err = str(exc)
                break
        else:
            err = err or "exhausted retries"
    return {"writer_id": wid, "commits": commits, "conflicts": conflicts, "latencies": latencies, "error": err}


def _seed(uri: str, safe: bool) -> None:
    opts = c.storage_options(safe)
    seed = _batch([-1], writer_id=-1, op=-1)
    lance.write_dataset(seed, uri, mode="overwrite", storage_options=opts)


def _distinct_ids(uri: str, safe: bool) -> tuple[int, int]:
    ds = lance.dataset(uri, storage_options=c.storage_options(safe))
    t = ds.to_table(columns=["id"])
    ids = t.column("id").to_pylist()
    return len(ids), len(set(ids))


def run_case(mode_safe: bool, n_writers: int, shape: str, keys_per_writer: int = 4000, n_ops: int = 3) -> dict:
    mode = "safe_ddb" if mode_safe else "unsafe_s3"
    name = f"{mode}_{shape}_{n_writers}w"
    uri = c.dataset_uri(PREFIX, name, safe=mode_safe)
    _seed(uri, mode_safe)

    if shape == "disjoint":
        tasks = [
            {"uri": uri, "safe": mode_safe, "writer_id": w, "n_ops": n_ops,
             "ids": list(range(w * keys_per_writer, (w + 1) * keys_per_writer))}
            for w in range(n_writers)
        ]
        expected_distinct = n_writers * keys_per_writer
    else:  # overlap — every writer hammers the same key set
        shared = list(range(keys_per_writer))
        tasks = [
            {"uri": uri, "safe": mode_safe, "writer_id": w, "n_ops": n_ops, "ids": shared}
            for w in range(n_writers)
        ]
        expected_distinct = keys_per_writer

    # spawn (not fork): lance starts a Tokio runtime at import; forking a live
    # native runtime can deadlock. spawn re-imports cleanly in each child.
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=n_writers, mp_context=mp.get_context("spawn")) as ex:
        results = list(ex.map(_writer, tasks))
    wall = time.perf_counter() - t0

    total_rows, distinct = _distinct_ids(uri, mode_safe)
    # seed row (id "k-1") is present in both expectations; account for it.
    distinct_data = distinct - 1
    lost = max(0, expected_distinct - distinct_data)

    all_lat = [x for r in results for x in r["latencies"]]
    all_lat.sort()

    def pct(p):
        if not all_lat:
            return 0.0
        i = min(len(all_lat) - 1, int(len(all_lat) * p / 100))
        return round(all_lat[i] * 1000, 2)

    return {
        "mode": mode,
        "shape": shape,
        "writers": n_writers,
        "n_ops": n_ops,
        "keys_per_writer": keys_per_writer,
        "expected_distinct_keys": expected_distinct,
        "actual_distinct_keys": distinct_data,
        "lost_keys": lost,
        "data_loss": lost > 0,
        "total_rows_incl_dupes": total_rows,
        "total_commits": sum(r["commits"] for r in results),
        "total_conflicts": sum(r["conflicts"] for r in results),
        "writer_errors": [r["error"] for r in results if r["error"]],
        "wall_s": round(wall, 3),
        "commit_p50_ms": pct(50),
        "commit_p95_ms": pct(95),
        "commit_p99_ms": pct(99),
    }


def main() -> None:
    c.ensure_ddb_commit_table()
    results = []
    for safe in (True, False):
        for shape in ("disjoint", "overlap"):
            for nw in (2, 4):
                try:
                    r = run_case(safe, nw, shape)
                except Exception as exc:  # noqa: BLE001
                    r = {"mode": "safe_ddb" if safe else "unsafe_s3", "shape": shape,
                         "writers": nw, "op": "FAILED", "error": str(exc)}
                results.append(r)
                print(f"[concurrency] {r.get('mode')} {shape} {nw}w -> "
                      f"lost={r.get('lost_keys')} conflicts={r.get('total_conflicts')}", flush=True)
    c.write_result("concurrency", {"results": results})


if __name__ == "__main__":
    main()
