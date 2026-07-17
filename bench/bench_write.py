"""Write-throughput ceiling under the pod cap.

Two workloads:
  * blob  — MOVEIT schema, `data` payload swept 1KB/10KB/100KB, append vs
            merge_insert (upsert on id, as prod does).
  * vector — 768-dim float32, append then create_index (IVF_PQ).

Each config reports rows/s, MB/s, peak RSS, CPU-seconds. Batch row-count is
scaled so each batch is ~128MB, keeping MinIO round-trips comparable.
"""

from __future__ import annotations

import lancedb
import numpy as np
import pyarrow as pa

import common as c

PREFIX = "write"
TARGET_BATCH_BYTES = 128 * 1024 * 1024


def bench_blob() -> list[dict]:
    db = lancedb.connect(c.db_uri(PREFIX), storage_options=c.storage_options(safe=False))
    out = []
    for payload in (1024, 10 * 1024, 100 * 1024):
        rows = max(1000, TARGET_BATCH_BYTES // payload)
        name = f"blob_{payload}"
        try:
            db.drop_table(name)
        except Exception:
            pass

        # --- append: 3 batches, measured in aggregate ---
        first = c.make_blob_rows(rows, payload, key_start=0)
        tbl = db.create_table(name, data=first, schema=c.moveit_schema(), mode="overwrite")
        appended = rows
        with c.Meter() as m:
            for b in range(1, 3):
                tbl.add(c.make_blob_rows(rows, payload, key_start=b * rows))
                appended += rows
        mb = appended * payload / 1e6
        out.append(
            {
                "workload": "blob",
                "op": "append",
                "payload_bytes": payload,
                "rows_per_batch": rows,
                "rows": appended - rows,  # rows added inside the meter
                "rows_per_s": round((appended - rows) / m.metrics.wall_s, 1),
                "mb_per_s": round((mb * (appended - rows) / appended) / m.metrics.wall_s, 1),
                **c.metrics_dict(m.metrics),
            }
        )

        # --- merge_insert: upsert the first batch's keys (all matched) ---
        with c.Meter() as m:
            (
                tbl.merge_insert("id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(c.make_blob_rows(rows, payload, key_start=0))
            )
        out.append(
            {
                "workload": "blob",
                "op": "merge_insert",
                "payload_bytes": payload,
                "rows_per_batch": rows,
                "rows": rows,
                "rows_per_s": round(rows / m.metrics.wall_s, 1),
                "mb_per_s": round((rows * payload / 1e6) / m.metrics.wall_s, 1),
                **c.metrics_dict(m.metrics),
            }
        )
        print(f"[write] blob payload={payload} done", flush=True)
    return out


def bench_vector(n: int = 200_000, dim: int = 768) -> list[dict]:
    db = lancedb.connect(c.db_uri(PREFIX), storage_options=c.storage_options(safe=False))
    name = "vector"
    try:
        db.drop_table(name)
    except Exception:
        pass

    rng = np.random.default_rng(42)

    def batch(start: int, count: int) -> pa.RecordBatch:
        vecs = rng.standard_normal((count, dim), dtype=np.float32)
        return pa.RecordBatch.from_arrays(
            [
                pa.array([f"v{start + i}" for i in range(count)], pa.string()),
                pa.array(list(vecs), pa.list_(pa.float32(), dim)),
                pa.array([f"cat{(start + i) % 10}" for i in range(count)], pa.string()),
                pa.array(rng.random(count), pa.float64()),
            ],
            schema=c.vector_schema(dim),
        )

    chunk = 50_000
    tbl = db.create_table(name, data=batch(0, chunk), schema=c.vector_schema(dim), mode="overwrite")
    out = []
    with c.Meter() as m:
        for start in range(chunk, n, chunk):
            tbl.add(batch(start, min(chunk, n - start)))
    added = n - chunk
    out.append(
        {
            "workload": "vector",
            "op": "append",
            "dim": dim,
            "rows": added,
            "rows_per_s": round(added / m.metrics.wall_s, 1),
            "mb_per_s": round((added * dim * 4 / 1e6) / m.metrics.wall_s, 1),
            **c.metrics_dict(m.metrics),
        }
    )

    # --- index build (IVF_PQ) — where the memory cap bites ---
    with c.Meter() as m:
        tbl.create_index(
            metric="l2",
            vector_column_name="vector",
            num_partitions=256,
            num_sub_vectors=96,
            replace=True,
        )
    out.append(
        {
            "workload": "vector",
            "op": "create_index_ivf_pq",
            "dim": dim,
            "rows": n,
            "index": "IVF256_PQ96",
            **c.metrics_dict(m.metrics),
        }
    )
    print("[write] vector done", flush=True)
    return out


def main() -> None:
    results = bench_blob()
    try:
        results += bench_vector()
    except Exception as exc:  # OOM / index failure under the cap is itself a finding
        results.append({"workload": "vector", "op": "FAILED", "error": str(exc)})
        print(f"[write] vector FAILED: {exc}", flush=True)
    c.write_result("write", {"results": results})


if __name__ == "__main__":
    main()
