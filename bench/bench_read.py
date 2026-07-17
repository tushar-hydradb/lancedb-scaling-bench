"""Read-throughput / latency ceiling under the pod cap.

Blob   — full projected scan, filtered scan (MOVEIT drain pattern), and keyset
         pages on `cursor`; reports scan MB/s and p50/p95/p99 latency.
Vector — ANN search QPS + latency percentiles vs nprobes, with and without an
         IVF_PQ index (flat brute force is the `no index` baseline).

Self-contained: builds its own tables so it doesn't depend on bench_write.
"""

from __future__ import annotations

import lancedb
import numpy as np
import pyarrow as pa

import common as c

PREFIX = "read"


def _pct(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    return round(float(np.percentile(xs, p)) * 1000, 2)  # -> ms


def build_blob(db, n: int, payload: int) -> lancedb.table.Table:
    name = "blob_read"
    try:
        db.drop_table(name)
    except Exception:
        pass
    chunk = 50_000
    tbl = db.create_table(name, data=c.make_blob_rows(min(chunk, n), payload), schema=c.moveit_schema(), mode="overwrite")
    for s in range(chunk, n, chunk):
        tbl.add(c.make_blob_rows(min(chunk, n - s), payload, key_start=s))
    return tbl


def bench_blob_reads(db, n: int = 300_000, payload: int = 1024) -> list[dict]:
    tbl = build_blob(db, n, payload)
    ds = tbl.to_lance()
    out = []

    # --- full projected scan (id, cursor, data) ---
    with c.Meter() as m:
        t = ds.to_table(columns=["id", "cursor", "data"])
    rows = t.num_rows
    out.append(
        {
            "workload": "blob",
            "op": "full_scan_projected",
            "rows": rows,
            "rows_per_s": round(rows / m.metrics.wall_s, 1),
            "mb_per_s": round((rows * payload / 1e6) / m.metrics.wall_s, 1),
            **c.metrics_dict(m.metrics),
        }
    )

    # --- filtered scan: numeric cursor window (the drain keyset pattern) ---
    # NB: we deliberately do NOT filter on object_type/connector_id here — see
    # the low-cardinality string-filter probe below (it panics in lance 0.33).
    lat = []
    for i in range(20):
        lo = i * (n // 40)
        with c.Meter() as m:
            _ = ds.to_table(
                columns=["id", "cursor", "data"],
                filter=f"cursor >= {lo} AND cursor < {lo + 5000}",
            )
        lat.append(m.metrics.wall_s)
    out.append(
        {
            "workload": "blob",
            "op": "filtered_scan_5k_window",
            "iters": len(lat),
            "p50_ms": _pct(lat, 50),
            "p95_ms": _pct(lat, 95),
            "p99_ms": _pct(lat, 99),
        }
    )

    # --- FINDING probe: filter on a low-cardinality string column ---
    # MOVEIT's drain filters by (connector_id, object_type). In lance 0.33 that
    # panics in lance-encoding (buffer.rs slice) when the column is constant /
    # low-cardinality. Record whether it works so the report flags it.
    probe = {"workload": "blob", "op": "lowcard_string_filter_probe",
             "filter": "object_type = 'messages'"}
    try:
        rows_hit = ds.to_table(columns=["id"], filter="object_type = 'messages'").num_rows
        probe["result"] = "ok"
        probe["rows"] = rows_hit
    except Exception as exc:  # noqa: BLE001
        probe["result"] = "PANIC/abort"
        probe["error"] = str(exc)[:200]
    out.append(probe)

    # --- keyset paging on cursor (page size 10k) ---
    page = 10_000
    last = -1
    pages = 0
    with c.Meter() as m:
        while True:
            t = ds.to_table(columns=["id", "cursor"], filter=f"cursor > {last}", limit=page)
            if t.num_rows == 0:
                break
            last = pa.compute.max(t.column("cursor")).as_py()
            pages += 1
            if pages > 100:
                break
    out.append(
        {
            "workload": "blob",
            "op": "keyset_paging",
            "page_size": page,
            "pages": pages,
            "pages_per_s": round(pages / m.metrics.wall_s, 2),
            **c.metrics_dict(m.metrics),
        }
    )
    print("[read] blob done", flush=True)
    return out


def bench_vector_search(db, n: int = 200_000, dim: int = 768, queries: int = 200) -> list[dict]:
    name = "vector_read"
    try:
        db.drop_table(name)
    except Exception:
        pass
    rng = np.random.default_rng(7)
    chunk = 50_000
    out = []

    def mk(start, count):
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

    tbl = db.create_table(name, data=mk(0, chunk), schema=c.vector_schema(dim), mode="overwrite")
    for s in range(chunk, n, chunk):
        tbl.add(mk(s, min(chunk, n - s)))

    qs = rng.standard_normal((queries, dim), dtype=np.float32)

    def run_search(use_nprobes: int | None) -> list[float]:
        lat = []
        for q in qs:
            with c.Meter(sample_hz=1) as m:
                sb = tbl.search(q).metric("l2").limit(10)
                if use_nprobes is not None:
                    sb = sb.nprobes(use_nprobes)
                _ = sb.to_arrow()
            lat.append(m.metrics.wall_s)
        return lat

    # --- no index: brute-force flat search baseline ---
    lat = run_search(None)
    out.append(
        {
            "workload": "vector",
            "op": "ann_search",
            "index": "none_flat",
            "queries": queries,
            "qps": round(queries / sum(lat), 1),
            "p50_ms": _pct(lat, 50),
            "p95_ms": _pct(lat, 95),
            "p99_ms": _pct(lat, 99),
        }
    )

    # --- with IVF_PQ index, sweep nprobes ---
    tbl.create_index(metric="l2", vector_column_name="vector", num_partitions=256, num_sub_vectors=96, replace=True)
    for nprobes in (10, 50):
        lat = run_search(nprobes)
        out.append(
            {
                "workload": "vector",
                "op": "ann_search",
                "index": "IVF256_PQ96",
                "nprobes": nprobes,
                "queries": queries,
                "qps": round(queries / sum(lat), 1),
                "p50_ms": _pct(lat, 50),
                "p95_ms": _pct(lat, 95),
                "p99_ms": _pct(lat, 99),
            }
        )
    print("[read] vector done", flush=True)
    return out


def main() -> None:
    db = lancedb.connect(c.db_uri(PREFIX), storage_options=c.storage_options(safe=False))
    results = bench_blob_reads(db)
    try:
        results += bench_vector_search(db)
    except Exception as exc:
        results.append({"workload": "vector", "op": "FAILED", "error": str(exc)})
        print(f"[read] vector FAILED: {exc}", flush=True)
    c.write_result("read", {"results": results})


if __name__ == "__main__":
    main()
