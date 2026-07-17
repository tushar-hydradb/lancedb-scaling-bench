"""The 2GB variable-width-column cap, and how to lift it.

LanceDB/Arrow use 32-bit offsets for `string`/`binary` columns, so a single
write array caps at ~2GiB (int32 max = 2,147,483,647 bytes). `large_string` /
`large_binary` use 64-bit offsets and remove it. This is exactly MOVEIT's `data`
column question ("large objects into a column, capped at 2GB — correct?").

Four probes (each heavy one isolated in its own process so an OOM-kill under the
4GB cap doesn't lose the whole run):
  1. construct a >2GB `binary` array        -> expect int32 offset failure
  2. construct a >2GB `large_binary` array   -> expect success (cap lifted)
  3. write that >2GB column to Lance in ONE batch -> may OOM under 4GB (finding)
  4. grow a column PAST 2GB via many <2GB append batches (plain binary)
     -> expect success (the practical path: cap is per-batch, not per-dataset)

Memory trick: `[chunk] * n` holds one buffer + n references (~1MB), while Arrow
still materialises the full >2GB values buffer — so the Python side stays light
and only the Arrow build / Lance write approaches the cap.
"""

from __future__ import annotations

import multiprocessing as mp

import lance
import numpy as np
import pyarrow as pa

import common as c

PREFIX = "cap2gb"
ROW_BYTES = 1_000_000  # 1 MB per row
OVER_2GB_ROWS = 2200  # 2.2 GB total, just over the int32 ceiling
INT32_MAX = 2_147_483_647


# --- exact-sized array build (no builder doubling, so it fits under the cap) --
def _build_large_binary(total: int, row_bytes: int) -> "pa.Array":
    """A >2GB `large_binary` array via from_buffers: one exact values buffer +
    int64 offsets. Avoids Arrow's builder capacity-doubling (which would need
    ~2x memory and OOM under the 4GB cap)."""
    values = b"\x00" * total  # single allocation, no doubling
    vbuf = pa.py_buffer(values)
    n = total // row_bytes
    offsets = np.arange(0, (n + 1) * row_bytes, row_bytes, dtype=np.int64)[: n + 1]
    obuf = pa.py_buffer(offsets.tobytes())
    return pa.Array.from_buffers(pa.large_binary(), n, [None, obuf, vbuf])


# --- isolated workers (module-level so spawn can import + run them) ----------
def _probe_construct(_unused: str, q: "mp.Queue") -> None:
    """Build a >2GB large_binary array (cap lifted), then prove the int32 cap by
    casting it to plain `binary` — that fails on offset overflow without copying
    the 2GB payload, so it's cheap."""
    total = ROW_BYTES * OVER_2GB_ROWS
    out = {"total_bytes": total, "over_int32_by_bytes": total - INT32_MAX}
    try:
        arr = _build_large_binary(total, ROW_BYTES)
        out["large_binary_constructed"] = True
        out["array_len"] = len(arr)
        try:
            arr.cast(pa.binary())  # int32 offsets can't address >2GB
            out["binary_cast_ok"] = True  # unexpected
            out["cap_confirmed"] = False
        except Exception as exc:  # noqa: BLE001 — this IS the cap
            out["binary_cast_ok"] = False
            out["cap_confirmed"] = True
            out["cap_error"] = str(exc)[:300]
    except Exception as exc:  # noqa: BLE001
        out["large_binary_constructed"] = False
        out["error"] = str(exc)[:300]
    q.put(out)


def _probe_write_one_batch(_unused: str, q: "mp.Queue") -> None:
    """Write a >2GB large_binary column to Lance in a single batch (may OOM
    under the cap — that's the finding: chunk your writes)."""
    total = ROW_BYTES * OVER_2GB_ROWS
    try:
        data = _build_large_binary(total, ROW_BYTES)
        ids = pa.array([f"big{i}" for i in range(len(data))], pa.string())
        tbl = pa.table({"id": ids, "data": data})
        uri = c.dataset_uri(PREFIX, "one_big_batch", safe=False)
        lance.write_dataset(tbl, uri, mode="overwrite", storage_options=c.storage_options(False))
        ds = lance.dataset(uri, storage_options=c.storage_options(False))
        q.put({"total_bytes": total, "written": True, "rows": ds.count_rows(), "error": None})
    except Exception as exc:  # noqa: BLE001
        q.put({"total_bytes": total, "written": False, "error": str(exc)[:300]})


def _run_isolated(target, *args) -> dict:
    """Run a worker in its own process; classify SIGKILL (-9) as an OOM-kill.

    Uses spawn (not fork): lance starts a Tokio runtime at import, and forking a
    live native runtime can deadlock."""
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    p = ctx.Process(target=target, args=(*args, q))
    p.start()
    p.join()
    if p.exitcode == 0 and not q.empty():
        return q.get()
    if p.exitcode == -9:
        return {"error": "OOM-killed (SIGKILL) under the memory cap", "oom": True, "exitcode": -9}
    return {"error": f"worker died exitcode={p.exitcode}", "exitcode": p.exitcode}


def probe_append_growth(target_bytes: int = 2_400_000_000, batch_bytes: int = 100_000_000) -> dict:
    """Grow a plain-`binary` column past 2GB via many sub-2GB append batches."""
    rows_per_batch = batch_bytes // ROW_BYTES
    chunk = b"x" * ROW_BYTES
    uri = c.dataset_uri(PREFIX, "append_growth", safe=False)
    opts = c.storage_options(False)

    def mk(start: int) -> pa.Table:
        return pa.table(
            {
                "id": pa.array([f"g{start + i}" for i in range(rows_per_batch)], pa.string()),
                "data": pa.array([chunk] * rows_per_batch, type=pa.binary()),
            }
        )

    written = 0
    n_batches = 0
    err = None
    try:
        lance.write_dataset(mk(0), uri, mode="overwrite", storage_options=opts)
        written += rows_per_batch * ROW_BYTES
        n_batches += 1
        while written < target_bytes:
            lance.write_dataset(mk(n_batches * rows_per_batch), uri, mode="append", storage_options=opts)
            written += rows_per_batch * ROW_BYTES
            n_batches += 1
    except Exception as exc:  # noqa: BLE001
        err = str(exc)[:300]

    fp = c.s3_footprint(PREFIX, "append_growth")
    return {
        "per_batch_bytes": batch_bytes,
        "batches": n_batches,
        "logical_column_bytes": written,
        "exceeded_2gb": written > INT32_MAX,
        "on_disk_bytes": fp.total_bytes,
        "fragments": fp.fragment_count,
        "error": err,
    }


def main() -> None:
    results = {
        "int32_max_bytes": INT32_MAX,
        "construct_and_cap": _run_isolated(_probe_construct, "x"),
        "write_one_2gb_batch": _run_isolated(_probe_write_one_batch, "x"),
        "append_growth_past_2gb": probe_append_growth(),
    }
    for k in ("construct_and_cap", "write_one_2gb_batch"):
        print(f"[cap2gb] {k}: {results[k]}", flush=True)
    print(f"[cap2gb] append_growth: {results['append_growth_past_2gb']}", flush=True)
    c.write_result("cap2gb", results)


if __name__ == "__main__":
    main()
