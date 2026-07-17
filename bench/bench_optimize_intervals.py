"""Row count vs on-disk storage, with optimize() at different cadences.

Grows a table one append (fragment) at a time, recording (rows, on-disk bytes,
fragments) after every append. At the chosen cadence it runs compaction +
cleanup, recording storage BEFORE compact, AFTER compact (old+new coexist =
the spike), and AFTER cleanup (reclaimed) — so plots.py can draw the storage
sawtooth. One series per cadence lets you compare compaction frequencies.
"""

from __future__ import annotations

from datetime import timedelta

import lance

import common as c

PREFIX = "optintervals"
ROWS_PER_APPEND = 5000
PAYLOAD = 1024
N_APPENDS = 100
CADENCES = [0, 20, 50]  # 0 = never optimize; others = compact+cleanup every N appends


def _footprint(name: str):
    fp = c.s3_footprint(PREFIX, name)
    return fp.total_bytes, fp.fragment_count


def run_cadence(cadence: int) -> dict:
    name = f"cad_{cadence}"
    uri = c.dataset_uri(PREFIX, name, safe=False)
    opts = c.storage_options(False)
    series = []  # {rows, bytes, fragments, phase}
    rows = 0

    lance.write_dataset(c.make_blob_rows(ROWS_PER_APPEND, PAYLOAD, 0), uri, mode="overwrite", storage_options=opts)
    rows += ROWS_PER_APPEND
    b, f = _footprint(name)
    series.append({"rows": rows, "bytes": b, "fragments": f, "phase": "append"})

    for i in range(1, N_APPENDS):
        lance.write_dataset(c.make_blob_rows(ROWS_PER_APPEND, PAYLOAD, i * ROWS_PER_APPEND),
                            uri, mode="append", storage_options=opts)
        rows += ROWS_PER_APPEND
        b, f = _footprint(name)
        series.append({"rows": rows, "bytes": b, "fragments": f, "phase": "append"})

        if cadence and i % cadence == 0:
            ds = lance.dataset(uri, storage_options=opts)
            ds.optimize.compact_files(target_rows_per_fragment=50 * ROWS_PER_APPEND)
            b, f = _footprint(name)
            series.append({"rows": rows, "bytes": b, "fragments": f, "phase": "compact_peak"})
            ds = lance.dataset(uri, storage_options=opts)
            ds.cleanup_old_versions(older_than=timedelta(microseconds=1))
            b, f = _footprint(name)
            series.append({"rows": rows, "bytes": b, "fragments": f, "phase": "post_cleanup"})

    final_b, final_f = _footprint(name)
    print(f"[optintervals] cadence={cadence}: {rows} rows -> {final_b/1e6:.1f}MB, {final_f} fragments", flush=True)
    return {"cadence": cadence, "series": series, "final_bytes": final_b, "final_fragments": final_f,
            "logical_bytes": rows * PAYLOAD}


def main() -> None:
    results = [run_cadence(cad) for cad in CADENCES]
    c.write_result("optintervals", {"rows_per_append": ROWS_PER_APPEND, "payload_bytes": PAYLOAD,
                                     "n_appends": N_APPENDS, "results": results})


if __name__ == "__main__":
    main()
