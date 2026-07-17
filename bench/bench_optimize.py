"""optimize() (compaction) overhead vs fragment count.

Simulates MOVEIT's many-small-writes pattern (one merge per sync cycle => many
fragments), then compacts and measures the cost on all three axes the user
asked about: CPU-seconds, peak RSS, and S3 bytes (rewrite amplification +
what cleanup reclaims). Repeated at growing fragment counts for a cost curve.

Uses the low-level `lance` optimize API for granular compact vs cleanup stats.
"""

from __future__ import annotations

from datetime import timedelta

import lance
import pyarrow as pa

import common as c

PREFIX = "optimize"
ROWS_PER_FRAG = 1000
PAYLOAD = 1024  # ~1KB per row


def _frag_batch(start: int) -> pa.RecordBatch:
    return c.make_blob_rows(ROWS_PER_FRAG, PAYLOAD, key_start=start)


def build_fragments(name: str, n_frags: int) -> str:
    uri = c.dataset_uri(PREFIX, name, safe=False)
    opts = c.storage_options(False)
    lance.write_dataset(_frag_batch(0), uri, mode="overwrite", storage_options=opts)
    for f in range(1, n_frags):
        lance.write_dataset(_frag_batch(f * ROWS_PER_FRAG), uri, mode="append", storage_options=opts)
    return uri


def run_point(n_frags: int) -> dict:
    name = f"frags_{n_frags}"
    uri = build_fragments(name, n_frags)
    opts = c.storage_options(False)

    before = c.s3_footprint(PREFIX, name)

    # --- compaction ---
    ds = lance.dataset(uri, storage_options=opts)
    with c.Meter() as m:
        metrics = ds.optimize.compact_files(target_rows_per_fragment=100 * ROWS_PER_FRAG)
    after_compact = c.s3_footprint(PREFIX, name)

    # --- cleanup old versions (reclaim the pre-compaction files) ---
    ds = lance.dataset(uri, storage_options=opts)
    cleanup = ds.cleanup_old_versions(older_than=timedelta(microseconds=1))
    after_cleanup = c.s3_footprint(PREFIX, name)

    return {
        "input_fragments": n_frags,
        "rows": n_frags * ROWS_PER_FRAG,
        "logical_bytes": n_frags * ROWS_PER_FRAG * PAYLOAD,
        "fragments_before": before.fragment_count,
        "fragments_after": after_compact.fragment_count,
        "fragments_removed": getattr(metrics, "fragments_removed", None),
        "fragments_added": getattr(metrics, "fragments_added", None),
        "files_removed": getattr(metrics, "files_removed", None),
        "files_added": getattr(metrics, "files_added", None),
        # S3 "disk" axis
        "bytes_before": before.total_bytes,
        "bytes_after_compact": after_compact.total_bytes,  # old+new coexist pre-cleanup
        "bytes_after_cleanup": after_cleanup.total_bytes,
        "rewrite_amplification": round(after_compact.total_bytes / max(1, before.total_bytes), 2),
        "bytes_reclaimed_by_cleanup": after_compact.total_bytes - after_cleanup.total_bytes,
        # cpu/mem axis
        "compact_wall_s": round(m.metrics.wall_s, 3),
        "compact_cpu_s": round(m.metrics.cpu_s, 3),
        "compact_peak_rss_mb": m.metrics.peak_rss_mb,
        "cpu_s_per_1k_frags": round(m.metrics.cpu_s / (n_frags / 1000), 3),
    }


def main() -> None:
    results = []
    for n in (100, 300, 500):
        try:
            r = run_point(n)
        except Exception as exc:  # noqa: BLE001
            r = {"input_fragments": n, "op": "FAILED", "error": str(exc)[:300]}
        results.append(r)
        print(f"[optimize] frags={n} -> wall={r.get('compact_wall_s')}s "
              f"rss={r.get('compact_peak_rss_mb')}MB amp={r.get('rewrite_amplification')}", flush=True)
    c.write_result("optimize", {"results": results})


if __name__ == "__main__":
    main()
