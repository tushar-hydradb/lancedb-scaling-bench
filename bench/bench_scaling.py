"""Vertical-scaling ceiling: add instances WHILE queries run.

Starts one worker process hammering the dataset, then adds another every
RAMP_INTERVAL seconds up to MAX_INSTANCES — all under the fixed pod CPU cap.
A background sampler records container CPU (cores) + RSS (MB) and the running
aggregate op count, marking each instance-add. plots.py turns this into
"throughput & CPU/mem vs time, with instance-add markers": you see throughput
climb until the CPU cap saturates, then plateau (adding instances past the core
count stops helping — the vertical ceiling).

Two profiles: read (cursor-window scans) and write (appends).
"""

from __future__ import annotations

import multiprocessing as mp
import time

import lance

import common as c

PREFIX = "scaling"
MAX_INSTANCES = 6
RAMP_INTERVAL = 6.0   # seconds between adding instances
TAIL = 6.0            # keep running after the last instance is added
BASE_ROWS = 300_000
PAYLOAD = 1024


def _read_worker(uri: str, opts: dict, counter, stop) -> None:
    ds = lance.dataset(uri, storage_options=opts)
    i = 0
    while not stop.is_set():
        lo = (i * 971) % 295_000
        try:
            ds.to_table(columns=["id", "cursor", "data"], filter=f"cursor >= {lo} AND cursor < {lo + 5000}")
            with counter.get_lock():
                counter.value += 1
        except Exception:  # noqa: BLE001
            pass
        i += 1


def _write_worker(uri: str, opts: dict, counter, stop, worker_id: int = 0) -> None:
    base = worker_id * 100_000_000
    i = 0
    while not stop.is_set():
        try:
            lance.write_dataset(c.make_blob_rows(2000, PAYLOAD, key_start=base + i * 2000),
                                uri, mode="append", storage_options=opts)
            with counter.get_lock():
                counter.value += 1
        except Exception:  # noqa: BLE001
            pass
        i += 1


def run_profile(profile: str) -> dict:
    ctx = mp.get_context("spawn")
    opts = c.storage_options(False)
    uri = c.dataset_uri(PREFIX, f"base_{profile}", safe=False)

    # Seed the dataset (readers need data; writers need an initial table).
    lance.write_dataset(c.make_blob_rows(50_000 if profile == "write" else BASE_ROWS, PAYLOAD, 0),
                        uri, mode="overwrite", storage_options=opts)
    if profile == "read":
        for s in range(50_000, BASE_ROWS, 50_000):
            lance.write_dataset(c.make_blob_rows(50_000, PAYLOAD, s), uri, mode="append", storage_options=opts)

    counters = [ctx.Value("L", 0) for _ in range(MAX_INSTANCES)]
    stop = ctx.Event()
    procs: list = []
    state = {"active": 0}

    def extra():
        active = state["active"]
        return {"active_instances": active, "agg_ops": sum(counters[i].value for i in range(active))}

    target = _read_worker if profile == "read" else _write_worker

    def spawn_one(idx: int):
        args = (uri, opts, counters[idx], stop) + ((idx,) if profile == "write" else ())
        p = ctx.Process(target=target, args=args)
        p.start()
        procs.append(p)
        state["active"] = idx + 1

    with c.CgroupSampler(interval=0.25, extra_fn=extra) as smp:
        spawn_one(0)
        smp.mark("instance_added", active=1)
        last_add = time.perf_counter()
        added = 1
        deadline = last_add + (MAX_INSTANCES - 1) * RAMP_INTERVAL + TAIL
        while time.perf_counter() < deadline:
            time.sleep(0.2)
            now = time.perf_counter()
            if added < MAX_INSTANCES and now - last_add >= RAMP_INTERVAL:
                spawn_one(added)
                smp.mark("instance_added", active=added + 1)
                added += 1
                last_add = now
        stop.set()
        for p in procs:
            p.join(timeout=10)

    total_ops = sum(cval.value for cval in counters)
    print(f"[scaling] {profile}: {total_ops} ops across ramp to {MAX_INSTANCES} instances, "
          f"{len(smp.samples)} samples", flush=True)
    return {"profile": profile, "samples": smp.samples, "events": smp.events,
            "max_instances": MAX_INSTANCES, "ramp_interval_s": RAMP_INTERVAL, "total_ops": total_ops,
            "per_worker_ops": [cval.value for cval in counters]}


def main() -> None:
    results = {}
    for profile in ("read", "write"):
        try:
            results[profile] = run_profile(profile)
        except Exception as exc:  # noqa: BLE001
            results[profile] = {"error": str(exc)[:200]}
            print(f"[scaling] {profile} FAILED: {exc}", flush=True)
    c.write_result("scaling", {"cpu_limit_cores": c.cgroup_cpu_limit_cores(), "profiles": results})


if __name__ == "__main__":
    main()
