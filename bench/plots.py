"""Render PNG graphs from the time-series result JSON.

Produces (into /results/graphs/):
  * ts_<pattern>.png      — CPU (cores) & RSS (MB) vs cumulative query count.
  * optimize_storage.png  — row count vs on-disk storage, one line per optimize
                            cadence, with the compaction sawtooth.
  * scaling_<profile>.png — throughput & CPU/mem vs time, instance-add markers.
"""

from __future__ import annotations

import json
import os

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = os.environ.get("BENCH_RESULTS_DIR", "/results")
GRAPHS = os.path.join(RESULTS, "graphs")


def _load(name: str):
    p = os.path.join(RESULTS, f"{name}.json")
    if not os.path.exists(p):
        return None
    with open(p) as fh:
        return json.load(fh)


def _interp_queries(samples: list[dict], events: list[dict]) -> list[float]:
    """Map each sample's timestamp to a cumulative query count via the op marks."""
    ev_t = [e["t"] for e in events]
    ev_q = [e.get("queries", 0) for e in events]
    out = []
    j = 0
    for s in samples:
        t = s["t"]
        while j + 1 < len(ev_t) and ev_t[j + 1] <= t:
            j += 1
        out.append(ev_q[j] if ev_q else 0)
    return out


# --- 1. CPU/mem vs query count, per pattern ---------------------------------
def plot_timeseries(data) -> None:
    cap = data.get("cpu_limit_cores")
    patterns = data.get("patterns", {})
    for name, pd in patterns.items():
        samples = pd.get("samples")
        if not samples:
            continue
        q = _interp_queries(samples, pd.get("events", []))
        cpu = [s["cpu_cores"] for s in samples]
        rss = [s["rss_mb"] for s in samples]

        fig, ax1 = plt.subplots(figsize=(9, 4.5))
        ax1.plot(q, cpu, color="tab:red", lw=1.4, label="CPU (cores)")
        ax1.set_xlabel("cumulative queries")
        ax1.set_ylabel("CPU (cores)", color="tab:red")
        ax1.tick_params(axis="y", labelcolor="tab:red")
        if cap:
            ax1.axhline(cap, ls="--", color="tab:red", alpha=0.4)
            ax1.text(0.01, cap, f" CPU cap = {cap} cores", color="tab:red", va="bottom", fontsize=8, transform=ax1.get_yaxis_transform())
        ax2 = ax1.twinx()
        ax2.plot(q, rss, color="tab:blue", lw=1.4, label="RSS (MB)")
        ax2.set_ylabel("RSS (MB)", color="tab:blue")
        ax2.tick_params(axis="y", labelcolor="tab:blue")

        ax1.set_title(f"{name} — CPU/mem vs query count\n({pd.get('unit','')}, cap {data.get('cpu_limit_cores')} cores)")
        fig.tight_layout()
        out = os.path.join(GRAPHS, f"ts_{name}.png")
        fig.savefig(out, dpi=110)
        plt.close(fig)
        print(f"[plots] {out}", flush=True)


# --- 2. rows vs storage, per optimize cadence -------------------------------
def plot_optimize_storage(data) -> None:
    results = data.get("results", [])
    if not results:
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    colors = plt.cm.viridis([0.1, 0.4, 0.65, 0.9])
    for r, col in zip(results, colors):
        cad = r["cadence"]
        label = "never" if cad == 0 else f"every {cad} appends"
        rows = [p["rows"] / 1e3 for p in r["series"]]
        mb = [p["bytes"] / 1e6 for p in r["series"]]
        ax.plot(rows, mb, color=col, lw=1.3, label=f"optimize: {label}")
        # mark compaction peaks so the sawtooth is legible
        peaks_x = [p["rows"] / 1e3 for p in r["series"] if p["phase"] == "compact_peak"]
        peaks_y = [p["bytes"] / 1e6 for p in r["series"] if p["phase"] == "compact_peak"]
        ax.scatter(peaks_x, peaks_y, color=col, s=18, marker="^", zorder=3)

    ax.set_xlabel("rows (thousands)")
    ax.set_ylabel("on-disk storage (MB)")
    ax.set_title("Row count vs storage — by optimize() cadence\n(▲ = post-compaction peak, before cleanup)")
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    out = os.path.join(GRAPHS, "optimize_storage.png")
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"[plots] {out}", flush=True)

    # secondary: fragments vs rows
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for r, col in zip(results, colors):
        cad = r["cadence"]
        label = "never" if cad == 0 else f"every {cad}"
        rows = [p["rows"] / 1e3 for p in r["series"] if p["phase"] in ("append", "post_cleanup")]
        frags = [p["fragments"] for p in r["series"] if p["phase"] in ("append", "post_cleanup")]
        ax.plot(rows, frags, color=col, lw=1.3, label=f"optimize: {label}")
    ax.set_xlabel("rows (thousands)")
    ax.set_ylabel("fragment count")
    ax.set_title("Fragment count vs rows — by optimize() cadence")
    ax.legend()
    ax.grid(alpha=0.25)
    fig.tight_layout()
    out = os.path.join(GRAPHS, "optimize_fragments.png")
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"[plots] {out}", flush=True)


# --- 3. scaling: throughput & cpu/mem vs time -------------------------------
def _throughput(samples: list[dict], win_s: float = 1.0) -> tuple[list[float], list[float]]:
    """Rolling throughput over a trailing ~win_s window.

    Adjacent-sample deltas (b-a) alias badly: a spawned instance briefly stretches
    the one sampler interval that straddles it, so a constant-rate counter dips
    then rebounds — a plot artifact (identical dip appears in the write profile),
    not a real throughput drop. Averaging over >=1s of samples removes it while
    preserving the true ramp/plateau shape.
    """
    ts, tp = [], []
    for k in range(1, len(samples)):
        i = k - 1
        while i > 0 and samples[k]["t"] - samples[i]["t"] < win_s:
            i -= 1
        dt = samples[k]["t"] - samples[i]["t"]
        if dt <= 0:
            continue
        d = samples[k].get("agg_ops", 0) - samples[i].get("agg_ops", 0)
        ts.append(samples[k]["t"])
        tp.append(max(0.0, d / dt))
    return ts, tp


def plot_scaling(data) -> None:
    cap = data.get("cpu_limit_cores")
    for profile, pd in data.get("profiles", {}).items():
        samples = pd.get("samples")
        if not samples:
            continue
        t = [s["t"] for s in samples]
        cpu = [s["cpu_cores"] for s in samples]
        rss = [s["rss_mb"] for s in samples]
        tp_t, tp = _throughput(samples)

        fig, ax1 = plt.subplots(figsize=(10, 5))
        ax1.plot(tp_t, tp, color="tab:green", lw=1.6, label="throughput (ops/s)")
        ax1.set_xlabel("elapsed time (s)")
        ax1.set_ylabel("throughput (ops/s)", color="tab:green")
        ax1.tick_params(axis="y", labelcolor="tab:green")

        ax2 = ax1.twinx()
        ax2.plot(t, cpu, color="tab:red", lw=1.2, alpha=0.8, label="CPU (cores)")
        ax2.set_ylabel("CPU cores (—) / RSS GB (··)", color="tab:red")
        ax2.tick_params(axis="y", labelcolor="tab:red")
        ax2.plot(t, [m / 1000 for m in rss], color="tab:blue", lw=1.0, ls=":", alpha=0.8, label="RSS (GB)")
        if cap:
            ax2.axhline(cap, ls="--", color="tab:red", alpha=0.4)

        for e in pd.get("events", []):
            ax1.axvline(e["t"], color="gray", ls="--", alpha=0.5)
            ax1.text(e["t"], ax1.get_ylim()[1] * 0.96, f"+inst → {e.get('active')}",
                     rotation=90, va="top", ha="right", fontsize=7, color="gray")

        ax1.set_title(f"scaling ({profile}) — throughput & CPU/mem as instances are added live\n"
                      f"(cap {cap} cores; ramp to {pd.get('max_instances')} instances)")
        lines = [ln for ln in (ax1.get_lines()[:1] + ax2.get_lines())
                 if not ln.get_label().startswith("_")]
        ax1.legend(lines, [ln.get_label() for ln in lines], loc="center right", fontsize=8)
        fig.tight_layout()
        out = os.path.join(GRAPHS, f"scaling_{profile}.png")
        fig.savefig(out, dpi=110)
        plt.close(fig)
        print(f"[plots] {out}", flush=True)


def _save(fig, name: str) -> None:
    out = os.path.join(GRAPHS, name)
    fig.savefig(out, dpi=110)
    plt.close(fig)
    print(f"[plots] {out}", flush=True)


def _legend(ax1, ax2) -> None:
    lines = [ln for ln in (ax1.get_lines() + ax2.get_lines()) if not ln.get_label().startswith("_")]
    ax1.legend(lines, [ln.get_label() for ln in lines], fontsize=8)


# --- 4. parallel ingest -----------------------------------------------------
def plot_ingest(data) -> None:
    cap = data.get("cpu_limit_cores")

    sweep = data.get("sweep_writers", [])
    if sweep:
        ns = [r["n_writers"] for r in sweep]
        agg = [r["agg_mb_per_s"] for r in sweep]
        base = agg[0] if agg else 0
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(ns, agg, "o-", color="tab:blue", lw=1.6, label="aggregate MB/s (measured)")
        ax.plot(ns, [base * n for n in ns], "--", color="gray", alpha=0.6, label="perfect-linear (N×single)")
        ax.set_xlabel("concurrent writers (one table each)")
        ax.set_ylabel("ingest throughput (MB/s)")
        ax.set_xticks(ns)
        ax.set_title("Parallel ingest throughput vs #writers\n(gap below the dashed line = where scaling stops: CPU or NIC/S3)")
        ax.legend()
        ax.grid(alpha=0.25)
        fig.tight_layout()
        _save(fig, "ingest_throughput_vs_writers.png")

    samples = data.get("samples", [])
    if samples:
        t = [s["t"] for s in samples]
        gb = [s.get("agg_bytes", 0) / 1e9 for s in samples]
        cpu = [s["cpu_cores"] for s in samples]
        rss = [s["rss_mb"] / 1000 for s in samples]
        fig, ax1 = plt.subplots(figsize=(10, 5))
        ax1.plot(t, gb, color="tab:green", lw=1.6, label="cumulative GB written")
        ax1.set_xlabel("elapsed time (s)")
        ax1.set_ylabel("cumulative GB", color="tab:green")
        ax1.tick_params(axis="y", labelcolor="tab:green")
        ax2 = ax1.twinx()
        ax2.plot(t, cpu, color="tab:red", lw=1.1, alpha=0.8, label="CPU (cores)")
        ax2.plot(t, rss, color="tab:blue", lw=1.0, ls=":", alpha=0.8, label="RSS (GB)")
        if cap:
            ax2.axhline(cap, ls="--", color="tab:red", alpha=0.4)
        ax2.set_ylabel("CPU cores (—) / RSS GB (··)", color="tab:red")
        ax2.tick_params(axis="y", labelcolor="tab:red")
        for e in data.get("events", []):
            if e.get("label") == "checkpoint":
                ax1.axvline(e["t"], color="gray", ls="--", alpha=0.25)
        ax1.set_title("Parallel ingest — cumulative GB & CPU/mem over time (checkpoints dashed)")
        _legend(ax1, ax2)
        fig.tight_layout()
        _save(fig, "ingest_timeseries.png")

    writers = data.get("writers", [])
    series = [te for te in writers if te.get("checkpoints")]
    if series:
        fig, ax = plt.subplots(figsize=(9, 5))
        for te in series:
            cks = te["checkpoints"]
            x = [ck["data_bytes"] / 1e9 for ck in cks]
            y = [ck["interval_mb_per_s"] for ck in cks]
            ax.plot(x, y, "o-", lw=1.2, label=te["table"])
        ax.set_xlabel("table size (GB)")
        ax.set_ylabel("interval ingest MB/s")
        ax.set_title("Ingest throughput vs table size\n(does write slow as the table / fragment count grows?)")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.25)
        fig.tight_layout()
        _save(fig, "ingest_mbps_vs_tablesize.png")


# --- 5. query degradation ---------------------------------------------------
_PCTS = (("p50_ms", "tab:green"), ("p95_ms", "tab:orange"), ("p99_ms", "tab:red"))


def plot_query(data) -> None:
    cells = data.get("cells", [])

    for variant in ("windowed_full", "metadata_only"):
        sc = sorted([c for c in cells if c["axis"] == "size" and c["variant"] == variant],
                    key=lambda x: x["table_gb"])
        if not sc:
            continue
        x = [c["table_gb"] for c in sc]
        fig, ax = plt.subplots(figsize=(9, 5))
        for p, col in _PCTS:
            ax.plot(x, [c[p] for c in sc], "o-", color=col, label=p)
        ax.set_xlabel("table size (GB)")
        ax.set_ylabel("latency (ms)")
        ax.set_title(f"Query latency vs table size — {variant}\n(windowed cursor scan; ~1 TB toward the right)")
        ax.legend()
        ax.grid(alpha=0.25)
        fig.tight_layout()
        _save(fig, f"query_pXX_vs_size_{variant}.png")

    tc = sorted([c for c in cells if c["axis"] == "table_count"], key=lambda x: x["n_tables"])
    if tc:
        x = [c["n_tables"] for c in tc]
        fig, ax1 = plt.subplots(figsize=(9, 5))
        for p, col in _PCTS:
            ax1.plot(x, [c[p] for c in tc], "o-", color=col, label=f"query {p}")
        ax1.set_xlabel("number of tables in the store")
        ax1.set_ylabel("single-table query latency (ms)")
        ax1.set_xticks(x)
        ax2 = ax1.twinx()
        ax2.plot(x, [c.get("list_p50_ms", 0) for c in tc], "s--", color="tab:blue", label="catalog list p50")
        ax2.set_ylabel("table_names() listing (ms)", color="tab:blue")
        ax2.tick_params(axis="y", labelcolor="tab:blue")
        ax1.set_title("Query & catalog-listing latency vs #tables\n(single-table reads are independent; only listing grows)")
        _legend(ax1, ax2)
        fig.tight_layout()
        _save(fig, "query_latency_vs_tables.png")

    cc = [c for c in cells if c["axis"] == "compaction" and c["variant"] == "windowed_full"]
    if len(cc) >= 2:
        metrics = ["p50_ms", "p95_ms", "p99_ms"]
        x = np.arange(len(metrics))
        w = 0.35
        fig, ax = plt.subplots(figsize=(8, 5))
        for i, st in enumerate(["uncompacted", "compacted"]):
            cell = next((c for c in cc if c["state"] == st), None)
            if not cell:
                continue
            ax.bar(x + (i - 0.5) * w, [cell[m] for m in metrics], w,
                   label=f"{st} ({cell.get('fragments')} frags)")
        ax.set_xticks(x)
        ax.set_xticklabels(metrics)
        ax.set_ylabel("latency (ms)")
        ax.set_title("Query latency: uncompacted vs one compaction (same table size)")
        ax.legend()
        ax.grid(alpha=0.25, axis="y")
        fig.tight_layout()
        _save(fig, "query_compacted_vs_not.png")

    nb = [c for c in cells if c["axis"] == "neighbor"]
    if nb:
        order = ([c for c in nb if c["load"] == "idle"]
                 + sorted([c for c in nb if c["load"] == "same"], key=lambda x: x["k"])
                 + sorted([c for c in nb if c["load"] == "other"], key=lambda x: x["k"]))
        labels = ["idle" if c["load"] == "idle" else f"{c['load']}×{c['k']}" for c in order]
        x = np.arange(len(order))
        fig, ax1 = plt.subplots(figsize=(10, 5))
        for p, col in _PCTS:
            ax1.plot(x, [c[p] for c in order], "o-", color=col, label=p)
        ax1.set_xticks(x)
        ax1.set_xticklabels(labels, rotation=30, ha="right")
        ax1.set_ylabel("conn_0 query latency (ms)")
        ax2 = ax1.twinx()
        ax2.plot(x, [c.get("neighbor_qps", 0) for c in order], "s--", color="gray", alpha=0.6, label="neighbor qps")
        ax2.set_ylabel("neighbor throughput (qps)", color="gray")
        ax1.set_title("Query latency vs busy neighbors (same table vs other tables)")
        _legend(ax1, ax2)
        fig.tight_layout()
        _save(fig, "query_latency_vs_neighbor.png")


# --- 6. compaction cadence --------------------------------------------------
def plot_cadence(data) -> None:
    turns = [t for t in data.get("turns", []) if not t.get("compact", {}).get("error")]
    if not turns:
        return
    x = [t["turn"] for t in turns]
    ref = data.get("reference", {})
    term_rss = ref.get("terminal_rss_mb")
    term_wall = ref.get("terminal_wall_s")
    pod = ref.get("pod_mem_mb")
    seed_gb = (data.get("seed") or {}).get("data_bytes", 0) / 1e9

    def op(t, k, f):
        v = t.get(k, {})
        return v.get(f)

    # (a) peak RSS per op per turn, with the terminal-compaction + pod-cap reference lines
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(x, [op(t, "compact", "peak_rss_mb") for t in turns], "o-", color="tab:red", lw=1.7, label="compact peak RSS")
    ax.plot(x, [op(t, "append", "peak_rss_mb") for t in turns], "s-", color="tab:blue", alpha=0.8, label="append peak RSS")
    ax.plot(x, [op(t, "read", "peak_rss_mb") for t in turns], "^-", color="tab:green", alpha=0.7, label="read peak RSS")
    if term_rss:
        ax.axhline(term_rss, ls="--", color="darkred", alpha=0.8,
                   label=f"terminal 1 TB compaction ({term_rss/1000:.1f} GB)")
    if pod:
        ax.axhline(pod, ls=":", color="black", alpha=0.6, label=f"MOVEIT pod cap ({pod/1024:.0f} GiB)")
    ax.set_xlabel("turn (read → append ~0.5 GB → compact)")
    ax.set_ylabel("peak RSS (MB, isolated process)")
    ax.set_title(f"Per-op peak RSS across {len(x)} compact-every-append turns\n"
                 f"(seed ~{seed_gb:.0f} GB; incremental compaction stays flat & far below the terminal spike)")
    ax.legend(fontsize=8, loc="center right")
    ax.grid(alpha=0.25)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    _save(fig, "cadence_rss_vs_turn.png")

    # (b) wall time per op per turn, with terminal-compaction reference
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.plot(x, [op(t, "compact", "wall_s") for t in turns], "o-", color="tab:red", lw=1.7, label="compact wall")
    ax.plot(x, [op(t, "append", "wall_s") for t in turns], "s-", color="tab:blue", alpha=0.8, label="append wall")
    ax.plot(x, [op(t, "read", "wall_s") for t in turns], "^-", color="tab:green", alpha=0.7, label="read wall")
    if term_wall:
        ax.axhline(term_wall, ls="--", color="darkred", alpha=0.8,
                   label=f"terminal 1 TB compaction ({term_wall/60:.0f} min)")
    ax.set_xlabel("turn (read → append ~0.5 GB → compact)")
    ax.set_ylabel("wall time (s, incl. dataset open)")
    ax.set_title(f"Per-op wall time across {len(x)} turns\n"
                 f"(each compaction only touches the fresh delta, so it stays flat as the table & version history grow)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.25)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    _save(fig, "cadence_wall_vs_turn.png")

    # (c) fragment sawtooth + un-cleaned version growth
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(x, [t.get("fragments") for t in turns], "o-", color="tab:purple", label="fragment count (post-compact)")
    ax1.set_xlabel("turn")
    ax1.set_ylabel("fragments", color="tab:purple")
    ax2 = ax1.twinx()
    ax2.plot(x, [t.get("version") for t in turns], "-", color="gray", alpha=0.6, label="dataset version (no cleanup)")
    ax2.set_ylabel("version (monotonic, cleanup off)", color="gray")
    ax1.set_title("Fragment count (bounded sawtooth) vs version growth across turns")
    _legend(ax1, ax2)
    fig.tight_layout()
    _save(fig, "cadence_fragments_vs_turn.png")


def main() -> None:
    os.makedirs(GRAPHS, exist_ok=True)
    ts = _load("timeseries")
    if ts:
        plot_timeseries(ts)
    opt = _load("optintervals")
    if opt:
        plot_optimize_storage(opt)
    sc = _load("scaling")
    if sc:
        plot_scaling(sc)
    ing = _load("parallel_ingest")
    if ing:
        plot_ingest(ing)
    qd = _load("query_degradation")
    if qd:
        plot_query(qd)
    cad = _load("compaction_cadence")
    if cad:
        plot_cadence(cad)
    print("[plots] done", flush=True)


if __name__ == "__main__":
    main()
