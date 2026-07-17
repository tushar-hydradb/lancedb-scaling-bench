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

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = "/results"
GRAPHS = "/results/graphs"


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
def _throughput(samples: list[dict]) -> tuple[list[float], list[float]]:
    ts, tp = [], []
    for a, b in zip(samples, samples[1:]):
        dt = b["t"] - a["t"]
        if dt <= 0:
            continue
        d = b.get("agg_ops", 0) - a.get("agg_ops", 0)
        ts.append(b["t"])
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
    print("[plots] done", flush=True)


if __name__ == "__main__":
    main()
