#!/usr/bin/env python3
"""Plot lance 8.0.0 compaction knob sweep (250 GB backlog, real S3, aarch64 box).
Reads knobs_results.jsonl, writes knobs_rss_wall_vs_threads.png."""
import json, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

here = os.path.dirname(__file__)
rows = [json.loads(l) for l in open(os.path.join(here, "knobs_results.jsonl")) if l.strip()]
A = sorted([r for r in rows if r["label"].startswith("A_threads=")],
           key=lambda r: r["num_threads"])
t = [r["num_threads"] for r in A]
rss = [r["peak_rss_mb"] / 1024 for r in A]      # GiB
wall = [r["wall_s"] for r in A]

# linear fit RSS ~ a + b*threads
n = len(t); sx = sum(t); sy = sum(rss)
sxx = sum(x*x for x in t); sxy = sum(x*y for x, y in zip(t, rss))
b = (n*sxy - sx*sy) / (n*sxx - sx*sx); a = (sy - b*sx) / n

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))

ax1.plot(t, rss, "o-", color="crimson", lw=2, ms=8, label="measured peak RSS")
xs = [0, 8.5]
ax1.plot(xs, [a + b*x for x in xs], "--", color="grey",
         label=f"fit: {a:.1f} + {b:.2f}·threads GiB")
ax1.axhline(30, color="black", ls=":", lw=1, label="box RAM 30 GiB")
ax1.set_xlabel("num_threads (CompactionOptions)")
ax1.set_ylabel("peak compaction RSS (GiB, VmHWM)")
ax1.set_title("Peak RSS scales ~linearly with num_threads")
ax1.set_xlim(0, 8.5); ax1.set_ylim(0, 32)
ax1.grid(alpha=.3); ax1.legend(fontsize=8)

ax2.plot(t, wall, "o-", color="navy", lw=2, ms=8)
for x, y in zip(t, wall):
    ax2.annotate(f"{y:.0f}s", (x, y), textcoords="offset points", xytext=(6, 6), fontsize=8)
ax2.set_xlabel("num_threads (CompactionOptions)")
ax2.set_ylabel("compaction wall time (s)")
ax2.set_title("Wall time: speedup flattens past 4 threads (IO-bound)")
ax2.set_xlim(0, 8.5); ax2.set_ylim(0, max(wall)*1.1)
ax2.grid(alpha=.3)

fig.suptitle("lance 8.0.0 compaction knobs — 250 GB backlog (250→166 frags), trpf=500, real S3, 8-vCPU/30 GB Graviton",
             fontsize=10)
fig.tight_layout()
out = os.path.join(here, "knobs_rss_wall_vs_threads.png")
fig.savefig(out, dpi=120)
print("wrote", out)
print(f"fit: RSS(GiB) = {a:.2f} + {b:.2f} * num_threads")
