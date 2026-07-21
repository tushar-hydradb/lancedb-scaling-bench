# Rust lance 8.0.0 compaction-knob sweep

**Question:** do `CompactionOptions.{num_threads, max_source_fragments, io_buffer_size}` actually
change compaction **peak RSS** and **wall time**? Answered against the exact crate MOVEIT links
(`lance = "8.0.0"`, arrow 58), not pylance 0.33 (which exposes only `num_threads`).

**Setup:** 250 GB uncompacted backlog (250 fragments × 200 rows × ~1 GB logical), MOVEIT's 8-column
drain schema, real S3, `trpf=500`. Fresh process per config, each `checkout_version(seed).restore()`s
the identical backlog then times one `compact_files`; `VmHWM` = kernel peak RSS. Box: aarch64 8-vCPU /
30 GB Graviton (`teammate@…`). Binary cross-compiled locally (`rust-compaction-knobs/`, `cross`).

## Results (`knobs_results.jsonl`)

### num_threads @ trpf=500 (250→166 frags) — the RAM+wall dial
| num_threads | peak RSS (GiB) | wall (s) |
|---|---|---|
| 1 | 4.33 | 238.9 |
| 2 | 6.66 | 122.4 |
| 4 | 12.52 | 68.7 |
| 8 | 21.37 | 50.5 |
| default (unset → ~all cores) | 18.37 | 51.5 |

Fit **RSS ≈ 2.04 + 2.45 × num_threads GiB**. Mechanism: `buffer_unordered(num_threads)`
(`lance-8.0.0 optimize.rs:790`) → N concurrent tasks × ~trpf-sized decode buffer. Wall speedup
flattens past 4 threads (IO/S3-bound).

### max_source_fragments @ huge trpf, threads=2 — NOT a memory knob
| msf | frags after | RSS | wall |
|---|---|---|---|
| 4 | 250 (no-op) | 56 MB | 0.0 s |
| 8 | 250 (no-op) | 55 MB | 0.0 s |
| 16 | 250 (no-op) | 56 MB | 0.0 s |
| none | 1 | 6.31 GiB | 221.1 s |

`max_source_fragments` is a **global cumulative** source-frag budget applied with `take_while`
(`optimize.rs:735`): it drops the first task exceeding the budget *and everything after*. With huge
`trpf`, all 250 frags pack into one task, so any msf<250 discards it → total no-op. It's a per-*call*
work throttle, not a per-group RAM cap.

### io_buffer_size @ trpf=500, threads=4 — immaterial
| io_buffer | RSS (GiB) | wall (s) |
|---|---|---|
| default | 12.52 | 68.7 |
| 32 MB | 12.53 | 68.1 |
| 128 MB | 12.83 | 67.6 |

<2.5% RSS / <2% wall.

## Takeaway
Set `num_threads` explicitly to bound compaction RAM (2 → ~8 GiB pod, 1 → ~5 GiB); keep `trpf` as the
query-latency knob. `max_source_fragments` chunks a backlog across calls (never set below a task's
frag count → silent no-op). Leave `io_buffer_size` default.

_Caveat: compressible `'x'` filler ⇒ wall is logical decode throughput, not S3 bytes; peak RSS (the
decompressed working set) is representative. Uncapped box ≠ MOVEIT pod._

See `../INTERPRETATION.md` §6, plot `knobs_rss_wall_vs_threads.png`.
