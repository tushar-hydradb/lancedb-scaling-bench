# rust-compaction-knobs

Does lance 8.0.0's `CompactionOptions` actually move compaction **peak RSS** + **wall time**?
Drives the *exact* crate MOVEIT links (`lance = "8.0.0"`, arrow 58), so numbers reflect production,
not the pylance 0.33 binding (which exposes only `num_threads` of the three knobs).

Two subcommands (config via env):
- `seed` — write `SEED_GB` as many small fragments (an uncompacted backlog). Prints `SEED_VERSION=`.
- `compact` — `checkout_version(SEED_VERSION).restore()` the identical backlog, then run ONE
  `compact_files` with the knobs from env; report `VmHWM` (kernel peak RSS) + wall as one JSON line.

Each `compact` is a **fresh process** so `VmHWM` is honestly attributable to that config.

Env knobs: `KNOB_NUM_THREADS`, `KNOB_MAX_SOURCE_FRAGMENTS`, `KNOB_IO_BUFFER_MB` (unset = lance default),
plus `BENCH_S3_URI`, `AWS_REGION`, `SEED_GB`, `MEAN_BYTES`, `ROWS_PER_ARRAY`, `TRPF`, `SEED_VERSION`, `LABEL`.

## Build (cross-compile to the aarch64 box, no rustup on the box)
```
cross build --target aarch64-unknown-linux-gnu --release
```
`Cross.toml` installs a modern `protoc` (≥3.12, for lance-encoding's proto build) in the container.
Ship `target/aarch64-unknown-linux-gnu/release/compaction-knobs` + `run_knobs.sh` to the box.

## Run the sweep
```
OUT=~/knobs_results.jsonl SEED_FILE=~/knobs_seed_version.txt AWS_REGION=us-east-1 ./run_knobs.sh
```
Resume-safe: seeds once, skips any config already in `$OUT`. Regimes A (num_threads @ trpf=500),
B (max_source_fragments @ huge trpf), C (io_buffer_size). Results/plot/writeup:
`../results/rust-knobs/` and `../results/INTERPRETATION.md` §6.
