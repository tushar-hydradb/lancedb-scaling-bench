#!/usr/bin/env bash
# Does lance 8.0.0's CompactionOptions actually move compaction peak-RSS + wall?
# Seeds a ~250 GB uncompacted backlog ONCE (250 fragments of 200 rows / ~1 GB
# each, all under trpf), then runs `compact` once per config as a FRESH process
# (honest per-config VmHWM), each restoring the identical seed version first.
#
# Geometry note: with 200-row source fragments and trpf=500, each merge group
# naturally stops at ~3 fragments (600 rows >= 500), so at prod trpf the RSS dial
# is num_threads (concurrent groups). max_source_fragments only binds when trpf
# is large enough that it DOESN'T cap the group — hence Regime B runs at a huge
# trpf so msf is the sole per-group cap.
#
# Resilient to power loss: seed version + every JSON result line are appended to
# disk as they happen; re-running skips the seed and any config already in $OUT.
set -uo pipefail

BIN="${BIN:-./compaction-knobs}"
export BENCH_S3_URI="${BENCH_S3_URI:-s3://lancedb-temp-tprf500-bucket/rust-knobs/table}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export SEED_GB="${SEED_GB:-250}"
export MEAN_BYTES="${MEAN_BYTES:-5000000}"
export ROWS_PER_ARRAY="${ROWS_PER_ARRAY:-200}"

OUT="${OUT:-results.jsonl}"
SEED_FILE="${SEED_FILE:-seed_version.txt}"
# huge trpf => trpf never caps a group; the whole table would merge into one
# group unless max_source_fragments caps it. Isolates msf as the RAM knob.
BIG_TRPF=1000000000

echo "[driver] uri=$BENCH_S3_URI seed=${SEED_GB}GB region=$AWS_REGION out=$OUT"

# ---- 1. Seed once (idempotent) --------------------------------------------
if [[ -s "$SEED_FILE" ]]; then
  SEED_VERSION="$(cat "$SEED_FILE")"
  echo "[driver] reusing existing seed version=$SEED_VERSION"
else
  echo "[driver] seeding ${SEED_GB}GB backlog (long part; ~1h at ~70MB/s)…"
  SEED_LINE="$(TRPF=500 "$BIN" seed | grep '^SEED_VERSION=')" || { echo "[driver] seed FAILED"; exit 1; }
  SEED_VERSION="${SEED_LINE#SEED_VERSION=}"
  echo "$SEED_VERSION" > "$SEED_FILE"
  echo "[driver] seeded version=$SEED_VERSION"
fi
export SEED_VERSION

# ---- run_one LABEL TRPF NUM_THREADS MAX_SOURCE_FRAGMENTS IO_BUFFER_MB -------
# "-" leaves a knob at lance's default (env unset).
run_one() {
  local label="$1" trpf="$2" nt="$3" msf="$4" io="$5"
  if grep -q "\"label\": \"$label\"" "$OUT" 2>/dev/null; then
    echo "[driver] skip $label (already in $OUT)"; return
  fi
  echo "[driver] === $label  trpf=$trpf threads=$nt msf=$msf io=$io  $(date -u +%H:%M:%S) ==="
  env TRPF="$trpf" LABEL="$label" \
    $( [[ "$nt"  != "-" ]] && echo "KNOB_NUM_THREADS=$nt" ) \
    $( [[ "$msf" != "-" ]] && echo "KNOB_MAX_SOURCE_FRAGMENTS=$msf" ) \
    $( [[ "$io"  != "-" ]] && echo "KNOB_IO_BUFFER_MB=$io" ) \
    "$BIN" compact | tee -a "$OUT"
  local rc="${PIPESTATUS[0]}"
  if [[ "$rc" -eq 137 || "$rc" -eq 139 ]]; then
    echo "{\"label\": \"$label\", \"trpf\": $trpf, \"num_threads\": \"$nt\", \"max_source_fragments\": \"$msf\", \"oom\": true, \"exit\": $rc}" | tee -a "$OUT"
  elif [[ "$rc" -ne 0 ]]; then
    echo "{\"label\": \"$label\", \"error_exit\": $rc}" | tee -a "$OUT"
  fi
}

# === Regime A: num_threads at prod trpf=500 (THE primary RSS/wall dial) ======
# Ordered safe->risky so we always get low-thread data even if 8 threads OOMs
# the 30 GB box. peak ~ num_threads * (~3 frags * ~1 GB).
run_one "A_threads=1"       500  1  -   -
run_one "A_threads=2"       500  2  -   -
run_one "A_threads=4"       500  4  -   -
run_one "A_threads=8"       500  8  -   -
run_one "A_default"         500  -  -   -   # lance picks num_threads = #cpus

# === Regime B: max_source_fragments as an independent per-group RAM cap ======
# trpf huge so trpf never caps; threads=2 fixed => peak ~ 2 * msf * ~1 GB.
# msf small->big should scale RSS linearly; unbounded (msf=-) reads the whole
# 250 GB into ~2 groups => expected OOM (the point: never leave it unbounded).
run_one "B_msf=4_t2"        $BIG_TRPF  2  4    -
run_one "B_msf=8_t2"        $BIG_TRPF  2  8    -
run_one "B_msf=16_t2"       $BIG_TRPF  2  16   -
run_one "B_msf=none_t2"     $BIG_TRPF  2  -    -   # expected OOM; run last

# === Regime C: io_buffer_size at prod trpf=500, threads=4 fixed =============
run_one "C_io=32_t4"        500  4  -   32
run_one "C_io=128_t4"       500  4  -   128

echo "[driver] DONE $(date -u +%H:%M:%S). results:"
cat "$OUT"
