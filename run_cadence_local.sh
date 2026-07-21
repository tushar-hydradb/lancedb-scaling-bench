#!/usr/bin/env bash
# Compaction-cadence bench — runs LOCALLY against the docker MinIO (bare venv,
# UNCAPPED so the true per-op RSS is visible), then re-renders plots + report.
#
#   BENCH_SMOKE=1 ./run_cadence_local.sh   # ~5 GB seed, 3 turns — wiring check
#   ./run_cadence_local.sh                 # full: 500 GB seed, 50 turns
#
# It seeds a table above `trpf` so compaction only ever touches the fresh delta,
# then loops read -> append(~0.5 GB) -> compact, recording wall + peak RSS per op.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

export BENCH_BUCKET="${BENCH_BUCKET:-lance-cadence}"
export MINIO_API_PORT="${MINIO_API_PORT:-9010}"

echo "[cadence] booting MinIO + bucket ($BENCH_BUCKET) ..."
BENCH_BUCKET="$BENCH_BUCKET" docker compose up -d minio createbucket
# wait for the one-shot bucket creation to finish
for _ in $(seq 1 30); do
  docker compose ps createbucket --format '{{.State}}' 2>/dev/null | grep -q exited && break
  sleep 1
done

# Local MinIO reached over the host-mapped port (NOT the in-compose DNS name).
export S3_ENDPOINT="http://127.0.0.1:${MINIO_API_PORT}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
unset BENCH_S3_REAL  # this is the MinIO path, not real S3
export BENCH_RESULTS_DIR="$ROOT/results"
export BENCH_CAP_LABEL="local-uncapped"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mpl}"
mkdir -p "$BENCH_RESULTS_DIR" "$MPLCONFIGDIR"

VENV="$ROOT/.venv"
# Pick a Python the pinned wheels support. Python 3.14 breaks matplotlib
# (Path.__deepcopy__ recursion / segfault) and lacks wheels for the pins.
PYBIN="${PYTHON:-}"
if [ -z "$PYBIN" ]; then
  for cand in python3.13 python3.12 python3.11 python3; do
    command -v "$cand" >/dev/null 2>&1 && PYBIN="$cand" && break
  done
fi
_supported() { case "$1" in 3.11|3.12|3.13) return 0 ;; *) return 1 ;; esac; }
_pyver="$("$PYBIN" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
if ! _supported "$_pyver"; then
  echo "[cadence] ERROR: $PYBIN is Python $_pyver; need 3.11-3.13 (3.14 breaks matplotlib)." >&2
  echo "[cadence] install python3.13 and re-run, or: PYTHON=python3.13 ./run_cadence_local.sh" >&2
  exit 1
fi
# Recreate the venv if missing or built with an unsupported interpreter.
if [ -d "$VENV" ]; then
  _vv="$("$VENV/bin/python" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
  _supported "$_vv" || { echo "[cadence] recreating .venv (was Python $_vv)"; rm -rf "$VENV"; }
fi
[ -d "$VENV" ] || "$PYBIN" -m venv "$VENV"
echo "[cadence] venv python: $PYBIN ($_pyver)"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$ROOT/requirements.txt"

cd "$ROOT/bench"
echo "[cadence] endpoint=$S3_ENDPOINT bucket=$BENCH_BUCKET smoke=${BENCH_SMOKE:-0}"
python bench_compaction_cadence.py
python plots.py
python report.py
echo "[cadence] done -> $BENCH_RESULTS_DIR/REPORT.md"
