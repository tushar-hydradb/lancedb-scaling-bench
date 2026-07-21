#!/usr/bin/env bash
# Compaction-cadence bench against REAL AWS S3 (uncapped) — for boxes that can't
# fit a 500 GB local MinIO store on disk. The table lives under
# s3://$BUCKET/cadence/, so local disk only holds transient write buffers.
#
# The within-run contrast (turn 1 = full backlog vs turns 2..N = steady state)
# is unaffected by S3 network latency because it's constant across every turn —
# so this stays a valid apples-to-apples comparison despite using real S3.
#
#   ./run_cadence_s3.sh                 # full: 500 GB seed, 50 turns
#   BENCH_SEED_GB=20 BENCH_TURNS=5 ./run_cadence_s3.sh   # quick check
#
# Results publish to s3://$BUCKET/cadence-results/ every 5 min + after each stage
# (so another power loss / spot reclaim doesn't lose them). REPORT.md is NOT
# pushed to the bucket root (that would clobber the 1 TB run's report).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"; cd "$ROOT"

export BENCH_BUCKET="${BENCH_BUCKET:-lancedb-temp-tprf500-bucket}"
if [ -z "${AWS_REGION:-}" ]; then AWS_REGION="$(aws configure get region 2>/dev/null || true)"; fi
if [ -z "${AWS_REGION:-}" ]; then
  _tok="$(curl -sS -X PUT 'http://169.254.169.254/latest/api/token' \
          -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
  AWS_REGION="$(curl -sS -H "X-aws-ec2-metadata-token: $_tok" \
          'http://169.254.169.254/latest/meta-data/placement/region' 2>/dev/null || true)"
fi
: "${AWS_REGION:=us-east-1}"; export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

export BENCH_S3_REAL=1
export BENCH_RESULTS_DIR="$ROOT/results"
export BENCH_CAP_LABEL="ec2-s3-uncapped"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mpl}"
mkdir -p "$BENCH_RESULTS_DIR" "$MPLCONFIGDIR"

VENV="$ROOT/.venv"
PYBIN="${PYTHON:-}"
if [ -z "$PYBIN" ]; then
  for cand in python3.13 python3.12 python3.11 python3; do
    command -v "$cand" >/dev/null 2>&1 && PYBIN="$cand" && break
  done
fi
_supported() { case "$1" in 3.11|3.12|3.13) return 0 ;; *) return 1 ;; esac; }
_pyver="$("$PYBIN" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
if ! _supported "$_pyver"; then
  echo "[cadence-s3] ERROR: $PYBIN is Python $_pyver; need 3.11-3.13 (3.14 breaks matplotlib)." >&2
  echo "[cadence-s3] retry with: PYTHON=python3.13 ./run_cadence_s3.sh" >&2
  exit 1
fi
if [ -d "$VENV" ]; then
  _vv="$("$VENV/bin/python" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
  _supported "$_vv" || { echo "[cadence-s3] recreating .venv (was Python $_vv)"; rm -rf "$VENV"; }
fi
[ -d "$VENV" ] || "$PYBIN" -m venv "$VENV"
echo "[cadence-s3] venv python: $PYBIN ($_pyver)"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$ROOT/requirements.txt"

publish() {
  [ "${BENCH_UPLOAD_REPORT:-1}" = "1" ] || return 0
  command -v aws >/dev/null 2>&1 || return 0
  aws s3 sync "$BENCH_RESULTS_DIR" "s3://$BENCH_BUCKET/cadence-results/" --only-show-errors || true
}
if [ "${BENCH_UPLOAD_REPORT:-1}" = "1" ] && command -v aws >/dev/null 2>&1; then
  ( while true; do sleep 300; publish; done ) &
  SYNC_PID=$!
  trap 'kill "$SYNC_PID" 2>/dev/null || true' EXIT
fi

cd "$ROOT/bench"
echo "[cadence-s3] bucket=$BENCH_BUCKET region=$AWS_REGION seed_gb=${BENCH_SEED_GB:-500} turns=${BENCH_TURNS:-50}"
python bench_compaction_cadence.py; publish
python plots.py
python report.py; publish
echo "[cadence-s3] done -> $BENCH_RESULTS_DIR/REPORT.md ; results at s3://$BENCH_BUCKET/cadence-results/"
