#!/usr/bin/env bash
# Large-scale (>1 TB) LanceDB bench — runs UNCAPPED on an EC2 box against real
# AWS S3, authenticating via the EC2 IAM *instance role* (no keys in env).
#
# Bare venv (not Docker) on purpose: a container can't read instance-role creds
# from IMDS unless the instance's metadata hop limit is raised to 2 — the venv
# path sidesteps that entirely.
#
# Prereqs on the box:
#   * an IAM instance role granting s3:{Get,Put,List,Delete}Object + s3:ListBucket
#     on the target bucket (the default AWS credential chain picks it up)
#   * python3.11+ with the venv module, and git-cloned/scp'd this repo
#
# Bucket defaults to lancedb-temp-bucket; region is taken from the creds' default
# (aws config, or EC2 IMDS). Override either with BENCH_BUCKET / AWS_REGION.
#
# Recommended first pass (validates IAM + measures real EC2->S3 MB/s before you
# commit to ~4 TB): BENCH_SMOKE=1 ./run_ec2.sh
# Full run (4 tables x ~1 TB): ./run_ec2.sh
# Teardown after capturing results/: aws s3 rm --recursive "s3://$BENCH_BUCKET/bigscale"
set -euo pipefail

export BENCH_BUCKET="${BENCH_BUCKET:-lancedb-temp-tprf500-bucket}"

# Region = the creds' default. Resolve from aws-config, then EC2 IMDS; fall back
# to us-east-1. Export both names so boto3 and Lance's object_store agree.
if [ -z "${AWS_REGION:-}" ]; then
  AWS_REGION="$(aws configure get region 2>/dev/null || true)"
fi
if [ -z "${AWS_REGION:-}" ]; then
  _tok="$(curl -sS -X PUT 'http://169.254.169.254/latest/api/token' \
          -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
  AWS_REGION="$(curl -sS -H "X-aws-ec2-metadata-token: $_tok" \
          'http://169.254.169.254/latest/meta-data/placement/region' 2>/dev/null || true)"
fi
: "${AWS_REGION:=us-east-1}"
export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV="$ROOT/.venv"

export BENCH_S3_REAL=1
export BENCH_RESULTS_DIR="$ROOT/results"
export BENCH_CAP_LABEL="ec2-uncapped"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mpl}"
mkdir -p "$BENCH_RESULTS_DIR" "$MPLCONFIGDIR"

# Pick a Python the pinned wheels support. Python 3.14 breaks matplotlib
# (Path.__deepcopy__ recursion) and lacks wheels for the pins; the validated
# stack is 3.13 (matching the Dockerfile). Override with PYTHON=python3.x.
PYBIN="${PYTHON:-}"
if [ -z "$PYBIN" ]; then
  for cand in python3.13 python3.12 python3.11 python3; do
    command -v "$cand" >/dev/null 2>&1 && PYBIN="$cand" && break
  done
fi
_supported() { case "$1" in 3.11|3.12|3.13) return 0 ;; *) return 1 ;; esac; }
_pyver="$("$PYBIN" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
if ! _supported "$_pyver"; then
  echo "[run_ec2] ERROR: $PYBIN is Python $_pyver; need 3.11-3.13 (3.14 breaks matplotlib)." >&2
  echo "[run_ec2] install python3.13 and re-run, or: PYTHON=python3.13 ./run_ec2.sh" >&2
  exit 1
fi

# Recreate the venv if missing or built with an unsupported interpreter.
if [ -d "$VENV" ]; then
  _vv="$("$VENV/bin/python" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo '?')"
  _supported "$_vv" || { echo "[run_ec2] recreating .venv (was Python $_vv)"; rm -rf "$VENV"; }
fi
[ -d "$VENV" ] || "$PYBIN" -m venv "$VENV"
echo "[run_ec2] venv python: $PYBIN ($_pyver)"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$ROOT/requirements.txt"

cd "$ROOT/bench"
echo "[run_ec2] bucket=$BENCH_BUCKET region=$AWS_REGION smoke=${BENCH_SMOKE:-0} cores=$(nproc)"
# Publish whatever's in results/ to the bucket (REPORT.md at root so it's a
# one-liner to grab; graphs/ alongside so its image links resolve; raw JSON under
# results/). Called after EACH stage so a mid-run death (e.g. an OOM during the
# 1 TB compaction) still leaves the ingest + query JSON in S3. Toggle with
# BENCH_UPLOAD_REPORT=0.
publish() {
  [ "${BENCH_UPLOAD_REPORT:-1}" = "1" ] || return 0
  command -v aws >/dev/null 2>&1 || { echo "[run_ec2] aws CLI missing; skip upload" >&2; return 0; }
  aws s3 sync "$BENCH_RESULTS_DIR" "s3://$BENCH_BUCKET/results/" --exclude "graphs/*" --only-show-errors || true
  [ -d "$BENCH_RESULTS_DIR/graphs" ] && aws s3 sync "$BENCH_RESULTS_DIR/graphs" "s3://$BENCH_BUCKET/graphs/" --only-show-errors || true
  [ -f "$BENCH_RESULTS_DIR/REPORT.md" ] && aws s3 cp "$BENCH_RESULTS_DIR/REPORT.md" "s3://$BENCH_BUCKET/REPORT.md" --only-show-errors || true
}

# Belt-and-suspenders: sync results/ (incl. the append-only *.jsonl detail logs
# and the frequently-snapshotted *.json) to S3 every 5 min while the long stages
# run, so an overnight OOM / spot-reclaim / SSH-drop still leaves fresh data.
if [ "${BENCH_UPLOAD_REPORT:-1}" = "1" ] && command -v aws >/dev/null 2>&1; then
  ( while true; do sleep 300; publish; done ) &
  SYNC_PID=$!
  trap 'kill "$SYNC_PID" 2>/dev/null || true' EXIT
fi

# Resume support: the ~4 h ingest writes results/parallel_ingest.json (with the
# per-table checkpoint versions). The query stage only READS that JSON + the 1 TB
# tables already in S3 — so a re-run can skip ingest entirely and pick up from the
# query stage. BENCH_SKIP_INGEST=1 does that.
if [ "${BENCH_SKIP_INGEST:-0}" = "1" ]; then
  [ -f "$BENCH_RESULTS_DIR/parallel_ingest.json" ] || {
    echo "[run_ec2] BENCH_SKIP_INGEST=1 but $BENCH_RESULTS_DIR/parallel_ingest.json is missing." >&2
    echo "[run_ec2] grab it first: aws s3 cp s3://$BENCH_BUCKET/results/parallel_ingest.json $BENCH_RESULTS_DIR/" >&2
    exit 1
  }
  echo "[run_ec2] BENCH_SKIP_INGEST=1 — resuming from existing parallel_ingest.json (no re-ingest)"
else
  python bench_parallel_ingest.py; publish
fi
python bench_query_degradation.py; publish
python plots.py
python report.py;                  publish

echo "[run_ec2] done -> $BENCH_RESULTS_DIR/REPORT.md"
if [ "${BENCH_UPLOAD_REPORT:-1}" = "1" ]; then
  echo "[run_ec2] report:  s3://$BENCH_BUCKET/REPORT.md"
  echo "[run_ec2] graphs:  s3://$BENCH_BUCKET/graphs/"
  echo "[run_ec2] rawjson: s3://$BENCH_BUCKET/results/"
fi
