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

export BENCH_BUCKET="${BENCH_BUCKET:-lancedb-temp-bucket}"

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

if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$ROOT/requirements.txt"

cd "$ROOT/bench"
echo "[run_ec2] bucket=$BENCH_BUCKET region=$AWS_REGION smoke=${BENCH_SMOKE:-0} cores=$(nproc)"
python bench_parallel_ingest.py
python bench_query_degradation.py
python plots.py
python report.py
echo "[run_ec2] done -> $BENCH_RESULTS_DIR/REPORT.md"
