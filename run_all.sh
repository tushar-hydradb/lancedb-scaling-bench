#!/usr/bin/env bash
# Boot the harness, run every bench inside the CAPPED container, render the report.
#
#   ./run_all.sh              # primary pass at MOVEIT prod cap (2 vCPU / 4 GiB)
#   CAP=1cpu2g ./run_all.sh   # optional staging-cap pass (1 vCPU / 2 GiB)
#
# Results land in ./results/ (mounted into the container); the report is
# ./results/REPORT.md.
set -euo pipefail
cd "$(dirname "$0")"

# --- resource cap -----------------------------------------------------------
case "${CAP:-2cpu4g}" in
  2cpu4g) export BENCH_CPUS=2.0 BENCH_MEM=4g   BENCH_CAP_LABEL=2cpu-4g ;;
  1cpu2g) export BENCH_CPUS=1.0 BENCH_MEM=2g   BENCH_CAP_LABEL=1cpu-2g ;;
  *) echo "unknown CAP=$CAP (use 2cpu4g|1cpu2g)"; exit 1 ;;
esac
echo ">> cap: ${BENCH_CAP_LABEL} (cpus=${BENCH_CPUS} mem=${BENCH_MEM}, disk uncapped)"

# Own the result files as the host user, not root.
export HOST_UID="$(id -u)" HOST_GID="$(id -g)"

mkdir -p results

# --- boot infra + capped runner (createbucket gates the bench container) ----
echo ">> docker compose up (build)…"
docker compose up -d --build

# Wait until the bench container is running its sleep loop.
echo ">> waiting for bench container…"
for _ in $(seq 1 30); do
  if docker compose exec -T bench python -c "print('ready')" >/dev/null 2>&1; then break; fi
  sleep 2
done

run() {
  local script="$1"
  echo ">> running ${script} …"
  # `|| true` so one bench crashing (e.g. an OOM finding) doesn't abort the rest;
  # the report renders whatever JSON was produced.
  docker compose exec -T bench python "/bench/${script}" || echo "!! ${script} exited non-zero (continuing)"
}

run bench_write.py
run bench_read.py
run bench_concurrency.py
run bench_2gb_column.py
run bench_optimize.py
# time-series / graph benches
run bench_timeseries.py
run bench_optimize_intervals.py
run bench_scaling.py

echo ">> rendering graphs…"
docker compose exec -T bench python /bench/plots.py || echo "!! plots failed (continuing)"

echo ">> rendering report…"
docker compose exec -T bench python /bench/report.py || true

echo
echo ">> DONE. Report: ./results/REPORT.md"
echo ">> Raw JSON:   ./results/*.json"
echo ">> Tear down with: docker compose down -v"
