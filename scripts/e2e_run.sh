#!/usr/bin/env bash
# One-command recorded run: start services -> ingest -> dbt -> quality -> spark -> streaming -> RESULTS.md -> stop.
# Usage: scripts/e2e_run.sh [--sample] [--events N]   (--sample = demo slice)
set -euo pipefail
cd "$(dirname "$0")/.."
EVENTS=1000000
while [ $# -gt 0 ]; do
  case "$1" in --sample) export SAMPLE_MODE=1 RESULTS_OUT="$PWD/docs/RESULTS_SAMPLE.md"; EVENTS=20000;; --events) EVENTS="$2"; shift;; esac; shift
done
source scripts/env.sh
export LOAD_ID="${LOAD_ID:-$(date +%Y%m%dT%H%M%S)}"
TIMINGS="$DATA_DIR/e2e_timings.tsv"; mkdir -p "$DATA_DIR"; : > "$TIMINGS"
cleanup() { scripts/kafka_native.sh stop >/dev/null 2>&1 || true; scripts/pg_native.sh stop >/dev/null 2>&1 || true; }
trap cleanup EXIT
step() {  # step <name> <cmd...>
  local name="$1"; shift; local t0; t0=$(date +%s.%N)
  echo "=== [$name] $*"; "$@"
  python -c "import sys,time; print('%s\t%.1f' % (sys.argv[1], time.time()-float(sys.argv[2])))" "$name" "$t0" >> "$TIMINGS"
}
T0=$(date +%s)
step services_up bash -c 'scripts/pg_native.sh start && { [ ! -x .tools/kafka/bin/kafka-server-start.sh ] || scripts/kafka_native.sh start; sleep 15; }'
step acquire python -m ingest.acquire
step load_raw python -m ingest.load
step geo_enrich python -m ingest.geo
step weather python -m ingest.weather
step dbt_build bash -c 'cd warehouse && dbt build --profiles-dir . ${SAMPLE_MODE:+--full-refresh}'
step quality python -m warehouse.quality
step spark python -m spark.run_all
step streaming python -m streaming.run --events "$EVENTS"
echo "$(( $(date +%s) - T0 ))" > "$DATA_DIR/e2e_total_s"
python scripts/gen_results.py
echo "e2e complete in $(cat "$DATA_DIR/e2e_total_s")s -> ${RESULTS_OUT:-docs/RESULTS.md}"
