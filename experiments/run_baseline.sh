#!/bin/sh
# usage: run_baseline.sh NAME SEED "START" "END" WARMUP_SECONDS [SUPPLY_SCALE] [DEMAND_SCALE]
# Runs one window and writes experiments/runs/NAME.json, plus NAME.ping-log.txt with what the
# ping log stored. Times are local NYC. The ping log is cleared before and after each run.
set -eu
cd "$(dirname "$0")/.."

name=$1 seed=$2 start=$3 end=$4 warmup=$5 supply_scale=${6:-1} demand_scale=${7:-1}
compose="docker compose -f deploy/baseline.yml"
slug=$(echo "$start" | tr -d ' :-')
demand="data/raw/window_$slug.csv"

data/.venv/bin/python data/export_demand.py \
    --demand data/raw/demand_2026-09.parquet --start "$start" --end "$end" --output "$demand"
epoch=$(python3 -c "import calendar,sys,time; print(calendar.timegm(time.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S')))" "$start")
duration=$(python3 -c "import calendar,sys,time; f=lambda s: calendar.timegm(time.strptime(s, '%Y-%m-%d %H:%M:%S')); print(f(sys.argv[2]) - f(sys.argv[1]))" "$start" "$end")
commit=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- crates proto deploy || echo -dirty)
cpus=$(docker info --format '{{.NCPU}}')
memory=$(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f", $1 / 1073741824}')

ports() { echo "ws://shard:$1,ws://shard:$2,ws://shard:$3,ws://shard:$4"; }

$compose down -v
$compose up -d --force-recreate shard
trap '$compose down -v' EXIT
$compose run --rm -T simulator \
    --name "$name" --git-commit "$commit" --vm-cpus "$cpus" --vm-memory-gib "$memory" --simulator-cpus 2 \
    --data-dir /data --demand "/data/$(basename "$demand")" \
    --driver-url "$(ports 7001 7003 7005 7007)" --rider-url "$(ports 7002 7004 7006 7008)" \
    --seed "$seed" --start-epoch "$epoch" --duration-seconds "$duration" --warmup-seconds "$warmup" \
    --supply-scale "$supply_scale" --demand-scale "$demand_scale" \
    > "experiments/runs/$name.json"
{
    echo "bytes on disk: $($compose exec -T shard du -sb /ping-log | cut -f1)"
    echo "last drop report: $($compose logs --no-log-prefix shard | grep 'ping log dropped' | tail -1)"
    echo "errors: $($compose logs --no-log-prefix shard | grep -c 'ping log .* failed' || true)"
} > "experiments/runs/$name.ping-log.txt"
cat "experiments/runs/$name.json"
