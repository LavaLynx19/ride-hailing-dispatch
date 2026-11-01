#!/bin/sh
# usage: run_baseline.sh NAME SEED "START" "END" WARMUP_SECONDS
# Runs one baseline window and writes experiments/runs/NAME.json. Times are local NYC.
set -eu
cd "$(dirname "$0")/.."

name=$1 seed=$2 start=$3 end=$4 warmup=$5
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

$compose up -d --force-recreate shard
trap '$compose down' EXIT
$compose run --rm -T simulator \
    --name "$name" --git-commit "$commit" --vm-cpus "$cpus" --vm-memory-gib "$memory" --simulator-cpus 2 \
    --data-dir /data --demand "/data/$(basename "$demand")" \
    --driver-url ws://shard:7001 --rider-url ws://shard:7002 \
    --seed "$seed" --start-epoch "$epoch" --duration-seconds "$duration" --warmup-seconds "$warmup" \
    > "experiments/runs/$name.json"
cat "experiments/runs/$name.json"
