#!/bin/sh
# usage: run_sharded.sh NAME SEED "START" "END" WARMUP_SECONDS [SUPPLY_SCALE] [DEMAND_SCALE]
# As run_baseline.sh, but on deploy/sharded.yml with SHARDS Shards (1, 2 or 4) in the
# environment, each with its own Postgres. Writes experiments/runs/NAME.json, NAME.stderr.txt,
# and NAME.shard-N-stats.txt per Shard. The invariant checker reads every Shard's Postgres.
# Policy comes from MATCHER, BATCH_WINDOW_MS and WIDENING, as in run_baseline.sh. SPIKE, if set,
# is "ZONE,START_SECONDS,SECONDS,MULTIPLIER" and is passed to the simulator as --spike.
set -eu
cd "$(dirname "$0")/.."

name=$1 seed=$2 start=$3 end=$4 warmup=$5 supply_scale=${6:-1} demand_scale=${7:-1}
export SHARDS=${SHARDS:-1}
case $SHARDS in
    1) profile="" ;;
    2) profile="--profile two" ;;
    4) profile="--profile four" ;;
    *) echo "SHARDS must be 1, 2 or 4" >&2; exit 1 ;;
esac
compose="docker compose -f deploy/sharded.yml $profile"
slug=$(echo "$start" | tr -d ' :-')
demand="data/raw/window_$slug.csv"

# Two hours before the start too, so Trips already in progress can warm-start drivers.
lookback=$(python3 -c "import calendar,sys,time; print(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(calendar.timegm(time.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S')) - 7200)))" "$start")
data/.venv/bin/python data/export_demand.py \
    --demand data/raw/demand_2026-09.parquet --start "$lookback" --end "$end" --output "$demand"
epoch=$(python3 -c "import calendar,sys,time; print(calendar.timegm(time.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S')))" "$start")
duration=$(python3 -c "import calendar,sys,time; f=lambda s: calendar.timegm(time.strptime(s, '%Y-%m-%d %H:%M:%S')); print(f(sys.argv[2]) - f(sys.argv[1]))" "$start" "$end")
commit=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- crates proto deploy migrations || echo -dirty)
cpus=$(docker info --format '{{.NCPU}}')
memory=$(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f", $1 / 1073741824}')

export MATCHER=${MATCHER:-greedy} BATCH_WINDOW_MS=${BATCH_WINDOW_MS:-} WIDENING=${WIDENING:-off}
policy="matcher=$MATCHER batch_window_ms=${BATCH_WINDOW_MS:-none} widening=$WIDENING shards=$SHARDS spike=${SPIKE:-none}"

last=$((SHARDS - 1))
urls() { # PORTS...
    list=""
    for shard in $(seq 0 $last); do
        for port in "$@"; do list="$list,ws://shard-$shard:$port"; done
    done
    echo "${list#,}"
}
databases=""
shards=""
for shard in $(seq 0 $last); do
    databases="$databases,postgres://postgres@postgres-$shard/dispatch"
    shards="$shards shard-$shard"
done
spike=""
if [ -n "${SPIKE:-}" ]; then spike="--spike $SPIKE"; fi

$compose down -v
$compose up -d --force-recreate $shards
trap '$compose down -v' EXIT
# shellcheck disable=SC2086
$compose run --rm -T simulator \
    --name "$name" --git-commit "$commit" --policy "$policy" --vm-cpus "$cpus" --vm-memory-gib "$memory" --simulator-cpus 2 \
    --data-dir /data --demand "/data/$(basename "$demand")" \
    --driver-url "$(urls 7001 7003 7005 7007)" --rider-url "$(urls 7002 7004 7006 7008)" \
    --seed "$seed" --start-epoch "$epoch" --duration-seconds "$duration" --warmup-seconds "$warmup" \
    --supply-scale "$supply_scale" --demand-scale "$demand_scale" $spike \
    --database-url "${databases#,}" \
    > "experiments/runs/$name.json" 2> "experiments/runs/$name.stderr.txt"
for shard in $(seq 0 $last); do
    $compose logs --timestamps --no-log-prefix "shard-$shard" | grep -E ' (stats|policy|faults):|shard [0-9]+ of |injected crash|serving map' \
        > "experiments/runs/$name.shard-$shard-stats.txt" || true
done
cat "experiments/runs/$name.json"
