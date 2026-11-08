#!/bin/sh
# usage: fault_campaign.sh OUTPUT
# Runs the two-Shard fault test (crates/shard/tests/two_shards.rs) under each fault mix, three
# seeds each, and appends each round's summary to OUTPUT. Needs DATABASE_URL, like the tests.
set -u
cd "$(dirname "$0")/.."
out=$1
none_faults="drop_request=0,drop_reply=0,delay_ms=0"
run() {
    name=$1 faults=$2 crashes=$3 partitions=$4 kills=$5
    for seed in 1 2 3; do
        echo "== $name seed $seed" >> "$out"
        CAMPAIGN_SEED=$seed CAMPAIGN_FAULTS="$faults" CAMPAIGN_CRASHES="$crashes" CAMPAIGN_PARTITIONS=$partitions CAMPAIGN_KILLS=$kills \
            cargo test -q -p shard --test two_shards -- --nocapture peer_faults 2>&1 \
            | grep -E '^(faults|Shard [01]:|invariant:|test result|thread)' >> "$out"
    done
}
run crash-after-reserve "$none_faults" "after_reserve=0.3" 0 0
run crash-after-confirm "$none_faults" "after_confirm=0.3" 0 0
run crash-before-settle "$none_faults" "before_settle=0.3" 0 0
run crash-before-release "$none_faults" "before_release=0.3" 0 0
run dropped-calls "drop_request=0.2,drop_reply=0.2,delay_ms=300" "" 0 0
run late-calls "drop_request=0,drop_reply=0,delay_ms=4500" "" 0 0
run past-expiry "drop_request=0,drop_reply=0,delay_ms=9000" "" 0 0
run partitions "$none_faults" "" 1 0
run kills "$none_faults" "" 0 1
run everything "drop_request=0.05,drop_reply=0.05,delay_ms=300" \
    "after_reserve=0.04,after_confirm=0.04,before_settle=0.04,before_release=0.04" 1 1
