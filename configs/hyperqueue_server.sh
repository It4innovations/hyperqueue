#!/bin/bash

#SBATCH --job-name=hyperqueue_server
#SBATCH --time=168:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4096M
#SBATCH --nodes=1
#SBATCH --threads-per-core=1
#SBATCH --account=$SLURM_ACCOUNT
#SBATCH --output=$HOME/logs/hyperqueue_server_%j.log
#SBATCH --error=$HOME/logs/hyperqueue_server_%j.log
#SBATCH --signal=B:USR1@300

set -euo pipefail

export PATH=/project/$SLURM_ACCOUNT/tools/hyperqueue/0.26.2:$PATH
export HQ_JOURNAL_DIR="$HOME/hyperqueue/journal"
export HQ_SERVER_DIR="$HOME/hyperqueue/server"

idle_timeout_seconds=1800  # 30 minutes
check_interval_seconds=30

mkdir -p "$HQ_JOURNAL_DIR/reports"

if hq server info >/dev/null 2>&1; then
    echo 'HQ is already running.'
    exit 0
fi

hq server start --journal "$HQ_JOURNAL_DIR/hq.journal" &
server_pid=$!
hq server wait
kill -0 "$server_pid"

shutdown() {
    trap - EXIT USR1
    local status=$1

    hq journal flush &&
        hq journal report "$HQ_JOURNAL_DIR/hq.journal" \
            "$HQ_JOURNAL_DIR/reports/hq_$(date -u +%Y-%m-%d_%H-%M-%S).html" || status=1

    hq server stop || status=1
    wait "$server_pid" || status=1
    exit "$status"
}

trap 'shutdown "$?"' EXIT
trap 'exit 0' USR1

if [[ $(hq --output-mode=json alloc list) == '[]' ]]; then
    bash "/project/$SLURM_ACCOUNT/tools/hyperqueue/nibi.sh"
fi

last_activity=$SECONDS
last_total_jobs=-1

while kill -0 "$server_pid" 2>/dev/null; do
    if stats=$(hq --output-mode=json job summary | python3 -c '
import json, sys
jobs = json.load(sys.stdin)
print(sum(jobs.get(s, 0) for s in ("Waiting", "Running", "Opened")), sum(jobs.values()))
'); then
        read -r active_jobs total_jobs <<< "$stats"

        if (( active_jobs > 0 || total_jobs != last_total_jobs )); then
            last_activity=$SECONDS
        elif (( SECONDS - last_activity >= idle_timeout_seconds )); then
            exit 0
        fi

        last_total_jobs=$total_jobs
    else
        last_activity=$SECONDS
    fi

    sleep "$check_interval_seconds" &
    wait "$!"
done

wait "$server_pid"
