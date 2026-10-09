#!/bin/bash
# usage: run_hq.sh N  -> RESULT hq N submit_s total_s
# HQ defaults to the head binary built by ../../e2e/fetch_binaries.py --build-local
B=$(dirname $(readlink -f $0)); N=$1; HQ=${HQ:-$B/../../e2e/bin/hq-head}
D=$(mktemp -d -p $B hq.XXXX); export HQ_SERVER_DIR=$D/srv
$HQ server start >$D/server.log 2>&1 &
until [ -e $D/srv/hq-current/access.json ]; do sleep 0.1; done
for w in 1 2; do $HQ worker start --cpus 1 --detect-resources none >$D/w$w.log 2>&1 & done
for _ in $(seq 100); do [ "$($HQ worker list --output-mode quiet | wc -l)" = 2 ] && break; sleep 0.1; done
t0=$(date +%s.%N)
$HQ submit --array 1-$N --stdout none --stderr none -- true >/dev/null
t1=$(date +%s.%N)
$HQ job wait all >/dev/null
t2=$(date +%s.%N)
echo "RESULT hq $N $(echo "$t1-$t0"|bc) $(echo "$t2-$t0"|bc)"
$HQ server stop >/dev/null; wait; rm -rf $D
