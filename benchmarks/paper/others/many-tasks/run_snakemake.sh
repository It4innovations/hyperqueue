#!/bin/bash
# usage: run_snakemake.sh N  -> RESULT snakemake N dryrun_s total_s
B=$(dirname $(readlink -f $0)); N=$1; W=$(mktemp -d -p $B smk.XXXX)
cd $W; export NTASKS=$N
t0=$(date +%s.%N)
$B/.venv/bin/snakemake -s $B/Snakefile -n --quiet all >/dev/null 2>&1
t1=$(date +%s.%N)
timeout 3600 $B/.venv/bin/snakemake -s $B/Snakefile --cores 2 --quiet all >log 2>&1 || { tail -3 log; }
t2=$(date +%s.%N)
echo "RESULT snakemake $N $(echo "$t1-$t0"|bc) $(echo "$t2-$t1"|bc) done=$(ls out | wc -l)"
cd $B; rm -rf $W
