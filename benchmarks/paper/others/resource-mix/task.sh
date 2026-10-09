#!/bin/bash
# usage: task.sh TASK_ID DURATION_VARIANT0 [DURATION_VARIANT1]
# Sleeps for the duration of the variant chosen by HQ and appends one record
# "task_id variant worker start end" to $RECORDS.
export LC_ALL=C
v=${HQ_RESOURCE_VARIANT:-0}
if [ "$v" = 1 ]; then d=$3; else d=$2; fi
s=$EPOCHREALTIME
sleep "$d"
e=$EPOCHREALTIME
echo "$1 $v $WORKER_LABEL $s $e" >> "$RECORDS"
