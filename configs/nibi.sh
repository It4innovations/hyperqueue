#!/usr/bin/env bash
set -euo pipefail

# Register once against a running HQ server built from this fork.
# Account selection is supplied by the caller, not stored in this config.

# Every task must request its exact worker/* class. Regular CPU tasks request
# worker/cpu; all five base CPU sizes share this class.
# Large-memory tasks request worker/cpuLarge; all three large sizes share it.
# New large allocations require a single task requesting strictly over 766000 MiB.
# GPU tasks additionally request gpus=N (Nextflow's accelerator directive).
# GPU queues reserve one GPU/MIG: keep SLURM's CUDA_VISIBLE_DEVICES
# or ROCR_VISIBLE_DEVICES rather than overriding its selected device.
# CPU task classes are absent from every GPU queue, and vice versa.
# No resources are autodetected; all capacities below are explicit.
# In this fork, new allocations use the first tier strictly
# above HQ's --time-request: 3, 12, 24, 72, 168h. Existing workers can accept
# any task that fits. Set --time-request explicitly; --time-limit is separate.
# CPU groups prefer full, half, quarter, eighth, then sixteenth allocations.
# Each smaller size has one shared queued/running worker across all five tiers.
# Each worker type also shares its total and queued allocation caps across all tiers.
# All CPU sizes except base eighth require 50% requested CPU demand before allocation.
# All workers stop after five idle minutes; connected workers accept any fitting task.

add_queue() {
    local name=$1 hours=$2 cpus=$3 memory_mib=$4 class_pool=$5
    shift 5

    hq alloc add slurm \
        --no-dry-run \
        --name "${name}-${hours}h" \
        --time-limit "${hours}h" \
        --max-workers-per-alloc 1 \
        --detect-resources none \
        --cpus "$cpus" \
        --resource "mem=sum(${memory_mib})" \
        --resource "$class_pool" \
        "$@"
}

for hours in 3 12 24 72 168; do
    add_queue cpu_base "$hours" 192 766000 'worker/cpu=sum(192)' \
        --group cpu_base --max-worker-count 100 --backlog 25 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=192 --threads-per-core=1 --mem=766000M --exclusive

    add_queue cpu_base_half "$hours" 96 383000 'worker/cpu=sum(96)' \
        --group cpu_base_half --max-worker-count 1 --backlog 1 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=96 --threads-per-core=1 --mem=383000M

    add_queue cpu_base_quarter "$hours" 48 191500 'worker/cpu=sum(48)' \
        --group cpu_base_quarter --max-worker-count 1 --backlog 1 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=48 --threads-per-core=1 --mem=191500M

    add_queue cpu_base_eighth "$hours" 24 95750 'worker/cpu=sum(24)' \
        --group cpu_base_eighth --max-worker-count 1 --backlog 1 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=24 --threads-per-core=1 --mem=95750M

    add_queue cpu_base_sixteenth "$hours" 12 47875 'worker/cpu=sum(12)' \
        --group cpu_base_sixteenth --max-worker-count 1 --backlog 1 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=12 --threads-per-core=1 --mem=47875M

    add_queue cpu_large "$hours" 192 6144000 'worker/cpuLarge=sum(192)' \
        --group cpu_large --max-worker-count 4 --backlog 2 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=192 --threads-per-core=1 --mem=6144000M --exclusive

    add_queue cpu_large_half "$hours" 96 3072000 'worker/cpuLarge=sum(96)' \
        --group cpu_large_half --max-worker-count 1 --backlog 1 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=96 --threads-per-core=1 --mem=3072000M

    add_queue cpu_large_quarter "$hours" 48 1536000 'worker/cpuLarge=sum(48)' \
        --group cpu_large_quarter --max-worker-count 1 --backlog 1 \
        --allocation-min-utilization 0.5 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=48 --threads-per-core=1 --mem=1536000M

    add_queue mi300a "$hours" 24 126750 'worker/mi300a=[0]' \
        --resource 'gpus=[0]' \
        --group mi300a --max-worker-count 8 --backlog 4 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=24 --threads-per-core=1 --mem=126750M --gres=gpu:mi300a:1

    add_queue h100_full "$hours" 14 256000 'worker/h100=[0]' \
        --resource 'gpus=[0]' \
        --group h100_full --max-worker-count 16 --backlog 8 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=14 --threads-per-core=1 --mem=256000M \
        --gres=gpu:nvidia_h100_80gb_hbm3:1

    add_queue h100_1g.10gb "$hours" 2 31744 'worker/h100mig10=[0]' \
        --resource 'gpus=[0]' \
        --group h100_1g.10gb --max-worker-count 48 --backlog 24 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=2 --threads-per-core=1 --mem=31744M \
        --gres=gpu:nvidia_h100_80gb_hbm3_1g.10gb:1

    add_queue h100_2g.20gb "$hours" 4 63488 'worker/h100mig20=[0]' \
        --resource 'gpus=[0]' \
        --group h100_2g.20gb --max-worker-count 24 --backlog 12 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=4 --threads-per-core=1 --mem=63488M \
        --gres=gpu:nvidia_h100_80gb_hbm3_2g.20gb:1

    add_queue H100-3g.40gb "$hours" 6 126976 'worker/h100mig40=[0]' \
        --resource 'gpus=[0]' \
        --group H100-3g.40gb --max-worker-count 24 --backlog 12 --idle-timeout 5m \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=6 --threads-per-core=1 --mem=126976M \
        --gres=gpu:nvidia_h100_80gb_hbm3_3g.40gb:1
done
