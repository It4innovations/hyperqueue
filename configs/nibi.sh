#!/usr/bin/env bash
set -euo pipefail

# Register once against a running HQ server built from this fork.
# Account selection is supplied by the caller, not stored in this config.

# Every task must request its exact worker/* class. In Nextflow, route CPU
# tasks <=748.GB to worker/cpu and tasks >748.GB to worker/cpuLarge.
# GPU tasks additionally request gpus=N (Nextflow's accelerator directive).
# GPU queues reserve one GPU/MIG: keep SLURM's CUDA_VISIBLE_DEVICES
# or ROCR_VISIBLE_DEVICES rather than overriding its selected device.
# CPU task classes are absent from every GPU queue, and vice versa.
# No resources are autodetected; all capacities below are explicit.
# In this fork, new allocations use the first tier strictly
# above HQ's --time-request: 3, 12, 24, 72, 168h. Existing workers can accept
# any task that fits. Set --time-request explicitly; --time-limit is separate.

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
    add_queue cpu "$hours" 192 766000 'worker/cpu=sum(192)' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=192 --threads-per-core=1 --mem=766000M --exclusive

    add_queue cpu-large "$hours" 192 6144000 'worker/cpuLarge=sum(192)' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=192 --threads-per-core=1 --mem=6144000M --exclusive

    add_queue mi300a "$hours" 24 126750 'worker/mi300a=[0]' \
        --resource 'gpus=[0]' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=24 --threads-per-core=1 --mem=126750M --gres=gpu:mi300a:1

    add_queue h100 "$hours" 14 256000 'worker/h100=[0]' \
        --resource 'gpus=[0]' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=14 --threads-per-core=1 --mem=256000M \
        --gres=gpu:nvidia_h100_80gb_hbm3:1

    add_queue h100-mig10 "$hours" 2 31744 'worker/h100mig10=[0]' \
        --resource 'gpus=[0]' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=2 --threads-per-core=1 --mem=31744M \
        --gres=gpu:nvidia_h100_80gb_hbm3_1g.10gb:1

    add_queue h100-mig20 "$hours" 4 63488 'worker/h100mig20=[0]' \
        --resource 'gpus=[0]' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=4 --threads-per-core=1 --mem=63488M \
        --gres=gpu:nvidia_h100_80gb_hbm3_2g.20gb:1

    add_queue h100-mig40 "$hours" 6 126976 'worker/h100mig40=[0]' \
        --resource 'gpus=[0]' \
        -- --account="$SLURM_ACCOUNT" --ntasks-per-node=1 \
        --cpus-per-task=6 --threads-per-core=1 --mem=126976M \
        --gres=gpu:nvidia_h100_80gb_hbm3_3g.40gb:1
done
