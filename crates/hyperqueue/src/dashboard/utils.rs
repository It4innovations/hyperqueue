use tako::hwstats::{MemoryStats, WorkerHwStateMessage};

pub fn get_memory_usage_pct(memory_stats: &MemoryStats) -> u64 {
    if memory_stats.total == 0 {
        return 0;
    }
    (((memory_stats.total - memory_stats.free) as f64 / (memory_stats.total as f64)) * 100.00)
        as u64
}

pub fn get_average_cpu_usage_for_worker(hw_state: &WorkerHwStateMessage) -> f64 {
    let num_cpus = hw_state
        .state
        .cpu_usage
        .cpu_per_core_percent_usage
        .len()
        .max(1);
    let cpu_usage_sum_per_core = hw_state
        .state
        .cpu_usage
        .cpu_per_core_percent_usage
        .iter()
        .copied()
        .reduce(|cpu_a, cpu_b| (cpu_a + cpu_b))
        .unwrap_or(0.0) as f64;
    cpu_usage_sum_per_core / num_cpus as f64
}

pub fn calculate_average(items: &[f64]) -> f64 {
    items.iter().sum::<f64>() / (items.len().max(1)) as f64
}
