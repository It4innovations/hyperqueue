use tako::messages::common::MemoryStats;
use tako::messages::worker::WorkerHwStateMessage;

pub fn calculate_memory_usage_percent(memory_stats: &MemoryStats) -> u64 {
    if memory_stats.total == 0 {
        return 0;
    }
    (((memory_stats.total - memory_stats.free) as f64 / (memory_stats.total as f64)) * 100.00)
        as u64
}

pub fn get_average_cpu_usage_for_worker(hw_state: &WorkerHwStateMessage) -> f32 {
    let num_cpus = hw_state
        .state
        .worker_cpu_usage
        .cpu_per_core_percent_usage
        .len();
    let cpu_usage_sum_per_core = hw_state
        .state
        .worker_cpu_usage
        .cpu_per_core_percent_usage
        .iter()
        .copied()
        .reduce(|cpu_a, cpu_b| (cpu_a + cpu_b))
        .unwrap_or(0.0);
    (cpu_usage_sum_per_core / num_cpus as f32) as f32
}
