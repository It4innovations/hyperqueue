use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct CpuStats {
    pub cpu_per_core_percent_usage: Vec<f32>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct MemoryStats {
    pub total: u64,
    pub free: u64,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct NetworkStats {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub rx_errors: u64,
    pub tx_errors: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GpuStats {
    pub id: String,
    pub processor_usage: f32,
    pub mem_usage: f32,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct GpuCollectionStats {
    pub gpus: Vec<GpuStats>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct WorkerHwState {
    pub cpu_usage: CpuStats,
    pub memory_usage: MemoryStats,
    pub network_usage: NetworkStats,
    pub nvidia_gpus: Option<GpuCollectionStats>,
    pub timestamp: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WorkerHwStateMessage {
    pub state: WorkerHwState,
}

#[derive(Deserialize, Serialize, Hash, Debug, PartialEq, Eq, Copy, Clone)]
pub enum GpuFamily {
    Nvidia,
    Amd,
}
