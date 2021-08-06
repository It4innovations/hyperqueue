use crate::messages::common::{CpuStats, MemoryStats, NetworkStats};
use psutil::cpu::CpuPercentCollector;
use psutil::network::NetIoCountersCollector;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct HwSampler {
    cpu_percent_collector: CpuPercentCollector,
    net_io_counters_collector: NetIoCountersCollector,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct WorkerHwState {
    pub worker_cpu_usage: CpuStats,
    pub worker_memory_usage: MemoryStats,
    pub worker_network_usage: NetworkStats,
    pub timestamp: u64,
}

impl HwSampler {
    pub fn init() -> Result<Self, psutil::Error> {
        Ok(Self {
            cpu_percent_collector: CpuPercentCollector::new()?,
            net_io_counters_collector: Default::default(),
        })
    }
    pub fn fetch_hw_state(&mut self) -> Result<WorkerHwState, psutil::Error> {
        let cpu_usage = self.cpu_percent_collector.cpu_percent_percpu()?;
        let memory_usage = psutil::memory::virtual_memory()?;
        let net_io_counters = self.net_io_counters_collector.net_io_counters()?;
        let mut timestamp: u64 = 0;
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(time) => timestamp = time.as_secs(),
            Err(err) => {
                log::warn!("unable to read time on worker: {:?}", err)
            }
        }
        Ok(WorkerHwState {
            worker_cpu_usage: CpuStats {
                cpu_per_core_percent_usage: cpu_usage,
            },
            worker_memory_usage: MemoryStats {
                total: memory_usage.total(),
                free: memory_usage.available(),
            },
            worker_network_usage: NetworkStats {
                rx_bytes: net_io_counters.bytes_recv(),
                tx_bytes: net_io_counters.bytes_sent(),
                rx_packets: net_io_counters.packets_recv(),
                tx_packets: net_io_counters.packets_sent(),
                rx_errors: net_io_counters.err_in(),
                tx_errors: net_io_counters.err_out(),
            },
            timestamp,
        })
    }
}
