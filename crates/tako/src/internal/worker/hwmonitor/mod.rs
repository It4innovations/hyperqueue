use crate::Set;
use crate::hwstats::{CpuStats, GpuFamily, MemoryStats, NetworkStats, WorkerHwState};
use std::time::SystemTime;
use sysinfo::{CpuRefreshKind, Networks, RefreshKind, System};

mod amd;
mod nvidia;

#[derive(Debug)]
pub(crate) struct HwSampler {
    system: System,
    networks: Networks,
    gpu_families: Set<GpuFamily>,
}

impl HwSampler {
    pub fn init(gpu_families: Set<GpuFamily>) -> Self {
        let system = System::new_with_specifics(
            RefreshKind::nothing().with_cpu(CpuRefreshKind::nothing().with_cpu_usage()),
        );
        let networks = Networks::new_with_refreshed_list();
        Self {
            system,
            networks,
            gpu_families,
        }
    }

    pub fn fetch_hw_state(&mut self) -> WorkerHwState {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.networks.refresh(false);

        let cpu_usage: Vec<f32> = self.system.cpus().iter().map(|c| c.cpu_usage()).collect();
        let rx_bytes: u64 = self.networks.values().map(|d| d.total_received()).sum();
        let tx_bytes: u64 = self.networks.values().map(|d| d.total_transmitted()).sum();
        let rx_packets: u64 = self
            .networks
            .values()
            .map(|d| d.total_packets_received())
            .sum();
        let tx_packets: u64 = self
            .networks
            .values()
            .map(|d| d.total_packets_transmitted())
            .sum();
        let rx_errors: u64 = self
            .networks
            .values()
            .map(|d| d.total_errors_on_received())
            .sum();
        let tx_errors: u64 = self
            .networks
            .values()
            .map(|d| d.total_errors_on_transmitted())
            .sum();

        let mut timestamp: u64 = 0;
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(time) => timestamp = time.as_secs(),
            Err(err) => {
                log::warn!("unable to read time on worker: {err:?}")
            }
        }

        let nvidia_gpus = if self.gpu_families.contains(&GpuFamily::Nvidia) {
            match nvidia::get_nvidia_gpu_state() {
                Ok(state) => Some(state),
                Err(error) => {
                    log::error!("Failed to fetch NVIDIA GPU state: {error:?}");
                    None
                }
            }
        } else {
            None
        };
        let amd_gpus = if self.gpu_families.contains(&GpuFamily::Amd) {
            match amd::get_amd_gpu_state() {
                Ok(state) => Some(state),
                Err(error) => {
                    log::error!("Failed to fetch AMD GPU state: {error:?}");
                    None
                }
            }
        } else {
            None
        };

        WorkerHwState {
            cpu_usage: CpuStats {
                cpu_per_core_percent_usage: cpu_usage,
            },
            memory_usage: MemoryStats {
                total: self.system.total_memory(),
                free: self.system.available_memory(),
            },
            network_usage: NetworkStats {
                rx_bytes,
                tx_bytes,
                rx_packets,
                tx_packets,
                rx_errors,
                tx_errors,
            },
            nvidia_gpus,
            amd_gpus,
            timestamp,
        }
    }
}
