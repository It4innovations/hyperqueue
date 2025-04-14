use crate::hwstats::GpuFamily;
use crate::internal::common::resources::ResourceDescriptor;
use crate::internal::common::Map;
use crate::Set;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerLostPolicy {
    Stop,
    FinishRunning,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OverviewConfiguration {
    /// How often should overview be gathered.
    /// If `None`, worker overview is currently disabled.
    pub send_interval: Option<Duration>,
    /// GPU families to monitor
    pub gpu_families: Set<GpuFamily>,
}

impl OverviewConfiguration {
    pub fn disabled() -> Self {
        Self {
            send_interval: None,
            gpu_families: Default::default(),
        }
    }

    /// Returns true if the user explicitly enabled overview for this worker.
    pub fn is_overview_enabled(&self) -> bool {
        self.send_interval.is_some()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerConfiguration {
    pub resources: ResourceDescriptor,

    pub listen_address: String,
    pub hostname: String,
    pub group: String,
    pub work_dir: PathBuf,
    pub heartbeat_interval: Duration,
    pub overview_configuration: OverviewConfiguration,
    pub idle_timeout: Option<Duration>,
    pub time_limit: Option<Duration>,
    pub on_server_lost: ServerLostPolicy,
    pub max_parallel_downloads: u32,
    pub max_download_tries: u32,
    pub wait_between_download_tries: Duration,

    pub extra: Map<String, String>,
}

/// This function is used from both the server and the worker to keep the same values
/// in the worker configuration without the need for repeated configuration exchange.
pub(crate) fn sync_worker_configuration(
    configuration: &mut WorkerConfiguration,
    server_idle_timeout: Option<Duration>,
) {
    if configuration.idle_timeout.is_none() {
        configuration.idle_timeout = server_idle_timeout;
    }
}
