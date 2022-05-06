use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tako::worker::WorkerConfiguration;

pub const WORKER_EXTRA_MANAGER_KEY: &str = "JobManager";

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ManagerType {
    Pbs,
    Slurm,
}

impl Display for ManagerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerType::Pbs => f.write_str("PBS"),
            ManagerType::Slurm => f.write_str("SLURM"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManagerInfo {
    pub manager: ManagerType,
    pub allocation_id: String,
    /// Time that remains until the job ends
    pub time_limit: Option<Duration>,
}

impl ManagerInfo {
    pub fn new(manager: ManagerType, job_id: String, time_limit: Option<Duration>) -> Self {
        Self {
            manager,
            allocation_id: job_id,
            time_limit,
        }
    }
}

pub trait GetManagerInfo {
    fn get_manager_info(&self) -> Option<ManagerInfo>;
}

impl GetManagerInfo for WorkerConfiguration {
    fn get_manager_info(&self) -> Option<ManagerInfo> {
        self.extra
            .get(WORKER_EXTRA_MANAGER_KEY)
            .and_then(|info| serde_json::from_str(info).ok())
    }
}
