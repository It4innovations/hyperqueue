use std::path::PathBuf;
use std::time::Duration;

use bstr::BString;
use serde::{Deserialize, Serialize};

use crate::common::resources::ResourceDescriptor;
use crate::common::Map;
use crate::worker::state::ServerLostPolicy;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskFailInfo {
    pub message: String,

    /*    #[serde(default)]
    #[serde(skip_serializing_if = "String::is_empty")]*/
    pub data_type: String,

    #[serde(with = "serde_bytes")]
    /*    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]*/
    pub error_data: Vec<u8>,
}

impl TaskFailInfo {
    pub fn from_string(message: String) -> Self {
        TaskFailInfo {
            message,
            data_type: Default::default(),
            error_data: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum StdioDef {
    Null,
    File(PathBuf),
    Pipe,
}

impl StdioDef {
    pub fn map_filename<F>(self, f: F) -> StdioDef
    where
        F: FnOnce(PathBuf) -> PathBuf,
    {
        match self {
            StdioDef::Null => StdioDef::Null,
            StdioDef::File(filename) => StdioDef::File(f(filename)),
            StdioDef::Pipe => StdioDef::Pipe,
        }
    }
}

impl Default for StdioDef {
    fn default() -> Self {
        StdioDef::Null
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgramDefinition {
    pub args: Vec<BString>,

    #[serde(default)]
    pub env: Map<BString, BString>,

    #[serde(default)]
    pub stdout: StdioDef,

    #[serde(default)]
    pub stderr: StdioDef,

    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub stdin: Vec<u8>,

    #[serde(default)]
    pub cwd: PathBuf,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerConfiguration {
    pub resources: ResourceDescriptor,

    pub listen_address: String,
    pub hostname: String,
    pub work_dir: PathBuf,
    pub log_dir: PathBuf,
    pub heartbeat_interval: Duration,
    pub send_overview_interval: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub time_limit: Option<Duration>,
    pub on_server_lost: ServerLostPolicy,

    pub extra: Map<String, String>,
}

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
