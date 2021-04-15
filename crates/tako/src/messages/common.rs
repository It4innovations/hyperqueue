use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::common::Map;
use crate::TaskTypeId;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskFailInfo {
    pub message: String,

    #[serde(default)]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub data_type: String,

    #[serde(with = "serde_bytes")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SubworkerKind {
    Stateless,
    Stateful,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubworkerDefinition {
    pub id: TaskTypeId,
    pub kind: SubworkerKind,
    pub program: ProgramDefinition,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgramDefinition {
    pub args: Vec<String>,

    #[serde(default)]
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub env: Map<String, String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<PathBuf>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerConfiguration {
    pub n_cpus: u32,

    pub listen_address: String,
    pub hostname: String,
    pub work_dir: PathBuf,
    pub log_dir: PathBuf,
    pub heartbeat_interval: Duration,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra: Vec<(String, String)>,
}
