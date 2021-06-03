use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::common::resources::ResourceDescriptor;
use crate::common::Map;
use crate::TaskTypeId;

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
    pub env: Map<String, String>,

    #[serde(default)]
    pub stdout: Option<PathBuf>,

    #[serde(default)]
    pub stderr: Option<PathBuf>,

    #[serde(default)]
    pub cwd: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkerConfiguration {
    pub resources: ResourceDescriptor,

    pub listen_address: String,
    pub hostname: String,
    pub work_dir: PathBuf,
    pub log_dir: PathBuf,
    pub heartbeat_interval: Duration,

    pub extra: Map<String, String>,
}
