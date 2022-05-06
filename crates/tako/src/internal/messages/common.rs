use serde::{Deserialize, Serialize};

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
