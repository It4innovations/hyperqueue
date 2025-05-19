use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskFailInfo {
    pub message: String,
}

impl TaskFailInfo {
    pub fn from_string(message: String) -> Self {
        TaskFailInfo { message }
    }
}
