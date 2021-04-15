use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub address: String,
    pub ncpus: u32,
    pub heartbeat_interval: u32, // number of ms between each heartbeat
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum GenericMessage {
    RegisterWorker(RegisterWorkerMsg),
}
