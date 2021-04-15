use crate::messages::common::WorkerConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorkerMsg {
    pub configuration: WorkerConfiguration,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum GenericMessage {
    RegisterWorker(RegisterWorkerMsg),
}
