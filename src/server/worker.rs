use crate::server::worker::WorkerState::Offline;
use crate::transfer::messages::WorkerInfo;
use crate::WorkerId;
use chrono::{DateTime, Utc};
use tako::messages::common::WorkerConfiguration;

pub enum WorkerState {
    Online,
    Offline(DateTime<Utc>),
}

pub struct Worker {
    worker_id: WorkerId,
    state: WorkerState,
    configuration: WorkerConfiguration,
}

impl Worker {
    pub fn new(worker_id: WorkerId, configuration: WorkerConfiguration) -> Self {
        Worker {
            worker_id,
            configuration,
            state: WorkerState::Online,
        }
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub fn configuration(&self) -> &WorkerConfiguration {
        &self.configuration
    }

    pub fn set_offline_state(&mut self) {
        self.state = Offline(Utc::now());
    }

    pub fn make_info(&self) -> WorkerInfo {
        WorkerInfo {
            id: self.worker_id,
            configuration: self.configuration.clone(),
            ended_at: match self.state {
                WorkerState::Online => None,
                Offline(d) => Some(d),
            },
        }
    }
}
