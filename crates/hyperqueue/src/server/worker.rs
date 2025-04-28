use chrono::{DateTime, Utc};
use tako::gateway::{LostWorkerReason, WorkerRuntimeInfo};
use tako::worker::WorkerConfiguration;

use crate::server::worker::WorkerState::Offline;
use crate::transfer::messages::{TaskTimestamp, WorkerExitInfo, WorkerInfo};
use tako::{TaskId, WorkerId};

#[derive(Default)]
pub struct ConnectedWorkerData {
    pub started_at: DateTime<Utc>,
}

pub enum WorkerState {
    Online(ConnectedWorkerData),
    Offline {
        connected: ConnectedWorkerData,
        exit_info: WorkerExitInfo,
    },
}

pub struct Worker {
    worker_id: WorkerId,
    state: WorkerState,
    last_task_started: Option<TaskTimestamp>,
    pub(crate) configuration: WorkerConfiguration,
}

impl Worker {
    pub fn new(worker_id: WorkerId, configuration: WorkerConfiguration) -> Self {
        Worker {
            worker_id,
            configuration,
            state: WorkerState::Online(ConnectedWorkerData {
                started_at: Utc::now(),
            }),
            last_task_started: None,
        }
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub fn configuration(&self) -> &WorkerConfiguration {
        &self.configuration
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        match &self.state {
            WorkerState::Online(connected) | Offline { connected, .. } => connected.started_at,
        }
    }

    pub fn update_task_started(&mut self, task_id: TaskId, now: DateTime<Utc>) {
        self.last_task_started = Some(TaskTimestamp { task_id, time: now });
    }

    pub fn set_offline_state(&mut self, reason: LostWorkerReason) {
        match self.state {
            WorkerState::Online(ref mut connected) => {
                self.state = Offline {
                    connected: std::mem::take(connected),
                    exit_info: WorkerExitInfo {
                        ended_at: Utc::now(),
                        reason,
                    },
                };
            }
            Offline { .. } => {
                panic!(
                    "Setting offline state of a worker {} that is already offline",
                    self.worker_id
                );
            }
        }
    }

    pub fn is_running(&self) -> bool {
        matches!(self.state, WorkerState::Online(_))
    }

    pub fn make_info(&self, runtime_info: Option<WorkerRuntimeInfo>) -> WorkerInfo {
        WorkerInfo {
            runtime_info,
            id: self.worker_id,
            configuration: self.configuration.clone(),
            started: self.started_at(),
            ended: match &self.state {
                WorkerState::Online(_) => None,
                Offline { exit_info, .. } => Some(exit_info.clone()),
            },
            last_task_started: self.last_task_started.clone(),
        }
    }
}
