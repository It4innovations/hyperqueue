use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;
use tako::Map;

use tako::gateway::LostWorkerReason;
use tako::worker::WorkerConfiguration;
use tako::WorkerId;

use crate::common::manager::info::{GetManagerInfo, ManagerInfo, ManagerType};
use crate::common::rpc::{initiate_request, make_rpc_queue, ResponseToken, RpcSender};
use crate::common::serverdir::ServerDir;
use crate::server::autoalloc::process::autoalloc_process;
use crate::server::autoalloc::state::AutoAllocState;
use crate::server::autoalloc::{Allocation, QueueId};
use crate::server::state::StateRef;
use crate::transfer::messages::{AllocationQueueParams, QueueData};
use crate::JobId;

#[derive(Debug)]
pub enum AutoAllocMessage {
    // Events
    WorkerConnected(WorkerId, ManagerInfo),
    WorkerLost(WorkerId, ManagerInfo, LostWorkerDetails),
    JobCreated(JobId),
    // Requests
    GetQueues(ResponseToken<Map<QueueId, QueueData>>),
    AddQueue {
        server_directory: PathBuf,
        manager: ManagerType,
        params: AllocationQueueParams,
        response: ResponseToken<anyhow::Result<QueueId>>,
    },
    RemoveQueue {
        id: QueueId,
        force: bool,
        response: ResponseToken<anyhow::Result<()>>,
    },
    PauseQueue {
        id: QueueId,
        response: ResponseToken<anyhow::Result<()>>,
    },
    ResumeQueue {
        id: QueueId,
        response: ResponseToken<anyhow::Result<()>>,
    },
    GetAllocations(QueueId, ResponseToken<anyhow::Result<Vec<Allocation>>>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LostWorkerDetails {
    pub reason: LostWorkerReason,
    pub lifetime: Duration,
}

pub struct AutoAllocService {
    sender: RpcSender<AutoAllocMessage>,
}

impl AutoAllocService {
    pub fn on_worker_connected(&self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        if let Some(manager_info) = configuration.get_manager_info() {
            self.send(AutoAllocMessage::WorkerConnected(worker_id, manager_info));
        }
    }
    pub fn on_worker_lost(
        &self,
        id: WorkerId,
        configuration: &WorkerConfiguration,
        details: LostWorkerDetails,
    ) {
        if let Some(manager_info) = configuration.get_manager_info() {
            self.send(AutoAllocMessage::WorkerLost(id, manager_info, details));
        }
    }

    pub fn on_job_created(&self, job_id: JobId) {
        self.send(AutoAllocMessage::JobCreated(job_id));
    }

    pub fn get_queues(&self) -> impl Future<Output = Map<QueueId, QueueData>> {
        let fut = initiate_request(|token| self.sender.send(AutoAllocMessage::GetQueues(token)));
        async move { fut.await.unwrap() }
    }

    pub fn add_queue(
        &self,
        server_dir: &ServerDir,
        manager: ManagerType,
        params: AllocationQueueParams,
    ) -> impl Future<Output = anyhow::Result<QueueId>> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::AddQueue {
                server_directory: server_dir.directory().to_path_buf(),
                manager,
                params,
                response: token,
            })
        });
        async move { fut.await.unwrap() }
    }
    pub fn remove_queue(
        &self,
        id: QueueId,
        force: bool,
    ) -> impl Future<Output = anyhow::Result<()>> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::RemoveQueue {
                id,
                force,
                response: token,
            })
        });
        async move { fut.await.unwrap() }
    }
    pub fn pause_queue(&self, id: QueueId) -> impl Future<Output = anyhow::Result<()>> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::PauseQueue {
                id,
                response: token,
            })
        });
        async move { fut.await.unwrap() }
    }
    pub fn resume_queue(&self, id: QueueId) -> impl Future<Output = anyhow::Result<()>> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::ResumeQueue {
                id,
                response: token,
            })
        });
        async move { fut.await.unwrap() }
    }

    pub fn get_allocations(
        &self,
        id: QueueId,
    ) -> impl Future<Output = anyhow::Result<Vec<Allocation>>> {
        let fut = initiate_request(|token| {
            self.sender
                .send(AutoAllocMessage::GetAllocations(id, token))
        });
        async move { fut.await.unwrap() }
    }

    fn send(&self, msg: AutoAllocMessage) {
        let _ = self.sender.send(msg);
    }
}

pub fn create_autoalloc_service(
    state_ref: StateRef,
) -> (AutoAllocService, impl Future<Output = ()>) {
    let (tx, rx) = make_rpc_queue();
    let autoalloc = AutoAllocState::new();
    let process = autoalloc_process(state_ref, autoalloc, rx);
    let service = AutoAllocService { sender: tx };
    (service, process)
}
