use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tako::Map;
use tako::control::ServerRef;

use tako::WorkerId;
use tako::gateway::LostWorkerReason;
use tako::worker::WorkerConfiguration;

use crate::common::manager::info::{GetManagerInfo, ManagerInfo};
use crate::common::rpc::{ResponseToken, RpcSender, initiate_request, make_rpc_queue};
use crate::server::autoalloc::process::autoalloc_process;
use crate::server::autoalloc::state::AutoAllocState;
use crate::server::autoalloc::{Allocation, QueueId, QueueParameters};
use crate::server::event::streamer::EventStreamer;
use crate::transfer::messages::QueueData;
use tako::JobId;
use tako::resources::ResourceDescriptor;

#[derive(Debug)]
pub enum AutoAllocMessage {
    // Events
    WorkerConnected {
        id: WorkerId,
        config: WorkerConfiguration,
        manager_info: ManagerInfo,
    },
    WorkerLost(WorkerId, ManagerInfo, LostWorkerDetails),
    // Some tasks were submitted to a job
    JobSubmitted(JobId),
    // Requests
    GetQueues(ResponseToken<Map<QueueId, QueueData>>),
    AddQueue {
        server_directory: PathBuf,
        params: QueueParameters,
        queue_id: Option<QueueId>,
        worker_resources: Option<ResourceDescriptor>,
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
    QuitService,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LostWorkerDetails {
    pub reason: LostWorkerReason,
    pub lifetime: Duration,
}

#[derive(Clone)]
pub struct AutoAllocService {
    sender: RpcSender<AutoAllocMessage>,
}

impl AutoAllocService {
    pub fn quit_service(&self) {
        self.send(AutoAllocMessage::QuitService);
    }

    pub fn on_worker_connected(&self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        if let Some(manager_info) = configuration.get_manager_info() {
            self.send(AutoAllocMessage::WorkerConnected {
                id: worker_id,
                config: configuration.clone(),
                manager_info,
            });
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

    pub fn on_job_submit(&self, job_id: JobId) {
        self.send(AutoAllocMessage::JobSubmitted(job_id));
    }

    pub async fn get_queues(&self) -> Map<QueueId, QueueData> {
        let fut = initiate_request(|token| self.sender.send(AutoAllocMessage::GetQueues(token)));
        fut.await.unwrap()
    }

    pub async fn add_queue(
        &self,
        server_dir: &Path,
        params: QueueParameters,
        queue_id: Option<QueueId>,
        worker_resources: Option<ResourceDescriptor>,
    ) -> anyhow::Result<QueueId> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::AddQueue {
                server_directory: server_dir.to_path_buf(),
                params,
                queue_id,
                worker_resources,
                response: token,
            })
        });
        fut.await?
    }
    pub async fn remove_queue(&self, id: QueueId, force: bool) -> anyhow::Result<()> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::RemoveQueue {
                id,
                force,
                response: token,
            })
        });
        fut.await?
    }
    pub async fn pause_queue(&self, id: QueueId) -> anyhow::Result<()> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::PauseQueue {
                id,
                response: token,
            })
        });
        fut.await?
    }
    pub async fn resume_queue(&self, id: QueueId) -> anyhow::Result<()> {
        let fut = initiate_request(|token| {
            self.sender.send(AutoAllocMessage::ResumeQueue {
                id,
                response: token,
            })
        });
        fut.await?
    }

    pub async fn get_allocations(&self, id: QueueId) -> anyhow::Result<Vec<Allocation>> {
        let fut = initiate_request(|token| {
            self.sender
                .send(AutoAllocMessage::GetAllocations(id, token))
        });
        fut.await?
    }

    fn send(&self, msg: AutoAllocMessage) {
        let _ = self.sender.send(msg);
    }
}

pub fn create_autoalloc_service(
    server_ref: ServerRef,
    queue_id_initial_value: u32,
    events: EventStreamer,
) -> (AutoAllocService, impl Future<Output = ()>) {
    let (tx, rx) = make_rpc_queue();
    let autoalloc = AutoAllocState::new(queue_id_initial_value);
    let process = autoalloc_process(server_ref, events, autoalloc, rx);
    let service = AutoAllocService { sender: tx };
    (service, process)
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::common::rpc::{RpcReceiver, make_rpc_queue};
    use crate::server::autoalloc::AutoAllocService;
    use crate::server::autoalloc::service::AutoAllocMessage;

    pub fn test_alloc_service() -> (AutoAllocService, RpcReceiver<AutoAllocMessage>) {
        let (tx, rx) = make_rpc_queue();
        let service = AutoAllocService { sender: tx };
        (service, rx)
    }
}
