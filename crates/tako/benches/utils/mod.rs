use tako::common::resources::ResourceDescriptor;
use tako::messages::common::{TaskFailInfo, WorkerConfiguration};
use tako::messages::gateway::LostWorkerReason;
use tako::messages::worker::ToWorkerMessage;
use tako::server::comm::Comm;
use tako::server::core::Core;
use tako::server::task::TaskRef;
use tako::server::worker::Worker;
use tako::{TaskId, WorkerId};

pub fn create_task(id: u64) -> TaskRef {
    TaskRef::new(id.into(), vec![], Default::default(), 0, false, false)
}
pub fn create_worker(id: u64) -> Worker {
    Worker::new(
        WorkerId::new(id as u32),
        WorkerConfiguration {
            resources: ResourceDescriptor {
                cpus: vec![],
                generic: vec![],
            },
            listen_address: "".to_string(),
            hostname: "".to_string(),
            work_dir: Default::default(),
            log_dir: Default::default(),
            heartbeat_interval: Default::default(),
            hw_state_poll_interval: None,
            idle_timeout: None,
            time_limit: None,
            extra: Default::default(),
        },
        Default::default(),
    )
}

pub fn add_tasks(core: &mut Core, count: usize) -> Vec<TaskRef> {
    let mut tasks = Vec::with_capacity(count);
    for id in 0..count {
        let task = create_task(id as u64);
        core.add_task(task.clone());
        tasks.push(task);
    }
    tasks
}

pub struct NullComm;

impl Comm for NullComm {
    fn send_worker_message(&mut self, _worker_id: WorkerId, _message: &ToWorkerMessage) {}

    fn broadcast_worker_message(&mut self, _message: &ToWorkerMessage) {}

    fn ask_for_scheduling(&mut self) {}

    fn send_client_task_finished(&mut self, _task_id: TaskId) {}

    fn send_client_task_started(&mut self, _task_id: TaskId, _worker_id: WorkerId) {}

    fn send_client_task_error(
        &mut self,
        _task_id: TaskId,
        _consumers_id: Vec<TaskId>,
        _error_info: TaskFailInfo,
    ) {
    }

    fn send_client_worker_new(
        &mut self,
        _worker_id: WorkerId,
        _configuration: &WorkerConfiguration,
    ) {
    }

    fn send_client_worker_lost(
        &mut self,
        _worker_id: WorkerId,
        _running_tasks: Vec<TaskId>,
        _reason: LostWorkerReason,
    ) {
    }
}
