use std::rc::Rc;
use tako::internal::server::core::Core;
use tako::internal::server::task::{Task, TaskConfiguration};
use tako::internal::server::worker::Worker;
use tako::resources::{
    ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, CPU_RESOURCE_NAME,
};
use tako::worker::ServerLostPolicy;
use tako::worker::WorkerConfiguration;
use tako::ItemId;
use tako::{TaskId, WorkerId};

pub fn create_task(id: TaskId) -> Task {
    let conf = TaskConfiguration {
        resources: Default::default(),
        user_priority: 0,
        time_limit: None,
        n_outputs: 0,
        crash_limit: 5,
    };
    Task::new(
        id,
        Default::default(),
        Rc::new(conf),
        Default::default(),
        false,
        false,
    )
}
pub fn create_worker(id: u64) -> Worker {
    Worker::new(
        WorkerId::new(id as u32),
        WorkerConfiguration {
            resources: ResourceDescriptor::new(vec![ResourceDescriptorItem {
                name: CPU_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::simple_indices(1),
            }]),
            listen_address: "".to_string(),
            hostname: "".to_string(),
            work_dir: Default::default(),
            log_dir: Default::default(),
            heartbeat_interval: Default::default(),
            send_overview_interval: None,
            idle_timeout: None,
            time_limit: None,
            on_server_lost: ServerLostPolicy::Stop,
            extra: Default::default(),
        },
        Default::default(),
    )
}

pub fn add_tasks(core: &mut Core, count: usize) -> Vec<TaskId> {
    let mut tasks = Vec::with_capacity(count);
    for id in 0..count {
        let task_id = TaskId::new(id as <TaskId as ItemId>::IdType);
        let task = create_task(task_id);
        core.add_task(task);
        tasks.push(task_id);
    }
    tasks
}
