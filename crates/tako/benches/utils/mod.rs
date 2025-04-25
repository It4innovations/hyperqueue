use std::rc::Rc;
use std::time::Duration;
use tako::ItemId;
use tako::gateway::TaskDataFlags;
use tako::internal::server::core::Core;
use tako::internal::server::task::{Task, TaskConfiguration};
use tako::internal::server::worker::Worker;
use tako::internal::worker::configuration::OverviewConfiguration;
use tako::resources::{
    CPU_RESOURCE_NAME, ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
};
use tako::worker::ServerLostPolicy;
use tako::worker::WorkerConfiguration;
use tako::{TaskId, WorkerId};

pub fn create_task(id: TaskId) -> Task {
    let conf = TaskConfiguration {
        resources: Default::default(),
        user_priority: 0,
        time_limit: None,
        crash_limit: 5,
        data_flags: TaskDataFlags::empty(),
    };
    Task::new(
        id,
        Default::default(),
        Default::default(),
        Rc::new(conf),
        Default::default(),
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
            group: "default".to_string(),
            work_dir: Default::default(),
            heartbeat_interval: Default::default(),
            overview_configuration: OverviewConfiguration::default(),
            idle_timeout: None,
            time_limit: None,
            on_server_lost: ServerLostPolicy::Stop,
            max_parallel_downloads: 2,
            max_download_tries: 2,
            wait_between_download_tries: Duration::from_secs(1),
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
