use std::rc::Rc;
use tako::common::resources::ResourceDescriptor;
use tako::messages::common::WorkerConfiguration;
use tako::server::core::Core;
use tako::server::task::{TaskConfiguration, TaskRef};
use tako::server::worker::Worker;
use tako::WorkerId;

pub fn create_task(id: u64) -> TaskRef {
    let conf = TaskConfiguration {
        resources: Default::default(),
        user_priority: 0,
        time_limit: None,
        n_outputs: 0,
    };
    TaskRef::new(
        id.into(),
        vec![],
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
            resources: ResourceDescriptor {
                cpus: vec![vec![1.into()]],
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
