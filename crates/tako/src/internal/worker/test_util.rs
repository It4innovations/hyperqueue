use crate::internal::common::resources::{Allocation, ResourceRequest};
use crate::internal::common::Map;
use crate::internal::messages::worker::ComputeTaskMsg;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::tests::utils::resources::cpus_compact;
use crate::internal::worker::rqueue::ResourceWaitQueue;
use crate::internal::worker::state::TaskMap;
use crate::internal::worker::task::Task;
use crate::{InstanceId, Priority, TaskId, WorkerId};
use std::time::Duration;

pub struct WorkerTaskBuilder {
    task_id: TaskId,
    instance_id: InstanceId,
    resources: ResourceRequest,
    user_priority: Priority,
    server_priority: Priority,
}

impl WorkerTaskBuilder {
    pub fn new<T: Into<TaskId>>(task_id: T) -> Self {
        WorkerTaskBuilder {
            task_id: task_id.into(),
            instance_id: 0.into(),
            resources: cpus_compact(1).finish(),
            user_priority: 0,
            server_priority: 0,
        }
    }

    pub fn resources(mut self, resources: ResourceRequest) -> Self {
        self.resources = resources;
        self
    }

    pub fn user_priority(mut self, priority: Priority) -> Self {
        self.user_priority = priority;
        self
    }

    pub fn server_priority(mut self, priority: Priority) -> Self {
        self.server_priority = priority;
        self
    }

    pub fn build(self) -> Task {
        Task::new(ComputeTaskMsg {
            id: self.task_id,
            instance_id: self.instance_id,
            user_priority: self.user_priority,
            scheduler_priority: self.server_priority,
            resources: self.resources,
            time_limit: None,
            n_outputs: 0,
            node_list: vec![],
            body: Default::default(),
        })
    }
}

pub fn worker_task<T: Into<TaskId>>(
    task_id: T,
    resources: ResourceRequest,
    u_priority: Priority,
) -> Task {
    WorkerTaskBuilder::new(task_id)
        .resources(resources)
        .user_priority(u_priority)
        .build()
}

pub(crate) struct ResourceQueueBuilder {
    task_map: TaskMap,
    pub(crate) queue: ResourceWaitQueue,
}

impl ResourceQueueBuilder {
    pub fn new(queue: ResourceWaitQueue) -> Self {
        Self {
            task_map: Default::default(),
            queue,
        }
    }

    pub fn add_task(&mut self, task: Task) {
        self.queue.add_task(&task);
        self.task_map.insert(task);
    }

    pub fn new_worker(&mut self, worker_id: WorkerId, wr: WorkerResources) {
        self.queue.new_worker(worker_id, wr);
    }

    pub fn start_tasks(&mut self) -> Map<u64, Allocation> {
        self.queue
            .try_start_tasks(&self.task_map, None)
            .into_iter()
            .map(|(t, a)| (t.as_num() as u64, a))
            .collect()
    }

    pub fn start_tasks_duration(&mut self, duration: Duration) -> Map<u64, Allocation> {
        self.queue
            .try_start_tasks(&self.task_map, Some(duration))
            .into_iter()
            .map(|(t, a)| (t.as_num() as u64, a))
            .collect()
    }
}

impl From<ResourceWaitQueue> for ResourceQueueBuilder {
    fn from(queue: ResourceWaitQueue) -> Self {
        Self::new(queue)
    }
}
