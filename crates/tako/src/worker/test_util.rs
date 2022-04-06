use crate::common::resources::{ResourceAllocation, ResourceRequest};
use crate::common::Map;
use crate::messages::worker::ComputeTaskMsg;
use crate::worker::rqueue::ResourceWaitQueue;
use crate::worker::state::TaskMap;
use crate::worker::task::Task;
use crate::{Priority, TaskId};
use std::time::Duration;

pub fn worker_task<T: Into<TaskId>>(
    task_id: T,
    resources: ResourceRequest,
    u_priority: Priority,
) -> Task {
    Task::new(ComputeTaskMsg {
        id: task_id.into(),
        instance_id: 0.into(),
        user_priority: u_priority,
        scheduler_priority: 0,
        resources,
        time_limit: None,
        n_outputs: 0,
        node_list: vec![],
        body: vec![],
    })
}

pub struct ResourceQueueBuilder {
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

    pub fn start_tasks(&mut self) -> Map<u64, ResourceAllocation> {
        self.queue
            .try_start_tasks(&self.task_map, None)
            .into_iter()
            .map(|(t, a)| (t.as_num() as u64, a))
            .collect()
    }

    pub fn start_tasks_duration(&mut self, duration: Duration) -> Map<u64, ResourceAllocation> {
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
