use crate::datasrv::DataObjectId;
use crate::gateway::TaskDataFlags;
use crate::internal::common::Map;
use crate::internal::common::resources::map::ResourceRqMap;
use crate::internal::common::resources::{Allocation, ResourceRequest, ResourceRequestVariants};
use crate::internal::messages::worker::{ComputeTaskSeparateData, ComputeTaskSharedData};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::tests::utils::resources::cpus_compact;
use crate::internal::worker::rqueue::ResourceWaitQueue;
use crate::internal::worker::state::TaskMap;
use crate::internal::worker::task::{Task, TaskState};
use crate::{InstanceId, Priority, TaskId, WorkerId};
use smallvec::smallvec;
use std::rc::Rc;
use std::time::Duration;

pub struct WorkerTaskBuilder {
    task_id: TaskId,
    instance_id: InstanceId,
    resources: Vec<ResourceRequest>,
    user_priority: Priority,
    server_priority: Priority,
    data_deps: Vec<DataObjectId>,
    data_flags: TaskDataFlags,
    task_state: TaskState,
}

impl WorkerTaskBuilder {
    pub fn new<T: Into<TaskId>>(task_id: T) -> Self {
        WorkerTaskBuilder {
            task_id: task_id.into(),
            instance_id: 0.into(),
            resources: Vec::new(),
            user_priority: 0,
            server_priority: 0,
            data_deps: Vec::new(),
            data_flags: TaskDataFlags::empty(),
            task_state: TaskState::Waiting(0),
        }
    }
    pub fn resources(mut self, resources: ResourceRequest) -> Self {
        self.resources.push(resources);
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

    pub fn build(self, requests: &mut ResourceRqMap) -> Task {
        let resources = ResourceRequestVariants::new(if self.resources.is_empty() {
            smallvec![cpus_compact(1).finish()]
        } else {
            self.resources.into()
        });
        let resource_rq_id = requests.get_or_create(resources.clone());

        Task::new(
            ComputeTaskSeparateData {
                resource_rq_id,
                shared_index: 0,
                id: self.task_id,
                instance_id: self.instance_id,
                scheduler_priority: self.server_priority,
                node_list: vec![],
                data_deps: self.data_deps,
                entry: None,
            },
            ComputeTaskSharedData {
                user_priority: self.user_priority,
                time_limit: None,
                data_flags: self.data_flags,
                body: Default::default(),
            },
            self.task_state,
        )
    }
}

pub fn worker_task_add<T: Into<TaskId>>(
    rbuilder: &mut ResourceQueueBuilder,
    resource_map: &mut ResourceRqMap,
    task_id: T,
    resources: ResourceRequest,
    u_priority: Priority,
) {
    let w = worker_task(task_id, resources, u_priority, resource_map);
    rbuilder.add_task(resource_map, w);
}

pub fn worker_task<T: Into<TaskId>>(
    task_id: T,
    resources: ResourceRequest,
    u_priority: Priority,
    requests: &mut ResourceRqMap,
) -> Task {
    WorkerTaskBuilder::new(task_id)
        .resources(resources)
        .user_priority(u_priority)
        .build(requests)
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

    pub fn add_task(&mut self, resource_map: &ResourceRqMap, task: Task) {
        self.queue.add_task(resource_map, &task);
        self.task_map.insert(task);
    }

    pub fn new_worker(
        &mut self,
        worker_id: WorkerId,
        wr: WorkerResources,
        resource_map: &ResourceRqMap,
    ) {
        self.queue.new_worker(worker_id, wr, resource_map);
    }

    pub fn start_tasks(&mut self, rqs: &ResourceRqMap) -> Map<u32, Rc<Allocation>> {
        self.queue
            .try_start_tasks(&self.task_map, rqs, None)
            .into_iter()
            .map(|(t, a, _)| (t.job_task_id().as_num(), a))
            .collect()
    }

    pub fn start_tasks_duration(
        &mut self,
        rqs: &ResourceRqMap,
        duration: Duration,
    ) -> Map<u32, Rc<Allocation>> {
        self.queue
            .try_start_tasks(&self.task_map, rqs, Some(duration))
            .into_iter()
            .map(|(t, a, _)| (t.job_task_id().as_num(), a))
            .collect()
    }
}

impl From<ResourceWaitQueue> for ResourceQueueBuilder {
    fn from(queue: ResourceWaitQueue) -> Self {
        Self::new(queue)
    }
}
