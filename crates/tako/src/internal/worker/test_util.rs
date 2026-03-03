use crate::datasrv::DataObjectId;
use crate::gateway::TaskDataFlags;
use crate::internal::common::Map;
use crate::internal::common::resources::map::ResourceRqMap;
use crate::internal::common::resources::{Allocation, ResourceRequest, ResourceRequestVariants};
use crate::internal::messages::worker::{ComputeTaskSeparateData, ComputeTaskSharedData};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::tests::utils::resources::cpus_compact;
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
    priority: Priority,
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
            priority: Priority::new(0),
            data_deps: Vec::new(),
            data_flags: TaskDataFlags::empty(),
            task_state: TaskState::Waiting {
                waiting_data_objects: 0,
            },
        }
    }
    pub fn resources(mut self, resources: ResourceRequest) -> Self {
        self.resources.push(resources);
        self
    }

    pub fn priority<P: Into<Priority>>(mut self, priority: P) -> Self {
        self.priority = priority.into();
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
                resource_rq_variant: 0.into(),
                priority: self.priority,
                node_list: vec![],
                data_deps: self.data_deps,
                entry: None,
            },
            ComputeTaskSharedData {
                time_limit: None,
                data_flags: self.data_flags,
                body: Default::default(),
            },
            self.task_state,
        )
    }
}
