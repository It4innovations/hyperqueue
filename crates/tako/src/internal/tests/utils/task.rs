use super::resources::ResBuilder;
use crate::datasrv::{DataId, DataObjectId};
use crate::gateway::TaskDataFlags;
use crate::internal::common::resources::{
    NumOfNodes, ResourceAmount, ResourceId, ResourceRequestVariants,
};
use crate::internal::messages::worker::TaskRunningMsg;
use crate::internal::server::task::{Task, TaskConfiguration};
use crate::resources::ResourceRequest;
use crate::{Priority, Set, TaskId};
use smallvec::SmallVec;
use std::rc::Rc;
use thin_vec::ThinVec;

pub struct TaskBuilder {
    id: TaskId,
    task_deps: Set<TaskId>,
    data_deps: ThinVec<DataObjectId>,
    finished_resources: Vec<ResourceRequest>,
    resources_builder: ResBuilder,
    user_priority: Priority,
    crash_limit: u32,
    data_flags: TaskDataFlags,
}

impl TaskBuilder {
    pub fn new<T: Into<TaskId>>(id: T) -> TaskBuilder {
        TaskBuilder {
            id: id.into(),
            task_deps: Default::default(),
            data_deps: Default::default(),
            finished_resources: vec![],
            resources_builder: Default::default(),
            user_priority: 0,
            crash_limit: 5,
            data_flags: TaskDataFlags::empty(),
        }
    }

    pub fn user_priority(mut self, value: Priority) -> TaskBuilder {
        self.user_priority = value;
        self
    }

    pub fn simple_deps(mut self, deps: &[&Task]) -> TaskBuilder {
        self.task_deps = deps.iter().map(|&tr| tr.id).collect();
        self
    }

    pub fn task_deps(mut self, deps: &[&Task]) -> TaskBuilder {
        self.task_deps = deps.iter().map(|&tr| tr.id).collect();
        self
    }

    pub fn data_dep(mut self, task: &Task, data_id: DataId) -> TaskBuilder {
        self.task_deps.insert(task.id);
        self.data_deps.push(DataObjectId::new(task.id, data_id));
        self
    }

    pub fn next_resources(mut self) -> TaskBuilder {
        self.finished_resources
            .push(self.resources_builder.finish());
        self.resources_builder = ResBuilder::default();
        self
    }

    pub fn resources(mut self, resources: ResBuilder) -> TaskBuilder {
        self.resources_builder = resources;
        self
    }

    pub fn n_nodes(mut self, count: NumOfNodes) -> TaskBuilder {
        self.resources_builder = self.resources_builder.n_nodes(count);
        self
    }

    pub fn cpus_compact<A: Into<ResourceAmount>>(mut self, count: A) -> TaskBuilder {
        self.resources_builder = self.resources_builder.cpus(count);
        self
    }

    pub fn time_request(mut self, time_s: u64) -> TaskBuilder {
        self.resources_builder = self.resources_builder.min_time_secs(time_s);
        self
    }

    pub fn add_resource<Id: Into<ResourceId>, A: Into<ResourceAmount>>(
        mut self,
        id: Id,
        amount: A,
    ) -> TaskBuilder {
        self.resources_builder = self.resources_builder.add(id, amount);
        self
    }

    pub fn build(self) -> Task {
        let last_resource = self.resources_builder.finish();
        let mut resources: SmallVec<[ResourceRequest; 1]> = self.finished_resources.into();
        resources.push(last_resource);
        for rq in &resources {
            rq.validate().unwrap();
        }
        let resources = ResourceRequestVariants::new(resources);
        Task::new(
            self.id,
            self.task_deps.into_iter().collect(),
            self.data_deps,
            Rc::new(TaskConfiguration {
                resources,
                time_limit: None,
                user_priority: self.user_priority,
                crash_limit: self.crash_limit,
                data_flags: self.data_flags,
            }),
            Default::default(),
        )
    }
}

pub fn task<T: Into<TaskId>>(id: T) -> Task {
    TaskBuilder::new(id.into()).build()
}

pub fn task_with_deps<T: Into<TaskId>>(id: T, deps: &[&Task]) -> Task {
    TaskBuilder::new(id.into()).simple_deps(deps).build()
}

pub fn task_running_msg<T: Into<TaskId>>(task_id: T) -> TaskRunningMsg {
    TaskRunningMsg {
        id: task_id.into(),
        context: Default::default(),
    }
}
