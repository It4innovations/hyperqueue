use super::resources::ResBuilder;
use crate::datasrv::DataObjectId;
use crate::gateway::{CrashLimit, TaskDataFlags};
use crate::internal::common::resources::map::GlobalResourceMapping;
use crate::internal::common::resources::{
    NumOfNodes, ResourceAmount, ResourceId, ResourceRequestVariants,
};
use crate::internal::messages::worker::TaskRunningMsg;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::get_or_create_raw_resource_rq_id;
use crate::internal::server::task::{Task, TaskConfiguration};
use crate::resources::{ResourceRequest, ResourceRqId};
use crate::tests::utils::env::TestComm;
use crate::{ResourceVariantId, Set, TaskId, UserPriority};
use smallvec::SmallVec;
use std::rc::Rc;
use thin_vec::ThinVec;

#[derive(Clone)]
pub struct TaskBuilder {
    task_deps: Set<TaskId>,
    data_deps: ThinVec<DataObjectId>,
    finished_resources: Vec<ResourceRequest>,
    resources_builder: ResBuilder,
    user_priority: UserPriority,
    crash_limit: CrashLimit,
    data_flags: TaskDataFlags,
}

impl TaskBuilder {
    pub fn new() -> TaskBuilder {
        TaskBuilder {
            task_deps: Default::default(),
            data_deps: Default::default(),
            finished_resources: vec![],
            resources_builder: Default::default(),
            user_priority: 0.into(),
            crash_limit: CrashLimit::default(),
            data_flags: TaskDataFlags::empty(),
        }
    }

    pub fn user_priority<P: Into<UserPriority>>(mut self, value: P) -> TaskBuilder {
        self.user_priority = value.into();
        self
    }

    pub fn task_deps(mut self, deps: &[TaskId]) -> TaskBuilder {
        self.task_deps = deps.iter().copied().collect();
        self
    }

    pub fn data_dep(mut self, task_id: TaskId, data_id: u32) -> TaskBuilder {
        self.task_deps.insert(task_id);
        self.data_deps
            .push(DataObjectId::new(task_id, data_id.into()));
        self
    }

    pub fn next_variant(mut self) -> TaskBuilder {
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

    pub fn cpus<A: Into<ResourceAmount>>(mut self, count: A) -> TaskBuilder {
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

    pub fn build_resource_rq_id(&self, core: &mut Core) -> ResourceRqId {
        let last_resource = self.resources_builder.clone().finish();
        let mut resources: SmallVec<[ResourceRequest; 1]> =
            self.finished_resources.iter().cloned().collect();

        resources.push(last_resource);
        for rq in &resources {
            rq.validate().unwrap();
        }
        let resources = ResourceRequestVariants::new(resources);
        let (rq_id, _) =
            get_or_create_raw_resource_rq_id(core, &mut TestComm::default(), resources);
        rq_id
    }

    pub fn build(&self, task_id: TaskId, core: &mut Core) -> Task {
        let rq_id = self.build_resource_rq_id(core);
        Task::new(
            task_id.into(),
            rq_id,
            self.task_deps.iter().copied().collect(),
            self.data_deps.clone(),
            None,
            Rc::new(TaskConfiguration {
                time_limit: None,
                user_priority: self.user_priority,
                crash_limit: self.crash_limit,
                data_flags: self.data_flags,
                body: Rc::new([]),
            }),
        )
    }
}

pub fn task_running_msg(task_id: TaskId) -> TaskRunningMsg {
    TaskRunningMsg {
        id: task_id,
        rv_id: ResourceVariantId::new(0),
        context: Default::default(),
    }
}
