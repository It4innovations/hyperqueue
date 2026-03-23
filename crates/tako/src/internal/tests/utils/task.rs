use super::resources::ResBuilder;
use crate::gateway::CrashLimit;
use crate::internal::common::resources::{
    NumOfNodes, ResourceAmount, ResourceId, ResourceRequestVariants,
};
use crate::internal::messages::worker::{
    ComputeTaskSeparateData, ComputeTaskSharedData, ComputeTasksMsg, TaskRunningMsg,
};
use crate::internal::server::core::Core;
use crate::internal::server::reactor::get_or_create_raw_resource_rq_id;
use crate::internal::server::task::{Task, TaskConfiguration};
use crate::resources::{ResourceRequest, ResourceRqId, ResourceRqMap};
use crate::tests::utils::env::TestComm;
use crate::{Priority, ResourceVariantId, Set, TaskId, UserPriority};
use smallvec::SmallVec;
use std::rc::Rc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct TaskBuilder {
    task_deps: Set<TaskId>,
    finished_resources: Vec<ResourceRequest>,
    resources_builder: ResBuilder,
    user_priority: UserPriority,
    crash_limit: CrashLimit,
    time_limit: Option<Duration>,
}

impl TaskBuilder {
    pub fn new() -> TaskBuilder {
        TaskBuilder {
            task_deps: Default::default(),
            finished_resources: vec![],
            resources_builder: Default::default(),
            user_priority: 0.into(),
            crash_limit: CrashLimit::default(),
            time_limit: None,
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

    pub fn next_variant(mut self) -> TaskBuilder {
        self.finished_resources
            .push(self.resources_builder.finish());
        self.resources_builder = ResBuilder::default();
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

    #[allow(dead_code)]
    pub fn time_limit(mut self, time_s: u64) -> TaskBuilder {
        self.time_limit = Some(Duration::from_secs(time_s));
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

    pub fn build_resource_rq_id_into_map(
        &self,
        resource_rq_map: &mut ResourceRqMap,
    ) -> ResourceRqId {
        let rqv = self.build_rqv();
        resource_rq_map.get_or_create(rqv)
    }

    pub fn build_rqv(&self) -> ResourceRequestVariants {
        let last_resource = self.resources_builder.clone().finish();
        let mut resources: SmallVec<[ResourceRequest; 1]> =
            self.finished_resources.iter().cloned().collect();

        resources.push(last_resource);
        for rq in &resources {
            rq.validate().unwrap();
        }
        ResourceRequestVariants::new(resources)
    }

    pub fn build_resource_rq_id(&self, core: &mut Core) -> ResourceRqId {
        let rqv = self.build_rqv();
        let (rq_id, _) = get_or_create_raw_resource_rq_id(core, &mut TestComm::default(), rqv);
        rq_id
    }

    pub fn build(&self, task_id: TaskId, core: &mut Core) -> Task {
        let rq_id = self.build_resource_rq_id(core);
        Task::new(
            task_id.into(),
            rq_id,
            self.task_deps.iter().copied().collect(),
            None,
            Rc::new(TaskConfiguration {
                time_limit: None,
                user_priority: self.user_priority,
                crash_limit: self.crash_limit,
                body: Rc::new([]),
            }),
        )
    }

    pub fn build_compute_msg(
        &self,
        task_id: TaskId,
        variant: ResourceVariantId,
        resource_rq_map: &mut ResourceRqMap,
    ) -> ComputeTasksMsg {
        let rq_id = self.build_resource_rq_id_into_map(resource_rq_map);
        ComputeTasksMsg {
            tasks: vec![ComputeTaskSeparateData {
                shared_index: 0,
                id: task_id,
                resource_rq_id: rq_id,
                resource_rq_variant: variant.into(),
                instance_id: Default::default(),
                priority: Priority::from_user_priority(self.user_priority),
                node_list: vec![],
                entry: None,
            }],
            shared_data: vec![ComputeTaskSharedData {
                time_limit: self.time_limit,
                body: Default::default(),
            }],
        }
    }
}

pub fn task_running_msg(task_id: TaskId) -> TaskRunningMsg {
    TaskRunningMsg {
        task_id: task_id,
        rv_id: ResourceVariantId::new(0),
        context: Default::default(),
    }
}
