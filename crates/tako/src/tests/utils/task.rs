use crate::common::resources::{
    CpuRequest, GenericResourceAmount, GenericResourceId, GenericResourceRequest, NumOfCpus,
    ResourceRequest,
};
use crate::messages::common::TaskConfiguration;
use crate::server::task::TaskRef;
use crate::TaskId;
use std::time::Duration;

pub struct TaskBuilder {
    id: TaskId,
    inputs: Vec<TaskRef>,
    n_outputs: u32,
    resources: ResourceRequest,
}

impl TaskBuilder {
    pub fn new<T: Into<TaskId>>(id: T) -> TaskBuilder {
        TaskBuilder {
            id: id.into(),
            inputs: Default::default(),
            n_outputs: 0,
            resources: Default::default(),
        }
    }

    pub fn deps(mut self, deps: &[&TaskRef]) -> TaskBuilder {
        self.inputs = deps.iter().map(|&tr| tr.clone()).collect();
        self
    }

    pub fn outputs(mut self, value: u32) -> TaskBuilder {
        self.n_outputs = value;
        self
    }

    pub fn cpus_compact(mut self, cpu_request: NumOfCpus) -> TaskBuilder {
        self.resources.set_cpus(CpuRequest::Compact(cpu_request));
        self
    }

    pub fn time_request(mut self, time: u64) -> TaskBuilder {
        self.resources.set_time(Duration::new(time, 0));
        self
    }

    pub fn generic_res<Id: Into<GenericResourceId>>(
        mut self,
        idx: Id,
        amount: GenericResourceAmount,
    ) -> TaskBuilder {
        self.resources.add_generic_request(GenericResourceRequest {
            resource: idx.into(),
            amount,
        });
        self
    }

    pub fn build(self) -> TaskRef {
        self.resources.validate().unwrap();
        TaskRef::new(
            self.id,
            self.inputs,
            TaskConfiguration {
                resources: self.resources,
                n_outputs: self.n_outputs,
                time_limit: None,
                body: Default::default(),
            },
            Default::default(),
            false,
            false,
        )
    }
}

pub fn task<T: Into<TaskId>>(id: T) -> TaskRef {
    TaskBuilder::new(id.into()).outputs(1).build()
}

pub fn task_with_deps<T: Into<TaskId>>(id: T, deps: &[&TaskRef], n_outputs: u32) -> TaskRef {
    TaskBuilder::new(id.into())
        .deps(deps)
        .outputs(n_outputs)
        .build()
}
