#![cfg(test)]

use crate::common::resources::ResourceRequest;
use crate::messages::common::TaskConfiguration;
use crate::messages::worker::ComputeTaskMsg;
use crate::worker::task::TaskRef;
use crate::{Priority, TaskId};

pub fn worker_task<T: Into<TaskId>>(
    task_id: T,
    resources: ResourceRequest,
    u_priority: Priority,
) -> TaskRef {
    TaskRef::new(ComputeTaskMsg {
        id: task_id.into(),
        instance_id: 0,
        dep_info: vec![],
        user_priority: u_priority,
        scheduler_priority: 0,
        configuration: TaskConfiguration {
            resources,
            n_outputs: 0,
            time_limit: None,
            body: vec![],
        },
    })
}
