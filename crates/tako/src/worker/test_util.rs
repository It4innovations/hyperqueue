#![cfg(test)]

use crate::common::resources::ResourceRequest;
use crate::messages::worker::ComputeTaskMsg;
use crate::worker::task::TaskRef;
use crate::{Priority, TaskId};

pub fn worker_task(task_id: TaskId, resources: ResourceRequest, u_priority: Priority) -> TaskRef {
    TaskRef::new(ComputeTaskMsg {
        id: task_id,
        type_id: 0,
        n_outputs: 0,
        dep_info: vec![],
        spec: vec![],
        user_priority: u_priority,
        scheduler_priority: 0,
        resources,
    })
}
