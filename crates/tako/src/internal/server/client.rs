use crate::internal::common::resources::{ResourceRequest, ResourceRequestVariants, ResourceRqId};

use crate::gateway::{
    ResourceRequestVariants as ClientResourceRequestVariants, SharedTaskConfiguration, TaskSubmit,
};

use crate::internal::common::resources::request::ResourceAllocRequest;
use crate::internal::server::comm::CommSender;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::on_new_tasks;
use crate::internal::server::task::{Task, TaskConfiguration};
use std::rc::Rc;

fn create_task_configuration(core: &mut Core, msg: SharedTaskConfiguration) -> TaskConfiguration {
    TaskConfiguration {
        time_limit: msg.time_limit,
        user_priority: msg.priority,
        crash_limit: msg.crash_limit,
        data_flags: msg.data_flags,
        body: msg.body,
    }
}

pub(crate) fn handle_new_tasks(
    core: &mut Core,
    comm: &mut CommSender,
    task_submit: TaskSubmit,
) -> crate::Result<()> {
    log::debug!("Client sends {} tasks", task_submit.tasks.len());
    if task_submit.tasks.is_empty() {
        return Ok(());
    }

    let configurations: Vec<_> = task_submit
        .shared_data
        .into_iter()
        .map(|c| Rc::new(create_task_configuration(core, c)))
        .collect();

    let mut tasks: Vec<Task> = Vec::with_capacity(task_submit.tasks.len());
    for task in task_submit.tasks {
        if core.is_used_task_id(task.id) {
            return Err(format!("Task id={} is already taken", task.id).into());
        }
        let idx = task.shared_data_index as usize;
        if idx >= configurations.len() {
            return Err(format!("Invalid configuration index {idx}").into());
        }
        let conf = &configurations[idx];
        let mut task = Task::new(
            task.id,
            task.resource_rq_id,
            task.task_deps,
            task.dataobj_deps,
            task.entry,
            conf.clone(),
        );
        task.scheduler_priority = -(task.id.job_id().as_num() as i32);
        tasks.push(task);
    }
    if !task_submit.adjust_instance_id_and_crash_counters.is_empty() {
        for task in &mut tasks {
            if let Some((instance_id, crash_counter)) = task_submit
                .adjust_instance_id_and_crash_counters
                .get(&task.id)
            {
                task.instance_id = *instance_id;
                task.crash_counter = *crash_counter;
            }
        }
    }
    on_new_tasks(core, comm, tasks);
    Ok(())
}
