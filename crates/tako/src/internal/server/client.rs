use crate::internal::common::resources::{ResourceRequest, ResourceRequestVariants};
use tokio::sync::mpsc::UnboundedSender;

use crate::gateway::{SharedTaskConfiguration, TaskSubmit};

use crate::internal::common::resources::request::ResourceRequestEntry;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::server::comm::{Comm, CommSender, CommSenderRef};
use crate::internal::server::core::{Core, CoreRef};
use crate::internal::server::reactor::{on_cancel_tasks, on_new_tasks};
use crate::internal::server::task::{Task, TaskConfiguration, TaskRuntimeState};
use crate::internal::server::worker::DEFAULT_WORKER_OVERVIEW_INTERVAL;
use crate::{InstanceId, Map, TaskId};
use std::rc::Rc;

fn create_task_configuration(
    core_ref: &mut Core,
    msg: SharedTaskConfiguration,
) -> TaskConfiguration {
    let resources = ResourceRequestVariants::new(
        msg.resources
            .variants
            .into_iter()
            .map(|rq| {
                ResourceRequest::new(
                    rq.n_nodes,
                    rq.min_time,
                    rq.resources
                        .into_iter()
                        .map(|r| {
                            let resource_id = core_ref.get_or_create_resource_id(&r.resource);
                            ResourceRequestEntry {
                                resource_id,
                                request: r.policy,
                            }
                        })
                        .collect(),
                )
            })
            .collect(),
    );

    TaskConfiguration {
        resources,
        time_limit: msg.time_limit,
        user_priority: msg.priority,
        crash_limit: msg.crash_limit,
        data_flags: msg.data_flags,
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

    for cfg in &configurations {
        if let Err(e) = cfg.resources.validate() {
            return Err(format!("Invalid task request {e:?}").into());
        }
    }

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
            task.task_deps,
            task.dataobj_deps,
            conf.clone(),
            task.body,
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
