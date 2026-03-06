use crate::gateway::ResourceRequestVariants as ClientResourceRequestVariants;
use crate::gateway::{CrashLimit, LostWorkerReason};
use crate::internal::common::resources::ResourceRqId;
use crate::internal::common::{Map, Set};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    NewWorkerMsg, RetractResponseMsg, TaskFinishedMsg, TaskIdsMsg, TaskRunningMsg, ToWorkerMessage,
};
use crate::internal::scheduler::TaskQueue;
use crate::internal::server::comm::Comm;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::task::ComputeTasksBuilder;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::{Worker, WorkerAssignment};
use crate::internal::server::workermap::WorkerMap;
use crate::{TaskId, WorkerId};
use std::fmt::Write;

pub(crate) fn on_new_worker(core: &mut Core, comm: &mut impl Comm, worker: Worker) {
    comm.broadcast_worker_message(&ToWorkerMessage::NewWorker(NewWorkerMsg {
        worker_id: worker.id,
        address: worker.configuration.listen_address.clone(),
        resources: worker.resources.to_transport(),
    }));

    comm.client()
        .on_worker_new(worker.id, &worker.configuration);

    comm.ask_for_scheduling();
    core.new_worker(worker);
}

pub(crate) fn on_remove_worker(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    reason: LostWorkerReason,
) {
    log::debug!("Removing worker {worker_id}");

    let mut running_tasks = Vec::new();

    let worker = core.remove_worker(worker_id);
    let CoreSplitMut {
        task_map,
        task_queues,
        worker_map,
        ..
    } = core.split_mut();
    match worker.assignment() {
        WorkerAssignment::Sn(sn) => {
            for task_id in &sn.assign_tasks {
                let task = task_map.get_task_mut(*task_id);
                if task.is_sn_running() {
                    running_tasks.push(*task_id);
                }
                task.increment_instance_id();
                task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                task_queues.add_ready_task(&task);
            }
        }
        WorkerAssignment::Mn(mn) => {
            running_tasks.push(mn.task_id);
            let task = task_map.get_task_mut(mn.task_id);
            task.increment_instance_id();
            match &mut task.state {
                TaskRuntimeState::RunningMultiNode(ws) => {
                    if worker_id == ws[0] {
                        // Root
                        for worker_id in &ws[1..] {
                            let worker = worker_map.get_worker_mut(*worker_id);
                            worker.reset_mn_task();
                        }
                        task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                        task_queues.add_ready_task(&task);
                    } else {
                        // Non-Root
                        ws.retain(|&x| x != worker_id);
                    }
                }
                _ => unreachable!(),
            };
        }
    }

    //core.reset_stealing_tasks_on_worker(worker_id);

    log::debug!("Running tasks on lost worker {worker_id}: {running_tasks:?}");

    comm.broadcast_worker_message(&ToWorkerMessage::LostWorker(worker_id));

    // IMPORTANT: We need to announce lost worker before failing the jobs
    // so in journal restoration we can detect what tasks were running
    // without explicit logging
    comm.client()
        .on_worker_lost(worker_id, &running_tasks, reason);

    for task_id in running_tasks {
        let task = core.get_task_mut(task_id);
        if CrashLimit::NeverRestart == task.configuration.crash_limit {
            log::debug!("Task {task_id} with never restart flag crashed");
            let error_info = TaskFailInfo {
                message: "Task was running on a lost worker while never restart flag was set."
                    .to_string(),
            };
            fail_task_helper(core, comm, None, task_id, error_info);
        } else if reason.is_failure() && task.increment_crash_counter() {
            let count = task.crash_counter;
            log::debug!("Task {task_id} reached crash limit {count}");
            let error_info = TaskFailInfo {
                message: format!(
                    "Task was running on a worker that was lost; the task has occurred {count} times in this situation and limit was reached."
                ),
            };
            fail_task_helper(core, comm, None, task_id, error_info);
        }
    }

    comm.ask_for_scheduling();
}

pub(crate) fn on_new_tasks(core: &mut Core, comm: &mut impl Comm, new_tasks: Vec<Task>) {
    assert!(!new_tasks.is_empty());
    for mut task in new_tasks.into_iter() {
        let mut count = 0;
        task.task_deps.retain(|t| {
            if let Some(task_dep) = core.find_task_mut(*t) {
                task_dep.add_consumer(task.id);
                if !task_dep.is_finished() {
                    count += 1;
                }
                true
            } else {
                false
            }
        });
        assert!(matches!(
            task.state,
            TaskRuntimeState::Waiting { unfinished_deps: 0 }
        ));
        task.state = TaskRuntimeState::Waiting {
            unfinished_deps: count,
        };
        core.add_task(task);
    }
    comm.ask_for_scheduling()
}

pub(crate) fn on_task_running(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    message: TaskRunningMsg,
) {
    let TaskRunningMsg {
        task_id,
        rv_id,
        context,
    } = message;

    let CoreSplitMut {
        task_map,
        worker_map,
        request_map,
        ..
    } = core.split_mut();
    let simple_worker_list = &[worker_id];
    if let Some(task) = task_map.find_task_mut(task_id) {
        let worker_ids = match &task.state {
            TaskRuntimeState::Assigned {
                worker_id: w_id,
                rv_id: assigned_rv_id,
            } => {
                assert_eq!(*w_id, worker_id);
                assert_eq!(*assigned_rv_id, rv_id);
                task.state = TaskRuntimeState::Running { worker_id, rv_id };
                /*let rqv = requests.get(task.resource_rq_id);
                workers
                    .get_mut(&worker_id)
                    .unwrap()
                    .start_task(task_id, rqv.get(rv_id));*/
                simple_worker_list.as_slice()
            }
            TaskRuntimeState::Retracting { source: w_id } => {
                assert_eq!(*w_id, worker_id);
                comm.ask_for_scheduling();
                task.state = TaskRuntimeState::Running { worker_id, rv_id };
                // Check for task redirection
                todo!()
            }
            TaskRuntimeState::RunningMultiNode(ws) => {
                // We have received that multi node task is started,
                // Because we do not distinguish between assigned and running state
                // for multi node tasks.
                // (we are not overbooking multi-node tasks, assigned state is not interesting)
                // we already have this task in running state
                // So we do nothing here
                assert_eq!(ws[0], worker_id);
                ws.as_slice()
            }
            TaskRuntimeState::Running { .. }
            | TaskRuntimeState::Waiting { .. }
            | TaskRuntimeState::Finished => {
                unreachable!()
            }
        };

        comm.client()
            .on_task_started(task_id, task.instance_id, worker_ids, rv_id, context);
    }
}

fn reset_mn_task_workers(worker_map: &mut WorkerMap, workers: &[WorkerId], task_id: TaskId) {
    for w in workers {
        let worker = worker_map.get_worker_mut(*w);
        assert_eq!(worker.mn_assignment().unwrap().task_id, task_id);
        worker.reset_mn_task();
    }
}

pub(crate) fn on_task_finished(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    msg: TaskFinishedMsg,
) {
    let task_id = msg.task_id;
    let CoreSplitMut {
        task_map,
        worker_map,
        task_queues,
        request_map,
        ..
    } = core.split_mut();
    if let Some(task) = task_map.find_task_mut(task_id) {
        log::debug!("Task id={} finished on worker={}", task_id, worker_id,);
        assert!(task.is_assigned_at(worker_id));
        let rqv = request_map.get(task.resource_rq_id);

        match &task.state {
            TaskRuntimeState::Assigned {
                worker_id: w_id,
                rv_id,
            }
            | TaskRuntimeState::Running {
                worker_id: w_id,
                rv_id,
            } => {
                assert_eq!(*w_id, worker_id);
                worker_map
                    .get_worker_mut(worker_id)
                    .remove_sn_task(task_id, rqv.get(*rv_id));
            }
            TaskRuntimeState::RunningMultiNode(ws) => {
                assert_eq!(ws[0], worker_id);
                reset_mn_task_workers(worker_map, ws, task_id);
            }
            TaskRuntimeState::Retracting { .. }
            | TaskRuntimeState::Waiting { .. }
            | TaskRuntimeState::Finished => {
                unreachable!()
            }
        }

        task.state = TaskRuntimeState::Finished;
        comm.ask_for_scheduling();
        comm.client().on_task_finished(task_id);
    } else {
        log::debug!("Unknown task finished id={task_id}");
        return;
    }

    let consumers: Vec<TaskId> = {
        let task = task_map.get_task(task_id);
        task.get_consumers().iter().copied().collect()
    };

    for consumer in consumers {
        let t = task_map.get_task_mut(consumer);
        if t.decrease_unfinished_deps() {
            task_queues.add_ready_task(t);
            comm.ask_for_scheduling();
        }
    }
    let state = core.remove_task(msg.task_id);
    assert!(matches!(state, TaskRuntimeState::Finished));
}

fn fail_task_helper(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: Option<WorkerId>,
    task_id: TaskId,
    error_info: TaskFailInfo,
) {
    let consumers: Vec<TaskId> = {
        let CoreSplitMut {
            task_map,
            worker_map,
            request_map,
            ..
        } = core.split_mut();
        if let Some(task) = task_map.find_task(task_id) {
            log::debug!("Task task_id={task_id} failed");
            if let Some(worker_id) = worker_id {
                if request_map.get(task.resource_rq_id).is_multi_node() {
                    let ws = task.mn_placement().unwrap();
                    assert_eq!(ws[0], worker_id);
                    reset_mn_task_workers(worker_map, ws, task_id);
                } else {
                    match &task.state {
                        TaskRuntimeState::Assigned {
                            worker_id: w,
                            rv_id,
                        }
                        | TaskRuntimeState::Running {
                            worker_id: w,
                            rv_id,
                        } => {
                            assert_eq!(worker_id, *w);
                            let rqv = request_map.get(task.resource_rq_id);
                            worker_map
                                .get_worker_mut(worker_id)
                                .remove_sn_task(task_id, rqv.get(*rv_id));
                        }
                        _ => {}
                    }
                }
            } else {
                assert!(task.is_waiting())
            }
            let mut s = Set::new();
            task.collect_recursive_consumers(task_map, &mut s);
            s.into_iter().collect()
        } else {
            log::debug!("Unknown task failed");
            return;
        }
    };

    for &consumer in &consumers {
        log::debug!("Task={consumer} canceled because of failed dependency");
        assert!(matches!(
            core.remove_task(consumer),
            TaskRuntimeState::Waiting { .. }
        ));
    }
    let state = core.remove_task(task_id);
    if worker_id.is_some() {
        assert!(matches!(
            state,
            TaskRuntimeState::Assigned { .. }
                | TaskRuntimeState::Running { .. }
                | TaskRuntimeState::RunningMultiNode(_)
        ));
    } else {
        assert!(matches!(state, TaskRuntimeState::Waiting { .. }));
    }
    drop(state);
    comm.ask_for_scheduling();
    let cancel_ids = comm.client().on_task_error(task_id, consumers, error_info);
    if !cancel_ids.is_empty() {
        on_cancel_tasks(core, comm, &cancel_ids);
    }
}

pub(crate) fn on_task_error(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
    error_info: TaskFailInfo,
) {
    fail_task_helper(core, comm, Some(worker_id), task_id, error_info)
}

pub(crate) fn on_cancel_tasks(core: &mut Core, comm: &mut impl Comm, task_ids: &[TaskId]) {
    let mut to_unregister = Set::with_capacity(task_ids.len());
    let mut running_ids: Map<WorkerId, Vec<TaskId>> = Map::default();

    log::debug!("Canceling {} tasks", task_ids.len());

    let CoreSplitMut {
        task_map,
        worker_map,
        request_map,
        ..
    } = core.split_mut();
    for &task_id in task_ids {
        log::debug!("Canceling task id={task_id}");
        if let Some(task) = task_map.find_task(task_id) {
            to_unregister.insert(task_id);
            task.collect_recursive_consumers(task_map, &mut to_unregister);
            match task.state {
                TaskRuntimeState::Waiting { .. } => {}
                TaskRuntimeState::Assigned {
                    worker_id: w_id,
                    rv_id,
                }
                | TaskRuntimeState::Running {
                    worker_id: w_id,
                    rv_id,
                } => {
                    let rqv = request_map.get(task.resource_rq_id);
                    worker_map
                        .get_worker_mut(w_id)
                        .remove_sn_task(task_id, rqv.get(rv_id));
                    running_ids.entry(w_id).or_default().push(task_id);
                }
                TaskRuntimeState::RunningMultiNode(ref ws) => {
                    for w_id in ws {
                        worker_map.get_worker_mut(*w_id).reset_mn_task();
                    }
                    running_ids.entry(ws[0]).or_default().push(task_id);
                }
                TaskRuntimeState::Retracting { source } => {
                    todo!(); // Need to check that task is nto redirected
                    running_ids.entry(source).or_default().push(task_id);
                }
                TaskRuntimeState::Finished => unreachable!(),
            };
        } else {
            log::debug!("Task is not here");
        }
    }

    core.remove_tasks_batched(&to_unregister);
    for (w_id, ids) in running_ids {
        comm.send_worker_message(w_id, &ToWorkerMessage::CancelTasks(TaskIdsMsg { ids }));
    }
    comm.ask_for_scheduling();
}

pub(crate) fn get_or_create_resource_rq_id(
    core: &mut Core,
    comm: &mut impl Comm,
    rqv: &ClientResourceRequestVariants,
) -> (ResourceRqId, bool) {
    let map = core.resource_map_mut();
    let (rq_id, is_new) = map.get_or_create_resource_rq_id(rqv);
    if is_new {
        let msg = ToWorkerMessage::NewResourceRequest(
            rq_id,
            map.get_resource_rq_map().get(rq_id).clone(),
        );
        comm.broadcast_worker_message(&msg);
        core.task_queues_mut().add_task_queue();
    }
    (rq_id, is_new)
}

#[cfg(test)]
pub(crate) fn get_or_create_raw_resource_rq_id(
    core: &mut Core,
    comm: &mut impl Comm,
    rqv: crate::resources::ResourceRequestVariants,
) -> (ResourceRqId, bool) {
    let map = core.resource_map_mut();
    let (rq_id, is_new) = map.get_or_create_rq_id(rqv);
    if is_new {
        core.task_queues_mut().add_task_queue();
        let map = core.resource_map_mut();
        let msg = ToWorkerMessage::NewResourceRequest(
            rq_id,
            map.get_resource_rq_map().get(rq_id).clone(),
        );
        comm.broadcast_worker_message(&msg);
    }
    (rq_id, is_new)
}
