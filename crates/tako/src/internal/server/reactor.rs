use crate::gateway::{CrashLimit, LostWorkerReason};
use crate::gateway::{
    ResourceRequest, ResourceRequestVariants as ClientResourceRequestVariants,
    ResourceRequestVariants,
};
use crate::internal::common::resources::ResourceRqId;
use crate::internal::common::{Map, Set};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    NewWorkerMsg, RetractResponseMsg, TaskIdsMsg, TaskRunningMsg, TaskUpdates, ToWorkerMessage,
    WorkerTaskUpdate,
};
use crate::internal::scheduler::{SchedulerState, TaskQueue};
use crate::internal::server::comm::Comm;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::task::ComputeTasksBuilder;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::{Worker, WorkerAssignment};
use crate::internal::server::workermap::WorkerMap;
use crate::resources::ResourceRqMap;
use crate::{ResourceVariantId, TaskId, WorkerId};
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

fn process_retracted(
    task_map: &mut TaskMap,
    worker_map: &mut WorkerMap,
    comm: &mut impl Comm,
    retracted: Vec<TaskId>,
) {
    if !retracted.is_empty() {
        log::debug!("Retracting task: {:?}", retracted);
        let mut to_workers: Map<WorkerId, Vec<TaskId>> = Map::new();
        for task_id in retracted {
            let task = task_map.get_task_mut(task_id);
            let worker_id = match &task.state {
                TaskRuntimeState::Prefilled { worker_id } => *worker_id,
                _ => unreachable!(),
            };
            to_workers.entry(worker_id).or_default().push(task_id);
            task.state = TaskRuntimeState::Retracting { worker_id };
            worker_map
                .get_worker_mut(worker_id)
                .remove_prefill_task(task_id);
        }
        for (worker_id, task_ids) in to_workers {
            comm.send_worker_message(
                worker_id,
                &ToWorkerMessage::RetractTasks(TaskIdsMsg { ids: task_ids }),
            );
        }
    }
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
        scheduler_state,
        ..
    } = core.split_mut();
    let mut retracted = Vec::new();
    match worker.assignment() {
        WorkerAssignment::Sn(sn) => {
            for task_id in &sn.assign_tasks {
                let task = task_map.get_task_mut(*task_id);
                if task.is_sn_running() {
                    running_tasks.push(*task_id);
                    task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                } else if task.is_retracting() {
                    assert!(scheduler_state.redirects.remove(task_id).is_some());
                } else {
                    task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                }
                task.increment_instance_id();
                task_queues.add_ready_task(&task, &mut retracted);
            }
            for task_id in &sn.prefilled_tasks {
                let task = task_map.get_task_mut(*task_id);
                task.increment_instance_id();
                task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                task_queues
                    .get_mut(task.resource_rq_id)
                    .move_prefilled_task_to_ready(*task_id);
            }
        }
        WorkerAssignment::Mn(mn) => {
            let task = task_map.get_task_mut(mn.task_id);
            match &mut task.state {
                TaskRuntimeState::RunningMultiNode(ws) => {
                    if worker_id == ws[0] {
                        // Root
                        for worker_id in &ws[1..] {
                            let worker = worker_map.get_worker_mut(*worker_id);
                            worker.reset_mn_task();
                        }
                        task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                        running_tasks.push(mn.task_id);
                        task.increment_instance_id();
                        task_queues.add_ready_task(&task, &mut retracted);
                    } else {
                        // Non-Root
                        ws.retain(|&x| x != worker_id);
                    }
                }
                _ => unreachable!(),
            };
        }
    }

    for task in task_map.tasks_mut() {
        match &task.state {
            TaskRuntimeState::Retracting { worker_id: w_id } if worker_id == *w_id => {
                if let Some((target_id, rv_id)) = scheduler_state.redirects.remove(&task.id) {
                    task.state = TaskRuntimeState::Assigned {
                        worker_id: target_id,
                        rv_id,
                    };
                    comm.send_worker_message(
                        target_id,
                        &ComputeTasksBuilder::single_task(task, rv_id, Vec::new()),
                    );
                } else {
                    task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
                }
            }
            _ => {}
        }
    }

    process_retracted(task_map, worker_map, comm, retracted);
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
            task_failed(core, comm, None, task_id, error_info);
        } else if reason.is_failure() && task.increment_crash_counter() {
            let count = task.crash_counter;
            log::debug!("Task {task_id} reached crash limit {count}");
            let error_info = TaskFailInfo {
                message: format!(
                    "Task was running on a worker that was lost; the task has occurred {count} times in this situation and limit was reached."
                ),
            };
            task_failed(core, comm, None, task_id, error_info);
        }
    }

    comm.ask_for_scheduling();
}

pub(crate) fn on_new_tasks(core: &mut Core, comm: &mut impl Comm, new_tasks: Vec<Task>) {
    assert!(!new_tasks.is_empty());
    let mut retracted = Vec::new();
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
        core.add_task(task, &mut retracted);
    }
    let CoreSplitMut {
        task_map,
        worker_map,
        ..
    } = core.split_mut();
    process_retracted(task_map, worker_map, comm, retracted);
    comm.ask_for_scheduling()
}

enum NeedScheduling {
    No,
    Removed {
        rq_id: ResourceRqId,
        rv_id: ResourceVariantId,
    },
    Yes,
}

impl NeedScheduling {
    pub fn set_removed(&mut self, rq_id: ResourceRqId, rv_id: ResourceVariantId) {
        match &self {
            NeedScheduling::No => {
                *self = NeedScheduling::Removed { rq_id, rv_id };
            }
            _ => {
                *self = NeedScheduling::Yes;
            }
        }
    }

    pub fn set_yes(&mut self) {
        *self = NeedScheduling::Yes;
    }
}

pub(crate) fn on_task_update(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    updates: TaskUpdates,
) {
    let mut need_scheduling = false;
    /// This relies on the fact that when worker switching to prefill, it will send Finish, followed by Start
    /// And this cannot happen in any other way
    let is_prefill_update = updates.len() == 2
        && matches!(updates[0], WorkerTaskUpdate::Finished { .. })
        && matches!(updates[1], WorkerTaskUpdate::RunningPrefilled { .. });
    for update in updates {
        match update {
            WorkerTaskUpdate::Finished { task_id } => {
                need_scheduling |= task_finished(core, &mut *comm, worker_id, task_id);
            }
            WorkerTaskUpdate::Failed { task_id, info } => {
                task_failed(core, &mut *comm, Some(worker_id), task_id, info);
                need_scheduling = true;
            }
            WorkerTaskUpdate::Running(msg) | WorkerTaskUpdate::RunningPrefilled(msg) => {
                need_scheduling |= task_running(core, &mut *comm, worker_id, msg);
            }
            WorkerTaskUpdate::RejectRequest {
                task_id,
                rv_id: rv_id,
            } => {
                need_scheduling |= task_reject(core, &mut *comm, worker_id, task_id, rv_id);
            }
            WorkerTaskUpdate::EnableRequest {
                resource_rq_id: rq_id,
                rv_id: rv_id,
            } => {
                request_enabled(core, &mut *comm, worker_id, rq_id, rv_id);
                need_scheduling = true;
            }
        }
    }
    if need_scheduling && !is_prefill_update {
        comm.ask_for_scheduling();
    }
}

fn task_running(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    message: TaskRunningMsg,
) -> bool {
    let TaskRunningMsg {
        task_id,
        rv_id,
        context,
    } = message;

    let CoreSplitMut {
        task_map,
        worker_map,
        request_map,
        task_queues,
        scheduler_state,
        ..
    } = core.split_mut();
    let simple_worker_list = &[worker_id];
    let Some(task) = task_map.find_task_mut(task_id) else {
        return false;
    };
    let (worker_ids, need_scheduling) = match &task.state {
        TaskRuntimeState::Assigned {
            worker_id: w_id,
            rv_id: assigned_rv_id,
        } => {
            assert_eq!(*w_id, worker_id);
            assert_eq!(*assigned_rv_id, rv_id);
            task.state = TaskRuntimeState::Running { worker_id, rv_id };
            (simple_worker_list.as_slice(), false)
        }
        TaskRuntimeState::Prefilled { worker_id: w_id } => {
            assert_eq!(*w_id, worker_id);
            task.state = TaskRuntimeState::Running { worker_id, rv_id };
            let rqv = request_map.get(task.resource_rq_id);
            worker_map
                .get_worker_mut(worker_id)
                .task_from_prefilled_to_started(task_id, rqv.get(rv_id));
            task_queues
                .get_mut(task.resource_rq_id)
                .remove(task.id, task.priority());
            (simple_worker_list.as_slice(), false)
        }
        TaskRuntimeState::Retracting { worker_id: w_id } => {
            assert_eq!(*w_id, worker_id);
            comm.ask_for_scheduling();
            task.state = TaskRuntimeState::Running { worker_id, rv_id };
            let rqv = request_map.get(task.resource_rq_id);
            worker_map
                .get_worker_mut(worker_id)
                .insert_sn_task(task_id, rqv.get(rv_id));
            try_remove_redirection(
                worker_map,
                scheduler_state,
                request_map,
                task_id,
                task.resource_rq_id,
            );
            (simple_worker_list.as_slice(), false)
        }
        TaskRuntimeState::RunningMultiNode(ws) => {
            // We have received that multi node task is started,
            // Because we do not distinguish between assigned and running state
            // for multi node tasks.
            // (we are not overbooking multi-node tasks, assigned state is not interesting)
            // we already have this task in running state
            // So we do nothing here
            assert_eq!(ws[0], worker_id);
            (ws.as_slice(), false)
        }
        TaskRuntimeState::Running { .. }
        | TaskRuntimeState::Waiting { .. }
        | TaskRuntimeState::Finished => {
            unreachable!()
        }
    };
    comm.client()
        .on_task_started(task_id, task.instance_id, worker_ids, rv_id, context);
    need_scheduling
}

fn reset_mn_task_workers(worker_map: &mut WorkerMap, workers: &[WorkerId], task_id: TaskId) {
    for w in workers {
        let worker = worker_map.get_worker_mut(*w);
        assert_eq!(worker.mn_assignment().unwrap().task_id, task_id);
        worker.reset_mn_task();
    }
}

fn task_reject(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
    resource_rq_variant: ResourceVariantId,
) -> bool {
    let CoreSplitMut {
        task_map,
        task_queues,
        worker_map,
        request_map,
        scheduler_state,
        ..
    } = core.split_mut();
    let Some(task) = task_map.find_task_mut(task_id) else {
        log::debug!("Unknown task rejected id={task_id}");
        return false;
    };
    log::debug!("Task id={task_id} (variant={resource_rq_variant}) rejected on worker={worker_id}");
    let worker = worker_map.get_worker_mut(worker_id);
    let resource_rq_id = task.resource_rq_id;
    worker.block_request(resource_rq_id, resource_rq_variant);
    match &task.state {
        TaskRuntimeState::Assigned {
            worker_id: w_id,
            rv_id,
        } => {
            if worker_id != *w_id {
                log::debug!("Rejection from invalid worker");
            }
            if resource_rq_variant != *rv_id {
                log::debug!("Rejection from invalid worker");
            }
            let rq = request_map.get(resource_rq_id).get(resource_rq_variant);
            worker.remove_sn_task(task_id, rq);
        }
        TaskRuntimeState::Prefilled { worker_id: w_id } => {
            if worker_id != *w_id {
                log::debug!("Rejection from invalid worker");
            }
            worker.remove_prefill_task(task_id);
            task_queues
                .get_mut(resource_rq_id)
                .remove_prefilled(task_id);
        }
        TaskRuntimeState::Retracting { worker_id: w_id } => {
            if worker_id != *w_id {
                log::debug!("Rejection from invalid worker");
                return false;
            }
            if let Some((target_id, rv_id)) = scheduler_state.redirects.remove(&task_id) {
                log::debug!("Transfering to {target_id}");
                task.state = TaskRuntimeState::Assigned {
                    worker_id: target_id,
                    rv_id,
                };
                comm.send_worker_message(
                    target_id,
                    &ComputeTasksBuilder::single_task(task, rv_id, Vec::new()),
                );
                return false;
            }
        }
        TaskRuntimeState::Waiting { .. }
        | TaskRuntimeState::Running { .. }
        | TaskRuntimeState::RunningMultiNode(_)
        | TaskRuntimeState::Finished => {
            unreachable!()
        }
    };
    task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 };
    let mut retracted = Vec::new();
    task_queues.add_ready_task(&task, &mut retracted);
    process_retracted(task_map, worker_map, comm, retracted);
    true
}

fn request_enabled(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    resource_rq_id: ResourceRqId,
    resource_rq_variant: ResourceVariantId,
) {
    let CoreSplitMut { worker_map, .. } = core.split_mut();
    log::debug!(
        "Resource request {resource_rq_id} (variant={resource_rq_variant}) enabled on worker={worker_id}"
    );
    let worker = worker_map.get_worker_mut(worker_id);
    worker.unblock_request(resource_rq_id, resource_rq_variant);
}

pub(crate) fn on_retract_response(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_ids: &[TaskId],
) {
    let CoreSplitMut {
        task_map,
        scheduler_state,
        ..
    } = core.split_mut();
    let mut to_workers: Map<WorkerId, Vec<(TaskId, ResourceVariantId)>> = Map::new();
    for task_id in task_ids {
        let task = task_map.get_task_mut(*task_id);
        if !matches!(task.state, TaskRuntimeState::Retracting { worker_id: w_id } if worker_id == w_id)
        {
            log::debug!("Retracted task {task_id} is in invalid state");
            continue;
        }
        if let Some((target_id, rv_id)) = scheduler_state.redirects.remove(task_id) {
            log::debug!("Task {task_id} retracted and redirected to {target_id}");
            task.state = TaskRuntimeState::Assigned {
                worker_id: target_id,
                rv_id,
            };
            to_workers
                .entry(target_id)
                .or_default()
                .push((*task_id, rv_id));
        } else {
            log::debug!("Task {task_id} retracted, no redirect");
            task.state = TaskRuntimeState::Waiting { unfinished_deps: 0 }
        }
    }
    for (target_id, tasks) in to_workers {
        let mut task_msg_builder = ComputeTasksBuilder::default();
        for (task_id, variant) in &tasks {
            let task = core.get_task_mut(*task_id);
            if let Some(msg) = task_msg_builder.add_task(task, Some(*variant), Vec::new()) {
                comm.send_worker_message(target_id, &msg);
            }
        }
        if let Some(msg) = task_msg_builder.into_last_message() {
            comm.send_worker_message(target_id, &msg);
        }
    }
}

fn task_finished(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
) -> bool {
    let CoreSplitMut {
        task_map,
        worker_map,
        task_queues,
        request_map,
        scheduler_state,
        ..
    } = core.split_mut();
    let mut retracted = Vec::new();
    if let Some(task) = task_map.find_task_mut(task_id) {
        log::debug!("Task id={} finished on worker={}", task_id, worker_id,);
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
            TaskRuntimeState::Retracting { worker_id: w_id } => {
                assert_eq!(*w_id, worker_id);
                try_remove_redirection(
                    worker_map,
                    scheduler_state,
                    request_map,
                    task_id,
                    task.resource_rq_id,
                );
            }
            TaskRuntimeState::Prefilled { .. }
            | TaskRuntimeState::Waiting { .. }
            | TaskRuntimeState::Finished => {
                unreachable!()
            }
        }
        task.state = TaskRuntimeState::Finished;
        comm.client().on_task_finished(task_id);
    } else {
        log::debug!("Unknown task finished id={task_id}");
        return false;
    }

    let consumers: Vec<TaskId> = {
        let task = task_map.get_task(task_id);
        task.get_consumers().iter().copied().collect()
    };

    for consumer in consumers {
        let t = task_map.get_task_mut(consumer);
        if t.decrease_unfinished_deps() {
            task_queues.add_ready_task(t, &mut retracted);
        }
    }
    let CoreSplitMut {
        task_map,
        worker_map,
        ..
    } = core.split_mut();
    process_retracted(task_map, worker_map, comm, retracted);
    let state = core.remove_task(task_id);
    assert!(matches!(state, TaskRuntimeState::Finished));
    true
}

fn try_remove_redirection(
    worker_map: &mut WorkerMap,
    scheduler_state: &mut SchedulerState,
    request_map: &ResourceRqMap,
    task_id: TaskId,
    resource_rq_id: ResourceRqId,
) {
    if let Some((worker_id, rv_id)) = scheduler_state.redirects.remove(&task_id) {
        let worker = worker_map.get_worker_mut(worker_id);
        let rq = request_map.get(resource_rq_id).get(rv_id);
        worker.remove_sn_task(task_id, &rq);
    }
}

fn task_failed(
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
            task_queues,
            scheduler_state,
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
                        TaskRuntimeState::Prefilled { worker_id: w } => {
                            assert_eq!(worker_id, *w);
                            task_queues
                                .get_mut(task.resource_rq_id)
                                .remove_prefilled(task_id);
                            worker_map
                                .get_worker_mut(worker_id)
                                .remove_prefill_task(task_id);
                        }
                        TaskRuntimeState::Retracting { worker_id: w } => {
                            assert_eq!(worker_id, *w);
                            try_remove_redirection(
                                worker_map,
                                scheduler_state,
                                request_map,
                                task_id,
                                task.resource_rq_id,
                            );
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
                | TaskRuntimeState::Prefilled { .. }
                | TaskRuntimeState::Retracting { .. }
                | TaskRuntimeState::Running { .. }
                | TaskRuntimeState::RunningMultiNode(_)
        ));
    } else {
        assert!(matches!(state, TaskRuntimeState::Waiting { .. }));
    }
    drop(state);
    let cancel_ids = comm.client().on_task_error(task_id, consumers, error_info);
    if !cancel_ids.is_empty() {
        on_cancel_tasks(core, comm, &cancel_ids);
    }
}

pub(crate) fn on_cancel_tasks(core: &mut Core, comm: &mut impl Comm, task_ids: &[TaskId]) {
    let mut to_unregister = Set::with_capacity(task_ids.len());
    let mut running_ids: Map<WorkerId, Vec<TaskId>> = Map::default();

    log::debug!("Canceling {} tasks", task_ids.len());

    let CoreSplitMut {
        task_map,
        worker_map,
        request_map,
        task_queues,
        scheduler_state,
        ..
    } = core.split_mut();
    for &task_id in task_ids {
        log::debug!("Canceling task id={task_id}");
        if let Some(task) = task_map.find_task(task_id) {
            to_unregister.insert(task_id);
            task.collect_recursive_consumers(task_map, &mut to_unregister);
            match task.state {
                TaskRuntimeState::Waiting { .. } => {
                    comm.ask_for_scheduling();
                }
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
                    comm.ask_for_scheduling();
                }
                TaskRuntimeState::RunningMultiNode(ref ws) => {
                    for w_id in ws {
                        worker_map.get_worker_mut(*w_id).reset_mn_task();
                    }
                    running_ids.entry(ws[0]).or_default().push(task_id);
                    comm.ask_for_scheduling();
                }
                TaskRuntimeState::Retracting { worker_id } => {
                    try_remove_redirection(
                        worker_map,
                        scheduler_state,
                        request_map,
                        task_id,
                        task.resource_rq_id,
                    );
                    running_ids.entry(worker_id).or_default().push(task_id);
                    comm.ask_for_scheduling();
                }
                TaskRuntimeState::Prefilled { worker_id: w_id } => {
                    task_queues
                        .get_mut(task.resource_rq_id)
                        .remove_prefilled(task_id);
                    worker_map.get_worker_mut(w_id).remove_prefill_task(task_id);
                    running_ids.entry(w_id).or_default().push(task_id);
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
