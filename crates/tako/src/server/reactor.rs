use crate::common::{Map, Set};
use crate::messages::common::TaskFailInfo;
use crate::messages::gateway::LostWorkerReason;
use crate::messages::worker::{
    NewWorkerMsg, StealResponse, StealResponseMsg, TaskFinishedMsg, TaskIdsMsg, TaskRunningMsg,
    ToWorkerMessage,
};
use crate::server::comm::Comm;
use crate::server::core::Core;
use crate::server::task::{DataInfo, Task, TaskRuntimeState};
use crate::server::task::{FinishInfo, WaitingInfo};
use crate::server::worker::Worker;
use crate::server::workermap::WorkerMap;
use crate::{TaskId, WorkerId};

pub fn on_new_worker(core: &mut Core, comm: &mut impl Comm, worker: Worker) {
    comm.broadcast_worker_message(&ToWorkerMessage::NewWorker(NewWorkerMsg {
        worker_id: worker.id,
        address: worker.configuration.listen_address.clone(),
    }));

    comm.send_client_worker_new(worker.id, &worker.configuration);

    comm.ask_for_scheduling();
    core.new_worker(worker);
}

pub fn on_remove_worker(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    reason: LostWorkerReason,
) {
    log::debug!("Removing worker {}", worker_id);

    let mut ready_to_assign = Vec::new();
    let mut removes = Vec::new();
    let mut running_tasks = Vec::new();

    let (task_map, worker_map) = core.split_tasks_workers_mut();
    let task_ids: Vec<_> = task_map.task_ids().collect();
    for task_id in task_ids {
        let mut task = task_map.get_task_mut(task_id);

        let new_state = match &mut task.state {
            TaskRuntimeState::Waiting(_) => continue,
            TaskRuntimeState::Assigned(w_id)
            | TaskRuntimeState::Running {
                worker_id: w_id, ..
            } => {
                if *w_id == worker_id {
                    log::debug!("Removing task task={} from lost worker", task_id);
                    task.increment_instance_id();
                    task.set_fresh_flag(true);
                    ready_to_assign.push(task_id);
                    if task.is_sn_running() {
                        running_tasks.push(task_id);
                    }
                    TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
                } else {
                    continue;
                }
            }
            TaskRuntimeState::Stealing(from_id, to_id) => {
                if *from_id == worker_id {
                    log::debug!("Canceling steal of task={} from lost worker", task_id);

                    if let Some(to_id) = to_id {
                        removes.push((*to_id, task_id));
                    }
                    task.increment_instance_id();
                    task.set_fresh_flag(true);
                    ready_to_assign.push(task_id);
                    TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
                } else if *to_id == Some(worker_id) {
                    log::debug!("Task={} is stealing target for lost worker", task_id);
                    TaskRuntimeState::Stealing(*from_id, None)
                } else {
                    continue;
                }
            }
            TaskRuntimeState::Finished(finfo) => {
                finfo.future_placement.remove(&worker_id);
                finfo.placement.remove(&worker_id);
                if finfo.placement.is_empty() {
                    todo!();
                    // We have lost last worker that have this data
                }
                continue;
            }
            TaskRuntimeState::RunningMultiNode(ws) => {
                if ws.contains(&worker_id) {
                    let root_worker_id = ws[0];
                    for w in ws {
                        worker_map.get_worker_mut(*w).reset_mn_task();
                    }
                    task.increment_instance_id();
                    task.set_fresh_flag(true);
                    ready_to_assign.push(task_id);
                    running_tasks.push(task_id);

                    // If non root then cancel task on root
                    if worker_id != root_worker_id {
                        comm.send_worker_message(
                            root_worker_id,
                            &ToWorkerMessage::CancelTasks(TaskIdsMsg { ids: vec![task_id] }),
                        )
                    }
                    TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
                } else {
                    continue;
                }
            }
        };
        task.state = new_state;
    }

    {
        let (tasks, workers) = core.split_tasks_workers_mut();
        for (w_id, task_id) in removes {
            let task = tasks.get_task(task_id);
            workers.get_worker_mut(w_id).remove_sn_task(task)
        }
    }

    for task_id in ready_to_assign {
        core.add_ready_to_assign(task_id);
    }

    let _ = core.remove_worker(worker_id);
    comm.send_client_worker_lost(worker_id, running_tasks, reason);
    comm.ask_for_scheduling();
}

pub fn on_new_tasks(core: &mut Core, comm: &mut impl Comm, new_tasks: Vec<Task>) {
    assert!(!new_tasks.is_empty());

    let mut task_map: Map<_, _> = new_tasks.into_iter().map(|t| (t.id, t)).collect();
    let ids: Vec<_> = task_map.keys().copied().collect();
    for task_id in ids {
        let mut task = task_map.remove(&task_id).unwrap();

        let mut count = 0;
        for ti in &task.inputs {
            let input_id = ti.task();
            let task_dep = task_map
                .get_mut(&input_id)
                .unwrap_or_else(|| core.get_task_mut(input_id));
            task_dep.add_consumer(task.id);
            if !task_dep.is_finished() {
                count += 1
            }
        }

        assert!(matches!(
            task.state,
            TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
        ));
        task.state = TaskRuntimeState::Waiting(WaitingInfo {
            unfinished_deps: count,
        });
        task_map.insert(task_id, task);
    }

    for (_, task) in task_map.into_iter() {
        core.add_task(task);
    }
    comm.ask_for_scheduling()
}

pub fn on_task_running(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    message: TaskRunningMsg,
) {
    let TaskRunningMsg {
        id: task_id,
        context,
    } = message;

    let (tasks, workers) = core.split_tasks_workers_mut();
    let simple_worker_list = &[worker_id];
    if let Some(mut task) = tasks.find_task_mut(task_id) {
        let worker_ids = match &task.state {
            TaskRuntimeState::Assigned(w_id) | TaskRuntimeState::Stealing(w_id, None) => {
                assert_eq!(*w_id, worker_id);
                task.state = TaskRuntimeState::Running { worker_id };
                simple_worker_list.as_slice()
            }
            TaskRuntimeState::Stealing(w_id, Some(target_id)) => {
                assert_eq!(*w_id, worker_id);
                let worker = workers.get_worker_mut(*target_id);
                worker.remove_sn_task(task);
                let worker = workers.get_worker_mut(*w_id);
                worker.insert_sn_task(task);
                comm.ask_for_scheduling();
                task.state = TaskRuntimeState::Running { worker_id };
                simple_worker_list.as_slice()
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
            | TaskRuntimeState::Waiting(_)
            | TaskRuntimeState::Finished(_) => {
                unreachable!()
            }
        };

        if task.is_observed() {
            comm.send_client_task_started(task_id, worker_ids, context);
        }
    }
}

fn reset_mn_task_workers(worker_map: &mut WorkerMap, workers: &[WorkerId], task_id: TaskId) {
    for w in workers {
        let worker = worker_map.get_worker_mut(*w);
        let mn = worker.mn_task().unwrap();
        assert_eq!(mn.task_id, task_id);
        worker.reset_mn_task();
    }
}

pub fn on_task_finished(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    msg: TaskFinishedMsg,
) {
    {
        let (tasks, workers) = core.split_tasks_workers_mut();
        if let Some(mut task) = tasks.find_task_mut(msg.id) {
            log::debug!("Task id={} finished on worker={}", task.id, worker_id);

            if task.configuration.resources.is_multi_node() {}

            assert!(task.is_assigned_or_stolen_from(worker_id));

            match &task.state {
                TaskRuntimeState::Assigned(w_id)
                | TaskRuntimeState::Running {
                    worker_id: w_id, ..
                } => {
                    assert_eq!(*w_id, worker_id);
                    workers.get_worker_mut(worker_id).remove_sn_task(task);
                }
                TaskRuntimeState::RunningMultiNode(ws) => {
                    assert_eq!(ws[0], worker_id);
                    reset_mn_task_workers(workers, ws, task.id);
                }
                TaskRuntimeState::Stealing(w_id, Some(target_w)) => {
                    assert_eq!(*w_id, worker_id);
                    workers.get_worker_mut(*target_w).remove_sn_task(task);
                }
                TaskRuntimeState::Stealing(w_id, None) => {
                    assert_eq!(*w_id, worker_id);
                    /* Do nothing */
                }
                TaskRuntimeState::Waiting(_) | TaskRuntimeState::Finished(_) => {
                    unreachable!();
                }
            }

            let mut placement = Set::default();

            if task.configuration.n_outputs > 0 {
                placement.insert(worker_id);
            }

            task.state = TaskRuntimeState::Finished(FinishInfo {
                data_info: DataInfo { size: msg.size },
                placement,
                future_placement: Default::default(),
            });
            comm.ask_for_scheduling();

            if task.is_observed() {
                comm.send_client_task_finished(task.id);
            }
        } else {
            log::debug!("Unknown task finished id={}", msg.id);
            return;
        }
    }

    // TODO: benchmark vec vs set
    let consumers: Set<TaskId> = {
        let task = core.get_task(msg.id);
        task.get_consumers().clone()
    };

    for consumer in consumers {
        let id = {
            let t = core.get_task_mut(consumer);
            if t.decrease_unfinished_deps() {
                t.id
            } else {
                continue;
            }
        };

        core.add_ready_to_assign(id);
        comm.ask_for_scheduling();
    }
    unregister_as_consumer(core, comm, msg.id);
    remove_task_if_possible(core, comm, msg.id);
}

pub fn on_steal_response(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    msg: StealResponseMsg,
) {
    for (task_id, response) in msg.responses {
        log::debug!(
            "Steal response from {}, task={} response={:?}",
            worker_id,
            task_id,
            response
        );
        if core.find_task(task_id).is_none() {
            log::debug!("Received trace response for invalid task {}", task_id);
            continue;
        }

        let new_state = {
            let (from_worker_id, to_worker_id) = {
                let task = core.get_task(task_id);
                if task.is_done_or_running() {
                    log::debug!("Received trace response for finished task={}", task_id);
                    continue;
                }
                if let TaskRuntimeState::Stealing(from_w, to_w) = &task.state {
                    assert_eq!(*from_w, worker_id);
                    (*from_w, *to_w)
                } else {
                    panic!(
                        "Invalid state of task={} when steal response occurred",
                        task_id
                    );
                }
            };

            match response {
                StealResponse::Ok => {
                    log::debug!("Task stealing was successful task={}", task_id);
                    if let Some(w_id) = to_worker_id {
                        let task = core.get_task(task_id);
                        comm.send_worker_message(w_id, &task.make_compute_message(Vec::new()));
                        TaskRuntimeState::Assigned(w_id)
                    } else {
                        comm.ask_for_scheduling();
                        core.add_ready_to_assign(task_id);
                        TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
                    }
                }
                StealResponse::Running => {
                    log::debug!("Task stealing was not successful task={}", task_id);

                    let (tasks, workers) = core.split_tasks_workers_mut();
                    let task = tasks.get_task(task_id);
                    if let Some(w_id) = to_worker_id {
                        workers.get_worker_mut(w_id).remove_sn_task(task)
                    }
                    workers.get_worker_mut(from_worker_id).insert_sn_task(task);
                    comm.ask_for_scheduling();
                    TaskRuntimeState::Running { worker_id }
                }
                StealResponse::NotHere => {
                    panic!(
                        "Received NotHere while stealing, it seems that Finished message got lost"
                    );
                }
            }
        };

        let mut task = core.get_task_mut(task_id);
        if let TaskRuntimeState::Waiting(_winfo) = &new_state {
            task.set_fresh_flag(true)
        };
        task.state = new_state;
    }
}

pub fn on_reset_keep_flag(core: &mut Core, comm: &mut impl Comm, task_id: TaskId) {
    let task = core.get_task_mut(task_id);
    task.set_keep_flag(false);
    remove_task_if_possible(core, comm, task_id);
}

pub fn on_set_observe_flag(
    core: &mut Core,
    comm: &mut impl Comm,
    task_id: TaskId,
    value: bool,
) -> bool {
    if let Some(task) = core.find_task_mut(task_id) {
        if value && task.is_finished() {
            comm.send_client_task_finished(task_id);
        }
        task.set_observed_flag(value);
        true
    } else {
        false
    }
}

pub fn on_task_error(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
    error_info: TaskFailInfo,
) {
    let consumers: Vec<TaskId> = {
        let (tasks, workers) = core.split_tasks_workers_mut();
        if let Some(task) = tasks.find_task(task_id) {
            log::debug!("Task task {} failed", task_id);
            if task.configuration.resources.is_multi_node() {
                let ws = task.mn_placement().unwrap();
                assert_eq!(ws[0], worker_id);
                reset_mn_task_workers(workers, ws, task_id);
            } else {
                assert!(task.is_assigned_or_stolen_from(worker_id));
                workers.get_worker_mut(worker_id).remove_sn_task(task);
            }
            task.collect_consumers(tasks).into_iter().collect()
        } else {
            log::debug!("Unknown task failed");
            return;
        }
    };
    // TODO: take taskmap in `unregister_as_consumer`
    unregister_as_consumer(core, comm, task_id);

    for &consumer in &consumers {
        {
            let task = core.get_task(consumer);
            log::debug!("Task={} canceled because of failed dependency", task.id);
            assert!(task.is_waiting());
        }
        unregister_as_consumer(core, comm, consumer);
    }

    assert!(matches!(
        core.remove_task(task_id),
        TaskRuntimeState::Assigned(_)
            | TaskRuntimeState::Running { .. }
            | TaskRuntimeState::Stealing(_, _)
            | TaskRuntimeState::RunningMultiNode(_)
    ));

    for &consumer in &consumers {
        // We can drop the resulting state as checks was done earlier
        assert!(matches!(
            core.remove_task(consumer),
            TaskRuntimeState::Waiting(_)
        ));
    }
    comm.send_client_task_error(task_id, consumers, error_info);
}

pub fn on_cancel_tasks(
    core: &mut Core,
    comm: &mut impl Comm,
    task_ids: &[TaskId],
) -> (Vec<TaskId>, Vec<TaskId>) {
    let mut to_unregister = Set::with_capacity(task_ids.len());
    let mut running_ids: Map<WorkerId, Vec<TaskId>> = Map::default();
    let mut already_finished: Vec<TaskId> = Vec::new();

    log::debug!("Canceling {} tasks", task_ids.len());

    for &task_id in task_ids {
        log::debug!("Canceling task id={}", task_id);

        if core.find_task(task_id).is_none() {
            log::debug!("Task is not here");
            already_finished.push(task_id);
            continue;
        }

        let mut remove = false;
        {
            let (tasks, workers) = core.split_tasks_workers_mut();
            let task = tasks.get_task(task_id);
            match task.state {
                TaskRuntimeState::Waiting(_) => {
                    to_unregister.insert(task_id);
                    to_unregister.extend(task.collect_consumers(tasks).into_iter());
                }
                TaskRuntimeState::Assigned(w_id)
                | TaskRuntimeState::Running {
                    worker_id: w_id, ..
                } => {
                    to_unregister.insert(task_id);
                    to_unregister.extend(task.collect_consumers(tasks).into_iter());
                    workers.get_worker_mut(w_id).remove_sn_task(task);
                    running_ids.entry(w_id).or_default().push(task_id);
                }
                TaskRuntimeState::RunningMultiNode(ref ws) => {
                    to_unregister.insert(task_id);
                    to_unregister.extend(task.collect_consumers(tasks).into_iter());
                    for w_id in ws {
                        workers.get_worker_mut(*w_id).reset_mn_task();
                    }
                    running_ids.entry(ws[0]).or_default().push(task_id);
                }
                TaskRuntimeState::Stealing(from_id, to_id) => {
                    to_unregister.insert(task_id);
                    to_unregister.extend(task.collect_consumers(tasks).into_iter());
                    if let Some(to_id) = to_id {
                        workers.get_worker_mut(to_id).remove_sn_task(task);
                    }
                    running_ids.entry(from_id).or_default().push(task_id);
                }
                TaskRuntimeState::Finished(_) => {
                    if task.is_keeped() {
                        remove = true;
                    }
                    already_finished.push(task_id);
                }
            };
        }
        if remove {
            core.get_task_mut(task_id).set_keep_flag(false);
            remove_task_if_possible(core, comm, task_id);
        }
    }

    for &task_id in &to_unregister {
        unregister_as_consumer(core, comm, task_id);
    }

    core.remove_tasks_batched(&to_unregister);

    for (w_id, ids) in running_ids {
        comm.send_worker_message(w_id, &ToWorkerMessage::CancelTasks(TaskIdsMsg { ids }));
    }

    comm.ask_for_scheduling();
    (to_unregister.into_iter().collect(), already_finished)
}

pub fn on_tasks_transferred(core: &mut Core, worker_id: WorkerId, task_id: TaskId) {
    log::debug!("Task id={} transferred to worker={}", task_id, worker_id);
    // TODO handle the race when task is removed from server before this message arrives
    if let Some(task) = core.find_task_mut(task_id) {
        match &mut task.state {
            TaskRuntimeState::Finished(ref mut winfo) => {
                winfo.placement.insert(worker_id);
            }
            TaskRuntimeState::Waiting(_)
            | TaskRuntimeState::Running { .. }
            | TaskRuntimeState::RunningMultiNode(_)
            | TaskRuntimeState::Assigned(_)
            | TaskRuntimeState::Stealing(_, _) => {
                panic!("Invalid task state");
            }
        };
    }
}

fn unregister_as_consumer(core: &mut Core, comm: &mut impl Comm, task_id: TaskId) {
    let inputs: Vec<TaskId> = core
        .get_task(task_id)
        .inputs
        .iter()
        .map(|ti| ti.task())
        .collect();
    for input_id in inputs {
        let input = core.get_task_mut(input_id);
        assert!(input.remove_consumer(task_id));
        remove_task_if_possible(core, comm, input_id);
    }
}

fn remove_task_if_possible(core: &mut Core, _comm: &mut impl Comm, task_id: TaskId) {
    if !core.get_task(task_id).is_removable() {
        return;
    }

    match core.remove_task(task_id) {
        TaskRuntimeState::Finished(finfo) => finfo.placement,
        _ => unreachable!(),
    };
    log::debug!("Task id={task_id} is no longer needed");
}
