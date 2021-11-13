use crate::common::trace::{
    trace_task_assign, trace_task_finish, trace_task_place, trace_worker_new,
    trace_worker_steal_response, trace_worker_steal_response_missing,
};
use crate::common::{Map, Set};
use crate::messages::common::TaskFailInfo;
use crate::messages::gateway::LostWorkerReason;
use crate::messages::worker::{
    NewWorkerMsg, StealResponse, StealResponseMsg, TaskFinishedMsg, TaskIdMsg, TaskIdsMsg,
    ToWorkerMessage,
};
use crate::server::comm::Comm;
use crate::server::core::Core;
use crate::server::task::{DataInfo, TaskRef, TaskRuntimeState};
use crate::server::task::{FinishInfo, Task, WaitingInfo};
use crate::server::worker::Worker;
use crate::{TaskId, WorkerId};

pub fn on_new_worker(core: &mut Core, comm: &mut impl Comm, worker: Worker) {
    {
        trace_worker_new(worker.id, &worker.configuration);

        comm.broadcast_worker_message(&ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: worker.id,
            address: worker.configuration.listen_address.clone(),
        }));

        comm.send_client_worker_new(worker.id, &worker.configuration);

        /*comm.send_scheduler_message(ToSchedulerMessage::NewWorker(worker.make_sched_info()));*/
        comm.ask_for_scheduling();
    }
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

    for task_ref in core.get_tasks() {
        let mut task = task_ref.get_mut();
        let task_id = task.id;

        let new_state = match &mut task.state {
            TaskRuntimeState::Waiting(_) => continue,
            TaskRuntimeState::Assigned(w_id) | TaskRuntimeState::Running(w_id) => {
                if *w_id == worker_id {
                    log::debug!("Removing task task={} from lost worker", task_id);
                    task.increment_instance_id();
                    task.set_fresh_flag(true);
                    ready_to_assign.push(task_ref.clone());
                    if task.is_running() {
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
                        removes.push((*to_id, task_ref.clone()));
                    }
                    task.increment_instance_id();
                    task.set_fresh_flag(true);
                    ready_to_assign.push(task_ref.clone());
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
            TaskRuntimeState::Released => {
                unreachable!()
            }
        };
        task.state = new_state;
    }

    for (w_id, task_ref) in removes {
        let task = task_ref.get();
        core.get_worker_mut_by_id_or_panic(w_id)
            .remove_task(&task, &task_ref)
    }

    /*if let Some(w_id) = remove_from {
        assert!(core.get_worker_mut_by_id_or_panic(w_id).tasks.remove(&task_ref))
    }*/

    for task_ref in ready_to_assign {
        core.add_ready_to_assign(task_ref);
    }

    let _worker = core.remove_worker(worker_id);
    comm.send_client_worker_lost(worker_id, running_tasks, reason);
    comm.ask_for_scheduling();

    //let worker = core.get_worker_by_id_or_panic(.into()worker_id);
}

pub fn on_new_tasks(core: &mut Core, comm: &mut impl Comm, new_tasks: Vec<TaskRef>) {
    assert!(!new_tasks.is_empty());

    /*let mut lowest_id = TaskId::MAX;
    let mut highest_id = 0;

    for task_ref in &new_tasks {
        let task_id = task_ref.get().id;
        lowest_id = lowest_id.min(task_id);
        highest_id = highest_id.max(task_id);
        log::debug!("New task id={}", task_id);
        trace_task_new(task_id, task_ref.get().inputs.iter().map(|tr| tr.get().id));
        core.add_task(task_ref.clone());
    }

    core.update_max_task_id(highest_id);*/

    for task_ref in new_tasks {
        {
            let mut task = task_ref.get_mut();

            let mut count = 0;
            for tr in &task.inputs {
                let mut task_dep = tr.get_mut();
                task_dep.add_consumer(task_ref.clone());
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
        }
        core.add_task(task_ref);
    }
    /*
    let mut count = new_tasks.len();
    let mut processed = Set::with_capacity(count);
    let mut stack: Vec<(TaskRef, usize)> = Default::default();

    let mut scheduler_infos = Vec::with_capacity(count);
    for task_ref in new_tasks {
        if !task_ref.get().has_consumers() {
            stack.push((task_ref, 0));
            while let Some((tr, c)) = stack.pop() {
                let ii = {
                    let task = tr.get();
                    task.dependencies.get(c).copied()
                };
                if let Some(inp) = ii {
                    stack.push((tr, c + 1));
                    if inp >= lowest_id && processed.insert(inp) {
                        stack.push((core.get_task_by_id_or_panic(inp).clone(), 0));
                    }
                } else {
                    count -= 1;
                    let task = tr.get();
                    scheduler_infos.push(task.make_sched_info());
                }
            }
        }
    }
    assert_eq!(count, 0);
    // comm.send_scheduler_message(ToSchedulerMessage::NewTasks(scheduler_infos));*/
    comm.ask_for_scheduling()
}

pub fn on_task_running(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
) {
    if let Some(task_ref) = core.get_task_by_id(task_id).cloned() {
        let mut task = task_ref.get_mut();
        let new_state = match task.state {
            TaskRuntimeState::Assigned(w_id) | TaskRuntimeState::Stealing(w_id, None) => {
                assert_eq!(w_id, worker_id);
                TaskRuntimeState::Running(worker_id)
            }
            TaskRuntimeState::Stealing(w_id, Some(target_id)) => {
                assert_eq!(w_id, worker_id);
                let worker = core.get_worker_mut_by_id_or_panic(target_id);
                worker.remove_task(&task, &task_ref);
                let worker = core.get_worker_mut_by_id_or_panic(w_id);
                worker.insert_task(&task, task_ref.clone());
                comm.ask_for_scheduling();
                TaskRuntimeState::Running(worker_id)
            }
            TaskRuntimeState::Running(_)
            | TaskRuntimeState::Waiting(_)
            | TaskRuntimeState::Finished(_)
            | TaskRuntimeState::Released => {
                unreachable!()
            }
        };
        task.state = new_state;
        if task.is_observed() {
            comm.send_client_task_started(task_id, worker_id);
        }
    }
}

pub fn on_task_finished(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    msg: TaskFinishedMsg,
) {
    if let Some(task_ref) = core.get_task_by_id(msg.id).cloned() {
        {
            let mut task = task_ref.get_mut();
            trace_task_finish(
                task.id,
                worker_id,
                msg.size,
                (0, 0), /* TODO: gather real computation */
            );
            log::debug!("Task id={} finished on worker={}", task.id, worker_id);
            assert!(task.is_assigned_or_stealed_from(worker_id));

            match &task.state {
                TaskRuntimeState::Assigned(w_id) | TaskRuntimeState::Running(w_id) => {
                    assert_eq!(*w_id, worker_id);
                    let worker = core.get_worker_mut_by_id_or_panic(worker_id);
                    worker.remove_task(&task, &task_ref);
                }
                TaskRuntimeState::Stealing(w_id, Some(target_w)) => {
                    assert_eq!(*w_id, worker_id);
                    let worker = core.get_worker_mut_by_id_or_panic(*target_w);
                    worker.remove_task(&task, &task_ref);
                }
                TaskRuntimeState::Stealing(w_id, None) => {
                    assert_eq!(*w_id, worker_id);
                    /* Do nothing */
                }
                TaskRuntimeState::Waiting(_)
                | TaskRuntimeState::Finished(_)
                | TaskRuntimeState::Released => {
                    unreachable!();
                }
            }

            let mut placement = Set::new();

            if task.configuration.n_outputs > 0 {
                placement.insert(worker_id);
            }

            task.state = TaskRuntimeState::Finished(FinishInfo {
                data_info: DataInfo { size: msg.size },
                placement,
                future_placement: Default::default(),
            });
            /*comm.send_scheduler_message(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Finished,
                id: task.id,
                worker: worker_id,
                size: Some(task.data_info().unwrap().size),
            }));*/
            comm.ask_for_scheduling();

            if task.is_observed() {
                comm.send_client_task_finished(task.id);
            }
        };

        {
            let task = task_ref.get();
            for consumer in task.get_consumers() {
                let mut t = consumer.get_mut();
                if t.decrease_unfinished_deps() {
                    core.add_ready_to_assign(consumer.clone());
                    comm.ask_for_scheduling();
                }
            }
        }

        let mut task = task_ref.get_mut();
        unregister_as_consumer(core, comm, &mut task, &task_ref);
        remove_task_if_possible(core, comm, &mut task);

        //self.notify_key_in_memory(&task_ref, notifications);
    } else {
        log::debug!("Unknown task finished id={}", msg.id);
    }
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
        let task_ref = core.get_task_by_id(task_id).cloned();
        match task_ref {
            Some(task_ref) => {
                let new_state = {
                    let task = task_ref.get();
                    if task.is_done_or_running() {
                        log::debug!("Received trace response for finished task={}", task_id);
                        trace_worker_steal_response(task.id, worker_id, 0.into(), "done");
                        continue;
                    }
                    let (from_worker_id, to_worker_id) =
                        if let TaskRuntimeState::Stealing(from_w, to_w) = &task.state {
                            assert_eq!(*from_w, worker_id);
                            (*from_w, *to_w)
                        } else {
                            panic!(
                                "Invalid state of task={} when steal response occured",
                                task_id
                            );
                        };

                    /*comm.send_scheduler_message(ToSchedulerMessage::TaskStealResponse(
                        TaskStealResponse {
                            id: task_id,
                            from_worker: worker_id,
                            to_worker: to_worker_id,
                            success,
                        },
                    ));*/

                    trace_worker_steal_response(
                        task_id,
                        worker_id,
                        to_worker_id.unwrap_or_else(|| WorkerId::new(0)),
                        match response {
                            StealResponse::Ok => "ok",
                            StealResponse::NotHere => "nothere",
                            StealResponse::Running => "running",
                        },
                    );

                    match response {
                        StealResponse::Ok => {
                            log::debug!("Task stealing was successful task={}", task_id);
                            trace_task_assign(
                                task_id,
                                to_worker_id.unwrap_or_else(|| WorkerId::new(0)),
                            );
                            if let Some(w_id) = to_worker_id {
                                comm.send_worker_message(w_id, &task.make_compute_message());
                                TaskRuntimeState::Assigned(w_id)
                            } else {
                                comm.ask_for_scheduling();
                                core.add_ready_to_assign(task_ref.clone());
                                TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
                            }
                        }
                        StealResponse::Running => {
                            log::debug!("Task stealing was not successful task={}", task_id);
                            if let Some(w_id) = to_worker_id {
                                core.get_worker_mut_by_id_or_panic(w_id)
                                    .remove_task(&task, &task_ref)
                            }
                            core.get_worker_mut_by_id_or_panic(from_worker_id)
                                .insert_task(&task, task_ref.clone());
                            comm.ask_for_scheduling();
                            TaskRuntimeState::Running(worker_id)
                        }
                        StealResponse::NotHere => {
                            panic!("Received NotHere while stealing, it seems that Finished message got lost");
                        }
                    }
                };
                {
                    let mut task = task_ref.get_mut();
                    if let TaskRuntimeState::Waiting(_winfo) = &new_state {
                        task.set_fresh_flag(true)
                    };
                    task.state = new_state;
                }
            }
            None => {
                log::debug!("Received trace response for invalid task {}", task_id);
                trace_worker_steal_response_missing(task_id, worker_id)
            }
        }
    }
}

pub fn on_reset_keep_flag(core: &mut Core, comm: &mut impl Comm, task_id: TaskId) {
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    let mut task = task_ref.get_mut();
    task.set_keep_flag(false);
    remove_task_if_possible(core, comm, &mut task);
}

pub fn on_set_keep_flag(core: &mut Core, task_id: TaskId) -> TaskRef {
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    task_ref.get_mut().set_keep_flag(true);
    task_ref
}

pub fn on_set_observe_flag(
    core: &mut Core,
    comm: &mut impl Comm,
    task_id: TaskId,
    value: bool,
) -> bool {
    if let Some(task_ref) = core.get_task_by_id(task_id) {
        let mut task = task_ref.get_mut();
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
    if let Some(task_ref) = core.get_task_by_id(task_id).cloned() {
        log::debug!("Task task {} failed", task_id);

        let task_refs: Vec<TaskRef> = {
            let task = task_ref.get();
            assert!(task.is_assigned_or_stealed_from(worker_id));
            core.get_worker_mut_by_id_or_panic(worker_id)
                .remove_task(&task, &task_ref);
            task.collect_consumers().into_iter().collect()
        };

        unregister_as_consumer(core, comm, &mut task_ref.get_mut(), &task_ref);

        for task_ref in &task_refs {
            let mut task = task_ref.get_mut();
            log::debug!("Task={} canceled because of failed dependency", task.id);
            assert!(task.is_waiting());
            unregister_as_consumer(core, comm, &mut task, task_ref);
        }

        assert!(matches!(
            core.remove_task(&mut task_ref.get_mut()),
            TaskRuntimeState::Assigned(_)
                | TaskRuntimeState::Running(_)
                | TaskRuntimeState::Stealing(_, _)
        ));
        //comm.send_scheduler_message(ToSchedulerMessage::RemoveTask(task_ref.get().id));

        for task_ref in &task_refs {
            // We can drop the resulting state as checks was done earlier
            let mut task = task_ref.get_mut();
            assert!(matches!(
                core.remove_task(&mut task),
                TaskRuntimeState::Waiting(_)
            ));
            //comm.send_scheduler_message(ToSchedulerMessage::RemoveTask(task.id));
        }
        comm.send_client_task_error(
            task_id,
            task_refs.iter().map(|tr| tr.get().id).collect(),
            error_info,
        );
    }
}

pub fn on_cancel_tasks(
    core: &mut Core,
    comm: &mut impl Comm,
    task_ids: &[TaskId],
) -> (Vec<TaskId>, Vec<TaskId>) {
    let mut task_refs = Set::with_capacity(task_ids.len());
    let mut running_ids: Map<WorkerId, Vec<TaskId>> = Map::new();
    let mut already_finished: Vec<TaskId> = Vec::new();

    log::debug!("Canceling {} tasks", task_ids.len());

    for task_id in task_ids {
        log::debug!("Canceling task id={}", task_id);
        let tr: Option<TaskRef> = core.get_task_by_id(*task_id).cloned();
        if let Some(task_ref) = tr {
            let mut task = task_ref.get_mut();
            match task.state {
                TaskRuntimeState::Waiting(_) => {
                    task_refs.insert(task_ref.clone());
                    task_refs.extend(task.collect_consumers().into_iter());
                }
                TaskRuntimeState::Assigned(w_id) | TaskRuntimeState::Running(w_id) => {
                    task_refs.insert(task_ref.clone());
                    task_refs.extend(task.collect_consumers().into_iter());
                    core.get_worker_mut_by_id_or_panic(w_id)
                        .remove_task(&task, &task_ref);
                    running_ids.entry(w_id).or_default().push(*task_id);
                }
                TaskRuntimeState::Stealing(from_id, to_id) => {
                    task_refs.insert(task_ref.clone());
                    task_refs.extend(task.collect_consumers().into_iter());
                    if let Some(to_id) = to_id {
                        core.get_worker_mut_by_id_or_panic(to_id)
                            .remove_task(&task, &task_ref);
                    }
                    running_ids.entry(from_id).or_default().push(*task_id);
                }
                TaskRuntimeState::Finished(_) => {
                    if task.is_keeped() {
                        task.set_keep_flag(false);
                        remove_task_if_possible(core, comm, &mut task);
                    }
                    already_finished.push(*task_id);
                }
                TaskRuntimeState::Released => {
                    unreachable!()
                }
            };
        } else {
            log::debug!("Task is not here");
            already_finished.push(*task_id);
        }
    }

    for task_ref in &task_refs {
        let mut task = task_ref.get_mut();
        unregister_as_consumer(core, comm, &mut task, task_ref);
    }

    core.remove_tasks_batched(&task_refs);

    for (w_id, ids) in running_ids {
        comm.send_worker_message(w_id, &ToWorkerMessage::CancelTasks(TaskIdsMsg { ids }));
    }

    comm.ask_for_scheduling();
    (
        task_refs.into_iter().map(|tr| tr.get().id).collect(),
        already_finished,
    )
}

fn get_task_or_send_delete(
    core: &Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
) -> Option<TaskRef> {
    let result = core.get_task_by_id(task_id).cloned();
    if result.is_none() {
        log::debug!(
            "Task id={} is not known to server; replaying with delete",
            task_id
        );
        comm.send_worker_message(
            worker_id,
            &ToWorkerMessage::DeleteData(TaskIdMsg { id: task_id }),
        );
    }
    result
}

pub fn on_tasks_transferred(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    task_id: TaskId,
) {
    log::debug!("Task id={} transfered on worker={}", task_id, worker_id);
    // TODO handle the race when task is removed from server before this message arrives
    if let Some(task_ref) = get_task_or_send_delete(core, comm, worker_id, task_id) {
        let mut task = task_ref.get_mut();
        match &mut task.state {
            TaskRuntimeState::Finished(ref mut winfo) => {
                winfo.placement.insert(worker_id);
            }
            TaskRuntimeState::Released
            | TaskRuntimeState::Waiting(_)
            | TaskRuntimeState::Running(_)
            | TaskRuntimeState::Assigned(_)
            | TaskRuntimeState::Stealing(_, _) => {
                panic!("Invalid task state");
            }
        };
        trace_task_place(task.id, worker_id);
        /*comm.send_scheduler_message(ToSchedulerMessage::TaskUpdate(TaskUpdate {
            id,
            state: TaskUpdateType::Placed,
            worker: worker_id,
            size: None,
        }));*/
    }
}

fn unregister_as_consumer(
    core: &mut Core,
    comm: &mut impl Comm,
    task: &mut Task,
    task_ref: &TaskRef,
) {
    for tr in &task.inputs {
        //let tr = core.get_task_by_id_or_panic(*input_id).clone();
        let mut t = tr.get_mut();
        assert!(t.remove_consumer(task_ref));
        remove_task_if_possible(core, comm, &mut t);
    }
}

fn remove_task_if_possible(core: &mut Core, comm: &mut impl Comm, task: &mut Task) {
    if task.is_removable() {
        let ws = match core.remove_task(task) {
            TaskRuntimeState::Finished(finfo) => finfo.placement,
            _ => unreachable!(),
        };
        for worker_id in ws {
            log::debug!(
                "Task id={} is no longer needed, deleting from worker={}",
                task.id,
                worker_id
            );
            comm.send_worker_message(
                worker_id,
                &ToWorkerMessage::DeleteData(TaskIdMsg { id: task.id }),
            );
        }
        task.clear();
        //comm.send_scheduler_message(ToSchedulerMessage::RemoveTask(task.id));
        //comm.send_client_task_removed(task.id);
    }
}
