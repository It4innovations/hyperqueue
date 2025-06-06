use crate::datasrv::{DataObjectId, OutputId};
use crate::gateway::{CrashLimit, LostWorkerReason};
use crate::internal::common::{Map, Set};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    NewWorkerMsg, StealResponse, StealResponseMsg, TaskFinishedMsg, TaskIdsMsg, TaskRunningMsg,
    ToWorkerMessage,
};
use crate::internal::server::comm::Comm;
use crate::internal::server::core::Core;
use crate::internal::server::dataobj::{DataObjectHandle, ObjsToRemoveFromWorkers, RefCount};
use crate::internal::server::task::WaitingInfo;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::Worker;
use crate::internal::server::workermap::WorkerMap;
use crate::{TaskId, WorkerId};
use std::fmt::Write;

// Scheduler priority increase for each t-level
pub(crate) const T_LEVEL_WEIGHT: i32 = 256;

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
    log::debug!("Removing worker {}", worker_id);

    let mut ready_to_assign = Vec::new();
    let mut removes = Vec::new();
    let mut running_tasks = Vec::new();

    let (task_map, worker_map) = core.split_tasks_workers_mut();
    let task_ids: Vec<_> = task_map.task_ids().collect();
    for task_id in task_ids {
        let task = task_map.get_task_mut(task_id);
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
            TaskRuntimeState::Finished => {
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

    for data_obj in core.data_objects_mut().iter_mut() {
        data_obj.remove_placement(worker_id);
    }

    log::debug!(
        "Running tasks on lost worker {}: {:?}",
        worker_id,
        running_tasks
    );
    let _ = core.remove_worker(worker_id);

    comm.broadcast_worker_message(&ToWorkerMessage::LostWorker(worker_id));

    // IMPORTANT: We need to announce lost worker before failing the jobs
    // so in journal restoration we can detect what tasks were running
    // without explicit logging
    comm.client()
        .on_worker_lost(worker_id, &running_tasks, reason.clone());

    for task_id in running_tasks {
        let task = core.get_task_mut(task_id);
        if CrashLimit::NeverRestart == task.configuration.crash_limit {
            log::debug!("Task {} with never restart flag crashed", task_id);
            let error_info = TaskFailInfo {
                message: "Task was running on a lost worker while never restart flag was set."
                    .to_string(),
            };
            fail_task_helper(core, comm, None, task_id, error_info);
        } else if reason.is_failure() && task.increment_crash_counter() {
            let count = task.crash_counter;
            log::debug!("Task {} reached crash limit {}", task_id, count);
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
        // We assign scheduler priority here, the goal is to set scheduler_priority as follows = t-level * T_LEVEL_WEIGHT - job_id
        // where t-level is the length of the maximal path from root tasks
        // Goal is to prioritize task graph components that were partially computed + prioritize older tasks (according job_id)
        // T-level is T_LEVEL_WEIGHT-times more important than job_id difference,
        // but large job_id difference will overweight t-level which is usually bounded, that is done by design.
        let mut priority = -(task.id.job_id().as_num() as i32);
        task.task_deps.retain(|t| {
            if let Some(task_dep) = core.find_task_mut(*t) {
                task_dep.add_consumer(task.id);
                if !task_dep.is_finished() {
                    priority =
                        std::cmp::max(priority, task_dep.scheduler_priority + T_LEVEL_WEIGHT);
                    count += 1
                }
                true
            } else {
                false
            }
        });
        task.set_scheduler_priority(priority);
        assert!(matches!(
            task.state,
            TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 })
        ));
        task.state = TaskRuntimeState::Waiting(WaitingInfo {
            unfinished_deps: count,
        });
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
        id: task_id,
        context,
    } = message;

    let (tasks, workers) = core.split_tasks_workers_mut();
    let simple_worker_list = &[worker_id];
    if let Some(task) = tasks.find_task_mut(task_id) {
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
            | TaskRuntimeState::Finished => {
                unreachable!()
            }
        };

        comm.client()
            .on_task_started(task_id, task.instance_id, worker_ids, context);
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

pub(crate) fn on_task_finished(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    msg: TaskFinishedMsg,
) {
    let task_id = msg.id;
    {
        let (tasks, workers) = core.split_tasks_workers_mut();
        if let Some(task) = tasks.find_task_mut(msg.id) {
            log::debug!(
                "Task id={} finished on worker={}; outputs={:?}",
                task_id,
                worker_id,
                &msg.outputs
            );

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
                TaskRuntimeState::Waiting(_) | TaskRuntimeState::Finished => {
                    unreachable!();
                }
            }

            task.state = TaskRuntimeState::Finished;
            comm.ask_for_scheduling();
            comm.client().on_task_finished(task_id);
        } else {
            log::debug!("Unknown task finished id={}", task_id);
            return;
        }
    }

    let consumers: Vec<TaskId> = {
        let task = core.get_task(msg.id);
        task.get_consumers().iter().copied().collect()
    };

    let output_ids_set: Set<OutputId> = msg.outputs.iter().map(|o| o.id).collect();
    let mut missing_inputs: Map<TaskId, Vec<OutputId>> = Map::new();

    let mut data_ref_counts: Map<OutputId, RefCount> = Map::new();

    for consumer in consumers {
        let id = {
            let t = core.get_task_mut(consumer);
            for obj_id in &t.data_deps {
                let data_id = obj_id.data_id;
                if obj_id.task_id == task_id {
                    if !output_ids_set.contains(&data_id) {
                        let data_list = missing_inputs.entry(consumer).or_default();
                        data_list.push(data_id);
                    } else {
                        let cs = data_ref_counts.entry(data_id).or_insert(0);
                        *cs += 1;
                    }
                }
            }
            if t.decrease_unfinished_deps() {
                t.id
            } else {
                continue;
            }
        };
        core.add_ready_to_assign(id);
        comm.ask_for_scheduling();
    }
    let mut objs_to_remove = ObjsToRemoveFromWorkers::new();
    let state = core.remove_task(msg.id, &mut objs_to_remove);
    assert!(matches!(state, TaskRuntimeState::Finished));

    for data in msg.outputs {
        let obj_id = DataObjectId::new(task_id, data.id);
        if let Some(ref_count) = data_ref_counts.get(&data.id) {
            core.add_data_object(DataObjectHandle::new(
                obj_id, worker_id, data.size, *ref_count,
            ));
        } else {
            objs_to_remove.add(worker_id, obj_id);
        }
    }

    objs_to_remove.send(comm);

    for (dep_task_id, missing_data_ids) in missing_inputs {
        let mut message = format!(
            "Task {task_id} did not produced expected output(s): {}",
            missing_data_ids[0]
        );
        const LIMIT: usize = 16;
        for data_id in missing_data_ids.iter().skip(1).take(LIMIT - 1) {
            write!(&mut message, ", {data_id}").unwrap();
        }
        if missing_data_ids.len() > LIMIT {
            message.write_str(", ...").unwrap();
        }
        fail_task_helper(core, comm, None, dep_task_id, TaskFailInfo { message })
    }
}

pub(crate) fn on_steal_response(
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
                    panic!("Invalid state of task={task_id} when steal response occurred");
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

        let task = core.get_task_mut(task_id);
        if let TaskRuntimeState::Waiting(_winfo) = &new_state {
            task.set_fresh_flag(true)
        };
        task.state = new_state;
    }
}

fn fail_task_helper(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: Option<WorkerId>,
    task_id: TaskId,
    error_info: TaskFailInfo,
) {
    let consumers: Vec<TaskId> = {
        let (tasks, workers) = core.split_tasks_workers_mut();
        if let Some(task) = tasks.find_task(task_id) {
            log::debug!("Task task_id={} failed", task_id);
            if let Some(worker_id) = worker_id {
                if task.configuration.resources.is_multi_node() {
                    let ws = task.mn_placement().unwrap();
                    assert_eq!(ws[0], worker_id);
                    reset_mn_task_workers(workers, ws, task_id);
                } else {
                    assert!(task.is_assigned_or_stolen_from(worker_id));
                    workers.get_worker_mut(worker_id).remove_sn_task(task);
                }
            } else {
                assert!(task.is_waiting())
            }
            let mut s = Set::new();
            task.collect_recursive_consumers(tasks, &mut s);
            s.into_iter().collect()
        } else {
            log::debug!("Unknown task failed");
            return;
        }
    };

    let mut objs_to_remove = ObjsToRemoveFromWorkers::new();

    for &consumer in &consumers {
        log::debug!("Task={} canceled because of failed dependency", consumer);
        assert!(matches!(
            core.remove_task(consumer, &mut objs_to_remove),
            TaskRuntimeState::Waiting(_)
        ));
    }
    let state = core.remove_task(task_id, &mut objs_to_remove);
    if worker_id.is_some() {
        assert!(matches!(
            state,
            TaskRuntimeState::Assigned(_)
                | TaskRuntimeState::Running { .. }
                | TaskRuntimeState::Stealing(_, _)
                | TaskRuntimeState::RunningMultiNode(_)
        ));
    } else {
        assert!(matches!(state, TaskRuntimeState::Waiting(_)));
    }
    drop(state);
    objs_to_remove.send(comm);
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

    let (tasks, workers) = core.split_tasks_workers_mut();
    for &task_id in task_ids {
        log::debug!("Canceling task id={}", task_id);
        if let Some(task) = tasks.find_task(task_id) {
            to_unregister.insert(task_id);
            task.collect_recursive_consumers(tasks, &mut to_unregister);
            match task.state {
                TaskRuntimeState::Waiting(_) => {}
                TaskRuntimeState::Assigned(w_id)
                | TaskRuntimeState::Running {
                    worker_id: w_id, ..
                } => {
                    workers.get_worker_mut(w_id).remove_sn_task(task);
                    running_ids.entry(w_id).or_default().push(task_id);
                }
                TaskRuntimeState::RunningMultiNode(ref ws) => {
                    for w_id in ws {
                        workers.get_worker_mut(*w_id).reset_mn_task();
                    }
                    running_ids.entry(ws[0]).or_default().push(task_id);
                }
                TaskRuntimeState::Stealing(from_id, to_id) => {
                    if let Some(to_id) = to_id {
                        workers.get_worker_mut(to_id).remove_sn_task(task);
                    }
                    running_ids.entry(from_id).or_default().push(task_id);
                }
                TaskRuntimeState::Finished => unreachable!(),
            };
        } else {
            log::debug!("Task is not here");
        }
    }

    let mut objs_to_remove = ObjsToRemoveFromWorkers::new();
    core.remove_tasks_batched(&to_unregister, &mut objs_to_remove);

    for (w_id, ids) in running_ids {
        comm.send_worker_message(w_id, &ToWorkerMessage::CancelTasks(TaskIdsMsg { ids }));
    }
    objs_to_remove.send(comm); // This needs to be sent after tasks are cancelled
    comm.ask_for_scheduling();
}

pub(crate) fn on_resolve_placement(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    data_id: DataObjectId,
) {
    // TODO: Maube randomize what placement to return?
    let placement = core
        .dataobj_map()
        .find_data_object(data_id)
        .and_then(|obj| obj.placement().iter().next().copied());
    comm.send_worker_message(
        worker_id,
        &ToWorkerMessage::PlacementResponse(data_id, placement),
    );
}
