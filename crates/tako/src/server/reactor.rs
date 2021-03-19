use crate::common::{Map, Set};

use crate::scheduler::protocol::{TaskStealResponse, TaskUpdate, TaskUpdateType};
use crate::server::core::Core;

use crate::messages::worker::{
    NewWorkerMsg, StealResponse, StealResponseMsg, TaskFinishedMsg, TaskIdMsg, TaskIdsMsg,
    ToWorkerMessage,
};

use crate::scheduler::{TaskAssignment, ToSchedulerMessage};
use crate::server::comm::Comm;
use crate::server::task::Task;
use crate::{WorkerId, TaskId};
use crate::server::task::{DataInfo, TaskRef, TaskRuntimeState};

use crate::common::trace::{
    trace_task_assign, trace_task_finish, trace_task_new, trace_task_place, trace_worker_new,
    trace_worker_steal, trace_worker_steal_response, trace_worker_steal_response_missing,
};
use crate::server::worker::Worker;
use crate::messages::gateway::ToGatewayMessage;
use crate::messages::common::{TaskFailInfo, SubworkerDefinition};

pub fn on_new_worker(core: &mut Core, comm: &mut impl Comm, worker: Worker) {
    {
        trace_worker_new(worker.id, worker.ncpus, worker.address());

        comm.broadcast_worker_message(&ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: worker.id,
            address: worker.listen_address.clone(),
        }));

        comm.send_scheduler_message(ToSchedulerMessage::NewWorker(worker.make_sched_info()));
    }
    core.new_worker(worker);
}

pub fn on_assignments(core: &mut Core, comm: &mut impl Comm, assignments: Vec<TaskAssignment>) {
    log::debug!("Assignments from scheduler: {:?}", assignments);
    let mut task_steals: Map<WorkerId, Vec<TaskId>> = Default::default();

    for assignment in assignments {
        let worker_id = assignment.worker;
        let task_id = assignment.task;
        let task_ref = core.get_task_by_id_or_panic(task_id).clone();
        task_ref.get_mut().scheduler_priority = assignment.priority;
        let state = {
            let task = task_ref.get();
            match &task.state {
                TaskRuntimeState::Waiting | TaskRuntimeState::Scheduled(_) => {
                    if task.is_ready() {
                        log::debug!(
                            "Task task={} scheduled & assigned to worker={}",
                            task_id,
                            worker_id
                        );
                        //core.compute_task(worker_ref.clone(), task_ref.clone(), comm);
                        send_compute(core, comm, &task, worker_id);
                        TaskRuntimeState::Assigned(worker_id)
                    } else {
                        log::debug!("Task task={} scheduled to worker={}", task_id, worker_id);
                        TaskRuntimeState::Scheduled(worker_id)
                    }
                }
                TaskRuntimeState::Assigned(w_id) => {
                    let previous_worker_id = *w_id;
                    log::debug!(
                        "Task task={} scheduled to worker={}; stealing (1) from worker={}",
                        task_id,
                        worker_id,
                        previous_worker_id
                    );
                    if previous_worker_id != worker_id {
                        trace_worker_steal(task.id, previous_worker_id, worker_id);
                        task_steals
                            .entry(previous_worker_id)
                            .or_default()
                            .push(task_id);
                        TaskRuntimeState::Stealing(previous_worker_id, worker_id)
                    } else {
                        TaskRuntimeState::Assigned(previous_worker_id)
                    }
                }
                TaskRuntimeState::Stealing(w_id, _) => {
                    log::debug!(
                        "Task task={} scheduled to worker={}; stealing (2) from worker={}",
                        task_id,
                        worker_id,
                        *w_id
                    );
                    TaskRuntimeState::Stealing(*w_id, worker_id)
                }
                TaskRuntimeState::Finished(_, _) | TaskRuntimeState::Released => {
                    log::debug!("Rescheduling non-active task={}", assignment.task);
                    continue;
                }
            }
        };
        task_ref.get_mut().state = state;
    }

    for (worker_id, task_ids) in task_steals {
        comm.send_worker_message(
            worker_id,
            &ToWorkerMessage::StealTasks(TaskIdsMsg { ids: task_ids }),
        );
    }
}

pub fn on_new_tasks(core: &mut Core, comm: &mut impl Comm, new_tasks: Vec<TaskRef>) {
    assert!(!new_tasks.is_empty());

    let mut lowest_id = TaskId::MAX;
    let mut highest_id = 0;

    for task_ref in &new_tasks {
        let task_id = task_ref.get().id;
        lowest_id = lowest_id.min(task_id);
        highest_id = highest_id.max(task_id);
        log::debug!("New task id={}", task_id);
        trace_task_new(task_id, &task_ref.get().dependencies);
        core.add_task(task_ref.clone());
    }

    core.update_max_task_id(highest_id);

    for task_ref in &new_tasks {
        let mut task = task_ref.get_mut();

        let mut count = 0;
        for task_id in &task.dependencies {
            let tr = core.get_task_by_id_or_panic(*task_id);
            let mut task_dep = tr.get_mut();
            task_dep.add_consumer(task_ref.clone());
            if *task_id >= lowest_id || !task_dep.is_finished() {
                count += 1
            }
        }
        assert_eq!(task.unfinished_inputs, 0);
        task.unfinished_inputs = count;
    }

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
    comm.send_scheduler_message(ToSchedulerMessage::NewTasks(scheduler_infos));
}

pub fn on_task_finished(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    msg: TaskFinishedMsg,
) {
    let task_ref = core.get_task_by_id_or_panic(msg.id).clone();
    {
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
            let mut set = Set::new();
            set.insert(worker_id);
            task.state = TaskRuntimeState::Finished(
                DataInfo {
                    size: msg.size,
                    //r#type: msg.r#type,
                },
                set,
            );
            comm.send_scheduler_message(ToSchedulerMessage::TaskUpdate(TaskUpdate {
                state: TaskUpdateType::Finished,
                id: task.id,
                worker: worker_id,
                size: Some(task.data_info().unwrap().size),
            }));
            if task.is_observed() {
                comm.send_client_task_finished(task.id);
            }
        };

        {
            let task = task_ref.get();
            for consumer in task.get_consumers() {
                let mut t = consumer.get_mut();
                assert!(t.is_waiting() || t.is_scheduled());
                t.unfinished_inputs -= 1;
                if t.unfinished_inputs == 0 {
                    let wr = match &t.state {
                        TaskRuntimeState::Scheduled(w) => Some(*w),
                        _ => None,
                    };
                    if let Some(w) = wr {
                        t.state = TaskRuntimeState::Assigned(w);
                        send_compute(core, comm, &t, w);
                    }
                }
            }
        }

        let mut task = task_ref.get_mut();
        unregister_as_consumer(core, comm, &mut task, &task_ref);
        remove_task_if_possible(core, comm, &mut task);

        //self.notify_key_in_memory(&task_ref, notifications);
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
        let task_ref = core.get_task_by_id(task_id);
        match task_ref {
            Some(task_ref) => {
                let new_state = {
                    let task = task_ref.get();
                    if task.is_done() {
                        log::debug!("Received trace response for finished task={}", task_id);
                        trace_worker_steal_response(task.id, worker_id, 0, "done");
                        continue;
                    }
                    let to_worker_id = if let TaskRuntimeState::Stealing(from_w, to_w) = &task.state {
                        assert_eq!(*from_w, worker_id);
                        *to_w
                    } else {
                        panic!(
                            "Invalid state of task={} when steal response occured",
                            task_id
                        );
                    };

                    let success = matches!(response, StealResponse::Ok);
                    comm.send_scheduler_message(ToSchedulerMessage::TaskStealResponse(
                        TaskStealResponse {
                            id: task_id,
                            from_worker: worker_id,
                            to_worker: to_worker_id,
                            success,
                        },
                    ));

                    trace_worker_steal_response(
                        task_id,
                        worker_id,
                        to_worker_id,
                        if success { "success" } else { "fail" },
                    );

                    if success {
                        log::debug!("Task stealing was successful task={}", task_id);
                        trace_task_assign(task_id, to_worker_id);
                        comm.send_worker_message(to_worker_id, &task.make_compute_message(core));
                        TaskRuntimeState::Assigned(to_worker_id)
                    } else {
                        log::debug!("Task stealing was not successful task={}", task_id);
                        TaskRuntimeState::Assigned(worker_id)
                    }
                };
                task_ref.get_mut().state = new_state;
            }
            None => {
                log::debug!("Received trace resposne for invalid task {}", task_id);
                trace_worker_steal_response_missing(task_id, worker_id)
            }
        }
    }
}

pub fn on_reset_keep_flag(core: &mut Core, comm: &mut impl Comm, task_id: TaskId) {
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    let mut task = task_ref.get_mut();
    task_ref.get_mut().set_keep_flag(false);
    remove_task_if_possible(core, comm, &mut task);
}

pub fn on_set_keep_flag(core: &mut Core, task_id: TaskId) -> TaskRef {
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    task_ref.get_mut().set_keep_flag(true);
    task_ref
}

pub fn on_set_observe_flag(core: &mut Core, comm: &mut impl Comm, task_id: TaskId, value: bool) -> bool {
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
    let task_ref = core.get_task_by_id_or_panic(task_id).clone();
    log::debug!("Task task {} failed", task_id);

    let task_refs: Vec<TaskRef> = {
        assert!(task_ref.get().is_assigned_or_stealed_from(worker_id));
        task_ref.get().collect_consumers().into_iter().collect()
    };

    unregister_as_consumer(core, comm, &mut task_ref.get_mut(), &task_ref);

    for task_ref in &task_refs {
        let mut task = task_ref.get_mut();
        log::debug!("Task={} canceled because of failed dependency", task.id);
        assert!(task.is_waiting() || task.is_scheduled());
        unregister_as_consumer(core, comm, &mut task, &task_ref);
    }

    assert!(matches!(
        core.remove_task(&mut task_ref.get_mut()),
        TaskRuntimeState::Assigned(_) | TaskRuntimeState::Stealing(_, _)
    ));
    comm.send_scheduler_message(ToSchedulerMessage::RemoveTask(task_ref.get().id));

    for task_ref in &task_refs {
        // We can drop the resulting state as checks was done earlier
        let mut task = task_ref.get_mut();
        assert!(matches!(
            core.remove_task(&mut task),
            TaskRuntimeState::Waiting | TaskRuntimeState::Scheduled(_)
        ));
        comm.send_scheduler_message(ToSchedulerMessage::RemoveTask(task.id));
    }

    comm.send_client_task_error(
        task_id,
        task_refs.iter().map(|tr| tr.get().id).collect(),
        error_info,
    );
}

pub fn on_tasks_transferred(
    core: &mut Core,
    comm: &mut impl Comm,
    worker_id: WorkerId,
    id: TaskId,
) {
    log::debug!("Task id={} transfered on worker={}", id, worker_id);
    // TODO handle the race when task is removed from server before this message arrives
    if let Some(task_ref) = core.get_task_by_id(id) {
        let mut task = task_ref.get_mut();
        match &mut task.state {
            TaskRuntimeState::Finished(_, ws) => {
                ws.insert(worker_id);
            }
            TaskRuntimeState::Released
            | TaskRuntimeState::Waiting
            | TaskRuntimeState::Scheduled(_)
            | TaskRuntimeState::Assigned(_)
            | TaskRuntimeState::Stealing(_, _) => {
                panic!("Invalid task state");
            }
        };
        trace_task_place(task.id, worker_id);
        comm.send_scheduler_message(ToSchedulerMessage::TaskUpdate(TaskUpdate {
            id,
            state: TaskUpdateType::Placed,
            worker: worker_id,
            size: None,
        }));
    } else {
        log::debug!(
            "Task id={} is not known to server; replaying with delete",
            id
        );
        comm.send_worker_message(worker_id, &ToWorkerMessage::DeleteData(TaskIdMsg { id }));
    }
}

pub fn on_register_subworker(
    core: &mut Core,
    comm: &mut impl Comm,
    sw_def: SubworkerDefinition
) -> crate::Result<()> {
    core.add_subworker_definition(sw_def.clone())?;
    comm.broadcast_worker_message(&ToWorkerMessage::RegisterSubworker(sw_def));
    Ok(())
}

#[inline]
fn send_compute(core: &mut Core, comm: &mut impl Comm, task: &Task, worker_id: WorkerId) {
    trace_task_assign(task.id, worker_id);
    comm.send_worker_message(worker_id, &task.make_compute_message(core));
}

fn unregister_as_consumer(
    core: &mut Core,
    comm: &mut impl Comm,
    task: &mut Task,
    task_ref: &TaskRef,
) {
    for input_id in &task.dependencies {
        let tr = core.get_task_by_id_or_panic(*input_id).clone();
        let mut t = tr.get_mut();
        assert!(t.remove_consumer(&task_ref));
        remove_task_if_possible(core, comm, &mut t);
    }
}

fn remove_task_if_possible(core: &mut Core, comm: &mut impl Comm, task: &mut Task) {
    if task.is_removable() {
        let ws = match core.remove_task(task) {
            TaskRuntimeState::Finished(_, ws) => ws,
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
        comm.send_scheduler_message(ToSchedulerMessage::RemoveTask(task.id));
        comm.send_client_task_removed(task.id);
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::protocol::WorkerInfo;
    use crate::server::protocol::messages::worker::ComputeTaskMsg;
    use crate::test_util::{
        create_test_comm, create_test_workers, finish_on_worker, sorted_vec,
        start_and_finish_on_worker, start_on_worker, submit_example_1, submit_test_tasks, task,
        task_with_deps,
    };

    #[test]
    fn test_worker_add() {
        let mut core = Core::default();
        assert_eq!(core.get_workers().count(), 0);

        let mut comm = create_test_comm();
        comm.emptiness_check();

        let worker = Worker::new(402, 20, "test1:123".into());
        on_new_worker(&mut core, &mut comm, worker);

        assert!(
            matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: 402, address: ref a
        }) if a == "test1:123")
        );

        assert!(matches!(
            comm.take_scheduler_msgs(1)[0],
            ToSchedulerMessage::NewWorker(WorkerInfo { id: 402, .. })
        ));

        comm.emptiness_check();
        assert_eq!(core.get_workers().count(), 1);

        let worker = Worker::new(502, 20, "test2:123".into());
        on_new_worker(&mut core, &mut comm, worker);
        assert!(
            matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: 502, address: ref a
        }) if a == "test2:123")
        );
        assert!(matches!(
            comm.take_scheduler_msgs(1)[0],
            ToSchedulerMessage::NewWorker(WorkerInfo { id: 502, .. })
        ));
        comm.emptiness_check();
        assert_eq!(core.get_workers().count(), 2);
    }

    #[test]
    fn test_submit_jobs() {
        let mut core = Core::default();
        let mut comm = create_test_comm();
        //new_workers(&mut core, &mut comm, vec![1]);

        let t1 = task(501);
        let t2 = task_with_deps(502, &[501]);
        on_new_tasks(&mut core, &mut comm, vec![t2, t1]);

        match &comm.take_scheduler_msgs(1)[0] {
            ToSchedulerMessage::NewTasks(ts) => {
                assert_eq!(ts.len(), 2);
                assert_eq!(ts[0].id, 501);
                assert_eq!(ts[1].id, 502);
            }
            _ => unreachable!(),
        }
        comm.emptiness_check();

        let t1 = core.get_task_by_id_or_panic(501).clone();
        let t2 = core.get_task_by_id_or_panic(502).clone();
        assert_eq!(t1.get().unfinished_inputs, 0);
        assert_eq!(t2.get().unfinished_inputs, 1);
        assert_eq!(t1.get().get_consumers().len(), 1);
        assert!(t1.get().get_consumers().contains(&t2));

        let t3 = task(604);
        let t4 = task_with_deps(602, &[501, 604]);
        let t5 = task_with_deps(603, &[604]);
        let t6 = task_with_deps(601, &[604, 602, 603, 502]);

        on_new_tasks(&mut core, &mut comm, vec![t6, t3, t4, t5]);

        match &comm.take_scheduler_msgs(1)[0] {
            ToSchedulerMessage::NewTasks(ts) => {
                assert_eq!(ts.len(), 4);
                assert_eq!(ts[0].id, 604);
                assert_eq!(ts[3].id, 601);
                assert_eq!(ts[3].inputs, vec![604, 602, 603, 502]);
            }
            _ => unreachable!(),
        }
        comm.emptiness_check();

        let t4 = core.get_task_by_id_or_panic(602).clone();
        let t6 = core.get_task_by_id_or_panic(601).clone();

        assert_eq!(t1.get().get_consumers().len(), 2);
        assert!(t1.get().get_consumers().contains(&t2));
        assert!(t1.get().get_consumers().contains(&t4));
        assert_eq!(t1.get().unfinished_inputs, 0);

        assert_eq!(t2.get().get_consumers().len(), 1);
        assert!(t2.get().get_consumers().contains(&t6));

        assert_eq!(t1.get().unfinished_inputs, 0);
        assert_eq!(t2.get().unfinished_inputs, 1);
        assert_eq!(t4.get().unfinished_inputs, 2);
        assert_eq!(t6.get().unfinished_inputs, 4);
    }

    #[test]
    fn test_assignments_and_finish() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);

        /*
           t1   t2    t4  t5
            \   /     |
              t3[k]   t7[k]
        */

        let t1 = task(11);
        let t2 = task(12);
        let t3 = task_with_deps(13, &[11, 12]);
        t3.get_mut().increment_keep_counter();
        let t4 = task(14);
        let t5 = task(15);
        //let t6 = task_with_deps(16, &[12]);
        let t7 = task_with_deps(17, &[14]);
        t7.get_mut().increment_keep_counter();

        submit_test_tasks(&mut core, &[t1, t2, t3, t4, t5, t7]);
        let mut comm = create_test_comm();
        on_assignments(
            &mut core,
            &mut comm,
            vec![
                TaskAssignment {
                    task: 11,
                    worker: 100,
                    priority: 12,
                },
                TaskAssignment {
                    task: 12,
                    worker: 101,
                    priority: 0,
                },
                TaskAssignment {
                    task: 13,
                    worker: 101,
                    priority: 0,
                },
                TaskAssignment {
                    task: 15,
                    worker: 100,
                    priority: 0,
                },
            ],
        );

        let msgs = comm.take_worker_msgs(100, 2);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg {
                id: 11,
                scheduler_priority: 12,
                ..
            })
        ));
        assert!(matches!(
            msgs[1],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg {
                id: 15,
                scheduler_priority: 0,
                ..
            })
        ));
        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: 12, .. })
        ));
        comm.emptiness_check();

        assert!(core.get_task_by_id_or_panic(11).get().is_assigned());
        assert!(core.get_task_by_id_or_panic(12).get().is_assigned());
        assert!(core.get_task_by_id_or_panic(13).get().is_scheduled());
        assert!(core.get_task_by_id_or_panic(17).get().is_waiting());

        /*let w1 = core.get_worker_by_id_or_panic(100).clone();
        let w2 = core.get_worker_by_id_or_panic(101).clone();*/
        let t5 = core.get_task_by_id_or_panic(15).clone();

        assert!(t5.get().is_assigned());

        // FINISH TASK WITHOUT CONSUMERS & KEEP FLAG
        on_task_finished(
            &mut core,
            &mut comm,
            100,
            TaskFinishedMsg { id: 15, size: 301 },
        );

        assert!(matches!(t5.get().state, TaskRuntimeState::Released));

        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 15 })
        ));
        let msgs = comm.take_scheduler_msgs(2);
        assert!(matches!(
            msgs[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: 15,
                worker: 100,
                size: Some(301),
                state: TaskUpdateType::Finished
            })
        ));
        assert!(matches!(msgs[1], ToSchedulerMessage::RemoveTask(15)));
        //assert_eq!(comm.take_client_task_finished(1), vec![15]);
        assert_eq!(comm.take_client_task_removed(1), vec![15]);
        comm.emptiness_check();

        assert!(core.get_task_by_id(15).is_none());

        // FINISHE TASK WITH CONSUMERS
        let t2 = core.get_task_by_id_or_panic(12).clone();
        assert!(t2.get().is_assigned());

        on_task_finished(
            &mut core,
            &mut comm,
            101,
            TaskFinishedMsg { id: 12, size: 5000 },
        );

        assert!(t2.get().is_finished());

        let msgs = comm.take_scheduler_msgs(1);
        assert!(matches!(
            msgs[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: 12,
                worker: 101,
                size: Some(5000),
                state: TaskUpdateType::Finished
            })
        ));
        //assert_eq!(comm.take_client_task_finished(1), vec![12]);
        comm.emptiness_check();

        assert!(core.get_task_by_id(12).is_some());

        on_task_finished(
            &mut core,
            &mut comm,
            100,
            TaskFinishedMsg { id: 11, size: 1000 },
        );

        let msgs = comm.take_scheduler_msgs(1);
        assert!(matches!(
            msgs[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: 11,
                worker: 100,
                size: Some(1000),
                state: TaskUpdateType::Finished
            })
        ));
        //assert_eq!(comm.take_client_task_finished(1), vec![11]);

        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: 13, .. })
        ));

        comm.emptiness_check();

        on_task_finished(
            &mut core,
            &mut comm,
            101,
            TaskFinishedMsg { id: 13, size: 1000 },
        );

        let msgs = comm.take_scheduler_msgs(3);
        assert!(matches!(
            msgs[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: 13,
                worker: 101,
                size: Some(1000),
                state: TaskUpdateType::Finished
            })
        ));
        assert!(matches!(msgs[1], ToSchedulerMessage::RemoveTask(11)));
        assert!(matches!(msgs[2], ToSchedulerMessage::RemoveTask(12)));
        assert_eq!(comm.take_client_task_finished(1), vec![13]);
        assert_eq!(comm.take_client_task_removed(2), vec![11, 12]);

        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 11 })
        ));

        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 12 })
        ));
        comm.emptiness_check();

        on_keep_counter_decrease(&mut core, &mut comm, 13);
        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 13 })
        ));
        assert_eq!(comm.take_client_task_removed(1), vec![13]);

        let msgs = comm.take_scheduler_msgs(1);
        assert!(matches!(msgs[0], ToSchedulerMessage::RemoveTask(13)));
        comm.emptiness_check();

        on_keep_counter_decrease(&mut core, &mut comm, 17);
        comm.emptiness_check();
    }

    #[test]
    fn test_running_task_on_error() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100);
        start_and_finish_on_worker(&mut core, 12, 101);

        start_on_worker(&mut core, 13, 102);
        assert!(core.get_task_by_id_or_panic(13).get().is_assigned());

        let mut comm = create_test_comm();
        on_task_error(
            &mut core,
            &mut comm,
            102,
            13,
            ErrorInfo {
                exception: Default::default(),
                traceback: Default::default(),
            },
        );
        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 11 })
        ));
        assert_eq!(
            sorted_vec(comm.take_scheduler_removal()),
            vec![11, 13, 15, 16, 17]
        );
        assert_eq!(comm.take_client_task_removed(1), vec![11]);
        let mut msgs = comm.take_client_task_errors(1);
        let (id, cs, _) = msgs.pop().unwrap();
        assert_eq!(id, 13);
        assert_eq!(sorted_vec(cs), vec![15, 16, 17]);
        comm.emptiness_check();
    }

    #[test]
    fn test_running_task_on_task_transferred_invalid() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        let mut comm = create_test_comm();
        on_tasks_transferred(&mut core, &mut comm, 102, 42);
        let msgs = comm.take_worker_msgs(102, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 42 })
        ));
        comm.emptiness_check();
    }

    #[test]
    fn test_running_task_on_task_transferred() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100);
        start_and_finish_on_worker(&mut core, 12, 101);
        start_on_worker(&mut core, 13, 101);

        let mut comm = create_test_comm();
        on_tasks_transferred(&mut core, &mut comm, 101, 11);

        let msgs = comm.take_scheduler_msgs(1);
        assert!(matches!(
            msgs[0],
            ToSchedulerMessage::TaskUpdate(TaskUpdate {
                id: 11,
                worker: 101,
                size: None,
                state: TaskUpdateType::Placed
            })
        ));
        comm.emptiness_check();

        let ws = core
            .get_task_by_id_or_panic(11)
            .get()
            .get_workers()
            .unwrap()
            .clone();
        let mut set = Set::new();
        set.insert(100);
        set.insert(101);
        assert_eq!(ws, set);
    }

    #[test]
    fn test_steal_tasks_ok() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100);
        start_and_finish_on_worker(&mut core, 12, 101);
        start_on_worker(&mut core, 13, 101);

        let mut comm = create_test_comm();
        on_assignments(
            &mut core,
            &mut comm,
            vec![TaskAssignment {
                task: 13,
                worker: 100,
                priority: 0,
            }],
        );
        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![13]));
        comm.emptiness_check();

        on_steal_response(
            &mut core,
            &mut comm,
            101,
            StealResponseMsg {
                responses: vec![
                    (13, StealResponse::Ok),
                    (123, StealResponse::NotHere),
                    (11, StealResponse::NotHere),
                ],
            },
        );

        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            &msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: 13, .. })
        ));

        let msgs = comm.take_scheduler_msgs(1);
        assert!(matches!(
            &msgs[0],
            ToSchedulerMessage::TaskStealResponse(TaskStealResponse {
                id: 13,
                to_worker: 100,
                from_worker: 101,
                success: true
            })
        ));
        comm.emptiness_check();
    }

    #[test]
    fn test_steal_tasks_running() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100);
        start_and_finish_on_worker(&mut core, 12, 101);
        start_on_worker(&mut core, 13, 101);

        let mut comm = create_test_comm();
        on_assignments(
            &mut core,
            &mut comm,
            vec![TaskAssignment {
                task: 13,
                worker: 100,
                priority: 0,
            }],
        );
        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![13]));
        comm.emptiness_check();

        on_steal_response(
            &mut core,
            &mut comm,
            101,
            StealResponseMsg {
                responses: vec![(13, StealResponse::Running)],
            },
        );

        let msgs = comm.take_scheduler_msgs(1);
        assert!(matches!(
            &msgs[0],
            ToSchedulerMessage::TaskStealResponse(TaskStealResponse {
                id: 13,
                to_worker: 100,
                from_worker: 101,
                success: false
            })
        ));
        comm.emptiness_check();
    }

    #[test]
    #[should_panic]
    fn finish_unassigned_task() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100);
        start_and_finish_on_worker(&mut core, 12, 101);
        finish_on_worker(&mut core, 13, 100);
    }
}
