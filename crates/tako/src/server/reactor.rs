use crate::common::{Set};

use crate::server::core::Core;

use crate::messages::worker::{
    NewWorkerMsg, StealResponse, StealResponseMsg, TaskFinishedMsg, TaskIdMsg,
    ToWorkerMessage,
};


use crate::server::comm::Comm;
use crate::server::task::{Task, WaitingInfo, FinishInfo};
use crate::{WorkerId, TaskId};
use crate::server::task::{DataInfo, TaskRef, TaskRuntimeState};
use crate::messages::worker::ComputeTaskMsg;

use crate::common::trace::{
    trace_task_assign, trace_task_finish, trace_task_place, trace_worker_new,
    trace_worker_steal_response, trace_worker_steal_response_missing,
};
use crate::server::worker::Worker;
use crate::messages::common::{TaskFailInfo, SubworkerDefinition};

pub fn on_new_worker(core: &mut Core, comm: &mut impl Comm, worker: Worker) {
    {
        trace_worker_new(worker.id, worker.ncpus, worker.address());

        comm.broadcast_worker_message(&ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: worker.id,
            address: worker.listen_address.clone(),
        }));

        /*comm.send_scheduler_message(ToSchedulerMessage::NewWorker(worker.make_sched_info()));*/
        comm.ask_for_scheduling();
    }
    core.new_worker(worker);
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
            assert!(matches!(task.state, TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 } )));
            task.state = TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: count });
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

            {
                let mut worker = core.get_worker_mut_by_id_or_panic(worker_id);
                assert!(worker.tasks.remove(&task_ref));
            }

            let mut placement = Set::new();
            placement.insert(worker_id);
            task.state = TaskRuntimeState::Finished(
                FinishInfo {
                    data_info: DataInfo {
                        size: msg.size,
                    },
                    placement,
                    future_placement: Default::default(),
                }
            );
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
                        trace_worker_steal_response(task.id, worker_id, 0, "done");
                        continue;
                    }
                    let (from_worker_id, to_worker_id) = if let TaskRuntimeState::Stealing(from_w, to_w) = &task.state {
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
                        to_worker_id,
                        match response {
                            StealResponse::Ok => "ok",
                            StealResponse::NotHere => "nothere",
                            StealResponse::Running => "running"
                        }
                    );

                    match response {
                        StealResponse::Ok => {
                            log::debug!("Task stealing was successful task={}", task_id);
                            trace_task_assign(task_id, to_worker_id);
                            comm.send_worker_message(to_worker_id, &task.make_compute_message());
                            log::debug!("Task stealing was successful task={}", task_id);
                            TaskRuntimeState::Assigned(to_worker_id)
                        },
                        StealResponse::Running => {
                            log::debug!("Task stealing was not successful task={}", task_id);
                            assert!(core.get_worker_mut_by_id_or_panic(to_worker_id).tasks.remove(&task_ref));
                            assert!(core.get_worker_mut_by_id_or_panic(from_worker_id).tasks.insert(task_ref.clone()));
                            comm.ask_for_scheduling();
                            TaskRuntimeState::Running(worker_id)
                        },
                        StealResponse::NotHere => {
                            panic!("Received NotHere while stealing, it seems that Finished message got lost");
                        }

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
    task.set_keep_flag(false);
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
        assert!(core.get_worker_mut_by_id_or_panic(worker_id).tasks.remove(&task_ref));
        task_ref.get().collect_consumers().into_iter().collect()
    };

    unregister_as_consumer(core, comm, &mut task_ref.get_mut(), &task_ref);

    for task_ref in &task_refs {
        let mut task = task_ref.get_mut();
        log::debug!("Task={} canceled because of failed dependency", task.id);
        assert!(task.is_waiting());
        unregister_as_consumer(core, comm, &mut task, &task_ref);
    }

    assert!(matches!(
        core.remove_task(&mut task_ref.get_mut()),
        TaskRuntimeState::Assigned(_) | TaskRuntimeState::Stealing(_, _)
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

/*#[inline]
fn send_compute(core: &mut Core, comm: &mut impl Comm, task: &Task, worker_id: WorkerId) {
    trace_task_assign(task.id, worker_id);
    comm.send_worker_message(worker_id, &task.make_compute_message(core));
}*/

fn unregister_as_consumer(
    core: &mut Core,
    comm: &mut impl Comm,
    task: &mut Task,
    task_ref: &TaskRef,
) {
    for tr in &task.inputs {
        //let tr = core.get_task_by_id_or_panic(*input_id).clone();
        let mut t = tr.get_mut();
        assert!(t.remove_consumer(&task_ref));
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


#[cfg(test)]
mod tests {
    use super::*;
        use crate::server::test_util::{create_test_comm, task, task_with_deps, create_test_workers, submit_test_tasks, submit_example_1, start_and_finish_on_worker, start_on_worker, sorted_vec, finish_on_worker, force_assign, force_reassign};
    use crate::scheduler::scheduler::tests::{create_test_scheduler};
    /*use crate::test_util::{
        create_test_comm, create_test_workers, finish_on_worker, sorted_vec,
        start_and_finish_on_worker, start_on_worker, submit_example_1, submit_test_tasks, task,
        task_with_deps,
    };*/

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

        comm.check_need_scheduling();
        comm.emptiness_check();
        assert_eq!(core.get_workers().count(), 1);

        let worker = Worker::new(502, 20, "test2:123".into());
        on_new_worker(&mut core, &mut comm, worker);
        assert!(
            matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: 502, address: ref a
        }) if a == "test2:123")
        );
        comm.check_need_scheduling();
        comm.emptiness_check();
        assert_eq!(core.get_workers().count(), 2);
    }

    #[test]
    fn test_submit_jobs() {
        let mut core = Core::default();
        let mut comm = create_test_comm();
        //new_workers(&mut core, &mut comm, vec![1]);

        let t1 = task(501);
        let t2 = task_with_deps(502, &[&t1]);
        on_new_tasks(&mut core, &mut comm, vec![t2, t1]);

        comm.check_need_scheduling();
        comm.emptiness_check();

        let t1 = core.get_task_by_id_or_panic(501).clone();
        let t2 = core.get_task_by_id_or_panic(502).clone();
        assert_eq!(t1.get().get_unfinished_deps(), 0);
        assert_eq!(t2.get().get_unfinished_deps(), 1);
        assert_eq!(t1.get().get_consumers().len(), 1);
        assert!(t1.get().get_consumers().contains(&t2));

        let t3 = task(604);
        let t4 = task_with_deps(602, &[&t1, &t3]);
        let t5 = task_with_deps(603, &[&t3]);
        let t6 = task_with_deps(601, &[&t3, &t4, &t5, &t2]);

        on_new_tasks(&mut core, &mut comm, vec![t6, t3, t4, t5]);
        comm.check_need_scheduling();
        comm.emptiness_check();

        let t4 = core.get_task_by_id_or_panic(602).clone();
        let t6 = core.get_task_by_id_or_panic(601).clone();

        assert_eq!(t1.get().get_consumers().len(), 2);
        assert!(t1.get().get_consumers().contains(&t2));
        assert!(t1.get().get_consumers().contains(&t4));
        assert_eq!(t1.get().get_unfinished_deps(), 0);

        assert_eq!(t2.get().get_consumers().len(), 1);
        assert!(t2.get().get_consumers().contains(&t6));

        assert_eq!(t1.get().get_unfinished_deps(), 0);
        assert_eq!(t2.get().get_unfinished_deps(), 1);
        assert_eq!(t4.get().get_unfinished_deps(), 2);
        assert_eq!(t6.get().get_unfinished_deps(), 4);
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
        t1.get_mut().user_priority = 12;
        let t2 = task(12);
        let t3 = task_with_deps(13, &[&t1, &t2]);
        t3.get_mut().set_keep_flag(true);
        let t4 = task(14);
        let t5 = task(15);
        //let t6 = task_with_deps(16, &[12]);
        let t7 = task_with_deps(17, &[&t4]);
        t7.get_mut().set_keep_flag(true);

        submit_test_tasks(&mut core, &[&t1, &t2, &t3, &t4, &t5, &t7]);
        let mut comm = create_test_comm();

        let mut scheduler = create_test_scheduler();

        force_assign(&mut core, &mut scheduler, 11, 100);
        force_assign(&mut core, &mut scheduler, 12, 101);
        force_assign(&mut core, &mut scheduler, 15, 100);

        assert!(t1.get().is_fresh());
        assert!(t3.get().is_fresh());

        scheduler.finish_scheduling(&mut comm);

        assert!(!t1.get().is_fresh());
        assert!(t3.get().is_fresh());

        assert_eq!(core.get_worker_by_id_or_panic(100).tasks.len(), 2);
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t1));
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t5));
        assert_eq!(core.get_worker_by_id_or_panic(101).tasks.len(), 1);
        assert!(core.get_worker_by_id_or_panic(101).tasks.contains(&t2));
        assert_eq!(core.get_worker_by_id_or_panic(102).tasks.len(), 0);

        let msgs = comm.take_worker_msgs(100, 2);
        dbg!(&msgs[0]);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg {
                id: 11,
                user_priority: 12,
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
        assert!(core.get_task_by_id_or_panic(13).get().is_waiting());
        assert!(core.get_task_by_id_or_panic(17).get().is_waiting());

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
        assert_eq!(core.get_worker_by_id_or_panic(100).tasks.len(), 1);
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t1));
        assert_eq!(core.get_worker_by_id_or_panic(101).tasks.len(), 1);
        assert!(core.get_worker_by_id_or_panic(101).tasks.contains(&t2));
        assert_eq!(core.get_worker_by_id_or_panic(102).tasks.len(), 0);


        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 15 })
        ));

        comm.check_need_scheduling();

        //assert_eq!(comm.take_client_task_finished(1), vec![15]);
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
        assert!(matches!(t5.get().state, TaskRuntimeState::Released));
        assert_eq!(core.get_worker_by_id_or_panic(100).tasks.len(), 1);
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t1));
        assert_eq!(core.get_worker_by_id_or_panic(101).tasks.len(), 0);
        assert_eq!(core.get_worker_by_id_or_panic(102).tasks.len(), 0);


        comm.check_need_scheduling();
        //assert_eq!(comm.take_client_task_finished(1), vec![12]);
        comm.emptiness_check();

        assert!(core.get_task_by_id(12).is_some());

        on_task_finished(
            &mut core,
            &mut comm,
            100,
            TaskFinishedMsg { id: 11, size: 1000 },
        );

        comm.check_need_scheduling();
        //assert_eq!(comm.take_client_task_finished(1), vec![11]);

        force_assign(&mut core, &mut scheduler, 13, 101);
        scheduler.finish_scheduling(&mut comm);

        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: 13, .. })
        ));

        comm.emptiness_check();

        on_set_observe_flag(&mut core, &mut comm, 13, true);

        on_task_finished(
            &mut core,
            &mut comm,
            101,
            TaskFinishedMsg { id: 13, size: 1000 },
        );

        comm.check_need_scheduling();

        assert_eq!(comm.take_client_task_finished(1), vec![13]);

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

        on_reset_keep_flag(&mut core, &mut comm, 13);
        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 13 })
        ));
        comm.emptiness_check();

        on_reset_keep_flag(&mut core, &mut comm, 17);
        comm.emptiness_check();
    }

    #[test]
    fn test_running_task_on_error() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100, 1000);
        start_and_finish_on_worker(&mut core, 12, 101, 1000);

        start_on_worker(&mut core, 13, 102);
        let t13 = core.get_task_by_id_or_panic(13).clone();
        assert!(t13.get().is_assigned());
        assert!(core.get_worker_by_id_or_panic(102).tasks.contains(&t13));

        let mut comm = create_test_comm();
        on_task_error(
            &mut core,
            &mut comm,
            102,
            13,
            TaskFailInfo {
                message: "".to_string(),
                data_type: "".to_string(),
                error_data: vec![]
            },
        );
        assert!(!core.get_worker_by_id_or_panic(102).tasks.contains(&t13));

        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            msgs[0],
            ToWorkerMessage::DeleteData(TaskIdMsg { id: 11 })
        ));
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
        start_and_finish_on_worker(&mut core, 11, 100, 1000);
        start_and_finish_on_worker(&mut core, 12, 101, 1000);
        start_on_worker(&mut core, 13, 101);

        let mut comm = create_test_comm();
        on_tasks_transferred(&mut core, &mut comm, 101, 11);

        comm.emptiness_check();

        let ws = core
            .get_task_by_id_or_panic(11)
            .get()
            .get_placement()
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
        start_and_finish_on_worker(&mut core, 11, 100, 1000);
        start_and_finish_on_worker(&mut core, 12, 101, 1000);
        start_on_worker(&mut core, 13, 101);

        let t3 = core.get_task_by_id_or_panic(13).clone();
        assert!(core.get_worker_by_id_or_panic(101).tasks.contains(&t3));
        assert!(!core.get_worker_by_id_or_panic(100).tasks.contains(&t3));

        let mut comm = create_test_comm();
        let mut scheduler = create_test_scheduler();

        force_reassign(&mut core, &mut scheduler, 13, 100);
        scheduler.finish_scheduling(&mut comm);

        assert!(!core.get_worker_by_id_or_panic(101).tasks.contains(&t3));
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t3));

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

        assert!(!core.get_worker_by_id_or_panic(101).tasks.contains(&t3));
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t3));

        let msgs = comm.take_worker_msgs(100, 1);
        assert!(matches!(
            &msgs[0],
            ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: 13, .. })
        ));
        comm.emptiness_check();
    }

    #[test]
    fn test_steal_tasks_running() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100, 1000);
        start_and_finish_on_worker(&mut core, 12, 101, 1000);
        start_on_worker(&mut core, 13, 101);

        let mut comm = create_test_comm();
        let mut scheduler = create_test_scheduler();

        force_reassign(&mut core, &mut scheduler, 13, 100);
        scheduler.finish_scheduling(&mut comm);

        let msgs = comm.take_worker_msgs(101, 1);
        assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![13]));
        comm.emptiness_check();

        let t3 = core.get_task_by_id_or_panic(13).clone();
        assert!(!core.get_worker_by_id_or_panic(101).tasks.contains(&t3));
        assert!(core.get_worker_by_id_or_panic(100).tasks.contains(&t3));


        on_steal_response(
            &mut core,
            &mut comm,
            101,
            StealResponseMsg {
                responses: vec![(13, StealResponse::Running)],
            },
        );

        assert!(core.get_worker_by_id_or_panic(101).tasks.contains(&t3));
        assert!(!core.get_worker_by_id_or_panic(100).tasks.contains(&t3));


        comm.check_need_scheduling();
        comm.emptiness_check();
    }

    #[test]
    #[should_panic]
    fn finish_unassigned_task() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1, 1]);
        submit_example_1(&mut core);
        finish_on_worker(&mut core, 11, 100, 1000);
    }
}
