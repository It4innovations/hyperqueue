#![cfg(test)]

use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::server::core::Core;
use crate::internal::server::task::Task;
use crate::internal::tests::utils::env::{create_test_comm, TestComm};
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_workers, finish_on_worker, submit_test_tasks,
};
use crate::internal::tests::utils::task::TaskBuilder;
use crate::{Priority, TaskId, WorkerId};

/*fn get_mn_placement(task: &Task) -> Vec<WorkerId> {
    match &task.state {
        TaskRuntimeState::RunningMultiNode(ws) => ws.clone(),
        _ => unreachable!(),
    }
}*/

#[derive(Debug)]
enum WorkerStatus {
    Root,
    NonRoot,
    None,
}

fn get_worker_status(ws: &[WorkerId], worker_id: WorkerId) -> WorkerStatus {
    if ws[0] == worker_id {
        WorkerStatus::Root
    } else if ws.contains(&worker_id) {
        WorkerStatus::NonRoot
    } else {
        WorkerStatus::None
    }
}

fn check_worker_status_change(s1: WorkerStatus, s2: WorkerStatus, ms: &[ToWorkerMessage]) {
    match (s1, s2) {
        (WorkerStatus::Root, WorkerStatus::Root) | (WorkerStatus::None, WorkerStatus::Root) => {
            assert!(matches!(ms, &[ToWorkerMessage::ComputeTask(_)]))
        }
        (WorkerStatus::NonRoot, WorkerStatus::Root) => assert!(matches!(
            ms,
            &[
                ToWorkerMessage::ComputeTask(_),
                ToWorkerMessage::SetReservation(false)
            ]
        )),
        (WorkerStatus::NonRoot, WorkerStatus::None) => {
            assert!(matches!(ms, &[ToWorkerMessage::SetReservation(false)]))
        }
        (WorkerStatus::None, WorkerStatus::NonRoot)
        | (WorkerStatus::Root, WorkerStatus::NonRoot) => {
            assert!(matches!(ms, &[ToWorkerMessage::SetReservation(true)]))
        }
        (WorkerStatus::NonRoot, WorkerStatus::NonRoot)
        | (WorkerStatus::Root, WorkerStatus::None)
        | (WorkerStatus::None, WorkerStatus::None) => {
            assert!(ms.is_empty())
        }
    }
}

#[test]
fn test_schedule_mn_simple() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[5, 5, 5, 5, 5]);

    let tasks: Vec<Task> = (1..=4)
        .map(|i| {
            TaskBuilder::new(i)
                .user_priority(i as Priority)
                .n_nodes(2)
                .build()
        })
        .collect();
    submit_test_tasks(&mut core, tasks);
    core.sanity_check();
    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();

    let test_mn_task = |task: &Task, comm: &mut TestComm, reservation: bool| -> Vec<WorkerId> {
        let ws = task.mn_placement().unwrap().to_vec();
        assert_eq!(ws.len(), 2);
        if let ToWorkerMessage::ComputeTask(m) = &comm.take_worker_msgs(ws[0], 1)[0] {
            assert_eq!(&m.node_list, &ws);
        } else {
            unreachable!()
        }
        if reservation {
            let msgs = comm.take_worker_msgs(ws[1], 1);
            assert!(matches!(&msgs[0], ToWorkerMessage::SetReservation(true)));
        }
        ws
    };

    let task3 = core.get_task(3.into());
    let ws3 = test_mn_task(&task3, &mut comm, true);
    let task4 = core.get_task(4.into());
    let ws4 = test_mn_task(&task4, &mut comm, true);
    for w in &ws4 {
        assert!(!ws3.contains(w));
    }
    assert!(core.get_task(2.into()).is_waiting());
    assert!(core.get_task(1.into()).is_waiting());
    comm.emptiness_check();

    finish_on_worker(&mut core, 3, ws3[0], 0);
    core.sanity_check();

    assert!(core.find_task(3.into()).is_none());
    for w in ws3 {
        assert!(core.get_worker_by_id_or_panic(w).mn_task().is_none());
    }

    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();

    let task2 = core.get_task(2.into());
    let ws2 = test_mn_task(&task2, &mut comm, false);
    comm.emptiness_check();

    finish_on_worker(&mut core, 3, ws2[0], 0);
    core.sanity_check();
}

#[test]
fn test_schedule_mn_reserve() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);

    let task1 = TaskBuilder::new(1).user_priority(10).n_nodes(3).build();
    let task2 = TaskBuilder::new(2).user_priority(5).n_nodes(2).build();
    let task3 = TaskBuilder::new(3).user_priority(0).n_nodes(3).build();

    submit_test_tasks(&mut core, vec![task1, task2, task3]);
    core.sanity_check();
    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();

    let ws1 = core.get_task(1.into()).mn_placement().unwrap().to_vec();
    assert!(matches!(
        comm.take_worker_msgs(ws1[0], 1)[0],
        ToWorkerMessage::ComputeTask(_)
    ));
    assert!(matches!(
        comm.take_worker_msgs(ws1[1], 1)[0],
        ToWorkerMessage::SetReservation(true)
    ));
    assert!(matches!(
        comm.take_worker_msgs(ws1[2], 1)[0],
        ToWorkerMessage::SetReservation(true)
    ));
    comm.emptiness_check();
    finish_on_worker(&mut core, 1, ws1[0], 0);
    scheduler.run_scheduling(&mut core, &mut comm);

    let ws2 = core.get_task(2.into()).mn_placement().unwrap().to_vec();
    for w in &[100, 101, 102] {
        let s1 = get_worker_status(&ws1, (*w).into());
        let s2 = get_worker_status(&ws2, (*w).into());
        let ms = comm.take_worker_msgs(*w, 0);
        check_worker_status_change(s1, s2, ms.as_slice());
    }
    comm.emptiness_check();
    core.sanity_check();

    finish_on_worker(&mut core, 2, ws2[0], 0);
    scheduler.run_scheduling(&mut core, &mut comm);
    let ws3 = core.get_task(3.into()).mn_placement().unwrap().to_vec();

    for w in &[100, 101, 102] {
        let s1 = get_worker_status(&ws2, (*w).into());
        let s2 = get_worker_status(&ws3, (*w).into());
        let ms = comm.take_worker_msgs(*w, 0);
        check_worker_status_change(s1, s2, ms.as_slice());
    }
    comm.emptiness_check();
    core.sanity_check();

    finish_on_worker(&mut core, 3, ws3[0], 0);
    scheduler.run_scheduling(&mut core, &mut comm);

    for w in &[100, 101, 102] {
        let s = get_worker_status(&ws3, (*w).into());
        let ms = comm.take_worker_msgs(*w, 0);
        check_worker_status_change(s, WorkerStatus::None, ms.as_slice());
    }
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_schedule_mn_fill() {
    let mut core = Core::default();
    let mut comm = create_test_comm();

    create_test_workers(
        &mut core,
        &[/* 11 workers */ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    );
    let task1 = TaskBuilder::new(1).n_nodes(3).build();
    let task2 = TaskBuilder::new(2).n_nodes(5).build();
    let task3 = TaskBuilder::new(3).n_nodes(1).build();
    let task4 = TaskBuilder::new(4).n_nodes(2).build();
    submit_test_tasks(&mut core, vec![task1, task2, task3, task4]);
    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    for w in core.get_workers() {
        assert!(w.mn_task().is_some());
    }
    for t in &[1, 2, 3, 4] {
        assert!(core.get_task(TaskId::new(*t)).is_mn_running());
    }
}

#[test]
fn test_mn_not_enough() {
    let mut core = Core::default();
    let mut comm = create_test_comm();

    create_test_workers(&mut core, &[1]);
    let task1 = TaskBuilder::new(1).n_nodes(3).build();
    let task2 = TaskBuilder::new(2).n_nodes(5).build();
    let task3 = TaskBuilder::new(3).n_nodes(11).build();
    let task4 = TaskBuilder::new(4).n_nodes(2).build();
    submit_test_tasks(&mut core, vec![task1, task2, task3, task4]);
    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    for w in core.get_workers() {
        assert!(w.mn_task().is_none());
    }
    for t in &[1, 2, 3, 4] {
        assert!(core.get_task(TaskId::new(*t)).is_waiting());
    }
}
