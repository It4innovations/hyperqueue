use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::server::core::Core;
use crate::internal::server::task::Task;
use crate::internal::tests::utils::env::{TestComm, TestEnv, create_test_comm};
use crate::internal::tests::utils::resources::ResBuilder;
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_worker, create_test_worker_config, create_test_workers,
    finish_on_worker, new_test_worker, submit_test_tasks,
};

use crate::internal::tests::utils::task::TaskBuilder;
use crate::resources::{ResourceDescriptor, ResourceIdMap};
use crate::{TaskId, WorkerId};
use std::time::Duration;

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
            assert!(matches!(ms, &[ToWorkerMessage::ComputeTasks(_)]))
        }
        (WorkerStatus::NonRoot, WorkerStatus::Root) => {
            assert!(matches!(ms, &[ToWorkerMessage::ComputeTasks(_),]))
        }
        (WorkerStatus::NonRoot, WorkerStatus::None) => {
            assert!(matches!(ms, &[]))
        }
        (WorkerStatus::None, WorkerStatus::NonRoot)
        | (WorkerStatus::Root, WorkerStatus::NonRoot) => {
            assert!(matches!(ms, &[]))
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
    let rmap = core.get_resource_map_mut();
    let tasks: Vec<Task> = (1..=4)
        .map(|i| {
            TaskBuilder::new(i)
                .user_priority(i as i32)
                .n_nodes(2)
                .build(rmap)
        })
        .collect();
    submit_test_tasks(&mut core, tasks);
    core.sanity_check();
    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();

    let test_mn_task = |task: &Task, comm: &mut TestComm| -> Vec<WorkerId> {
        let ws = task.mn_placement().unwrap().to_vec();
        assert_eq!(ws.len(), 2);
        if let ToWorkerMessage::ComputeTasks(m) = &comm.take_worker_msgs(ws[0], 1)[0] {
            assert_eq!(&m.tasks[0].node_list, &ws);
        } else {
            unreachable!()
        }
        ws
    };

    let task3 = core.get_task(3.into());
    let ws3 = test_mn_task(task3, &mut comm);
    let task4 = core.get_task(4.into());
    let ws4 = test_mn_task(task4, &mut comm);
    for w in &ws4 {
        assert!(!ws3.contains(w));
    }
    assert!(core.get_task(2.into()).is_waiting());
    assert!(core.get_task(1.into()).is_waiting());
    comm.emptiness_check();

    finish_on_worker(&mut core, 3, ws3[0]);
    core.sanity_check();

    assert!(core.find_task(3.into()).is_none());
    for w in ws3 {
        assert!(core.get_worker_by_id_or_panic(w).mn_task().is_none());
    }

    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();

    let task2 = core.get_task(2.into());
    let ws2 = test_mn_task(task2, &mut comm);
    comm.emptiness_check();

    finish_on_worker(&mut core, 3, ws2[0]);
    core.sanity_check();
}

#[test]
fn test_schedule_mn_reserve() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);

    let rmap = core.get_resource_map_mut();
    let task1 = TaskBuilder::new(1).user_priority(10).n_nodes(3).build(rmap);
    let task2 = TaskBuilder::new(2).user_priority(5).n_nodes(2).build(rmap);
    let task3 = TaskBuilder::new(3).user_priority(0).n_nodes(3).build(rmap);

    submit_test_tasks(&mut core, vec![task1, task2, task3]);
    core.sanity_check();
    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();

    let ws1 = core.get_task(1.into()).mn_placement().unwrap().to_vec();
    assert!(matches!(
        comm.take_worker_msgs(ws1[0], 1)[0],
        ToWorkerMessage::ComputeTasks(_)
    ));
    assert!(!core.get_worker_by_id_or_panic(ws1[0]).is_reserved());
    assert!(core.get_worker_by_id_or_panic(ws1[1]).is_reserved());
    assert!(core.get_worker_by_id_or_panic(ws1[2]).is_reserved());
    comm.emptiness_check();
    finish_on_worker(&mut core, 1, ws1[0]);
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

    finish_on_worker(&mut core, 2, ws2[0]);
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

    finish_on_worker(&mut core, 3, ws3[0]);
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
    let rmap = core.get_resource_map_mut();
    let task1 = TaskBuilder::new(1).n_nodes(3).build(rmap);
    let task2 = TaskBuilder::new(2).n_nodes(5).build(rmap);
    let task3 = TaskBuilder::new(3).n_nodes(1).build(rmap);
    let task4 = TaskBuilder::new(4).n_nodes(2).build(rmap);
    submit_test_tasks(&mut core, vec![task1, task2, task3, task4]);
    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    for w in core.get_workers() {
        assert!(w.mn_task().is_some());
    }
    for t in &[1, 2, 3, 4] {
        assert!(core.get_task(TaskId::new_test(*t)).is_mn_running());
    }
}

#[test]
fn test_mn_not_enough() {
    let mut core = Core::default();
    let mut comm = create_test_comm();

    create_test_workers(&mut core, &[4]);
    let rmap = core.get_resource_map_mut();
    let task1 = TaskBuilder::new(1).n_nodes(3).build(rmap);
    let task2 = TaskBuilder::new(2).n_nodes(5).build(rmap);
    let task3 = TaskBuilder::new(3).n_nodes(11).build(rmap);
    let task4 = TaskBuilder::new(4).n_nodes(2).build(rmap);
    let r1 = rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(3).finish_v());
    let r2 = rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(5).finish_v());
    let r3 = rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(11).finish_v());
    let r4 = rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(2).finish_v());
    submit_test_tasks(&mut core, vec![task1, task2, task3, task4]);
    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    for w in core.get_workers() {
        assert!(w.mn_task().is_none());
    }
    for t in &[1, 2, 3, 4] {
        assert!(core.get_task(TaskId::new_test(*t)).is_waiting());
    }

    let (mn_queue, _, _) = core.multi_node_queue_split();
    assert!(mn_queue.is_sleeping(r1));
    assert!(mn_queue.is_sleeping(r2));
    assert!(mn_queue.is_sleeping(r3));
    assert!(mn_queue.is_sleeping(r4));
}

#[test]
fn test_mn_sleep_wakeup_one_by_one() {
    let mut core = Core::default();
    let mut comm = create_test_comm();

    let rmap = core.get_resource_map_mut();
    let task1 = TaskBuilder::new(1).n_nodes(4).user_priority(10).build(rmap);
    submit_test_tasks(&mut core, vec![task1]);

    create_test_workers(&mut core, &[4, 1]);

    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    assert!(core.task_map().get_task(1.into()).is_waiting());

    let rmap = core.get_resource_map_mut();
    let task2 = TaskBuilder::new(2).n_nodes(2).user_priority(1).build(rmap);
    submit_test_tasks(&mut core, vec![task2]);
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    assert!(core.task_map().get_task(1.into()).is_waiting());
    assert!(core.task_map().get_task(2.into()).is_mn_running());

    let w = core.task_map().get_task(2.into()).mn_root_worker().unwrap();
    finish_on_worker(&mut core, 2, w);
    create_test_worker(&mut core, 500.into(), 1);
    create_test_worker(&mut core, 501.into(), 1);
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    assert!(core.task_map().get_task(1.into()).is_mn_running());
}

#[test]
fn test_mn_sleep_wakeup_at_once() {
    let mut core = Core::default();
    let mut comm = create_test_comm();

    create_test_workers(&mut core, &[4, 1]);
    let rmap = core.get_resource_map_mut();
    let task1 = TaskBuilder::new(1).n_nodes(4).user_priority(10).build(rmap);
    let task2 = TaskBuilder::new(2).n_nodes(2).user_priority(1).build(rmap);
    submit_test_tasks(&mut core, vec![task1, task2]);

    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    assert!(core.task_map().get_task(1.into()).is_waiting());
    assert!(core.task_map().get_task(2.into()).is_mn_running());
}

#[test]
fn test_mn_schedule_on_groups() {
    let mut core = Core::default();

    let resource_map = ResourceIdMap::from_vec(vec!["cpus".to_string()]);
    let worker_id = WorkerId::new(100);
    let mut wcfg1 = create_test_worker_config(worker_id, ResourceDescriptor::simple_cpus(1));
    wcfg1.group = "group1".to_string();
    new_test_worker(&mut core, worker_id, wcfg1, &resource_map);

    let worker_id = WorkerId::new(101);
    let mut wcfg2 = create_test_worker_config(worker_id, ResourceDescriptor::simple_cpus(1));
    wcfg2.group = "group2".to_string();
    new_test_worker(&mut core, worker_id, wcfg2, &resource_map);

    let mut comm = create_test_comm();
    let rmap = core.get_resource_map_mut();
    let task1 = TaskBuilder::new(1).n_nodes(2).build(rmap);
    submit_test_tasks(&mut core, vec![task1]);

    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    assert!(core.task_map().get_task(1.into()).is_waiting());
}

#[test]
fn test_schedule_mn_time_request1() {
    let mut rt = TestEnv::new();
    rt.new_workers_ext(&[
        (1, None, Vec::new()),
        (1, Some(Duration::new(29_999, 0)), Vec::new()),
        (1, Some(Duration::new(30_001, 0)), Vec::new()),
    ]);
    rt.new_task(TaskBuilder::new(1).n_nodes(3).time_request(30_000));
    rt.schedule();
    assert!(rt.task(1.into()).is_waiting());

    rt.new_task(TaskBuilder::new(2).n_nodes(2).time_request(30_000));
    rt.schedule();
    assert!(rt.task(1.into()).is_waiting());
    assert!(rt.task(2.into()).is_mn_running());
}

#[test]
fn test_schedule_mn_time_request2() {
    let mut rt = TestEnv::new();
    rt.new_workers_ext(&[
        (1, Some(Duration::new(59_999, 0)), Vec::new()),
        (1, Some(Duration::new(29_999, 0)), Vec::new()),
        (1, Some(Duration::new(30_001, 0)), Vec::new()),
    ]);
    rt.new_task(TaskBuilder::new(1).n_nodes(3).time_request(23_998));
    rt.schedule();
    assert!(rt.task(1.into()).is_mn_running());
}
