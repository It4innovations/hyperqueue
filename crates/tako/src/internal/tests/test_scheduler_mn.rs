use crate::WorkerId;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::scheduler::create_task_batches;
use crate::internal::server::task::Task;
use crate::resources::ResourceAmount;
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::resources::ResBuilder;
use crate::tests::utils::scheduler::TestCase;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;
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
fn test_mn_task_batches1() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let now = std::time::Instant::now();

    rt.new_task(&TaskBuilder::new().n_nodes(4));
    let a = create_task_batches(rt.core(), now, None);
    assert!(a.is_empty());

    rt.new_task(&TaskBuilder::new().n_nodes(2));
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a[0].size, 1);
    assert!(!a[0].limit_reached);

    rt.new_task(&TaskBuilder::new().n_nodes(2));
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a[0].size, 1);
    assert!(a[0].limit_reached);
}

#[test]
fn test_mn_task_batches2() {
    let mut rt = TestEnv::new();
    let now = std::time::Instant::now();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);

    rt.new_task(&TaskBuilder::new().user_priority(0).n_nodes(3));
    rt.new_task(&TaskBuilder::new().user_priority(5).n_nodes(2));
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 2);
    assert_eq!(a[0].size, 1);
    assert_eq!(a[1].size, 1);
    assert_eq!(a[0].cuts.len(), 1);
    assert_eq!(a[1].cuts.len(), 0);
}

#[test]
fn test_schedule_mn_simple() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5, 5, 5]);
    let t1 = rt.new_task(&TaskBuilder::new().user_priority(1).n_nodes(2));
    let t2 = rt.new_task(&TaskBuilder::new().user_priority(2).n_nodes(2));
    let t3 = rt.new_task(&TaskBuilder::new().user_priority(3).n_nodes(2));
    let t4 = rt.new_task(&TaskBuilder::new().user_priority(4).n_nodes(2));

    let mut comm = TestComm::new();
    rt.schedule_with_comm(&mut comm);
    rt.sanity_check();

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

    let ws3 = test_mn_task(rt.task(t3), &mut comm);
    let ws4 = test_mn_task(rt.task(t4), &mut comm);
    for w in &ws4 {
        assert!(!ws3.contains(w));
    }
    assert!(rt.task(t2).is_waiting());
    assert!(rt.task(t1).is_waiting());
    comm.emptiness_check();

    rt.finish_task(t3, ws3[0]);
    rt.sanity_check();

    assert!(!rt.task_exists(t3));
    for w in ws3 {
        assert!(
            rt.core()
                .get_worker_by_id_or_panic(w)
                .mn_assignment()
                .is_none()
        );
    }

    rt.schedule_with_comm(&mut comm);
    rt.sanity_check();

    let ws2 = test_mn_task(rt.task(t2), &mut comm);
    comm.emptiness_check();

    rt.finish_task(t3, ws2[0]);
    rt.sanity_check();
}

#[test]
fn test_schedule_mn_reserve() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);

    let t1 = rt.new_task(&TaskBuilder::new().user_priority(10).n_nodes(3));
    let t2 = rt.new_task(&TaskBuilder::new().user_priority(5).n_nodes(2));
    let t3 = rt.new_task(&TaskBuilder::new().user_priority(0).n_nodes(3));
    rt.sanity_check();
    let mut comm = TestComm::new();
    rt.schedule_with_comm(&mut comm);
    rt.sanity_check();

    let ws1 = rt.task(t1).mn_placement().unwrap().to_vec();
    assert!(matches!(
        comm.take_worker_msgs(ws1[0], 1)[0],
        ToWorkerMessage::ComputeTasks(_)
    ));
    assert!(!rt.core().get_worker_by_id_or_panic(ws1[0]).is_reserved());
    assert!(rt.core().get_worker_by_id_or_panic(ws1[1]).is_reserved());
    assert!(rt.core().get_worker_by_id_or_panic(ws1[2]).is_reserved());
    comm.emptiness_check();
    rt.finish_task(t1, ws1[0]);
    rt.schedule_with_comm(&mut comm);

    let ws2 = rt.task(t2).mn_placement().unwrap().to_vec();
    for w in &ws {
        let s1 = get_worker_status(&ws1, (*w).into());
        let s2 = get_worker_status(&ws2, (*w).into());
        let ms = comm.take_worker_msgs(*w, 0);
        check_worker_status_change(s1, s2, ms.as_slice());
    }
    comm.emptiness_check();
    rt.sanity_check();

    rt.finish_task(t2, ws2[0]);
    rt.schedule_with_comm(&mut comm);
    let ws3 = rt.task(t3).mn_placement().unwrap().to_vec();

    for w in &ws {
        let s1 = get_worker_status(&ws2, (*w).into());
        let s2 = get_worker_status(&ws3, (*w).into());
        let ms = comm.take_worker_msgs(*w, 0);
        check_worker_status_change(s1, s2, ms.as_slice());
    }
    comm.emptiness_check();
    rt.sanity_check();

    rt.finish_task(t3, ws3[0]);
    rt.schedule_with_comm(&mut comm);

    for w in &ws {
        let s = get_worker_status(&ws3, (*w).into());
        let ms = comm.take_worker_msgs(*w, 0);
        check_worker_status_change(s, WorkerStatus::None, ms.as_slice());
    }
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_schedule_mn_fill() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[/* 11 workers */ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));
    let t2 = rt.new_task(&TaskBuilder::new().n_nodes(5));
    let t3 = rt.new_task(&TaskBuilder::new().n_nodes(1));
    let t4 = rt.new_task(&TaskBuilder::new().n_nodes(2));
    rt.schedule();
    rt.sanity_check();
    for w in rt.core().get_workers() {
        assert!(w.mn_assignment().is_some());
    }
    for t in &[t1, t2, t3, t4] {
        assert!(rt.task(*t).is_mn_running());
    }
}

#[test]
fn test_mn_not_enough() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[4]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));
    let t2 = rt.new_task(&TaskBuilder::new().n_nodes(5));
    let t3 = rt.new_task(&TaskBuilder::new().n_nodes(11));
    let t4 = rt.new_task(&TaskBuilder::new().n_nodes(2));
    let rmap = rt.core().resource_map_mut();
    rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(3).finish_v());
    rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(5).finish_v());
    rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(11).finish_v());
    rmap.get_resource_rq_id(&ResBuilder::default().n_nodes(2).finish_v());
    rt.schedule();
    rt.sanity_check();
    for w in rt.core().get_workers() {
        assert!(w.mn_assignment().is_none());
    }
    for t in &[t1, t2, t3, t4] {
        assert!(rt.task(*t).is_waiting());
    }
}

#[test]
fn test_mn_sleep_wakeup_one_by_one() {
    let mut rt = TestEnv::new();

    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(4).user_priority(10));
    rt.new_workers_cpus(&[4, 1]);

    rt.schedule();
    rt.sanity_check();
    assert!(rt.task(t1).is_waiting());

    let t2 = rt.new_task(&TaskBuilder::new().n_nodes(2).user_priority(1));
    rt.schedule();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_mn_running());

    let w = rt.task(t2).mn_root_worker().unwrap();
    rt.finish_task(t2, w);
    rt.new_worker(&WorkerBuilder::new(1));
    rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    rt.sanity_check();
    assert!(rt.task(t1).is_mn_running());
}

#[test]
fn test_mn_sleep_wakeup_at_once() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[4, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(4).user_priority(10));
    let t2 = rt.new_task(&TaskBuilder::new().n_nodes(2).user_priority(1));
    rt.schedule();
    rt.sanity_check();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_mn_running());
}

#[test]
fn test_mn_schedule_on_groups() {
    let mut rt = TestEnv::new();

    rt.new_worker(&WorkerBuilder::new(1).group("group1"));
    rt.new_worker(&WorkerBuilder::new(1).group("group2"));

    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(2));
    rt.schedule();
    rt.sanity_check();
    assert!(rt.task(t1).is_waiting());
}

#[test]
fn test_schedule_mn_time_request1() {
    let mut rt = TestEnv::new();

    rt.new_worker(&WorkerBuilder::new(1));
    rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(29_999, 0)));
    rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(30_001, 0)));

    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3).time_request(30_000));
    rt.schedule();
    assert!(rt.task(t1).is_waiting());

    let t2 = rt.new_task(&TaskBuilder::new().n_nodes(2).time_request(30_000));
    rt.schedule();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_mn_running());
}

#[test]
fn test_schedule_mn_time_request2() {
    let mut rt = TestEnv::new();
    rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(59_999, 0)));
    rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(29_999, 0)));
    rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(30_001, 0)));
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3).time_request(23_998));
    rt.schedule();
    assert!(rt.task(t1).is_mn_running());
}

#[test]
fn test_schedule_mn_and_sn1() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[4, 4]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(2).user_priority(2));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(1));
    rt.schedule();
    assert!(rt.task(t1).is_mn_running());
    assert!(rt.task(t2).is_waiting());
}

#[test]
fn test_schedule_mn_and_sn2() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[4, 4]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(2).user_priority(1));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    rt.schedule();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_assigned());
}

#[test]
fn test_schedule_mn_and_sn3() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[4, 4]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(2).user_priority(1));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(1));
    rt.schedule();
    assert!(rt.task(t1).is_mn_running());
    assert!(rt.task(t2).is_waiting());
}

#[test]
fn test_schedule_mn_and_sn4() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[4, 3, 4]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(2).user_priority(1));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(1));
    rt.schedule();
    assert!(rt.task(t1).is_mn_running());
    assert!(rt.task(t2).is_assigned());
}
