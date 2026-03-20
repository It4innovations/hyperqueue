use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::scheduler::{PriorityCut, SchedulerConfig, create_task_batches};
use crate::internal::server::reactor::on_retract_response;
use crate::internal::server::task::TaskRuntimeState;
use crate::internal::tests::utils::scheduler::TestCase;
use crate::internal::worker::comm::WorkerComm::Test;
use crate::resources::ResourceRqId;
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;
use crate::{Priority, ResourceVariantId, TaskId, WorkerId};
use std::time::Duration;

#[test]
fn test_task_grouping_basic() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert!(a.is_empty());

    let t1 = rt.new_task(&TaskBuilder::new().user_priority(123));
    let a = create_task_batches(rt.core(), now, None);
    let task1 = rt.core().get_task(t1);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].resource_rq_id, task1.resource_rq_id);
    assert!(a[0].cuts.is_empty());
    assert_eq!(a[0].size, 1);
    assert!(!a[0].limit_reached);

    let t2 = rt.new_task(&TaskBuilder::new().user_priority(20));
    let t3 = rt.new_task(&TaskBuilder::new().user_priority(5));
    let t4 = rt.new_task(&TaskBuilder::new().user_priority(123));
    let t5 = rt.new_task(&TaskBuilder::new().user_priority(20));

    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 1);
    let r1 = rt.task(t1).resource_rq_id;
    assert_eq!(a[0].resource_rq_id, r1);
    assert!(a[0].cuts.is_empty());
    assert_eq!(a[0].size, 5);
    assert!(!a[0].limit_reached);

    let t6 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));
    let t7 = rt.new_task(&TaskBuilder::new().cpus(123).user_priority(123));
    let t8 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));
    let t9 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));

    // Subitted tasks:
    // 1 cpus: 123 123 20 20 5
    // 2 cpus: 123 123 123
    // 123 cpus:  123
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 2);
    let task1 = rt.task(t1);
    let task6 = rt.task(t6);
    let task7 = rt.task(t7);
    assert_eq!(a[0].resource_rq_id, task1.resource_rq_id);
    assert_eq!(a[0].size, 5);
    assert!(!a[0].limit_reached);
    assert_eq!(
        a[0].cuts,
        vec![PriorityCut {
            size: 2,
            blockers: vec![
                (task6.resource_rq_id, Some(3)),
                (task7.resource_rq_id, None)
            ],
        }]
    );
    assert_eq!(a[1].resource_rq_id, task6.resource_rq_id);
    assert_eq!(a[1].size, 3);
    assert!(!a[1].limit_reached);
    assert_eq!(a[1].cuts, vec![]);
}

#[test]
fn test_task_grouping_blocker() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5]);
    rt.new_task(&TaskBuilder::new().user_priority(2));
    rt.new_task(&TaskBuilder::new().cpus(2).user_priority(1));
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 2);
    assert!(a[0].is_blocker);
    assert!(!a[1].is_blocker);
}

#[test]
fn test_task_group_saturation() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let t1 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    let t3 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(4));
    let t4 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(4));
    let t5 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(6));
    let t6 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(6));
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].size, 3);
    assert!(a[0].limit_reached);
    assert!(a[0].cuts.is_empty());

    let t10 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(5));
    let t11 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(0));

    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 2);
    assert_eq!(a[0].size, 3);
    assert!(a[0].limit_reached);
    assert_eq!(
        a[0].cuts,
        vec![PriorityCut {
            size: 2,
            blockers: vec![(ResourceRqId::new(1), Some(1))],
        },]
    );
    assert_eq!(a[1].size, 2);
    assert!(!a[1].limit_reached);
    assert_eq!(
        a[1].cuts,
        vec![
            PriorityCut {
                size: 0,
                blockers: vec![(ResourceRqId::new(0), Some(2))],
            },
            PriorityCut {
                size: 1,
                blockers: vec![(ResourceRqId::new(0), None)],
            }
        ]
    );
}

#[test]
fn test_task_batching2() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[3, 3, 3]);
    rt.new_task_running(&TaskBuilder::new().cpus(1), ws[0]);
    rt.new_task_running(&TaskBuilder::new().cpus(2), ws[1]);
    rt.new_task_running(&TaskBuilder::new().cpus(3), ws[2]);

    rt.new_task(&TaskBuilder::new().cpus(2));
    rt.new_task(&TaskBuilder::new().cpus(1));
    rt.new_task(&TaskBuilder::new().cpus(3));
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 3);
    assert!(a[0].cuts.is_empty());
    assert!(a[1].cuts.is_empty());
    assert!(a[2].cuts.is_empty());
}

#[test]
fn test_schedule_no_priorities() {
    let w3 = WorkerBuilder::new(3);
    let w4 = WorkerBuilder::new(4);

    let mut c = TestCase::new();
    c.w(&w4);
    c.w(&w3);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[3]);
    c.w(&w3).expect_tasks(&[ts[0]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2]);
    c.w(&w4).expect_tasks(&[ts[0]]);
    c.w(&w4);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 2]);
    c.w(&w4).expect_tasks(&ts);
    c.w(&w4);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 2, 2]);
    c.w(&w4).expect_tasks(&[ts[0], ts[2]]);
    c.w(&w4).expect_tasks(&[ts[1]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 2, 2, 2]);
    c.w(&w4).expect_tasks(&[ts[0], ts[2]]);
    c.w(&w4).expect_tasks(&[ts[1], ts[3]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 2, 2, 2, 2]);
    c.w(&w4).expect_tasks(&[ts[0], ts[2]]);
    c.w(&w4).expect_tasks(&[ts[1], ts[3]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 3]);
    c.w(&w4).expect_tasks(&[ts[1]]);
    c.w(&w4).expect_tasks(&[ts[0]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 3]);
    c.w(&w3).expect_tasks(&[ts[1]]);
    c.w(&w4).expect_tasks(&[ts[0]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[5, 5, 1, 1, 1, 1, 1]);
    c.w(&w4).expect_tasks(&[ts[2], ts[4], ts[5], ts[6]]);
    c.w(&w4).expect_tasks(&[ts[3]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[3, 4, 2]);
    c.w(&w4).expect_tasks(&[ts[1]]);
    c.w(&w4).expect_tasks(&[ts[0]]);
    c.check();
}

#[test]
fn test_schedule_priorities() {
    let w4 = WorkerBuilder::new(4);
    let w10 = WorkerBuilder::new(10);

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 2), (1, 2)]);
    c.w(&w4).expect_tasks(&[ts[0], ts[1]]);
    c.w(&w4);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 2), (2, 2)]);
    c.w(&w4).expect_tasks(&[ts[1], ts[0]]);
    c.w(&w4);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(0, 4), (0, 4), (1, 2), (2, 3)]);
    c.w(&w4).expect_tasks(&[ts[3]]);
    c.w(&w4).expect_tasks(&[ts[2]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(0, 4), (0, 4), (1, 2), (1, 3)]);
    c.w(&w4).expect_tasks(&[ts[3]]);
    c.w(&w4).expect_tasks(&[ts[2]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 4), (1, 4), (1, 2), (1, 3)]);
    c.w(&w4).eq_class(0).expect_tasks(&[ts[0]]);
    c.w(&w4).eq_class(0).expect_tasks(&[ts[1]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(0, 2), (4, 2), (3, 1), (2, 3)]);
    c.w(&w4).eq_class(0).expect_tasks(&[ts[1], ts[0]]);
    c.w(&w4).eq_class(0).expect_tasks(&[ts[2], ts[3]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 5), (0, 4)]);
    c.w(&w4).expect_tasks(&[ts[1]]);
    c.w(&w4);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(0, 2), (4, 2), (2, 4)]);
    c.w(&w4).eq_class(0).expect_tasks(&[ts[1], ts[0]]);
    c.w(&w4).eq_class(0).expect_tasks(&[ts[2]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(9, 2), (7, 1), (6, 2)]);
    c.w(&w4).expect_tasks(&ts[..2]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(9, 2), (7, 1), (6, 2), (5, 1)]);
    c.w(&w4).expect_tasks(&ts[..2]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[
        (9, 2), // cumsum: 2
        (8, 1), // cumsum: 3
        (7, 2), // cumsum: 5
        (6, 1), // cumsum: 6
        (5, 2), // cumsum: 8
        (4, 1), // cumsum: 9
        (3, 2), // cumsum: 11
        (2, 1), // cumsum: 12
    ]);
    c.w(&w10).expect_tasks(&ts[..6]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 3), (1, 3), (1, 3), (0, 1)]);
    c.w(&w4).expect_tasks(&[ts[0], ts[3]]);
    c.check();
}

#[test]
fn test_schedule_no_irrelevant_blocking() {
    let w3 = WorkerBuilder::new(3);
    let w5 = WorkerBuilder::new(5);

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(10, 5), (0, 1)]);
    c.w(&w3).expect_tasks(&[ts[1]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(10, 5), (9, 5), (0, 1)]);
    c.w(&w3).expect_tasks(&[ts[2]]);
    c.w(&w5).expect_tasks(&[ts[0]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(10, 3), (9, 2), (8, 5), (0, 1)]);
    c.w(&w5).expect_tasks(&[ts[0], ts[1]]);
    c.w(&w3).expect_tasks(&[ts[3]]);
    c.check();
}

#[test]
fn test_schedule_some_tasks_running() {
    let w3 = WorkerBuilder::new(3);
    let mut c = TestCase::new();
    c.pc_tasks(&[(1, 3)]);
    c.w(&w3).running_c(1).expect_tasks(&[]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 2)]);
    c.w(&w3).running_c(1).expect_tasks(&[ts[0]]);
    c.check();

    let mut c = TestCase::new();
    c.pc_tasks(&[(1, 3), (0, 1)]);
    c.w(&w3).running_c(1).expect_tasks(&[]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 1, 3]);
    c.w(&w3).running_c(1).expect_tasks(&[ts[0]]);
    c.w(&w3).running_c(2).expect_tasks(&[ts[1]]);
    c.w(&w3).running_c(2).running_c(1).expect_tasks(&[]);
    c.check();

    /* Enable when reservations are implemented
    let mut c = TestCase::new();
    let ts = c.c_tasks(&[2, 1]);
    c.pc_tasks(&[(1, 3)]);
    c.w(&w3).running_c(1).expect_tasks(&[ts[0]]);
    c.w(&w3).running_c(2).expect_tasks(&[ts[1]]);
    c.w(&w3).running_c(2).running_c(1).expect_tasks(&[]);
    c.check();
     */
}

#[test]
fn test_priority_switching() {
    for (w_cpus, count_a, count_b) in [
        (1, 2, 0),
        (2, 3, 1),
        (3, 4, 2),
        (4, 6, 2),
        (5, 7, 3),
        (6, 8, 4),
        (7, 10, 4),
        (8, 12, 4),
        (9, 12, 5),
        (10, 12, 5),
    ] {
        let mut rt = TestEnv::new();
        rt.new_named_resource("foo");
        let ta = TaskBuilder::new().cpus(1);
        let tb = TaskBuilder::new().cpus(1).add_resource(1, 1);
        let w4 = WorkerBuilder::new(w_cpus).res_sum("foo", 10_000);
        rt.new_worker(&w4);
        rt.new_worker(&w4);
        // Create batches:
        // 3a - 2b - 4a - 2b -  5a    - 1b
        // 3a0b 3a2b 7a2b 7a4b  12a4b 12a5b
        rt.new_tasks(3, &ta.clone().user_priority(10));
        rt.new_tasks(2, &tb.clone().user_priority(9));
        rt.new_tasks(1, &ta.clone().user_priority(8));
        rt.new_tasks(3, &ta.clone().user_priority(7));
        rt.new_tasks(1, &tb.clone().user_priority(6));
        rt.new_tasks(1, &tb.clone().user_priority(5));
        rt.new_tasks(5, &ta.clone().user_priority(4));
        rt.new_tasks(1, &tb.clone().user_priority(3));
        rt.schedule();
        let mut counts = assigned_counts(&mut rt);
        assert_eq!(counts[0], count_a);
        assert_eq!(counts[1], count_b);
    }
}

// TODO: Rezervace
// TODO: Handle situation with many task batches

#[test]
fn test_schedule_gap_filling() {
    let w6 = WorkerBuilder::new(6);
    let w12 = WorkerBuilder::new(12);
    let w8 = WorkerBuilder::new(8);

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 8), (1, 8), (0, 4)]);
    c.w(&w12).expect_tasks(&[ts[0], ts[2]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 3), (1, 3), (1, 3), (0, 2)]);
    c.w(&w6).expect_tasks(&[ts[0], ts[1]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 3), (1, 3), (1, 3), (0, 1), (0, 1)]);
    c.w(&w8).expect_tasks(&[ts[0], ts[1], ts[3], ts[4]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 3), (1, 3), (1, 3), (2, 1), (0, 1)]);
    c.w(&w8).expect_tasks(&[ts[3], ts[0], ts[1], ts[4]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[
        (1, 3),
        (1, 3),
        (1, 3),
        (2, 1),
        (0, 1),
        (0, 1),
        (0, 1),
        (0, 1),
    ]);
    c.w(&w8).expect_tasks(&[ts[3], ts[0], ts[1], ts[4]]);
    c.check();
}

fn assigned_counts(rt: &mut TestEnv) -> Vec<usize> {
    let mut counts = vec![0; rt.core().get_resource_rq_map().size()];
    for task in rt.task_map().tasks() {
        if task.is_assigned() {
            counts[task.resource_rq_id.as_usize()] += 1;
        }
    }
    counts
}

#[test]
fn test_schedule_gap_filling2() {
    for extra in &[true, false] {
        let mut rt = TestEnv::new();
        rt.new_named_resource("foo");
        rt.new_worker(&WorkerBuilder::new(8));
        rt.new_workers(3, &WorkerBuilder::new(4).res_sum("foo", 1));

        let ta = TaskBuilder::new().cpus(1);
        let tb = TaskBuilder::new().cpus(3);
        let tc = TaskBuilder::new().cpus(4).add_resource(1, 1);

        rt.new_tasks(7, &ta.clone().user_priority(1));
        rt.new_tasks(3, &tb.clone().user_priority(2));
        rt.new_tasks(3, &tc.clone().user_priority(2));
        if *extra {
            rt.new_tasks(2, &tb.clone().user_priority(-1));
            rt.new_tasks(3, &tc.clone().user_priority(-2));
            rt.new_tasks(1, &ta.clone().user_priority(-3));
            rt.new_tasks(2, &tb.clone().user_priority(-4));
            rt.new_tasks(3, &tc.clone().user_priority(-5));
            rt.new_tasks(1, &ta.clone().user_priority(-6));
        }

        rt.schedule();

        let counts = assigned_counts(&mut rt);
        assert_eq!(counts[0], 2);
        assert_eq!(counts[1], 2);
        assert_eq!(counts[2], 3);

        rt.schedule();
    }
}

#[test]
fn test_schedule_gap_filling3() {
    let mut rt = TestEnv::new();
    rt.new_named_resource("foo");
    rt.new_workers(2, &WorkerBuilder::new(34));

    let ta = TaskBuilder::new().cpus(9);
    let tb = TaskBuilder::new().cpus(3);

    rt.new_tasks(5, &tb.clone().user_priority(10));
    rt.new_tasks(6, &ta.clone().user_priority(10));
    rt.new_tasks(5, &tb.clone().user_priority(9));
    rt.schedule();
    let counts = assigned_counts(&mut rt);
    assert_eq!(counts, [4, 6]);
}

#[test]
fn test_schedule_gap_filling4() {
    let mut rt = TestEnv::new();
    rt.new_named_resource("foo");
    rt.new_named_resource("bar");
    rt.new_named_resource("goo");
    rt.new_workers(
        2,
        &WorkerBuilder::new(3).res_sum("foo", 10).res_sum("goo", 10),
    );
    rt.new_worker(&WorkerBuilder::new(3).res_sum("foo", 10).res_sum("bar", 10));

    rt.new_tasks(
        5,
        &TaskBuilder::new()
            .cpus(2)
            .add_resource(3, 1)
            .user_priority(10),
    );
    rt.new_tasks(
        2,
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(1, 1)
            .user_priority(9),
    );
    rt.new_tasks(
        10,
        &TaskBuilder::new()
            .cpus(3)
            .add_resource(1, 1)
            .add_resource(2, 1)
            .user_priority(8),
    );
    rt.schedule();
    let counts = assigned_counts(&mut rt);
    assert_eq!(counts, [2, 2, 1]);
}

#[test]
fn test_schedule_reservation_simple() {
    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(3, 3), (2, 2)]);
    c.w(&WorkerBuilder::new(3))
        .eq_class(0)
        .running_c(1)
        .expect_tasks(&[]);
    c.w(&WorkerBuilder::new(3))
        .eq_class(0)
        .running_c(1)
        .expect_tasks(&[ts[1]]);
    c.check();
}

#[test]
fn test_schedule_reservation2() {
    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(3, 3), (2, 1), (2, 1)]);
    c.w(&WorkerBuilder::new(3)).eq_class(0).running_c(1);
    c.w(&WorkerBuilder::new(3))
        .eq_class(0)
        .running_c(1)
        .expect_tasks(&[ts[1], ts[2]]);
    c.check();
}

#[test]
fn test_schedule_reservation3() {
    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(3, 3), (2, 1), (2, 1)]);
    c.w(&WorkerBuilder::new(3))
        .running_c(2)
        .expect_tasks(&[ts[1]]);
    c.w(&WorkerBuilder::new(3)).running_c(1);
    c.check();
}

#[test]
fn test_schedule_reservation4() {
    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(4, 3), (3, 3), (3, 3), (2, 1), (2, 1)]);
    c.w(&WorkerBuilder::new(4))
        .running_c(1)
        .expect_tasks(&[ts[0]]);
    c.w(&WorkerBuilder::new(3))
        .running_c(2)
        .expect_tasks(&[ts[3]]);
    c.w(&WorkerBuilder::new(3)).running_c(2);
    c.w(&WorkerBuilder::new(3)).running_c(1);
    c.check();
}

#[test]
fn test_schedule_reservation5() {
    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(4, 3), (3, 3), (3, 3), (2, 1), (2, 1)]);
    c.w(&WorkerBuilder::new(3))
        .running_c(2)
        .expect_request(1, &TaskBuilder::new());
    c.w(&WorkerBuilder::new(3)).running_c(2);
    c.w(&WorkerBuilder::new(3)).running_c(1);
    c.w(&WorkerBuilder::new(4))
        .expect_request(1, &TaskBuilder::new().cpus(3))
        .expect_request(1, &TaskBuilder::new());
    c.check();
}

#[test]
fn test_schedule_multiple_resources1() {
    let w4_1 = WorkerBuilder::new(4).res_range("gpus", 1, 1);
    let w4_2 = WorkerBuilder::new(4).res_range("gpus", 1, 2);
    let tb2_1 = TaskBuilder::new().cpus(2).add_resource(1, 1);
    let tb1_2 = TaskBuilder::new().cpus(1).add_resource(1, 2);
    let tb2 = TaskBuilder::new().cpus(2);

    let create = || TestCase::new().resources(&["gpus"]);

    let mut c = create();
    let t1 = c.t(&tb2_1);
    let t2 = c.t(&tb2_1);
    c.w(&w4_2).expect_tasks(&[t1, t2]);
    c.check();

    let mut c = create();
    let t1 = c.t(&tb2_1);
    c.t(&tb2_1);
    c.w(&w4_1).expect_tasks(&[t1]);
    c.check();

    let mut c = create();
    let t1 = c.t(&tb2);
    c.w(&w4_2).expect_tasks(&[t1]);
    c.check();

    let mut c = create();
    let t1 = c.t(&tb1_2);
    c.w(&w4_2).expect_tasks(&[t1]);
    c.check();

    let mut c = create();
    let t1 = c.t(&tb1_2);
    c.w(&w4_1).expect_tasks(&[]);
    c.check();

    let mut c = TestCase::new().resources(&["gpus", "foo"]);
    let ta = TaskBuilder::new().cpus(2).add_resource(1, 1); // 2 cpus + 1 foo
    let tb = TaskBuilder::new().add_resource(1, 1).add_resource(2, 2); // 1 cpus + 1 gpus + 2 foo
    let tc = TaskBuilder::new().cpus(4); // 4 cpus
    c.t(&ta);
    c.ts(2, &tb);
    c.ts(2, &tc);
    c.t(&tb);
    c.w(&WorkerBuilder::new(6)).expect_request(1, &tc);
    c.w(&WorkerBuilder::new(3).res_sum("gpus", 2))
        .expect_request(1, &ta);
    c.w(&WorkerBuilder::new(5).res_sum("gpus", 20).res_sum("foo", 4))
        .expect_request(2, &tb);
    c.check();
}

#[test]
fn test_schedule_multiple_resources2() {
    let tb2_1 = TaskBuilder::new().cpus(2).add_resource(1, 1);
    let tb2 = TaskBuilder::new().cpus(2);

    let create = || {
        let mut c = TestCase::new().resources(&["gpus"]);
        c.ts(10, &tb2);
        c.ts(10, &tb2_1);
        c
    };

    let mut c = create();
    c.w(&WorkerBuilder::new(6)).expect_request(3, &tb2);
    c.check();

    let mut c = create();
    c.w(&WorkerBuilder::new(6).res_sum("gpus", 10))
        .expect_request(3, &tb2_1);
    c.check();

    let mut c = create();
    c.w(&WorkerBuilder::new(6).res_sum("gpus", 2))
        .expect_request(2, &tb2_1)
        .expect_request(1, &tb2);
    c.check();

    let mut c = create();
    c.w(&WorkerBuilder::new(6).res_sum("gpus", 2))
        .expect_request(2, &tb2_1)
        .expect_request(1, &tb2);
    c.w(&WorkerBuilder::new(6)).expect_request(3, &tb2);
    c.check();
}

#[test]
fn test_schedule_variants1() {
    let tb1 = TaskBuilder::new().cpus(2).next_variant().cpus(5);

    let mut c = TestCase::new();
    c.ts(2, &tb1);
    c.w(&WorkerBuilder::new(11)).expect_request_v(2, &tb1, 1);
    c.check();

    let mut c = TestCase::new();
    c.ts(3, &tb1);
    c.w(&WorkerBuilder::new(11)).expect_request_v(2, &tb1, 1);
    c.check();

    let mut c = TestCase::new();
    c.ts(3, &tb1);
    c.w(&WorkerBuilder::new(14))
        .expect_request_v(2, &tb1, 1)
        .expect_request_v(1, &tb1, 0);
    c.check();

    let mut c = TestCase::new();
    c.ts(10, &tb1);
    c.w(&WorkerBuilder::new(8)).expect_request_v(4, &tb1, 0);
    c.check();

    let mut c = TestCase::new();
    c.ts(3, &tb1);
    c.w(&WorkerBuilder::new(8))
        .expect_request_v(1, &tb1, 0)
        .expect_request_v(1, &tb1, 1);
    c.check();
}

#[test]
fn test_schedule_variants2() {
    let tb1 = TaskBuilder::new()
        .cpus(6)
        .next_variant()
        .cpus(2)
        .add_resource(1, 2);

    let create = || TestCase::new().resources(&["gpus"]);

    let mut c = create();
    c.ts(10, &tb1);
    c.w(&WorkerBuilder::new(12)).expect_request_v(2, &tb1, 0);
    c.check();

    let mut c = create();
    c.ts(10, &tb1);
    c.w(&WorkerBuilder::new(12).res_sum("gpus", 4))
        .expect_request_v(1, &tb1, 0)
        .expect_request_v(2, &tb1, 1);
    c.check();

    let mut c = create();
    c.ts(10, &tb1);
    c.w(&WorkerBuilder::new(12).res_sum("gpus", 20))
        .expect_request_v(6, &tb1, 1);
    c.check();
}

fn task_count(msg: &ToWorkerMessage) -> usize {
    match msg {
        ToWorkerMessage::ComputeTasks(cm) => cm.tasks.len(),
        _ => 0,
    }
}

#[test]
fn test_no_deps_scattering_1() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[5, 5, 5]);
    rt.new_tasks(4, &TaskBuilder::new());

    let mut comm = rt.schedule();

    let m1 = comm.take_worker_msgs(ws[0], 0);
    let m2 = comm.take_worker_msgs(ws[1], 0);
    let m3 = comm.take_worker_msgs(ws[2], 0);
    comm.emptiness_check();
    rt.core().sanity_check();

    let c1 = if m1.len() > 0 { task_count(&m1[0]) } else { 0 };
    let c2 = if m2.len() > 0 { task_count(&m2[0]) } else { 0 };
    let c3 = if m3.len() > 0 { task_count(&m3[0]) } else { 0 };

    assert_eq!(c1, 4);
    assert_eq!(c2, 0);
    assert_eq!(c3, 0);
}

#[test]
fn test_no_deps_scattering_2() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);

    let mut submit_and_check = |expected| {
        let _t = rt.new_task_default();
        rt.schedule();
        let mut counts: Vec<_> = rt
            .core()
            .get_workers()
            .map(|w| w.sn_assignment().unwrap().assign_tasks.len())
            .collect();
        counts.sort();
        assert_eq!(counts, expected);
    };

    for i in 1..=5 {
        submit_and_check(vec![0, 0, i as usize])
    }

    for i in 1..=5 {
        submit_and_check(vec![0, i as usize, 5]);
    }

    for i in 1..=5 {
        submit_and_check(vec![i as usize, 5, 5]);
    }

    submit_and_check(vec![5, 5, 5]);
    submit_and_check(vec![5, 5, 5]);
}

#[test]
fn test_no_deps_distribute() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 10,
        proactive_filling_max: 20,
        ..Default::default()
    });
    let ws = rt.new_workers_cpus(&[10, 10, 10]);
    rt.new_tasks(150, &TaskBuilder::new());

    let mut comm = rt.schedule();

    let m1 = comm.take_worker_msgs(ws[0], 1);
    let m2 = comm.take_worker_msgs(ws[1], 1);
    let m3 = comm.take_worker_msgs(ws[2], 1);
    comm.emptiness_check();
    rt.sanity_check();

    assert_eq!(task_count(&m1[0]), 30);
    assert_eq!(task_count(&m2[0]), 30);
    assert_eq!(task_count(&m3[0]), 30);
}

#[test]
fn test_resource_time_assign() {
    let mut rt = TestEnv::new();

    let w1 = rt.new_worker(&WorkerBuilder::new(10).time_limit(Duration::new(100, 0)));

    let _t1 = rt.new_task(&TaskBuilder::new().time_request(170));
    let t2 = rt.new_task_default();
    let t3 = rt.new_task(&TaskBuilder::new().time_request(99));

    rt.schedule();
    rt.check_worker_tasks(w1, &[t2, t3]);
}

#[test]
fn test_resource_time_balance1() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut rt = TestEnv::new();

    let w1 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(50, 0)));
    let w2 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(200, 0)));
    let w3 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(100, 0)));

    let t1 = rt.new_task(&TaskBuilder::new().time_request(170));
    let t2 = rt.new_task(&TaskBuilder::new());
    let t3 = rt.new_task(&TaskBuilder::new().time_request(99));

    rt.schedule();
    rt.check_worker_tasks(w1, &[t2]);
    rt.check_worker_tasks(w2, &[t1]);
    rt.check_worker_tasks(w3, &[t3]);
}

#[test]
fn test_generic_resource_assign2() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);

    let w1 = rt.new_worker(&WorkerBuilder::new(10).res_range("Res0", 1, 10));
    let w2 = rt.new_worker(&WorkerBuilder::new(10));
    let w3 = rt.new_worker(
        &WorkerBuilder::new(10)
            .res_range("Res0", 1, 10)
            .res_sum("Res1", 1_000_000),
    );

    let ts1 = rt.new_tasks(50, &TaskBuilder::new().add_resource(1, 1));
    let _ts2 = rt.new_tasks(50, &TaskBuilder::new().add_resource(1, 2));

    rt.schedule();

    assert_eq!(rt.worker_tasks(w1).len(), 10);
    assert_eq!(rt.worker_tasks(w2).len(), 0);
    assert_eq!(rt.worker_tasks(w3).len(), 10);
    assert!(
        rt.worker_tasks(w1)
            .iter()
            .all(|task_id| ts1.contains(task_id))
    );
    assert!(
        rt.worker_tasks(w2)
            .iter()
            .all(|task_id| ts1.contains(task_id))
    );
}

#[test]
fn test_generic_resource_balance1() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    let w1 = rt.new_worker(&WorkerBuilder::new(10).res_range("Res0", 1, 10));
    let w2 = rt.new_worker(&WorkerBuilder::new(10));
    let w3 = rt.new_worker(
        &WorkerBuilder::new(10)
            .res_range("Res0", 1, 10)
            .res_sum("Res1", 1_000_000),
    );

    rt.new_tasks(4, &TaskBuilder::new().cpus(1).add_resource(1, 5));
    rt.schedule();

    assert_eq!(rt.worker_tasks(w1).len(), 2);
    assert_eq!(rt.worker_tasks(w2).len(), 0);
    assert_eq!(rt.worker_tasks(w3).len(), 2);
}

#[test]
fn test_generic_resource_balance2() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    let w1 = rt.new_worker(&WorkerBuilder::new(10).res_range("Res0", 1, 10));
    let w2 = rt.new_worker(&WorkerBuilder::new(10));
    let w3 = rt.new_worker(
        &WorkerBuilder::new(10)
            .res_range("Res0", 1, 10)
            .res_sum("Res1", 1_000_000),
    );

    rt.new_task(&TaskBuilder::new().cpus(1).add_resource(1, 5));
    rt.new_task(
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(1, 5)
            .add_resource(2, 500_000),
    );
    rt.new_task(&TaskBuilder::new().cpus(1).add_resource(1, 5));
    rt.new_task(
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(1, 5)
            .add_resource(2, 500_000),
    );
    rt.schedule();

    assert_eq!(rt.worker_tasks(w1).len(), 2);
    assert_eq!(rt.worker_tasks(w2).len(), 0);
    assert_eq!(rt.worker_tasks(w3).len(), 2);
}

#[test]
fn test_generic_resource_balancing3() {
    // Submits 80 (rq1) + 20 (rq2) tasks,
    // Expects:
    // on w1 there will be 2 rq1 assigned + 38 prefilled rq1
    // on w2 there will be 1 rq1 and 1 rq2 + 38 prefilled rq1 and 19 prefilled rq2

    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 0,
        proactive_filling_max: 100,
        ..Default::default()
    });
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(2));
    let w2 = rt.new_worker(&WorkerBuilder::new(2).res_range("Res0", 1, 1));
    let ts1 = rt.new_tasks(80, &TaskBuilder::new());
    let ts2 = rt.new_tasks(20, &TaskBuilder::new().cpus(1).add_resource(1, 1));

    let rq1 = rt.task(ts1[0]).resource_rq_id;
    let rq2 = rt.task(ts2[0]).resource_rq_id;

    rt.schedule();

    let w = rt.worker(w1);
    let a = w.sn_assignment().unwrap();
    assert_eq!(a.assign_tasks.len(), 2);
    assert!(
        a.assign_tasks
            .iter()
            .all(|t| rt.task(*t).resource_rq_id == rq1)
    );
    assert_eq!(a.prefilled_tasks.len(), 38);
    assert_eq!(
        a.prefilled_tasks
            .iter()
            .filter(|t| rt.task(**t).resource_rq_id == rq1)
            .count(),
        38
    );

    let w = rt.worker(w2);
    let a = w.sn_assignment().unwrap();
    assert_eq!(a.assign_tasks.len(), 2);
    assert_eq!(a.prefilled_tasks.len(), 57);
    assert_eq!(
        a.prefilled_tasks
            .iter()
            .filter(|t| rt.task(**t).resource_rq_id == rq1)
            .count(),
        38
    );
    assert_eq!(
        a.prefilled_tasks
            .iter()
            .filter(|t| rt.task(**t).resource_rq_id == rq2)
            .count(),
        19
    );
}

#[test]
fn test_generic_resource_variants1() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(4));
    let w2 = rt.new_worker(&WorkerBuilder::new(4).res_range("Res0", 1, 2));

    let task = TaskBuilder::new()
        .cpus(2)
        .next_variant()
        .cpus(1)
        .add_resource(1, 1);
    rt.new_tasks(4, &task);
    rt.schedule();

    assert_eq!(rt.worker_tasks(w1).len(), 2);
    assert_eq!(rt.worker_tasks(w2).len(), 2);
}

#[test]
fn test_generic_resource_variants2() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(4));
    let w2 = rt.new_worker(&WorkerBuilder::new(4).res_range("Res0", 1, 2));

    let task = TaskBuilder::new()
        .cpus(8)
        .next_variant()
        .cpus(1)
        .add_resource(1, 1);
    rt.new_tasks(4, &task);
    rt.schedule();

    assert_eq!(rt.worker_tasks(w1).len(), 0);
    assert_eq!(rt.worker_tasks(w2).len(), 2);
}

#[test]
fn test_generic_resource_variants3() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(2));
    let w2 = rt.new_worker(&WorkerBuilder::new(5).res_range("Res0", 1, 1));

    let task = TaskBuilder::new()
        .cpus(3)
        .next_variant()
        .cpus(1)
        .add_resource(1, 1);
    rt.new_tasks(4, &task);
    rt.schedule();

    assert_eq!(rt.worker_tasks(w1).len(), 0);
    assert_eq!(rt.worker_tasks(w2).len(), 2);
}

#[test]
fn test_scheduler_two_running_three_waiting() {
    let mut rt = TestEnv::new();
    rt.new_named_resource("foo");
    let w = rt.new_worker(&WorkerBuilder::new(8).res_range("foo", 1, 4));
    let ts = rt.new_tasks(4, &TaskBuilder::new().cpus(1).add_resource(1, 2));
    rt.assign_and_start_task(ts[0], w, 0);
    rt.assign_and_start_task(ts[1], w, 0);
    let t5 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(1));

    rt.schedule();

    assert!(rt.task(t5).is_assigned());
    assert!(rt.task(ts[0]).is_sn_running());
    assert!(rt.task(ts[1]).is_sn_running());
    assert!(rt.task(ts[2]).is_waiting());
    assert!(rt.task(ts[3]).is_waiting());
}

#[test]
fn test_many_cuts() {
    let mut rt = TestEnv::new();
    rt.new_workers(300, &WorkerBuilder::new(8));
    let mut ts1 = Vec::new();
    let mut ts2 = Vec::new();
    for i in 0..3200 {
        ts1.push(rt.new_task(&TaskBuilder::new().cpus(1).user_priority(i)));
        ts2.push(rt.new_task(&TaskBuilder::new().cpus(2).user_priority(i)));
    }
    rt.schedule();
    let c1 = ts1.iter().filter(|t| rt.task(**t).is_assigned()).count();
    let c2 = ts2.iter().filter(|t| rt.task(**t).is_assigned()).count();
    dbg!(c1, c2);
    assert!(c1.abs_diff(c2) < 10);
    assert!(c1.abs_diff(800) < 10);
    assert!(c2.abs_diff(800) < 10);
}

fn prefill_count(rt: &mut TestEnv, worker_id: WorkerId) -> u32 {
    let n = rt
        .worker(worker_id)
        .sn_assignment()
        .unwrap()
        .prefilled_tasks
        .len();
    let mut count = 0;
    for task in rt.core().task_map().tasks() {
        match task.state {
            TaskRuntimeState::Prefilled { worker_id: w_id } if w_id == worker_id => {
                count += 1;
            }
            _ => {}
        }
    }
    assert_eq!(count, n);
    n as u32
}

#[test]
fn test_prefill_basic() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 4,
        proactive_filling_max: 32,
        ..Default::default()
    });
    let ws = rt.new_workers(2, &WorkerBuilder::new(8));
    let tasks = rt.new_tasks(300, &TaskBuilder::new().cpus(4));
    let resource_rq_id = rt.task(tasks[0]).resource_rq_id;
    let priority = rt.task(tasks[0]).priority();
    let mut comm = rt.schedule();
    for w in &ws {
        let msg = comm.take_worker_msgs(*w, 1);
        match &msg[0] {
            ToWorkerMessage::ComputeTasks(ts) => {
                assert_eq!(ts.tasks.len(), 34);
                for (i, t) in ts.tasks.iter().enumerate() {
                    assert_eq!(t.resource_rq_variant.is_none(), i < 32);
                }
            }
            _ => panic!("unexpected message"),
        };
    }
    comm.emptiness_check();
    for w in &ws {
        assert_eq!(prefill_count(&mut rt, *w), 32);
    }
    let queue = rt.core().task_queues_mut().get(resource_rq_id);
    let p: Vec<_> = queue.iter_priority_sizes().collect();
    assert_eq!(p, vec![(priority, 296)]); // 296 = 300 - 4 assigned tasks
}

#[test]
fn test_prefill_choose_waiting() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 3,
        proactive_filling_max: 6,
        ..Default::default()
    });
    let w1 = rt.new_worker(&WorkerBuilder::new(1));
    rt.new_tasks(15, &TaskBuilder::new());
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w1), 6);
    let w2 = rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w1), 6);
    assert_eq!(prefill_count(&mut rt, w2), 4);
    let w3 = rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w1), 6);
    assert_eq!(prefill_count(&mut rt, w2), 4);
    assert_eq!(prefill_count(&mut rt, w3), 0);
}

#[test]
fn test_prefill_steal() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 3,
        proactive_filling_max: 6,
        ..Default::default()
    });
    let w1 = rt.new_worker(&WorkerBuilder::new(1));
    let tasks = rt.new_tasks(9, &TaskBuilder::new());
    let resource_rq_id = rt.task(tasks[0]).resource_rq_id;
    let priority = rt.task(tasks[0]).priority();
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w1), 5);
    let w2 = rt.new_worker(&WorkerBuilder::new(5));

    let queue = rt.core().task_queues_mut().get(resource_rq_id);
    let p: Vec<_> = queue.iter_priority_sizes().collect();
    assert_eq!(p, vec![(priority, 8)]);

    let mut comm = rt.schedule();
    let msg = comm.take_worker_msgs(w1, 1);
    assert!(matches!(&msg[0], ToWorkerMessage::RetractTasks(ts) if ts.ids.len() == 2));
    let msg = comm.take_worker_msgs(w2, 1);
    match &msg[0] {
        ToWorkerMessage::ComputeTasks(ts) => {
            assert_eq!(ts.tasks.len(), 3);
        }
        _ => unreachable!(),
    }
    comm.emptiness_check();
    let r = rt
        .core()
        .split()
        .scheduler_state
        .redirects
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let rv = ResourceVariantId::new(0);
    assert_eq!(r, vec![(w2, rv), (w2, rv)]);
    assert_eq!(prefill_count(&mut rt, w1), 3);
    assert_eq!(prefill_count(&mut rt, w2), 0);
    assert_eq!(rt.worker(w1).sn_assignment().unwrap().assign_tasks.len(), 1);
    assert_eq!(rt.worker(w2).sn_assignment().unwrap().assign_tasks.len(), 5);
    assert_eq!(rt.core().split_mut().scheduler_state.redirects.len(), 2);
    let (t, _) = rt
        .core()
        .split_mut()
        .scheduler_state
        .redirects
        .iter()
        .next()
        .unwrap();
    let t = *t;

    let mut comm = TestComm::new();
    on_retract_response(rt.core(), &mut comm, w1, &[t]);
    let msgs = comm.take_worker_msgs(w2, 1);
    match &msgs[0] {
        ToWorkerMessage::ComputeTasks(ts) => {
            assert_eq!(ts.tasks.len(), 1);
            assert_eq!(ts.tasks[0].id, t);
        }
        _ => unreachable!(),
    }
    comm.emptiness_check();
    match &rt.task(t).state {
        TaskRuntimeState::Assigned { worker_id, .. } => {
            assert_eq!(*worker_id, w2);
        }
        _ => unreachable!(),
    }
    assert_eq!(rt.core().split_mut().scheduler_state.redirects.len(), 1);
    rt.sanity_check();
}

#[test]
pub fn test_schedule_variant_gap() {
    let mut rt = TestEnv::new();
    rt.new_named_resource("gpus");
    // 8 cpus OR 1 cpus + 2 gpus
    rt.new_tasks(
        10,
        &TaskBuilder::new()
            .user_priority(10)
            .cpus(8)
            .next_variant()
            .cpus(4)
            .add_resource(1, 2),
    );
    let ts = rt.new_tasks(10, &TaskBuilder::new());
    rt.new_worker(&WorkerBuilder::new(14).res_sum("gpus", 4));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 2);
}
