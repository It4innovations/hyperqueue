use crate::internal::common::resources::ResourceId;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::scheduler::{PriorityCut, SchedulerConfig, create_task_batches};
use crate::internal::server::reactor::on_retract_response;
use crate::internal::server::task::TaskRuntimeState;
use crate::internal::tests::utils::scheduler::TestCase;
use crate::resources::ResourceRqId;
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;
use crate::{ResourceVariantId, TaskId, WorkerId};
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

    let _t2 = rt.new_task(&TaskBuilder::new().user_priority(20));
    let _t3 = rt.new_task(&TaskBuilder::new().user_priority(5));
    let _t4 = rt.new_task(&TaskBuilder::new().user_priority(123));
    let _t5 = rt.new_task(&TaskBuilder::new().user_priority(20));

    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 1);
    let r1 = rt.task(t1).resource_rq_id;
    assert_eq!(a[0].resource_rq_id, r1);
    assert!(a[0].cuts.is_empty());
    assert_eq!(a[0].size, 5);
    assert!(!a[0].limit_reached);

    let t6 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));
    let t7 = rt.new_task(&TaskBuilder::new().cpus(123).user_priority(123));
    let _t8 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));
    let _t9 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(123));

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
fn test_task_grouping_same_level_needs_no_cut() {
    // a: 5 4, b: 4. Nothing of b is strictly above a's task at 4, so `a` needs no cut.
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5]);
    let a5 = rt.new_task(&TaskBuilder::new().user_priority(5));
    rt.new_task(&TaskBuilder::new().user_priority(4));
    rt.new_task(&TaskBuilder::new().cpus(2).user_priority(4));
    let rq_a = rt.task(a5).resource_rq_id;
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 2);
    assert_eq!(a[0].resource_rq_id, rq_a);
    assert_eq!(a[0].size, 2);
    assert_eq!(a[0].cuts, vec![]);
    assert_eq!(
        a[1].cuts,
        vec![PriorityCut {
            size: 0,
            blockers: vec![(rq_a, Some(1))],
        }]
    );
}

#[test]
fn test_task_grouping_no_repeated_cut() {
    // c: 9, a: 5 4, b: 4. The tasks of other requests above a's task at 4 are the same as
    // above its task at 5 (just c), so `a` gets a single cut.
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[10]);
    let a5 = rt.new_task(&TaskBuilder::new().user_priority(5));
    rt.new_task(&TaskBuilder::new().user_priority(4));
    let b4 = rt.new_task(&TaskBuilder::new().cpus(2).user_priority(4));
    let c9 = rt.new_task(&TaskBuilder::new().cpus(3).user_priority(9));
    let rq_a = rt.task(a5).resource_rq_id;
    let rq_b = rt.task(b4).resource_rq_id;
    let rq_c = rt.task(c9).resource_rq_id;
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 3);
    let get = |rq| a.iter().find(|b| b.resource_rq_id == rq).unwrap();
    assert_eq!(get(rq_a).size, 2);
    assert_eq!(
        get(rq_a).cuts,
        vec![PriorityCut {
            size: 0,
            blockers: vec![(rq_c, Some(1))],
        }]
    );
    assert_eq!(get(rq_b).cuts.len(), 1);
    assert_eq!(get(rq_b).cuts[0].size, 0);
    assert_eq!(get(rq_c).cuts, vec![]);
}

#[test]
fn test_task_group_saturation() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let _t1 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    let _t2 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(2));
    let _t3 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(4));
    let _t4 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(4));
    let _t5 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(6));
    let _t6 = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(6));
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now, None);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].size, 3);
    assert!(a[0].limit_reached);
    assert!(a[0].cuts.is_empty());

    let _t10 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(5));
    let _t11 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(0));

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
        let counts = assigned_counts(&mut rt);
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
    let ws = rt.new_workers(2, &WorkerBuilder::new(34));

    let ta = TaskBuilder::new().cpus(3);
    let tb = TaskBuilder::new().cpus(9);

    rt.new_tasks(5, &ta.clone().user_priority(10));
    let ts2 = rt.new_tasks(6, &tb.clone().user_priority(10));
    let ts3 = rt.new_tasks(5, &ta.clone().user_priority(9));
    rt.schedule();

    for w in ws {
        let mut cpus = 0;
        let mut t3count = 0;
        for t in &rt.worker(w).sn_assignment().unwrap().assigned_tasks {
            if ts2.contains(t) {
                cpus += 9;
            } else {
                cpus += 3;
                if ts3.contains(t) {
                    t3count += 1;
                }
            }
        }
        assert_eq!(cpus, 33);
        assert!(t3count <= 2);
    }
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
    let _ts = c.pc_tasks(&[(4, 3), (3, 3), (3, 3), (2, 1), (2, 1)]);
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
    let _t1 = c.t(&tb1_2);
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
            .map(|w| w.sn_assignment().unwrap().assigned_tasks.len())
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
    assert_eq!(a.assigned_tasks.len(), 2);
    assert!(
        a.assigned_tasks
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
    assert_eq!(a.assigned_tasks.len(), 2);
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

#[cfg(not(feature = "microlp"))] // To big tests for microlp
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
    let queue = rt.core().split_mut().task_queues.get(resource_rq_id);
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

    let queue = rt.core().split_mut().task_queues.get(resource_rq_id);
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
    assert_eq!(
        rt.worker(w1).sn_assignment().unwrap().assigned_tasks.len(),
        1
    );
    assert_eq!(
        rt.worker(w2).sn_assignment().unwrap().assigned_tasks.len(),
        5
    );
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
fn test_gap_over_redirected_retracting_task() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 3,
        proactive_filling_max: 6,
        ..Default::default()
    });

    // -- Precondition 1: a redirected retracting task in w2's `assigned_tasks`.
    // Same opening as `test_prefill_steal`: w1 prefills, then the larger w2 steals part of that
    // prefill, so those tasks become `Retracting` (on w1) while being assigned to w2.
    let w1 = rt.new_worker(&WorkerBuilder::new(1));
    let low = rt.new_tasks(9, &TaskBuilder::new());
    let low_rq_id = rt.task(low[0]).resource_rq_id;
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w1), 5, "w1 did not prefill");
    let w2 = rt.new_worker(&WorkerBuilder::new(5));
    rt.schedule();

    // Asserted rather than assumed: if prefill ever stops producing this state, the test must
    // fail loudly instead of passing while reproducing nothing.
    assert!(
        !rt.core().split().scheduler_state.redirects.is_empty(),
        "no redirect was created, so the state under test does not exist"
    );
    let retracting_on_w2 = rt
        .worker(w2)
        .sn_assignment()
        .unwrap()
        .assigned_tasks
        .iter()
        .filter(|task_id| rt.task(**task_id).rv_id().is_none())
        .count();
    assert!(
        retracting_on_w2 > 0,
        "w2 holds no assigned task without an rv_id; the unwrap cannot be reached"
    );

    // -- Precondition 2: spare capacity for the low-priority request.
    // The solver skips a batch that has no placement variables, and after the steal both w1 and
    // w2 are full -- so without this the low-priority batch, and with it the entire
    // priority-condition path, is never visited. A worker added now cannot undo the redirect
    // already recorded above. 2 cpus is deliberately too narrow for the blocker below, so this
    // worker contributes capacity without becoming a candidate for it.
    rt.new_worker(&WorkerBuilder::new(2));

    // -- Precondition 3: a blocker, so the solver builds a priority condition at all.
    // A cut is only emitted when a *second* queue is non-empty at a higher priority, so this
    // needs a distinct resource request. 5 cpus fits w2's total width -- w2 is therefore
    // `is_capable_to_run_rqv` and is not skipped by the impossible-filter -- but w2 has no room
    // for it right now, which is what makes it block.
    rt.new_tasks(2, &TaskBuilder::new().cpus(5).user_priority(10));

    let batches = create_task_batches(rt.core(), std::time::Instant::now(), None);
    let low_batch = batches
        .iter()
        .find(|b| b.resource_rq_id == low_rq_id)
        .expect("the low-priority request must still have a batch");
    assert!(
        low_batch.cuts.iter().any(|cut| !cut.blockers.is_empty()),
        "no blocker cut was produced, so the gap path is never entered: {:?}",
        low_batch.cuts
    );

    rt.schedule();
    rt.sanity_check();
}

/// Prefill has to be weighted by worker capacity, otherwise a small worker is handed as many
/// tasks as a large one and takes proportionally longer to drain them. Prefilled tasks are
/// removed from the global queue and are not reclaimed while regular tasks of the same priority
/// remain, so the small worker ends up sitting on an older job's tail long after every large
/// worker has moved on to newer jobs.
#[test]
fn test_prefill_weighted_by_worker_capacity() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 0,
        proactive_filling_max: 32,
        ..Default::default()
    });
    let w_big = rt.new_worker(&WorkerBuilder::new(16));
    let w_small = rt.new_worker(&WorkerBuilder::new(2));
    rt.new_tasks(300, &TaskBuilder::new());
    rt.schedule();

    // `proactive_filling_max` applies to the largest worker; everyone else is scaled down by
    // capacity. An unweighted split would give both workers 32.
    let big = prefill_count(&mut rt, w_big);
    let small = prefill_count(&mut rt, w_small);
    assert_eq!(big, 32);
    assert_eq!(small, 4);

    // The property that actually matters: both hold the same *duration* of backlog, i.e. the
    // same number of task generations (two each here).
    assert_eq!(big / 16, small / 2);
    rt.sanity_check();
}

#[test]
fn test_priority_is_not_overridden_by_weight() {
    let narrow = TaskBuilder::new().cpus(1).user_priority(10);
    let wide = TaskBuilder::new().cpus(4).user_priority(0).weight(10.0);

    let mut c = TestCase::new();
    c.n_tasks(10, &narrow);
    c.n_tasks(4, &wide);
    // All capacity goes to the high-priority 1-cpu tasks despite the 10x weight on the others.
    c.w(&WorkerBuilder::new(8)).expect_request(8, &narrow);
    c.w(&WorkerBuilder::new(2)).expect_request(2, &narrow);
    c.check();
}

#[test]
fn test_weight_prefers_request_at_equal_priority() {
    let narrow = TaskBuilder::new().cpus(1);
    let wide = TaskBuilder::new().cpus(4).weight(4.0);

    let mut c = TestCase::new();
    c.n_tasks(10, &narrow);
    c.n_tasks(2, &wide);
    c.w(&WorkerBuilder::new(8)).expect_request(2, &wide);
    c.w(&WorkerBuilder::new(2)).expect_request(2, &narrow);
    c.check();
}

/// The prefill depth must be measured against the largest worker in the *cluster*, not the
/// largest one eligible for prefill in this round. A worker is skipped while it still holds
/// prefill of the request, so the eligible set is routinely all-small -- and if the reference
/// capacity is taken from it, the depth springs back to `proactive_filling_max` for a tiny
/// worker, which is the whole bug.
#[test]
fn test_prefill_depth_when_large_worker_is_ineligible() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 0,
        proactive_filling_max: 32,
        ..Default::default()
    });
    let w_big = rt.new_worker(&WorkerBuilder::new(16));
    rt.new_tasks(400, &TaskBuilder::new());
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w_big), 32);

    // w_big now holds prefill of this request, so it is excluded from further prefill and
    // only the 2-cpu worker is eligible.
    let w_small = rt.new_worker(&WorkerBuilder::new(2));
    rt.schedule();
    assert_eq!(prefill_count(&mut rt, w_big), 32);
    assert_eq!(prefill_count(&mut rt, w_small), 4);
    rt.sanity_check();
}

#[test]
pub fn test_schedule_running() {
    let mut rt = TestEnv::new();
    let w = rt.new_worker(&WorkerBuilder::new(14));
    for _ in 0..8 {
        rt.new_task_running(&TaskBuilder::new(), w);
    }
    let ts = rt.new_tasks(10, &TaskBuilder::new());
    rt.schedule();
    assert_eq!(
        rt.worker(w).sn_assignment().unwrap().assigned_tasks.len(),
        14
    );
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 6);
}

#[test]
pub fn test_schedule_variant_gap1() {
    for running in [0, 1, 2] {
        let mut rt = TestEnv::new();
        rt.new_named_resource("gpus");
        let w = rt.new_worker(&WorkerBuilder::new(14).res_sum("gpus", 4));
        for _ in 0..running {
            rt.new_task_running(&TaskBuilder::new(), w);
        }

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
        rt.schedule();
        assert_eq!(
            ts.iter().filter(|t| rt.task(**t).is_assigned()).count(),
            2 - running
        );
    }
}

#[test]
pub fn test_schedule_resource_weights1() {
    let mut rt = TestEnv::new();
    let t1 = rt.new_task(&TaskBuilder::new().cpus(3));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(2).weight(1.49));
    rt.new_worker(&WorkerBuilder::new(4));
    rt.schedule();
    assert!(rt.task(t1).is_assigned());
    assert!(rt.task(t2).is_waiting());

    let mut rt = TestEnv::new();
    let t1 = rt.new_task(&TaskBuilder::new().cpus(3).weight(1.0));
    let t2 = rt.new_task(&TaskBuilder::new().cpus(2).weight(1.51));
    rt.new_worker(&WorkerBuilder::new(4));
    rt.schedule();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_assigned());
}

#[test]
pub fn test_schedule_resource_weights2() {
    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(5, &TaskBuilder::new().cpus(3).weight(1.1));
    let t1 = rt.new_task(&TaskBuilder::new().cpus_all());
    rt.new_worker(&WorkerBuilder::new(12));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 4);
    assert!(rt.task(t1).is_waiting());

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(5, &TaskBuilder::new().cpus(3));
    let t1 = rt.new_task(&TaskBuilder::new().cpus_all().weight(1.1));
    rt.new_worker(&WorkerBuilder::new(12));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 0);
    assert!(rt.task(t1).is_assigned());
}

#[test]
pub fn test_schedule_min_utilization1() {
    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(2, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(9).min_utilization(1.0));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 0);

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(3, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(9).min_utilization(1.0));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 3);

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(2, &TaskBuilder::new().cpus(3));
    let w = rt.new_worker(&WorkerBuilder::new(9).min_utilization(1.0));
    rt.new_task_running(&TaskBuilder::new().cpus(3), w);
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 2);
}

#[test]
pub fn test_schedule_min_utilization2() {
    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(2, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(0.5));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 2);

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(2, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(0.51));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 0);

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(3, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(0.51));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 3);

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(3, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(0.75));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 3);

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(3, &TaskBuilder::new().cpus(3));
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(0.76));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 0);
}

#[test]
pub fn test_schedule_min_utilization3() {
    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(3, &TaskBuilder::new().cpus(3).weight(2.0));
    let t2 = rt.new_task(&TaskBuilder::new().cpus_all());
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(1.0));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 0);
    assert!(rt.task(t2).is_assigned());

    let mut rt = TestEnv::new();
    let ts = rt.new_tasks(4, &TaskBuilder::new().cpus(3).weight(2.0));
    let t2 = rt.new_task(&TaskBuilder::new().cpus_all());
    rt.new_worker(&WorkerBuilder::new(12).min_utilization(1.0));
    rt.schedule();
    assert_eq!(ts.iter().filter(|t| rt.task(**t).is_assigned()).count(), 4);
    assert!(!rt.task(t2).is_assigned());
}

// Bounded-solve tests: unit tests default to a generous mip_time_limit
// (SchedulerConfig::default), so these opt into a short one explicitly.

#[test]
fn test_schedule_many_distinct_shapes_stays_bounded() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        mip_time_limit: Duration::from_secs(5),
        ..Default::default()
    });
    rt.new_named_resource("mem");
    for _ in 0..20 {
        rt.new_worker(&WorkerBuilder::new(64).res_sum("mem", 459_000));
    }
    for i in 0..60u32 {
        rt.new_tasks(2, &TaskBuilder::new().cpus(1 + (i % 60)));
    }

    let start = std::time::Instant::now();
    rt.schedule();
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[test]
fn test_schedule_bounded_infeasible_returns_none_safely() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        mip_time_limit: Duration::from_secs(5),
        ..Default::default()
    });
    rt.new_worker(&WorkerBuilder::new(4));
    let t = rt.new_task(&TaskBuilder::new().cpus(999));

    rt.schedule();
    assert!(!rt.task(t).is_assigned());
}

#[test]
fn test_schedule_bounded_is_optimal_true_when_solve_converges() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        mip_time_limit: Duration::from_secs(5),
        ..Default::default()
    });
    rt.new_worker(&WorkerBuilder::new(4));
    rt.new_tasks(2, &TaskBuilder::new().cpus(1));

    assert!(rt.schedule_solution().is_optimal);
}

#[test]
fn test_schedule_reservation_priority() {
    let mut c = TestCase::new();
    let ht = c.t(&TaskBuilder::new().cpus(6).user_priority(10));
    c.ts(6, &TaskBuilder::new().cpus(1));
    c.w(&WorkerBuilder::new(6)).running_c(6);
    c.w(&WorkerBuilder::new(6)).expect_tasks(&[ht]);
    c.check();
}

#[test]
fn test_schedule_reservation_used_when_worker_frees_up() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(4, &WorkerBuilder::new(6));
    let running: Vec<_> = ws
        .iter()
        .map(|w| rt.new_task_running(&TaskBuilder::new().cpus(4), *w))
        .collect();
    let blocker = rt.new_task(&TaskBuilder::new().cpus(6).user_priority(10));
    let small = rt.new_tasks(30, &TaskBuilder::new().cpus(1));

    fn on_worker(rt: &TestEnv, task_id: TaskId, worker_id: WorkerId) -> bool {
        matches!(
            rt.task(task_id).state,
            TaskRuntimeState::Assigned { worker_id: w, .. } if w == worker_id
        )
    }

    rt.schedule();

    // One worker is held back for the blocker, the three others take two tasks each.
    let reserved_idx = ws
        .iter()
        .position(|w| !small.iter().any(|t| on_worker(&rt, *t, *w)))
        .expect("no worker was reserved for the blocker");
    assert_eq!(
        small.iter().filter(|t| rt.task(**t).is_assigned()).count(),
        6
    );
    assert!(rt.task(blocker).is_waiting());

    // The reserved worker becomes completely free, which is exactly what the blocker waits for.
    let reserved = ws[reserved_idx];
    rt.finish_task(running[reserved_idx], reserved);
    rt.schedule();

    assert!(
        on_worker(&rt, blocker, reserved),
        "reserved worker {reserved} was not used for the blocker, state: {:?}",
        rt.task(blocker).state
    );
    assert_eq!(
        small
            .iter()
            .filter(|t| on_worker(&rt, **t, reserved))
            .count(),
        0
    );
}

#[test]
fn test_schedule_lone_blocker_holds_freed_capacity() {
    /// Narrow tasks placed while `n_wide` blockers are queued; must be 0 for every `n_wide`.
    fn narrow_placed(n_wide: usize) -> usize {
        let mut rt = TestEnv::new();
        let ws = rt.new_workers(4, &WorkerBuilder::new(8));
        for (idx, w) in ws.iter().enumerate() {
            // The first worker has just freed 2 of its 8 cpus; the rest are saturated.
            let running = if idx == 0 { 6 } else { 8 };
            rt.new_task_running(&TaskBuilder::new().cpus(running), *w);
        }
        rt.new_tasks(n_wide, &TaskBuilder::new().cpus(8).user_priority(10));
        let narrow = rt.new_tasks(30, &TaskBuilder::new().cpus(1));
        rt.schedule();
        narrow.iter().filter(|t| rt.task(**t).is_assigned()).count()
    }

    for n_wide in 1..=4 {
        assert_eq!(
            narrow_placed(n_wide),
            0,
            "low priority work took the freed cpus with {n_wide} wide task(s) queued"
        );
    }
}

#[test]
fn test_schedule_lone_blocker_accumulates_capacity_over_rounds() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(4, &WorkerBuilder::new(8));
    let mut running: Vec<Vec<TaskId>> = ws
        .iter()
        .map(|w| {
            (0..8)
                .map(|_| rt.new_task_running(&TaskBuilder::new().cpus(1), *w))
                .collect()
        })
        .collect();
    let blocker = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    rt.new_tasks(60, &TaskBuilder::new().cpus(1));

    // Eight rounds of "one task finishes everywhere" is exactly what a held worker needs to reach
    // 8 free cpus; the extra round gives the scheduler the chance to place the blocker afterwards.
    for _tick in 0..=8 {
        rt.schedule();
        if rt.task(blocker).is_assigned() {
            return;
        }
        for (idx, w) in ws.iter().enumerate() {
            if let Some(task_id) = running[idx].pop() {
                rt.finish_task(task_id, *w);
            }
        }
    }

    let free: Vec<String> = ws
        .iter()
        .map(|w| {
            let a = rt.worker(*w).sn_assignment().unwrap();
            format!("w{w}: {:?}", a.free_resources.get(ResourceId::new(0)))
        })
        .collect();
    panic!(
        "blocker never started; no worker reassembled 8 free cpus ({})",
        free.join(", ")
    );
}

#[test]
fn test_schedule_reservation_holds_emptiest_worker() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(4, &WorkerBuilder::new(8));
    // w0 has 2 cpus free, w1 has 4, the rest are saturated. w1 is the closest to fitting an
    // 8-cpu task, even though w0 comes first in worker order.
    for (idx, w) in ws.iter().enumerate() {
        let running = match idx {
            0 => 6,
            1 => 4,
            _ => 8,
        };
        rt.new_task_running(&TaskBuilder::new().cpus(running), *w);
    }
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    let narrow = rt.new_tasks(30, &TaskBuilder::new().cpus(1));
    rt.schedule();

    let placed_on = |rt: &TestEnv, worker_id: WorkerId| {
        narrow
            .iter()
            .filter(|t| {
                matches!(rt.task(**t).state,
                    TaskRuntimeState::Assigned { worker_id: w, .. } if w == worker_id)
            })
            .count()
    };
    assert_eq!(placed_on(&rt, ws[1]), 0, "the emptiest worker must be held");
    assert_eq!(
        placed_on(&rt, ws[0]),
        2,
        "the other worker must still be used"
    );
}

#[test]
fn test_schedule_reservation_leaves_other_workers_for_backfill() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(3, &WorkerBuilder::new(8));
    for w in &ws {
        rt.new_task_running(&TaskBuilder::new().cpus(6), *w);
    }
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    let narrow = rt.new_tasks(30, &TaskBuilder::new().cpus(1));
    rt.schedule();

    let held: Vec<_> = ws
        .iter()
        .filter(|w| {
            !narrow.iter().any(|t| {
                matches!(rt.task(*t).state,
                    TaskRuntimeState::Assigned { worker_id, .. } if worker_id == **w)
            })
        })
        .collect();
    assert_eq!(held.len(), 1, "exactly one worker is held for the blocker");
    assert_eq!(
        narrow.iter().filter(|t| rt.task(**t).is_assigned()).count(),
        4,
        "the two other workers keep their 2 free cpus busy"
    );
}

/// Narrow tasks assigned to each of `workers`, in order.
fn narrow_per_worker(rt: &TestEnv, narrow: &[TaskId], workers: &[WorkerId]) -> Vec<usize> {
    workers
        .iter()
        .map(|w| {
            narrow
                .iter()
                .filter(|t| {
                    matches!(rt.task(**t).state,
                        TaskRuntimeState::Assigned { worker_id, .. } if worker_id == *w)
                })
                .count()
        })
        .collect()
}

#[test]
fn test_schedule_placeable_blocker_holds_nothing() {
    // A wide blocker that a free worker can host right now needs no held worker, so capable but
    // partially occupied workers keep backfilling.
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(4, &WorkerBuilder::new(8));
    for w in &ws[1..] {
        rt.new_task_running(&TaskBuilder::new().cpus(6), *w);
    }
    let blocker = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    let narrow = rt.new_tasks(30, &TaskBuilder::new().cpus(1));
    rt.schedule();

    assert!(
        matches!(rt.task(blocker).state,
            TaskRuntimeState::Assigned { worker_id, .. } if worker_id == ws[0]),
        "the blocker starts on the free worker"
    );
    assert_eq!(
        narrow_per_worker(&rt, &narrow, &ws[1..]),
        vec![2, 2, 2],
        "no worker is held, so every partial worker backfills its 2 free cpus"
    );
}

#[test]
fn test_schedule_lower_priority_cannot_take_capacity_of_higher() {
    // One free worker can host either the higher-priority 4-cpu task or the lower-priority 8-cpu
    // task, but not both, so priority must give it to the 4-cpu task.
    //
    // The saturated worker is what makes this fail. It is capable of the 4-cpu task but has no
    // free cpus, so the solver gets a reservation variable for that task there, and reserving a
    // worker with nothing free costs no capacity. Setting it discharges the priority condition
    // "8-cpu tasks may run only once the 4-cpu task is served", after which the 8-cpu task is the
    // more valuable placement. Held workers do not close this: held-worker depth is the number of
    // blockers no worker can host *before* the solve, and the free worker is counted as hosting
    // the 4-cpu task although the solve then gives it to the 8-cpu one.
    //
    // Without the saturated worker the 4-cpu task is placed correctly.
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(2, &WorkerBuilder::new(8));
    for _ in 0..8 {
        rt.new_task_running(&TaskBuilder::new().cpus(1), ws[1]);
    }
    let high = rt.new_task(&TaskBuilder::new().cpus(4).user_priority(11));
    let wide = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    rt.schedule();

    assert!(
        matches!(rt.task(high).state,
            TaskRuntimeState::Assigned { worker_id, .. } if worker_id == ws[0]),
        "the higher-priority task must take the free worker; high = {:?}, wide = {:?}",
        rt.task(high).state,
        rt.task(wide).state
    );
    assert!(
        !rt.task(wide).is_assigned(),
        "the lower-priority task must wait; wide = {:?}",
        rt.task(wide).state
    );
}

#[test]
fn test_schedule_reservation_cannot_take_capacity_of_higher_blocker() {
    // A reservation for a request must obey that request's own priority conditions, exactly as
    // its placements do. Here `x` (8 cpus, priority 10) cannot run anywhere now, so the emptiest
    // capable worker `w1` is held for it. `h` (4 cpus + a "foo" only `w1` has, priority 20)
    // outranks `x` and fits into `w1`'s free cpus. Reserving `w1` for `x` would consume those
    // cpus and leave `h` waiting behind a lower-priority request.
    //
    // The objective alone does not prevent it. `h` is incapable of the other workers, so a
    // reservation for `x` releases their freed cpus for narrow work without `h` blocking it, and
    // eight workers' worth of backfill outweighs placing `h`.
    let mut rt = TestEnv::new();
    rt.new_named_resource("foo");
    let w1 = rt.new_worker(&WorkerBuilder::new(8).res_sum("foo", 1000));
    rt.new_task_running(&TaskBuilder::new().cpus(4), w1);
    let others = rt.new_workers(8, &WorkerBuilder::new(8));
    for w in &others {
        rt.new_task_running(&TaskBuilder::new().cpus(6), *w);
    }
    let h = rt.new_task(
        &TaskBuilder::new()
            .cpus(4)
            .add_resource(1, 1)
            .user_priority(20),
    );
    let x = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    rt.new_tasks(40, &TaskBuilder::new().cpus(1));
    rt.schedule();

    assert!(
        matches!(rt.task(h).state,
            TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w1),
        "the higher-priority task must take the free cpus on w1; h = {:?}, x = {:?}",
        rt.task(h).state,
        rt.task(x).state
    );
}

#[test]
fn test_schedule_held_worker_not_refilled_when_one_reservation_serves_blocker() {
    // Two 8-cpu tasks share one request, but the narrow request's priority lies between them:
    // `x_high` (10) > narrow (5) > `x_low` (3). The held count covers the whole batch, so two
    // workers are held, while the narrow request is blocked only by `x_high`, i.e. at threshold 1.
    // One reservation therefore serves the blocker for the narrow request, and a second held
    // worker is left with free capacity.
    //
    // `w0` is the emptiest worker and should accumulate for `x_high`. If narrow work may enter a
    // held worker whenever the blocker is served elsewhere, the solver reserves `w1` instead (a
    // reservation on a higher index is cheaper) and refills `w0`, so `x_high` waits for the fuller
    // worker to drain.
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(2, &WorkerBuilder::new(8));
    let mut running: Vec<Vec<TaskId>> = [4, 6]
        .iter()
        .zip(&ws)
        .map(|(n, w)| {
            (0..*n)
                .map(|_| rt.new_task_running(&TaskBuilder::new().cpus(1), *w))
                .collect()
        })
        .collect();
    let x_high = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    rt.new_tasks(40, &TaskBuilder::new().cpus(1).user_priority(5));
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(3));

    // `w0` needs 4 finishes to fit an 8-cpu task, `w1` needs 6.
    for _tick in 0..=4 {
        rt.schedule();
        if rt.task(x_high).is_assigned() {
            return;
        }
        for (idx, w) in ws.iter().enumerate() {
            if let Some(task_id) = running[idx].pop() {
                rt.finish_task(task_id, *w);
            }
        }
    }
    let free: Vec<String> = ws
        .iter()
        .map(|w| {
            let a = rt.worker(*w).sn_assignment().unwrap();
            format!("w{w}: {:?}", a.free_resources.get(ResourceId::new(0)))
        })
        .collect();
    panic!(
        "the priority-10 task did not start once the emptiest worker drained ({})",
        free.join(", ")
    );
}

#[test]
fn test_schedule_no_worker_held_for_blocker_tasks_the_blocked_request_outranks() {
    // `x_high` (10) > narrow (5) > `x_low` (3), both x tasks 8 cpus and unplaceable now. Narrow is
    // blocked only by `x_high`, so only one worker is needed for it: the emptiest, `w0`. Holding a
    // second worker for `x_low` would keep narrow off it although narrow outranks `x_low`.
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(2, &WorkerBuilder::new(8));
    for (n, w) in [4, 6].iter().zip(&ws) {
        for _ in 0..*n {
            rt.new_task_running(&TaskBuilder::new().cpus(1), *w);
        }
    }
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    let narrow = rt.new_tasks(40, &TaskBuilder::new().cpus(1).user_priority(5));
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(3));
    rt.schedule();

    assert_eq!(
        narrow_per_worker(&rt, &narrow, &ws),
        vec![0, 2],
        "w0 is held for x_high; w1 must be free for narrow work"
    );
}

#[test]
fn test_schedule_held_worker_not_refilled_at_a_shallower_threshold() {
    // Two lower requests block on the same 8-cpu request at different thresholds:
    // `x_a` (10) > `l1` (9) > `x_b` (8) > `l2` (7). `l2` needs both x tasks served, so two workers
    // are held and both carry a reservation variable; `l1` needs only `x_a`, i.e. one. A single
    // reservation on the fuller `w1` serves `x_a` for `l1`, so the emptiest `w0` must stay closed to
    // `l1` by the held-worker constraint itself, or `l1` refills it and `x_a` waits for `w1`.
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(2, &WorkerBuilder::new(8));
    let mut running: Vec<Vec<TaskId>> = [4, 6]
        .iter()
        .zip(&ws)
        .map(|(n, w)| {
            (0..*n)
                .map(|_| rt.new_task_running(&TaskBuilder::new().cpus(1), *w))
                .collect()
        })
        .collect();
    let x_a = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    rt.new_tasks(40, &TaskBuilder::new().cpus(1).user_priority(9));
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(8));
    rt.new_tasks(40, &TaskBuilder::new().cpus(2).user_priority(7));

    for _tick in 0..=4 {
        rt.schedule();
        if rt.task(x_a).is_assigned() {
            return;
        }
        for (idx, w) in ws.iter().enumerate() {
            if let Some(task_id) = running[idx].pop() {
                rt.finish_task(task_id, *w);
            }
        }
    }
    let free: Vec<String> = ws
        .iter()
        .map(|w| {
            let a = rt.worker(*w).sn_assignment().unwrap();
            format!("w{w}: {:?}", a.free_resources.get(ResourceId::new(0)))
        })
        .collect();
    panic!(
        "the priority-10 task did not start once the emptiest worker drained ({})",
        free.join(", ")
    );
}

#[test]
fn test_schedule_reservation_pays_on_a_large_cluster() {
    // Placement weights scale with 1 / (free cpus in the cluster), so on a large cluster each
    // placement is worth very little. The reservation penalty must scale with them: a reservation
    // that unlocks placements has to pay for itself regardless of cluster size.
    //
    // `x` (8 cpus + 1 "foo", priority 10) fits nowhere now. `w0` is held for it. `w1` is capable
    // but not held, so narrow work there is blocked until `x` is served, which only a reservation
    // on `w0` can do. `big` contributes 10 000 free cpus but has no "foo": `x` never runs there, so
    // narrow work fills it unconditionally and more narrow tasks wait than it can take.
    let mut rt = TestEnv::new();
    rt.new_named_resource("foo");
    let big = rt.new_worker(&WorkerBuilder::new(10_000));
    let w0 = rt.new_worker(&WorkerBuilder::new(8).res_sum("foo", 1));
    let w1 = rt.new_worker(&WorkerBuilder::new(8).res_sum("foo", 1));
    rt.new_task_running(&TaskBuilder::new().cpus(4), w0);
    rt.new_task_running(&TaskBuilder::new().cpus(6), w1);
    rt.new_task(
        &TaskBuilder::new()
            .cpus(8)
            .add_resource(1, 1)
            .user_priority(10),
    );
    let narrow = rt.new_tasks(10_020, &TaskBuilder::new().cpus(1).user_priority(5));
    rt.schedule();

    assert_eq!(
        narrow_per_worker(&rt, &narrow, &[big, w0, w1]),
        vec![10_000, 0, 2],
        "a reservation on w0 must release w1 even when placements are worth little"
    );
}

#[test]
fn test_schedule_one_worker_cannot_be_reserved_for_two_blockers() {
    // `x` and `y` are different requests at equal priority, both needing the single "foo" of a
    // worker, and neither fits anywhere now. `w0` has 7 free cpus but its "foo" is taken; `w1` is
    // fully occupied. Neither covers any part of either request, so both are held on the higher
    // id, `w1`, which has no free resources. Reservations there consume nothing, so without a
    // per-worker limit `x` and `y` are both served by one worker that can later host only one
    // of them, and narrow work fills `w0` (seven tasks). With the limit, the one not reserved for is
    // held back on `w0` by its own condition, and `w0` offers no gap either: `y` alone could never
    // use more than 7 of its 8 cpus, but `x` can use all eight, and a gap is only capacity that no
    // waiting blocker can use.
    let mut rt = TestEnv::new();
    rt.new_named_resource("foo");
    let w0 = rt.new_worker(&WorkerBuilder::new(8).res_sum("foo", 1));
    let w1 = rt.new_worker(&WorkerBuilder::new(8).res_sum("foo", 1));
    rt.new_task_running(&TaskBuilder::new().cpus(1).add_resource(1, 1), w0);
    rt.new_task_running(&TaskBuilder::new().cpus(8).add_resource(1, 1), w1);
    rt.new_task(
        &TaskBuilder::new()
            .cpus(8)
            .add_resource(1, 1)
            .user_priority(10),
    );
    rt.new_task(
        &TaskBuilder::new()
            .cpus(7)
            .add_resource(1, 1)
            .user_priority(10),
    );
    let narrow = rt.new_tasks(20, &TaskBuilder::new().cpus(1).user_priority(5));
    rt.schedule();

    assert_eq!(
        narrow_per_worker(&rt, &narrow, &[w0, w1]),
        vec![0, 0],
        "one worker can be reserved for at most one blocker, so one of them stays unserved"
    );
}

#[test]
fn test_schedule_reservation_leaves_gap_allowance() {
    // A reservation withholds the worker's free resources for the blocker, but part of them is
    // capacity the blocker can never use: its gap allowance. The priority condition already lets
    // gap tasks run there, and the reservation must not take that capacity away.
    //
    // Two workers of 16 cpus and 4 gpus each run four 1-cpu + 1-gpu tasks, so every gpu is taken
    // and the blocker (2 cpus + 1 gpu) fits nowhere. A full packing of the blocker uses 8 cpus
    // and all 4 gpus, so 8 cpus per worker are never usable by it. While k of the running tasks
    // remain it fits 4 - k times and cannot use 8 + k cpus, so the gap is 8 cpus (the bound
    // `C - M(C) - o` would charge the running tasks' 4 cpus and give only 4). `w1` is held (tie,
    // higher id); a reservation there serves the blocker and releases `w0`, but it must consume
    // only 12 - 8 = 4 of `w1`'s free cpus, leaving room for 8 gap tasks.
    let mut rt = TestEnv::new();
    rt.new_named_resource("gpu");
    let ws = rt.new_workers(2, &WorkerBuilder::new(16).res_sum("gpu", 4));
    for w in &ws {
        for _ in 0..4 {
            rt.new_task_running(&TaskBuilder::new().cpus(1).add_resource(1, 1), *w);
        }
    }
    rt.new_task(
        &TaskBuilder::new()
            .cpus(2)
            .add_resource(1, 1)
            .user_priority(10),
    );
    let narrow = rt.new_tasks(40, &TaskBuilder::new().cpus(1).user_priority(5));
    rt.schedule();

    assert_eq!(
        narrow_per_worker(&rt, &narrow, &ws),
        vec![12, 8],
        "w0 is released by the reservation; the reserved w1 keeps its 8-cpu gap"
    );
}

#[test]
fn test_schedule_reservation_example() {
    // ten 8-cpu workers, each running a single
    // 4-cpu task, one 6-cpu task at priority 2 and a hundred 1-cpu tasks at priority 1. The 6-cpu
    // task fits nowhere, so one worker is held and reserved for it, which releases the other nine
    // (4 cpus each). On the reserved worker the reservation withholds only what the 6-cpu task
    // could use: 8 mod 6 = 2 cpus are its gap and stay available.
    //
    // The single 4-cpu occupant matters: four 1-cpu occupants could leave 6 cpus free at an
    // intermediate point, and the gap would be 0.
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(10, &WorkerBuilder::new(8));
    for w in &ws {
        rt.new_task_running(&TaskBuilder::new().cpus(4), *w);
    }
    rt.new_task(&TaskBuilder::new().cpus(6).user_priority(2));
    let narrow = rt.new_tasks(100, &TaskBuilder::new().cpus(1).user_priority(1));
    rt.schedule();

    let mut expected = vec![4; 9];
    expected.push(2);
    assert_eq!(narrow_per_worker(&rt, &narrow, &ws), expected);
}

#[test]
fn test_schedule_blockers_hold_all_needed_workers() {
    const N_WORKERS: usize = 4;

    /// Narrow tasks placed with `n_partial` workers holding 2 freed cpus and `n_blockers` queued.
    fn narrow_placed(n_partial: usize, n_blockers: usize) -> usize {
        let mut rt = TestEnv::new();
        let ws = rt.new_workers(N_WORKERS, &WorkerBuilder::new(8));
        for (idx, w) in ws.iter().enumerate() {
            let running = if idx < n_partial { 6 } else { 8 };
            rt.new_task_running(&TaskBuilder::new().cpus(running), *w);
        }
        rt.new_tasks(n_blockers, &TaskBuilder::new().cpus(8).user_priority(10));
        let narrow = rt.new_tasks(30, &TaskBuilder::new().cpus(1));
        rt.schedule();
        narrow.iter().filter(|t| rt.task(**t).is_assigned()).count()
    }

    for n_partial in 1..=N_WORKERS {
        for n_blockers in 1..=N_WORKERS {
            // The batch limit is one per capable worker, because no worker can host an 8-cpu task
            // right now. At the limit `blocking_size` becomes `None`, the cut is unconditional on
            // every capable worker and backfill stops there for pre-existing reasons that have
            // nothing to do with holding.
            let expected = if n_blockers >= N_WORKERS {
                0
            } else {
                2 * n_partial.saturating_sub(n_blockers)
            };
            assert_eq!(
                narrow_placed(n_partial, n_blockers),
                expected,
                "{n_partial} worker(s) with freed cpus, {n_blockers} blocker(s) queued"
            );
        }
    }
}

#[test]
fn test_schedule_blockers_accumulate_in_parallel() {
    /// Blockers started within one worker's drain; must be all of them.
    fn started_within_one_drain(n_blockers: usize) -> (usize, Vec<String>) {
        let mut rt = TestEnv::new();
        let ws = rt.new_workers(4, &WorkerBuilder::new(8));
        let mut running: Vec<Vec<TaskId>> = ws
            .iter()
            .map(|w| {
                (0..8)
                    .map(|_| rt.new_task_running(&TaskBuilder::new().cpus(1), *w))
                    .collect()
            })
            .collect();
        let blockers = rt.new_tasks(n_blockers, &TaskBuilder::new().cpus(8).user_priority(10));
        rt.new_tasks(60, &TaskBuilder::new().cpus(1));

        for _tick in 0..=8 {
            rt.schedule();
            if blockers.iter().all(|t| rt.task(*t).is_assigned()) {
                break;
            }
            for (idx, w) in ws.iter().enumerate() {
                if let Some(task_id) = running[idx].pop() {
                    rt.finish_task(task_id, *w);
                }
            }
        }

        let placed = blockers
            .iter()
            .filter(|t| rt.task(**t).is_assigned())
            .count();
        let free = ws
            .iter()
            .map(|w| {
                let a = rt.worker(*w).sn_assignment().unwrap();
                format!("w{w}: {:?}", a.free_resources.get(ResourceId::new(0)))
            })
            .collect();
        (placed, free)
    }

    for n_blockers in 1..=4 {
        let (placed, free) = started_within_one_drain(n_blockers);
        assert_eq!(
            placed,
            n_blockers,
            "only {placed} of {n_blockers} blockers started; workers drained one at a time ({})",
            free.join(", ")
        );
    }
}

#[test]
fn test_schedule_wide_workers_held_one_per_blocker() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(3, &WorkerBuilder::new(16));
    let mut running: Vec<Vec<TaskId>> = ws
        .iter()
        .map(|w| {
            (0..16)
                .map(|_| rt.new_task_running(&TaskBuilder::new().cpus(1), *w))
                .collect()
        })
        .collect();
    let blockers = rt.new_tasks(2, &TaskBuilder::new().cpus(8).user_priority(10));
    rt.new_tasks(60, &TaskBuilder::new().cpus(1));

    // Eight rounds is what a held worker needs to reach the 8 free cpus a blocker wants; both
    // blockers must be served by that point, not one after the other.
    for _tick in 0..=8 {
        rt.schedule();
        if blockers.iter().all(|t| rt.task(*t).is_assigned()) {
            return;
        }
        for (idx, w) in ws.iter().enumerate() {
            if let Some(task_id) = running[idx].pop() {
                rt.finish_task(task_id, *w);
            }
        }
    }

    let placed = blockers
        .iter()
        .filter(|t| rt.task(**t).is_assigned())
        .count();
    let free: Vec<String> = ws
        .iter()
        .map(|w| {
            let a = rt.worker(*w).sn_assignment().unwrap();
            format!("w{w}: {:?}", a.free_resources.get(ResourceId::new(0)))
        })
        .collect();
    panic!(
        "only {placed} of 2 blockers started; one wide worker was credited for both ({})",
        free.join(", ")
    );
}

#[test]
fn test_reservation_one_assigned() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_max: 0,
        ..Default::default()
    });

    rt.new_worker(&WorkerBuilder::new(3));
    let mut held = Vec::new();
    for _ in 0..4 {
        let w = rt.new_worker(&WorkerBuilder::new(3));
        rt.new_task_running(&TaskBuilder::new().cpus(1), w);
        held.push(w);
    }

    let blocker = rt.new_tasks(2, &TaskBuilder::new().cpus(3).user_priority(10));
    let filler = rt.new_tasks(8, &TaskBuilder::new().cpus(1).user_priority(5));

    rt.schedule();

    let blocker_scheduled = blocker
        .iter()
        .filter(|t| rt.task(**t).is_assigned())
        .count();
    assert_eq!(blocker_scheduled, 1);

    let filler_scheduled = held
        .iter()
        .map(|w| {
            filler
                .iter()
                .filter(|t| rt.worker_tasks(*w).contains(*t))
                .count()
        })
        .sum::<usize>();
    assert_eq!(filler_scheduled, 6);
}

#[test]
fn test_schedule_gap_tasks_do_not_move_the_hold() {
    // The blocker needs 6 cpus. `w0` runs one 4-cpu task: 4 cpus free, 2 of them a gap the blocker
    // can never use. `w1` runs one 5-cpu task: 3 cpus free, 2 of them a gap. `w0` is held and its
    // gap is filled with 2 narrow tasks. Then 3 narrow tasks finish on `w1`. By plain free cpus
    // `w1` (3) now looks better than `w0` (2), but the blocker can use only 1 of them, against 2 on
    // `w0`. The hold must stay on `w0`: filling a gap never makes a worker less useful to the
    // blocker, so it must not cost the worker its hold (and then its drained capacity).
    let mut rt = TestEnv::new();
    let w0 = rt.new_worker(&WorkerBuilder::new(8));
    let w1 = rt.new_worker(&WorkerBuilder::new(8));
    rt.new_task_running(&TaskBuilder::new().cpus(4), w0);
    rt.new_task_running(&TaskBuilder::new().cpus(5), w1);
    rt.new_task(&TaskBuilder::new().cpus(6).user_priority(10));
    let narrow = rt.new_tasks(40, &TaskBuilder::new().cpus(1).user_priority(5));
    rt.schedule();
    assert_eq!(narrow_per_worker(&rt, &narrow, &[w0, w1]), vec![2, 3]);

    let finished: Vec<_> = narrow
        .iter()
        .copied()
        .filter(|t| {
            matches!(rt.task(*t).state,
                TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w1)
        })
        .collect();
    for t in &finished {
        rt.finish_task(*t, w1);
    }
    let live: Vec<_> = narrow
        .iter()
        .copied()
        .filter(|t| !finished.contains(t))
        .collect();
    rt.schedule();
    assert_eq!(
        narrow_per_worker(&rt, &live, &[w0, w1]),
        vec![2, 3],
        "w0 keeps its hold; w1 is refilled"
    );
}

#[test]
fn test_schedule_blocker_count_allowance_is_not_multiplied_per_worker() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers(3, &WorkerBuilder::new(12));
    for w in &ws {
        rt.new_task_running(&TaskBuilder::new().cpus(5), *w);
    }
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(2));
    let above = rt.new_tasks(3, &TaskBuilder::new().cpus(1).user_priority(3));
    let below = rt.new_tasks(40, &TaskBuilder::new().cpus(1).user_priority(1));

    rt.schedule();

    let per_worker: Vec<usize> = narrow_per_worker(&rt, &above, &ws)
        .iter()
        .zip(narrow_per_worker(&rt, &below, &ws))
        .map(|(a, b)| a + b)
        .collect();
    assert!(
        per_worker.iter().any(|placed| *placed <= 4),
        "every worker took more than its 4-cpu gap ({per_worker:?}), \
         so the blocker cannot start anywhere once the 5-cpu tasks finish"
    );
}

#[test]
fn test_schedule_two_requests_share_one_gap_allowance() {
    let mut rt = TestEnv::new();
    let foo = rt.new_named_resource("foo");
    let w = rt.new_worker(&WorkerBuilder::new(12).res_sum("foo", 10));
    rt.new_task_running(&TaskBuilder::new().cpus(5), w);
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    let plain = rt.new_tasks(20, &TaskBuilder::new().cpus(1).user_priority(5));
    let with_foo = rt.new_tasks(
        20,
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(foo, 1)
            .user_priority(5),
    );

    rt.schedule();

    let plain_placed = narrow_per_worker(&rt, &plain, &[w])[0];
    let foo_placed = narrow_per_worker(&rt, &with_foo, &[w])[0];
    assert!(
        plain_placed + foo_placed <= 4,
        "worker runs {} lower-priority cpus ({plain_placed} plain + {foo_placed} with foo), \
         but the blocker leaves a gap of 4 there",
        plain_placed + foo_placed
    );
}

#[test]
fn test_schedule_soft_rejected_worker_is_still_limited_by_the_blocker() {
    let mut rt = TestEnv::new();
    let w = rt.new_worker(&WorkerBuilder::new(12));
    rt.new_task_running(&TaskBuilder::new().cpus(5), w);
    let blocker = rt.new_task(&TaskBuilder::new().cpus(8).user_priority(10));
    let narrow = rt.new_tasks(20, &TaskBuilder::new().cpus(1).user_priority(1));

    let blocker_rq = rt.task(blocker).resource_rq_id;
    rt.core()
        .get_worker_mut(w)
        .block_request(blocker_rq, ResourceVariantId::new(0));

    rt.schedule();

    assert_eq!(
        narrow_per_worker(&rt, &narrow, &[w]),
        vec![4],
        "a soft-rejected worker keeps its gap limit"
    );
}

/// The excess of a request over a worker's gap must never exceed what the request actually runs
/// there. The excess lowers the shared gap constraint, so excess that no task uses would buy other
/// requests room beyond the gap, paid from an allowance spent on nothing.
///
/// `w0` has 12 cpus with a 5-cpu task running: 7 free, and a gap of 4 for the 8-cpu blocker at
/// priority 5. The donor request (2-cpu tasks) has three tasks above the blocker, so it carries an
/// allowance of 3, and they are placed on `w1`, which is too small for the blocker. The donor
/// therefore runs nothing on `w0` while holding an allowance there. Its own tasks may use that
/// allowance and outrank the blocker, but the other two requests have no allowance at all: they
/// share `w0`'s gap and must stay within it together, 4 cpus rather than 4 each.
#[test]
fn test_schedule_unused_allowance_does_not_widen_the_shared_gap() {
    let mut rt = TestEnv::new();
    let foo = rt.new_named_resource("foo");
    let w0 = rt.new_worker(&WorkerBuilder::new(12).res_sum("foo", 10));
    let w1 = rt.new_worker(&WorkerBuilder::new(6));
    rt.new_task_running(&TaskBuilder::new().cpus(5), w0);
    rt.new_tasks(3, &TaskBuilder::new().cpus(2).user_priority(9));
    rt.new_tasks(10, &TaskBuilder::new().cpus(2).user_priority(1));
    rt.new_task(&TaskBuilder::new().cpus(8).user_priority(5));
    let b1 = rt.new_tasks(20, &TaskBuilder::new().cpus(1).user_priority(1));
    let b2 = rt.new_tasks(
        20,
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(foo, 1)
            .user_priority(1),
    );

    rt.schedule();

    let b1_cpus = narrow_per_worker(&rt, &b1, &[w0])[0];
    let b2_cpus = narrow_per_worker(&rt, &b2, &[w0])[0];
    assert!(
        b1_cpus + b2_cpus <= 4,
        "requests without an allowance use {} cpus of w0 ({b1_cpus} + {b2_cpus}), \
         but they share a gap of 4",
        b1_cpus + b2_cpus
    );
}

/// Gap filling is bounded by what *all* the blockers of a worker could use together, not by what
/// each of them could use alone. On a 12-cpu worker with a 5-cpu and a 7-cpu blocker the two
/// gaps are 2 and 5, but 5 + 7 fills the worker exactly, so there is nothing to fill.
#[test]
fn test_schedule_gap_is_bounded_by_the_mix_of_blockers() {
    let mut rt = TestEnv::new();
    rt.new_worker(&WorkerBuilder::new(12));
    let wide = rt.new_tasks(2, &TaskBuilder::new().cpus(5).user_priority(10));
    let wider = rt.new_tasks(2, &TaskBuilder::new().cpus(7).user_priority(10));
    let narrow = rt.new_tasks(8, &TaskBuilder::new().cpus(1).user_priority(0));
    rt.schedule();
    let placed = |ts: &[TaskId]| ts.iter().filter(|t| rt.task(**t).is_assigned()).count();
    assert_eq!(placed(&wide), 1);
    assert_eq!(placed(&wider), 1);
    assert_eq!(
        placed(&narrow),
        0,
        "the blockers pack into the worker exactly, so no narrow task may take a cpu"
    );
}

/// The same worker with one cpu more does leave a gap of one, and exactly one narrow task fits.
#[test]
fn test_schedule_mix_of_blockers_can_still_leave_a_gap() {
    let mut rt = TestEnv::new();
    rt.new_worker(&WorkerBuilder::new(13));
    let wide = rt.new_tasks(2, &TaskBuilder::new().cpus(5).user_priority(10));
    let wider = rt.new_tasks(2, &TaskBuilder::new().cpus(7).user_priority(10));
    let narrow = rt.new_tasks(8, &TaskBuilder::new().cpus(1).user_priority(0));
    rt.schedule();
    let placed = |ts: &[TaskId]| ts.iter().filter(|t| rt.task(**t).is_assigned()).count();
    assert_eq!(placed(&wide), 1);
    assert_eq!(placed(&wider), 1);
    assert_eq!(placed(&narrow), 1);
}

/// A single blocker keeps its own gap: the joint bound only exists for a mix.
#[test]
fn test_schedule_single_blocker_keeps_its_gap() {
    let mut rt = TestEnv::new();
    rt.new_worker(&WorkerBuilder::new(12));
    let wider = rt.new_tasks(3, &TaskBuilder::new().cpus(7).user_priority(10));
    let narrow = rt.new_tasks(8, &TaskBuilder::new().cpus(1).user_priority(0));
    rt.schedule();
    let placed = |ts: &[TaskId]| ts.iter().filter(|t| rt.task(**t).is_assigned()).count();
    assert_eq!(placed(&wider), 1);
    assert_eq!(
        placed(&narrow),
        5,
        "12 - 7 = 5 cpus no 7-cpu task can ever use"
    );
}
