use crate::internal::scheduler2::{PriorityCut, create_task_batches};
use crate::internal::tests::utils::scheduler::TestCase;
use crate::resources::ResourceRqId;
use crate::tests::utils::env::TestEnv;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;
use psutil::process::Status::Dead;

#[test]
fn test_task_grouping_basic() {
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);
    let now = std::time::Instant::now();
    let a = create_task_batches(rt.core(), now);
    assert!(a.is_empty());

    let t1 = rt.new_task(&TaskBuilder::new().user_priority(123));
    let a = create_task_batches(rt.core(), now);
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

    let a = create_task_batches(rt.core(), now);
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

    let a = create_task_batches(rt.core(), now);
    assert_eq!(a.len(), 2);
    let task1 = rt.task(t1);
    let task6 = rt.task(t6);
    assert_eq!(a[0].resource_rq_id, task1.resource_rq_id);
    assert_eq!(a[0].size, 5);
    assert!(!a[0].limit_reached);
    assert_eq!(
        a[0].cuts,
        vec![PriorityCut {
            size: 2,
            blockers: vec![(ResourceRqId::new(1), Some(3))],
        }]
    );
    assert_eq!(a[1].resource_rq_id, task6.resource_rq_id);
    assert_eq!(a[1].size, 3);
    assert!(!a[1].limit_reached);
    assert_eq!(a[1].cuts, vec![]);
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
    let a = create_task_batches(rt.core(), now);
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].size, 3);
    assert!(a[0].limit_reached);
    assert!(a[0].cuts.is_empty());

    let t10 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(5));
    let t11 = rt.new_task(&TaskBuilder::new().cpus(1).user_priority(0));

    let a = create_task_batches(rt.core(), now);
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
    let a = create_task_batches(rt.core(), now);
    dbg!(&a);
    assert_eq!(a.len(), 2);
    assert!(a[0].cuts.is_empty());
    assert!(a[1].cuts.is_empty());
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
    c.w(&w4).expect_tasks(&[ts[0]]);
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

// TODO: Nemozny scheduling neblokuje
// TODO: Vice zdroju
// TODO: Vice variant
// TODO: Rezervace
// TODO: worker.expect_cpus()

#[test]
fn test_schedule_gap_filling() {
    let w6 = WorkerBuilder::new(6);
    let w12 = WorkerBuilder::new(12);

    /*    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 8), (1, 8), (0, 4)]);
    c.w(&w12).expect_tasks(&[ts[0], ts[2]]);
    c.check();

    let mut c = TestCase::new();
    let ts = c.pc_tasks(&[(1, 3), (1, 3), (1, 3), (0, 2)]);
    c.w(&w6).expect_tasks(&[ts[0], ts[2]]);
    c.check();*/
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
