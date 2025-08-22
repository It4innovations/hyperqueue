use crate::TaskId;
use crate::internal::common::Set;
use crate::internal::messages::worker::{
    StealResponse, StealResponseMsg, TaskOutput, ToWorkerMessage,
};
use crate::internal::server::core::Core;
use crate::internal::server::reactor::on_steal_response;
use crate::internal::server::task::Task;
use crate::internal::tests::utils::env::{TestEnv, create_test_comm};
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_worker, create_test_workers, finish_on_worker,
    start_and_finish_on_worker_with_data, submit_test_tasks,
};
use crate::internal::tests::utils::task::TaskBuilder;
use crate::internal::tests::utils::task::task;
use crate::internal::tests::utils::workflows::submit_example_4;
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceUnits};
use std::time::Duration;

fn task_count(msg: &ToWorkerMessage) -> usize {
    match msg {
        ToWorkerMessage::ComputeTasks(cm) => cm.tasks.len(),
        _ => 0,
    }
}

#[test]
fn test_no_deps_scattering_1() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[5, 5, 5]);

    let tasks: Vec<Task> = (1..=4).map(task).collect();
    submit_test_tasks(&mut core, tasks);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling_without_balancing(&mut core, &mut comm);

    let m1 = comm.take_worker_msgs(100, 0);
    let m2 = comm.take_worker_msgs(101, 0);
    let m3 = comm.take_worker_msgs(102, 0);
    comm.emptiness_check();
    core.sanity_check();

    let c1 = if m1.len() > 0 { task_count(&m1[0]) } else { 0 };
    let c2 = if m2.len() > 0 { task_count(&m2[0]) } else { 0 };
    let c3 = if m3.len() > 0 { task_count(&m3[0]) } else { 0 };

    assert_eq!(c1 + c2 + c3, 4);
    assert!(c1 == 4 || c2 == 4 && c3 == 4);
}

#[test]
fn test_no_deps_scattering_2() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[5, 5, 5]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();

    let mut submit_and_check = |id, expected| {
        let t = task(id);
        submit_test_tasks(&mut core, vec![t]);
        scheduler.run_scheduling_without_balancing(&mut core, &mut comm);
        let mut counts: Vec<_> = core.get_workers().map(|w| w.sn_tasks().len()).collect();
        counts.sort();
        assert_eq!(counts, expected);
    };

    for i in 1..=5 {
        submit_and_check(i, vec![0, 0, i as usize])
    }

    for i in 1..=5 {
        submit_and_check(i + 100, vec![0, i as usize, 5]);
    }

    for i in 1..=5 {
        submit_and_check(i + 200, vec![i as usize, 5, 5]);
    }

    submit_and_check(300, vec![5, 5, 6]);
    submit_and_check(301, vec![5, 6, 6]);
    submit_and_check(302, vec![6, 6, 6]);
    submit_and_check(303, vec![6, 6, 7]);
    submit_and_check(304, vec![6, 7, 7]);
}

#[test]
fn test_no_deps_distribute_without_balance() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[10, 10, 10]);

    let tasks: Vec<Task> = (1..=150).map(task).collect();
    submit_test_tasks(&mut core, tasks);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    let need_balance = scheduler.run_scheduling_without_balancing(&mut core, &mut comm);

    let m1 = comm.take_worker_msgs(100, 1);
    let m2 = comm.take_worker_msgs(101, 1);
    let m3 = comm.take_worker_msgs(102, 1);
    comm.emptiness_check();
    core.sanity_check();

    assert_eq!(task_count(&m1[0]), 50);
    assert_eq!(task_count(&m2[0]), 50);
    assert_eq!(task_count(&m3[0]), 50);
    assert!(!need_balance);
}

#[test]
fn test_no_deps_distribute_with_balance() {
    //setup_logging();
    let mut core = Core::default();
    create_test_workers(&mut core, &[2, 2, 2]);

    assert_eq!(core.get_worker_map().len(), 3);
    for w in core.get_workers() {
        assert!(w.is_underloaded());
    }

    let mut active_ids: Set<TaskId> = (1..301).map(|id| id.into()).collect();
    let tasks: Vec<Task> = (1..301).map(task).collect();
    submit_test_tasks(&mut core, tasks);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    let m1 = comm.take_worker_msgs(100, 1);
    let m2 = comm.take_worker_msgs(101, 1);
    let m3 = comm.take_worker_msgs(102, 1);
    comm.emptiness_check();
    core.sanity_check();

    assert!(matches!(&m1[0], ToWorkerMessage::ComputeTasks(msg) if msg.tasks.len() >= 29));
    assert!(matches!(&m2[0], ToWorkerMessage::ComputeTasks(msg) if msg.tasks.len() >= 29));
    assert!(matches!(&m3[0], ToWorkerMessage::ComputeTasks(msg) if msg.tasks.len() >= 29));

    for w in core.get_workers() {
        assert!(!w.is_underloaded());
    }

    let mut finish_all = |core: &mut Core, msgs, worker_id| {
        for m in msgs {
            match m {
                ToWorkerMessage::ComputeTasks(cm) => {
                    for task in cm.tasks {
                        assert!(active_ids.remove(&task.id));
                        finish_on_worker(core, task.id, worker_id);
                    }
                }
                _ => unreachable!(),
            };
        }
    };

    finish_all(&mut core, m1, 100);
    finish_all(&mut core, m3, 102);

    core.assert_underloaded(&[100, 102]);
    core.assert_not_underloaded(&[101]);

    scheduler.run_scheduling(&mut core, &mut comm);

    core.assert_not_underloaded(&[100, 101, 102]);

    // TODO: Finish stealing

    let x1 = comm.take_worker_msgs(101, 1);

    let stealing = match &x1[0] {
        ToWorkerMessage::StealTasks(tasks) => tasks.ids.clone(),
        _ => {
            unreachable!()
        }
    };

    comm.emptiness_check();
    core.sanity_check();

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: stealing.iter().map(|t| (*t, StealResponse::Ok)).collect(),
        },
    );

    let n1 = comm.take_worker_msgs(100, 0);
    let n3 = comm.take_worker_msgs(102, 0);

    assert!(n1.len() > 5);
    assert!(n3.len() > 5);
    assert_eq!(n1.len() + n3.len(), stealing.len());

    core.assert_not_underloaded(&[100, 101, 102]);

    comm.emptiness_check();
    core.sanity_check();

    finish_all(&mut core, n1, 100);
    finish_all(&mut core, n3, 102);
    assert_eq!(
        active_ids.len(),
        core.get_worker_by_id_or_panic(101.into()).sn_tasks().len()
    );

    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_resource_balancing1() {
    let mut rt = TestEnv::new();
    rt.new_workers(&[4, 6]);

    // 10 - 3, 11 - 4, 12 - 2
    rt.new_assigned_tasks_cpus(&[&[3, 4, 2]]);
    rt.balance();
    rt.check_worker_tasks(100, &[10]);
    rt.check_worker_tasks(101, &[11, 12]);

    let mut rt = TestEnv::new();
    rt.new_workers(&[6, 3]);
    rt.new_assigned_tasks_cpus(&[&[3, 4, 2]]);
    rt.balance();
    rt.check_worker_tasks(100, &[11, 12]);
    rt.check_worker_tasks(101, &[10]);
}

#[test]
fn test_resource_balancing2() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
    ]]);
    rt.balance();
    assert_eq!(rt.worker_load(100).get(0.into()), u(12));
    assert_eq!(rt.worker_load(101).get(0.into()), u(12));
    assert_eq!(rt.worker_load(102).get(0.into()), u(12));
}

#[test]
fn test_resource_balancing3() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[
        &[
            4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
        ],
        &[1, 1, 1, 1], // 4
        &[6],          // 6
    ]);
    rt.balance();
    assert!(rt.worker_load(100).get(0.into()) >= u(12));
    assert!(rt.worker_load(101).get(0.into()) >= u(12));
    assert!(rt.worker_load(102).get(0.into()) >= u(12));
}

#[test]
fn test_resource_balancing4() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        2, 4, 2, 4, /* 12 */ 4, 4, 4, /* 12 */ 2, 2, 2, 2, 4, /* 12 */
    ]]);
    rt.balance();
    assert_eq!(rt.worker_load(100).get(0.into()), u(12));
    assert_eq!(rt.worker_load(101).get(0.into()), u(12));
    assert_eq!(rt.worker_load(102).get(0.into()), u(12));
}

#[test]
fn test_resource_balancing5() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        2, 4, 2, 4, 2, /* 14 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
    ]]);
    rt.balance();
    rt.check_worker_load_lower_bounds(&[u(10), u(12), u(12)]);
}

#[test]
fn test_resource_balancing6() {
    let _ = env_logger::builder().is_test(true).try_init();
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12, 12, 12]);
    rt.new_assigned_tasks_cpus(&[
        &[2, 2, 4],
        &[4, 4, 4, 4],
        &[4, 4, 4],
        &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 /* 12 */],
        &[12],
    ]);
    rt.balance();
    rt.check_worker_load_lower_bounds(&[u(12), u(12), u(12), u(12), u(12)]);
}

#[test]
fn test_resource_balancing7() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();

    rt.new_workers(&[12, 12, 12]);

    rt.new_task_running(TaskBuilder::new(10).cpus_compact(2), 100);
    rt.new_task_running(TaskBuilder::new(11).cpus_compact(2), 100);
    rt.new_task_running(TaskBuilder::new(12).cpus_compact(4), 100);

    rt.new_task_running(TaskBuilder::new(20).cpus_compact(4), 101);
    rt.new_task_assigned(TaskBuilder::new(21).cpus_compact(4), 101); // <- only assigned
    rt.new_task_running(TaskBuilder::new(22).cpus_compact(4), 101);
    rt.new_task_running(TaskBuilder::new(23).cpus_compact(4), 101);

    rt.new_task_running(TaskBuilder::new(30).cpus_compact(4), 102);
    rt.new_task_running(TaskBuilder::new(31).cpus_compact(4), 102);
    rt.new_task_running(TaskBuilder::new(33).cpus_compact(4), 102);

    rt.schedule();
    rt.check_worker_load_lower_bounds(&[u(12), u(12), u(12)]);
}

#[test]
fn test_resources_blocked_workers() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[4, 8, 2]);

    rt.new_assigned_tasks_cpus(&[&[4, 4, 4, 4, 4]]);
    rt.balance();
    assert!(rt.worker_load(100).get(0.into()) >= u(4));
    assert!(rt.worker_load(101).get(0.into()) >= u(8));
    assert_eq!(rt.worker_load(102).get(0.into()), u(0));

    assert!(!rt.worker(100).is_parked());
    assert!(!rt.worker(101).is_parked());
    assert!(rt.worker(102).is_parked());
    rt.core().sanity_check();

    rt.new_ready_tasks_cpus(&[3]);
    rt.schedule();

    assert!(!rt.worker(100).is_parked());
    assert!(!rt.worker(101).is_parked());
    assert!(rt.worker(102).is_parked());

    rt.new_ready_tasks_cpus(&[1]);
    rt.schedule();

    assert!(!rt.worker(100).is_parked());
    assert!(!rt.worker(101).is_parked());
    assert!(!rt.worker(102).is_parked());

    rt.balance();

    assert!(!rt.worker(100).is_parked());
    assert!(!rt.worker(101).is_parked());
    assert!(!rt.worker(102).is_parked());
}

#[test]
fn test_resources_no_workers1() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers(&[4, 8, 2]);

    rt.new_ready_tasks_cpus(&[8, 8, 16, 24]);
    rt.schedule();
    assert_eq!(rt.worker_load(100).get(0.into()), u(0));
    assert_eq!(rt.worker_load(101).get(0.into()), u(16));
    assert_eq!(rt.worker_load(102).get(0.into()), u(0));

    let sn = rt.core().take_sleeping_tasks();
    assert_eq!(sn.len(), 2);
}

#[test]
fn test_resources_no_workers2() {
    fn check(task_cpu_counts: &[ResourceUnits]) {
        println!("Checking order {task_cpu_counts:?}");

        let mut rt = TestEnv::new();

        let unschedulable_index = task_cpu_counts
            .iter()
            .position(|&count| count > 10)
            .unwrap() as u32
            + rt.task_id_counter;

        rt.new_workers(&[8, 8, 8]);

        rt.new_ready_tasks_cpus(task_cpu_counts);
        rt.schedule();
        assert!(rt.worker_load(100).get(0.into()).is_zero());
        assert!(rt.worker_load(101).get(0.into()).is_zero());
        assert!(rt.worker_load(102).get(0.into()).is_zero());

        rt.new_workers(&[9, 10]);
        rt.schedule();
        assert!(rt.worker_load(100).get(0.into()).is_zero());
        assert!(rt.worker_load(101).get(0.into()).is_zero());
        assert!(rt.worker_load(102).get(0.into()).is_zero());
        assert_eq!(rt.worker(103).sn_tasks().len(), 1);
        assert_eq!(rt.worker(104).sn_tasks().len(), 1);

        let sn = rt.core().take_sleeping_tasks();
        assert_eq!(sn.len(), 1);
        assert_eq!(sn[0], TaskId::new_test(unschedulable_index));
    }

    check(&[9, 10, 11]);
    check(&[9, 11, 10]);
    check(&[10, 9, 11]);
    check(&[10, 11, 9]);
    check(&[11, 9, 10]);
    check(&[11, 10, 9]);
}

#[test]
fn test_resource_time_assign() {
    let mut rt = TestEnv::new();

    rt.new_workers_ext(&[(1, Some(Duration::new(100, 0)), Vec::new())]);

    rt.new_task(TaskBuilder::new(10).time_request(170));
    rt.new_task(TaskBuilder::new(11));
    rt.new_task(TaskBuilder::new(12).time_request(99));

    rt.schedule();
    rt.finish_scheduling();
    rt.check_worker_tasks(100, &[11, 12]);
}

#[test]
fn test_resource_time_balance1() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut rt = TestEnv::new();

    rt.new_workers_ext(&[
        (1, Some(Duration::new(50, 0)), Vec::new()),
        (1, Some(Duration::new(200, 0)), Vec::new()),
        (1, Some(Duration::new(100, 0)), Vec::new()),
    ]);

    rt.new_task(TaskBuilder::new(10).time_request(170));
    rt.new_task(TaskBuilder::new(11));
    rt.new_task(TaskBuilder::new(12).time_request(99));

    rt.test_assign(10, 101);
    rt.test_assign(11, 101);
    rt.test_assign(12, 102);

    //rt.schedule();
    rt.balance();
    rt.check_worker_tasks(100, &[11]);
    rt.check_worker_tasks(101, &[10]);
    rt.check_worker_tasks(102, &[12]);
}

#[test]
fn test_generic_resource_assign2() {
    //let _ = env_logger::init();
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    rt.new_workers_ext(&[
        // Worker 100
        (10, None, vec![ResourceDescriptorItem::range("Res0", 1, 10)]),
        // Worker 101
        (10, None, vec![]),
        // Worker 102
        (
            10,
            None,
            vec![
                ResourceDescriptorItem::range("Res0", 1, 10),
                ResourceDescriptorItem::sum("Res1", 1_000_000),
            ],
        ),
    ]);
    for i in 0..50 {
        rt.new_task(TaskBuilder::new(i).add_resource(1, 1));
    }
    for i in 50..100 {
        rt.new_task(TaskBuilder::new(i).add_resource(2, 2));
    }
    rt.schedule();

    assert_eq!(
        rt.core()
            .get_worker_by_id(101.into())
            .unwrap()
            .sn_tasks()
            .len(),
        0
    );
    assert!(
        rt.core()
            .get_worker_by_id(100.into())
            .unwrap()
            .sn_tasks()
            .len()
            > 10
    );
    assert!(
        rt.core()
            .get_worker_by_id(102.into())
            .unwrap()
            .sn_tasks()
            .len()
            > 10
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id(100.into())
            .unwrap()
            .sn_tasks()
            .len()
            + rt.core()
                .get_worker_by_id(102.into())
                .unwrap()
                .sn_tasks()
                .len(),
        100
    );
    assert!(
        rt.core()
            .get_worker_by_id(100.into())
            .unwrap()
            .sn_tasks()
            .iter()
            .all(|task_id| task_id.job_task_id().as_num() < 50)
    );

    assert!(!rt.worker(100).is_parked());
    assert!(rt.worker(101).is_parked());
    assert!(!rt.worker(102).is_parked());
}

#[test]
fn test_generic_resource_balance1() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    rt.new_workers_ext(&[
        // Worker 100
        (10, None, vec![ResourceDescriptorItem::range("Res0", 1, 10)]),
        // Worker 101
        (10, None, vec![]),
        // Worker 102
        (
            10,
            None,
            vec![
                ResourceDescriptorItem::range("Res0", 1, 10),
                ResourceDescriptorItem::sum("Res1", 1_000_000),
            ],
        ),
    ]);

    rt.new_task(TaskBuilder::new(1).cpus_compact(1).add_resource(1, 5));
    rt.new_task(TaskBuilder::new(2).cpus_compact(1).add_resource(1, 5));
    rt.new_task(TaskBuilder::new(3).cpus_compact(1).add_resource(1, 5));
    rt.new_task(TaskBuilder::new(4).cpus_compact(1).add_resource(1, 5));
    rt.schedule();

    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(100.into())
            .sn_tasks()
            .len(),
        2
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(101.into())
            .sn_tasks()
            .len(),
        0
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(102.into())
            .sn_tasks()
            .len(),
        2
    );
}

#[test]
fn test_generic_resource_balance2() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    rt.new_workers_ext(&[
        // Worker 100
        (10, None, vec![ResourceDescriptorItem::range("Res0", 1, 10)]),
        // Worker 101
        (10, None, vec![]),
        // Worker 102
        (
            10,
            None,
            vec![
                ResourceDescriptorItem::range("Res0", 1, 10),
                ResourceDescriptorItem::sum("Res1", 1_000_000),
            ],
        ),
    ]);

    rt.new_task(TaskBuilder::new(1).cpus_compact(1).add_resource(1, 5));
    rt.new_task(
        TaskBuilder::new(2)
            .cpus_compact(1)
            .add_resource(1, 5)
            .add_resource(2, 500_000),
    );
    rt.new_task(TaskBuilder::new(3).cpus_compact(1).add_resource(1, 5));
    rt.new_task(
        TaskBuilder::new(4)
            .cpus_compact(1)
            .add_resource(1, 5)
            .add_resource(2, 500_000),
    );
    rt.schedule();

    /*dbg!(rt
    .core()
    .get_worker_by_id_or_panic(102.into())
    .sn_tasks()
    .len());*/

    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(100.into())
            .sn_tasks()
            .len(),
        2
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(101.into())
            .sn_tasks()
            .len(),
        0
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(102.into())
            .sn_tasks()
            .len(),
        2
    );
}

#[test]
fn test_generic_resource_balancing3() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    rt.new_workers_ext(&[
        // Worker 100
        (2, None, vec![]),
        // Worker 101
        (2, None, vec![ResourceDescriptorItem::range("Res0", 1, 1)]),
    ]);

    for i in 1..=80 {
        rt.new_task(TaskBuilder::new(i).cpus_compact(1));
    }
    for i in 81..=100 {
        rt.new_task(TaskBuilder::new(i).cpus_compact(1).add_resource(1, 1));
    }

    rt.schedule();

    assert!(
        rt.core()
            .get_worker_by_id_or_panic(100.into())
            .sn_tasks()
            .len()
            >= 40
    );
    assert!(
        rt.core()
            .get_worker_by_id_or_panic(101.into())
            .sn_tasks()
            .len()
            >= 40
    );
}

#[test]
fn test_generic_resource_variants1() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    rt.new_workers_ext(&[
        // Worker 100
        (4, None, vec![]),
        // Worker 101
        (4, None, vec![ResourceDescriptorItem::range("Res0", 1, 2)]),
    ]);

    for i in 1..=4 {
        let task = TaskBuilder::new(i)
            .cpus_compact(2)
            .next_resources()
            .cpus_compact(1)
            .add_resource(1, 1);
        rt.new_task(task);
    }
    rt.schedule();

    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(100.into())
            .sn_tasks()
            .len(),
        2
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(101.into())
            .sn_tasks()
            .len(),
        2
    );
}

#[test]
fn test_generic_resource_variants2() {
    let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    rt.new_workers_ext(&[
        // Worker 100
        (4, None, vec![]),
        // Worker 101
        (4, None, vec![ResourceDescriptorItem::range("Res0", 1, 2)]),
    ]);

    for i in 1..=4 {
        let task = TaskBuilder::new(i)
            .cpus_compact(8)
            .next_resources()
            .cpus_compact(2)
            .add_resource(1, 1);
        rt.new_task(task);
    }
    rt.schedule();

    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(100.into())
            .sn_tasks()
            .len(),
        0
    );
    assert_eq!(
        rt.core()
            .get_worker_by_id_or_panic(101.into())
            .sn_tasks()
            .len(),
        4
    );
}

#[test]
fn test_task_data_deps_initial_placing() {
    let test_data = vec![
        (100, 100, 100_000, 100_000, 100_000, 100),
        (100, 101, 100_000, 100_000, 100_000, 101),
        (100, 101, 201_000, 100_000, 100_000, 100),
        (100, 101, 100_000, 40_000, 80_000, 101),
        (100, 101, 100_000, 40_000, 40_000, 100),
    ];
    let mut test_data2: Vec<(u32, u32, u64, u64, u64, u32)> = Vec::new();
    let inverse = |w| if w == 100 { 101 } else { 100 };
    for row in test_data {
        test_data2.push(row);
        test_data2.push((
            inverse(row.0),
            inverse(row.1),
            row.2,
            row.3,
            row.4,
            inverse(row.5),
        ));
    }

    for (worker1, worker2, size1, size2a, size2b, target_worker) in &test_data2 {
        let mut core = Core::default();
        submit_example_4(&mut core);
        create_test_workers(&mut core, &[1, 1]);
        start_and_finish_on_worker_with_data(
            &mut core,
            1,
            *worker1,
            vec![TaskOutput {
                id: 0.into(),
                size: *size1,
            }],
        );
        start_and_finish_on_worker_with_data(
            &mut core,
            2,
            *worker2,
            vec![
                TaskOutput {
                    id: 0.into(),
                    size: *size2a,
                },
                TaskOutput {
                    id: 1.into(),
                    size: *size2b,
                },
            ],
        );
        core.assert_ready(&[3]);
        let mut comm = create_test_comm();
        let mut scheduler = create_test_scheduler();
        scheduler.run_scheduling(&mut core, &mut comm);
        assert_eq!(
            core.get_task(3.into()).get_assigned_worker(),
            Some((*target_worker).into())
        );
        //comm.emptiness_check();
        core.sanity_check();
    }
}

#[test]
fn test_task_data_deps_balancing() {
    let _ = env_logger::builder().is_test(true).try_init();
    for odd in [0u32, 1u32] {
        for late_worker in [true, false] {
            let mut core = Core::default();
            let t1 = TaskBuilder::new(1).build();
            let t2 = TaskBuilder::new(2).build();
            let mut ts: Vec<_> = (10u32..110u32)
                .map(|i| {
                    TaskBuilder::new(TaskId::new_test(i))
                        .data_dep(&t1, i - 10)
                        .data_dep(&t2, i - 10)
                        .build()
                })
                .collect();
            ts.insert(0, t1);
            ts.insert(0, t2);
            submit_test_tasks(&mut core, ts);
            if late_worker {
                create_test_workers(&mut core, &[1]);
            } else {
                create_test_workers(&mut core, &[1, 1]);
            }
            let mut set_data = |task_id: u32, worker_id: u32| {
                start_and_finish_on_worker_with_data(
                    &mut core,
                    task_id,
                    worker_id,
                    (0u32..100u32)
                        .map(|i| TaskOutput {
                            id: i.into(),
                            size: if (i % 2) == odd { 100 } else { 5_000 },
                        })
                        .collect(),
                )
            };
            set_data(1, 100);
            set_data(2, 100);

            let ids: Vec<_> = (10..110).collect();
            core.assert_ready(&ids);
            if late_worker {
                create_test_worker(&mut core, 101.into(), 1);
            }
            let mut comm = create_test_comm();
            let mut scheduler = create_test_scheduler();
            scheduler.run_scheduling(&mut core, &mut comm);

            let worker = &core.get_worker_by_id(101.into()).unwrap();
            let n1_count = worker
                .sn_tasks()
                .iter()
                .map(|task_id| {
                    if task_id.job_task_id().as_num() % 2 == odd {
                        1
                    } else {
                        0
                    }
                })
                .sum::<u32>();
            assert!(n1_count > 40);
        }
    }
}
