#![cfg(test)]

use crate::TaskId;
use crate::internal::common::Set;
use crate::internal::common::index::ItemId;
use crate::internal::messages::worker::{StealResponse, StealResponseMsg, ToWorkerMessage};
use crate::internal::server::core::Core;
use crate::internal::server::reactor::on_steal_response;
use crate::internal::server::task::Task;
use crate::internal::tests::utils::env::{TestEnv, create_test_comm};
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_workers, finish_on_worker, submit_test_tasks,
};
use crate::internal::tests::utils::task::TaskBuilder;
use crate::internal::tests::utils::task::task;
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceUnits};
use std::time::Duration;

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

    assert_eq!(m1.len() + m2.len() + m3.len(), 4);
    assert!(
        (m1.len() == 4 && m2.is_empty() && m3.is_empty())
            || (m1.is_empty() && m2.len() == 4 && m3.is_empty())
            || (m1.is_empty() && m2.is_empty() && m3.len() == 4)
    );
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

    let m1 = comm.take_worker_msgs(100, 0);
    let m2 = comm.take_worker_msgs(101, 0);
    let m3 = comm.take_worker_msgs(102, 0);
    comm.emptiness_check();
    core.sanity_check();

    assert_eq!(m1.len(), 50);
    assert_eq!(m2.len(), 50);
    assert_eq!(m3.len(), 50);
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

    let m1 = comm.take_worker_msgs(100, 0);
    let m2 = comm.take_worker_msgs(101, 0);
    let m3 = comm.take_worker_msgs(102, 0);
    comm.emptiness_check();
    core.sanity_check();

    assert_eq!(m1.len() + m2.len() + m3.len(), 300);
    assert!(m1.len() >= 29);
    assert!(m2.len() >= 29);
    assert!(m3.len() >= 29);

    for w in core.get_workers() {
        assert!(!w.is_underloaded());
    }

    let mut finish_all = |core: &mut Core, msgs, worker_id| {
        for m in msgs {
            match m {
                ToWorkerMessage::ComputeTask(cm) => {
                    assert!(active_ids.remove(&cm.id));
                    finish_on_worker(core, cm.id, worker_id);
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
    rt.check_worker_load_lower_bounds(&[u(12), u(12), u(12)]);
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
        println!("Checking order {:?}", task_cpu_counts);

        let mut rt = TestEnv::new();

        let unschedulable_index = task_cpu_counts
            .iter()
            .position(|&count| count > 10)
            .unwrap() as <TaskId as ItemId>::IdType
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
        assert_eq!(sn[0], TaskId::new(unschedulable_index));
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
            .all(|task_id| task_id.as_num() < 50)
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
