#![cfg(test)]

use crate::common::resources::GenericResourceDescriptor;
use crate::common::Set;
use crate::messages::worker::{StealResponse, StealResponseMsg, ToWorkerMessage};
use crate::scheduler::state::tests::create_test_scheduler;
use crate::server::core::Core;
use crate::server::reactor::on_steal_response;
use crate::server::task::TaskRef;
use crate::server::test_util::{
    create_test_comm, create_test_workers, finish_on_worker, start_and_finish_on_worker,
    submit_example_1, submit_test_tasks, task, TaskBuilder, TestEnv,
};
use crate::TaskId;
use std::time::Duration;

#[test]
fn test_no_deps_distribute() {
    //setup_logging();
    let mut core = Core::default();
    create_test_workers(&mut core, &[2, 2, 2]);

    assert_eq!(core.get_worker_map().len(), 3);
    for w in core.get_workers() {
        assert!(w.is_underloaded());
    }

    let mut active_ids: Set<TaskId> = (1..301).collect();
    let tasks: Vec<TaskRef> = (1..301).map(|i| task(i)).collect();
    let task_refs: Vec<&TaskRef> = tasks.iter().collect();
    submit_test_tasks(&mut core, &task_refs);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    let m1 = comm.take_worker_msgs(100, 0);
    let m2 = comm.take_worker_msgs(101, 0);
    let m3 = comm.take_worker_msgs(102, 0);
    comm.emptiness_check();
    core.sanity_check();

    assert_eq!(m1.len() + m2.len() + m3.len(), 300);
    assert!(m1.len() >= 10);
    assert!(m2.len() >= 10);
    assert!(m3.len() >= 10);

    for w in core.get_workers() {
        assert!(!w.is_underloaded());
    }

    let mut finish_all = |core: &mut Core, msgs, worker_id| {
        for m in msgs {
            match m {
                ToWorkerMessage::ComputeTask(cm) => {
                    assert!(active_ids.remove(&cm.id));
                    finish_on_worker(core, cm.id, worker_id, 1000);
                }
                _ => unreachable!(),
            };
        }
    };

    finish_all(&mut core, m1, 100);
    finish_all(&mut core, m3, 102);

    assert!(core.get_worker_by_id_or_panic(100).is_underloaded());
    assert!(!core.get_worker_by_id_or_panic(101).is_underloaded());
    assert!(core.get_worker_by_id_or_panic(102).is_underloaded());

    scheduler.run_scheduling(&mut core, &mut comm);

    assert!(!core.get_worker_by_id_or_panic(100).is_underloaded());
    assert!(!core.get_worker_by_id_or_panic(101).is_underloaded());
    assert!(!core.get_worker_by_id_or_panic(102).is_underloaded());

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
        101,
        StealResponseMsg {
            responses: stealing.iter().map(|t| (*t, StealResponse::Ok)).collect(),
        },
    );

    let n1 = comm.take_worker_msgs(100, 0);
    let n3 = comm.take_worker_msgs(102, 0);

    assert!(n1.len() > 5);
    assert!(n3.len() > 5);
    assert_eq!(n1.len() + n3.len(), stealing.len());

    assert!(!core.get_worker_by_id_or_panic(100).is_underloaded());
    assert!(!core.get_worker_by_id_or_panic(101).is_underloaded());
    assert!(!core.get_worker_by_id_or_panic(102).is_underloaded());

    comm.emptiness_check();
    core.sanity_check();

    finish_all(&mut core, n1, 100);
    finish_all(&mut core, n3, 102);
    assert_eq!(
        active_ids.len(),
        core.get_worker_by_id_or_panic(101).tasks().len()
    );

    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_minimal_transfer_no_balance1() {
    /*11  12
      \  / \
      13   14

      11 - is big on W100
      12 - is small on W101
    */

    let mut core = Core::default();
    create_test_workers(&mut core, &[2, 2, 2]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 10000);
    start_and_finish_on_worker(&mut core, 12, 101, 1000);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    comm.take_worker_msgs(100, 1);
    comm.take_worker_msgs(101, 1);

    assert_eq!(
        core.get_task_by_id_or_panic(13)
            .get()
            .get_assigned_worker()
            .unwrap(),
        100
    );
    assert_eq!(
        core.get_task_by_id_or_panic(14)
            .get()
            .get_assigned_worker()
            .unwrap(),
        101
    );

    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_minimal_transfer_no_balance2() {
    /*11  12
      \  / \
      13   14

      11 - is small on W100
      12 - is big on W102
    */
    //setup_logging();
    let mut core = Core::default();
    create_test_workers(&mut core, &[2, 2, 2]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 1000);
    start_and_finish_on_worker(&mut core, 12, 101, 10000);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    comm.take_worker_msgs(101, 2);

    assert_eq!(
        core.get_task_by_id_or_panic(13)
            .get()
            .get_assigned_worker()
            .unwrap(),
        101
    );
    assert_eq!(
        core.get_task_by_id_or_panic(14)
            .get()
            .get_assigned_worker()
            .unwrap(),
        101
    );

    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_minimal_transfer_after_balance() {
    /*11  12
      \  / \
      13   14

      11 - is on W100
      12 - is on W100
    */

    //setup_logging();
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 10000);
    start_and_finish_on_worker(&mut core, 12, 100, 10000);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    dbg!(&comm);

    comm.take_worker_msgs(100, 1);
    comm.take_worker_msgs(101, 1);

    assert_eq!(
        core.get_task_by_id_or_panic(13)
            .get()
            .get_assigned_worker()
            .unwrap(),
        100
    );
    assert_eq!(
        core.get_task_by_id_or_panic(14)
            .get()
            .get_assigned_worker()
            .unwrap(),
        101
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
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
    ]]);
    rt.balance();
    assert_eq!(rt.worker_load(100).get_n_cpus(), 12);
    assert_eq!(rt.worker_load(101).get_n_cpus(), 12);
    assert_eq!(rt.worker_load(102).get_n_cpus(), 12);
}

#[test]
fn test_resource_balancing3() {
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
    assert!(rt.worker_load(100).get_n_cpus() >= 12);
    assert!(rt.worker_load(101).get_n_cpus() >= 12);
    assert!(rt.worker_load(102).get_n_cpus() >= 12);
}

#[test]
fn test_resource_balancing4() {
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        2, 4, 2, 4, /* 12 */ 4, 4, 4, /* 12 */ 2, 2, 2, 2, 4, /* 12 */
    ]]);
    rt.balance();
    assert_eq!(rt.worker_load(100).get_n_cpus(), 12);
    assert_eq!(rt.worker_load(101).get_n_cpus(), 12);
    assert_eq!(rt.worker_load(102).get_n_cpus(), 12);
}

#[test]
fn test_resource_balancing5() {
    let mut rt = TestEnv::new();
    rt.new_workers(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        2, 4, 2, 4, 2, /* 14 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
    ]]);
    rt.balance();
    rt.check_worker_load_lower_bounds(&[10, 12, 12]);
}

#[test]
fn test_resource_balancing6() {
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
    rt.check_worker_load_lower_bounds(&[12, 12, 12]);
}

#[test]
fn test_resource_balancing7() {
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
    rt.check_worker_load_lower_bounds(&[12, 12, 12]);
}

#[test]
fn test_resources_blocked_workers() {
    let mut rt = TestEnv::new();
    rt.new_workers(&[4, 8, 2]);

    rt.new_assigned_tasks_cpus(&[&[4, 4, 4, 4, 4]]);
    rt.balance();
    assert!(rt.worker_load(100).get_n_cpus() >= 4);
    assert!(rt.worker_load(101).get_n_cpus() >= 8);
    assert_eq!(rt.worker_load(102).get_n_cpus(), 0);

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
    let mut rt = TestEnv::new();
    rt.new_workers(&[4, 8, 2]);

    rt.new_ready_tasks_cpus(&[8, 8, 16, 24]);
    rt.schedule();
    assert_eq!(rt.worker_load(100).get_n_cpus(), 0);
    assert_eq!(rt.worker_load(101).get_n_cpus(), 16);
    assert_eq!(rt.worker_load(102).get_n_cpus(), 0);

    let s = rt.core().take_sleeping_tasks();
    assert_eq!(s.len(), 2);
}

#[test]
fn test_resources_no_workers2() {
    let mut rt = TestEnv::new();
    rt.new_workers(&[8, 8, 8]);

    rt.new_ready_tasks_cpus(&[9, 10, 11]);
    rt.schedule();
    assert_eq!(rt.worker_load(100).get_n_cpus(), 0);
    assert_eq!(rt.worker_load(101).get_n_cpus(), 0);
    assert_eq!(rt.worker_load(102).get_n_cpus(), 0);

    rt.new_workers(&[9, 10]);
    rt.schedule();
    assert_eq!(rt.worker_load(100).get_n_cpus(), 0);
    assert_eq!(rt.worker_load(101).get_n_cpus(), 0);
    assert_eq!(rt.worker_load(102).get_n_cpus(), 0);
    assert_eq!(rt.worker(103).tasks().len(), 1);
    assert_eq!(rt.worker(104).tasks().len(), 1);

    let s = rt.core().take_sleeping_tasks();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].get().id, 12);
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
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    rt.new_workers_ext(&[
        // Worker 100
        (
            10,
            None,
            vec![GenericResourceDescriptor::indices("Res0", 10)],
        ),
        // Worker 101
        (10, None, vec![]),
        // Worker 102
        (
            10,
            None,
            vec![
                GenericResourceDescriptor::indices("Res0", 10),
                GenericResourceDescriptor::sum("Res1", 1000_000),
            ],
        ),
    ]);
    for i in 0..50 {
        rt.new_task(TaskBuilder::new(i).generic_res(0, 1));
    }
    for i in 50..100 {
        rt.new_task(TaskBuilder::new(i).generic_res(1, 2));
    }
    rt.schedule();
    assert_eq!(rt.core().get_worker_by_id(101).unwrap().tasks().len(), 0);
    assert!(rt.core().get_worker_by_id(100).unwrap().tasks().len() > 10);
    assert!(rt.core().get_worker_by_id(102).unwrap().tasks().len() > 10);
    assert_eq!(
        rt.core().get_worker_by_id(100).unwrap().tasks().len()
            + rt.core().get_worker_by_id(102).unwrap().tasks().len(),
        100
    );
    assert!(rt
        .core()
        .get_worker_by_id(100)
        .unwrap()
        .tasks()
        .iter()
        .all(|t| t.get().id < 50));

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
        (
            10,
            None,
            vec![GenericResourceDescriptor::indices("Res0", 10)],
        ),
        // Worker 101
        (10, None, vec![]),
        // Worker 102
        (
            10,
            None,
            vec![
                GenericResourceDescriptor::indices("Res0", 10),
                GenericResourceDescriptor::sum("Res1", 1000_000),
            ],
        ),
    ]);

    rt.new_task(TaskBuilder::new(1).cpus_compact(1).generic_res(0, 5));
    rt.new_task(TaskBuilder::new(2).cpus_compact(1).generic_res(0, 5));
    rt.new_task(TaskBuilder::new(3).cpus_compact(1).generic_res(0, 5));
    rt.new_task(TaskBuilder::new(4).cpus_compact(1).generic_res(0, 5));
    rt.schedule();

    assert_eq!(rt.core().get_worker_by_id_or_panic(100).tasks().len(), 2);
    assert_eq!(rt.core().get_worker_by_id_or_panic(101).tasks().len(), 0);
    assert_eq!(rt.core().get_worker_by_id_or_panic(102).tasks().len(), 2);
}

#[test]
fn test_generic_resource_balance2() {
    let _ = env_logger::init();
    let mut rt = TestEnv::new();
    rt.new_generic_resource(2);
    rt.new_workers_ext(&[
        // Worker 100
        (
            10,
            None,
            vec![GenericResourceDescriptor::indices("Res0", 10)],
        ),
        // Worker 101
        (10, None, vec![]),
        // Worker 102
        (
            10,
            None,
            vec![
                GenericResourceDescriptor::indices("Res0", 10),
                GenericResourceDescriptor::sum("Res1", 1000_000),
            ],
        ),
    ]);

    rt.new_task(TaskBuilder::new(1).cpus_compact(1).generic_res(0, 5));
    rt.new_task(
        TaskBuilder::new(2)
            .cpus_compact(1)
            .generic_res(0, 5)
            .generic_res(1, 500_000),
    );
    rt.new_task(TaskBuilder::new(3).cpus_compact(1).generic_res(0, 5));
    rt.new_task(
        TaskBuilder::new(4)
            .cpus_compact(1)
            .generic_res(0, 5)
            .generic_res(1, 500_000),
    );
    rt.schedule();

    dbg!(rt.core().get_worker_by_id_or_panic(102).tasks().len());

    assert_eq!(rt.core().get_worker_by_id_or_panic(100).tasks().len(), 2);
    assert_eq!(rt.core().get_worker_by_id_or_panic(101).tasks().len(), 0);
    assert_eq!(rt.core().get_worker_by_id_or_panic(102).tasks().len(), 2);
}
