use crate::internal::common::Set;
use crate::internal::messages::worker::{
    RetractResponse, RetractResponseMsg, TaskOutput, ToWorkerMessage,
};
use crate::internal::server::core::Core;
use crate::internal::server::reactor::on_steal_response;
use crate::internal::tests::utils::env::TestEnv;
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, finish_on_worker, start_and_finish_on_worker_with_data,
};
use crate::internal::tests::utils::task::TaskBuilder;
use crate::internal::tests::utils::workflows::submit_example_4;
use crate::resources::{ResourceAmount, ResourceUnits};
use crate::tests::utils::env::TestComm;
use crate::tests::utils::worker::WorkerBuilder;
use crate::{TaskId, WorkerId};
use std::time::Duration;

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

    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::new();
    scheduler.run_scheduling_without_balancing(rt.core(), &mut comm);

    let m1 = comm.take_worker_msgs(ws[0], 0);
    let m2 = comm.take_worker_msgs(ws[1], 0);
    let m3 = comm.take_worker_msgs(ws[2], 0);
    comm.emptiness_check();
    rt.core().sanity_check();

    let c1 = if m1.len() > 0 { task_count(&m1[0]) } else { 0 };
    let c2 = if m2.len() > 0 { task_count(&m2[0]) } else { 0 };
    let c3 = if m3.len() > 0 { task_count(&m3[0]) } else { 0 };

    assert_eq!(c1 + c2 + c3, 4);
    assert!(c1 == 4 || c2 == 4 && c3 == 4);
}

#[test]
fn test_no_deps_scattering_2() {
    todo!()
    /*let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[5, 5, 5]);

    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::new();
    let mut submit_and_check = |expected| {
        let _t = rt.new_task_default();
        scheduler.run_scheduling_without_balancing(rt.core(), &mut comm);
        let mut counts: Vec<_> = rt
            .core()
            .get_workers()
            .map(|w| w.sn_tasks().len())
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

    submit_and_check(vec![5, 5, 6]);
    submit_and_check(vec![5, 6, 6]);
    submit_and_check(vec![6, 6, 6]);
    submit_and_check(vec![6, 6, 7]);
    submit_and_check(vec![6, 7, 7]);*/
}

#[test]
fn test_no_deps_distribute_without_balance() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[10, 10, 10]);
    rt.new_tasks(150, &TaskBuilder::new());

    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::new();
    let need_balance = scheduler.run_scheduling_without_balancing(rt.core(), &mut comm);

    let m1 = comm.take_worker_msgs(ws[0], 1);
    let m2 = comm.take_worker_msgs(ws[1], 1);
    let m3 = comm.take_worker_msgs(ws[2], 1);
    comm.emptiness_check();
    rt.sanity_check();

    assert_eq!(task_count(&m1[0]), 50);
    assert_eq!(task_count(&m2[0]), 50);
    assert_eq!(task_count(&m3[0]), 50);
    assert!(!need_balance);
}

#[test]
fn test_no_deps_distribute_with_balance() {
    todo!()
    /*let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[2, 2, 2]);

    assert_eq!(rt.core().get_worker_map().len(), 3);

    let mut active_ids: Set<TaskId> = (1..=300).map(|_| rt.new_task_default()).collect();

    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::new();
    scheduler.run_scheduling(rt.core(), &mut comm);

    let m1 = comm.take_worker_msgs(ws[0], 1);
    let m2 = comm.take_worker_msgs(ws[1], 1);
    let m3 = comm.take_worker_msgs(ws[2], 1);
    comm.emptiness_check();
    rt.sanity_check();

    assert!(matches!(&m1[0], ToWorkerMessage::ComputeTasks(msg) if msg.tasks.len() >= 29));
    assert!(matches!(&m2[0], ToWorkerMessage::ComputeTasks(msg) if msg.tasks.len() >= 29));
    assert!(matches!(&m3[0], ToWorkerMessage::ComputeTasks(msg) if msg.tasks.len() >= 29));

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

    finish_all(rt.core(), m1, ws[0]);
    finish_all(rt.core(), m3, ws[2]);

    rt.core().assert_underloaded(&[ws[0], ws[2]]);
    rt.core().assert_not_underloaded(&[ws[1]]);

    scheduler.run_scheduling(rt.core(), &mut comm);

    rt.core().assert_not_underloaded(&ws);

    // TODO: Finish stealing

    let x1 = comm.take_worker_msgs(ws[1], 1);

    let stealing = match &x1[0] {
        ToWorkerMessage::StealTasks(tasks) => tasks.ids.clone(),
        _ => {
            unreachable!()
        }
    };

    comm.emptiness_check();
    rt.sanity_check();

    on_steal_response(
        rt.core(),
        &mut comm,
        ws[1],
        StealResponseMsg {
            responses: stealing.iter().map(|t| (*t, StealResponse::Ok)).collect(),
        },
    );

    let n1 = comm.take_worker_msgs(ws[0], 0);
    let n3 = comm.take_worker_msgs(ws[2], 0);

    assert!(n1.len() > 5);
    assert!(n3.len() > 5);
    assert_eq!(n1.len() + n3.len(), stealing.len());

    rt.core().assert_not_underloaded(&ws);

    comm.emptiness_check();
    rt.sanity_check();

    finish_all(rt.core(), n1, ws[0]);
    finish_all(rt.core(), n3, ws[2]);
    assert_eq!(
        active_ids.len(),
        rt.core().get_worker_by_id_or_panic(ws[1]).sn_tasks().len()
    );

    comm.emptiness_check();
    rt.sanity_check();
     */
}

#[test]
fn test_resource_balancing1() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[4, 6]);

    let ts = &rt.new_assigned_tasks_cpus(&[&[3, 4, 2]])[0];
    rt.balance();
    rt.check_worker_tasks(ws[0], &[ts[1]]);
    rt.check_worker_tasks(ws[1], &[ts[0], ts[2]]);

    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[6, 3]);
    let ts = &rt.new_assigned_tasks_cpus(&[&[3, 4, 2]])[0];
    rt.balance();
    rt.check_worker_tasks(ws[0], &[ts[1], ts[2]]);
    rt.check_worker_tasks(ws[1], &[ts[0]]);
}

#[test]
fn test_resource_balancing2() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
    ]]);
    rt.balance();
    assert_eq!(rt.worker_load(ws[0]).get(0.into()), u(12));
    assert_eq!(rt.worker_load(ws[1]).get(0.into()), u(12));
    assert_eq!(rt.worker_load(ws[2]).get(0.into()), u(12));
}

#[test]
fn test_resource_balancing3() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[
        &[
            4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */ 4, 4, 4, /* 12 */
        ],
        &[1, 1, 1, 1], // 4
        &[6],          // 6
    ]);
    rt.balance();
    assert!(rt.worker_load(ws[0]).get(0.into()) >= u(12));
    assert!(rt.worker_load(ws[1]).get(0.into()) >= u(12));
    assert!(rt.worker_load(ws[2]).get(0.into()) >= u(12));
}

#[test]
fn test_resource_balancing4() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[12, 12, 12]);

    rt.new_assigned_tasks_cpus(&[&[
        2, 4, 2, 4, /* 12 */ 4, 4, 4, /* 12 */ 2, 2, 2, 2, 4, /* 12 */
    ]]);
    rt.balance();
    assert_eq!(rt.worker_load(ws[0]).get(0.into()), u(12));
    assert_eq!(rt.worker_load(ws[1]).get(0.into()), u(12));
    assert_eq!(rt.worker_load(ws[2]).get(0.into()), u(12));
}

#[test]
fn test_resource_balancing5() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[12, 12, 12]);

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
    rt.new_workers_cpus(&[12, 12, 12, 12, 12]);
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

    let ws = rt.new_workers_cpus(&[12, 12, 12]);

    rt.new_task_running(&TaskBuilder::new().cpus(2), ws[0]);
    rt.new_task_running(&TaskBuilder::new().cpus(2), ws[0]);
    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[0]);

    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[1]);
    rt.new_task_assigned(&TaskBuilder::new().cpus(4), ws[1]); // <- only assigned
    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[1]);
    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[1]);

    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[2]);
    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[2]);
    rt.new_task_running(&TaskBuilder::new().cpus(4), ws[2]);

    rt.schedule();
    rt.check_worker_load_lower_bounds(&[u(12), u(12), u(12)]);
}

#[test]
fn test_resources_blocked_workers() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[4, 8, 2]);

    rt.new_assigned_tasks_cpus(&[&[4, 4, 4, 4, 4]]);
    rt.balance();
    assert!(rt.worker_load(ws[0]).get(0.into()) >= u(4));
    assert!(rt.worker_load(ws[1]).get(0.into()) >= u(8));
    assert_eq!(rt.worker_load(ws[2]).get(0.into()), u(0));

    rt.core().sanity_check();

    rt.new_tasks_cpus(&[3]);
    rt.schedule();

    rt.new_tasks_cpus(&[1]);
    rt.schedule();

    rt.balance();
}

#[test]
fn test_resources_no_workers1() {
    let u = ResourceAmount::new_units;
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[4, 8, 2]);

    rt.new_tasks_cpus(&[8, 8, 16, 24]);
    rt.schedule();
    assert_eq!(rt.worker_load(ws[0]).get(0.into()), u(0));
    assert_eq!(rt.worker_load(ws[1]).get(0.into()), u(16));
    assert_eq!(rt.worker_load(ws[2]).get(0.into()), u(0));
}

#[test]
fn test_resources_no_workers2() {
    todo!()
    /*fn check(task_cpu_counts: &[ResourceUnits]) {
        println!("Checking order {task_cpu_counts:?}");

        let mut rt = TestEnv::new();

        let unschedulable_index = task_cpu_counts
            .iter()
            .position(|&count| count > 10)
            .unwrap();

        let mut ws = rt.new_workers_cpus(&[8, 8, 8]);
        let task_ids = rt.new_tasks_cpus(task_cpu_counts);
        rt.schedule();
        assert!(rt.worker_load(ws[0]).get(0.into()).is_zero());
        assert!(rt.worker_load(ws[1]).get(0.into()).is_zero());
        assert!(rt.worker_load(ws[2]).get(0.into()).is_zero());

        ws.append(&mut rt.new_workers_cpus(&[9, 10]));
        rt.schedule();
        assert!(rt.worker_load(ws[0]).get(0.into()).is_zero());
        assert!(rt.worker_load(ws[1]).get(0.into()).is_zero());
        assert!(rt.worker_load(ws[2]).get(0.into()).is_zero());
        assert_eq!(rt.worker(ws[3]).sn_tasks().len(), 1);
        assert_eq!(rt.worker(ws[4]).sn_tasks().len(), 1);

        let sn = rt.core().take_sleeping_tasks();
        assert_eq!(sn.len(), 1);
        assert_eq!(sn[0], task_ids[unschedulable_index]);
    }

    check(&[9, 10, 11]);
    check(&[9, 11, 10]);
    check(&[10, 9, 11]);
    check(&[10, 11, 9]);
    check(&[11, 9, 10]);
    check(&[11, 10, 9]);*/
}

#[test]
fn test_resource_time_assign() {
    let mut rt = TestEnv::new();

    let w1 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(100, 0)));

    let _t1 = rt.new_task(&TaskBuilder::new().time_request(170));
    let t2 = rt.new_task_default();
    let t3 = rt.new_task(&TaskBuilder::new().time_request(99));

    rt.schedule();
    rt.finish_scheduling();
    rt.check_worker_tasks(w1, &[t2, t3]);
}

#[test]
fn test_resource_time_balance1() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut rt = TestEnv::new();

    let w1 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(50, 0)));
    let w2 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(200, 0)));
    let w3 = rt.new_worker(&WorkerBuilder::new(1).time_limit(Duration::new(100, 0)));

    let t1 = rt.new_task_assigned(&TaskBuilder::new().time_request(170), w2);
    let t2 = rt.new_task_assigned(&TaskBuilder::new(), w2);
    let t3 = rt.new_task_assigned(&TaskBuilder::new().time_request(99), w3);

    rt.balance();
    rt.check_worker_tasks(w1, &[t2]);
    rt.check_worker_tasks(w2, &[t1]);
    rt.check_worker_tasks(w3, &[t3]);
}

#[test]
fn test_generic_resource_assign2() {
    todo!()
    /*let mut rt = TestEnv::new();
    rt.new_generic_resource(2);

    let w1 = rt.new_worker(&WorkerBuilder::new(10).res_range("Res0", 1, 10));
    let w2 = rt.new_worker(&WorkerBuilder::new(10));
    let w3 = rt.new_worker(
        &WorkerBuilder::new(10)
            .res_range("Res0", 1, 10)
            .res_sum("Res1", 1_000_000),
    );

    rt.new_tasks(50, &TaskBuilder::new().add_resource(1, 1));
    rt.new_tasks(50, &TaskBuilder::new().add_resource(2, 2));

    rt.schedule();

    assert_eq!(rt.core().get_worker_by_id(w2).unwrap().sn_tasks().len(), 0);
    assert!(rt.core().get_worker_by_id(w1).unwrap().sn_tasks().len() > 10);
    assert!(rt.core().get_worker_by_id(w3).unwrap().sn_tasks().len() > 10);
    assert_eq!(
        rt.core().get_worker_by_id(w1).unwrap().sn_tasks().len()
            + rt.core().get_worker_by_id(w3).unwrap().sn_tasks().len(),
        100
    );
    assert!(
        rt.core()
            .get_worker_by_id(w1)
            .unwrap()
            .sn_tasks()
            .iter()
            .all(|task_id| task_id.job_task_id().as_num() < 50)
    );

    assert!(!rt.worker(w1).is_parked());
    assert!(rt.worker(w2).is_parked());
    assert!(!rt.worker(w3).is_parked());*/
}

#[test]
fn test_generic_resource_balance1() {
    todo!()
    /*let mut rt = TestEnv::new();
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

    assert_eq!(rt.core().get_worker_by_id_or_panic(w1).sn_tasks().len(), 2);
    assert_eq!(rt.core().get_worker_by_id_or_panic(w2).sn_tasks().len(), 0);
    assert_eq!(rt.core().get_worker_by_id_or_panic(w3).sn_tasks().len(), 2);*/
}

#[test]
fn test_generic_resource_balance2() {
    todo!()
    /*let mut rt = TestEnv::new();
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

    assert_eq!(rt.core().get_worker_by_id_or_panic(w1).sn_tasks().len(), 2);
    assert_eq!(rt.core().get_worker_by_id_or_panic(w2).sn_tasks().len(), 0);
    assert_eq!(rt.core().get_worker_by_id_or_panic(w3).sn_tasks().len(), 2);*/
}

#[test]
fn test_generic_resource_balancing3() {
    todo!()
    /*let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(2));
    let w2 = rt.new_worker(&WorkerBuilder::new(2).res_range("Res0", 1, 1));
    let _w3 = rt.new_tasks(80, &TaskBuilder::new());
    rt.new_tasks(20, &TaskBuilder::new().cpus(1).add_resource(1, 1));

    rt.schedule();

    assert!(rt.core().get_worker_by_id_or_panic(w1).sn_tasks().len() >= 40);
    assert!(rt.core().get_worker_by_id_or_panic(w2).sn_tasks().len() >= 40);*/
}

#[test]
fn test_generic_resource_variants1() {
    todo!()
    /*let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(4));
    let w2 = rt.new_worker(&WorkerBuilder::new(4).res_range("Res0", 1, 2));

    let task = TaskBuilder::new()
        .cpus(2)
        .next_resources()
        .cpus(1)
        .add_resource(1, 1);
    rt.new_tasks(4, &task);
    rt.schedule();

    assert_eq!(rt.core().get_worker_by_id_or_panic(w1).sn_tasks().len(), 2);
    assert_eq!(rt.core().get_worker_by_id_or_panic(w2).sn_tasks().len(), 2);
     */
}

#[test]
fn test_generic_resource_variants2() {
    todo!()
    /*let mut rt = TestEnv::new();
    rt.new_generic_resource(1);
    let w1 = rt.new_worker(&WorkerBuilder::new(4));
    let w2 = rt.new_worker(&WorkerBuilder::new(4).res_range("Res0", 1, 2));

    let task = TaskBuilder::new()
        .cpus(8)
        .next_resources()
        .cpus(2)
        .add_resource(1, 1);
    rt.new_tasks(4, &task);
    rt.schedule();

    assert_eq!(rt.core().get_worker_by_id_or_panic(w1).sn_tasks().len(), 0);
    assert_eq!(rt.core().get_worker_by_id_or_panic(w2).sn_tasks().len(), 4);*/
}

#[test]
fn test_task_data_deps_initial_placing() {
    /*let test_data = vec![
        (0, 0, 100_000, 100_000, 100_000, 0),
        (0, 1, 100_000, 100_000, 100_000, 1),
        (0, 1, 201_000, 100_000, 100_000, 0),
        (0, 1, 100_000, 40_000, 80_000, 1),
        (0, 1, 100_000, 40_000, 40_000, 0),
    ];
    let mut test_data2: Vec<(usize, usize, u64, u64, u64, usize)> = Vec::new();
    let inverse = |w| if w == 0 { 1 } else { 0 };
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
        let mut rt = TestEnv::new();
        let wf = submit_example_4(&mut rt);
        let ws = rt.new_workers_cpus(&[1, 1, 1]);
        start_and_finish_on_worker_with_data(
            rt.core(),
            wf[0],
            ws[*worker1],
            vec![TaskOutput {
                id: 0.into(),
                size: *size1,
            }],
        );
        start_and_finish_on_worker_with_data(
            rt.core(),
            wf[1],
            ws[*worker2],
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
        rt.core().assert_ready(&[wf[2]]);
        let mut comm = TestComm::new();
        let mut scheduler = create_test_scheduler();
        scheduler.run_scheduling(rt.core(), &mut comm);
        assert_eq!(
            rt.task(wf[2]).get_assigned_worker(),
            Some(ws[*target_worker])
        );
        rt.sanity_check();
    }*/
}

#[test]
fn test_task_data_deps_balancing() {
    todo!()
    /*let _ = env_logger::builder().is_test(true).try_init();
    for odd in [0u32, 1u32] {
        for late_worker in [true, false] {
            let mut rt = TestEnv::new();
            let t1 = rt.new_task_default();
            let t2 = rt.new_task_default();
            let ts: Vec<_> = (0..100u32)
                .map(|i| rt.new_task(&TaskBuilder::new().data_dep(t1, i).data_dep(t2, i)))
                .collect();
            let mut ws = if late_worker {
                rt.new_workers_cpus(&[1])
            } else {
                rt.new_workers_cpus(&[1, 1])
            };
            let mut set_data = |task_id: TaskId, worker_id: WorkerId| {
                start_and_finish_on_worker_with_data(
                    rt.core(),
                    task_id,
                    worker_id,
                    (0..100u32)
                        .map(|i| TaskOutput {
                            id: i.into(),
                            size: if (i % 2) == odd { 100 } else { 5_000 },
                        })
                        .collect(),
                )
            };
            set_data(t1, ws[0]);
            set_data(t2, ws[0]);

            rt.core().assert_ready(&ts);
            if late_worker {
                ws.push(rt.new_worker_cpus(1))
            }
            let mut comm = TestComm::new();
            let mut scheduler = create_test_scheduler();
            scheduler.run_scheduling(rt.core(), &mut comm);

            let n1_count = rt
                .worker(ws[1])
                .sn_tasks()
                .iter()
                .map(|task_id| {
                    if (task_id.job_task_id().as_num() + 1) % 2 == odd {
                        1
                    } else {
                        0
                    }
                })
                .sum::<u32>();
            dbg!(n1_count);
            assert!(n1_count > 40);
        }
    }*/
}

#[test]
fn test_resource_priority_balancing() {
    todo!()
    /*let mut rt = TestEnv::new();
    //                    0  1  2  3  4  5  6  7  8  9  10
    let ws = rt.new_workers_cpus(&[4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 8]);

    rt.set_job(7, 100);
    for i in 0..=9 {
        rt.new_task_running(&TaskBuilder::new().cpus(4), ws[i]);
        for _j in 1..(4 + i) {
            rt.new_task_assigned(&TaskBuilder::new().cpus(4), ws[i]);
        }
    }

    rt.set_job(1, 1);
    for _ in 1..=3 {
        rt.new_task_assigned(&TaskBuilder::new().cpus(4), ws[0]);
    }

    rt.balance();
    assert!(
        rt.get_worker_tasks(ws[10])
            .iter()
            .filter(|t| t.job_id() == 1.into())
            .count()
            >= 2
    );*/
}
