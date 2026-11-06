use crate::gateway::DrainMode;
use crate::gateway::LostWorkerReason;
use crate::internal::common::resources::ResourceDescriptor;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    ComputeTasksMsg, NewWorkerMsg, TaskIdsMsg, ToWorkerMessage, WorkerTaskUpdate,
};
use crate::internal::scheduler::SchedulerConfig;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::{
    on_cancel_tasks, on_new_tasks, on_new_worker, on_remove_worker, on_retract_response,
    on_set_worker_drain, on_task_update,
};
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::Worker;
use crate::internal::tests::utils::shared::{res_kind_groups, res_kind_sum};
use crate::internal::tests::utils::sorted_vec;
use crate::internal::tests::utils::task::{TaskBuilder, task_running_msg};
use crate::internal::tests::utils::workflows::{submit_example_1, submit_example_3};
use crate::internal::worker::configuration::OverviewConfiguration;
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceIdMap};
use crate::tests::utils::env::{TestComm, TestEnv};
use crate::tests::utils::worker::WorkerBuilder;
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{ResourceVariantId, TaskId, WorkerId};
use smallvec::smallvec;
use std::time::{Duration, Instant};

#[test]
fn test_worker_add() {
    let mut core = Core::default();
    assert_eq!(core.get_workers().count(), 0);

    let mut comm = TestComm::new();
    comm.emptiness_check();

    let wcfg = WorkerConfiguration {
        resources: ResourceDescriptor::simple_cpus(4),
        listen_address: "test1:123".into(),
        hostname: "test1".to_string(),
        work_dir: Default::default(),
        heartbeat_interval: Duration::from_millis(1000),
        overview_configuration: OverviewConfiguration {
            send_interval: Some(Duration::from_millis(1000)),
            gpu_families: Default::default(),
        },
        idle_timeout: None,
        time_limit: None,
        retract_check_interval: Duration::from_secs(30),
        on_server_lost: ServerLostPolicy::Stop,
        min_utilization: 0.0,
        extra: Default::default(),
        group: "default".to_string(),
    };

    let worker = Worker::new(
        402.into(),
        wcfg,
        &ResourceIdMap::from_vec(vec!["cpus".to_string()]),
        Instant::now(),
    );
    on_new_worker(&mut core, &mut comm, worker);

    let new_w = comm.client.take_new_workers();
    assert_eq!(new_w.len(), 1);
    assert_eq!(new_w[0].0.as_num(), 402);

    assert_eq!(
        new_w[0].1.resources.resources[0].kind.size(),
        ResourceAmount::new_units(4)
    );

    assert!(
        matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id, address: ref _a, resources: ref r,
        }) if worker_id.as_num() == 402 && r.n_resources == vec![ResourceAmount::new_units(4)])
    );

    comm.check_need_scheduling();
    comm.emptiness_check();
    assert_eq!(core.get_workers().count(), 1);

    let wcfg2 = WorkerConfiguration {
        resources: ResourceDescriptor::new(
            vec![
                ResourceDescriptorItem {
                    name: "cpus".to_string(),
                    kind: res_kind_groups(&[vec!["2", "3", "4"], vec!["100", "150"]]),
                },
                ResourceDescriptorItem {
                    name: "mem".to_string(),
                    kind: res_kind_sum(100_000_000),
                },
            ],
            Default::default(),
        ),
        listen_address: "test2:123".into(),
        hostname: "test2".to_string(),
        group: "default".to_string(),
        work_dir: Default::default(),
        heartbeat_interval: Duration::from_millis(1000),
        overview_configuration: OverviewConfiguration {
            send_interval: Some(Duration::from_millis(1000)),
            gpu_families: Default::default(),
        },
        idle_timeout: None,
        time_limit: None,
        retract_check_interval: Default::default(),
        on_server_lost: ServerLostPolicy::Stop,
        min_utilization: 0.0,
        extra: Default::default(),
    };

    let worker = Worker::new(
        502.into(),
        wcfg2,
        &ResourceIdMap::from_vec(vec![
            "cpus".to_string(),
            "gpus".to_string(),
            "mem".to_string(),
        ]),
        Instant::now(),
    );
    on_new_worker(&mut core, &mut comm, worker);

    let new_w = comm.client.take_new_workers();
    assert_eq!(new_w.len(), 1);
    assert_eq!(new_w[0].0.as_num(), 502);
    assert_eq!(new_w[0].1.resources.resources.len(), 2);

    assert!(
        matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id, address: ref a, resources: ref r,
        }) if worker_id.as_num() == 502 && a == "test2:123" && r.n_resources == vec![ResourceAmount::new_units(5), ResourceAmount::new_units(0), ResourceAmount::new_units(100_000_000)])
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert_eq!(core.get_workers().count(), 2);
}

#[test]
fn test_submit_jobs() {
    let mut rt = TestEnv::new();
    let mut comm = TestComm::new();

    let t1 = TaskId::new(100.into(), 501.into());
    let t2 = TaskId::new(100.into(), 502.into());
    let task1 = TaskBuilder::new().build(t1, rt.core());
    let task2 = TaskBuilder::new().task_deps(&[t1]).build(t2, rt.core());

    on_new_tasks(rt.core(), &mut comm, vec![task1, task2]);

    comm.check_need_scheduling();
    comm.emptiness_check();

    let task1 = rt.task(t1);
    let task2 = rt.task(t2);
    assert_eq!(task1.get_unfinished_deps(), 0);
    assert_eq!(task2.get_unfinished_deps(), 1);

    check_task_consumers_exact(task1, &[task2]);

    let t3 = TaskId::new(100.into(), 604.into());
    let t4 = TaskId::new(100.into(), 503.into());
    let t5 = TaskId::new(100.into(), 603.into());
    let t6 = TaskId::new(100.into(), 601.into());
    let task3 = TaskBuilder::new().build(t3, rt.core());
    let task4 = TaskBuilder::new().task_deps(&[t1, t3]).build(t4, rt.core());
    let task5 = TaskBuilder::new().task_deps(&[t3]).build(t5, rt.core());
    let task6 = TaskBuilder::new()
        .task_deps(&[t3, t4, t5, t2])
        .build(t6, rt.core());

    on_new_tasks(rt.core(), &mut comm, vec![task3, task4, task5, task6]);

    comm.check_need_scheduling();
    comm.emptiness_check();

    let t1 = rt.task(t1);
    let t2 = rt.task(t2);
    let t4 = rt.task(t4);
    let t6 = rt.task(t6);

    check_task_consumers_exact(t1, &[t2, t4]);
    assert_eq!(t1.get_unfinished_deps(), 0);

    check_task_consumers_exact(t2, &[t6]);

    assert_eq!(t1.get_unfinished_deps(), 0);
    assert_eq!(t2.get_unfinished_deps(), 1);
    assert_eq!(t4.get_unfinished_deps(), 2);
    assert_eq!(t6.get_unfinished_deps(), 4);
}

fn task_finished(task_id: TaskId) -> WorkerTaskUpdate {
    WorkerTaskUpdate::Finished { task_id }
}

#[test]
fn test_assignments_and_finish() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[2, 3, 1]);

    /*
       t1   t2[3]    t4[3]   t5[1]
        \    /         |
          t3           t7
    */

    let t1 = rt.new_task(&TaskBuilder::new().user_priority(12).cpus(1));
    let t2 = rt.new_task(&TaskBuilder::new().user_priority(12).cpus(3));
    let t3 = rt.new_task(
        &TaskBuilder::new()
            .user_priority(12)
            .cpus(1)
            .task_deps(&[t1, t2]),
    );
    let t4 = rt.new_task(&TaskBuilder::new().cpus(3));
    let t5 = rt.new_task(&TaskBuilder::new().user_priority(12).cpus(1));
    let t7 = rt.new_task(&TaskBuilder::new().task_deps(&[t4]));

    let mut comm = rt.schedule();

    let msgs = comm.take_worker_msgs(ws[0], 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTasks(ComputeTasksMsg {
            tasks,
            shared_data: _,
        }) if tasks.len() == 2 && tasks[0].id == t1 &&
              tasks[1].id == t5 && tasks[0].shared_index == 0
    ));
    let msgs = comm.take_worker_msgs(ws[1], 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTasks(ComputeTasksMsg {
            tasks,
            ..
        }) if tasks[0].id == t2
    ));
    comm.emptiness_check();

    rt.core().assert_assigned(&[t1, t2]);
    rt.core().assert_waiting(&[t3, t7]);

    assert!(rt.task(t5).is_assigned());

    on_task_update(rt.core(), &mut comm, ws[0], smallvec![task_finished(t5)]);

    assert!(rt.core().find_task(t5).is_none());
    check_worker_tasks_exact(rt.core(), ws[0], &[t1]);
    check_worker_tasks_exact(rt.core(), ws[1], &[t2]);
    check_worker_tasks_exact(rt.core(), ws[2], &[]);

    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t5);
    comm.emptiness_check();

    assert!(!rt.task_exists(t5));
    assert!(rt.task(t2).is_assigned());

    on_task_update(rt.core(), &mut comm, ws[1], smallvec![task_finished(t2)]);

    assert!(!rt.task_exists(t2));
    check_worker_tasks_exact(rt.core(), ws[0], &[t1]);
    check_worker_tasks_exact(rt.core(), ws[1], &[]);
    check_worker_tasks_exact(rt.core(), ws[2], &[]);

    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t2);
    comm.emptiness_check();
    rt.sanity_check();
    on_task_update(rt.core(), &mut comm, ws[0], smallvec![task_finished(t1)]);
    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    comm.check_need_scheduling();
    comm.emptiness_check();

    let mut comm = rt.schedule();

    let msgs = comm.take_worker_msgs(ws[0], 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTasks(ComputeTasksMsg {
            tasks,
            ..
        }) if tasks[0].id == t3
    ));

    let msgs = comm.take_worker_msgs(ws[1], 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTasks(ComputeTasksMsg {
            tasks,
            ..
        }) if tasks[0].id == t4
    ));
    rt.sanity_check();

    on_task_update(rt.core(), &mut comm, ws[0], smallvec![task_finished(t3)]);

    comm.check_need_scheduling();

    assert_eq!(comm.client.take_task_finished(1), vec![t3]);
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_running_task_on_error() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let wf = submit_example_1(&mut rt);
    rt.start_and_finish_task(wf[0], ws[0]);
    rt.start_and_finish_task(wf[1], ws[1]);

    rt.assign_task(wf[2], ws[2]);
    rt.core().assert_assigned(&[wf[2]]);
    assert!(worker_has_task(rt.core(), ws[2], wf[2]));

    let mut comm = TestComm::new();
    let info = TaskFailInfo::from_string("".to_string());
    on_task_update(
        rt.core(),
        &mut comm,
        ws[2],
        smallvec![WorkerTaskUpdate::Failed {
            task_id: wf[2],
            info
        }],
    );
    assert!(!worker_has_task(rt.core(), ws[2], wf[2]));

    let mut msgs = comm.client.take_task_errors(1);
    let (id, cs, _) = msgs.pop().unwrap();
    assert_eq!(id, wf[2]);
    assert_eq!(sorted_vec(cs), vec![wf[4], wf[5], wf[6]]);
    comm.check_need_scheduling();
    comm.emptiness_check();

    assert!(!rt.task_exists(wf[5]));
    assert!(!rt.task_exists(wf[4]));
    rt.sanity_check();
}

#[test]
fn finish_task_without_outputs() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(1);
    let t1 = rt.new_task_assigned(&TaskBuilder::new(), w1);

    let mut comm = TestComm::new();
    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(t1)]);
    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_task_cancel() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[10, 10, 10]);
    let wf = submit_example_1(&mut rt);

    let t1 = rt.new_task_default();
    let t2 = rt.new_task_default();
    let t3 = rt.new_task_default();

    rt.assign_task(wf[0], ws[1]);
    rt.assign_task(wf[1], ws[1]);
    rt.assign_task(t1, ws[1]);
    rt.assign_task(t2, ws[0]);

    let mut comm = TestComm::new();
    on_cancel_tasks(rt.core(), &mut comm, &[wf[0], wf[1], t1, t2, wf[2]]);

    let msgs = comm.take_worker_msgs(ws[0], 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![t2])
    );

    let msgs = comm.take_worker_msgs(ws[1], 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if sorted_vec(ids.clone()) == vec![wf[0], wf[1], t1])
    );

    assert_eq!(rt.core().task_map().len(), 1);
    assert!(rt.task_exists(t3));

    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_worker_lost_with_mn_task_non_root() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));
    rt.start_task_mn(t1, &[ws[3], ws[1], ws[0]]);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);
    rt.sanity_check();
    assert_eq!(comm.client.take_lost_workers().len(), 1);
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w == ws[1]));
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert_eq!(rt.task(t1).mn_placement().unwrap(), &[ws[3], ws[0]]);
}

#[test]
fn test_worker_lost_with_mn_task_root() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));

    rt.start_task_mn(t1, &[ws[3], ws[1], ws[0]]);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, ws[3], LostWorkerReason::HeartbeatLost);
    rt.sanity_check();
    assert_eq!(comm.client.take_lost_workers().len(), 1);
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w == ws[3]
    ));
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t1).is_waiting());
}

#[test]
fn test_worker_crashing_task() {
    let mut rt = TestEnv::new();
    let t1 = rt.new_task_default();
    assert_eq!(rt.task(t1).crash_counter, 0);

    for x in 1..=5 {
        let mut comm = TestComm::new();
        let worker_id = rt.new_worker(&WorkerBuilder::new(1));
        rt.assign_and_start_task(t1, worker_id, 0);
        on_remove_worker(
            rt.core(),
            &mut comm,
            worker_id,
            LostWorkerReason::HeartbeatLost,
        );
        let mut lw = comm.client.take_lost_workers();
        assert_eq!(lw[0].0, worker_id);
        comm.check_need_scheduling();
        assert!(matches!(
            comm.take_broadcasts(1)[0],
            ToWorkerMessage::LostWorker(w)
            if worker_id == w
        ));
        assert_eq!(std::mem::take(&mut lw[0].1), vec![t1]);
        if x == 5 {
            let errs = comm.client.take_task_errors(1);
            assert_eq!(errs[0].0, t1);
            assert_eq!(
                errs[0].2.message,
                "Task was running on a worker that was lost; the task has occurred 5 times in this situation and limit was reached."
            );
        } else {
            assert_eq!(rt.task(t1).crash_counter, x);
        }
        comm.emptiness_check();
    }
}

#[test]
fn test_task_mn_fail() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));
    rt.start_task_mn(t1, &[ws[3], ws[1], ws[0]]);
    let mut comm = TestComm::new();
    let info = TaskFailInfo::from_string("".to_string());
    on_task_update(
        rt.core(),
        &mut comm,
        ws[3],
        smallvec![WorkerTaskUpdate::Failed { task_id: t1, info }],
    );
    rt.sanity_check();
    let msgs = comm.client.take_task_errors(1);
    assert_eq!(msgs[0].0, t1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.core().find_task(1.into()).is_none());
    for w in &ws {
        assert!(rt.core().get_worker(*w).mn_assignment().is_none());
    }
}

#[test]
fn test_task_mn_cancel() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));
    rt.start_task_mn(t1, &[ws[3], ws[1], ws[0]]);
    let mut comm = TestComm::new();

    on_cancel_tasks(rt.core(), &mut comm, &[t1]);
    rt.sanity_check();
    let msgs = comm.take_worker_msgs(ws[3], 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![t1])
    );
    comm.check_need_scheduling();
    comm.emptiness_check();

    let comm = rt.schedule();
    rt.sanity_check();
    comm.emptiness_check();

    assert!(!rt.task_exists(t1));
    for w in rt.core().get_workers() {
        assert!(w.mn_assignment().is_none());
    }
}

#[test]
fn test_running_task() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[10, 10, 10]);
    let t1 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);
    let t2 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);

    let mut comm = TestComm::new();

    comm.emptiness_check();

    let up = WorkerTaskUpdate::Running(task_running_msg(t1));
    on_task_update(rt.core(), &mut comm, ws[1], smallvec![up]);
    assert_eq!(comm.client.take_task_running(1), vec![t1]);
    comm.emptiness_check();

    let up = WorkerTaskUpdate::Running(task_running_msg(t2));
    on_task_update(rt.core(), &mut comm, ws[1], smallvec![up]);
    assert_eq!(comm.client.take_task_running(1)[0], t2);
    comm.emptiness_check();

    assert!(matches!(
        rt.task(t1).state,
        TaskRuntimeState::Running {
            worker_id,
            ..
        } if worker_id == ws[1]
    ));
    assert!(matches!(
        rt.task(t2).state,
        TaskRuntimeState::Running {
            worker_id,
            ..
        } if worker_id == ws[1]
    ));

    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);
    let mut lw = comm.client.take_lost_workers();
    assert_eq!(lw[0].0, ws[1]);
    assert_eq!(sorted_vec(std::mem::take(&mut lw[0].1)), vec![t1, t2]);
    comm.check_need_scheduling();
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w == ws[1]
    ));
    comm.emptiness_check();
}

#[test]
fn lost_worker_with_running_and_assign_tasks() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[10, 10, 10]);
    let wf = submit_example_1(&mut rt);

    let t1 = rt.new_task_default();
    let t2 = rt.new_task_default();

    rt.assign_task(wf[0], ws[1]);
    rt.assign_and_start_task(wf[1], ws[1], 0);
    rt.assign_task(t1, ws[1]);

    rt.core().assert_running(&[wf[1]]);
    assert_eq!(rt.task(wf[1]).instance_id, 0.into());

    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);

    assert_eq!(comm.client.take_lost_workers(), vec![(ws[1], vec![wf[1]])]);

    rt.core().assert_ready(&[wf[0], wf[1]]);
    assert_eq!(rt.task(wf[1]).instance_id, 1.into());
    assert!(rt.task(t1).is_ready());
    rt.core().assert_ready(&[t1]);

    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w == ws[1]
    ));

    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.core().assert_ready(&[t2]);
    rt.sanity_check();
}

fn check_worker_tasks_exact(core: &Core, worker_id: WorkerId, tasks: &[TaskId]) {
    let worker = core.get_worker(worker_id);
    let sn = worker.sn_assignment().unwrap();
    assert_eq!(sn.assigned_tasks.len(), tasks.len());
    for task in tasks {
        assert!(sn.assigned_tasks.contains(task));
    }
}

fn worker_has_task(core: &Core, worker_id: WorkerId, task_id: TaskId) -> bool {
    core.get_worker(worker_id)
        .sn_assignment()
        .unwrap()
        .assigned_tasks
        .contains(&task_id)
}

fn check_task_consumers_exact(task: &Task, consumers: &[&Task]) {
    let task_consumers = task.get_consumers();

    assert_eq!(task_consumers.len(), consumers.len());
    for consumer in consumers {
        assert!(task_consumers.contains(&consumer.id));
    }
}

#[test]
fn test_task_deps() {
    let mut rt = TestEnv::new();

    let wf = submit_example_3(&mut rt);
    let w1 = rt.new_worker_cpus(1);
    rt.start_and_finish_task(wf[1], w1);
    rt.core().assert_waiting(&[wf[2], wf[3], wf[5]]);
    rt.core().assert_ready(&[wf[4]]);

    rt.start_and_finish_task(wf[0], w1);
    rt.core().assert_waiting(&[wf[5]]);
    rt.core().assert_ready(&[wf[2], wf[3], wf[4]]);
}

#[test]
fn test_worker_groups() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1]);
    let g = rt.core().worker_group("default").unwrap();
    assert_eq!(&sorted_vec(g.worker_ids().collect()), &ws);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);
    let g = rt.core().worker_group("default").unwrap();
    assert_eq!(sorted_vec(g.worker_ids().collect()), vec![ws[0]]);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, ws[0], LostWorkerReason::HeartbeatLost);
    assert!(rt.core().worker_group("default").is_none());
}

#[test]
fn test_task_reject1() {
    let mut rt = TestEnv::new();
    let w = rt.new_worker_cpus(4);
    let t = rt.new_task_default();
    rt.schedule();
    assert!(rt.task(t).is_assigned());
    let mut comm = TestComm::new();
    on_task_update(
        rt.core(),
        &mut comm,
        w,
        smallvec![WorkerTaskUpdate::RejectRequest {
            task_id: t,
            rv_id: Some(ResourceVariantId::new(0))
        }],
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t).is_waiting());
    assert!(rt.worker(w).is_free());

    rt.schedule();
    assert!(rt.task(t).is_waiting());
    assert!(rt.worker(w).is_free());

    let rq_id = rt.task(t).resource_rq_id;
    on_task_update(
        rt.core(),
        &mut comm,
        w,
        smallvec![WorkerTaskUpdate::EnableRequest {
            resource_rq_id: rq_id,
            rv_id: ResourceVariantId::new(0)
        }],
    );
    comm.check_need_scheduling();
    comm.emptiness_check();

    rt.schedule();
    assert!(rt.task(t).is_assigned());
}

#[test]
fn test_task_reject2() {
    let mut rt = TestEnv::new();
    let w = rt.new_worker_cpus(4);
    let t = rt.new_task(&TaskBuilder::new().cpus(4).next_variant().cpus(2));
    rt.schedule();
    assert!(
        matches!(&rt.task(t).state, TaskRuntimeState::Assigned { worker_id, rv_id } if *worker_id == w && rv_id.as_num() == 0)
    );
    let mut comm = TestComm::new();
    on_task_update(
        rt.core(),
        &mut comm,
        w,
        smallvec![WorkerTaskUpdate::RejectRequest {
            task_id: t,
            rv_id: Some(ResourceVariantId::new(0))
        }],
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t).is_waiting());
    rt.schedule();
    assert!(
        matches!(&rt.task(t).state, TaskRuntimeState::Assigned { worker_id, rv_id } if *worker_id == w && rv_id.as_num() == 1)
    );
    assert!(!rt.worker(w).is_free());
}

#[test]
fn test_task_reject3() {
    let mut rt = TestEnv::new();
    let w = rt.new_worker_cpus(4);
    let t1 = rt.new_task_default();
    let t2 = rt.new_task_default();
    rt.schedule();
    assert!(rt.task(t1).is_assigned());
    assert!(rt.task(t2).is_assigned());
    let mut comm = TestComm::new();
    on_task_update(
        rt.core(),
        &mut comm,
        w,
        smallvec![WorkerTaskUpdate::RejectRequest {
            task_id: t1,
            rv_id: Some(ResourceVariantId::new(0))
        }],
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_assigned());

    on_task_update(
        rt.core(),
        &mut comm,
        w,
        smallvec![WorkerTaskUpdate::RejectRequest {
            task_id: t2,
            rv_id: Some(ResourceVariantId::new(0))
        }],
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_waiting());
}

fn get_prefilled(rt: &mut TestEnv, tasks: &[TaskId]) -> Option<TaskId> {
    tasks.iter().find(|t| rt.task(**t).is_prefilled()).copied()
}

fn get_assigned(rt: &mut TestEnv, tasks: &[TaskId]) -> Option<TaskId> {
    tasks.iter().find(|t| rt.task(**t).is_assigned()).copied()
}

fn setup_prefill(rt: &mut TestEnv) -> (WorkerId, TaskId, TaskId) {
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 1,
        proactive_filling_max: 1,
        ..Default::default()
    });
    let tasks = rt.new_tasks(3, &TaskBuilder::new());
    let w1 = rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    let prefilled = get_prefilled(rt, &tasks).unwrap();
    let assigned = get_assigned(rt, &tasks).unwrap();
    (w1, assigned, prefilled)
}

#[test]
fn test_prefill_submit_high_priority() {
    for cpus in [1, 2] {
        let mut rt = TestEnv::new();
        let (w1, _t1, t2) = setup_prefill(&mut rt);
        let mut comm = TestComm::new();

        let t1 = TaskId::new(100.into(), 501.into());
        let task1 = TaskBuilder::new()
            .user_priority(10)
            .cpus(cpus)
            .build(t1, rt.core());
        on_new_tasks(rt.core(), &mut comm, vec![task1]);
        comm.check_need_scheduling();
        match &comm.take_worker_msgs(w1, 1)[0] {
            ToWorkerMessage::RetractTasks(ts) => {
                assert_eq!(ts.ids, vec![t2]);
            }
            _ => panic!("Invalid worker msg"),
        }
        comm.emptiness_check();
        match rt.task(t2).state {
            TaskRuntimeState::Retracting { worker_id } => {
                assert_eq!(worker_id, w1);
            }
            _ => panic!("Invalid state"),
        }
        rt.sanity_check();
    }
}

#[test]
fn test_prefill_retracted_and_prefill_again() {
    let mut rt = TestEnv::new();
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 0,
        proactive_filling_max: 1,
        ..Default::default()
    });
    let tasks1 = rt.new_tasks(3, &TaskBuilder::new());
    let w = rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    let p1 = get_prefilled(&mut rt, &tasks1).unwrap();
    let a1 = get_assigned(&mut rt, &tasks1).unwrap();
    let new_task = rt.new_task(&TaskBuilder::new().user_priority(1));
    assert!(matches!(
        rt.task(p1).state,
        TaskRuntimeState::Retracting { worker_id } if w == worker_id
    ));
    rt.finish_task(a1, w);
    rt.schedule();
    assert!(rt.task(new_task).is_assigned());
    assert!(rt.task(p1).is_retracting());
}

#[test]
fn test_prefill_submit_same_priority() {
    for cpus in [1, 2] {
        let mut rt = TestEnv::new();
        let (w1, _t1, t2) = setup_prefill(&mut rt);
        let mut comm = TestComm::new();

        let t1 = TaskId::new(100.into(), 501.into());
        let task1 = TaskBuilder::new().cpus(cpus).build(t1, rt.core());
        on_new_tasks(rt.core(), &mut comm, vec![task1]);
        comm.check_need_scheduling();
        comm.emptiness_check();
        match rt.task(t2).state {
            TaskRuntimeState::Prefilled { worker_id } => {
                assert_eq!(worker_id, w1);
            }
            _ => panic!("Invalid state"),
        }
        rt.sanity_check();
    }
}

#[test]
fn test_prefill_worker_lost() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, w1, LostWorkerReason::ConnectionLost);
    comm.check_need_scheduling();
    comm.take_broadcasts(1);
    comm.client.take_lost_workers();
    comm.emptiness_check();
    assert!(rt.task(t1).is_waiting());
    assert!(rt.task(t2).is_waiting());
    rt.sanity_check();
}

#[test]
fn test_prefill_started_on_same_worker() {
    let mut rt = TestEnv::new();

    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 0,
        proactive_filling_max: 3,
        ..Default::default()
    });

    let t1 = rt.new_task_default();
    let w1 = rt.new_worker(&WorkerBuilder::new(2));
    rt.schedule();
    assert!(rt.task(t1).is_assigned());
    let tasks = rt.new_tasks(2, &TaskBuilder::new());
    rt.schedule();
    let prefilled: TaskId = get_prefilled(&mut rt, &tasks).unwrap();
    let up1 = WorkerTaskUpdate::Finished { task_id: t1 };
    let mut comm = TestComm::new();
    on_task_update(rt.core(), &mut comm, w1, smallvec![up1]);
    rt.schedule();
    assert!(rt.task(prefilled).is_retracting());
    let up2 = WorkerTaskUpdate::Running(task_running_msg(prefilled));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up2]);
    rt.sanity_check();
}

#[test]
fn test_prefill_retracted_but_already_started() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();

    let t3 = TaskId::new(100.into(), 501.into());
    let task3 = TaskBuilder::new().user_priority(10).build(t3, rt.core());
    on_new_tasks(rt.core(), &mut comm, vec![task3]);
    comm.check_need_scheduling();
    match &comm.take_worker_msgs(w1, 1)[0] {
        ToWorkerMessage::RetractTasks(ts) => assert_eq!(ts.ids, vec![t2]),
        _ => panic!("Invalid worker msg"),
    }
    comm.emptiness_check();
    assert!(rt.task(t2).is_retracting());

    rt.finish_task(t1, w1);
    let up = WorkerTaskUpdate::Running(task_running_msg(t2));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    comm.client.take_task_running(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.finish_task(t2, w1);
    rt.schedule();
}

#[test]
fn test_prefill_started() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    let up1 = WorkerTaskUpdate::Finished { task_id: t1 };
    let up2 = WorkerTaskUpdate::RunningPrefilled(task_running_msg(t2));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up1, up2]);
    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    assert_eq!(comm.client.take_task_running(1)[0], t2);
    comm.emptiness_check();
    match rt.task(t2).state {
        TaskRuntimeState::Running { worker_id, .. } => {
            assert_eq!(worker_id, w1);
        }
        _ => panic!(),
    }
    assert!(!rt.task_exists(t1));
    assert!(
        rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .prefilled_tasks
            .is_empty()
    );
    rt.sanity_check();
}

#[test]
fn test_prefill_rejected() {
    let mut rt = TestEnv::new();
    let (w1, _t1, t2) = setup_prefill(&mut rt);
    let up = WorkerTaskUpdate::RejectRequest {
        task_id: t2,
        rv_id: Some(0.into()),
    };
    let mut comm = TestComm::new();
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t2).is_waiting());
    assert!(!rt.worker(w1).blocked_requests.is_empty());
    rt.sanity_check();
}

#[test]
fn test_prefill_failed() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    let up1 = WorkerTaskUpdate::Finished { task_id: t1 };
    let up2 = WorkerTaskUpdate::Failed {
        task_id: t2,
        info: TaskFailInfo::from_string("".to_string()),
    };
    on_task_update(rt.core(), &mut comm, w1, smallvec![up1, up2]);
    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    assert_eq!(comm.client.take_task_errors(1)[0].0, t2);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.task_exists(t1));
    assert!(!rt.task_exists(t2));
    assert!(rt.worker(w1).is_free());
    assert!(
        rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .prefilled_tasks
            .is_empty()
    );
    rt.sanity_check();
}

#[test]
fn test_prefill_cancel() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    on_cancel_tasks(rt.core(), &mut comm, &[t2]);
    let msgs = comm.take_worker_msgs(w1, 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![t2])
    );
    comm.emptiness_check();
    assert!(rt.task_exists(t1));
    assert!(!rt.task_exists(t2));
    rt.sanity_check();
}

fn setup_retracting(rt: &mut TestEnv) -> (WorkerId, WorkerId, TaskId) {
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 1,
        proactive_filling_max: 1,
        ..Default::default()
    });
    let tasks = rt.new_tasks(3, &TaskBuilder::new());
    let w1 = rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    let w2 = rt.new_worker(&WorkerBuilder::new(2));
    rt.schedule();
    let t = tasks.iter().find(|t| rt.task(**t).is_retracting()).unwrap();
    (w1, w2, *t)
}

#[test]
fn test_steal_finished() {
    let mut rt = TestEnv::new();
    let (w1, _w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(t)]);
    assert_eq!(comm.client.take_task_finished(1), vec![t]);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.task_exists(t));
    rt.sanity_check();
}

#[test]
fn test_steal_running() {
    let mut rt = TestEnv::new();
    let (w1, _w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    let up1 = task_finished(
        *rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .assigned_tasks
            .iter()
            .next()
            .unwrap(),
    );
    let up2 = WorkerTaskUpdate::Running(task_running_msg(t));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up1, up2]);
    assert_eq!(comm.client.take_task_running(1), vec![t]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    match &rt.task(t).state {
        TaskRuntimeState::Running { worker_id, .. } => {
            assert_eq!(*worker_id, w1);
        }
        _ => panic!(),
    }
    rt.sanity_check();
}

#[test]
fn test_steal_failed() {
    let mut rt = TestEnv::new();
    let (w1, _w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    let up1 = task_finished(
        *rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .assigned_tasks
            .iter()
            .next()
            .unwrap(),
    );
    let up2 = WorkerTaskUpdate::Failed {
        task_id: t,
        info: TaskFailInfo::from_string("".to_string()),
    };
    on_task_update(rt.core(), &mut comm, w1, smallvec![up1, up2]);
    comm.client.take_task_finished(1);
    assert_eq!(comm.client.take_task_errors(1)[0].0, t);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.task_exists(t));
    rt.sanity_check();
}

#[test]
fn test_steal_cancel() {
    let mut rt = TestEnv::new();
    let (w1, _w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    on_cancel_tasks(rt.core(), &mut comm, &[t]);
    match &comm.take_worker_msgs(w1, 1)[0] {
        ToWorkerMessage::CancelTasks(msg) => {
            assert_eq!(msg.ids, vec![t]);
        }
        _ => panic!(),
    }
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.task_exists(t));
    rt.sanity_check();
}

#[test]
fn test_steal_source_worker_lost() {
    let mut rt = TestEnv::new();
    let (w1, w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, w1, LostWorkerReason::ConnectionLost);
    comm.check_need_scheduling();
    comm.take_broadcasts(1);
    match &comm.take_worker_msgs(w2, 1)[0] {
        ToWorkerMessage::ComputeTasks(_) => {}
        _ => panic!(),
    }
    comm.client.take_lost_workers();
    comm.emptiness_check();
    match &rt.task(t).state {
        TaskRuntimeState::Assigned { worker_id, .. } => {
            assert_eq!(*worker_id, w2);
        }
        _ => panic!(),
    }
    rt.sanity_check();
}

#[test]
fn test_steal_rejected() {
    let mut rt = TestEnv::new();
    let (w1, w2, t) = setup_retracting(&mut rt);
    let up = WorkerTaskUpdate::RejectRequest {
        task_id: t,
        rv_id: Some(0.into()),
    };
    let mut comm = TestComm::new();
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    comm.take_worker_msgs(w2, 1);
    comm.emptiness_check();
    match &rt.task(t).state {
        TaskRuntimeState::Assigned { worker_id, .. } => {
            assert_eq!(*worker_id, w2);
        }
        _ => panic!(),
    }
    assert!(!rt.worker(w1).blocked_requests.is_empty());
    rt.sanity_check();
}

#[test]
fn test_steal_target_worker_lost() {
    let mut rt = TestEnv::new();
    let (w1, w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, w2, LostWorkerReason::ConnectionLost);
    comm.check_need_scheduling();
    comm.take_broadcasts(1);
    comm.client.take_lost_workers();
    comm.emptiness_check();
    match &rt.task(t).state {
        TaskRuntimeState::Retracting { worker_id, .. } => {
            assert_eq!(*worker_id, w1);
        }
        _ => panic!(),
    }
    assert!(rt.core().split().scheduler_state.redirects.is_empty());
    rt.sanity_check();
}

fn drain(rt: &mut TestEnv, comm: &mut TestComm, worker_id: WorkerId, stop_when_idle: bool) {
    let mode = DrainMode { stop_when_idle };
    on_set_worker_drain(rt.core(), comm, &[worker_id], Some(mode));
    assert_eq!(
        comm.client.take_drain_changes(1),
        vec![(worker_id, Some(mode))]
    );
}

#[test]
fn test_drain_worker_blocks_new_tasks() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(1);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    comm.check_need_scheduling();
    comm.emptiness_check();

    let t1 = rt.new_task_default();
    rt.schedule();
    assert!(rt.task(t1).is_waiting());

    let w2 = rt.new_worker_cpus(1);
    rt.schedule();
    assert!(matches!(
        rt.task(t1).state,
        TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w2
    ));

    let t2 = rt.new_task_default();
    on_set_worker_drain(rt.core(), &mut comm, &[w1], None);
    assert_eq!(comm.client.take_drain_changes(1), vec![(w1, None)]);
    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.schedule();
    assert!(matches!(
        rt.task(t2).state,
        TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w1
    ));
    rt.sanity_check();
}

#[test]
fn test_drain_worker_keeps_running_tasks() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(2);
    let t1 = rt.new_task_running(&TaskBuilder::new(), w1);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t1).is_sn_running());

    rt.finish_task(t1, w1);
    assert!(!rt.worker(w1).is_stopping());
    rt.sanity_check();
}

#[test]
fn test_drain_worker_retracts_prefilled() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    comm.check_need_scheduling();
    match &comm.take_worker_msgs(w1, 1)[0] {
        ToWorkerMessage::RetractTasks(ts) => assert_eq!(ts.ids, vec![t2]),
        _ => panic!("Invalid worker msg"),
    }
    comm.emptiness_check();
    assert!(rt.task(t1).is_assigned());
    assert!(rt.task(t2).is_retracting());
    assert!(
        rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .prefilled_tasks
            .is_empty()
    );
    rt.sanity_check();

    // Retracted task is moved to another worker
    let w2 = rt.new_worker_cpus(1);
    rt.schedule();
    on_retract_response(rt.core(), &mut comm, w1, &[t2]);
    assert!(matches!(
        rt.task(t2).state,
        TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w2
    ));
    rt.sanity_check();
}

#[test]
fn test_drain_worker_retract_without_redirect() {
    let mut rt = TestEnv::new();
    let (w1, _t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    on_retract_response(rt.core(), &mut comm, w1, &[t2]);
    assert!(rt.task(t2).is_waiting());
    rt.schedule();
    assert!(rt.task(t2).is_waiting());
    rt.sanity_check();
}

#[test]
fn test_drain_worker_prefilled_started_before_retract() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    comm.take_worker_msgs(w1, 1);
    let up1 = WorkerTaskUpdate::Finished { task_id: t1 };
    let up2 = WorkerTaskUpdate::RunningPrefilled(task_running_msg(t2));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up1, up2]);
    assert!(rt.task(t2).is_sn_running());
    assert!(!rt.worker(w1).is_stopping());
    rt.sanity_check();
}

#[test]
fn test_drain_idle_worker_stop_when_idle() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(1);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    comm.check_need_scheduling();
    assert!(matches!(
        comm.take_worker_msgs(w1, 1)[0],
        ToWorkerMessage::Stop
    ));
    comm.emptiness_check();
    assert!(rt.worker(w1).is_stopping());
}

#[test]
fn test_drain_worker_stop_when_idle_after_finish() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(2);
    let t1 = rt.new_task_running(&TaskBuilder::new(), w1);
    let t2 = rt.new_task_running(&TaskBuilder::new(), w1);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    comm.check_need_scheduling();
    comm.emptiness_check();

    let up = WorkerTaskUpdate::Finished { task_id: t1 };
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.worker(w1).is_stopping());

    let up = WorkerTaskUpdate::Finished { task_id: t2 };
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    assert!(matches!(
        comm.take_worker_msgs(w1, 1)[0],
        ToWorkerMessage::Stop
    ));
    comm.emptiness_check();
    assert!(rt.worker(w1).is_stopping());
}

fn assert_stopped_by_drain(rt: &mut TestEnv, comm: &mut TestComm, worker_id: WorkerId) {
    assert!(matches!(
        comm.take_worker_msgs(worker_id, 1)[0],
        ToWorkerMessage::Stop
    ));
    assert!(matches!(
        rt.worker(worker_id).stop_reason,
        Some((LostWorkerReason::Drained, _))
    ));
}

#[test]
fn test_drain_stop_when_idle_waits_for_retract_started_task() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    comm.check_need_scheduling();
    comm.take_worker_msgs(w1, 1);
    comm.emptiness_check();

    // The worker has nothing assigned, but t2 may still be started by the worker
    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(t1)]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.worker(w1).is_stopping());

    // The worker has started t2 before it received the retract
    let up = WorkerTaskUpdate::RunningPrefilled(task_running_msg(t2));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    assert!(rt.task(t2).is_sn_running());
    assert!(!rt.worker(w1).is_stopping());
    comm.check_need_scheduling();
    comm.client.take_task_running(1);
    comm.emptiness_check();

    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(t2)]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    assert_stopped_by_drain(&mut rt, &mut comm, w1);
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_drain_stop_when_idle_after_retract_response() {
    let mut rt = TestEnv::new();
    let (w1, t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    comm.check_need_scheduling();
    comm.take_worker_msgs(w1, 1);

    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(t1)]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.worker(w1).is_stopping());

    on_retract_response(rt.core(), &mut comm, w1, &[t2]);
    assert!(rt.task(t2).is_waiting());
    assert_stopped_by_drain(&mut rt, &mut comm, w1);
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_drain_stop_when_idle_waits_for_scheduler_retract() {
    let mut rt = TestEnv::new();
    // The scheduler has already started to retract `t` from w1 before the drain
    let (w1, w2, t) = setup_retracting(&mut rt);
    let assigned = rt
        .core()
        .split()
        .task_map
        .tasks()
        .find(|task| matches!(task.state, TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w1))
        .unwrap()
        .id;
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    comm.check_need_scheduling();
    comm.emptiness_check();

    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(assigned)]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.worker(w1).is_stopping());

    on_retract_response(rt.core(), &mut comm, w1, &[t]);
    comm.take_worker_msgs(w2, 1);
    assert_stopped_by_drain(&mut rt, &mut comm, w1);
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_drain_cancels_redirect_to_drained_worker() {
    let mut rt = TestEnv::new();
    // `t` is being retracted from w1 and it is redirected to w2
    let (w1, w2, t) = setup_retracting(&mut rt);
    let assigned = rt
        .core()
        .split()
        .task_map
        .tasks()
        .find(|task| matches!(task.state, TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w2))
        .unwrap()
        .id;
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w2, true);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.core().split().scheduler_state.redirects.is_empty());
    assert!(
        !rt.worker(w2)
            .sn_assignment()
            .unwrap()
            .assigned_tasks
            .contains(&t)
    );
    rt.sanity_check();

    // The finished retract must not send the task to the drained worker
    on_retract_response(rt.core(), &mut comm, w1, &[t]);
    comm.emptiness_check();
    assert!(rt.task(t).is_waiting());
    rt.sanity_check();

    on_task_update(rt.core(), &mut comm, w2, smallvec![task_finished(assigned)]);
    comm.client.take_task_finished(1);
    comm.check_need_scheduling();
    assert_stopped_by_drain(&mut rt, &mut comm, w2);
    comm.emptiness_check();
    rt.sanity_check();
}

fn setup_mn_task(rt: &mut TestEnv) -> (WorkerId, WorkerId, TaskId) {
    rt.new_workers_cpus(&[1, 1]);
    let t = rt.new_task(&TaskBuilder::new().n_nodes(2));
    rt.schedule();
    let ws = rt.task(t).mn_placement().unwrap().to_vec();
    (ws[0], ws[1], t)
}

#[test]
fn test_drain_stop_when_idle_mn_task_finished() {
    let mut rt = TestEnv::new();
    let (root, non_root, t) = setup_mn_task(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, non_root, true);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(!rt.worker(non_root).is_stopping());

    on_task_update(rt.core(), &mut comm, root, smallvec![task_finished(t)]);
    comm.client.take_task_finished(1);
    assert_stopped_by_drain(&mut rt, &mut comm, non_root);
    assert!(!rt.worker(root).is_stopping());
    rt.sanity_check();
}

#[test]
fn test_drain_stop_when_idle_mn_task_canceled() {
    let mut rt = TestEnv::new();
    let (root, non_root, t) = setup_mn_task(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, non_root, true);
    on_cancel_tasks(rt.core(), &mut comm, &[t]);
    assert!(matches!(
        comm.take_worker_msgs(root, 1)[0],
        ToWorkerMessage::CancelTasks(_)
    ));
    assert_stopped_by_drain(&mut rt, &mut comm, non_root);
    rt.sanity_check();
}

#[test]
fn test_drain_stop_when_idle_mn_root_lost() {
    let mut rt = TestEnv::new();
    let (root, non_root, _t) = setup_mn_task(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, non_root, true);
    on_remove_worker(rt.core(), &mut comm, root, LostWorkerReason::ConnectionLost);
    assert_stopped_by_drain(&mut rt, &mut comm, non_root);
    rt.sanity_check();
}

#[test]
fn test_drain_stop_when_idle_task_canceled() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(1);
    let t = rt.new_task(&TaskBuilder::new());
    rt.schedule();
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, true);
    assert!(!rt.worker(w1).is_stopping());

    on_cancel_tasks(rt.core(), &mut comm, &[t]);
    // The worker has to learn about the cancellation before it is stopped
    let msgs = comm.take_worker_msgs(w1, 2);
    assert!(matches!(msgs[0], ToWorkerMessage::CancelTasks(_)));
    assert!(matches!(msgs[1], ToWorkerMessage::Stop));
    assert!(matches!(
        rt.worker(w1).stop_reason,
        Some((LostWorkerReason::Drained, _))
    ));
    rt.sanity_check();
}

#[test]
fn test_plain_drain_does_not_track_retracts() {
    let mut rt = TestEnv::new();
    let (w1, _w2, _t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    assert!(rt.worker(w1).drain_retracting.is_empty());
    rt.sanity_check();
}

#[test]
fn test_drain_more_workers_at_once() {
    let mut rt = TestEnv::new();
    // `t` is being retracted from w1 and it is redirected to w2
    let (w1, w2, t) = setup_retracting(&mut rt);
    let mut comm = TestComm::new();
    let mode = DrainMode {
        stop_when_idle: true,
    };
    on_set_worker_drain(rt.core(), &mut comm, &[w1, w2], Some(mode));
    comm.client.take_drain_changes(2);
    assert_eq!(rt.worker(w1).drain_retracting, vec![t]);
    assert!(rt.worker(w2).drain_retracting.is_empty());
    assert!(rt.core().split().scheduler_state.redirects.is_empty());
    rt.sanity_check();
}

#[test]
fn test_drain_duplicate_worker_ids() {
    let mut rt = TestEnv::new();
    let (w1, _t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    let mode = DrainMode {
        stop_when_idle: false,
    };
    on_set_worker_drain(rt.core(), &mut comm, &[w1, w1], Some(mode));
    assert_eq!(comm.client.take_drain_changes(1), vec![(w1, Some(mode))]);
    assert!(rt.task(t2).is_retracting());
    rt.sanity_check();
}

/// Prefilled task is retracted by a drain and the worker is undrained before the retract response
/// comes. The scheduler then places the still retracting task on its original worker.
fn setup_retract_to_same_worker(rt: &mut TestEnv) -> (WorkerId, TaskId) {
    rt.set_scheduler_config(SchedulerConfig {
        proactive_filling_reserve: 0,
        proactive_filling_max: 1,
        ..Default::default()
    });
    let tasks = rt.new_tasks(2, &TaskBuilder::new());
    let w1 = rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    let assigned = get_assigned(rt, &tasks).unwrap();
    let t = get_prefilled(rt, &tasks).unwrap();

    let mut comm = TestComm::new();
    drain(rt, &mut comm, w1, false);
    comm.take_worker_msgs(w1, 1);
    on_task_update(rt.core(), &mut comm, w1, smallvec![task_finished(assigned)]);
    comm.client.take_task_finished(1);
    on_set_worker_drain(rt.core(), &mut comm, &[w1], None);
    comm.client.take_drain_changes(1);
    assert!(rt.task(t).is_retracting());

    rt.schedule().emptiness_check();
    assert!(
        rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .assigned_tasks
            .contains(&t)
    );
    (w1, t)
}

#[test]
fn test_retract_to_same_worker_records_redirect() {
    let mut rt = TestEnv::new();
    let (w1, t) = setup_retract_to_same_worker(&mut rt);
    assert_eq!(
        rt.core()
            .split()
            .scheduler_state
            .redirects
            .get(&t)
            .map(|r| r.0),
        Some(w1)
    );

    let mut comm = TestComm::new();
    on_retract_response(rt.core(), &mut comm, w1, &[t]);
    match &comm.take_worker_msgs(w1, 1)[0] {
        ToWorkerMessage::ComputeTasks(msg) => {
            assert_eq!(msg.tasks.len(), 1);
            assert_eq!(msg.tasks[0].id, t);
        }
        _ => panic!("Invalid worker msg"),
    }
    comm.emptiness_check();
    assert!(matches!(
        rt.task(t).state,
        TaskRuntimeState::Assigned { worker_id, .. } if worker_id == w1
    ));
    assert!(rt.core().split().scheduler_state.redirects.is_empty());
    rt.sanity_check();
}

#[test]
fn test_retract_to_same_worker_lost() {
    let mut rt = TestEnv::new();
    let (w1, t) = setup_retract_to_same_worker(&mut rt);
    let mut comm = TestComm::new();
    on_remove_worker(rt.core(), &mut comm, w1, LostWorkerReason::ConnectionLost);
    assert!(rt.task(t).is_waiting());
    assert!(rt.core().split().scheduler_state.redirects.is_empty());
    rt.sanity_check();
}

#[test]
fn test_retract_to_same_worker_drained_again() {
    let mut rt = TestEnv::new();
    let (w1, t) = setup_retract_to_same_worker(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.sanity_check();

    on_retract_response(rt.core(), &mut comm, w1, &[t]);
    comm.emptiness_check();
    assert!(rt.task(t).is_waiting());
    rt.sanity_check();
}

#[test]
fn test_retract_to_same_worker_task_started() {
    let mut rt = TestEnv::new();
    let (w1, t) = setup_retract_to_same_worker(&mut rt);
    let mut comm = TestComm::new();
    let up = WorkerTaskUpdate::RunningPrefilled(task_running_msg(t));
    on_task_update(rt.core(), &mut comm, w1, smallvec![up]);
    assert!(rt.task(t).is_sn_running());
    assert!(rt.core().split().scheduler_state.redirects.is_empty());
    rt.sanity_check();
}

#[test]
fn test_drain_change_events() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(1);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    comm.check_need_scheduling();

    // Draining again with the same setting does not produce an event
    let mode = DrainMode {
        stop_when_idle: false,
    };
    on_set_worker_drain(rt.core(), &mut comm, &[w1], Some(mode));
    comm.check_need_scheduling();
    comm.emptiness_check();

    on_set_worker_drain(rt.core(), &mut comm, &[w1], None);
    assert_eq!(comm.client.take_drain_changes(1), vec![(w1, None)]);
    comm.check_need_scheduling();

    // Undraining a worker that is not drained does not produce an event
    on_set_worker_drain(rt.core(), &mut comm, &[w1], None);
    comm.check_need_scheduling();
    comm.emptiness_check();
}

#[test]
fn test_drain_worker_not_prefilled_again() {
    let mut rt = TestEnv::new();
    let (w1, _t1, t2) = setup_prefill(&mut rt);
    let mut comm = TestComm::new();
    drain(&mut rt, &mut comm, w1, false);
    on_retract_response(rt.core(), &mut comm, w1, &[t2]);
    rt.schedule();
    assert!(
        rt.worker(w1)
            .sn_assignment()
            .unwrap()
            .prefilled_tasks
            .is_empty()
    );
    rt.sanity_check();
}
