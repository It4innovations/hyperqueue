use std::time::{Duration, Instant};

use crate::datasrv::DataObjectId;
use crate::gateway::LostWorkerReason;
use crate::internal::common::resources::ResourceDescriptor;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    ComputeTasksMsg, NewWorkerMsg, TaskFinishedMsg, TaskIdsMsg, TaskOutput, ToWorkerMessage,
};
use crate::internal::messages::worker::{StealResponse, StealResponseMsg};
use crate::internal::scheduler::state::SchedulerState;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::{
    on_cancel_tasks, on_new_tasks, on_new_worker, on_remove_worker, on_steal_response,
    on_task_error, on_task_finished, on_task_running,
};
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::Worker;
use crate::internal::tests::utils::env::create_test_comm;
use crate::internal::tests::utils::schedule::{
    assign_to_worker, create_test_scheduler, finish_on_worker, force_assign, set_as_running,
    start_and_finish_on_worker, start_mn_task_on_worker, start_on_worker_running,
};
use crate::internal::tests::utils::shared::{res_kind_groups, res_kind_sum};
use crate::internal::tests::utils::sorted_vec;
use crate::internal::tests::utils::task::{TaskBuilder, task_running_msg};
use crate::internal::tests::utils::workflows::{submit_example_1, submit_example_3};
use crate::internal::worker::configuration::{
    DEFAULT_MAX_DOWNLOAD_TRIES, DEFAULT_MAX_PARALLEL_DOWNLOADS,
    DEFAULT_WAIT_BETWEEN_DOWNLOAD_TRIES, OverviewConfiguration,
};
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceIdMap};
use crate::tests::utils::env::TestEnv;
use crate::tests::utils::worker::WorkerBuilder;
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{Priority, TaskId, WorkerId};

#[test]
fn test_worker_add() {
    let mut core = Core::default();
    assert_eq!(core.get_workers().count(), 0);

    let mut comm = create_test_comm();
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
        on_server_lost: ServerLostPolicy::Stop,
        max_parallel_downloads: DEFAULT_MAX_PARALLEL_DOWNLOADS,
        max_download_tries: DEFAULT_MAX_DOWNLOAD_TRIES,
        wait_between_download_tries: DEFAULT_WAIT_BETWEEN_DOWNLOAD_TRIES,
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
        on_server_lost: ServerLostPolicy::Stop,
        max_parallel_downloads: DEFAULT_MAX_PARALLEL_DOWNLOADS,
        max_download_tries: DEFAULT_MAX_DOWNLOAD_TRIES,
        wait_between_download_tries: DEFAULT_WAIT_BETWEEN_DOWNLOAD_TRIES,
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
fn test_scheduler_priority() {
    let mut rt = TestEnv::new();
    //new_workers(&mut core, &mut comm, vec![1]);

    let t = TaskBuilder::new();

    let t1 = rt.new_task(&t);
    let _t2 = rt.new_task(&TaskBuilder::new().task_deps(&[t1]));
    let t3 = rt.new_task(&t);
    let _t4 = rt.new_task(&TaskBuilder::new().task_deps(&[t3]));

    rt.set_job(123, 1);
    rt.new_task_default();
    rt.set_job(122, 0);
    let t5 = rt.new_task_default();

    rt.set_job(123, 2);
    rt.new_task(&TaskBuilder::new().task_deps(&[t5]));
    rt.set_job(123, 4);
    let t8 = rt.new_task_default();

    assert_eq!(
        rt.task(t1).priority(),
        Priority::from_user_priority(0.into()).add_inverted_priority_u32(1)
    );
    assert_eq!(
        rt.task(t8).priority(),
        Priority::from_user_priority(0.into()).add_inverted_priority_u32(123)
    );
}

#[test]
fn test_submit_jobs() {
    let mut rt = TestEnv::new();
    let mut comm = create_test_comm();

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

    let rmap = rt.core().resource_map_mut();
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

fn no_data_task_finished(task_id: TaskId) -> TaskFinishedMsg {
    TaskFinishedMsg {
        id: task_id,
        outputs: vec![],
    }
}

#[test]
fn test_assignments_and_finish() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);

    /*
       t1   t2    t4  t5
        \   /     |
          t3[k]   t7[k]
    */

    let t1 = rt.new_task(&TaskBuilder::new().user_priority(12));
    let t2 = rt.new_task_default();
    let t3 = rt.new_task(&TaskBuilder::new().task_deps(&[t1, t2]));
    let t4 = rt.new_task_default();
    let t5 = rt.new_task_default();
    let t7 = rt.new_task(&TaskBuilder::new().task_deps(&[t4]));

    let mut comm = create_test_comm();

    let mut scheduler = create_test_scheduler();

    force_assign(rt.core(), &mut scheduler, t1, ws[0]);
    force_assign(rt.core(), &mut scheduler, t2, ws[1]);
    force_assign(rt.core(), &mut scheduler, t5, ws[0]);

    rt.core().assert_fresh(&[t1, t3]);

    scheduler.finish_scheduling(rt.core(), &mut comm);

    rt.core().assert_not_fresh(&[t1]);
    rt.core().assert_fresh(&[t3]);

    check_worker_tasks_exact(rt.core(), ws[0], &[t1, t5]);
    check_worker_tasks_exact(rt.core(), ws[1], &[t2]);
    check_worker_tasks_exact(rt.core(), ws[2], &[]);

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

    // FINISH TASK WITHOUT CONSUMERS & KEEP FLAG
    on_task_finished(rt.core(), &mut comm, ws[0], no_data_task_finished(t5));

    assert!(rt.core().find_task(t5.into()).is_none());
    check_worker_tasks_exact(rt.core(), ws[0], &[t1]);
    check_worker_tasks_exact(rt.core(), ws[1], &[t2]);
    check_worker_tasks_exact(rt.core(), ws[2], &[]);

    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t5);
    comm.emptiness_check();

    assert!(!rt.task_exists(t5));

    // FINISHED TASK WITH CONSUMERS
    assert!(rt.task(t2).is_assigned());

    on_task_finished(rt.core(), &mut comm, ws[1], no_data_task_finished(t2));

    assert!(!rt.task_exists(t2));
    check_worker_tasks_exact(rt.core(), ws[0], &[t1]);
    check_worker_tasks_exact(rt.core(), ws[1], &[]);
    check_worker_tasks_exact(rt.core(), ws[2], &[]);

    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t2);
    comm.emptiness_check();

    on_task_finished(rt.core(), &mut comm, ws[0], no_data_task_finished(t1));

    comm.check_need_scheduling();

    force_assign(rt.core(), &mut scheduler, t3, ws[1]);
    scheduler.finish_scheduling(rt.core(), &mut comm);

    let msgs = comm.take_worker_msgs(ws[1], 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTasks(ComputeTasksMsg {
            tasks,
            ..
        }) if tasks[0].id == t3
    ));

    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    comm.emptiness_check();
    rt.sanity_check();

    on_task_finished(rt.core(), &mut comm, ws[1], no_data_task_finished(t3));

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
    start_and_finish_on_worker(rt.core(), wf[0], ws[0]);
    start_and_finish_on_worker(rt.core(), wf[1], ws[1]);

    assign_to_worker(rt.core(), wf[2], ws[2]);
    rt.core().assert_assigned(&[wf[2]]);
    assert!(worker_has_task(rt.core(), ws[2], wf[2]));

    let mut comm = create_test_comm();
    on_task_error(
        rt.core(),
        &mut comm,
        ws[2],
        wf[2],
        TaskFailInfo {
            message: "".to_string(),
        },
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
fn test_steal_tasks_ok() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let wf = submit_example_1(&mut rt);
    start_and_finish_on_worker(rt.core(), wf[0], ws[0]);
    start_and_finish_on_worker(rt.core(), wf[1], ws[1]);

    let task_id = wf[2];
    assign_to_worker(rt.core(), task_id, ws[1]);

    assert!(worker_has_task(rt.core(), ws[1], task_id));
    assert!(!worker_has_task(rt.core(), ws[0], task_id));

    let mut comm = create_test_comm();
    let mut scheduler = create_test_scheduler();

    force_reassign(rt.core(), &mut scheduler, task_id, ws[0]);
    scheduler.finish_scheduling(rt.core(), &mut comm);

    assert!(!worker_has_task(rt.core(), ws[1], task_id));
    assert!(worker_has_task(rt.core(), ws[0], task_id));

    let msgs = comm.take_worker_msgs(ws[1], 1);
    assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![task_id]));
    comm.emptiness_check();

    on_steal_response(
        rt.core(),
        &mut comm,
        ws[1],
        StealResponseMsg {
            responses: vec![
                (task_id.into(), StealResponse::Ok),
                (123.into(), StealResponse::NotHere),
                (11.into(), StealResponse::NotHere),
            ],
        },
    );

    assert!(!worker_has_task(rt.core(), ws[1], task_id));
    assert!(worker_has_task(rt.core(), ws[0], task_id));

    let msgs = comm.take_worker_msgs(ws[0], 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTasks(ComputeTasksMsg { tasks, .. })
        if tasks[0].id == wf[2]
    ));
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_steal_tasks_running() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let wf = submit_example_1(&mut rt);
    start_and_finish_on_worker(rt.core(), wf[0], ws[0]);
    start_and_finish_on_worker(rt.core(), wf[1], ws[1]);
    assign_to_worker(rt.core(), wf[2], ws[1]);

    let mut comm = create_test_comm();
    let mut scheduler = create_test_scheduler();

    force_reassign(rt.core(), &mut scheduler, wf[2], ws[0]);
    scheduler.finish_scheduling(rt.core(), &mut comm);

    let msgs = comm.take_worker_msgs(ws[1], 1);
    assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![wf[2]]));
    comm.emptiness_check();

    assert!(!worker_has_task(rt.core(), ws[1], wf[2]));
    assert!(worker_has_task(rt.core(), ws[0], wf[2]));

    on_task_running(rt.core(), &mut comm, ws[1], task_running_msg(wf[2]));
    comm.client.take_task_running(1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.sanity_check();

    on_steal_response(
        rt.core(),
        &mut comm,
        ws[1],
        StealResponseMsg {
            responses: vec![(wf[2], StealResponse::Running)],
        },
    );

    assert!(worker_has_task(rt.core(), ws[1], wf[2]));
    assert!(!worker_has_task(rt.core(), ws[0], wf[2]));
    rt.sanity_check();
    comm.emptiness_check();
}

#[test]
#[should_panic]
fn finish_unassigned_task() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_workers_cpus(&[1, 1, 1]);
    let wf = submit_example_1(&mut rt);
    finish_on_worker(rt.core(), wf[0], w1[0]);
}

#[test]
fn finish_task_without_outputs() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(1);
    let t1 = rt.new_task_assigned(&TaskBuilder::new(), w1);

    let mut comm = create_test_comm();
    on_task_finished(rt.core(), &mut comm, w1, no_data_task_finished(t1));
    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    comm.emptiness_check();
    rt.sanity_check();
}

#[test]
fn test_task_cancel() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let wf = submit_example_1(&mut rt);

    let t1 = rt.new_task_default();
    let t2 = rt.new_task_default();
    let t3 = rt.new_task_default();

    assign_to_worker(rt.core(), wf[0], ws[1]);
    assign_to_worker(rt.core(), wf[1], ws[1]);
    assign_to_worker(rt.core(), t1, ws[1]);
    assign_to_worker(rt.core(), t2, ws[0]);

    start_stealing(rt.core(), wf[1], ws[0]);
    set_as_running(rt.core(), wf[1], ws[1]);
    fail_steal(rt.core(), wf[1], ws[1]);
    start_stealing(rt.core(), t1, ws[0]);
    start_stealing(rt.core(), t2, ws[1]);

    let mut comm = create_test_comm();
    on_cancel_tasks(rt.core(), &mut comm, &vec![wf[0], wf[1], t1, t2, wf[2]]);

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
    start_mn_task_on_worker(rt.core(), t1, vec![ws[3], ws[1], ws[0]]);
    let mut comm = create_test_comm();
    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);
    rt.sanity_check();
    assert_eq!(comm.client.take_lost_workers().len(), 1);
    let msgs = comm.take_worker_msgs(ws[3], 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![t1])
    );
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w == ws[1]));
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.task(t1).is_waiting());
}

#[test]
fn test_worker_lost_with_mn_task_root() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));

    start_mn_task_on_worker(rt.core(), t1, vec![ws[3], ws[1], ws[0]]);
    let mut comm = create_test_comm();
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
        let mut comm = create_test_comm();
        let worker_id = rt.new_worker(&WorkerBuilder::new(1));
        start_on_worker_running(rt.core(), t1, worker_id);
        on_remove_worker(
            rt.core(),
            &mut comm,
            worker_id.into(),
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
    start_mn_task_on_worker(rt.core(), t1, vec![ws[3], ws[1], ws[0]]);
    let mut comm = create_test_comm();
    on_task_error(
        rt.core(),
        &mut comm,
        ws[3],
        t1,
        TaskFailInfo {
            message: "".to_string(),
        },
    );
    rt.sanity_check();
    let msgs = comm.client.take_task_errors(1);
    assert_eq!(msgs[0].0, t1);
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(rt.core().find_task(1.into()).is_none());
    for w in &ws {
        assert!(
            rt.core()
                .get_worker_by_id_or_panic((*w).into())
                .mn_task()
                .is_none()
        );
    }
}

#[test]
fn test_task_mn_cancel() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1, 1]);
    let t1 = rt.new_task(&TaskBuilder::new().n_nodes(3));
    start_mn_task_on_worker(rt.core(), t1, vec![ws[3], ws[1], ws[0]]);
    let mut comm = create_test_comm();
    on_cancel_tasks(rt.core(), &mut comm, &[t1]);
    rt.sanity_check();
    let msgs = comm.take_worker_msgs(ws[3], 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![t1])
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(rt.core(), &mut comm);
    rt.sanity_check();
    comm.emptiness_check();

    assert!(!rt.task_exists(t1));
    for w in rt.core().get_workers() {
        assert!(w.mn_task.is_none());
    }
}

#[test]
fn test_running_task() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let t1 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);
    let t2 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);

    let mut comm = create_test_comm();

    comm.emptiness_check();

    on_task_running(rt.core(), &mut comm, ws[1], task_running_msg(t1));
    assert_eq!(comm.client.take_task_running(1), vec![t1]);
    comm.emptiness_check();

    on_task_running(rt.core(), &mut comm, ws[1], task_running_msg(t2));
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
fn test_finished_before_steal_response() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let t1 = rt.new_task_default();
    assign_to_worker(rt.core(), t1, ws[1]);
    start_stealing(rt.core(), t1, ws[2]);
    assert!(worker_has_task(rt.core(), ws[2], t1));

    let mut comm = create_test_comm();
    on_task_finished(rt.core(), &mut comm, ws[1], no_data_task_finished(t1));

    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_finished(1)[0], t1);
    comm.emptiness_check();

    assert!(!worker_has_task(rt.core(), ws[1], t1));
    assert!(!worker_has_task(rt.core(), ws[2], t1));

    on_steal_response(
        rt.core(),
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(1.into(), StealResponse::NotHere)],
        },
    );

    comm.emptiness_check();

    assert!(!worker_has_task(rt.core(), ws[1], t1));
    assert!(!worker_has_task(rt.core(), ws[2], t1));
}

#[test]
fn test_running_before_steal_response() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let t1 = rt.new_task_default();
    assign_to_worker(rt.core(), t1, ws[1]);
    start_stealing(rt.core(), t1, ws[2]);
    assert!(worker_has_task(rt.core(), ws[2], t1));

    let mut comm = create_test_comm();
    on_task_running(rt.core(), &mut comm, ws[1], task_running_msg(t1));
    comm.check_need_scheduling();
    assert_eq!(comm.client.take_task_running(1)[0], t1);
    comm.emptiness_check();

    assert!(worker_has_task(rt.core(), ws[1], t1));
    assert!(!worker_has_task(rt.core(), ws[2], t1));

    on_steal_response(
        rt.core(),
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(1.into(), StealResponse::Running)],
        },
    );

    comm.emptiness_check();
    assert!(worker_has_task(rt.core(), ws[1], t1));
    assert!(!worker_has_task(rt.core(), ws[2], t1));
}

fn test_after_cancel_messages() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let t1 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);
    let t2 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);
    let t3 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);
    let t4 = rt.new_task_assigned(&TaskBuilder::new(), ws[1]);
    cancel_tasks(rt.core(), &[t1, t2, t3, t4]);

    let mut comm = create_test_comm();
    on_task_finished(rt.core(), &mut comm, ws[1], no_data_task_finished(t1));
    comm.emptiness_check();

    on_steal_response(
        rt.core(),
        &mut comm,
        ws[1],
        StealResponseMsg {
            responses: vec![(t2, StealResponse::Ok)],
        },
    );
    comm.emptiness_check();

    on_steal_response(
        rt.core(),
        &mut comm,
        ws[1],
        StealResponseMsg {
            responses: vec![(t2, StealResponse::Running)],
        },
    );
    comm.emptiness_check();

    on_task_error(
        rt.core(),
        &mut comm,
        ws[1],
        t3,
        TaskFailInfo {
            message: "".to_string(),
        },
    );
    comm.emptiness_check();

    on_task_running(rt.core(), &mut comm, ws[1], task_running_msg(t4));
    comm.emptiness_check();
}

#[test]
fn lost_worker_with_running_and_assign_tasks() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1, 1]);
    let wf = submit_example_1(&mut rt);

    let t1 = rt.new_task_default();
    let t2 = rt.new_task_default();

    assign_to_worker(rt.core(), wf[0], ws[1]);
    assign_to_worker(rt.core(), wf[1], ws[1]);
    assign_to_worker(rt.core(), t1, ws[1]);
    assign_to_worker(rt.core(), t2, ws[0]);

    start_stealing(rt.core(), wf[1], ws[0]);
    set_as_running(rt.core(), wf[1], ws[1]);
    fail_steal(rt.core(), wf[1], ws[1]);
    start_stealing(rt.core(), t1, ws[0]);
    start_stealing(rt.core(), t2, ws[1]);

    rt.core().assert_running(&[wf[1]]);
    assert_eq!(rt.task(wf[1]).instance_id, 0.into());

    rt.core()
        .assert_task_condition(&[wf[0], wf[1], t1, t2], |t| !t.is_fresh());

    let mut comm = create_test_comm();
    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);

    assert_eq!(comm.client.take_lost_workers(), vec![(ws[1], vec![wf[1]])]);

    rt.core().assert_ready(&[wf[0], wf[1]]);
    assert_eq!(rt.task(wf[1]).instance_id, 1.into());
    assert!(rt.task(t1).is_ready());
    rt.core().assert_ready(&[t1]);
    rt.core().assert_fresh(&[wf[0], wf[1], t1]);
    assert!(matches!(
        rt.task(t2).state,
        TaskRuntimeState::Stealing(w, None) if w == ws[0]
    ));

    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w == ws[1]
    ));

    comm.check_need_scheduling();
    comm.emptiness_check();

    on_steal_response(
        rt.core(),
        &mut comm,
        ws[0],
        StealResponseMsg {
            responses: vec![(t2.into(), StealResponse::Ok)],
        },
    );

    rt.core().assert_ready(&[t2]);
    rt.core().assert_fresh(&[t2]);

    comm.check_need_scheduling();
    comm.emptiness_check();
    rt.sanity_check();
}

fn force_reassign<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    scheduler: &mut SchedulerState,
    task_id: T,
    worker_id: W,
) {
    // The same as force_assign, but do not expect that task in ready_to_assign array
    scheduler.assign(core, task_id.into(), worker_id.into());
}

fn fail_steal<W: Into<WorkerId>, T: Into<TaskId>>(core: &mut Core, task_id: T, worker_id: W) {
    let mut comm = create_test_comm();
    on_steal_response(
        core,
        &mut comm,
        worker_id.into(),
        StealResponseMsg {
            responses: vec![(task_id.into(), StealResponse::Running)],
        },
    )
}

fn start_stealing<W: Into<WorkerId>>(core: &mut Core, task_id: TaskId, new_worker_id: W) {
    let mut scheduler = create_test_scheduler();
    force_reassign(core, &mut scheduler, task_id, new_worker_id.into());
    let mut comm = create_test_comm();
    scheduler.finish_scheduling(core, &mut comm);
}

fn cancel_tasks<T: Into<TaskId> + Copy>(core: &mut Core, task_ids: &[T]) {
    let mut comm = create_test_comm();
    on_cancel_tasks(
        core,
        &mut comm,
        &task_ids.iter().map(|&v| v.into()).collect::<Vec<_>>(),
    );
}

fn check_worker_tasks_exact(core: &Core, worker_id: WorkerId, tasks: &[TaskId]) {
    let worker = core.get_worker_by_id_or_panic(worker_id.into());
    assert_eq!(worker.sn_tasks().len(), tasks.len());
    for task in tasks {
        assert!(worker.sn_tasks().contains(task));
    }
}

fn worker_has_task(core: &Core, worker_id: WorkerId, task_id: TaskId) -> bool {
    core.get_worker_by_id_or_panic(worker_id.into())
        .sn_tasks()
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
    start_and_finish_on_worker(rt.core(), wf[1], w1);
    rt.core().assert_waiting(&[wf[2], wf[3], wf[5]]);
    rt.core().assert_ready(&[wf[4]]);

    start_and_finish_on_worker(rt.core(), wf[0], w1);
    rt.core().assert_waiting(&[wf[5]]);
    rt.core().assert_ready(&[wf[2], wf[3], wf[4]]);
}

#[test]
fn test_worker_groups() {
    let mut rt = TestEnv::new();
    let ws = rt.new_workers_cpus(&[1, 1]);
    let g = rt.core().worker_group("default").unwrap();
    assert_eq!(&sorted_vec(g.worker_ids().collect()), &ws);
    let mut comm = create_test_comm();
    on_remove_worker(rt.core(), &mut comm, ws[1], LostWorkerReason::HeartbeatLost);
    let g = rt.core().worker_group("default").unwrap();
    assert_eq!(sorted_vec(g.worker_ids().collect()), vec![ws[0]]);
    let mut comm = create_test_comm();
    on_remove_worker(rt.core(), &mut comm, ws[0], LostWorkerReason::HeartbeatLost);
    assert!(rt.core().worker_group("default").is_none());
}

#[test]
fn test_data_deps_no_output() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(4);
    let t1 = rt.new_task_default();
    let t2 = rt.new_task(&TaskBuilder::new().data_dep(t1, 11));
    assign_to_worker(rt.core(), t1, w1);
    rt.sanity_check();
    let mut comm = create_test_comm();
    on_task_finished(
        rt.core(),
        &mut comm,
        w1,
        TaskFinishedMsg {
            id: t1,
            outputs: vec![],
        },
    );
    assert_eq!(comm.client.take_task_finished(1), vec![t1]);
    let errors = comm.client.take_task_errors(1);
    assert_eq!(errors[0].0, t2);
    assert_eq!(
        &errors[0].2.message,
        "Task 1@1 did not produced expected output(s): 11"
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
}

#[test]
fn test_data_deps_missing_outputs() {
    let mut rt = TestEnv::new();
    let w1 = rt.new_worker_cpus(4);
    let t1 = rt.new_task_default();
    let t2 = rt.new_task(
        &TaskBuilder::new()
            .data_dep(t1, 10)
            .data_dep(t1, 11)
            .data_dep(t1, 100)
            .data_dep(t1, 101),
    );
    assign_to_worker(rt.core(), t1, w1);
    rt.sanity_check();
    let mut comm = create_test_comm();
    on_task_finished(
        rt.core(),
        &mut comm,
        w1,
        TaskFinishedMsg {
            id: t1,
            outputs: vec![
                TaskOutput {
                    id: 10.into(),
                    size: 500,
                },
                TaskOutput {
                    id: 101.into(),
                    size: 12,
                },
                TaskOutput {
                    id: 405.into(),
                    size: 12,
                },
                TaskOutput {
                    id: 406.into(),
                    size: 12,
                },
            ],
        },
    );
    assert_eq!(comm.client.take_task_finished(1), vec![t1]);
    let errors = comm.client.take_task_errors(1);
    assert_eq!(errors[0].0, t2);
    assert_eq!(
        &errors[0].2.message,
        "Task 1@1 did not produced expected output(s): 11, 100"
    );
    let messages = comm.take_worker_msgs(w1, 2);
    assert!(
        matches!(&messages[0], ToWorkerMessage::RemoveDataObjects(x) if sorted_vec(x.to_vec()) == vec![DataObjectId::new(t1, 405.into()), DataObjectId::new(t1, 406.into())])
    );
    assert!(
        matches!(&messages[1], ToWorkerMessage::RemoveDataObjects(x) if sorted_vec(x.to_vec()) == vec![DataObjectId::new(t1, 10.into()), DataObjectId::new(t1, 101.into())])
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
}

#[test]
fn test_data_deps_basic() {
    let mut rt = TestEnv::new();
    let t1 = rt.new_task_default();
    let t2 = rt.new_task(&TaskBuilder::new().data_dep(t1, 0));
    let t3 = rt.new_task(&TaskBuilder::new().data_dep(t2, 123).data_dep(t2, 478));
    assert_eq!(rt.task(t2).task_deps, [t1]);
    rt.core().assert_waiting(&[t2, t3]);
    rt.core().assert_ready(&[t1]);
    let w1 = rt.new_worker_cpus(4);
    let mut comm = create_test_comm();
    assign_to_worker(rt.core(), t1, w1);

    on_task_finished(
        rt.core(),
        &mut comm,
        w1,
        TaskFinishedMsg {
            id: t1,
            outputs: vec![TaskOutput {
                id: 0.into(),
                size: 1,
            }],
        },
    );
    comm.check_need_scheduling();
    comm.client.take_task_finished(1);
    comm.emptiness_check();

    rt.core().assert_waiting(&[t3]);
    rt.core().assert_ready(&[t2]);

    assign_to_worker(rt.core(), t2, w1);

    let o = rt.core().dataobj_map();
    assert_eq!(o.len(), 1);
    o.get_data_object(DataObjectId::new(t1, 0.into()));

    on_task_finished(
        rt.core(),
        &mut comm,
        w1,
        TaskFinishedMsg {
            id: t2,
            outputs: vec![
                TaskOutput {
                    id: 123.into(),
                    size: 1,
                },
                TaskOutput {
                    id: 478.into(),
                    size: 1,
                },
            ],
        },
    );
    comm.check_need_scheduling();
    let messages = comm.take_worker_msgs(w1, 1);
    assert!(
        matches!(&messages[0], ToWorkerMessage::RemoveDataObjects(x) if x.len() == 1 && x[0] == DataObjectId::new(t1, 0.into()))
    );
    comm.client.take_task_finished(1);
    comm.emptiness_check();
    rt.core().assert_ready(&[t3]);

    let o = rt.core().dataobj_map();
    assert_eq!(o.len(), 2);
    o.get_data_object(DataObjectId::new(t2, 123.into()));
    o.get_data_object(DataObjectId::new(t2, 478.into()));
}
