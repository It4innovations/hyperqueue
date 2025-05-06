use std::time::Duration;

use crate::datasrv::DataObjectId;
use crate::gateway::LostWorkerReason;
use crate::internal::common::resources::ResourceDescriptor;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    ComputeTaskMsg, NewWorkerMsg, TaskFinishedMsg, TaskIdsMsg, TaskOutput, ToWorkerMessage,
};
use crate::internal::messages::worker::{StealResponse, StealResponseMsg};
use crate::internal::scheduler::state::SchedulerState;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::{
    T_LEVEL_WEIGHT, on_cancel_tasks, on_new_tasks, on_new_worker, on_remove_worker,
    on_steal_response, on_task_error, on_task_finished, on_task_running,
};
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::Worker;
use crate::internal::tests::utils::env::create_test_comm;
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_worker, create_test_workers, finish_on_worker, force_assign,
    start_and_finish_on_worker, start_mn_task_on_worker, start_on_worker, start_on_worker_running,
    submit_test_tasks,
};
use crate::internal::tests::utils::shared::{res_kind_groups, res_kind_sum};
use crate::internal::tests::utils::sorted_vec;
use crate::internal::tests::utils::task::{TaskBuilder, task, task_running_msg, task_with_deps};
use crate::internal::tests::utils::workflows::{submit_example_1, submit_example_3};
use crate::internal::tests::utils::{env, schedule};
use crate::internal::worker::configuration::{
    DEFAULT_MAX_DOWNLOAD_TRIES, DEFAULT_MAX_PARALLEL_DOWNLOADS,
    DEFAULT_WAIT_BETWEEN_DOWNLOAD_TRIES, OverviewConfiguration,
};
use crate::resources::{ResourceAmount, ResourceDescriptorItem, ResourceMap};
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{TaskId, WorkerId};

#[test]
fn test_worker_add() {
    let mut core = Core::default();
    assert_eq!(core.get_workers().count(), 0);

    let mut comm = create_test_comm();
    comm.emptiness_check();

    let wcfg = WorkerConfiguration {
        resources: ResourceDescriptor::simple(4),
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
        ResourceMap::from_vec(vec!["cpus".to_string()]),
    );
    on_new_worker(&mut core, &mut comm, worker);

    let new_w = comm.take_new_workers();
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
        resources: ResourceDescriptor::new(vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: res_kind_groups(&[vec!["2", "3", "4"], vec!["100", "150"]]),
            },
            ResourceDescriptorItem {
                name: "mem".to_string(),
                kind: res_kind_sum(100_000_000),
            },
        ]),
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
        ResourceMap::from_vec(vec![
            "cpus".to_string(),
            "gpus".to_string(),
            "mem".to_string(),
        ]),
    );
    on_new_worker(&mut core, &mut comm, worker);

    let new_w = comm.take_new_workers();
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
    let mut core = Core::default();
    let mut comm = create_test_comm();
    //new_workers(&mut core, &mut comm, vec![1]);

    let t1 = task(501);
    let t2 = task_with_deps(502, &[&t1]);
    let t3 = task(503);
    let t4 = task_with_deps(504, &[&t2]);

    let task_id5 = TaskId::new(123.into(), 1.into());
    let t5 = TaskBuilder::new(task_id5).build();
    let task_id6 = TaskId::new(122.into(), 0.into());
    let t6 = TaskBuilder::new(task_id6).build();
    let task_id7 = TaskId::new(123.into(), 2.into());
    let t7 = TaskBuilder::new(task_id7).task_deps(&[&t5]).build();
    let task_id8 = TaskId::new(123.into(), 4.into());
    let t8 = TaskBuilder::new(task_id8).build();

    on_new_tasks(&mut core, &mut comm, vec![t1, t2, t3, t4, t5, t6, t7, t8]);

    assert_eq!(core.get_task(TaskId::new_test(501)).scheduler_priority, 0);
    assert_eq!(
        core.get_task(TaskId::new_test(502)).scheduler_priority,
        T_LEVEL_WEIGHT
    );
    assert_eq!(core.get_task(TaskId::new_test(503)).scheduler_priority, 0);
    assert_eq!(
        core.get_task(TaskId::new_test(504)).scheduler_priority,
        2 * T_LEVEL_WEIGHT
    );

    assert_eq!(core.get_task(task_id5).scheduler_priority, -123);
    assert_eq!(core.get_task(task_id6).scheduler_priority, -122);
    assert_eq!(
        core.get_task(task_id7).scheduler_priority,
        -123 + T_LEVEL_WEIGHT
    );
    assert_eq!(core.get_task(task_id8).scheduler_priority, -123);
}

#[test]
fn test_submit_jobs() {
    let mut core = Core::default();
    let mut comm = create_test_comm();
    //new_workers(&mut core, &mut comm, vec![1]);

    let t1 = task(501);
    let t2 = task_with_deps(502, &[&t1]);
    on_new_tasks(&mut core, &mut comm, vec![t1, t2]);

    comm.check_need_scheduling();
    comm.emptiness_check();

    let t1 = core.get_task(501.into());
    let t2 = core.get_task(502.into());
    assert_eq!(t1.get_unfinished_deps(), 0);
    assert_eq!(t2.get_unfinished_deps(), 1);

    check_task_consumers_exact(t1, &[t2]);

    let t3 = task(604);
    let t4 = task_with_deps(602, &[t1, &t3]);
    let t5 = task_with_deps(603, &[&t3]);
    let t6 = task_with_deps(601, &[&t3, &t4, &t5, t2]);

    on_new_tasks(&mut core, &mut comm, vec![t3, t4, t5, t6]);
    comm.check_need_scheduling();
    comm.emptiness_check();

    let t1 = core.get_task(501.into());
    let t2 = core.get_task(502.into());
    let t4 = core.get_task(602.into());
    let t6 = core.get_task(601.into());

    check_task_consumers_exact(t1, &[t2, t4]);
    assert_eq!(t1.get_unfinished_deps(), 0);

    check_task_consumers_exact(t2, &[t6]);

    assert_eq!(t1.get_unfinished_deps(), 0);
    assert_eq!(t2.get_unfinished_deps(), 1);
    assert_eq!(t4.get_unfinished_deps(), 2);
    assert_eq!(t6.get_unfinished_deps(), 4);
}

fn no_data_task_finished(task_id: u32) -> TaskFinishedMsg {
    TaskFinishedMsg {
        id: task_id.into(),
        outputs: vec![],
    }
}

#[test]
fn test_assignments_and_finish() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);

    /*
       t1   t2    t4  t5
        \   /     |
          t3[k]   t7[k]
    */

    let t1 = TaskBuilder::new(11).user_priority(12).build();
    let t2 = task(12);
    let t3 = task_with_deps(13, &[&t1, &t2]);
    let t4 = task(14);
    let t5 = task(15);
    let t7 = task_with_deps(17, &[&t4]);

    let (id1, id2, id3, id5, id7) = (t1.id, t2.id, t3.id, t5.id, t7.id);

    submit_test_tasks(&mut core, vec![t1, t2, t3, t4, t5, t7]);
    let mut comm = create_test_comm();

    let mut scheduler = create_test_scheduler();

    force_assign(&mut core, &mut scheduler, 11, 100);
    force_assign(&mut core, &mut scheduler, 12, 101);
    force_assign(&mut core, &mut scheduler, 15, 100);

    core.assert_fresh(&[id1, id3]);

    scheduler.finish_scheduling(&mut core, &mut comm);

    core.assert_not_fresh(&[id1]);
    core.assert_fresh(&[id3]);

    check_worker_tasks_exact(&core, 100, &[id1, id5]);
    check_worker_tasks_exact(&core, 101, &[id2]);
    check_worker_tasks_exact(&core, 102, &[]);

    let msgs = comm.take_worker_msgs(100, 2);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id,
            user_priority: 12,
            ..
        }) if id.job_task_id().as_num() == 11
    ));
    assert!(matches!(
        msgs[1],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id,
            scheduler_priority: 0,
            ..
        }) if id.job_task_id().as_num() == 15
    ));
    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id,
            ..
        }) if id.job_task_id().as_num() == 12
    ));
    comm.emptiness_check();

    core.assert_assigned(&[id1, id2]);
    core.assert_waiting(&[id3, id7]);

    assert!(core.get_task(15.into()).is_assigned());

    // FINISH TASK WITHOUT CONSUMERS & KEEP FLAG
    on_task_finished(&mut core, &mut comm, 100.into(), no_data_task_finished(15));

    assert!(core.find_task(15.into()).is_none());
    check_worker_tasks_exact(&core, 100, &[id1]);
    check_worker_tasks_exact(&core, 101, &[id2]);
    check_worker_tasks_exact(&core, 102, &[]);

    comm.check_need_scheduling();
    assert_eq!(comm.take_client_task_finished(1)[0], TaskId::new_test(15));
    comm.emptiness_check();

    assert!(core.find_task(15.into()).is_none());

    // FINISHED TASK WITH CONSUMERS
    assert!(core.get_task(12.into()).is_assigned());

    on_task_finished(&mut core, &mut comm, 101.into(), no_data_task_finished(12));

    assert!(core.find_task(12.into()).is_none());
    check_worker_tasks_exact(&core, 100, &[id1]);
    check_worker_tasks_exact(&core, 101, &[]);
    check_worker_tasks_exact(&core, 102, &[]);

    comm.check_need_scheduling();
    assert_eq!(comm.take_client_task_finished(1)[0], TaskId::new_test(12));
    comm.emptiness_check();

    on_task_finished(&mut core, &mut comm, 100.into(), no_data_task_finished(11));

    comm.check_need_scheduling();

    force_assign(&mut core, &mut scheduler, 13, 101);
    scheduler.finish_scheduling(&mut core, &mut comm);

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id,
            ..
        }) if id.job_task_id().as_num() == 13
    ));

    assert_eq!(comm.take_client_task_finished(1)[0], TaskId::new_test(11));
    comm.emptiness_check();
    core.sanity_check();

    on_task_finished(&mut core, &mut comm, 101.into(), no_data_task_finished(13));

    comm.check_need_scheduling();

    assert_eq!(
        comm.take_client_task_finished(1),
        vec![TaskId::new_test(13)]
    );
    comm.emptiness_check();
    core.sanity_check();

    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_running_task_on_error() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100);
    start_and_finish_on_worker(&mut core, 12, 101);

    start_on_worker(&mut core, 13, 102);
    core.assert_assigned(&[13]);
    assert!(worker_has_task(&core, 102, 13));

    let mut comm = create_test_comm();
    on_task_error(
        &mut core,
        &mut comm,
        102.into(),
        13.into(),
        TaskFailInfo {
            message: "".to_string(),
        },
    );
    assert!(!worker_has_task(&core, 102, 13));

    let mut msgs = comm.take_client_task_errors(1);
    let (id, cs, _) = msgs.pop().unwrap();
    assert_eq!(id.job_task_id().as_num(), 13);
    assert_eq!(
        sorted_vec(cs),
        vec![
            TaskId::new_test(15),
            TaskId::new_test(16),
            TaskId::new_test(17)
        ]
    );
    comm.emptiness_check();

    assert!(core.find_task(16.into()).is_none());
    assert!(core.find_task(15.into()).is_none());
    core.sanity_check();
}

#[test]
fn test_steal_tasks_ok() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100);
    start_and_finish_on_worker(&mut core, 12, 101);

    let task_id = 13;
    start_on_worker(&mut core, task_id, 101);

    assert!(worker_has_task(&core, 101, task_id));
    assert!(!worker_has_task(&core, 100, task_id));

    let mut comm = create_test_comm();
    let mut scheduler = create_test_scheduler();

    force_reassign(&mut core, &mut scheduler, task_id, 100);
    scheduler.finish_scheduling(&mut core, &mut comm);

    assert!(!worker_has_task(&core, 101, task_id));
    assert!(worker_has_task(&core, 100, task_id));

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(
        matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![TaskId::new_test(task_id)])
    );
    comm.emptiness_check();

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![
                (task_id.into(), StealResponse::Ok),
                (123.into(), StealResponse::NotHere),
                (11.into(), StealResponse::NotHere),
            ],
        },
    );

    assert!(!worker_has_task(&core, 101, task_id));
    assert!(worker_has_task(&core, 100, task_id));

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg { id, .. })
        if id.job_task_id().as_num() == 13
    ));
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_steal_tasks_running() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100);
    start_and_finish_on_worker(&mut core, 12, 101);
    start_on_worker(&mut core, 13, 101);

    let mut comm = create_test_comm();
    let mut scheduler = create_test_scheduler();

    force_reassign(&mut core, &mut scheduler, 13, 100);
    scheduler.finish_scheduling(&mut core, &mut comm);

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(
        matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![TaskId::new_test(13)])
    );
    comm.emptiness_check();

    assert!(!worker_has_task(&core, 101, 13));
    assert!(worker_has_task(&core, 100, 13));

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(13.into(), StealResponse::Running)],
        },
    );

    assert!(worker_has_task(&core, 101, 13));
    assert!(!worker_has_task(&core, 100, 13));

    comm.check_need_scheduling();
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
#[should_panic]
fn finish_unassigned_task() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    finish_on_worker(&mut core, 11, 100);
}

#[test]
fn finish_task_without_outputs() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1]);
    let t1 = task_with_deps(1, &[]);
    submit_test_tasks(&mut core, vec![t1]);
    start_on_worker(&mut core, 1, 100);

    let mut comm = create_test_comm();
    on_task_finished(&mut core, &mut comm, 100.into(), no_data_task_finished(1));
    comm.check_need_scheduling();
    assert_eq!(comm.take_client_task_finished(1)[0], TaskId::new_test(1));
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_task_cancel() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);

    let t40 = task(40);
    let t41 = task(41);
    let t42 = task(42);

    submit_test_tasks(&mut core, vec![t40, t41, t42]);

    start_and_finish_on_worker(&mut core, 11, 101);
    start_on_worker(&mut core, 12, 101);
    start_on_worker(&mut core, 40, 101);
    start_on_worker(&mut core, 41, 100);

    fail_steal(&mut core, 12, 101, 100);
    start_stealing(&mut core, 40, 100);
    start_stealing(&mut core, 41, 101);

    let mut comm = create_test_comm();
    let (ct, ft) = on_cancel_tasks(
        &mut core,
        &mut comm,
        &vec![11, 12, 40, 41, 33]
            .into_iter()
            .map(|id| id.into())
            .collect::<Vec<_>>(),
    );

    assert_eq!(
        sorted_vec(ct),
        vec![12, 13, 14, 15, 16, 17, 40, 41]
            .into_iter()
            .map(|id| id.into())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        sorted_vec(ft),
        vec![TaskId::new_test(11), TaskId::new_test(33)]
    );

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![TaskId::new_test(41)])
    );

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if sorted_vec(ids.clone()) == vec![TaskId::new_test(12), TaskId::new_test(40)])
    );

    assert_eq!(core.task_map().len(), 1);
    assert!(core.find_task(42.into()).is_some());

    comm.check_need_scheduling();
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_worker_lost_with_mn_task_non_root() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1, 1]);
    let task1 = TaskBuilder::new(1).n_nodes(3).build();
    submit_test_tasks(&mut core, vec![task1]);
    start_mn_task_on_worker(
        &mut core,
        TaskId::new_test(1),
        vec![WorkerId::new(103), WorkerId::new(101), WorkerId::new(100)],
    );
    let mut comm = create_test_comm();
    on_remove_worker(
        &mut core,
        &mut comm,
        101.into(),
        LostWorkerReason::HeartbeatLost,
    );
    core.sanity_check();
    assert_eq!(comm.take_lost_workers().len(), 1);
    let msgs = comm.take_worker_msgs(103, 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![TaskId::new_test(1)])
    );
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w.as_num() == 101));
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(core.get_task(TaskId::new_test(1)).is_waiting());
}

#[test]
fn test_worker_lost_with_mn_task_root() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1, 1]);
    let task1 = TaskBuilder::new(1).n_nodes(3).build();
    submit_test_tasks(&mut core, vec![task1]);
    start_mn_task_on_worker(
        &mut core,
        TaskId::new_test(1),
        vec![WorkerId::new(103), WorkerId::new(101), WorkerId::new(100)],
    );
    let mut comm = create_test_comm();
    on_remove_worker(
        &mut core,
        &mut comm,
        103.into(),
        LostWorkerReason::HeartbeatLost,
    );
    core.sanity_check();
    assert_eq!(comm.take_lost_workers().len(), 1);
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w.as_num() == 103
    ));
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(core.get_task(TaskId::new_test(1)).is_waiting());
}

#[test]
fn test_worker_crashing_task() {
    let mut core = Core::default();

    let t1 = task(1);
    submit_test_tasks(&mut core, vec![t1]);
    assert_eq!(core.get_task(TaskId::new_test(1)).crash_counter, 0);

    for x in 1..=5 {
        let mut comm = create_test_comm();
        let worker_id = 100 + x;
        create_test_worker(&mut core, worker_id.into(), 1);
        start_on_worker_running(&mut core, 1, worker_id);
        on_remove_worker(
            &mut core,
            &mut comm,
            worker_id.into(),
            LostWorkerReason::HeartbeatLost,
        );
        let mut lw = comm.take_lost_workers();
        assert_eq!(lw[0].0, WorkerId::new(worker_id));
        comm.check_need_scheduling();
        assert!(matches!(
            comm.take_broadcasts(1)[0],
            ToWorkerMessage::LostWorker(w)
            if worker_id == w.as_num()
        ));
        if x == 5 {
            let errs = comm.take_client_task_errors(1);
            assert_eq!(errs[0].0, TaskId::new_test(1));
            assert_eq!(
                errs[0].2.message,
                "Task was running on a worker that was lost; the task has occurred 5 times in this situation and limit was reached."
            );
            assert_eq!(std::mem::take(&mut lw[0].1), vec![]);
        } else {
            assert_eq!(std::mem::take(&mut lw[0].1), vec![TaskId::new_test(1)]);
            assert_eq!(core.get_task(TaskId::new_test(1)).crash_counter, x);
        }
        comm.emptiness_check();
    }
}

#[test]
fn test_task_mn_fail() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1, 1]);
    let task1 = TaskBuilder::new(1).n_nodes(3).build();
    submit_test_tasks(&mut core, vec![task1]);
    start_mn_task_on_worker(
        &mut core,
        TaskId::new_test(1),
        vec![WorkerId::new(103), WorkerId::new(101), WorkerId::new(100)],
    );
    let mut comm = create_test_comm();
    on_task_error(
        &mut core,
        &mut comm,
        103.into(),
        1.into(),
        TaskFailInfo {
            message: "".to_string(),
        },
    );
    core.sanity_check();
    let msgs = comm.take_client_task_errors(1);
    assert_eq!(msgs[0].0, TaskId::new_test(1));
    comm.emptiness_check();
    assert!(core.find_task(1.into()).is_none());
    for w in &[100, 101, 102, 103] {
        assert!(
            core.get_worker_map()
                .get_worker((*w).into())
                .mn_task()
                .is_none()
        );
    }
}

#[test]
fn test_task_mn_cancel() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1, 1]);
    let task1 = TaskBuilder::new(1).n_nodes(3).build();
    submit_test_tasks(&mut core, vec![task1]);
    start_mn_task_on_worker(
        &mut core,
        TaskId::new_test(1),
        vec![WorkerId::new(103), WorkerId::new(101), WorkerId::new(100)],
    );
    let mut comm = create_test_comm();
    let (ct, ft) = on_cancel_tasks(&mut core, &mut comm, &[TaskId::new_test(1)]);
    core.sanity_check();
    let msgs = comm.take_worker_msgs(103, 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![TaskId::new_test(1)])
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert!(ft.is_empty());
    assert_eq!(ct, vec![TaskId::new_test(1)]);

    let mut scheduler = create_test_scheduler();
    scheduler.run_scheduling(&mut core, &mut comm);
    core.sanity_check();
    assert!(matches!(
        &comm.take_worker_msgs(100, 1)[0],
        &ToWorkerMessage::SetReservation(false)
    ));
    assert!(matches!(
        &comm.take_worker_msgs(101, 1)[0],
        &ToWorkerMessage::SetReservation(false)
    ));
    comm.emptiness_check();

    assert!(core.find_task(1.into()).is_none());
    for w in core.get_workers() {
        assert!(w.mn_task.is_none());
    }
}

#[test]
fn test_running_task() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    let t2 = task(2);
    submit_test_tasks(&mut core, vec![t1, t2]);
    start_on_worker(&mut core, 1, 101);
    start_on_worker(&mut core, 2, 101);

    let mut comm = create_test_comm();

    comm.emptiness_check();

    on_task_running(&mut core, &mut comm, 101.into(), task_running_msg(1));
    assert_eq!(comm.take_client_task_running(1), vec![TaskId::new_test(1)]);
    comm.emptiness_check();

    on_task_running(&mut core, &mut comm, 101.into(), task_running_msg(2));
    assert_eq!(comm.take_client_task_running(1)[0], TaskId::new_test(2));
    comm.emptiness_check();

    assert!(matches!(
        core.task(1).state,
        TaskRuntimeState::Running {
            worker_id,
            ..
        } if worker_id.as_num() == 101
    ));
    assert!(matches!(
        core.task(2).state,
        TaskRuntimeState::Running {
            worker_id,
            ..
        } if worker_id.as_num() == 101
    ));

    on_remove_worker(
        &mut core,
        &mut comm,
        101.into(),
        LostWorkerReason::HeartbeatLost,
    );
    let mut lw = comm.take_lost_workers();
    assert_eq!(lw[0].0, WorkerId::new(101));
    assert_eq!(
        sorted_vec(std::mem::take(&mut lw[0].1)),
        vec![TaskId::new_test(1), TaskId::new_test(2)]
    );
    comm.check_need_scheduling();
    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w.as_num() == 101
    ));
    comm.emptiness_check();
}

#[test]
fn test_finished_before_steal_response() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    submit_test_tasks(&mut core, vec![t1]);
    start_on_worker(&mut core, 1, 101);
    start_stealing(&mut core, 1, 102);
    assert!(worker_has_task(&core, 102, 1));

    let mut comm = create_test_comm();
    on_task_finished(&mut core, &mut comm, 101.into(), no_data_task_finished(1));

    comm.check_need_scheduling();
    assert_eq!(comm.take_client_task_finished(1)[0], TaskId::new_test(1));
    comm.emptiness_check();

    assert!(!worker_has_task(&core, 101, 1));
    assert!(!worker_has_task(&core, 102, 1));

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(1.into(), StealResponse::NotHere)],
        },
    );

    comm.emptiness_check();

    assert!(!worker_has_task(&core, 101, 1));
    assert!(!worker_has_task(&core, 102, 1));
}

#[test]
fn test_running_before_steal_response() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    submit_test_tasks(&mut core, vec![t1]);
    start_on_worker(&mut core, 1, 101);
    start_stealing(&mut core, 1, 102);
    assert!(worker_has_task(&core, 102, 1));

    let mut comm = create_test_comm();
    on_task_running(&mut core, &mut comm, 101.into(), task_running_msg(1));
    comm.check_need_scheduling();
    assert_eq!(comm.take_client_task_running(1)[0], TaskId::new_test(1));
    comm.emptiness_check();

    assert!(worker_has_task(&core, 101, 1));
    assert!(!worker_has_task(&core, 102, 1));

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(1.into(), StealResponse::Running)],
        },
    );

    comm.emptiness_check();
    assert!(worker_has_task(&core, 101, 1));
    assert!(!worker_has_task(&core, 102, 1));
}

#[test]
fn test_ready_to_assign_is_empty_after_cancel() {
    let mut core = Core::default();
    let t1 = task(1);
    submit_test_tasks(&mut core, vec![t1]);
    cancel_tasks(&mut core, &[1]);
    assert!(core.take_single_node_ready_to_assign().is_empty());
}

#[test]
fn test_after_cancel_messages() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    let t2 = task(2);
    let t3 = task(3);
    let t4 = task(4);
    submit_test_tasks(&mut core, vec![t1, t2, t3, t4]);
    start_on_worker(&mut core, 1, 101);
    start_on_worker(&mut core, 2, 101);
    start_on_worker(&mut core, 3, 101);
    start_on_worker(&mut core, 4, 101);

    cancel_tasks(&mut core, &[1, 2, 3, 4]);

    let mut comm = create_test_comm();
    on_task_finished(&mut core, &mut comm, 101.into(), no_data_task_finished(1));
    comm.emptiness_check();

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(2.into(), StealResponse::Ok)],
        },
    );
    comm.emptiness_check();

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(2.into(), StealResponse::Running)],
        },
    );
    comm.emptiness_check();

    on_task_error(
        &mut core,
        &mut comm,
        101.into(),
        3.into(),
        TaskFailInfo {
            message: "".to_string(),
        },
    );
    comm.emptiness_check();

    on_task_running(&mut core, &mut comm, 101.into(), task_running_msg(4));
    comm.emptiness_check();
}

#[test]
fn lost_worker_with_running_and_assign_tasks() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);

    let t40 = task(40);
    let t41 = task(41);
    submit_test_tasks(&mut core, vec![t40, t41]);

    start_on_worker(&mut core, 11, 101);
    start_on_worker(&mut core, 12, 101);
    start_on_worker(&mut core, 40, 101);
    start_on_worker(&mut core, 41, 100);

    fail_steal(&mut core, 12, 101, 100);
    start_stealing(&mut core, 40, 100);
    start_stealing(&mut core, 41, 101);

    core.assert_running(&[12]);
    assert_eq!(core.get_task(12.into()).instance_id, 0.into());

    core.assert_task_condition(&[11, 12, 40, 41], |t| !t.is_fresh());

    let mut comm = create_test_comm();
    on_remove_worker(
        &mut core,
        &mut comm,
        101.into(),
        LostWorkerReason::HeartbeatLost,
    );

    assert_eq!(
        comm.take_lost_workers(),
        vec![(WorkerId::new(101), vec![TaskId::new_test(12)])]
    );

    assert_eq!(core.take_single_node_ready_to_assign().len(), 3);
    core.assert_ready(&[11, 12]);
    assert_eq!(core.get_task(12.into()).instance_id, 1.into());
    assert!(core.get_task(40.into()).is_ready());
    core.assert_ready(&[40]);
    core.assert_fresh(&[11, 12, 40]);
    assert!(matches!(
        core.get_task(41.into()).state,
        TaskRuntimeState::Stealing(w, None) if w.as_num() == 100
    ));

    assert!(matches!(
        comm.take_broadcasts(1)[0],
        ToWorkerMessage::LostWorker(w) if w.as_num() == 101
    ));

    comm.check_need_scheduling();
    comm.emptiness_check();

    on_steal_response(
        &mut core,
        &mut comm,
        100.into(),
        StealResponseMsg {
            responses: vec![(41.into(), StealResponse::Ok)],
        },
    );

    assert_eq!(core.take_single_node_ready_to_assign().len(), 1);
    core.assert_ready(&[41]);
    core.assert_fresh(&[41]);

    comm.check_need_scheduling();
    comm.emptiness_check();

    core.sanity_check();
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

fn fail_steal<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
    target_worker_id: W,
) {
    let task_id = task_id.into();
    start_stealing(core, task_id, target_worker_id.into());
    let mut comm = env::create_test_comm();
    on_steal_response(
        core,
        &mut comm,
        worker_id.into(),
        StealResponseMsg {
            responses: vec![(task_id, StealResponse::Running)],
        },
    )
}

fn start_stealing<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    new_worker_id: W,
) {
    let mut scheduler = schedule::create_test_scheduler();
    force_reassign(core, &mut scheduler, task_id.into(), new_worker_id.into());
    let mut comm = env::create_test_comm();
    scheduler.finish_scheduling(core, &mut comm);
}

fn cancel_tasks<T: Into<TaskId> + Copy>(core: &mut Core, task_ids: &[T]) {
    let mut comm = env::create_test_comm();
    on_cancel_tasks(
        core,
        &mut comm,
        &task_ids.iter().map(|&v| v.into()).collect::<Vec<_>>(),
    );
}

fn check_worker_tasks_exact(core: &Core, worker_id: u32, tasks: &[TaskId]) {
    let worker = core.get_worker_by_id_or_panic(worker_id.into());
    assert_eq!(worker.sn_tasks().len(), tasks.len());
    for task in tasks {
        assert!(worker.sn_tasks().contains(task));
    }
}

fn worker_has_task<T: Into<TaskId>>(core: &Core, worker_id: u32, task_id: T) -> bool {
    core.get_worker_by_id_or_panic(worker_id.into())
        .sn_tasks()
        .contains(&task_id.into())
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
    let mut core = Core::default();
    //create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_3(&mut core);
    assert_eq!(core.get_read_to_assign().len(), 2);
    create_test_workers(&mut core, &[1]);
    start_and_finish_on_worker(&mut core, 2, 100);
    core.assert_waiting(&[3, 4, 6]);
    core.assert_ready(&[5]);

    start_and_finish_on_worker(&mut core, 1, 100);
    core.assert_waiting(&[6]);
    core.assert_ready(&[3, 4, 5]);
}

#[test]
fn test_worker_groups() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1]);
    let g = core.worker_group("default").unwrap();
    assert_eq!(
        sorted_vec(g.worker_ids().collect()),
        vec![WorkerId::new(100), WorkerId::new(101)]
    );
    let mut comm = create_test_comm();
    on_remove_worker(
        &mut core,
        &mut comm,
        101.into(),
        LostWorkerReason::HeartbeatLost,
    );
    let g = core.worker_group("default").unwrap();
    assert_eq!(
        sorted_vec(g.worker_ids().collect()),
        vec![WorkerId::new(100)]
    );
    let mut comm = create_test_comm();
    on_remove_worker(
        &mut core,
        &mut comm,
        100.into(),
        LostWorkerReason::HeartbeatLost,
    );
    assert!(core.worker_group("default").is_none());
}

#[test]
fn test_data_deps_no_output() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[4]);
    let t1 = TaskBuilder::new(1).build();
    let t2 = TaskBuilder::new(2).data_dep(&t1, 11).build();
    submit_test_tasks(&mut core, vec![t1, t2]);
    start_on_worker(&mut core, 1, 100);
    core.sanity_check();
    let mut comm = create_test_comm();
    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 1.into(),
            outputs: vec![],
        },
    );
    assert_eq!(comm.take_client_task_finished(1), vec![TaskId::new_test(1)]);
    let errors = comm.take_client_task_errors(1);
    assert_eq!(errors[0].0, TaskId::new_test(2));
    assert_eq!(
        &errors[0].2.message,
        "Task 0@1 did not produced expected output(s): 11"
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
}

#[test]
fn test_data_deps_missing_outputs() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[4]);
    let t1 = TaskBuilder::new(1).build();
    let t2 = TaskBuilder::new(2)
        .data_dep(&t1, 10)
        .data_dep(&t1, 11)
        .data_dep(&t1, 100)
        .data_dep(&t1, 101)
        .build();
    submit_test_tasks(&mut core, vec![t1, t2]);
    start_on_worker(&mut core, 1, 100);
    core.sanity_check();
    let mut comm = create_test_comm();
    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 1.into(),
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
    assert_eq!(comm.take_client_task_finished(1), vec![TaskId::new_test(1)]);
    let errors = comm.take_client_task_errors(1);
    assert_eq!(errors[0].0, TaskId::new_test(2));
    assert_eq!(
        &errors[0].2.message,
        "Task 0@1 did not produced expected output(s): 11, 100"
    );
    let messages = comm.take_worker_msgs(100, 2);
    assert!(
        matches!(&messages[0], ToWorkerMessage::RemoveDataObjects(x) if sorted_vec(x.to_vec()) == vec![DataObjectId::new(1.into(), 405.into()), DataObjectId::new(1.into(), 406.into())])
    );
    assert!(
        matches!(&messages[1], ToWorkerMessage::RemoveDataObjects(x) if sorted_vec(x.to_vec()) == vec![DataObjectId::new(1.into(), 10.into()), DataObjectId::new(1.into(), 101.into())])
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
}

#[test]
fn test_data_deps_basic() {
    let mut core = Core::default();
    let t1 = TaskBuilder::new(1).build();
    let t2 = TaskBuilder::new(2).data_dep(&t1, 0).build();
    let t3 = TaskBuilder::new(3)
        .data_dep(&t2, 123)
        .data_dep(&t2, 478)
        .build();
    submit_test_tasks(&mut core, vec![t1, t2, t3]);
    assert_eq!(core.get_task(2.into()).task_deps, [TaskId::new_test(1)]);
    core.assert_waiting(&[2, 3]);
    core.assert_ready(&[1]);
    create_test_workers(&mut core, &[4]);
    let mut comm = create_test_comm();
    start_on_worker(&mut core, 1, 100);

    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 1.into(),
            outputs: vec![TaskOutput {
                id: 0.into(),
                size: 1,
            }],
        },
    );
    comm.check_need_scheduling();
    comm.take_client_task_finished(1);
    comm.emptiness_check();

    core.assert_waiting(&[3]);
    core.assert_ready(&[2]);

    start_on_worker(&mut core, 2, 100);

    let o = core.dataobj_map();
    assert_eq!(o.len(), 1);
    o.get_data_object(DataObjectId::new(1.into(), 0.into()));

    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 2.into(),
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
    let messages = comm.take_worker_msgs(100, 1);
    assert!(
        matches!(&messages[0], ToWorkerMessage::RemoveDataObjects(x) if x.len() == 1 && x[0] == DataObjectId::new(1.into(), 0.into()))
    );
    comm.take_client_task_finished(1);
    comm.emptiness_check();
    core.assert_ready(&[3]);

    let o = core.dataobj_map();
    assert_eq!(o.len(), 2);
    o.get_data_object(DataObjectId::new(2.into(), 123.into()));
    o.get_data_object(DataObjectId::new(2.into(), 478.into()));
}
