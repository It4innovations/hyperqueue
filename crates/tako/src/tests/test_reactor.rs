use std::time::Duration;

use crate::common::index::AsIdVec;
use crate::common::resources::ResourceDescriptor;
use crate::common::Set;
use crate::messages::common::{TaskFailInfo, WorkerConfiguration};
use crate::messages::gateway::LostWorkerReason;
use crate::messages::worker::{
    ComputeTaskMsg, NewWorkerMsg, TaskFinishedMsg, TaskIdMsg, TaskIdsMsg, ToWorkerMessage,
};
use crate::messages::worker::{StealResponse, StealResponseMsg};
use crate::scheduler::state::SchedulerState;
use crate::server::core::Core;
use crate::server::reactor::{
    on_cancel_tasks, on_new_tasks, on_new_worker, on_remove_worker, on_reset_keep_flag,
    on_set_observe_flag, on_steal_response, on_task_error, on_task_finished, on_task_running,
    on_tasks_transferred,
};
use crate::server::task::{TaskRef, TaskRuntimeState};
use crate::server::worker::Worker;
use crate::tests::utils::env::create_test_comm;
use crate::tests::utils::schedule::{
    create_test_scheduler, create_test_workers, finish_on_worker, force_assign,
    start_and_finish_on_worker, start_on_worker, submit_test_tasks,
};
use crate::tests::utils::sorted_vec;
use crate::tests::utils::task::{task, task_with_deps, TaskBuilder};
use crate::tests::utils::workflows::{submit_example_1, submit_example_3};
use crate::tests::utils::{env, schedule};
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
        log_dir: Default::default(),
        heartbeat_interval: Duration::from_millis(1000),
        hw_state_poll_interval: Some(Duration::from_millis(1000)),
        idle_timeout: None,
        time_limit: None,
        extra: Default::default(),
    };

    let worker = Worker::new(402.into(), wcfg, Default::default());
    on_new_worker(&mut core, &mut comm, worker);

    let new_w = comm.take_new_workers();
    assert_eq!(new_w.len(), 1);
    assert_eq!(new_w[0].0.as_num(), 402);
    assert_eq!(new_w[0].1.resources.cpus, vec![vec![0, 1, 2, 3].to_ids()]);

    assert!(
        matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: WorkerId(402), address: ref a
        }) if a == "test1:123")
    );

    comm.check_need_scheduling();
    comm.emptiness_check();
    assert_eq!(core.get_workers().count(), 1);

    let wcfg2 = WorkerConfiguration {
        resources: ResourceDescriptor::new(
            vec![vec![2, 3, 4].to_ids(), vec![100, 150].to_ids()],
            Vec::new(),
        ),
        listen_address: "test2:123".into(),
        hostname: "test2".to_string(),
        work_dir: Default::default(),
        log_dir: Default::default(),
        heartbeat_interval: Duration::from_millis(1000),
        hw_state_poll_interval: Some(Duration::from_millis(1000)),
        idle_timeout: None,
        time_limit: None,
        extra: Default::default(),
    };

    let worker = Worker::new(502.into(), wcfg2, Default::default());
    on_new_worker(&mut core, &mut comm, worker);

    let new_w = comm.take_new_workers();
    assert_eq!(new_w.len(), 1);
    assert_eq!(new_w[0].0.as_num(), 502);
    assert_eq!(
        new_w[0].1.resources.cpus,
        vec![vec![2, 3, 4].to_ids(), vec![100, 150].to_ids()]
    );

    assert!(
        matches!(comm.take_broadcasts(1)[0], ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: WorkerId(502), address: ref a
        }) if a == "test2:123")
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
    assert_eq!(core.get_workers().count(), 2);
}

#[test]
fn test_submit_jobs() {
    let mut core = Core::default();
    let mut comm = create_test_comm();
    //new_workers(&mut core, &mut comm, vec![1]);

    let t1 = task(501);
    let t2 = task_with_deps(502, &[&t1], 1);
    on_new_tasks(&mut core, &mut comm, vec![t2, t1]);

    comm.check_need_scheduling();
    comm.emptiness_check();

    let t1 = core.get_task_by_id_or_panic(501.into()).clone();
    let t2 = core.get_task_by_id_or_panic(502.into()).clone();
    assert_eq!(t1.get().get_unfinished_deps(), 0);
    assert_eq!(t2.get().get_unfinished_deps(), 1);
    assert_eq!(t1.get().get_consumers().len(), 1);
    assert!(t1.get().get_consumers().contains(&t2));

    let t3 = task(604);
    let t4 = task_with_deps(602, &[&t1, &t3], 1);
    let t5 = task_with_deps(603, &[&t3], 1);
    let t6 = task_with_deps(601, &[&t3, &t4, &t5, &t2], 1);

    on_new_tasks(&mut core, &mut comm, vec![t6, t3, t4, t5]);
    comm.check_need_scheduling();
    comm.emptiness_check();

    let t4 = core.get_task_by_id_or_panic(602.into()).clone();
    let t6 = core.get_task_by_id_or_panic(601.into()).clone();

    assert_eq!(t1.get().get_consumers().len(), 2);
    assert!(t1.get().get_consumers().contains(&t2));
    assert!(t1.get().get_consumers().contains(&t4));
    assert_eq!(t1.get().get_unfinished_deps(), 0);

    assert_eq!(t2.get().get_consumers().len(), 1);
    assert!(t2.get().get_consumers().contains(&t6));

    assert_eq!(t1.get().get_unfinished_deps(), 0);
    assert_eq!(t2.get().get_unfinished_deps(), 1);
    assert_eq!(t4.get().get_unfinished_deps(), 2);
    assert_eq!(t6.get().get_unfinished_deps(), 4);
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

    let t1 = TaskBuilder::new(11).user_priority(12).outputs(1).build();
    let t2 = task(12);
    let t3 = task_with_deps(13, &[&t1, &t2], 1);
    t3.get_mut().set_keep_flag(true);
    let t4 = task(14);
    let t5 = task(15);
    //let t6 = task_with_deps(16, &[12]);
    let t7 = task_with_deps(17, &[&t4], 1);
    t7.get_mut().set_keep_flag(true);

    submit_test_tasks(&mut core, &[&t1, &t2, &t3, &t4, &t5, &t7]);
    let mut comm = create_test_comm();

    let mut scheduler = create_test_scheduler();

    force_assign(&mut core, &mut scheduler, 11, 100);
    force_assign(&mut core, &mut scheduler, 12, 101);
    force_assign(&mut core, &mut scheduler, 15, 100);

    assert!(t1.get().is_fresh());
    assert!(t3.get().is_fresh());

    scheduler.finish_scheduling(&mut comm);

    assert!(!t1.get().is_fresh());
    assert!(t3.get().is_fresh());

    check_worker_tasks_exact(&core, 100, &[&t1, &t5]);
    check_worker_tasks_exact(&core, 101, &[&t2]);
    check_worker_tasks_exact(&core, 102, &[]);

    let msgs = comm.take_worker_msgs(100, 2);
    dbg!(&msgs[0]);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id: TaskId(11),
            user_priority: 12,
            ..
        })
    ));
    assert!(matches!(
        msgs[1],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id: TaskId(15),
            scheduler_priority: 0,
            ..
        })
    ));
    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: TaskId(12), .. })
    ));
    comm.emptiness_check();

    core.assert_assigned(&[11, 12]);
    core.assert_waiting(&[13, 17]);

    let t5 = core.get_task_by_id_or_panic(15.into()).clone();
    assert!(t5.get().is_assigned());

    // FINISH TASK WITHOUT CONSUMERS & KEEP FLAG
    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 15.into(),
            size: 301,
        },
    );

    assert!(matches!(t5.get().state, TaskRuntimeState::Released));
    check_worker_tasks_exact(&core, 100, &[&t1]);
    check_worker_tasks_exact(&core, 101, &[&t2]);
    check_worker_tasks_exact(&core, 102, &[]);

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(15) })
    ));

    comm.check_need_scheduling();

    //assert_eq!(comm.take_client_task_finished(1), vec![15]);
    comm.emptiness_check();

    assert!(core.get_task_by_id(15.into()).is_none());

    // FINISHED TASK WITH CONSUMERS
    let t2 = core.get_task_by_id_or_panic(12.into()).clone();
    assert!(t2.get().is_assigned());

    on_task_finished(
        &mut core,
        &mut comm,
        101.into(),
        TaskFinishedMsg {
            id: 12.into(),
            size: 5000,
        },
    );

    assert!(t2.get().is_finished());
    assert!(matches!(t5.get().state, TaskRuntimeState::Released));
    check_worker_tasks_exact(&core, 100, &[&t1]);
    check_worker_tasks_exact(&core, 101, &[]);
    check_worker_tasks_exact(&core, 102, &[]);

    comm.check_need_scheduling();
    //assert_eq!(comm.take_client_task_finished(1), vec![12]);
    comm.emptiness_check();

    assert!(core.get_task_by_id(12.into()).is_some());

    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 11.into(),
            size: 1000,
        },
    );

    comm.check_need_scheduling();
    //assert_eq!(comm.take_client_task_finished(1), vec![11]);

    force_assign(&mut core, &mut scheduler, 13, 101);
    scheduler.finish_scheduling(&mut comm);

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: TaskId(13), .. })
    ));

    comm.emptiness_check();
    core.sanity_check();

    on_set_observe_flag(&mut core, &mut comm, 13.into(), true);

    on_task_finished(
        &mut core,
        &mut comm,
        101.into(),
        TaskFinishedMsg {
            id: 13.into(),
            size: 1000,
        },
    );

    comm.check_need_scheduling();

    assert_eq!(comm.take_client_task_finished(1), vec![13].to_ids());

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(11) })
    ));

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(12) })
    ));
    comm.emptiness_check();

    on_reset_keep_flag(&mut core, &mut comm, 13.into());
    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(13) })
    ));
    comm.emptiness_check();
    core.sanity_check();

    on_reset_keep_flag(&mut core, &mut comm, 17.into());
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_running_task_on_error() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 1000);
    start_and_finish_on_worker(&mut core, 12, 101, 1000);

    start_on_worker(&mut core, 13, 102);
    let t13 = core.get_task_by_id_or_panic(13.into()).clone();
    assert!(t13.get().is_assigned());
    assert!(worker_has_task(&core, 102, &t13));

    let mut comm = create_test_comm();
    on_task_error(
        &mut core,
        &mut comm,
        102.into(),
        13.into(),
        TaskFailInfo {
            message: "".to_string(),
            data_type: "".to_string(),
            error_data: vec![],
        },
    );
    assert!(!worker_has_task(&core, 102, &t13));

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(11) })
    ));
    let mut msgs = comm.take_client_task_errors(1);
    let (id, cs, _) = msgs.pop().unwrap();
    assert_eq!(id.as_num(), 13);
    assert_eq!(sorted_vec(cs), vec![15, 16, 17].to_ids());
    comm.emptiness_check();

    assert!(core.get_task_by_id(16.into()).is_none());
    assert!(core.get_task_by_id(15.into()).is_none());
    core.sanity_check();
}

#[test]
fn test_running_task_on_task_transferred_invalid() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let mut comm = create_test_comm();
    on_tasks_transferred(&mut core, &mut comm, 102.into(), 42.into());
    let msgs = comm.take_worker_msgs(102, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(42) })
    ));
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_running_task_on_task_transferred() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 1000);
    start_and_finish_on_worker(&mut core, 12, 101, 1000);
    start_on_worker(&mut core, 13, 101);

    let mut comm = create_test_comm();
    on_tasks_transferred(&mut core, &mut comm, 101.into(), 11.into());

    comm.emptiness_check();

    let ws = core
        .get_task_by_id_or_panic(11.into())
        .get()
        .get_placement()
        .unwrap()
        .clone();
    let mut set = Set::new();
    set.insert(WorkerId::new(100));
    set.insert(WorkerId::new(101));
    assert_eq!(ws, set);
    core.sanity_check();
}

#[test]
fn test_steal_tasks_ok() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 1000);
    start_and_finish_on_worker(&mut core, 12, 101, 1000);
    start_on_worker(&mut core, 13, 101);

    let t3 = core.get_task_by_id_or_panic(13.into()).clone();
    assert!(worker_has_task(&core, 101, &t3));
    assert!(!worker_has_task(&core, 100, &t3));

    let mut comm = create_test_comm();
    let mut scheduler = create_test_scheduler();

    force_reassign(&mut core, &mut scheduler, 13, 100);
    scheduler.finish_scheduling(&mut comm);

    assert!(!worker_has_task(&core, 101, &t3));
    assert!(worker_has_task(&core, 100, &t3));

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![13].to_ids()));
    comm.emptiness_check();

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![
                (13.into(), StealResponse::Ok),
                (123.into(), StealResponse::NotHere),
                (11.into(), StealResponse::NotHere),
            ],
        },
    );

    assert!(!worker_has_task(&core, 101, &t3));
    assert!(worker_has_task(&core, 100, &t3));

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(matches!(
        &msgs[0],
        ToWorkerMessage::ComputeTask(ComputeTaskMsg { id: TaskId(13), .. })
    ));
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_steal_tasks_running() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);
    start_and_finish_on_worker(&mut core, 11, 100, 1000);
    start_and_finish_on_worker(&mut core, 12, 101, 1000);
    start_on_worker(&mut core, 13, 101);

    let mut comm = create_test_comm();
    let mut scheduler = create_test_scheduler();

    force_reassign(&mut core, &mut scheduler, 13, 100);
    scheduler.finish_scheduling(&mut comm);

    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(&msgs[0], ToWorkerMessage::StealTasks(ids) if ids.ids == vec![13].to_ids()));
    comm.emptiness_check();

    let t3 = core.get_task_by_id_or_panic(13.into()).clone();
    assert!(!worker_has_task(&core, 101, &t3));
    assert!(worker_has_task(&core, 100, &t3));

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(13.into(), StealResponse::Running)],
        },
    );

    assert!(worker_has_task(&core, 101, &t3));
    assert!(!worker_has_task(&core, 100, &t3));

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
    finish_on_worker(&mut core, 11, 100, 1000);
}

#[test]
fn finish_task_without_outputs() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1]);
    let t1 = task_with_deps(1, &[], 0);
    submit_test_tasks(&mut core, &[&t1]);
    start_on_worker(&mut core, 1, 100);

    let mut comm = create_test_comm();
    on_task_finished(
        &mut core,
        &mut comm,
        100.into(),
        TaskFinishedMsg {
            id: 1.into(),
            size: 0,
        },
    );
    comm.check_need_scheduling();
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

    submit_test_tasks(&mut core, &[&t40, &t41, &t42]);

    start_and_finish_on_worker(&mut core, 11, 101, 1000);
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
    assert_eq!(sorted_vec(ft), vec![11, 33].to_ids());

    let msgs = comm.take_worker_msgs(100, 1);
    assert!(
        matches!(&msgs[0], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if ids == &vec![41].to_ids())
    );

    let msgs = comm.take_worker_msgs(101, 2);
    dbg!(&msgs);
    assert!(matches!(
        &msgs[0],
        &ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(11) })
    ));
    assert!(
        matches!(&msgs[1], &ToWorkerMessage::CancelTasks(TaskIdsMsg { ref ids }) if sorted_vec(ids.clone()) == vec![12, 40].to_ids())
    );

    assert_eq!(core.get_task_map().len(), 1);
    assert!(core.get_task_by_id(42.into()).is_some());

    comm.check_need_scheduling();
    comm.emptiness_check();
    core.sanity_check();
}

#[test]
fn test_running_task() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    let t2 = task(2);
    submit_test_tasks(&mut core, &[&t1, &t2]);
    start_on_worker(&mut core, 1, 101);
    start_on_worker(&mut core, 2, 101);

    let mut comm = create_test_comm();

    on_set_observe_flag(&mut core, &mut comm, 1.into(), true);
    comm.emptiness_check();

    on_task_running(&mut core, &mut comm, 101.into(), 1.into());
    assert_eq!(comm.take_client_task_running(1), vec![1].to_ids());
    comm.emptiness_check();

    on_task_running(&mut core, &mut comm, 101.into(), 2.into());
    comm.emptiness_check();

    assert!(matches!(
        t1.get().state,
        TaskRuntimeState::Running(WorkerId(101))
    ));
    assert!(matches!(
        t2.get().state,
        TaskRuntimeState::Running(WorkerId(101))
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
        vec![1, 2].to_ids()
    );
    comm.check_need_scheduling();
    comm.emptiness_check();
}

#[test]
fn test_finished_before_steal_response() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    submit_test_tasks(&mut core, &[&t1]);
    start_on_worker(&mut core, 1, 101);
    start_stealing(&mut core, 1, 102);
    assert!(worker_has_task(&core, 102, &t1));

    let mut comm = create_test_comm();
    on_task_finished(
        &mut core,
        &mut comm,
        101.into(),
        TaskFinishedMsg {
            id: 1.into(),
            size: 0,
        },
    );
    let msgs = comm.take_worker_msgs(101, 1);
    assert!(matches!(
        msgs[0],
        ToWorkerMessage::DeleteData(TaskIdMsg { id: TaskId(1) })
    ));
    comm.check_need_scheduling();
    comm.emptiness_check();

    assert!(!worker_has_task(&core, 101, &t1));
    assert!(!worker_has_task(&core, 102, &t1));

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(1.into(), StealResponse::NotHere)],
        },
    );

    comm.emptiness_check();

    assert!(!worker_has_task(&core, 101, &t1));
    assert!(!worker_has_task(&core, 102, &t1));
}

#[test]
fn test_running_before_steal_response() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    submit_test_tasks(&mut core, &[&t1]);
    start_on_worker(&mut core, 1, 101);
    start_stealing(&mut core, 1, 102);
    assert!(worker_has_task(&core, 102, &t1));

    let mut comm = create_test_comm();
    on_task_running(&mut core, &mut comm, 101.into(), 1.into());
    comm.check_need_scheduling();
    comm.emptiness_check();

    assert!(worker_has_task(&core, 101, &t1));
    assert!(!worker_has_task(&core, 102, &t1));

    on_steal_response(
        &mut core,
        &mut comm,
        101.into(),
        StealResponseMsg {
            responses: vec![(1.into(), StealResponse::Running)],
        },
    );

    comm.emptiness_check();
    assert!(worker_has_task(&core, 101, &t1));
    assert!(!worker_has_task(&core, 102, &t1));
}

#[test]
fn test_ready_to_assign_is_empty_after_cancel() {
    let mut core = Core::default();
    let t1 = task(1);
    submit_test_tasks(&mut core, &[&t1]);
    cancel_tasks(&mut core, &[1]);
    assert!(core.take_ready_to_assign().is_empty());
}

#[test]
fn test_after_cancel_messages() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    let t1 = task(1);
    let t2 = task(2);
    let t3 = task(3);
    let t4 = task(4);
    submit_test_tasks(&mut core, &[&t1, &t2, &t3, &t4]);
    start_on_worker(&mut core, 1, 101);
    start_on_worker(&mut core, 2, 101);
    start_on_worker(&mut core, 3, 101);
    start_on_worker(&mut core, 4, 101);

    cancel_tasks(&mut core, &[1, 2, 3, 4]);

    let mut comm = create_test_comm();
    on_task_finished(
        &mut core,
        &mut comm,
        101.into(),
        TaskFinishedMsg {
            id: 1.into(),
            size: 100,
        },
    );
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
            data_type: "".to_string(),
            error_data: vec![],
        },
    );
    comm.emptiness_check();

    on_task_running(&mut core, &mut comm, 101.into(), 4.into());
    comm.emptiness_check();
}

#[test]
fn lost_worker_with_running_and_assign_tasks() {
    let mut core = Core::default();
    create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_1(&mut core);

    let t40 = task(40);
    let t41 = task(41);
    submit_test_tasks(&mut core, &[&t40, &t41]);

    start_on_worker(&mut core, 11, 101);
    start_on_worker(&mut core, 12, 101);
    start_on_worker(&mut core, 40, 101);
    start_on_worker(&mut core, 41, 100);

    fail_steal(&mut core, 12, 101, 100);
    start_stealing(&mut core, 40, 100);
    start_stealing(&mut core, 41, 101);

    core.assert_running(&[12]);
    assert_eq!(
        core.get_task_by_id_or_panic(12.into()).get().instance_id,
        0.into()
    );

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
        vec![(WorkerId::new(101), vec![12].to_ids())]
    );

    assert_eq!(core.take_ready_to_assign().len(), 3);
    core.assert_ready(&[11, 12]);
    assert_eq!(
        core.get_task_by_id_or_panic(12.into()).get().instance_id,
        1.into()
    );
    assert!(core.get_task_by_id_or_panic(40.into()).get().is_ready());
    core.assert_ready(&[40]);
    core.assert_fresh(&[11, 12, 40]);
    assert!(matches!(
        core.get_task_by_id_or_panic(41.into()).get().state,
        TaskRuntimeState::Stealing(WorkerId(100), None)
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

    assert_eq!(core.take_ready_to_assign().len(), 1);
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
    let task_ref = core.get_task_by_id_or_panic(task_id.into()).clone();
    let mut task = task_ref.get_mut();
    scheduler.assign(core, &mut task, task_ref.clone(), worker_id.into());
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
    scheduler.finish_scheduling(&mut comm);
}

fn cancel_tasks<T: Into<TaskId> + Copy>(core: &mut Core, task_ids: &[T]) {
    let mut comm = env::create_test_comm();
    on_cancel_tasks(
        core,
        &mut comm,
        &task_ids.iter().map(|&v| v.into()).collect::<Vec<_>>(),
    );
}

fn check_worker_tasks_exact(core: &Core, worker_id: u32, tasks: &[&TaskRef]) {
    let worker = core.get_worker_by_id_or_panic(worker_id.into());
    assert_eq!(worker.tasks().len(), tasks.len());
    for task in tasks {
        assert!(worker.tasks().contains(&task.get().id));
    }
}

fn worker_has_task(core: &Core, worker_id: u32, task: &TaskRef) -> bool {
    core.get_worker_by_id_or_panic(worker_id.into())
        .tasks()
        .contains(&task.get().id)
}

#[test]
fn test_task_deps() {
    let mut core = Core::default();
    //create_test_workers(&mut core, &[1, 1, 1]);
    submit_example_3(&mut core);
    assert_eq!(core.get_read_to_assign().len(), 2);
    create_test_workers(&mut core, &[1]);
    start_and_finish_on_worker(&mut core, 2, 100, 0);
    core.assert_waiting(&[3, 4, 6]);
    core.assert_ready(&[5]);

    start_and_finish_on_worker(&mut core, 1, 100, 0);
    core.assert_waiting(&[6]);
    core.assert_ready(&[3, 4, 5]);
}
