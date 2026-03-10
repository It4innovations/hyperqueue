use crate::internal::messages::worker::{
    FromWorkerMessage, NewWorkerMsg, TaskRunningMsg, ToWorkerMessage, WorkerResourceCounts,
    WorkerTaskUpdate,
};
use crate::internal::tests::utils::resources::ra_builder;
use crate::internal::worker::rpc::process_worker_message;
use crate::resources::ResourceRqId;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::wenv::WorkerTestEnv;
use crate::tests::utils::worker::WorkerBuilder;
use crate::{ResourceVariantId, Set, TaskId, WorkerId};
use std::ops::Deref;

fn pop_task_running(msg: &mut FromWorkerMessage) -> TaskRunningMsg {
    match msg {
        FromWorkerMessage::TaskUpdate(update) => match update.pop().unwrap() {
            WorkerTaskUpdate::Running(msg) => msg,
            _ => unreachable!(),
        },
        _ => {
            unreachable!()
        }
    }
}

fn pop_task_rejection(msg: &mut FromWorkerMessage) -> TaskId {
    match msg {
        FromWorkerMessage::TaskUpdate(update) => match update.pop().unwrap() {
            WorkerTaskUpdate::RejectRequest { task_id, rv_id: _ } => task_id,
            _ => unreachable!(),
        },
        _ => {
            unreachable!()
        }
    }
}

fn check_empty_update(msg: &mut FromWorkerMessage) {
    match msg {
        FromWorkerMessage::TaskUpdate(update) => {
            assert!(update.is_empty());
        }
        _ => {
            unreachable!()
        }
    }
}

#[test]
fn test_worker_start_task() {
    let mut rt = WorkerTestEnv::new(&WorkerBuilder::new(4).time_limit_s(100));

    let msg = rt.compute_msg(TaskId::new_test(7), 0, &TaskBuilder::new().cpus(3));

    let mut state = rt.state().get_mut();
    process_worker_message(&mut state, ToWorkerMessage::ComputeTasks(msg));
    let comm = state.comm().test();
    comm.check_task_started(1);
    let mut msg = comm.take_messages(1);
    assert_eq!(pop_task_running(&mut msg[0]).task_id, TaskId::new_test(7));
    check_empty_update(&mut msg[0]);
    comm.check_emptiness();

    let task = state.get_running_task(7.into());
    assert_eq!(task.task.id, TaskId::new_test(7));
    assert_eq!(state.running_tasks.len(), 1);
    assert!(state.running_tasks.find(&TaskId::new_test(7)).is_some());
    drop(state);

    let msg = rt.compute_msg(TaskId::new_test(9), 0, &TaskBuilder::new().cpus(3));

    let mut state = rt.state().get_mut();
    process_worker_message(&mut state, ToWorkerMessage::ComputeTasks(msg));

    let comm = state.comm().test();
    let mut msg = comm.take_messages(1);
    assert_eq!(pop_task_rejection(&mut msg[0]), TaskId::new_test(9));
    check_empty_update(&mut msg[0]);
    comm.check_emptiness();

    assert!(
        state
            .blocked_requests
            .contains(&(ResourceRqId::new(0), ResourceVariantId::new(0)))
    );

    drop(state);

    let msg = rt.compute_msg(
        TaskId::new_test(11),
        0,
        &TaskBuilder::new().cpus(1).time_request(120),
    );
    let rq_id = msg.tasks[0].resource_rq_id;

    let mut state = rt.state().get_mut();
    process_worker_message(&mut state, ToWorkerMessage::ComputeTasks(msg));

    let comm = state.comm().test();
    let mut msg = comm.take_messages(1);
    assert_eq!(pop_task_rejection(&mut msg[0]), TaskId::new_test(11));
    check_empty_update(&mut msg[0]);
    comm.check_emptiness();

    assert!(
        !state
            .blocked_requests
            .contains(&(rq_id, ResourceVariantId::new(0)))
    );

    drop(state);

    let msg = rt.compute_msg(
        TaskId::new_test(13),
        0,
        &TaskBuilder::new().cpus(1).time_request(90),
    );
    let rq = msg.tasks[0].resource_rq_id;
    let mut state = rt.state().get_mut();
    process_worker_message(&mut state, ToWorkerMessage::ComputeTasks(msg));

    assert!(
        !state
            .blocked_requests
            .contains(&(rq, ResourceVariantId::new(0)))
    );

    let comm = state.comm().test();
    comm.check_task_started(1);
    let mut msg = comm.take_messages(1);
    assert_eq!(pop_task_running(&mut msg[0]).task_id, TaskId::new_test(13));
    check_empty_update(&mut msg[0]);
    comm.check_emptiness();
}

#[test]
fn test_worker_other_workers() {
    let mut rt = WorkerTestEnv::new(&WorkerBuilder::new(4).time_limit_s(100));
    let mut state = rt.state().get_mut();
    assert!(state.worker_addresses.is_empty());

    let r1 = WorkerResourceCounts {
        n_resources: ra_builder(&[2, 0, 1]).deref().clone(),
    };

    let r2 = WorkerResourceCounts {
        n_resources: ra_builder(&[2, 1]).deref().clone(),
    };

    process_worker_message(
        &mut state,
        ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: 30.into(),
            address: "abc".to_string(),
            resources: r1.clone(),
        }),
    );
    let comm = state.comm().test();
    comm.check_emptiness();
    assert_eq!(state.worker_addresses.len(), 1);
    assert_eq!(state.worker_addresses[&WorkerId::from(30)], "abc");
    let mut s = Set::new();
    s.insert(WorkerId::from(30));

    process_worker_message(
        &mut state,
        ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: 40.into(),
            address: "efg".to_string(),
            resources: r1.clone(),
        }),
    );

    let comm = state.comm().test();
    comm.check_emptiness();
    assert_eq!(state.worker_addresses.len(), 2);
    assert_eq!(state.worker_addresses[&WorkerId::from(30)], "abc");
    assert_eq!(state.worker_addresses[&WorkerId::from(40)], "efg");
    s.insert(WorkerId::from(40));

    process_worker_message(
        &mut state,
        ToWorkerMessage::NewWorker(NewWorkerMsg {
            worker_id: 50.into(),
            address: "xyz".to_string(),
            resources: r2.clone(),
        }),
    );

    let comm = state.comm().test();
    comm.check_emptiness();
    assert_eq!(state.worker_addresses.len(), 3);

    process_worker_message(&mut state, ToWorkerMessage::LostWorker(40.into()));
    assert_eq!(state.worker_addresses.len(), 2);
    assert!(state.worker_addresses.get(&WorkerId::new(40)).is_none());
    process_worker_message(&mut state, ToWorkerMessage::LostWorker(30.into()));
}
