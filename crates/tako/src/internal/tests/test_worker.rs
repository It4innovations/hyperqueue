use crate::internal::common::resources::request::GenericResourceRequests;
use crate::internal::messages::worker::{ComputeTaskMsg, ToWorkerMessage};
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::rpc::process_worker_message;
use crate::internal::worker::state::WorkerStateRef;
use crate::launcher::{LaunchContext, StopReason, TaskLaunchData, TaskLauncher};
use crate::resources::{CpuRequest, ResourceDescriptor, ResourceMap, ResourceRequest, TimeRequest};
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{TaskId, WorkerId};
use std::time::Duration;
use tokio::sync::oneshot::Receiver;

#[derive(Default)]
struct TestLauncher;

impl TaskLauncher for TestLauncher {
    fn build_task(
        &self,
        _ctx: LaunchContext,
        _stop_receiver: Receiver<StopReason>,
    ) -> crate::Result<TaskLaunchData> {
        // Test should not directly call this function
        unreachable!()
    }
}

fn create_test_worker_config() -> WorkerConfiguration {
    WorkerConfiguration {
        resources: ResourceDescriptor::simple(4),
        listen_address: "test1:123".into(),
        hostname: "test1".to_string(),
        work_dir: Default::default(),
        log_dir: Default::default(),
        heartbeat_interval: Duration::from_millis(1000),
        send_overview_interval: Some(Duration::from_millis(1000)),
        idle_timeout: None,
        time_limit: None,
        on_server_lost: ServerLostPolicy::Stop,
        extra: Default::default(),
    }
}

fn create_test_worker_state(config: WorkerConfiguration) -> WorkerStateRef {
    WorkerStateRef::new(
        WorkerComm::new_test_comm(),
        WorkerId::from(100),
        config,
        None,
        ResourceMap::default(),
        Box::new(TestLauncher::default()),
    )
}

fn create_dummy_compute_msg(task_id: TaskId) -> ComputeTaskMsg {
    ComputeTaskMsg {
        id: task_id,
        instance_id: Default::default(),
        user_priority: 0,
        scheduler_priority: 0,
        resources: Default::default(),
        time_limit: None,
        n_outputs: 0,
        node_list: vec![],
        body: vec![],
    }
}

#[test]
fn test_worker_start_task() {
    let config = create_test_worker_config();
    let state_ref = create_test_worker_state(config);
    let mut msg = create_dummy_compute_msg(7.into());
    let rq = ResourceRequest::new(
        0,
        CpuRequest::Compact(3),
        TimeRequest::default(),
        GenericResourceRequests::default(),
    );
    msg.resources = rq.clone();
    let mut state = state_ref.get_mut();
    process_worker_message(&mut state, ToWorkerMessage::ComputeTask(msg));
    let comm = state.comm().test();
    comm.check_start_task_notifications(1);
    comm.check_emptiness();

    assert_eq!(state.find_task(7.into()).unwrap().id, TaskId::new(7));
    assert!(state.running_tasks.is_empty());
    let requests = state.ready_task_queue.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0], rq);
}

#[test]
fn test_worker_reservation() {
    let state_ref = create_test_worker_state(create_test_worker_config());
    let mut state = state_ref.get_mut();
    let finish_time = state.last_task_finish_time;
    assert!(!state.reservation);
    process_worker_message(&mut state, ToWorkerMessage::SetReservation(true));
    let comm = state.comm().test();
    comm.check_emptiness();
    assert!(state.reservation);
    assert_eq!(state.last_task_finish_time, finish_time);
    process_worker_message(&mut state, ToWorkerMessage::SetReservation(false));
    let comm = state.comm().test();
    comm.check_emptiness();
    assert!(!state.reservation);
    assert_ne!(state.last_task_finish_time, finish_time);
}
