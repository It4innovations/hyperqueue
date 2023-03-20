use crate::internal::common::resources::request::{ResourceRequestEntries, ResourceRequestEntry};
use crate::internal::common::resources::ResourceRequestVariants;
use crate::internal::messages::worker::{
    ComputeTaskMsg, NewWorkerMsg, ToWorkerMessage, WorkerResourceCounts,
};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::tests::utils::resources::ResourceRequestBuilder;
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::rpc::process_worker_message;
use crate::internal::worker::state::WorkerStateRef;
use crate::launcher::{LaunchContext, StopReason, TaskLaunchData, TaskLauncher};
use crate::resources::{
    AllocationRequest, ResourceDescriptor, ResourceMap, ResourceRequest, TimeRequest,
};
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{Set, TaskId, WorkerId};
use smallvec::smallvec;
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
        group: "default".to_string(),
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
    let resource_map = ResourceMap::from_vec(
        config
            .resources
            .resources
            .iter()
            .map(|x| x.name.clone())
            .collect(),
    );
    WorkerStateRef::new(
        WorkerComm::new_test_comm(),
        WorkerId::from(100),
        config,
        None,
        resource_map,
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
        body: Default::default(),
    }
}

#[test]
fn test_worker_start_task() {
    let config = create_test_worker_config();
    let state_ref = create_test_worker_state(config);
    let mut msg = create_dummy_compute_msg(7.into());
    /*let mut entries = ResourceRequestEntries::new();
    entries.push(ResourceRequestEntry {
        resource_id: 0.into(),
        request: AllocationRequest::Compact(3),
    });
    let rq = ResourceRequest::new(0, TimeRequest::default(), entries);*/
    let rq = ResourceRequestBuilder::default().cpus(3).finish_v();
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
fn test_worker_start_task_resource_variants() {
    let config = create_test_worker_config();
    let state_ref = create_test_worker_state(config);
    let mut msg = create_dummy_compute_msg(7.into());
    let rq1 = ResourceRequestBuilder::default().cpus(2).add(1, 1).finish();
    let rq2 = ResourceRequestBuilder::default().cpus(4).finish();
    let rq = ResourceRequestVariants::new(smallvec![rq1.clone(), rq2.clone()]);
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

#[test]
fn test_worker_other_workers() {
    let state_ref = create_test_worker_state(create_test_worker_config());
    let mut state = state_ref.get_mut();
    assert!(state.worker_addresses.is_empty());
    assert!(state.ready_task_queue.worker_resources().is_empty());

    let r1 = WorkerResourceCounts {
        n_resources: vec![2, 0, 1],
    };
    let wr1 = WorkerResources::from_transport(r1.clone());

    let r2 = WorkerResourceCounts {
        n_resources: vec![2, 1],
    };
    let wr2 = WorkerResources::from_transport(r2.clone());

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
    assert_eq!(state.ready_task_queue.worker_resources().len(), 1);
    let mut s = Set::new();
    s.insert(WorkerId::from(30));
    assert_eq!(state.ready_task_queue.worker_resources()[&wr1], s);

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
    assert_eq!(state.ready_task_queue.worker_resources().len(), 1);
    s.insert(WorkerId::from(40));
    assert_eq!(state.ready_task_queue.worker_resources()[&wr1], s);

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
    assert_eq!(state.ready_task_queue.worker_resources().len(), 2);
    let mut t = Set::new();
    t.insert(WorkerId::from(50));
    assert_eq!(state.ready_task_queue.worker_resources()[&wr1], s);
    assert_eq!(state.ready_task_queue.worker_resources()[&wr2], t);

    process_worker_message(&mut state, ToWorkerMessage::LostWorker(40.into()));
    assert_eq!(state.worker_addresses.len(), 2);
    assert!(state.worker_addresses.get(&WorkerId::new(40)).is_none());
    assert_eq!(state.ready_task_queue.worker_resources().len(), 2);
    s.remove(&WorkerId::new(40));
    assert_eq!(state.ready_task_queue.worker_resources()[&wr1], s);
    assert_eq!(state.ready_task_queue.worker_resources()[&wr2], t);

    process_worker_message(&mut state, ToWorkerMessage::LostWorker(30.into()));
    assert!(state
        .ready_task_queue
        .worker_resources()
        .get(&wr1)
        .is_none());
    assert_eq!(state.ready_task_queue.worker_resources()[&wr2], t);
}
