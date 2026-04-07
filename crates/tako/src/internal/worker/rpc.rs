use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, Stream, StreamExt};
use orion::aead::SecretKey;
use orion::aead::streaming::StreamOpener;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio::time::{MissedTickBehavior, sleep};

use crate::WorkerId;
use crate::comm::{ConnectionRegistration, RegisterWorker};
use crate::hwstats::{WorkerHwState, WorkerHwStateMessage};
use crate::internal::common::WrappedRcRefCell;
use crate::internal::common::resources::Allocation;
use crate::internal::common::resources::map::ResourceIdMap;
use crate::internal::messages::worker::{
    FromWorkerMessage, RetractResponseMsg, TaskResourceAllocation, TaskUpdates, ToWorkerMessage,
    WorkerOverview, WorkerRegistrationResponse, WorkerStopReason, WorkerTaskUpdate,
};
use crate::internal::server::rpc::ConnectionDescriptor;
use crate::internal::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, seal_message, serialize,
};
use crate::internal::transfer::transport::make_protocol_builder;
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::configuration::{
    ServerLostPolicy, WorkerConfiguration, sync_worker_configuration,
};
use crate::internal::worker::hwmonitor::HwSampler;
use crate::internal::worker::localcomm::handle_local_comm;
use crate::internal::worker::reactor::compute_tasks;
use crate::internal::worker::state::{WorkerState, WorkerStateRef};
use crate::launcher::TaskLauncher;
use futures::future::Either;
use tokio::sync::Notify;

async fn start_listener() -> crate::Result<(TcpListener, u16)> {
    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let listener = TcpListener::bind(address).await?;
    let port = listener.local_addr()?.port();
    log::info!("Listening on port {port}");
    Ok((listener, port))
}

async fn connect_to_server(addresses: &[SocketAddr]) -> crate::Result<(TcpStream, SocketAddr)> {
    log::info!("Connecting to server (candidate addresses = {addresses:?})");

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(addresses).await {
            Ok(stream) => {
                let address = stream.peer_addr()?;
                log::debug!("Connected to server at {address:?}");
                return Ok((stream, address));
            }
            Err(e) => {
                log::error!("Could not connect to server, error: {e}");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Result::Err(crate::Error::GenericError(
        "Server could not be connected".into(),
    ))
}

pub async fn connect_to_server_and_authenticate(
    server_addresses: &[SocketAddr],
    secret_key: Option<Arc<SecretKey>>,
) -> crate::Result<ConnectionDescriptor> {
    if secret_key.is_none() {
        log::warn!("No worker key: Unauthenticated and unencrypted connection to server");
    }
    let (stream, address) = connect_to_server(server_addresses).await?;
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();
    let (sealer, opener) =
        do_authentication(0, "worker", "server", secret_key, &mut writer, &mut reader).await?;
    Ok(ConnectionDescriptor {
        address,
        receiver: reader,
        sender: writer,
        sealer,
        opener,
    })
}

// Maximum time to wait for running tasks to be shutdown when worker ends.
const MAX_WAIT_FOR_RUNNING_TASKS_SHUTDOWN: Duration = Duration::from_secs(5);

/// Connects to the server and starts a message receiving loop.
/// The worker will attempt to clean up after itself once it's stopped or once stop_flag is notified.
pub async fn run_worker(
    scheduler_addresses: Vec<SocketAddr>,
    mut configuration: WorkerConfiguration,
    secret_key: Option<Arc<SecretKey>>,
    launcher_setup: impl Fn(&str, WorkerId) -> Box<dyn TaskLauncher>,
    stop_flag: Arc<Notify>,
) -> crate::Result<(
    (WorkerId, WorkerConfiguration),
    impl Future<Output = crate::Result<()>>,
)> {
    let (_listener, port) = start_listener().await?;
    configuration.listen_address = format!("{}:{}", configuration.hostname, port);
    let ConnectionDescriptor {
        mut sender,
        mut receiver,
        mut opener,
        mut sealer,
        ..
    } = connect_to_server_and_authenticate(&scheduler_addresses, secret_key.clone()).await?;
    {
        let message = ConnectionRegistration::Worker(RegisterWorker {
            configuration: configuration.clone(),
        });
        let data = serialize(&message)?.into();
        sender.send(seal_message(&mut sealer, data)).await?;
    }

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let heartbeat_interval = configuration.heartbeat_interval;
    let retract_check_interval = configuration.retract_check_interval;
    let time_limit = configuration.time_limit;

    let (worker_id, state_ref) = {
        match timeout(Duration::from_secs(15), receiver.next()).await {
            Ok(Some(data)) => {
                let WorkerRegistrationResponse {
                    worker_id,
                    other_workers,
                    resource_names,
                    resource_rq_map,
                    server_idle_timeout,
                    server_uid,
                    worker_overview_interval_override,
                } = open_message(&mut opener, &data?)?;

                sync_worker_configuration(&mut configuration, server_idle_timeout);

                let comm = WorkerComm::new(queue_sender);
                let launcher = launcher_setup(&server_uid, worker_id);
                let state_ref = WorkerStateRef::new(
                    comm,
                    worker_id,
                    configuration.clone(),
                    ResourceIdMap::from_vec(resource_names),
                    resource_rq_map,
                    launcher,
                    server_uid,
                );

                {
                    let mut state = state_ref.get_mut();
                    state.worker_overview_interval_override = worker_overview_interval_override;
                    for worker_info in other_workers {
                        state.new_worker(worker_info);
                    }
                }

                (worker_id, state_ref)
            }
            Ok(None) => panic!("Connection closed without receiving registration response"),
            Err(_) => panic!("Did not receive worker registration response"),
        }
    };

    let local_conn_listener = state_ref.get().lc_state.borrow().create_listener()?;
    let local_comm_fut = handle_local_comm(local_conn_listener, state_ref.clone());

    let heartbeat_fut = heartbeat_process(heartbeat_interval, state_ref.clone());
    let retract_check_fut = retract_check_process(retract_check_interval, state_ref.clone());
    let overview_fut = send_overview_loop(state_ref.clone());

    let time_limit_fut = match time_limit {
        None => Either::Left(futures::future::pending::<()>()),
        Some(d) => Either::Right(tokio::time::sleep(d)),
    };
    let future = async move {
        let send_loop = forward_queue_to_sealed_sink(queue_receiver, sender, sealer);
        tokio::pin! {
            let send_loop = send_loop;
        }

        let result: crate::Result<Option<FromWorkerMessage>> = tokio::select! {
            r = worker_message_loop(state_ref.clone(), receiver, opener) => {
                log::debug!("Server read connection has disconnected");
                r.map(|_| None)
            }
            r = &mut send_loop => {
                log::debug!("Server write connection has disconnected");
                r.map_err(|e| e.into()).map(|_| None)
            },
            _ = time_limit_fut => {
                log::info!("Time limit reached");
                Ok(Some(FromWorkerMessage::Stop(WorkerStopReason::TimeLimitReached)))
            }
            _ = stop_flag.notified() => {
                log::info!("Worker received an external stop notification");
                Ok(Some(FromWorkerMessage::Stop(WorkerStopReason::Interrupted)))
            }
            _ = heartbeat_fut => { unreachable!() }
            _ = retract_check_fut => { unreachable!() }
            _ = overview_fut => { unreachable!() }
            _ = local_comm_fut => { unreachable!() }
        };

        // Handle sending stop info to the server and finishing running tasks gracefully.
        let result = match result {
            Ok(Some(msg)) => {
                // Worker wants to end gracefully, send message to the server
                {
                    state_ref.get_mut().comm().send_message_to_server(msg);
                    state_ref.get_mut().comm().drop_sender();
                }
                send_loop.await?;
                Ok(())
            }
            Ok(None) => {
                // Graceful shutdown from server
                Ok(())
            }
            Err(e) => {
                // Server has disconnected
                tokio::select! {
                    r = finish_tasks_on_server_lost(state_ref.clone()) => r
                }
                Err(e)
            }
        };

        // At this point, there can still be some tasks that are running.
        // We cancel them here to make sure that we do not leak their spawned processes, if possible.
        // The futures of the tasks are scheduled onto the current tokio Runtime using spawn_local,
        // therefore we do not need to await any specific future to drive them forward.
        // try_start_tasks is not being polled, therefore no new tasks should be started.
        cancel_running_tasks_on_worker_end(state_ref).await;
        result
    };

    // Provide a local task set for spawning futures
    let future = async move {
        let set = tokio::task::LocalSet::new();
        set.run_until(future).await
    };

    Ok(((worker_id, configuration), future))
}

async fn finish_tasks_on_server_lost(state: WorkerStateRef) {
    let on_server_lost = state.get().configuration.on_server_lost.clone();
    match on_server_lost {
        ServerLostPolicy::Stop => {}
        ServerLostPolicy::FinishRunning => {
            let notify = {
                let mut state = state.get_mut();
                state.drop_non_running_tasks();

                if !state.is_empty() {
                    let notify = Rc::new(Notify::new());
                    state.comm().set_idle_worker_notify(notify.clone());
                    Some(notify)
                } else {
                    None
                }
            };
            if let Some(notify) = notify {
                log::info!("Waiting for finishing running tasks");
                notify.notified().await;
                log::info!("All running tasks were finished");
            } else {
                log::info!("No running tasks remain")
            }
        }
    }
}

async fn cancel_running_tasks_on_worker_end(state: WorkerStateRef) {
    let notify = {
        let mut state = state.get_mut();
        state.drop_non_running_tasks();
        if state.running_tasks.is_empty() {
            return;
        }
        for task in state.running_tasks.values_mut() {
            task.cancel();
        }
        let notify = Rc::new(Notify::new());
        state.comm().set_idle_worker_notify(notify.clone());
        notify
    };
    log::info!("Waiting for stopping running tasks");
    match tokio::time::timeout(MAX_WAIT_FOR_RUNNING_TASKS_SHUTDOWN, notify.notified()).await {
        Ok(_) => {
            log::info!("All running tasks were stopped");
        }
        Err(_) => {
            log::info!("Timed out while waiting for running tasks to stop");
        }
    }
}

/// Repeatedly sends a heartbeat message to the server.
async fn heartbeat_process(heartbeat_interval: Duration, state_ref: WrappedRcRefCell<WorkerState>) {
    let mut interval = tokio::time::interval(heartbeat_interval);
    loop {
        interval.tick().await;
        {
            let mut state = state_ref.get_mut();
            state
                .comm()
                .send_message_to_server(FromWorkerMessage::Heartbeat);
        }
        log::debug!("Heartbeat sent");
    }
}

/// Repeatedly check overdue task
async fn retract_check_process(check_interval: Duration, state_ref: WrappedRcRefCell<WorkerState>) {
    let mut interval = tokio::time::interval(check_interval);
    loop {
        interval.tick().await;
        {
            let mut state = state_ref.get_mut();
            if !state.prefilled_tasks.is_empty()
                && let Some(remaining_time) = state.remaining_time()
            {
                log::debug!("Checking tasks for retract");
                let mut to_remove = Vec::new();
                let mut updates = TaskUpdates::new();
                for (rq_id, tasks) in &state.prefilled_tasks {
                    let rqv = state.resource_rq_map.get(*rq_id);
                    if remaining_time < rqv.min_time() {
                        to_remove.push(*rq_id);
                        for task in tasks {
                            // Hard reject, we never unblock this rejection so we do not need to update blocked requests
                            updates.push(WorkerTaskUpdate::RejectRequest {
                                task_id: task.id,
                                rv_id: None,
                            });
                        }
                    }
                }
                if !updates.is_empty() {
                    state
                        .comm()
                        .send_message_to_server(FromWorkerMessage::TaskUpdate(updates));
                    for rq_id in to_remove {
                        state.prefilled_tasks.remove(&rq_id);
                    }
                }
            }
        }
    }
}

pub(crate) fn process_worker_message(state: &mut WorkerState, message: ToWorkerMessage) -> bool {
    match message {
        ToWorkerMessage::ComputeTasks(msg) => {
            compute_tasks(state, msg);
        }
        ToWorkerMessage::RetractTasks(msg) => {
            log::debug!("Steal {} attempts", msg.ids.len());
            let responses = state.retract_tasks(&msg.ids);
            log::debug!(
                "Retract attempt: tried: {:?}, successful: {responses:?}",
                msg.ids
            );
            if !msg.ids.is_empty() {
                let message = FromWorkerMessage::RetractResponse(RetractResponseMsg {
                    retracted: responses,
                });
                state.comm().send_message_to_server(message);
            }
        }
        ToWorkerMessage::CancelTasks(msg) => {
            for task_id in msg.ids {
                state.cancel_task(task_id);
            }
        }
        ToWorkerMessage::NewWorker(msg) => {
            state.new_worker(msg);
        }
        ToWorkerMessage::LostWorker(worker_id) => {
            state.remove_worker(worker_id);
        }
        ToWorkerMessage::Stop => {
            log::info!("Received stop command");
            return true;
        }
        ToWorkerMessage::SetOverviewIntervalOverride(r#override) => {
            state.worker_overview_interval_override = r#override;
        }
        ToWorkerMessage::NewResourceRequest(rq_id, rqv) => {
            let new_id = state.register_resource_rq(rqv);
            assert_eq!(rq_id, new_id);
        }
    }
    false
}

/// Runs until there are messages coming from the server.
async fn worker_message_loop(
    state_ref: WorkerStateRef,
    mut stream: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    mut opener: Option<StreamOpener>,
) -> crate::Result<()> {
    while let Some(data) = stream.next().await {
        let data = data?;
        let message: ToWorkerMessage = open_message(&mut opener, &data)?;
        let mut state = state_ref.get_mut();
        if process_worker_message(&mut state, message) {
            return Ok(());
        }
    }
    log::debug!("Connection to server is closed");
    Err("Server connection closed".into())
}

/// This is a background overview interval loop.
///
/// If overview is enabled, the loop wakes up every <overview-interval> interval and gathers HW
/// stats.
/// If overview is disabled, the loop wakes up every second to check if it should re-enable worker
/// overviews.
///
/// This approach is used because the server can temporarily enable worker overviews even if they
/// were originally disabled, so we need to always run the loop.
/// When overviews are currently disabled, the loop will essentially do nothing.
async fn send_overview_loop(state_ref: WorkerStateRef) -> crate::Result<()> {
    // Used to send overviews from the gathering thread
    let (overview_tx, mut overview_rx) = tokio::sync::mpsc::channel::<WorkerHwState>(1);
    // Used to trigger overview collection from the main thread
    let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel::<()>(1);

    let initial_overview = state_ref.get().configuration.overview_configuration.clone();

    // Fetching the HW state performs blocking I/O, therefore we should do it in a separate thread.
    std::thread::spawn(move || -> crate::Result<()> {
        let mut sampler = HwSampler::init(initial_overview.gpu_families)?;
        while let Some(_trigger) = trigger_rx.blocking_recv() {
            let hw_state = sampler.fetch_hw_state()?;
            log::info!("Gathering HW stats");
            if let Err(error) = overview_tx.blocking_send(hw_state) {
                log::error!("Cannot send HW state to overview loop: {error:?}");
                break;
            }
        }
        Ok(())
    });

    // How often should we check if we should re-enable overview collection
    let idle_check_duration = Duration::from_secs(1);

    let create_interval = |duration| {
        let mut interval = tokio::time::interval(duration);
        // If we miss a deadline, perform the next step after `duration` is elapsed from `tick()`
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick is immediate, so skip it to avoid gathering the overview twice in a
        // row
        interval.reset();
        interval
    };

    let mut poll_interval = create_interval(
        initial_overview
            .send_interval
            .unwrap_or(idle_check_duration),
    );

    loop {
        poll_interval.tick().await;

        let current_overview_duration = {
            let state = state_ref.get();
            state
                .configuration
                .overview_configuration
                .send_interval
                .or(state.worker_overview_interval_override)
        };
        if let Some(current_duration) = current_overview_duration {
            let _ = trigger_tx.send(()).await;
            if let Some(hw_state) = overview_rx.recv().await {
                let mut worker_state = state_ref.get_mut();
                let message = FromWorkerMessage::Overview(Box::new(WorkerOverview {
                    id: worker_state.worker_id,
                    running_tasks: worker_state
                        .running_tasks
                        .values()
                        .map(|task| {
                            (
                                task.task.id,
                                resource_allocation_to_msg(
                                    &task.allocation,
                                    worker_state.get_resource_map(),
                                ),
                            )
                            // TODO: Modify this when more cpus are allowed
                        })
                        .collect(),
                    hw_state: Some(WorkerHwStateMessage { state: hw_state }),
                }));
                worker_state.comm().send_message_to_server(message);
            }
            // If the current overview interval is different from before, switch to the current one
            poll_interval = create_interval(current_duration);
        } else if poll_interval.period() != idle_check_duration {
            // The overview is disabled now, switch to idle mode duration if needed.
            poll_interval = create_interval(idle_check_duration);
        }
    }
}

fn resource_allocation_to_msg(
    allocation: &Allocation,
    resource_map: &ResourceIdMap,
) -> TaskResourceAllocation {
    TaskResourceAllocation {
        resources: allocation
            .resources
            .iter()
            .map(
                |alloc| crate::internal::messages::worker::ResourceAllocation {
                    resource: resource_map
                        .get_name(alloc.resource_id)
                        .unwrap_or("unknown")
                        .to_string(),
                    amount: alloc.amount,
                    indices: alloc
                        .indices
                        .iter()
                        .map(|i| (i.index, i.fractions))
                        .collect(),
                },
            )
            .collect(),
    }
}
