use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, Stream, StreamExt};
use orion::aead::streaming::StreamOpener;
use orion::aead::SecretKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;
use tokio::time::timeout;

use crate::comm::{ConnectionRegistration, RegisterWorker};
use crate::hwstats::WorkerHwStateMessage;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::{Allocation, AllocationValue};
use crate::internal::common::WrappedRcRefCell;
use crate::internal::messages::worker::{
    FromWorkerMessage, StealResponseMsg, TaskResourceAllocation, TaskResourceAllocationValue,
    ToWorkerMessage, WorkerOverview, WorkerRegistrationResponse, WorkerStopReason,
};
use crate::internal::server::rpc::ConnectionDescriptor;
use crate::internal::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, seal_message, serialize,
};
use crate::internal::transfer::transport::make_protocol_builder;
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::configuration::{
    sync_worker_configuration, ServerLostPolicy, WorkerConfiguration,
};
use crate::internal::worker::hwmonitor::HwSampler;
use crate::internal::worker::reactor::run_task;
use crate::internal::worker::state::{WorkerState, WorkerStateRef};
use crate::internal::worker::task::Task;
use crate::launcher::TaskLauncher;
use crate::WorkerId;
use futures::future::Either;
use tokio::sync::Notify;

async fn start_listener() -> crate::Result<(TcpListener, u16)> {
    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let listener = TcpListener::bind(address).await?;
    let port = {
        let socketaddr = listener.local_addr()?;
        socketaddr.port()
    };
    log::info!("Listening on port {}", port);
    Ok((listener, port))
}

async fn connect_to_server(addresses: &[SocketAddr]) -> crate::Result<(TcpStream, SocketAddr)> {
    log::info!(
        "Connecting to server (candidate addresses = {:?})",
        addresses
    );

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(addresses).await {
            Ok(stream) => {
                let address = stream.peer_addr()?;
                log::debug!("Connected to server at {address:?}");
                return Ok((stream, address));
            }
            Err(e) => {
                log::error!("Could not connect to server, error: {}", e);
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
    secret_key: &Option<Arc<SecretKey>>,
) -> crate::Result<ConnectionDescriptor> {
    let (stream, address) = connect_to_server(server_addresses).await?;
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();
    let (sealer, opener) = do_authentication(
        0,
        "worker".to_string(),
        "server".to_string(),
        secret_key.clone(),
        &mut writer,
        &mut reader,
    )
    .await?;
    Ok(ConnectionDescriptor {
        address,
        receiver: reader,
        sender: writer,
        sealer,
        opener,
    })
}

pub async fn run_worker(
    scheduler_addresses: &[SocketAddr],
    mut configuration: WorkerConfiguration,
    secret_key: Option<Arc<SecretKey>>,
    launcher_setup: Box<dyn TaskLauncher>,
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
    } = connect_to_server_and_authenticate(scheduler_addresses, &secret_key).await?;
    {
        let message = ConnectionRegistration::Worker(RegisterWorker {
            configuration: configuration.clone(),
        });
        let data = serialize(&message)?.into();
        sender.send(seal_message(&mut sealer, data)).await?;
    }

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let heartbeat_interval = configuration.heartbeat_interval;
    let send_overview_interval = configuration.send_overview_interval;
    let time_limit = configuration.time_limit;

    let (worker_id, state, start_task_notify) = {
        match timeout(Duration::from_secs(15), receiver.next()).await {
            Ok(Some(data)) => {
                let WorkerRegistrationResponse {
                    worker_id,
                    other_workers,
                    resource_names,
                    server_idle_timeout,
                } = open_message(&mut opener, &data?)?;

                sync_worker_configuration(&mut configuration, server_idle_timeout);

                let start_task_notify = Rc::new(Notify::new());
                let comm = WorkerComm::new(queue_sender, start_task_notify.clone());

                let state_ref = WorkerStateRef::new(
                    comm,
                    worker_id,
                    configuration.clone(),
                    secret_key,
                    ResourceMap::from_vec(resource_names),
                    launcher_setup,
                );

                {
                    let mut state = state_ref.get_mut();
                    for worker_info in other_workers {
                        state.new_worker(worker_info);
                    }
                }

                (worker_id, state_ref, start_task_notify)
            }
            Ok(None) => panic!("Connection closed without receiving registration response"),
            Err(_) => panic!("Did not receive worker registration response"),
        }
    };

    let heartbeat_fut = heartbeat_process(heartbeat_interval, state.clone());
    let idle_timeout_fut = match configuration.idle_timeout {
        Some(timeout) => Either::Left(idle_timeout_process(timeout, state.clone())),
        None => Either::Right(futures::future::pending()),
    };

    let overview_fut = match send_overview_interval {
        None => Either::Left(futures::future::pending()),
        Some(interval) => Either::Right(send_overview_loop(state.clone(), interval)),
    };

    let time_limit_fut = match time_limit {
        None => Either::Left(futures::future::pending::<()>()),
        Some(d) => Either::Right(tokio::time::sleep(d)),
    };

    let future = async move {
        let try_start_tasks = task_starter_process(state.clone(), start_task_notify);
        let send_loop = forward_queue_to_sealed_sink(queue_receiver, sender, sealer);
        tokio::pin! {
            let send_loop = send_loop;
            let try_start_tasks = try_start_tasks;
        }

        let result: crate::Result<Option<FromWorkerMessage>> = tokio::select! {
            r = worker_message_loop(state.clone(), receiver, opener) => {
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
            _ = idle_timeout_fut => {
                log::info!("Idle timeout reached");
                Ok(Some(FromWorkerMessage::Stop(WorkerStopReason::IdleTimeout)))
            }
            _ = &mut try_start_tasks => { unreachable!() }
            _ = heartbeat_fut => { unreachable!() }
            _ = overview_fut => { unreachable!() }
        };

        match result {
            Ok(Some(msg)) => {
                // Worker wants to end gracefully, send message to the server
                {
                    state.get_mut().comm().send_message_to_server(msg);
                    state.get_mut().comm().drop_sender();
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
                    _ = &mut try_start_tasks => { unreachable!() }
                    r = finish_tasks_on_server_lost(state) => r
                }
                Err(e)
            }
        }
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

/// Tries to start tasks after a new task appears or some task finishes.
async fn task_starter_process(state_ref: WrappedRcRefCell<WorkerState>, notify: Rc<Notify>) {
    loop {
        notify.notified().await;

        let mut state = state_ref.get_mut();
        state.start_task_scheduled = false;

        let remaining_time = if let Some(limit) = state.configuration.time_limit {
            let life_time = std::time::Instant::now() - state.start_time;
            if life_time >= limit {
                log::debug!("Trying to start a task after time limit");
                break;
            }
            Some(limit - life_time)
        } else {
            None
        };
        loop {
            let (task_map, ready_task_queue) = state.borrow_tasks_and_queue();
            let allocations = ready_task_queue.try_start_tasks(task_map, remaining_time);
            if allocations.is_empty() {
                break;
            }

            for (task_id, allocation, resource_index) in allocations {
                run_task(&mut state, &state_ref, task_id, allocation, resource_index);
            }
        }
    }
}

/// Repeatedly sends a heartbeat message to the server.
async fn heartbeat_process(heartbeat_interval: Duration, state_ref: WrappedRcRefCell<WorkerState>) {
    let mut interval = tokio::time::interval(heartbeat_interval);
    loop {
        interval.tick().await;
        state_ref
            .get_mut()
            .comm()
            .send_message_to_server(FromWorkerMessage::Heartbeat);
        log::debug!("Heartbeat sent");
    }
}

/// Runs until an idle timeout happens.
/// Idle timeout occurs when the worker doesn't have anything to do for the specified duration.
async fn idle_timeout_process(idle_timeout: Duration, state_ref: WrappedRcRefCell<WorkerState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        let state = state_ref.get();
        if !state.has_tasks() && !state.reservation {
            let elapsed = state.last_task_finish_time.elapsed();
            if elapsed > idle_timeout {
                break;
            }
        }
    }
}

pub(crate) fn process_worker_message(state: &mut WorkerState, message: ToWorkerMessage) -> bool {
    match message {
        ToWorkerMessage::ComputeTask(msg) => {
            log::debug!("Task assigned: {}", msg.id);
            let task = Task::new(msg);
            state.add_task(task);
        }
        ToWorkerMessage::StealTasks(msg) => {
            log::debug!("Steal {} attempts", msg.ids.len());
            let responses: Vec<_> = msg
                .ids
                .iter()
                .map(|task_id| {
                    let response = state.steal_task(*task_id);
                    log::debug!("Steal attempt: {}, response {:?}", task_id, response);
                    (*task_id, response)
                })
                .collect();
            let message = FromWorkerMessage::StealResponse(StealResponseMsg { responses });
            state.comm().send_message_to_server(message);
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
        ToWorkerMessage::SetReservation(on_off) => {
            state.reservation = on_off;
            if !on_off {
                state.reset_idle_timer();
            }
        }
        ToWorkerMessage::Stop => {
            log::info!("Received stop command");
            return true;
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

async fn send_overview_loop(state_ref: WorkerStateRef, interval: Duration) -> crate::Result<()> {
    let mut sampler = HwSampler::init()?;
    let mut poll_interval = tokio::time::interval(interval);
    loop {
        poll_interval.tick().await;
        let mut worker_state = state_ref.get_mut();

        let message = FromWorkerMessage::Overview(WorkerOverview {
            id: worker_state.worker_id,
            running_tasks: worker_state
                .running_tasks
                .iter()
                .map(|&task_id| {
                    let task = worker_state.get_task(task_id);
                    let allocation: &Allocation = task.resource_allocation().unwrap();
                    (
                        task_id,
                        resource_allocation_to_msg(allocation, worker_state.get_resource_map()),
                    )
                    // TODO: Modify this when more cpus are allowed
                })
                .collect(),
            hw_state: Some(WorkerHwStateMessage {
                state: sampler.fetch_hw_state()?,
            }),
        });
        worker_state.comm().send_message_to_server(message);
    }
}

fn resource_allocation_to_msg(
    allocation: &Allocation,
    resource_map: &ResourceMap,
) -> TaskResourceAllocation {
    TaskResourceAllocation {
        resources: allocation
            .resources
            .iter()
            .map(
                |alloc| crate::internal::messages::worker::ResourceAllocation {
                    resource: resource_map
                        .get_name(alloc.resource)
                        .unwrap_or("unknown")
                        .to_string(),
                    value: match &alloc.value {
                        AllocationValue::Indices(indices) => {
                            TaskResourceAllocationValue::Indices(indices.iter().cloned().collect())
                        }
                        AllocationValue::Sum(amount) => TaskResourceAllocationValue::Sum(*amount),
                    },
                },
            )
            .collect(),
    }
}
