use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::aead::streaming::{StreamOpener, StreamSealer};
use orion::aead::SecretKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;
use tokio::time::timeout;

use crate::common::resources::map::ResourceMap;
use crate::common::resources::{GenericResourceAllocationValue, ResourceAllocation};
use crate::common::WrappedRcRefCell;
use crate::messages::common::WorkerConfiguration;
use crate::messages::worker::{
    ConnectionRegistration, FromWorkerMessage, RegisterWorker, StealResponseMsg,
    TaskResourceAllocation, ToWorkerMessage, WorkerHwStateMessage, WorkerOverview,
    WorkerRegistrationResponse,
};
use crate::server::rpc::ConnectionDescriptor;
use crate::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, seal_message, serialize,
};
use crate::transfer::transport::make_protocol_builder;
use crate::transfer::DataConnection;
use crate::worker::hwmonitor::HwSampler;
use crate::worker::launcher::TaskLauncher;
use crate::worker::reactor::run_task;
use crate::worker::state::{ServerLostPolicy, WorkerState, WorkerStateRef};
use crate::worker::task::Task;
use crate::WorkerId;
use futures::future::Either;
use tokio::sync::Notify;

async fn start_listener() -> crate::Result<(TcpListener, String)> {
    let address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let listener = TcpListener::bind(address).await?;
    let address = {
        let socketaddr = listener.local_addr()?;
        format!(
            "{}:{}",
            gethostname::gethostname().into_string().unwrap(),
            socketaddr.port()
        )
    };
    log::info!("Listening on {}", address);
    Ok((listener, address))
}

async fn connect_to_server(address: SocketAddr) -> crate::Result<(TcpStream, SocketAddr)> {
    log::info!("Connecting to server {}", address);

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                log::debug!("Connected to server");
                return Ok((stream, address));
            }
            Err(e) => {
                log::error!("Could not connect to {}, error: {}", address, e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Result::Err(crate::Error::GenericError(
        "Server could not be connected".into(),
    ))
}

pub async fn connect_to_server_and_authenticate(
    server_address: SocketAddr,
    secret_key: &Option<Arc<SecretKey>>,
) -> crate::Result<ConnectionDescriptor> {
    let (stream, address) = connect_to_server(server_address).await?;
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

pub async fn handle_server_connection(
    state: WorkerStateRef,
    receiver: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    opener: Option<StreamOpener>,
    queue_receiver: tokio::sync::mpsc::UnboundedReceiver<bytes::Bytes>,
    sender: impl Sink<Bytes, Error = std::io::Error> + Unpin,
    sealer: std::option::Option<StreamSealer>,
) -> crate::Result<()> {
    if let Err(e) = tokio::select! {
        r = worker_message_loop(state.clone(), receiver, opener) => r,
        r = forward_queue_to_sealed_sink(queue_receiver, sender, sealer) => r.map_err(|e| e.into())
    } {
        let on_server_lost = state.get().configuration.on_server_lost.clone();
        log::error!(
            "Server connection failed: {}, on-server-lost: {:?}",
            e,
            on_server_lost,
        );
        match on_server_lost {
            ServerLostPolicy::Stop => Ok(()),
            ServerLostPolicy::FinishRunning => {
                state.get_mut().drop_non_running_tasks();
                let notify = Rc::new(Notify::new());
                state.get_mut().worker_is_empty_notify = Some(notify.clone());
                let is_empty = state.get().is_empty();
                if is_empty {
                    log::info!("No running tasks remain")
                } else {
                    log::info!("Waiting for finishing running tasks");
                    notify.notified().await;
                    log::info!("All running tasks were finished");
                }
                Ok(())
            }
        }
    } else {
        Ok(())
    }
}

pub async fn run_worker(
    scheduler_address: SocketAddr,
    mut configuration: WorkerConfiguration,
    secret_key: Option<Arc<SecretKey>>,
    launcher_setup: Box<dyn TaskLauncher>,
) -> crate::Result<((WorkerId, WorkerConfiguration), impl Future<Output = ()>)> {
    let (_listener, address) = start_listener().await?;
    configuration.listen_address = address;
    let ConnectionDescriptor {
        mut sender,
        mut receiver,
        mut opener,
        mut sealer,
        ..
    } = connect_to_server_and_authenticate(scheduler_address, &secret_key).await?;
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

    let (worker_id, state) = {
        match timeout(Duration::from_secs(15), receiver.next()).await {
            Ok(Some(data)) => {
                let message: WorkerRegistrationResponse = open_message(&mut opener, &data?)?;
                (
                    message.worker_id,
                    WorkerStateRef::new(
                        message.worker_id,
                        configuration.clone(),
                        secret_key,
                        queue_sender,
                        message.worker_addresses,
                        ResourceMap::from_vec(message.resource_names),
                        launcher_setup,
                    ),
                )
            }
            Ok(None) => panic!("Connection closed without receiving registration response"),
            Err(_) => panic!("Did not receive worker registration response"),
        }
    };

    let try_start_tasks = task_starter_process(state.clone());

    let heartbeat = heartbeat_process(heartbeat_interval, state.clone());
    let overview_loop = match send_overview_interval {
        None => Either::Left(futures::future::pending()),
        Some(interval) => Either::Right(send_overview_loop(state.clone(), interval)),
    };

    let time_limit_fut = match time_limit {
        None => Either::Left(futures::future::pending::<()>()),
        Some(d) => Either::Right(tokio::time::sleep(d)),
    };

    Ok(((worker_id, configuration), async move {
        tokio::select! {
            () = try_start_tasks => { unreachable!() }
            _ = handle_server_connection(state.clone(), receiver, opener, queue_receiver, sender, sealer) => {}
            _ = heartbeat => { unreachable!() }
            _ = overview_loop => { unreachable!() }
            _ = time_limit_fut => {
                log::info!("Time limit reached");
            }
        }
    }))
}

/// Tries to start tasks after a new task appears or some task finishes.
async fn task_starter_process(state_ref: WrappedRcRefCell<WorkerState>) {
    let notify = state_ref.get().start_task_notify.clone();
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

            for (task_id, allocation) in allocations {
                run_task(&mut state, &state_ref, task_id, allocation);
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
            .get()
            .send_message_to_server(FromWorkerMessage::Heartbeat);
        log::debug!("Heartbeat sent");
    }
}

async fn worker_message_loop(
    state_ref: WorkerStateRef,
    mut stream: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    mut opener: Option<StreamOpener>,
) -> crate::Result<()> {
    while let Some(data) = stream.next().await {
        let data = data?;
        let message: ToWorkerMessage = open_message(&mut opener, &data)?;
        let mut state = state_ref.get_mut();
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
                state.send_message_to_server(message);
            }
            ToWorkerMessage::CancelTasks(msg) => {
                for task_id in msg.ids {
                    state.cancel_task(task_id);
                }
            }
            ToWorkerMessage::NewWorker(msg) => {
                log::debug!("New worker={} announced at {}", msg.worker_id, &msg.address);
                assert!(state
                    .worker_addresses
                    .insert(msg.worker_id, msg.address)
                    .is_none())
            }
            ToWorkerMessage::Stop => return Ok(()),
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
        let worker_state = state_ref.get();

        let message = FromWorkerMessage::Overview(WorkerOverview {
            id: worker_state.worker_id,
            running_tasks: worker_state
                .running_tasks
                .iter()
                .map(|&task_id| {
                    let task = worker_state.get_task(task_id);
                    let allocation: &ResourceAllocation = task.resource_allocation().unwrap();
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
        worker_state.send_message_to_server(message);
    }
}

fn resource_allocation_to_msg(
    allocation: &ResourceAllocation,
    resource_map: &ResourceMap,
) -> TaskResourceAllocation {
    TaskResourceAllocation {
        cpus: allocation.cpus.to_vec(),
        generic_allocations: allocation
            .generic_allocations
            .iter()
            .map(|alloc| crate::messages::worker::GenericResourceAllocation {
                resource: resource_map
                    .get_name(alloc.resource)
                    .unwrap_or("unknown")
                    .to_string(),
                value: match &alloc.value {
                    GenericResourceAllocationValue::Indices(indices) => {
                        crate::messages::worker::GenericResourceAllocationValue::Indices(
                            indices.iter().cloned().collect(),
                        )
                    }
                    GenericResourceAllocationValue::Sum(amount) => {
                        crate::messages::worker::GenericResourceAllocationValue::Sum(*amount)
                    }
                },
            })
            .collect(),
    }
}

pub async fn connection_initiator(
    listener: TcpListener,
    state_ref: WorkerStateRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let stream = make_protocol_builder().new_framed(socket);

        let state = state_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            connection_rpc_loop(stream, state, address)
                .await
                .expect("Connection failed");
            log::debug!("Connection ended: {}", address);
        });
    }
}

async fn connection_rpc_loop(
    _stream: DataConnection,
    _state_ref: WorkerStateRef,
    _address: SocketAddr,
) -> crate::Result<()> {
    todo!()
    /*enum FetchHelperResult {
        OneShot(tokio::sync::oneshot::Receiver<(SerializationType, Bytes)>),
        DirectResult(DataResponse, Option<Bytes>),
    }

    loop {
        let data = match stream.next().await {
            None => return Ok(()),
            Some(data) => data?,
        };
        let request: DataRequest = rmp_serde::from_slice(&data)?;

        match request {
            DataRequest::FetchRequest(msg) => {
                log::debug!("Object {} request from {} started", msg.task_id, address);
                let result = {
                    /* It is now a little bix complex, as we need to get .await
                       out of borrow of data object and state
                    */
                    let maybe_data_ref = state_ref.get().data_objects.get(&msg.task_id).cloned();
                    let data_ref = match maybe_data_ref {
                        None => {
                            log::debug!("Object is not here");
                            let response = DataResponse::NotAvailable;
                            stream
                                .send(rmp_serde::to_vec_named(&response).unwrap().into())
                                .await?;
                            continue;
                        }
                        Some(x) => x,
                    };
                    let mut data_obj = data_ref.get_mut();
                    match &mut data_obj.state {
                        DataObjectState::Remote(_) => {
                            log::debug!("Object is marked as remote");
                            FetchHelperResult::DirectResult(DataResponse::NotAvailable, None)
                        }
                        DataObjectState::InSubworkers(insw) => {
                            let (sender, receiver) = tokio::sync::oneshot::channel();
                            data_obj.state = start_local_download(
                                &mut state_ref.get_mut(),
                                msg.task_id,
                                insw,
                                Subscriber::OneShot(sender),
                            );
                            FetchHelperResult::OneShot(receiver)
                        }
                        DataObjectState::LocalDownloading(ref mut ld) => {
                            let (sender, receiver) = tokio::sync::oneshot::channel();
                            ld.subscribers.push(Subscriber::OneShot(sender));
                            FetchHelperResult::OneShot(receiver)
                        }
                        DataObjectState::Local(local_data) => {
                            FetchHelperResult::DirectResult(
                                DataResponse::Data(FetchResponseData {
                                    serializer: local_data.serializer.clone(),
                                }),
                                Some(local_data.bytes.clone()),
                            )
                            //let data = rmp_serde::to_vec_named(&).unwrap();

                            /*    Either::Right(
                            stream.send(data.into()).await?;
                            stream.send(local_data.bytes.clone()).await?;
                            log::debug!("Object {} request from {} finished", data_id, address);
                            continue*/
                        }
                        DataObjectState::Removed => unreachable!(),
                    }
                };

                let (out_msg, opt_bytes) = match result {
                    FetchHelperResult::DirectResult(msg, opt_bytes) => (msg, opt_bytes),
                    FetchHelperResult::OneShot(receiver) => match receiver.await {
                        Ok((serializer, bytes)) => (
                            DataResponse::Data(FetchResponseData { serializer }),
                            Some(bytes),
                        ),
                        Err(_) => (DataResponse::NotAvailable, None),
                    },
                };
                let data = serialize(&out_msg).unwrap();
                stream.send(data.into()).await?;
                if let Some(bytes) = opt_bytes {
                    stream.send(bytes).await?;
                }
                log::debug!("Object {} request from {} finished", msg.task_id, address);
            }
            DataRequest::UploadData(msg) => {
                log::debug!("Object {} upload from {} started", msg.task_id, address);
                let data = match stream.next().await {
                    None => {
                        log::error!(
                            "Object {} started to upload but data did not arrived",
                            msg.task_id
                        );
                        return Ok(());
                    }
                    Some(data) => data?,
                };
                let mut error = None;
                {
                    let mut state = state_ref.get_mut();
                    match state.data_objects.get(&msg.task_id) {
                        None => {
                            let data_ref = DataObjectRef::new(
                                msg.task_id,
                                data.len() as u64,
                                DataObjectState::Local(LocalData {
                                    serializer: msg.serializer,
                                    bytes: data.into(),
                                    subworkers: Default::default(),
                                }),
                            );
                            state.add_data_object(data_ref);
                        }
                        Some(data_ref) => {
                            let data_obj = data_ref.get();
                            match &data_obj.state {
                                DataObjectState::Remote(_) => {
                                    /* set the data and check waiting tasks */
                                    todo!()
                                }
                                DataObjectState::InSubworkers(_)
                                | DataObjectState::LocalDownloading(_) => {
                                    log::debug!(
                                        "Uploaded data {} is already in subworkers",
                                        &msg.task_id
                                    );
                                    todo!()
                                }
                                DataObjectState::Local(local) => {
                                    log::debug!(
                                        "Uploaded data {} is already in worker",
                                        &msg.task_id
                                    );
                                    if local.serializer != msg.serializer
                                        || local.bytes.len() != data.len()
                                    {
                                        log::error!(
                                            "Incompatible data {} was data uploaded",
                                            &msg.task_id
                                        );
                                        error = Some("Incompatible data was uploaded".into());
                                    }
                                }
                                DataObjectState::Removed => unreachable!(),
                            }
                        }
                    };
                };

                log::debug!("Object {} upload from {} finished", &msg.task_id, address);
                let response = DataResponse::DataUploaded(UploadResponseMsg {
                    task_id: msg.task_id,
                    error,
                });
                let data = serialize(&response).unwrap();
                stream.send(data.into()).await?;
            }
        }
    }*/
}
