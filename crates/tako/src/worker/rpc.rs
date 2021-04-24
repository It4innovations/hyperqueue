use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::FuturesUnordered;
use futures::{SinkExt, Stream, StreamExt};
use orion::aead::streaming::StreamOpener;
use orion::aead::SecretKey;
use tokio::net::lookup_host;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::LocalSet;
use tokio::time::sleep;
use tokio::time::timeout;

use crate::common::data::SerializationType;
use crate::messages::common::WorkerConfiguration;
use crate::messages::worker::{
    FromWorkerMessage, RegisterWorker, StealResponseMsg, ToWorkerMessage, WorkerOverview,
    WorkerRegistrationResponse,
};
use crate::server::worker::WorkerId;
use crate::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, seal_message,
};
use crate::transfer::fetch::fetch_data;
use crate::transfer::messages::{DataRequest, DataResponse, FetchResponseData, UploadResponseMsg};
use crate::transfer::transport::{connect_to_worker, make_protocol_builder};
use crate::transfer::DataConnection;
use crate::worker::data::{DataObjectRef, DataObjectState, LocalData, Subscriber};
use crate::worker::reactor::{assign_task, start_local_download};
use crate::worker::state::WorkerStateRef;
use crate::worker::task::TaskRef;
use crate::Priority;
use crate::PriorityTuple;
use std::sync::Arc;
use std::future::Future;

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

async fn connect_to_server(scheduler_address: &str) -> crate::Result<TcpStream> {
    log::info!("Connecting to server {}", scheduler_address);
    let address = lookup_host(&scheduler_address)
        .await?
        .next()
        .expect("Invalid scheduler address");

    let max_attempts = 20;
    for _ in 0..max_attempts {
        match TcpStream::connect(address).await {
            Ok(stream) => {
                log::debug!("Connected to server");
                return Ok(stream);
            }
            Err(e) => {
                log::error!("Could not connect to {}, error: {}", scheduler_address, e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Result::Err(crate::Error::GenericError(
        "Server could not be connected".into(),
    ))
}

pub async fn run_worker(
    scheduler_address: &str,
    mut configuration: WorkerConfiguration,
    secret_key: Option<Arc<SecretKey>>,
) -> crate::Result<((WorkerId, WorkerConfiguration), impl Future<Output=()>)> {
    let (listener, address) = start_listener().await?;
    configuration.listen_address = address;
    let stream = connect_to_server(&scheduler_address).await?;
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();
    let (mut sealer, mut opener) = do_authentication(
        0,
        "worker".to_string(),
        "server".to_string(),
        secret_key.clone(),
        &mut writer,
        &mut reader,
    )
    .await?;

    let taskset = LocalSet::default();

    {
        let message = RegisterWorker {
            configuration: configuration.clone(),
        };
        let data = rmp_serde::to_vec_named(&message)?.into();
        writer.send(seal_message(&mut sealer, data)).await?;
    }

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let (download_sender, download_reader) =
        tokio::sync::mpsc::unbounded_channel::<(DataObjectRef, (Priority, Priority))>();
    let heartbeat_interval = configuration.heartbeat_interval;

    let (worker_id, state) = {
        match timeout(Duration::from_secs(15), reader.next()).await {
            Ok(Some(data)) => {
                let message: WorkerRegistrationResponse = open_message(&mut opener, &data?)?;
                (message.worker_id, WorkerStateRef::new(
                    message.worker_id,
                    configuration.clone(),
                    secret_key,
                    queue_sender,
                    download_sender,
                    message.worker_addresses,
                    message.subworker_definitions,
                ))
            }
            Ok(None) => panic!("Connection closed without receiving registration response"),
            Err(_) => panic!("Did not received worker registration response"),
        }
    };

    let state_ref2 = state.clone();
    let state_ref3 = state.clone();

    let try_start_tasks = async move {
        let notify = state_ref2.get().start_task_notify.clone();
        loop {
            notify.notified().await;
            let mut state = state_ref2.get_mut();
            state.start_task_scheduled = false;
            if state.free_cpus == 0 {
                continue;
            }
            while let Some((task_ref, _)) = state.ready_task_queue.pop() {
                assign_task(&mut state, &task_ref);
                if state.free_cpus == 0 {
                    break;
                }
            }
        }
    };

    let heartbeat = async move {
        let mut interval = tokio::time::interval(heartbeat_interval);
        let data: Bytes = rmp_serde::to_vec_named(&FromWorkerMessage::Heartbeat)
            .unwrap()
            .into();
        loop {
            interval.tick().await;
            state_ref3.get().send_message_to_server(data.clone());
            log::debug!("Heartbeat sent");
        }
    };

    /*log::info!("Starting {} subworkers", ncpus);
    let (subworkers, sw_processes) =
        start_subworkers(&state, subworker_paths, "python3", ncpus).await?;
    log::debug!("Subworkers started");

    state.get_mut().set_subworkers(subworkers);*/

    Ok(((worker_id, configuration), async move {
        tokio::select! {
            () = try_start_tasks => { unreachable!() }
            _ = worker_message_loop(state.clone(), reader, opener) => {
                panic!("Connection to server lost");
            }
            _ = forward_queue_to_sealed_sink(queue_receiver, writer, sealer) => {
                panic!("Cannot send a message to server");
            }
            _result = taskset.run_until(connection_initiator(listener, state.clone())) => {
                panic!("Taskset failed");
            }
            /*idx = sw_processes => {
                panic!("Subworker process {} failed", idx);
            }*/
            _ = worker_data_downloader(state, download_reader) => {
                unreachable!()
            }
            _ = heartbeat => { unreachable!() }
        }
    }))
    //Ok(())
}

const MAX_RUNNING_DOWNLOADS: usize = 32;
const MAX_ATTEMPTS: u32 = 8;

async fn download_data(state_ref: WorkerStateRef, data_ref: DataObjectRef) {
    for attempt in 0..MAX_ATTEMPTS {
        let (worker_id, data_id) = {
            let data_obj = data_ref.get();
            let workers = match &data_obj.state {
                DataObjectState::Remote(rs) => &rs.workers,
                DataObjectState::Local(_)
                | DataObjectState::Removed
                | DataObjectState::InSubworkers(_)
                | DataObjectState::LocalDownloading(_) => {
                    /* It is already finished */
                    return;
                }
            };
            if data_obj.consumers.is_empty() {
                // Task that requested data was removed (because of work stealing)
                return;
            }
            let worker_id: WorkerId = state_ref.get_mut().random_choice(&workers).clone();
            (worker_id, data_obj.id)
        };

        let worker_conn = state_ref.get_mut().pop_worker_connection(worker_id);
        let stream = if let Some(stream) = worker_conn {
            stream
        } else {
            let address = state_ref
                .get()
                .get_worker_address(worker_id)
                .unwrap()
                .clone();
            connect_to_worker(address).await.unwrap()
        };

        match fetch_data(stream, data_id).await {
            Ok((stream, data, serializer)) => {
                let mut state = state_ref.get_mut();
                state.return_worker_connection(worker_id, stream);
                state.on_data_downloaded(data_ref, data, serializer);
                return;
            }
            Err(e) => {
                log::error!(
                    "Download of id={} failed; error={}; attempt={}/{}",
                    data_ref.get().id,
                    e,
                    attempt,
                    MAX_ATTEMPTS
                );
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    log::error!(
        "Failed to download id={} after all attemps",
        data_ref.get().id
    );
    todo!();
}

async fn worker_data_downloader(
    state_ref: WorkerStateRef,
    mut stream: tokio::sync::mpsc::UnboundedReceiver<(DataObjectRef, PriorityTuple)>,
) {
    // TODO: Limit downloads, more parallel downloads, respect priorities
    // TODO: Reuse connections
    let mut queue: priority_queue::PriorityQueue<DataObjectRef, PriorityTuple> = Default::default();
    //let mut random = SmallRng::from_entropy();
    //let mut stream = stream;

    let mut running = FuturesUnordered::new();

    loop {
        tokio::select! {
            s = stream.recv() => {
               let (data_ref, priority) = s.unwrap();
               queue.push_increase(data_ref, priority);
               loop {
                    match stream.recv().await {
                        Some((data_ref, priority)) => queue.push_increase(data_ref, priority),
                        None => break,
                    };
               }
            },
            _ = running.next(), if !running.is_empty() => {}
        }

        while running.len() < MAX_RUNNING_DOWNLOADS {
            if let Some((data_ref, _)) = queue.pop() {
                log::debug!("Getting data={} from download queue", data_ref.get().id);
                running.push(download_data(state_ref.clone(), data_ref));
            } else {
                break;
            }
        }
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
            ToWorkerMessage::ComputeTask(mut msg) => {
                log::debug!("Task assigned: {}", msg.id);
                let dep_info = std::mem::take(&mut msg.dep_info);
                let task_ref = TaskRef::new(msg);
                for (task_id, size, workers) in dep_info {
                    state.add_dependancy(&task_ref, task_id, size, workers);
                }
                state.add_task(task_ref);
            }
            ToWorkerMessage::DeleteData(msg) => {
                state.remove_data_by_id(msg.id);
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
                state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap().into());
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
            ToWorkerMessage::RegisterSubworker(sw_def) => {
                state.subworker_definitions.insert(sw_def.id, sw_def);
            }
            ToWorkerMessage::GetOverview => {
                let message = FromWorkerMessage::Overview(WorkerOverview {
                    id: state.worker_id,
                    running_tasks: state
                        .running_tasks
                        .iter()
                        .map(|tr| {
                            let task = tr.get();
                            (task.id, 1) // TODO: Modify this when more cpus are allowed
                        })
                        .collect(),
                    placed_data: state.data_objects.keys().copied().collect(),
                });
                state.send_message_to_server(rmp_serde::to_vec_named(&message).unwrap().into());
            }
        }
    }
    Ok(())
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
    mut stream: DataConnection,
    state_ref: WorkerStateRef,
    address: SocketAddr,
) -> crate::Result<()> {
    enum FetchHelperResult {
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
                let data = rmp_serde::to_vec_named(&out_msg).unwrap();
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
                let data = rmp_serde::to_vec_named(&response).unwrap();
                stream.send(data.into()).await?;
            }
        }
    }
}
