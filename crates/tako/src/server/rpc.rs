use core::time::Duration;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::stream::{SplitSink, SplitStream};
use futures::{Stream, StreamExt};
use orion::aead::streaming::{StreamOpener, StreamSealer};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::common::error::DsError;
use crate::messages::gateway::LostWorkerReason;
use crate::messages::worker::{
    ConnectionRegistration, FromWorkerMessage, RegisterWorker, WorkerRegistrationResponse,
};
use crate::server::comm::CommSenderRef;
use crate::server::core::CoreRef;
use crate::server::reactor::{
    on_new_worker, on_remove_worker, on_steal_response, on_task_error, on_task_finished,
    on_task_running, on_tasks_transferred,
};
use crate::server::worker::Worker;
use crate::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, serialize,
};
use crate::transfer::transport::make_protocol_builder;
use crate::WorkerId;

pub struct ConnectionDescriptor {
    pub address: std::net::SocketAddr,
    pub receiver: SplitStream<Framed<tokio::net::TcpStream, LengthDelimitedCodec>>,
    pub sender: SplitSink<Framed<tokio::net::TcpStream, LengthDelimitedCodec>, bytes::Bytes>,
    pub sealer: Option<StreamSealer>,
    pub opener: Option<StreamOpener>,
}

pub async fn connection_initiator(
    listener: TcpListener,
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let core_ref2 = core_ref.clone();
        let comm_ref = comm_ref.clone();
        let rpc_task = tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            let (connection, message) =
                match worker_authentication(&core_ref2, socket, address).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("Worker connection ended with: {:?}", e);
                        return;
                    }
                };
            match message {
                ConnectionRegistration::Worker(msg) => {
                    match worker_rpc_loop(&core_ref2, &comm_ref, connection, msg).await {
                        Ok(_) => log::debug!("Connection ended: {}", address),
                        Err(e) => log::warn!("Worker connection ended with: {:?}", e),
                    }
                }
                ConnectionRegistration::Custom => {
                    if let Some(handler) = core_ref2.get().custom_conn_handler() {
                        handler(connection);
                    }
                }
            }
        });
        core_ref.get_mut().add_rpc_handle(rpc_task);
    }
}

pub async fn worker_authentication(
    core_ref: &CoreRef,
    stream: TcpStream,
    address: std::net::SocketAddr,
) -> crate::Result<(ConnectionDescriptor, ConnectionRegistration)> {
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();

    let secret_key = core_ref.get().secret_key().clone();
    let has_key = secret_key.is_some();
    let (sealer, mut opener) = do_authentication(
        0,
        "server".to_string(),
        "worker".to_string(),
        secret_key.clone(),
        &mut writer,
        &mut reader,
    )
    .await?;
    assert_eq!(sealer.is_some(), has_key);

    let message_data = timeout(Duration::from_secs(15), reader.next())
        .await
        .map_err(|_| "Worker registration did not arrived")?
        .ok_or_else(|| {
            DsError::from("The remote side closed connection without worker registration")
        })??;

    let message: ConnectionRegistration = open_message(&mut opener, &message_data)?;

    log::debug!("Worker registration from {}", address);

    let connection = ConnectionDescriptor {
        address,
        receiver: reader,
        sender: writer,
        opener,
        sealer,
    };
    Ok((connection, message))
}

async fn worker_rpc_loop(
    core_ref: &CoreRef,
    comm_ref: &CommSenderRef,
    connection: ConnectionDescriptor,
    msg: RegisterWorker,
) -> crate::Result<()> {
    let worker_id = core_ref.get_mut().new_worker_id();
    log::info!(
        "Worker {} registered from {}",
        worker_id,
        connection.address
    );

    let heartbeat_interval = msg.configuration.heartbeat_interval;
    log::debug!("Worker heartbeat: {:?}", heartbeat_interval);
    // Sanity that interval is not too small
    assert!(heartbeat_interval.as_millis() > 150);

    let mut configuration = msg.configuration;
    // Update idle_timeout configuration from server default
    if configuration.idle_timeout.is_none() {
        configuration.idle_timeout = *core_ref.get().idle_timeout();
    }
    let idle_timeout = configuration.idle_timeout;

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    {
        let mut core = core_ref.get_mut();

        /* Make sure that all resources provided by Worker has an Id */
        for descriptor in &configuration.resources.generic {
            core.get_or_create_generic_resource_id(&descriptor.name);
        }
        let worker = Worker::new(worker_id, configuration, core.generic_resource_names());

        on_new_worker(&mut core, &mut *comm_ref.get_mut(), worker);
    }

    /* Send registration message, this has to be after on_new_worker
    because of registration of resources */
    let message = WorkerRegistrationResponse {
        worker_id,
        worker_addresses: core_ref.get().get_worker_addresses(),
        resource_names: core_ref.get().generic_resource_names().to_vec(),
    };
    queue_sender
        .send(serialize(&message).unwrap().into())
        .unwrap();

    comm_ref.get_mut().add_worker(worker_id, queue_sender);
    let snd_loop =
        forward_queue_to_sealed_sink(queue_receiver, connection.sender, connection.sealer);

    let periodic_check = async move {
        let mut interval = {
            tokio::time::interval(
                heartbeat_interval
                    .min(idle_timeout.unwrap_or(heartbeat_interval))
                    // Sanity check that interval is not too short
                    .max(Duration::from_millis(500)),
            )
        };
        loop {
            interval.tick().await;
            let now = Instant::now();
            let mut core = core_ref.get_mut();
            let mut worker = core.get_worker_mut_by_id_or_panic(worker_id);
            let elapsed = now - worker.last_heartbeat;

            if let Some(timeout) = worker.configuration.idle_timeout {
                if worker.tasks().is_empty() {
                    let elapsed = now - worker.last_occupied;
                    if elapsed > timeout {
                        log::debug!("Idle timeout, worker={}", worker.id);
                        break LostWorkerReason::IdleTimeout;
                    }
                } else {
                    worker.last_occupied = now;
                }
            }

            if elapsed > heartbeat_interval * 2 {
                log::debug!("Heartbeat not arrived, worker={}", worker.id);
                break LostWorkerReason::HeartbeatLost;
            }
        }
    };

    let reason = tokio::select! {
        e = worker_receive_loop(core_ref.clone(), comm_ref.clone(), worker_id, connection.receiver, connection.opener) => {
            log::debug!("Receive loop terminated ({:?}), worker={}", e, worker_id);
            LostWorkerReason::ConnectionLost
        }
        e = snd_loop => {
            log::debug!("Sending loop terminated: {:?}, worker={}", e, worker_id);
            LostWorkerReason::ConnectionLost
        }
        r = periodic_check => {
            log::debug!("Heartbeat loop terminated, worker={}", worker_id);
            r
        }
    };

    log::info!(
        "Worker {} connection closed (connection: {})",
        worker_id,
        connection.address
    );
    let mut core = core_ref.get_mut();
    let mut comm = comm_ref.get_mut();
    let stopping = { core.get_worker_by_id_or_panic(worker_id).is_stopping() };
    comm.remove_worker(worker_id);
    on_remove_worker(
        &mut core,
        &mut *comm,
        worker_id,
        if stopping {
            LostWorkerReason::Stopped
        } else {
            reason
        },
    );
    //core.remove_worker(worker_id);
    Ok(())
}

pub async fn worker_receive_loop<
    Reader: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
>(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    worker_id: WorkerId,
    mut receiver: Reader,
    mut opener: Option<StreamOpener>,
) -> crate::Result<()> {
    while let Some(message) = receiver.next().await {
        let message: FromWorkerMessage = open_message(&mut opener, &message?)?;
        let mut core = core_ref.get_mut();
        let mut comm = comm_ref.get_mut();
        match message {
            FromWorkerMessage::TaskFinished(msg) => {
                on_task_finished(&mut core, &mut *comm, worker_id, msg);
            }
            FromWorkerMessage::TaskRunning(msg) => {
                on_task_running(&mut core, &mut *comm, worker_id, msg.id);
            }
            FromWorkerMessage::TaskFailed(msg) => {
                on_task_error(&mut core, &mut *comm, worker_id, msg.id, msg.info);
            }
            FromWorkerMessage::DataDownloaded(msg) => {
                on_tasks_transferred(&mut core, &mut *comm, worker_id, msg.id)
            }
            FromWorkerMessage::StealResponse(msg) => {
                on_steal_response(&mut core, &mut *comm, worker_id, msg)
            }
            FromWorkerMessage::Heartbeat => {
                if let Some(worker) = core.get_worker_mut(worker_id) {
                    log::debug!("Heartbeat received, worker={}", worker_id);
                    worker.last_heartbeat = Instant::now();
                };
            }
            FromWorkerMessage::Overview(overview) => {
                if let Some(worker) = core.get_worker_mut(worker_id) {
                    let sender = worker.overview_callbacks.remove(0);
                    let _ = sender.send(overview);
                };
            }
        }
    }
    Ok(())
}
