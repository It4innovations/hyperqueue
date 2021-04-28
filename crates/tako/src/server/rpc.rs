use core::time::Duration;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream, StreamExt};
use orion::aead::streaming::{StreamOpener, StreamSealer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::common::error::DsError;
use crate::messages::worker::{FromWorkerMessage, RegisterWorker, WorkerRegistrationResponse};
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

pub async fn connection_initiator(
    listener: TcpListener,
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
) -> crate::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let core_ref = core_ref.clone();
        let comm_ref = comm_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New connection: {}", address);
            match worker_authentication(core_ref, comm_ref, socket, address).await {
                Ok(_) => { /* Do nothing */ }
                Err(e) => {
                    log::warn!("Connection ended with: {:?}", e);
                }
            }
            log::debug!("Connection ended: {}", address);
        });
    }
}

pub async fn worker_authentication<T: AsyncRead + AsyncWrite>(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    stream: T,
    address: std::net::SocketAddr,
) -> crate::Result<()> {
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

    let message: RegisterWorker = open_message(&mut opener, &message_data)?;

    log::debug!("Worker registration from {}", address);
    worker_rpc_loop(
        &core_ref, &comm_ref, address, reader, writer, message, sealer, opener,
    )
    .await?;

    Ok(())
}

async fn worker_rpc_loop<
    Reader: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    Writer: Sink<Bytes, Error = std::io::Error> + Unpin,
>(
    core_ref: &CoreRef,
    comm_ref: &CommSenderRef,
    address: std::net::SocketAddr,
    receiver: Reader,
    sender: Writer,
    msg: RegisterWorker,
    sealer: Option<StreamSealer>,
    opener: Option<StreamOpener>,
) -> crate::Result<()> {
    let worker_id = core_ref.get_mut().new_worker_id();
    log::info!("Worker {} registered from {}", worker_id, address);

    let heartbeat_interval = msg.configuration.heartbeat_interval;
    log::debug!("Worker heartbeat: {:?}", heartbeat_interval);
    // Sanity that interval is not too small
    assert!(heartbeat_interval.as_millis() > 150);

    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    let message = WorkerRegistrationResponse {
        worker_id,
        worker_addresses: core_ref.get().get_worker_addresses(),
        subworker_definitions: core_ref.get().get_subworker_definitions().clone(),
    };
    queue_sender
        .send(serialize(&message).unwrap().into())
        .unwrap();

    let worker = Worker::new(worker_id, msg.configuration);

    on_new_worker(&mut core_ref.get_mut(), &mut *comm_ref.get_mut(), worker);
    comm_ref.get_mut().add_worker(worker_id, queue_sender);

    let snd_loop = forward_queue_to_sealed_sink(queue_receiver, sender, sealer);

    let heartbeat_check = async move {
        let mut interval = tokio::time::interval(heartbeat_interval);
        loop {
            interval.tick().await;
            let core = core_ref.get();
            let elapsed = core
                .get_worker_by_id_or_panic(worker_id)
                .last_heartbeat
                .elapsed();
            if elapsed > heartbeat_interval * 2 {
                break;
            }
        }
    };

    tokio::select! {
        e = worker_receive_loop(core_ref.clone(), comm_ref.clone(), worker_id, receiver, opener) => {
            log::debug!("Receive loop terminated ({:?}), worker={}", e, worker_id);
        }
        e = snd_loop => {
            log::debug!("Sending loop terminated: {:?}, worker={}", e, worker_id);
        }
        () = heartbeat_check => {
            log::debug!("Heartbeat check failed, worker={}", worker_id);
        }
    };

    log::info!(
        "Worker {} connection closed (connection: {})",
        worker_id,
        address
    );
    let mut core = core_ref.get_mut();
    let mut comm = comm_ref.get_mut();
    comm.remove_worker(worker_id);
    on_remove_worker(&mut core, &mut *comm, worker_id);
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
                core.get_worker_mut(worker_id).map(|worker| {
                    log::debug!("Heartbeat received, worker={}", worker_id);
                    worker.last_heartbeat = Instant::now();
                });
            }
            FromWorkerMessage::Overview(overview) => {
                core.get_worker_mut(worker_id).map(|worker| {
                    let sender = worker.overview_callbacks.remove(0);
                    let _ = sender.send(overview);
                });
            }
        }
    }
    Ok(())
}
