use core::time::Duration;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::stream::{SplitSink, SplitStream};
use futures::{Stream, StreamExt};
use orion::aead::streaming::{StreamOpener, StreamSealer};
use smallvec::smallvec;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::WorkerId;
use crate::comm::{ConnectionRegistration, RegisterWorker};
use crate::gateway::LostWorkerReason;
use crate::internal::common::error::DsError;
use crate::internal::common::taskgroup::TaskGroup;
use crate::internal::messages::worker::{
    FromWorkerMessage, NewWorkerMsg, ToWorkerMessage, WorkerRegistrationResponse, WorkerStopReason,
};
use crate::internal::server::comm::{Comm, CommSenderRef};
use crate::internal::server::core::CoreRef;
use crate::internal::server::reactor::{
    on_new_worker, on_remove_worker, on_resolve_placement, on_steal_response, on_task_error,
    on_task_finished, on_task_running,
};
use crate::internal::server::worker::{DEFAULT_WORKER_OVERVIEW_INTERVAL, Worker};
use crate::internal::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, serialize,
};
use crate::internal::transfer::transport::make_protocol_builder;
use crate::internal::worker::configuration::sync_worker_configuration;

pub struct ConnectionDescriptor {
    pub address: std::net::SocketAddr,
    pub receiver: SplitStream<Framed<tokio::net::TcpStream, LengthDelimitedCodec>>,
    pub sender: SplitSink<Framed<tokio::net::TcpStream, LengthDelimitedCodec>, bytes::Bytes>,
    pub sealer: Option<StreamSealer>,
    pub opener: Option<StreamOpener>,
}

pub(crate) async fn connection_initiator(
    listener: TcpListener,
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
) -> crate::Result<()> {
    let group = TaskGroup::default();
    loop {
        let (socket, address) = group.run_until(listener.accept()).await?;
        socket.set_nodelay(true)?;
        let core_ref2 = core_ref.clone();
        let comm_ref = comm_ref.clone();
        group.add_task(async move {
            log::debug!("New connection: {address}");
            let (connection, message) =
                match worker_authentication(&core_ref2, socket, address).await {
                    Ok(r) => r,
                    Err(e) => {
                        log::warn!("Worker connection ended with: {e:?}");
                        return;
                    }
                };
            match message {
                ConnectionRegistration::Worker(msg) => {
                    match worker_rpc_loop(&core_ref2, &comm_ref, connection, msg).await {
                        Ok(_) => log::debug!("Connection ended: {address}"),
                        Err(e) => log::warn!("Worker connection ended with: {e:?}"),
                    }
                }
                ConnectionRegistration::Custom => {
                    if let Some(handler) = core_ref2.get().custom_conn_handler() {
                        handler(connection);
                    }
                }
            }
        });
    }
}

pub(crate) async fn worker_authentication(
    core_ref: &CoreRef,
    stream: TcpStream,
    address: std::net::SocketAddr,
) -> crate::Result<(ConnectionDescriptor, ConnectionRegistration)> {
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();

    let secret_key = core_ref.get().secret_key().cloned();
    let (sealer, mut opener) =
        do_authentication(0, "server", "worker", secret_key, &mut writer, &mut reader).await?;
    let message_data = timeout(Duration::from_secs(15), reader.next())
        .await
        .map_err(|_| "Worker registration did not arrive")?
        .ok_or_else(|| {
            DsError::from("The remote side closed connection without worker registration")
        })??;

    let message: ConnectionRegistration = open_message(&mut opener, &message_data)?;

    log::debug!("Worker registration from {address}");

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
    log::debug!("Worker heartbeat interval: {heartbeat_interval:?}");
    // Sanity that interval is not too small
    assert!(heartbeat_interval.as_millis() > 150);

    let mut configuration = msg.configuration;
    sync_worker_configuration(&mut configuration, *core_ref.get().idle_timeout());

    let idle_timeout = configuration.idle_timeout;
    let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

    {
        let mut core = core_ref.get_mut();

        /* Make sure that all resources provided by Worker has an Id */
        for item in &configuration.resources.resources {
            core.get_or_create_resource_id(&item.name);
        }
        let now = Instant::now();
        let worker = Worker::new(
            worker_id,
            configuration.clone(),
            &core.create_resource_map(),
            now,
        );

        on_new_worker(&mut core, &mut *comm_ref.get_mut(), worker);
    }

    /* Send registration message, this has to be after on_new_worker
    because of registration of resources */
    let message: WorkerRegistrationResponse = {
        let core = core_ref.get();
        WorkerRegistrationResponse {
            worker_id,
            resource_names: core.create_resource_map().into_vec(),
            other_workers: core
                .get_workers()
                .filter_map(|w| {
                    if w.id != worker_id {
                        Some(NewWorkerMsg {
                            worker_id: w.id(),
                            address: w.configuration().listen_address.clone(),
                            resources: w.resources.to_transport(),
                        })
                    } else {
                        None
                    }
                })
                .collect(),
            server_idle_timeout: *core.idle_timeout(),
            server_uid: core.server_uid().to_string(),
            worker_overview_interval_override: if core.worker_overview_listeners() > 0 {
                Some(DEFAULT_WORKER_OVERVIEW_INTERVAL)
            } else {
                None
            },
        }
    };
    queue_sender
        .send(serialize(&message).unwrap().into())
        .unwrap();

    comm_ref.get_mut().add_worker(worker_id, queue_sender);
    let snd_loop =
        forward_queue_to_sealed_sink(queue_receiver, connection.sender, connection.sealer);

    let comm_ref2 = comm_ref.clone();
    let periodic_check = async move {
        let mut interval = {
            tokio::time::interval(
                // Idle timeout might be smaller than 500ms in tests.
                #[allow(clippy::manual_clamp)]
                heartbeat_interval
                    .min(idle_timeout.map(|t| t / 16).unwrap_or(heartbeat_interval))
                    .min(Duration::from_secs(60 * 5))
                    // Sanity check that interval is not too short
                    .max(Duration::from_millis(500)),
            )
        };
        let mut retract_interval = Duration::from_secs(60 * 3);
        if let Some(idle_timeout) = idle_timeout {
            retract_interval = retract_interval.min(idle_timeout / 2);
        }
        let retract_interval = retract_interval.max(Duration::from_millis(500));
        let mut last_retract_check = Instant::now();
        loop {
            interval.tick().await;
            let mut core = core_ref.get_mut();
            let (task_map, worker_map) = core.split_tasks_workers_mut();
            let worker = worker_map.get_worker_mut(worker_id);
            let now = Instant::now();
            let elapsed = now - worker.last_heartbeat;
            if elapsed > heartbeat_interval * 2 {
                log::debug!("Heartbeat not arrived, worker={}", worker.id);
                break LostWorkerReason::HeartbeatLost;
            }

            let elapsed = now - last_retract_check;
            if elapsed > retract_interval {
                log::debug!("Trying to retract overtime tasks, worker={}", worker.id);
                let mut comm = comm_ref2.get_mut();
                worker.retract_overtime_tasks(&mut *comm, task_map, now);
                last_retract_check = now;
            }

            if let Some(timeout) = idle_timeout
                && worker.idle_timestamp + timeout < now
                && worker.is_free()
                && !worker.is_reserved()
            {
                log::debug!("Idle timeout reached, worker={}", worker.id);
                worker.set_stop(LostWorkerReason::IdleTimeout);
                let mut comm = comm_ref2.get_mut();
                comm.send_worker_message(worker_id, &ToWorkerMessage::Stop);
            }

            if let Some((_, stop)) = worker.stop_reason {
                let delay = Duration::from_secs(60);
                if now > stop + delay {
                    log::debug!("Stopping of worker timeout");
                    break LostWorkerReason::ConnectionLost;
                }
            }
        }
    };

    let reason = tokio::select! {
        result = worker_receive_loop(core_ref.clone(), comm_ref.clone(), worker_id, connection.receiver, connection.opener) => {
            log::debug!("Receive loop terminated ({result:?}), worker={worker_id}");
            if let Ok(Some(reason)) = result {
                match reason {
                    WorkerStopReason::IdleTimeout => LostWorkerReason::IdleTimeout,
                    WorkerStopReason::TimeLimitReached => LostWorkerReason::TimeLimitReached,
                    WorkerStopReason::Interrupted => LostWorkerReason::ConnectionLost
                }
            } else {
                LostWorkerReason::ConnectionLost
            }
        }
        e = snd_loop => {
            log::debug!("Sending loop terminated: {e:?}, worker={worker_id}");
            LostWorkerReason::ConnectionLost
        }
        r = periodic_check => {
            log::debug!("Worker time check loop terminated, worker={worker_id}");
            r
        }
    };

    log::info!(
        "Worker {} ({}) connection closed: {}",
        worker_id,
        connection.address,
        reason
    );
    let mut core = core_ref.get_mut();
    let mut comm = comm_ref.get_mut();
    let reason = core
        .get_worker_by_id_or_panic(worker_id)
        .stop_reason
        .map(|(r, _)| r)
        .unwrap_or(reason);
    comm.remove_worker(worker_id);
    on_remove_worker(&mut core, &mut *comm, worker_id, reason);
    Ok(())
}

pub(crate) async fn worker_receive_loop<
    Reader: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
>(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    worker_id: WorkerId,
    mut receiver: Reader,
    mut opener: Option<StreamOpener>,
) -> crate::Result<Option<WorkerStopReason>> {
    while let Some(message) = receiver.next().await {
        let message: FromWorkerMessage = open_message(&mut opener, &message?)?;
        let mut core = core_ref.get_mut();
        let mut comm = comm_ref.get_mut();
        match message {
            FromWorkerMessage::TaskFinished(msg) => {
                on_task_finished(&mut core, &mut *comm, worker_id, msg);
            }
            FromWorkerMessage::TaskRunning(msg) => {
                on_task_running(&mut core, &mut *comm, worker_id, msg);
            }
            FromWorkerMessage::TaskFailed(msg) => {
                on_task_error(&mut core, &mut *comm, worker_id, msg.id, msg.info);
            }
            FromWorkerMessage::StealResponse(msg) => {
                on_steal_response(&mut core, &mut *comm, worker_id, msg)
            }
            FromWorkerMessage::Heartbeat => {
                if let Some(worker) = core.get_worker_mut(worker_id) {
                    log::trace!("Heartbeat received, worker={worker_id}");
                    worker.last_heartbeat = Instant::now();
                };
            }
            FromWorkerMessage::Overview(overview) => {
                comm.client().on_worker_overview(overview);
            }
            FromWorkerMessage::Stop(reason) => {
                return Ok(Some(reason));
            }
            FromWorkerMessage::PlacementQuery(data_id) => {
                on_resolve_placement(&mut core, &mut *comm, worker_id, data_id);
            }
            FromWorkerMessage::NewPlacement(data_id) => {
                log::debug!("New placement for {data_id}: worker {worker_id}");
                if let Some(obj) = core.data_objects_mut().find_data_object_mut(data_id) {
                    obj.add_placement(worker_id);
                } else {
                    comm_ref.get_mut().send_worker_message(
                        worker_id,
                        &ToWorkerMessage::RemoveDataObjects(smallvec![data_id]),
                    );
                    log::debug!("Placement for invalid object");
                }
            }
        }
    }
    Ok(None)
}
