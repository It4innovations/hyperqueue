use std::rc::Rc;
use std::sync::Arc;

use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::kdf::SecretKey;
use tako::messages::gateway::{
    CancelTasks, FromGatewayMessage, NewTasksMessage, StopWorkerRequest, TaskDef, ToGatewayMessage,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use crate::server::job::{Job, JobId, JobState};
use crate::server::rpc::TakoServer;
use crate::server::state::StateRef;
use crate::transfer::connection::ServerConnection;
use crate::transfer::messages::{
    CancelJobResponse, FromClientMessage, JobInfo, JobInfoResponse, JobStatus, SubmitRequest,
    SubmitResponse, ToClientMessage, WorkerListResponse,
};

use crate::WorkerId;

pub async fn handle_client_connections(
    state_ref: StateRef,
    tako_ref: TakoServer,
    listener: TcpListener,
    end_flag: Rc<Notify>,
    key: Arc<SecretKey>,
) {
    while let Ok((connection, _)) = listener.accept().await {
        let state_ref = state_ref.clone();
        let tako_ref = tako_ref.clone();
        let end_flag = end_flag.clone();
        let key = key.clone();
        tokio::task::spawn_local(async move {
            if let Err(e) = handle_client(connection, state_ref, tako_ref, end_flag, key).await {
                log::error!("Client error: {}", e);
            }
        });
    }
}

async fn handle_client(
    socket: TcpStream,
    state_ref: StateRef,
    tako_ref: TakoServer,
    end_flag: Rc<Notify>,
    key: Arc<SecretKey>,
) -> crate::Result<()> {
    log::debug!("New client connection");
    let socket = ServerConnection::accept_client(socket, key).await?;
    let (tx, rx) = socket.split();

    client_rpc_loop(tx, rx, state_ref, tako_ref, end_flag).await;
    log::debug!("Client connection ended");
    Ok(())
}

pub async fn client_rpc_loop<
    Tx: Sink<ToClientMessage> + Unpin,
    Rx: Stream<Item = crate::Result<FromClientMessage>> + Unpin,
>(
    mut tx: Tx,
    mut rx: Rx,
    state_ref: StateRef,
    tako_ref: TakoServer,
    end_flag: Rc<Notify>,
) {
    while let Some(message_result) = rx.next().await {
        match message_result {
            Ok(message) => {
                let response = match message {
                    FromClientMessage::Submit(msg) => {
                        handle_submit(&state_ref, &tako_ref, msg).await
                    }
                    FromClientMessage::JobInfo(msg) => {
                        compute_job_info(
                            &state_ref,
                            &tako_ref,
                            msg.job_ids,
                            msg.include_program_def,
                        )
                        .await
                    }
                    FromClientMessage::Stop => {
                        end_flag.notify_one();
                        break;
                    }
                    FromClientMessage::WorkerList => handle_worker_list(&state_ref).await,
                    FromClientMessage::StopWorker(msg) => {
                        handle_worker_stop(&tako_ref, msg.worker_id).await.unwrap()
                    }
                    FromClientMessage::Cancel(msg) => {
                        handle_job_cancel(&state_ref, &tako_ref, msg.job_id).await
                    }
                };
                assert!(tx.send(response).await.is_ok());
            }
            Err(e) => {
                log::error!("Cannot parse client message: {}", e);
                assert!(tx
                    .send(ToClientMessage::Error(format!(
                        "Cannot parse message: {}",
                        e
                    )))
                    .await
                    .is_ok());
                return;
            }
        }
    }
}

async fn handle_worker_stop(
    tako_ref: &TakoServer,
    worker_id: WorkerId,
) -> crate::Result<ToClientMessage> {
    match tako_ref
        .send_message(FromGatewayMessage::StopWorker(StopWorkerRequest {
            worker_id,
        }))
        .await?
    {
        ToGatewayMessage::WorkerStopped => Ok(ToClientMessage::StopWorkerResponse),
        ToGatewayMessage::Error(error) => Ok(ToClientMessage::Error(error.message)),
        msg => panic!("Received invalid response to worker stop: {:?}", msg),
    }
}

async fn compute_job_info(
    state_ref: &StateRef,
    tako_ref: &TakoServer,
    job_ids: Option<Vec<JobId>>,
    include_program_def: bool,
) -> ToClientMessage {
    let state = state_ref.get();
    if let Some(job_ids) = job_ids {
        ToClientMessage::JobInfoResponse(JobInfoResponse {
            jobs: job_ids
                .into_iter()
                .filter_map(|job_id| {
                    state
                        .get_job(job_id)
                        .map(|j| j.make_job_info(include_program_def))
                })
                .collect(),
        })
    } else {
        ToClientMessage::JobInfoResponse(JobInfoResponse {
            jobs: state
                .jobs()
                .map(|j| j.make_job_info(include_program_def))
                .collect(),
        })
    }
}

async fn handle_job_cancel(
    state_ref: &StateRef,
    tako_ref: &TakoServer,
    job_id: JobId,
) -> ToClientMessage {
    {
        let mut state = state_ref.get_mut();
        match state.get_job(job_id) {
            None => return ToClientMessage::CancelJobResponse(CancelJobResponse::InvalidJob),
            Some(job) => {
                match job.state {
                    JobState::Waiting | JobState::Running => { /* Continue */ }
                    JobState::Finished | JobState::Failed(_) | JobState::Canceled => {
                        return ToClientMessage::CancelJobResponse(
                            CancelJobResponse::AlreadyFinished,
                        )
                    }
                }
            }
        };
    }

    match tako_ref
        .send_message(FromGatewayMessage::CancelTasks(CancelTasks {
            tasks: vec![job_id],
        }))
        .await
        .unwrap()
    {
        ToGatewayMessage::CancelTasksResponse(msg) => {
            if !msg.already_finished.is_empty() {
                assert_eq!(msg.already_finished.len(), 1);
                return ToClientMessage::CancelJobResponse(CancelJobResponse::AlreadyFinished);
            }
        }
        ToGatewayMessage::Error(msg) => {
            log::debug!("Canceling job failed: {}", msg.message);
            return ToClientMessage::Error(format!("Canceling job failed: {}", msg.message));
        }
        _ => {
            panic!("Invalid message");
        }
    }

    {
        let mut state = state_ref.get_mut();
        let job = state.get_job_mut(job_id);
        match job {
            None => return ToClientMessage::CancelJobResponse(CancelJobResponse::InvalidJob),
            Some(job) => {
                match job.state {
                    JobState::Waiting | JobState::Running => { /* Continue */ }
                    JobState::Finished | JobState::Failed(_) | JobState::Canceled => {
                        return ToClientMessage::CancelJobResponse(
                            CancelJobResponse::AlreadyFinished,
                        )
                    }
                }
                job.state = JobState::Canceled
            }
        };
    }

    ToClientMessage::CancelJobResponse(CancelJobResponse::Canceled)
}

async fn handle_submit(
    state_ref: &StateRef,
    tako_ref: &TakoServer,
    message: SubmitRequest,
) -> ToClientMessage {
    let (task_def, task_id, program_def) = {
        let mut state = state_ref.get_mut();
        let task_id = state.new_job_id();

        let mut program_def = message.spec;

        let stdout = format!("stdout.{}", task_id);
        let stderr = format!("stderr.{}", task_id);
        program_def.stdout = Some(message.cwd.join(stdout));
        program_def.stderr = Some(message.cwd.join(stderr));

        let body = rmp_serde::to_vec_named(&program_def).unwrap();
        let job = Job::new(task_id, message.name.clone(), program_def.clone());
        state.add_job(job);

        (
            TaskDef {
                id: task_id,
                type_id: 0,
                body,
                keep: false,
                observe: true,
                n_outputs: Default::default(),
            },
            task_id,
            program_def,
        )
    };
    match tako_ref
        .send_message(FromGatewayMessage::NewTasks(NewTasksMessage {
            tasks: vec![task_def],
        }))
        .await
        .unwrap()
    {
        ToGatewayMessage::NewTasksResponse(_) => { /* Ok */ }
        _ => {
            panic!("Invalid response");
        }
    };

    ToClientMessage::SubmitResponse(SubmitResponse {
        job: JobInfo {
            id: task_id,
            name: message.name,
            status: JobStatus::Waiting,
            worker_id: None,
            error: None,
            spec: Some(program_def),
        },
    })
}

async fn handle_worker_list(state_ref: &StateRef) -> ToClientMessage {
    let state = state_ref.get();

    ToClientMessage::WorkerListResponse(WorkerListResponse {
        workers: state
            .get_workers()
            .values()
            .map(|w| w.make_info())
            .collect(),
    })
}
