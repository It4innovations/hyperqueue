use chrono::Utc;
use std::fmt::Debug;
use std::sync::Arc;

use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::kdf::SecretKey;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};

use tako::gateway::{CancelTasks, FromGatewayMessage, StopWorkerRequest, ToGatewayMessage};
use tako::{Set, TaskGroup};

use crate::client::status::{Status, job_status};
use crate::common::serverdir::ServerDir;
use crate::server::event::Event;
use crate::server::job::JobTaskCounters;
use crate::server::state::{State, StateRef};
use crate::transfer::connection::ServerConnection;
use crate::transfer::messages::{
    CancelJobResponse, CloseJobResponse, FromClientMessage, IdSelector, JobDetail,
    JobDetailResponse, JobInfoResponse, StopWorkerResponse, TaskSelector, ToClientMessage,
    WorkerListResponse,
};
use crate::transfer::messages::{ForgetJobResponse, WaitForJobsResponse};
use crate::{JobId, JobTaskCount, WorkerId, unwrap_tako_id};

pub mod autoalloc;
mod submit;

use crate::common::error::HqError;
use crate::server::Senders;
use crate::server::client::submit::handle_open_job;
pub(crate) use submit::{submit_job_desc, validate_submit};

pub async fn handle_client_connections(
    state_ref: StateRef,
    senders: &Senders,
    server_dir: ServerDir,
    listener: TcpListener,
    end_flag: Arc<Notify>,
    key: Option<Arc<SecretKey>>,
) {
    let group = TaskGroup::default();
    while let Ok((connection, _)) = group.run_until(listener.accept()).await {
        let state_ref = state_ref.clone();
        let senders2 = senders.clone();
        let end_flag = end_flag.clone();
        let key = key.clone();
        let server_dir = server_dir.clone();

        group.add_task(async move {
            if let Err(e) =
                handle_client(connection, server_dir, state_ref, &senders2, end_flag, key).await
            {
                log::error!("Client error: {}", e);
            }
        });
    }
}

async fn handle_client(
    socket: TcpStream,
    server_dir: ServerDir,
    state_ref: StateRef,
    carrier: &Senders,
    end_flag: Arc<Notify>,
    key: Option<Arc<SecretKey>>,
) -> crate::Result<()> {
    log::debug!("New client connection");
    let socket = ServerConnection::accept_client(socket, key).await?;
    let (tx, rx) = socket.split();

    client_rpc_loop(tx, rx, server_dir, state_ref, carrier, end_flag).await;
    log::debug!("Client connection ended");
    Ok(())
}

async fn stream_history_events<Tx: Sink<ToClientMessage, Error = HqError> + Unpin + 'static>(
    tx: &mut Tx,
    mut history: mpsc::UnboundedReceiver<Event>,
) {
    log::debug!("Resending history started");
    while let Some(e) = history.recv().await {
        if tx.send(ToClientMessage::Event(e)).await.is_err() {
            return;
        }
    }
}

async fn stream_events<
    Tx: Sink<ToClientMessage, Error = HqError> + Unpin + 'static,
    Rx: Stream<Item = crate::Result<FromClientMessage>> + Unpin,
>(
    tx: &mut Tx,
    rx: &mut Rx,
    mut current: mpsc::UnboundedReceiver<Event>,
) {
    log::debug!("History streaming completed");
    loop {
        let r = tokio::select! {
            r = current.recv() => r,
            _ = rx.next() => {
                log::debug!("Event streaming terminated");
                return
            }
        };
        if let Some(e) = r {
            if tx.send(ToClientMessage::Event(e)).await.is_err() {
                return;
            }
        } else {
            break;
        }
    }
    log::debug!("Event streaming completed")
}

pub async fn client_rpc_loop<
    Tx: Sink<ToClientMessage, Error = HqError> + Unpin + 'static,
    Rx: Stream<Item = crate::Result<FromClientMessage>> + Unpin,
>(
    mut tx: Tx,
    mut rx: Rx,
    server_dir: ServerDir,
    state_ref: StateRef,
    senders: &Senders,
    end_flag: Arc<Notify>,
) where
    Tx::Error: Debug,
{
    while let Some(message_result) = rx.next().await {
        match message_result {
            Ok(message) => {
                let response = match message {
                    FromClientMessage::Submit(msg) => {
                        submit::handle_submit(&state_ref, senders, msg).await
                    }
                    FromClientMessage::JobInfo(msg) => compute_job_info(&state_ref, &msg.selector),
                    FromClientMessage::Stop => {
                        end_flag.notify_one();
                        break;
                    }
                    FromClientMessage::WorkerList => handle_worker_list(&state_ref).await,
                    FromClientMessage::WorkerInfo(msg) => {
                        handle_worker_info(&state_ref, msg.worker_id).await
                    }
                    FromClientMessage::StopWorker(msg) => {
                        handle_worker_stop(&state_ref, senders, msg.selector).await
                    }
                    FromClientMessage::Cancel(msg) => {
                        handle_job_cancel(&state_ref, senders, &msg.selector).await
                    }
                    FromClientMessage::ForgetJob(msg) => {
                        handle_job_forget(&state_ref, &msg.selector, msg.filter)
                    }
                    FromClientMessage::JobDetail(msg) => {
                        compute_job_detail(&state_ref, msg.job_id_selector, msg.task_selector)
                    }
                    FromClientMessage::AutoAlloc(msg) => {
                        autoalloc::handle_autoalloc_message(&server_dir, senders, msg).await
                    }
                    FromClientMessage::WaitForJobs(msg) => {
                        handle_wait_for_jobs_message(&state_ref, msg.selector, msg.wait_for_close)
                            .await
                    }
                    FromClientMessage::OpenJob(job_description) => {
                        handle_open_job(&state_ref, senders, job_description)
                    }
                    FromClientMessage::CloseJob(msg) => {
                        handle_job_close(&state_ref, senders, &msg.selector).await
                    }
                    FromClientMessage::StreamEvents(msg) => {
                        log::debug!("Start streaming events to client");
                        /* We create two event queues, one for historic events and one for live events
                        So while historic events are loaded from the file and streamed, live events are already
                        collected and sent immediately the historic events are sent */
                        let (tx1, rx1) = mpsc::unbounded_channel::<Event>();
                        let live = if msg.live_events {
                            let (tx2, rx2) = mpsc::unbounded_channel::<Event>();
                            let listener_id = senders.events.register_listener(tx2);
                            Some((rx2, listener_id))
                        } else {
                            None
                        };
                        senders.events.replay_journal(tx1);
                        stream_history_events(&mut tx, rx1).await;
                        if let Some((rx2, listener_id)) = live {
                            let _ = tx.send(ToClientMessage::EventLiveBoundary).await;
                            stream_events(&mut tx, &mut rx, rx2).await;
                            senders.events.unregister_listener(listener_id);
                        }
                        break;
                    }
                    FromClientMessage::ServerInfo => {
                        ToClientMessage::ServerInfo(state_ref.get().server_info().clone())
                    }
                    FromClientMessage::PruneJournal => {
                        handle_prune_journal(&state_ref, senders).await
                    }
                    FromClientMessage::FlushJournal => {
                        if let Some(callback) = senders.events.flush_journal() {
                            let _ = callback.await;
                        };
                        ToClientMessage::Finished
                    }
                };
                if let Err(error) = tx.send(response).await {
                    log::error!("Cannot reply to client: {error:?}");
                    break;
                }
            }
            Err(e) => {
                log::error!("Cannot parse client message: {}", e);
                if tx
                    .send(ToClientMessage::Error(format!("Cannot parse message: {e}")))
                    .await
                    .is_err()
                {
                    log::error!(
                        "Cannot send error response to client, it has probably disconnected."
                    );
                }
            }
        }
    }
}

async fn handle_prune_journal(state_ref: &StateRef, senders: &Senders) -> ToClientMessage {
    log::debug!("Client asked for journal prunning");
    let (live_jobs, live_workers) = {
        let state = state_ref.get();
        let live_jobs: Set<_> = state
            .jobs()
            .filter_map(|job| {
                if job.is_terminated() {
                    None
                } else {
                    Some(job.job_id)
                }
            })
            .collect();
        let live_workers = state
            .get_workers()
            .values()
            .filter_map(|worker| {
                if worker.is_running() {
                    Some(worker.worker_id())
                } else {
                    None
                }
            })
            .collect();
        (live_jobs, live_workers)
    };
    if let Some(receiver) = senders.events.prune_journal(live_jobs, live_workers) {
        let _ = receiver.await;
    }
    ToClientMessage::Finished
}

/// Waits until all jobs matched by the `selector` are finished (either by completing successfully,
/// failing or being canceled).
async fn handle_wait_for_jobs_message(
    state_ref: &StateRef,
    selector: IdSelector,
    wait_for_close: bool,
) -> ToClientMessage {
    let update_counters = |response: &mut WaitForJobsResponse, counters: &JobTaskCounters| {
        if counters.n_canceled_tasks > 0 {
            response.canceled += 1;
        } else if counters.n_failed_tasks > 0 {
            response.failed += 1;
        } else {
            response.finished += 1;
        }
    };

    let (receivers, mut response) = {
        let mut state = state_ref.get_mut();
        let job_ids: Vec<JobId> = get_job_ids(&state, &selector);

        let mut response = WaitForJobsResponse::default();
        let mut receivers = vec![];

        for job_id in job_ids {
            match state.get_job_mut(job_id) {
                Some(job) => {
                    if job.has_no_active_tasks() && !(wait_for_close && job.is_open()) {
                        update_counters(&mut response, &job.counters);
                    } else {
                        let rx = job.subscribe_to_completion(wait_for_close);
                        receivers.push(rx);
                    }
                }
                None => response.invalid += 1,
            }
        }
        (receivers, response)
    };

    let results = futures::future::join_all(receivers).await;
    let state = state_ref.get();

    for result in results {
        match result {
            Ok(job_id) => {
                match state.get_job(job_id) {
                    Some(job) => update_counters(&mut response, &job.counters),
                    None => continue,
                };
            }
            Err(err) => log::error!("Error while waiting on job(s): {:?}", err),
        };
    }

    ToClientMessage::WaitForJobsResponse(response)
}

async fn handle_worker_stop(
    state_ref: &StateRef,
    senders: &Senders,
    selector: IdSelector,
) -> ToClientMessage {
    log::debug!("Client asked for worker termination {:?}", selector);
    let mut responses: Vec<(WorkerId, StopWorkerResponse)> = Vec::new();

    let worker_ids: Vec<WorkerId> = match selector {
        IdSelector::Specific(array) => array.iter().map(|id| id.into()).collect(),
        IdSelector::All => state_ref
            .get()
            .get_workers()
            .iter()
            .filter(|(_, worker)| worker.make_info().ended.is_none())
            .map(|(_, worker)| worker.worker_id())
            .collect(),
        IdSelector::LastN(n) => {
            let mut ids: Vec<_> = state_ref.get().get_workers().keys().copied().collect();
            ids.sort_by_key(|&k| std::cmp::Reverse(k));
            ids.truncate(n as usize);
            ids
        }
    };

    for worker_id in worker_ids {
        if let Some(worker) = state_ref.get().get_worker(worker_id) {
            if worker.make_info().ended.is_some() {
                responses.push((worker_id, StopWorkerResponse::AlreadyStopped));
                continue;
            }
        } else {
            responses.push((worker_id, StopWorkerResponse::InvalidWorker));
            continue;
        }
        let response = senders
            .backend
            .send_tako_message(FromGatewayMessage::StopWorker(StopWorkerRequest {
                worker_id,
            }))
            .await;

        match response {
            Ok(result) => match result {
                ToGatewayMessage::WorkerStopped => {
                    responses.push((worker_id, StopWorkerResponse::Stopped))
                }
                ToGatewayMessage::Error(error) => {
                    responses.push((worker_id, StopWorkerResponse::Failed(error.message)))
                }
                msg => panic!("Received invalid response to worker: {worker_id} stop: {msg:?}"),
            },
            Err(err) => {
                responses.push((worker_id, StopWorkerResponse::Failed(err.to_string())));
                log::error!("Unable to stop worker: {} error: {:?}", worker_id, err);
            }
        }
    }
    ToClientMessage::StopWorkerResponse(responses)
}

fn compute_job_detail(
    state_ref: &StateRef,
    job_id_selector: IdSelector,
    task_selector: Option<TaskSelector>,
) -> ToClientMessage {
    let state = state_ref.get();

    let job_ids: Vec<JobId> = get_job_ids(&state, &job_id_selector);

    let mut responses: Vec<(JobId, Option<JobDetail>)> = Vec::new();
    for job_id in job_ids {
        let opt_detail = state
            .get_job(job_id)
            .map(|j| j.make_job_detail(task_selector.as_ref()));

        if let Some(detail) = opt_detail {
            responses.push((job_id, Some(detail)));
        } else {
            responses.push((job_id, None));
        }
    }
    ToClientMessage::JobDetailResponse(JobDetailResponse {
        details: responses,
        server_uid: state.server_info().server_uid.clone(),
    })
}

fn get_job_ids(state: &State, selector: &IdSelector) -> Vec<JobId> {
    match &selector {
        IdSelector::All => state.jobs().map(|job| job.job_id).collect(),
        IdSelector::LastN(n) => state.last_n_ids(*n).collect(),
        IdSelector::Specific(array) => array.iter().map(|id| id.into()).collect(),
    }
}

fn compute_job_info(state_ref: &StateRef, selector: &IdSelector) -> ToClientMessage {
    let state = state_ref.get();

    let jobs: Vec<_> = match selector {
        IdSelector::All => state.jobs().map(|j| j.make_job_info()).collect(),
        IdSelector::LastN(n) => state
            .last_n_ids(*n)
            .filter_map(|id| state.get_job(id))
            .map(|j| j.make_job_info())
            .collect(),
        IdSelector::Specific(array) => array
            .iter()
            .filter_map(|id| state.get_job(JobId::new(id)))
            .map(|j| j.make_job_info())
            .collect(),
    };
    ToClientMessage::JobInfoResponse(JobInfoResponse { jobs })
}

async fn handle_job_cancel(
    state_ref: &StateRef,
    carrier: &Senders,
    selector: &IdSelector,
) -> ToClientMessage {
    let job_ids: Vec<JobId> = match selector {
        IdSelector::All => state_ref
            .get()
            .jobs()
            .map(|job| job.make_job_info())
            .filter(|job_info| matches!(job_status(job_info), Status::Waiting | Status::Running))
            .map(|job_info| job_info.id)
            .collect(),
        IdSelector::LastN(n) => state_ref.get().last_n_ids(*n).collect(),
        IdSelector::Specific(array) => array.iter().map(|id| id.into()).collect(),
    };

    let mut responses: Vec<(JobId, CancelJobResponse)> = Vec::new();
    for job_id in job_ids {
        let response = cancel_job(state_ref, carrier, job_id).await;
        responses.push((job_id, response));
    }

    ToClientMessage::CancelJobResponse(responses)
}

async fn handle_job_close(
    state_ref: &StateRef,
    senders: &Senders,
    selector: &IdSelector,
) -> ToClientMessage {
    let mut state = state_ref.get_mut();
    let job_ids: Vec<JobId> = match selector {
        IdSelector::All => state
            .jobs()
            .filter(|job| job.is_open())
            .map(|job| job.job_id)
            .collect(),
        IdSelector::LastN(n) => state.last_n_ids(*n).collect(),
        IdSelector::Specific(array) => array.iter().map(|id| id.into()).collect(),
    };

    let now = Utc::now();
    let responses: Vec<(JobId, CloseJobResponse)> = job_ids
        .into_iter()
        .map(|job_id| {
            let response = if let Some(job) = state.get_job_mut(job_id) {
                if job.is_open() {
                    job.close();
                    job.check_termination(senders, now);
                    CloseJobResponse::Closed
                } else {
                    CloseJobResponse::AlreadyClosed
                }
            } else {
                CloseJobResponse::InvalidJob
            };
            (job_id, response)
        })
        .collect();

    ToClientMessage::CloseJobResponse(responses)
}

async fn cancel_job(state_ref: &StateRef, senders: &Senders, job_id: JobId) -> CancelJobResponse {
    let tako_task_ids;
    {
        let n_tasks = match state_ref.get().get_job(job_id) {
            None => {
                return CancelJobResponse::InvalidJob;
            }
            Some(job) => {
                tako_task_ids = job.non_finished_task_ids();
                job.n_tasks()
            }
        };
        if tako_task_ids.is_empty() {
            return CancelJobResponse::Canceled(Vec::new(), n_tasks);
        }
    }

    let canceled_tasks = match senders
        .backend
        .send_tako_message(FromGatewayMessage::CancelTasks(CancelTasks {
            tasks: tako_task_ids,
        }))
        .await
        .unwrap()
    {
        ToGatewayMessage::CancelTasksResponse(msg) => msg.cancelled_tasks,
        ToGatewayMessage::Error(msg) => {
            return CancelJobResponse::Failed(msg.message);
        }
        _ => panic!("Invalid message"),
    };

    let mut state = state_ref.get_mut();
    if let Some(job) = state.get_job_mut(job_id) {
        let canceled_ids: Vec<_> = canceled_tasks
            .iter()
            .map(|tako_id| {
                let (j_id, task_id) = unwrap_tako_id(*tako_id);
                assert_eq!(j_id, job_id);
                job.set_cancel_state(task_id, senders);
                task_id
            })
            .collect();
        let already_finished = job.n_tasks() - canceled_ids.len() as JobTaskCount;
        CancelJobResponse::Canceled(canceled_ids, already_finished)
    } else {
        CancelJobResponse::Canceled(vec![], 0)
    }
}

fn handle_job_forget(
    state_ref: &StateRef,
    selector: &IdSelector,
    allowed_statuses: Vec<Status>,
) -> ToClientMessage {
    let mut state = state_ref.get_mut();
    let job_ids: Vec<JobId> = get_job_ids(&state, selector);
    let mut forgotten: usize = 0;

    for &job_id in &job_ids {
        let can_be_forgotten = state
            .get_job(job_id)
            .map(|j| {
                j.is_terminated() && allowed_statuses.contains(&job_status(&j.make_job_info()))
            })
            .unwrap_or(false);
        if can_be_forgotten {
            state.forget_job(job_id);
            forgotten += 1;
        }
    }
    state.try_release_memory();
    let ignored = job_ids.len() - forgotten;

    ToClientMessage::ForgetJobResponse(ForgetJobResponse { forgotten, ignored })
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

async fn handle_worker_info(state_ref: &StateRef, worker_id: WorkerId) -> ToClientMessage {
    let state = state_ref.get();

    ToClientMessage::WorkerInfoResponse(state.get_worker(worker_id).map(|w| w.make_info()))
}
