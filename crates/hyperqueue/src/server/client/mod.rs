use chrono::Utc;
use std::fmt::Debug;
use std::sync::Arc;

use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::kdf::SecretKey;
use tako::{Set, TaskGroup, TaskId};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Notify, mpsc};

use crate::client::status::{Status, job_status};
use crate::common::serverdir::ServerDir;
use crate::server::event::Event;
use crate::server::job::{JobTaskCounters, JobTaskState};
use crate::server::state::{State, StateRef};
use crate::transfer::connection::accept_client;
use crate::transfer::messages::{
    CancelJobResponse, CloseJobResponse, FromClientMessage, IdSelector, JobDetail,
    JobDetailResponse, JobInfoResponse, JobSubmitDescription, StopWorkerResponse, SubmitRequest,
    TaskSelector, ToClientMessage, WorkerListResponse,
};
use crate::transfer::messages::{ForgetJobResponse, WaitForJobsResponse};
use tako::{JobId, JobTaskCount, WorkerId};

pub mod autoalloc;
mod submit;

use crate::common::serialization::Serialized;
use crate::server::Senders;
use crate::server::client::submit::{handle_open_job, handle_task_explain};
use crate::server::event::payload::EventPayload;
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
                log::error!("Client error: {e}");
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
    let socket = accept_client(socket, key).await?;
    let (tx, rx) = socket.split();

    client_rpc_loop(tx, rx, server_dir, state_ref, carrier, end_flag).await;
    log::debug!("Client connection ended");
    Ok(())
}

async fn stream_history_events<Tx: Sink<ToClientMessage, Error = tako::Error> + Unpin + 'static>(
    tx: &mut Tx,
    mut history: mpsc::UnboundedReceiver<Event>,
) {
    log::debug!("Resending history started");

    let mut events = Vec::with_capacity(1024);
    let capacity = events.capacity();
    while history.recv_many(&mut events, capacity).await != 0 {
        let events = std::mem::replace(&mut events, Vec::with_capacity(capacity));
        if tx
            .send_all(&mut futures::stream::iter(events).map(|e| Ok(ToClientMessage::Event(e))))
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn stream_events<
    Tx: Sink<ToClientMessage, Error = tako::Error> + Unpin + 'static,
    Rx: Stream<Item = tako::Result<FromClientMessage>> + Unpin,
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
                return;
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
    Tx: Sink<ToClientMessage, Error = tako::Error> + Unpin + 'static,
    Rx: Stream<Item = tako::Result<FromClientMessage>> + Unpin,
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
                        submit::handle_submit(&state_ref, senders, msg)
                    }
                    FromClientMessage::JobInfo(msg) => compute_job_info(&state_ref, &msg.selector),
                    FromClientMessage::Stop => {
                        end_flag.notify_one();
                        break;
                    }
                    FromClientMessage::WorkerList => handle_worker_list(&state_ref),
                    FromClientMessage::WorkerInfo(msg) => {
                        handle_worker_info(&state_ref, senders, msg.worker_id, msg.runtime_info)
                    }
                    FromClientMessage::StopWorker(msg) => {
                        handle_worker_stop(&state_ref, senders, msg.selector)
                    }
                    FromClientMessage::Cancel(msg) => {
                        handle_job_cancel(&state_ref, senders, &msg.selector).await
                    }
                    FromClientMessage::ForgetJob(msg) => {
                        handle_job_forget(&state_ref, senders, &msg.selector, msg.filter)
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
                        let enable_worker_overviews = msg.enable_worker_overviews;
                        if enable_worker_overviews {
                            senders.server_control.add_worker_overview_listener();
                        }
                        log::debug!("Start streaming events to client");
                        /* We create two event queues, one for historic events and one for live events
                        So while historic events are loaded from the file and streamed, live events are already
                        collected and sent immediately once the historic events are sent */
                        let (tx1, rx1) = mpsc::unbounded_channel::<Event>();
                        let live = if msg.live_events {
                            let (tx2, rx2) = mpsc::unbounded_channel::<Event>();
                            let listener_id = senders.events.register_listener(tx2);
                            Some((rx2, listener_id))
                        } else {
                            None
                        };

                        // If we use a journal, we can replay historical events from it.
                        // If not, we can at least try to reconstruct a few basic events
                        // based on the current state.
                        if senders.events.is_journal_enabled() {
                            senders.events.start_journal_replay(tx1);
                        } else if let Err(e) = reconstruct_historical_events(state_ref, tx1) {
                            log::error!("Cannot reconstruct historical state: {e:?}");
                        }

                        stream_history_events(&mut tx, rx1).await;

                        if let Some((rx2, listener_id)) = live {
                            let _ = tx.send(ToClientMessage::EventLiveBoundary).await;
                            stream_events(&mut tx, &mut rx, rx2).await;
                            senders.events.unregister_listener(listener_id);
                        }

                        if enable_worker_overviews {
                            senders.server_control.remove_worker_overview_listener();
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
                    FromClientMessage::TaskExplain(request) => {
                        handle_task_explain(&state_ref, senders, request)
                    }
                };
                if let Err(error) = tx.send(response).await {
                    log::error!("Cannot reply to client: {error:?}");
                    break;
                }
            }
            Err(e) => {
                log::error!("Cannot parse client message: {e}");
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

/// Tries to reconstruct historical events based on the current state.
fn reconstruct_historical_events(
    state: StateRef,
    event_sender: UnboundedSender<Event>,
) -> anyhow::Result<()> {
    let state = state.get();

    // We buffer events in memory so that we can sort them by timestamp
    let mut events = Vec::with_capacity(1024);

    events.push(Event::at(
        state.server_info().start_date,
        EventPayload::ServerStart {
            server_uid: state.server_info().server_uid.clone(),
        },
    ));

    // Reconstruct worker connections
    for (id, worker) in state.get_workers() {
        events.push(Event::at(
            worker.started_at(),
            EventPayload::WorkerConnected(*id, Box::new(worker.configuration().clone())),
        ));
    }

    // Reconstruct job submits
    for job in state.jobs() {
        events.push(Event::at(
            job.submission_date,
            EventPayload::JobOpen(job.job_id, job.job_desc.clone()),
        ));
        for submit in &job.submit_descs {
            let submit_request = Serialized::new(&SubmitRequest {
                job_desc: job.job_desc.clone(),
                submit_desc: JobSubmitDescription {
                    task_desc: submit.description().task_desc.clone(),
                    submit_dir: submit.description().submit_dir.clone(),
                    stream_path: submit.description().stream_path.clone(),
                },
                job_id: Some(job.job_id),
            })?;
            events.push(Event::at(
                submit.submitted_at(),
                EventPayload::Submit {
                    job_id: job.job_id,
                    closed_job: false,
                    serialized_desc: submit_request,
                },
            ));
        }

        for (id, task) in &job.tasks {
            // Task start
            let started_data = match &task.state {
                JobTaskState::Running { started_data }
                | JobTaskState::Finished { started_data, .. }
                | JobTaskState::Failed {
                    started_data: Some(started_data),
                    ..
                }
                | JobTaskState::Canceled {
                    started_data: Some(started_data),
                    ..
                } => Some(started_data.clone()),
                JobTaskState::Waiting
                | JobTaskState::Failed { .. }
                | JobTaskState::Canceled { .. } => None,
            };
            if let Some(started_data) = started_data {
                events.push(Event::at(
                    started_data.start_date,
                    EventPayload::TaskStarted {
                        task_id: TaskId::new(job.job_id, *id),
                        instance_id: started_data.context.instance_id,
                        workers: started_data.worker_ids.clone(),
                    },
                ));
            }

            // Task end

            match &task.state {
                JobTaskState::Finished { end_date, .. } => {
                    events.push(Event::at(
                        *end_date,
                        EventPayload::TaskFinished {
                            task_id: TaskId::new(job.job_id, *id),
                        },
                    ));
                }
                JobTaskState::Failed {
                    end_date, error, ..
                } => {
                    events.push(Event::at(
                        *end_date,
                        EventPayload::TaskFailed {
                            task_id: TaskId::new(job.job_id, *id),
                            error: error.clone(),
                        },
                    ));
                }
                JobTaskState::Canceled { cancelled_date, .. } => {
                    if let Some(task_ids) = events.last_mut().and_then(|e| {
                        if let EventPayload::TasksCanceled { task_ids, .. } = &mut e.payload {
                            (e.time == *cancelled_date).then_some(task_ids)
                        } else {
                            None
                        }
                    }) {
                        task_ids.push(TaskId::new(job.job_id, *id))
                    } else {
                        events.push(Event::at(
                            *cancelled_date,
                            EventPayload::TasksCanceled {
                                task_ids: vec![TaskId::new(job.job_id, *id)],
                            },
                        ));
                    }
                }
                JobTaskState::Waiting | JobTaskState::Running { .. } => {}
            };
        }

        // Job end
        if let Some(completion_date) = job.completion_date {
            events.push(Event::at(
                completion_date,
                EventPayload::JobCompleted(job.job_id),
            ));
        }
    }

    // Reconstruct worker disconnections
    // In theory, if a worker disconnect event had the same timestamp as something else,
    // keep the worker disconnection after that after event
    for (id, worker) in state.get_workers() {
        if let Some(ended) = worker.make_info(None).ended {
            events.push(Event::at(
                ended.ended_at,
                EventPayload::WorkerLost(*id, ended.reason),
            ));
        }
    }

    // Use a stable sort to keep relative ordering between events the same
    events.sort_by_key(|e| e.time);
    for event in events {
        event_sender.send(event)?;
    }
    Ok(())
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
            Err(err) => log::error!("Error while waiting on job(s): {err:?}"),
        };
    }

    ToClientMessage::WaitForJobsResponse(response)
}

fn handle_worker_stop(
    state_ref: &StateRef,
    senders: &Senders,
    selector: IdSelector,
) -> ToClientMessage {
    log::debug!("Client asked for worker termination {selector:?}");
    let mut responses: Vec<(WorkerId, StopWorkerResponse)> = Vec::new();

    let worker_ids: Vec<WorkerId> = match selector {
        IdSelector::Specific(array) => array.iter().map(|id| id.into()).collect(),
        IdSelector::All => state_ref
            .get()
            .get_workers()
            .iter()
            .filter(|(_, worker)| worker.make_info(None).ended.is_none())
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
            if worker.make_info(None).ended.is_some() {
                responses.push((worker_id, StopWorkerResponse::AlreadyStopped));
                continue;
            }
        } else {
            responses.push((worker_id, StopWorkerResponse::InvalidWorker));
            continue;
        }
        let response = senders.server_control.stop_worker(worker_id);

        match response {
            Ok(()) => responses.push((worker_id, StopWorkerResponse::Stopped)),
            Err(err) => {
                responses.push((worker_id, StopWorkerResponse::Failed(err.to_string())));
                log::error!("Unable to stop worker: {worker_id} error: {err:?}");
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
    senders: &Senders,
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
        let response = cancel_job(state_ref, senders, job_id).await;
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
    let task_ids = match state_ref.get().get_job(job_id) {
        None => {
            return CancelJobResponse::InvalidJob;
        }
        Some(job) => {
            let tasks = job.non_finished_task_ids();
            if tasks.is_empty() {
                return CancelJobResponse::Canceled(Vec::new(), job.n_tasks());
            }
            tasks
        }
    };
    if !task_ids.is_empty() {
        senders.server_control.cancel_tasks(&task_ids);
    }
    let mut state = state_ref.get_mut();
    if let Some(job) = state.get_job_mut(job_id) {
        let already_finished = job.n_tasks() - task_ids.len() as JobTaskCount;
        let job_task_ids = task_ids
            .iter()
            .map(|task_id| task_id.job_task_id())
            .collect();
        job.set_cancel_state(task_ids, senders);
        CancelJobResponse::Canceled(job_task_ids, already_finished)
    } else {
        CancelJobResponse::Canceled(vec![], 0)
    }
}

fn handle_job_forget(
    state_ref: &StateRef,
    senders: &Senders,
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
    senders.server_control.try_release_memory();

    let ignored = job_ids.len() - forgotten;

    ToClientMessage::ForgetJobResponse(ForgetJobResponse { forgotten, ignored })
}

fn handle_worker_list(state_ref: &StateRef) -> ToClientMessage {
    let state = state_ref.get();

    ToClientMessage::WorkerListResponse(WorkerListResponse {
        workers: state
            .get_workers()
            .values()
            .map(|w| w.make_info(None))
            .collect(),
    })
}

fn handle_worker_info(
    state_ref: &StateRef,
    senders: &Senders,
    worker_id: WorkerId,
    runtime_info: bool,
) -> ToClientMessage {
    let state = state_ref.get();
    ToClientMessage::WorkerInfoResponse(state.get_worker(worker_id).map(|w| {
        w.make_info(if runtime_info && w.is_running() {
            senders.server_control.worker_info(worker_id)
        } else {
            None
        })
    }))
}
