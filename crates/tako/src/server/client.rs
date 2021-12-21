use crate::common::resources::{GenericResourceRequest, ResourceRequest};
use futures::future::join_all;
use futures::{StreamExt, TryFutureExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use crate::common::rpc::forward_queue_to_sink_with_map;
use crate::messages::gateway::{
    CancelTasksResponse, CollectedOverview, ErrorResponse, FromGatewayMessage, NewTasksMessage,
    NewTasksResponse, OverviewRequest, TaskConf, TaskInfo, TaskState, TaskUpdate,
    TasksInfoResponse, ToGatewayMessage,
};
use crate::messages::worker::{ToWorkerMessage, WorkerOverview};
use crate::server::comm::{Comm, CommSender, CommSenderRef};
use crate::server::core::{Core, CoreRef};
use crate::server::monitoring::MonitoringEvent;
use crate::server::reactor::{on_cancel_tasks, on_new_tasks, on_set_observe_flag};
use crate::server::task::{Task, TaskConfiguration, TaskInput, TaskRuntimeState};
use crate::transfer::transport::make_protocol_builder;
use std::rc::Rc;

pub async fn client_connection_handler(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    listener: UnixListener,
    client_sender: UnboundedSender<ToGatewayMessage>,
    client_receiver: UnboundedReceiver<ToGatewayMessage>,
) {
    if let Ok((stream, _)) = listener.accept().await {
        let framed = make_protocol_builder().new_framed(stream);
        let (sender, mut receiver) = framed.split();
        let send_loop = forward_queue_to_sink_with_map(client_receiver, sender, |msg| {
            rmp_serde::to_vec_named(&msg).unwrap().into()
        });
        {
            let core = core_ref.get();
            let mut comm = comm_ref.get_mut();
            for worker in core.get_workers() {
                comm.send_client_worker_new(worker.id, &worker.configuration);
            }
        }
        let receive_loop = async move {
            while let Some(data) = receiver.next().await {
                // TODO: Instead of unwrap, send error message to client
                let data = data.unwrap();
                let message: Result<FromGatewayMessage, _> = rmp_serde::from_slice(&data);
                let error = match message {
                    Ok(message) => {
                        process_client_message(&core_ref, &comm_ref, &client_sender, message).await
                    }
                    Err(e) => Some(format!("Invalid format of message: {}", e.to_string())),
                };
                if let Some(message) = error {
                    client_sender
                        .send(ToGatewayMessage::Error(ErrorResponse { message }))
                        .unwrap();
                }
            }
        };
        tokio::select! {
            r = send_loop => { r.unwrap() },
            () = receive_loop => {},
        }
    } else {
        panic!("Invalid connection from client");
    }
    log::info!("Client connection terminated");
}

fn create_task_configuration(core_ref: &mut Core, msg: TaskConf) -> TaskConfiguration {
    let resources_msg = msg.resources;
    let generic_resources = resources_msg
        .generic
        .into_iter()
        .map(|req| {
            let resource = core_ref.get_or_create_generic_resource_id(&req.resource);
            GenericResourceRequest {
                resource,
                amount: req.amount,
            }
        })
        .collect();

    let resources = ResourceRequest::new(
        resources_msg.cpus,
        resources_msg.min_time,
        generic_resources,
    );

    TaskConfiguration {
        resources,
        n_outputs: msg.n_outputs,
        time_limit: msg.time_limit,
        user_priority: msg.priority,
    }
}

pub async fn fetch_worker_overviews(
    core_ref: &CoreRef,
    comm_ref: &CommSenderRef,
    overview_request: OverviewRequest,
) -> Vec<WorkerOverview> {
    let overview_receivers = {
        let mut core = core_ref.get_mut();

        let mut overview_receivers = Vec::with_capacity(core.get_worker_map().len());
        for worker in core.get_workers_mut() {
            //for each worker, create a one-shot channel, pass it to the worker and store it's receiver
            let (sender, receiver) = oneshot::channel();
            worker.overview_callbacks.push(sender);
            overview_receivers.push(receiver.into_future());
        }
        comm_ref
            .get_mut()
            .broadcast_worker_message(&ToWorkerMessage::GetOverview(overview_request));
        overview_receivers
    };
    let worker_overviews: Vec<WorkerOverview> = join_all(overview_receivers)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();
    log::debug!("Gathering overview finished");
    worker_overviews
}

pub async fn process_client_message(
    core_ref: &CoreRef,
    comm_ref: &CommSenderRef,
    client_sender: &UnboundedSender<ToGatewayMessage>,
    message: FromGatewayMessage,
) -> Option<String> {
    match message {
        FromGatewayMessage::ObserveTasks(msg) => {
            let mut core = core_ref.get_mut();
            let mut comm = comm_ref.get_mut();
            for task_id in msg.tasks {
                log::debug!("Client start observing task={}", task_id);
                if !on_set_observe_flag(&mut core, &mut *comm, task_id, true) {
                    log::debug!(
                        "Client ask for observing of invalid (old?) task={}",
                        task_id
                    );
                    client_sender
                        .send(ToGatewayMessage::TaskUpdate(TaskUpdate {
                            id: task_id,
                            state: if core.is_used_task_id(task_id) {
                                TaskState::Finished
                            } else {
                                TaskState::Invalid
                            },
                        }))
                        .unwrap();
                };
            }
            None
        }
        FromGatewayMessage::NewTasks(msg) => handle_new_tasks(
            &mut core_ref.get_mut(),
            &mut comm_ref.get_mut(),
            client_sender,
            msg,
        ),
        FromGatewayMessage::ServerInfo => {
            let core = core_ref.get();
            assert!(client_sender
                .send(ToGatewayMessage::ServerInfo(core.get_server_info()))
                .is_ok());
            None
        }
        FromGatewayMessage::GetTaskInfo(request) => {
            log::debug!("Client asked for task info");
            if !request.tasks.is_empty() {
                todo!();
            }
            let core = core_ref.get();
            let task_map = core.task_map();
            let task_infos = task_map
                .task_ids()
                .map(|task_id| {
                    let task = task_map.get_task(task_id);
                    TaskInfo {
                        id: task.id,
                        state: match task.state {
                            TaskRuntimeState::Waiting(_) => TaskState::Waiting,
                            TaskRuntimeState::Assigned(_) => TaskState::Waiting,
                            TaskRuntimeState::Stealing(_, _) => TaskState::Waiting,
                            TaskRuntimeState::Running(_) => TaskState::Waiting,
                            TaskRuntimeState::Finished(_) => TaskState::Finished,
                            TaskRuntimeState::Released => {
                                unreachable!()
                            }
                        },
                    }
                })
                .collect();
            assert!(client_sender
                .send(ToGatewayMessage::TaskInfo(TasksInfoResponse {
                    tasks: task_infos
                }))
                .is_ok());
            None
        }
        FromGatewayMessage::GetOverview(overview_request) => {
            log::debug!("Client ask for overview");
            let worker_overviews =
                fetch_worker_overviews(core_ref, comm_ref, overview_request).await;
            assert!(client_sender
                .send(ToGatewayMessage::Overview(CollectedOverview {
                    worker_overviews
                }))
                .is_ok());
            None
        }
        FromGatewayMessage::CancelTasks(msg) => {
            log::debug!("Client asked for canceling tasks");
            let mut core = core_ref.get_mut();
            let mut comm = comm_ref.get_mut();
            let (cancelled_tasks, already_finished) =
                on_cancel_tasks(&mut core, &mut *comm, &msg.tasks);
            assert!(client_sender
                .send(ToGatewayMessage::CancelTasksResponse(CancelTasksResponse {
                    cancelled_tasks,
                    already_finished
                }))
                .is_ok());
            None
        }
        FromGatewayMessage::StopWorker(msg) => {
            let mut core = core_ref.get_mut();
            if let Some(ref mut worker) = core.get_worker_mut(msg.worker_id) {
                worker.set_stopping_flag(true);
                let mut comm = comm_ref.get_mut();
                comm.send_worker_message(msg.worker_id, &ToWorkerMessage::Stop);
                assert!(client_sender.send(ToGatewayMessage::WorkerStopped).is_ok());
                None
            } else {
                Some(format!("Worker with id {} not found", msg.worker_id))
            }
        }
        FromGatewayMessage::GetMonitoringEvents(request) => {
            let after_id = request.after_id.unwrap_or(0);

            let mut core = core_ref.get_mut();
            let events: Vec<MonitoringEvent> = core
                .get_event_storage()
                .get_events_after(after_id)
                .cloned()
                .collect();
            assert!(client_sender
                .send(ToGatewayMessage::MonitoringEvents(events))
                .is_ok());
            None
        }
    }
}

fn handle_new_tasks(
    core: &mut Core,
    comm: &mut CommSender,
    client_sender: &UnboundedSender<ToGatewayMessage>,
    msg: NewTasksMessage,
) -> Option<String> {
    log::debug!("Client sends {} tasks", msg.tasks.len());
    if msg.tasks.is_empty() {
        return Some("Task submission is empty".to_string());
    }

    let configurations: Vec<_> = msg
        .configurations
        .into_iter()
        .map(|c| {
            assert!(c.n_outputs == 0 || c.n_outputs == 1); // TODO: Implementation for more outputs
            let keep = c.keep;
            let observe = c.observe;
            (Rc::new(create_task_configuration(core, c)), keep, observe)
        })
        .collect();

    let mut tasks: Vec<Task> = Vec::with_capacity(msg.tasks.len());
    for task in msg.tasks {
        if core.is_used_task_id(task.id) {
            return Some(format!("Task id={} is already taken", task.id));
        }
        let idx = task.conf_idx as usize;
        if idx >= configurations.len() {
            return Some(format!("Invalid configuration index {}", idx));
        }
        let (conf, keep, observe) = &configurations[idx];
        let inputs: Vec<_> = task
            .task_deps
            .iter()
            .map(|&task_id| TaskInput::new_task_dependency(task_id))
            .collect();
        let task = Task::new(task.id, inputs, conf.clone(), task.body, *keep, *observe);
        tasks.push(task);
    }
    on_new_tasks(core, comm, tasks);

    assert!(client_sender
        .send(ToGatewayMessage::NewTasksResponse(NewTasksResponse {
            n_waiting_for_workers: 0 // TODO
        }))
        .is_ok());
    None
}
