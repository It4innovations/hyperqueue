use futures::future::join_all;
use futures::{StreamExt, TryFutureExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use crate::common::rpc::forward_queue_to_sink_with_map;
use crate::messages::gateway::{
    CancelTasksResponse, CollectedOverview, ErrorResponse, FromGatewayMessage,
    GenericResourceNames, NewTasksResponse, TaskInfo, TaskState, TaskUpdate, TasksInfoResponse,
    ToGatewayMessage,
};
use crate::messages::worker::{ToWorkerMessage, WorkerOverview};
use crate::server::comm::{Comm, CommSenderRef};
use crate::server::core::CoreRef;
use crate::server::reactor::{on_cancel_tasks, on_new_tasks, on_set_observe_flag};
use crate::server::task::{TaskRef, TaskRuntimeState};
use crate::transfer::transport::make_protocol_builder;

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
        FromGatewayMessage::NewTasks(msg) => {
            log::debug!("Client sends {} tasks", msg.tasks.len());
            if msg.tasks.is_empty() {
                return Some("Task submission is empty".to_string());
            }
            let mut tasks: Vec<TaskRef> = Vec::with_capacity(msg.tasks.len());
            let mut core = core_ref.get_mut();

            for task in msg.tasks {
                if core.is_used_task_id(task.id) {
                    return Some(format!("Task id={} is already taken", task.id));
                }
                assert!(task.conf.n_outputs == 0 || task.conf.n_outputs == 1); // TODO: Implementation for more outputs

                let task_ref = TaskRef::new(
                    task.id,
                    Vec::new(),
                    task.conf,
                    task.priority,
                    task.keep,
                    task.observe,
                );
                tasks.push(task_ref);
            }
            let mut comm = comm_ref.get_mut();
            on_new_tasks(&mut core, &mut *comm, tasks);

            assert!(client_sender
                .send(ToGatewayMessage::NewTasksResponse(NewTasksResponse {
                    n_waiting_for_workers: 0 // TODO
                }))
                .is_ok());
            None
        }
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
            let task_infos = core
                .get_tasks()
                .map(|tref| {
                    let task = tref.get();
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
        FromGatewayMessage::GetGenericResourceNames(msg) => {
            let mut core = core_ref.get_mut();
            for name in msg.resource_names {
                core.get_or_create_generic_resource_id(&name);
            }
            let message = ToGatewayMessage::GenericResourceNames(GenericResourceNames {
                resource_names: core.generic_resource_names().to_vec(),
            });
            assert!(client_sender.send(message).is_ok());
            None
        }
    }
}
