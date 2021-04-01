use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use crate::messages::gateway::{ToGatewayMessage, FromGatewayMessage, NewTasksResponse, ErrorResponse, TaskUpdate, TaskState, ServerInfo, TaskInfo, TasksInfoResponse};
use tokio::net::UnixListener;
use crate::transfer::transport::make_protocol_builder;
use crate::common::rpc::{forward_queue_to_sink, forward_queue_to_sink_with_map};
use futures::StreamExt;
use std::error::Error;
use crate::server::task::{TaskRef, TaskRuntimeState};
use crate::server::reactor::{on_new_tasks, on_set_observe_flag};
use crate::server::core::CoreRef;
use crate::server::comm::CommSenderRef;


pub async fn client_connection_handler(core_ref: CoreRef, comm_ref: CommSenderRef, mut listener: UnixListener, client_sender: UnboundedSender<ToGatewayMessage>, client_receiver: UnboundedReceiver<ToGatewayMessage>) {
    if let Some(Ok(mut stream)) = listener.next().await {
        let mut framed = make_protocol_builder().new_framed(stream);
        let (sender, mut receiver) = framed.split();
        let send_loop = forward_queue_to_sink_with_map(client_receiver, sender, |msg| rmp_serde::to_vec_named(&msg).unwrap().into());
        let receive_loop = async move {
            while let Some(data) = receiver.next().await {
                // TODO: Instead of unwrap, send error message to client
                let data = data.unwrap();
                let message : Result<FromGatewayMessage, _> = rmp_serde::from_slice(&data);
                let error = match message {
                    Ok(message) => process_client_message(&core_ref, &comm_ref, &client_sender, message).await,
                    Err(e) => Some(format!("Invalid format of message: {}", e.to_string()))
                };
                if let Some(message) = error {
                    client_sender.send(ToGatewayMessage::Error(ErrorResponse{message}));
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

pub async fn process_client_message(core_ref: &CoreRef, comm_ref: &CommSenderRef, client_sender: &UnboundedSender<ToGatewayMessage>, message: FromGatewayMessage) -> Option<String> {
    match message {
        FromGatewayMessage::ObserveTasks(msg) => {
            let mut core = core_ref.get_mut();
            let mut comm = comm_ref.get_mut();
            for task_id in msg.tasks {
                log::debug!("Client start observing task={}", task_id);
                if !on_set_observe_flag(&mut core, &mut *comm, task_id, true) {
                    log::debug!("Client ask for observing of invalid (old?) task={}", task_id);
                    client_sender.send(ToGatewayMessage::TaskUpdate(TaskUpdate {
                        id: task_id,
                        state: if core.is_used_task_id(task_id) { TaskState::Finished } else { TaskState::Invalid }
                    }));
                };
            }
            None
        },
        FromGatewayMessage::NewTasks(msg) => {
            log::debug!("Client sends {} tasks", msg.tasks.len());
            if msg.tasks.is_empty() {
                return Some("Task submission is empty".to_string())
            }
            let mut tasks : Vec<TaskRef> = Vec::with_capacity(msg.tasks.len());
            let mut core = core_ref.get_mut();
            for task in msg.tasks {
                if core.is_used_task_id(task.id) {
                    return Some(format!("Task id={} is already taken", task.id))
                }
                let task_ref = TaskRef::new(
                    task.id,
                    task.type_id,
                    task.body,
                    Vec::new(),
                    0,
                    0,
                    task.keep,
                    task.observe,
                );
                tasks.push(task_ref);
            }
            let mut comm = comm_ref.get_mut();
            on_new_tasks(&mut core, &mut *comm, tasks);

            assert!(client_sender.send(ToGatewayMessage::NewTasksResponse(NewTasksResponse {
                n_waiting_for_workers: 0  // TODO
            })).is_ok());
            None
        }
        FromGatewayMessage::RegisterSubworker(sw_def) => {
            todo!()
        }
        FromGatewayMessage::ServerInfo => {
            let core = core_ref.get();
            assert!(client_sender.send(ToGatewayMessage::ServerInfo(core.get_server_info())).is_ok());
            None
        }
        FromGatewayMessage::GetTaskInfo(request) => {
            log::debug!("Client asked for task info");
            let core = core_ref.get();
            let task_infos = core.get_tasks().map(|tref| {
                let task = tref.get();
                TaskInfo {
                    id: task.id,
                    state: match task.state {
                        TaskRuntimeState::Waiting(_) => { TaskState::Waiting }
                        TaskRuntimeState::Assigned(_) => { TaskState::Waiting }
                        TaskRuntimeState::Stealing(_, _) => { TaskState::Waiting }
                        TaskRuntimeState::Running(_) => { TaskState::Waiting }
                        TaskRuntimeState::Finished(_, _) => { TaskState::Finished }
                        TaskRuntimeState::Released => { unreachable!() }
                    }
                }
            }).collect();
            assert!(client_sender.send(ToGatewayMessage::TaskInfo(TasksInfoResponse { tasks: task_infos })).is_ok());
            None
        }
    }
}