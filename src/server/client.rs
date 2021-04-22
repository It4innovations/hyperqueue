use std::rc::Rc;

use futures::{Sink, SinkExt, Stream, StreamExt};
use orion::kdf::SecretKey;
use tako::messages::gateway::{FromGatewayMessage, NewTasksMessage, TaskDef, TaskInfo, TaskInfoRequest, ToGatewayMessage};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;

use crate::server::job::Job;
use crate::server::rpc::TakoServer;
use crate::server::state::StateRef;
use crate::transfer::connection::ServerConnection;
use crate::transfer::messages::{FromClientMessage, JobInfo, JobState, StatsResponse, SubmitMessage, SubmitResponse, ToClientMessage};

pub async fn handle_client_connections(
    state_ref: StateRef,
    tako_ref: TakoServer,
    listener: TcpListener,
    end_flag: Sender<()>,
    key: Option<Rc<SecretKey>>,
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
    end_flag: Sender<()>,
    key: Option<Rc<SecretKey>>,
) -> crate::Result<()> {
    log::debug!("New client connection");
    let socket = ServerConnection::accept_client(socket, key).await?;
    let (tx, rx) = socket.split();

    client_rpc_loop(tx, rx, state_ref, tako_ref, end_flag)
        .await;
    log::debug!("Client connection ended");
    Ok(())
}

pub async fn client_rpc_loop<
    Tx: Sink<ToClientMessage> + Unpin,
    Rx: Stream<Item=crate::Result<FromClientMessage>> + Unpin
>(
    mut tx: Tx,
    mut rx: Rx,
    state_ref: StateRef,
    tako_ref: TakoServer,
    end_flag: Sender<()>,
) {
    while let Some(message_result) = rx.next().await {
        if let Ok(message) = message_result {
            let response = match message {
                FromClientMessage::Submit(msg) => {
                    handle_submit(&state_ref, &tako_ref, msg).await
                }
                FromClientMessage::Stats => {
                    compute_stats(&state_ref, &tako_ref).await
                }
                FromClientMessage::Stop => {
                    end_flag.send(()).await.unwrap();
                    continue;
                }
            };
            assert!(tx.send(response).await.is_ok());
        } else {
            log::error!("Cannot parse client message");
            assert!(tx.send(ToClientMessage::Error("Cannot parse message".into())).await.is_ok());
            return;
        }
    }
}

async fn compute_stats(state_ref: &StateRef, tako_ref: &TakoServer) -> ToClientMessage {
    /*let response = tako_ref.send_message(FromGatewayMessage::GetTaskInfo(TaskInfoRequest { tasks: vec![] })).await;

    println!("{:?}", response);

    let task_info : Map<TaskId, TaskInfo> = match response.unwrap() {
        ToGatewayMessage::TaskInfo(info) => {
            info.tasks.into_iter().map(|t| (t.id, t)).collect()
        }
        r => panic!("Invalid server response {:?}", r)
    };*/

    let state = state_ref.get();
    ToClientMessage::StatsResponse(StatsResponse {
        workers: vec![],
        jobs: state.jobs().map(|j| j.make_job_info()).collect(),
    })
}

async fn handle_submit(state_ref: &StateRef, tako_ref: &TakoServer, message: SubmitMessage) -> ToClientMessage {
    let mut state = state_ref.get_mut();
    let task_id = state.new_job_id();

    let mut program_def = message.spec;

    let stdout = format!("stdout.{}", task_id);
    let stderr = format!("stdout.{}", task_id);
    program_def.stdout = Some(message.cwd.join(stdout).into());
    program_def.stderr = Some(message.cwd.join(stderr).into());

    let task_def = TaskDef {
        id: task_id,
        type_id: 0,
        body: rmp_serde::to_vec_named(&program_def).unwrap(),
        keep: false,
        observe: true,
        n_outputs: Default::default(),
    };

    let job = Job::new(task_id, message.name.clone(), program_def);
    state.add_job(job);

    match tako_ref.send_message(FromGatewayMessage::NewTasks(NewTasksMessage { tasks: vec![task_def] })).await.unwrap() {
        ToGatewayMessage::NewTasksResponse(_) => { /* Ok */ }
        _ => { panic!("Invalid response"); }
    };

    ToClientMessage::SubmitResponse(SubmitResponse {
        job: JobInfo {
            id: task_id,
            name: message.name,
            state: JobState::Waiting,
        }
    })
}
