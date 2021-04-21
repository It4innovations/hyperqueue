use tokio::net::TcpListener;
use tokio::io::{AsyncRead, AsyncWrite};
use crate::messages::{FromClientMessage, StatsResponse, ToClientMessage, SubmitMessage, SubmitResponse, JobInfo, JobState};
use futures::{StreamExt, SinkExt};
use crate::server::state::StateRef;
use crate::server::job::Job;
use crate::common::protocol::make_protocol_builder;
use crate::server::rpc::TakoServer;
use tako::messages::gateway::{FromGatewayMessage, ToGatewayMessage, TaskDef, NewTasksMessage};

pub async fn handle_client_connections(
    state_ref: StateRef,
    tako_ref: TakoServer,
    listener: TcpListener
) {
    // TODO: handle invalid connection
    while let Ok((connection, _)) = listener.accept().await {
        let state_ref = state_ref.clone();
        let tako_ref = tako_ref.clone();
        tokio::task::spawn_local(async move {
            log::debug!("New client connection");
            client_rpc_loop(connection, state_ref, tako_ref)
                .await;
            log::debug!("Client connection ended");
        });
    }
}

pub async fn client_rpc_loop<T: AsyncRead + AsyncWrite>(
    stream: T,
    state_ref: StateRef,
    tako_ref: TakoServer,
) {
    let (mut writer, mut reader) = make_protocol_builder().new_framed(stream).split();
    while let Some(message_result) = reader.next().await {
        if let Ok(message_data) = message_result {
            let message: Result<FromClientMessage, _> = rmp_serde::from_slice(&message_data);
            let response = match message {
                Ok(FromClientMessage::Submit(msg)) => {
                    handle_submit(&state_ref, &tako_ref, msg).await
                }
                Ok(FromClientMessage::Stats) => {
                    compute_stats(&state_ref, &tako_ref).await
                }
                Err(_) => {
                    log::error!("Cannot parse client message");
                    ToClientMessage::Error("Cannot parse message".into())
                }
            };
            let send_data = rmp_serde::to_vec_named(&response).unwrap();
            assert!(writer.send(send_data.into()).await.is_ok());
        } else {
            log::error!("Incomplete client message received");
            return;
        }
    }
}

async fn compute_stats(state_ref: &StateRef, tako_ref: &TakoServer) -> ToClientMessage {
    /*let response = tako_ref.send_message(FromGatewayMessage::GetTaskInfo(TaskInfoRequest { tasks: vec![] })).await;

    println!("{:?}", response);

    let task_info : Map<TaskId, TaskInfo> = match response {
        ToGatewayMessage::TaskInfo(info) => {
            info.tasks.into_iter().map(|t| (t.id, t)).collect()
        }
        r => {
            panic!("Invalid server response {:?}", r);
        }
    };*/

    let state = state_ref.get();
    ToClientMessage::StatsResponse(StatsResponse {
        workers: vec![],
        jobs: state.jobs().map(|j| j.make_job_info()).collect()
    })
}

async fn handle_submit(state_ref: &StateRef, tako_ref: &TakoServer, message: SubmitMessage) -> ToClientMessage {
    let mut state = state_ref.get_mut();
    let task_id = state.new_job_id();

    let mut program_def = message.spec;

    let stdout =  format!("stdout.{}", task_id);
    let stderr =  format!("stdout.{}", task_id);
    program_def.stdout = Some(message.cwd.join(stdout).into());
    program_def.stderr = Some(message.cwd.join(stderr).into());

    let task_def = TaskDef {
        id: task_id,
        type_id: 0,
        body: rmp_serde::to_vec_named(&program_def).unwrap(),
        keep: false,
        observe: true,
        n_outputs: Default::default()
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
            state: JobState::Waiting
        }
    })
}
