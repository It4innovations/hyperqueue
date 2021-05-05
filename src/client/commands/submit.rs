use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobStatus, SubmitRequest, ToClientMessage};

pub async fn submit_computation(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    commands: Vec<String>,
) -> crate::Result<()> {
    let mut name = extract_task_name_from_path(&commands
        .get(0)
        .map(|t| t.to_string())
        .unwrap_or_else(|| "job".to_string())
    );
    let message = FromClientMessage::Submit(SubmitRequest {
        name: name.clone(),
        cwd: std::env::current_dir().unwrap(),
        spec: ProgramDefinition {
            args: commands,
            env: Default::default(),
            stdout: None,
            stderr: None,
            cwd: None
        },
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    response.job.status = JobStatus::Submitted;
    print_job_detail(gsettings, response.job);
    Ok(())
}

fn extract_task_name_from_path(job_path: &str) ->String{
    let mut job_path: String = job_path.to_string();
    while job_path.ends_with("/") {
        job_path.pop();
    }
    if job_path.is_empty() {
        job_path = "<empty>".to_string();
    }
    let split = job_path.split('/').collect::<Vec<_>>();
    split[split.len()-1].to_string()
}