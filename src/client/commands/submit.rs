use std::path::PathBuf;

use tako::messages::common::ProgramDefinition;

use crate::client::handle_message;
use crate::common::error::error;
use crate::server::bootstrap::get_client_connection;
use crate::transfer::messages::{FromClientMessage, SubmitMessage, ToClientMessage};
use crate::client::job::print_job_stats;

pub async fn submit_computation(rundir_path: PathBuf, commands: Vec<String>) -> crate::Result<()> {
    let mut connection = get_client_connection(rundir_path).await?;

    // TODO: Strip path
    let name = commands.get(0).map(|t| t.to_string()).unwrap_or_else(|| "job".to_string());

    let message = FromClientMessage::Submit(SubmitMessage {
        name: name.clone(),
        cwd: std::env::current_dir().unwrap(),
        spec: ProgramDefinition {
            args: commands,
            env: Default::default(),
            stdout: None,
            stderr: None,
        },
    });
    let response = handle_message(connection.send_and_receive(message).await)?;
    match response {
        ToClientMessage::SubmitResponse(sr) => print_job_stats(vec![sr.job]),
        msg => return error(format!("Received an invalid message {:?}", msg))
    };
    Ok(())
}
