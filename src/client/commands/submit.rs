use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;
use crate::{rpc_call, Error};
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobStatus, SubmitRequest, ToClientMessage};
use std::path::PathBuf;
use crate::common::error::{HqError, invalid_args_error, error};

pub async fn submit_computation(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    name: Option<String>,
    commands: Vec<String>,
) -> crate::Result<()> {
    let name = match name {
        None => {
            commands
                .get(0)
                .and_then(|t| {
                    PathBuf::from(t)
                        .file_name()
                        .and_then(|t| t.to_str().map(|s| s.to_string()))
                })
                .unwrap_or_else(|| "job".to_string())
        }

        Some(name) => {
            validate_and_sanitize_name(name)?
        }
    };

    let message = FromClientMessage::Submit(SubmitRequest {
        name,
        cwd: std::env::current_dir().unwrap(),
        spec: ProgramDefinition {
            args: commands,
            env: Default::default(),
            stdout: None,
            stderr: None,
            cwd: None,
        },
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    response.job.status = JobStatus::Submitted;
    print_job_detail(gsettings, response.job);
    Ok(())
}

 fn validate_and_sanitize_name(mut name:String) ->anyhow::Result<String, Error> {
     if name.contains('\n') || name.contains('\t'){
         error(format!("name cannot have a newline or a tab"))
     } else if name.len()>40 {
         error(format!("name cannot be more than 40 characters"))
     } else {
         Ok((name))
     }
 }