use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;
use crate::{rpc_call};
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobStatus, SubmitRequest, ToClientMessage};

use anyhow::anyhow;
use std::path::PathBuf;

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

        Some(name) => validate_name(name)?
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

 fn validate_name(name:String)
                  ->anyhow::Result<String, anyhow::Error> {

     if name.contains('\n') || name.contains('\t'){
         return Err(anyhow!("name cannot have a newline or a tab"));
     } else if name.len()>40 {
         return Err(anyhow!("name cannot be more than 40 characters"));
     } else {
         Ok(name)
     }
 }