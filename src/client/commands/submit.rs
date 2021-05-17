use std::path::PathBuf;

use clap::Clap;
use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;
use crate::common::arraydef::ArrayDef;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobType, SubmitRequest, ToClientMessage};

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct SubmitOpts {
    command: String,
    args: Vec<String>,

    #[clap(long)]
    array: Option<ArrayDef>,
}

pub async fn submit_computation(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    opts: SubmitOpts,
) -> crate::Result<()> {
    let name = PathBuf::from(&opts.command)
        .file_name()
        .and_then(|t| t.to_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "job".to_string());

    let mut args = opts.args;
    args.insert(0, opts.command);

    let job_type = opts.array.map(JobType::Array).unwrap_or(JobType::Simple);

    let submit_cwd = std::env::current_dir().unwrap();
    let stdout = submit_cwd.join("stdout.%{JOB_ID}.%{TASK_ID}");
    let stderr = submit_cwd.join("stderr.%{JOB_ID}.%{TASK_ID}");

    let message = FromClientMessage::Submit(SubmitRequest {
        job_type,
        name,
        submit_cwd,
        spec: ProgramDefinition {
            args,
            env: Default::default(),
            stdout: Some(stdout),
            stderr: Some(stderr),
            cwd: None,
        },
    });
    let response = rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    print_job_detail(gsettings, response.job, true, false);
    Ok(())
}
