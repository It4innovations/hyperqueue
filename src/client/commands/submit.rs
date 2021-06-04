use std::path::PathBuf;

use clap::Clap;
use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;

use crate::client::resources::parse_cpu_request;
use crate::common::arraydef::ArrayDef;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobType, SubmitRequest, ToClientMessage};
use anyhow::anyhow;
use std::str::FromStr;
use tako::common::resources::{CpuRequest, ResourceRequest};

// I am wrapping CpuRequest + implementing FromStr as I am not able to provide
// own parser function into clap??
struct ArgCpuRequest(CpuRequest);

impl FromStr for ArgCpuRequest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_cpu_request(s).map(ArgCpuRequest)
    }
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct SubmitOpts {
    command: String,
    args: Vec<String>,

    #[clap(long)]
    array: Option<ArrayDef>,

    #[clap(long, default_value = "1")]
    cpus: ArgCpuRequest,

    #[clap(long)]
    name: Option<String>,

    #[clap(long)]
    pin: bool,
}

impl SubmitOpts {
    fn resource_request(&self) -> ResourceRequest {
        ResourceRequest::new(self.cpus.0.clone())
    }
}

pub async fn submit_computation(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    opts: SubmitOpts,
) -> crate::Result<()> {
    let resources = opts.resource_request();

    resources.validate()?;

    let name = if let Some(name) = opts.name {
        validate_name(name)?
    } else {
        PathBuf::from(&opts.command)
            .file_name()
            .and_then(|t| t.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "job".to_string())
    };

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
        resources,
        pin: opts.pin,
    });
    let response = rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    print_job_detail(gsettings, response.job, true, false);
    Ok(())
}

fn validate_name(name: String) -> anyhow::Result<String, anyhow::Error> {
    match name {
        name if name.contains('\n') || name.contains('\t') => {
            Err(anyhow!("name cannot have a newline or a tab"))
        }
        name if name.len() > 40 => Err(anyhow!("name cannot be more than 40 characters")),
        name => Ok(name),
    }
}
