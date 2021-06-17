use std::path::{Path, PathBuf};

use clap::Clap;
use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;

use crate::client::resources::parse_cpu_request;
use crate::common::arraydef::ArrayDef;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobType, SubmitRequest, ToClientMessage};
use crate::{rpc_call, JobTaskCount};
use anyhow::anyhow;
use bstr::BString;
use std::io::BufRead;
use std::str::FromStr;
use std::{fs, io};
use tako::common::resources::{CpuRequest, ResourceRequest};

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

    /// Number and placement of CPUs for each job
    #[clap(long, default_value = "1")]
    cpus: ArgCpuRequest,

    /// Name of the job
    #[clap(long)]
    name: Option<String>,

    /// Pin the job to the cores specified in `--cpus`.
    #[clap(long)]
    pin: bool,

    /// Path where the standard output of the job will be stored
    #[clap(long)]
    stdout: Option<String>,

    /// Path where the standard error of the job will be stored
    #[clap(long)]
    stderr: Option<String>,

    // Parameters for creating array jobs
    /// Create a task array where a task will be created for each line of the given file.
    /// The corresponding line will be passed to the task in environment variable `HQ_ENTRY`.
    #[clap(long, conflicts_with("array"), value_hint = clap::ValueHint::FilePath)]
    each_line: Option<PathBuf>,

    #[clap(long)]
    /// Create a task array where a task will be created for each number in the specified number range.
    /// Each task will be passed an environment variable `HQ_TASK_ID`.
    ///
    /// `--array=5` - create task array with five jobs with task IDs 0, 1, 2, 3, 4
    ///
    /// `--array=3-5` - create task array with three jobs with task IDs 3, 4, 5
    array: Option<ArrayDef>,
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
) -> anyhow::Result<()> {
    let resources = opts.resource_request();
    resources.validate()?;

    let (job_type, entries) = if let Some(filename) = opts.each_line {
        let lines = read_lines(&filename)?;
        let def = ArrayDef::simple_range(0, lines.len() as JobTaskCount);
        (JobType::Array(def), Some(lines))
    } else {
        (
            opts.array.map(JobType::Array).unwrap_or(JobType::Simple),
            None,
        )
    };

    let name = if let Some(name) = opts.name {
        validate_name(name)?
    } else {
        PathBuf::from(&opts.command)
            .file_name()
            .and_then(|t| t.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "job".to_string())
    };

    let mut args: Vec<BString> = opts
        .args
        .iter()
        .map(|x| BString::from(x.as_str()))
        .collect();
    args.insert(0, opts.command.into());

    let submit_cwd = std::env::current_dir().unwrap();
    let stdout = if opts.stdout.as_deref() == Some("none") {
        None
    } else {
        Some(
            submit_cwd.join(
                opts.stdout
                    .as_deref()
                    .unwrap_or("stdout.%{JOB_ID}.%{TASK_ID}"),
            ),
        )
    };

    let stderr = if opts.stderr.as_deref() == Some("none") {
        None
    } else {
        Some(
            submit_cwd.join(
                opts.stderr
                    .as_deref()
                    .unwrap_or("stderr.%{JOB_ID}.%{TASK_ID}"),
            ),
        )
    };

    let message = FromClientMessage::Submit(SubmitRequest {
        job_type,
        name,
        submit_cwd,
        spec: ProgramDefinition {
            args,
            env: Default::default(),
            stdout,
            stderr,
            cwd: None,
        },
        resources,
        pin: opts.pin,
        entries,
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

// We need to read it as bytes, because not all our users uses UTF-8
fn read_lines(filename: &Path) -> anyhow::Result<Vec<BString>> {
    log::info!("Reading file: {}", filename.display());
    if fs::metadata(filename)?.len() > 100 << 20 {
        log::warn!("Reading file bigger then 100MB");
    };
    let file = std::fs::File::open(filename)?;
    let results: Result<Vec<BString>, std::io::Error> = io::BufReader::new(file)
        .split(b'\n')
        .map(|x| x.map(BString::from))
        .collect();
    Ok(results?)
}
