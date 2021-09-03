use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs, io};

use anyhow::anyhow;
use bstr::BString;
use clap::Clap;
use tako::common::resources::{CpuRequest, ResourceRequest};
use tako::messages::common::{ProgramDefinition, StdioDef};

use crate::client::commands::wait::wait_for_jobs;
use crate::client::globalsettings::GlobalSettings;
use crate::client::job::{get_worker_map, print_job_detail};
use crate::client::resources::parse_cpu_request;
use crate::client::status::StatusList;
use crate::common::arraydef::IntArray;
use crate::common::placeholders::{JOB_ID_PLACEHOLDER, TASK_ID_PLACEHOLDER};
use crate::common::timeutils::ArgDuration;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, JobType, ResubmitRequest, Selector, SubmitRequest, ToClientMessage,
};
use crate::{rpc_call, JobId, JobTaskCount, Map};

const SUBMIT_ARRAY_LIMIT: JobTaskCount = 999;
const DEFAULT_STDOUT_PATH: &str =
    const_format::concatcp!("job-", JOB_ID_PLACEHOLDER, "/stdout.", TASK_ID_PLACEHOLDER);
const DEFAULT_STDERR_PATH: &str =
    const_format::concatcp!("job-", JOB_ID_PLACEHOLDER, "/stderr.", TASK_ID_PLACEHOLDER);

struct ArgCpuRequest(CpuRequest);

impl FromStr for ArgCpuRequest {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_cpu_request(s).map(ArgCpuRequest)
    }
}

#[derive(Debug)]
pub struct ArgEnvironmentVar {
    key: BString,
    value: BString,
}

impl FromStr for ArgEnvironmentVar {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            anyhow::bail!("Environment variable cannot be empty");
        }
        let var = match s.find('=') {
            Some(position) => ArgEnvironmentVar {
                key: s[..position].into(),
                value: s[position + 1..].into(),
            },
            None => ArgEnvironmentVar {
                key: s.into(),
                value: Default::default(),
            },
        };
        Ok(var)
    }
}

/// Represents a filepath. If "none" is passed to it, it will behave as if no path is needed.
struct StdioArg(StdioDef);

impl FromStr for StdioArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(StdioArg(match s {
            "none" => StdioDef::Null,
            _ => StdioDef::File(s.into()),
        }))
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

    /// Working directory for the submitted job.
    /// The path must be accessible from a worker node
    #[clap(long, default_value("%{SUBMIT_DIR}"))]
    cwd: PathBuf,

    /// Path where the standard output of the job will be stored.
    /// The path must be accessible from a worker node
    #[clap(long)]
    stdout: Option<StdioArg>,

    /// Path where the standard error of the job will be stored.
    /// The path must be accessible from a worker node
    #[clap(long)]
    stderr: Option<StdioArg>,

    /// Specify additional environment variable for the job.
    /// You can pass this flag multiple times to pass multiple variables
    ///
    /// `--env=KEY=VAL` - set an environment variable named `KEY` with the value `VAL`
    #[clap(long, multiple_occurrences(true))]
    pub env: Vec<ArgEnvironmentVar>,

    // Parameters for creating array jobs
    /// Create a task array where a task will be created for each line of the given file.
    /// The corresponding line will be passed to the task in environment variable `HQ_ENTRY`.
    #[clap(long, conflicts_with("array"), value_hint = clap::ValueHint::FilePath)]
    each_line: Option<PathBuf>,

    /// Create a task array where a task will be created for each number in the specified number range.
    /// Each task will be passed an environment variable `HQ_TASK_ID`.
    ///
    /// `--array=5` - create task array with one job with task ID 5
    ///
    /// `--array=3-5` - create task array with three jobs with task IDs 3, 4, 5
    #[clap(long)]
    array: Option<IntArray>,

    /// Maximum number of permitted task failures.
    /// If this limit is reached, the job will fail immediately.
    #[clap(long)]
    max_fails: Option<JobTaskCount>,

    #[clap(long, default_value = "0")]
    priority: tako::Priority,

    #[clap(long)]
    /// Time limit per task. E.g. --time-limit=10min
    time_limit: Option<ArgDuration>,

    /// Wait on the job(s) execution.
    #[clap(long)]
    wait: bool,

    /// Stream the output of tasks into this log file.
    #[clap(long)]
    log: Option<PathBuf>,
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

    let (job_type, entries) = if let Some(ref filename) = opts.each_line {
        let lines = read_lines(filename)?;
        let def = IntArray::from_range(0, lines.len() as JobTaskCount);
        (JobType::Array(def), Some(lines))
    } else {
        (
            opts.array
                .as_ref()
                .cloned()
                .map(JobType::Array)
                .unwrap_or(JobType::Simple),
            None,
        )
    };

    check_suspicious_options(&opts, &job_type);

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

    let log = opts.log;
    let cwd = Some(opts.cwd);
    let stdout = opts.stdout.map(|x| x.0).unwrap_or_else(|| {
        if log.is_none() {
            StdioDef::File(DEFAULT_STDOUT_PATH.into())
        } else {
            StdioDef::Pipe
        }
    });
    let stderr = opts.stderr.map(|x| x.0).unwrap_or_else(|| {
        if log.is_none() {
            StdioDef::File(DEFAULT_STDERR_PATH.into())
        } else {
            StdioDef::Pipe
        }
    });

    let env_count = opts.env.len();
    let env: Map<_, _> = opts
        .env
        .into_iter()
        .map(|env| (env.key, env.value))
        .collect();

    if env.len() != env_count {
        log::warn!(
            "Some environment variables were ignored. Check if you haven't used duplicate keys."
        )
    }

    let message = FromClientMessage::Submit(SubmitRequest {
        job_type,
        name,
        spec: ProgramDefinition {
            args,
            env,
            stdout,
            stderr,
            cwd,
        },
        resources,
        pin: opts.pin,
        entries,
        max_fails: opts.max_fails,
        submit_dir: std::env::current_dir().unwrap().to_str().unwrap().into(),
        priority: opts.priority,
        time_limit: opts.time_limit.map(|x| x.into()),
        log,
    });

    let response = rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    let info = response.job.info.clone();

    print_job_detail(
        gsettings,
        response.job,
        true,
        false,
        get_worker_map(connection).await?,
    );
    if opts.wait {
        wait_for_jobs(connection, Selector::Specific(IntArray::from_id(info.id))).await?;
    }
    Ok(())
}

/// Warn user about suspicious submit parameters
fn check_suspicious_options(opts: &SubmitOpts, job_type: &JobType) {
    let is_path_some =
        |path: Option<&StdioArg>| -> bool { path.map_or(true, |x| !matches!(x.0, StdioDef::Null)) };
    let check_path = |path: Option<&StdioArg>, stream: &str| {
        let path = path.and_then(|stdio| match &stdio.0 {
            StdioDef::File(path) => path.to_str(),
            _ => None,
        });
        if let Some(path) = path {
            if !path.contains(TASK_ID_PLACEHOLDER) {
                log::warn!("You have submitted an array job, but the `{}` path does not contain task ID placeholder.\n\
                Individual tasks might thus overwrite the file. Consider adding `{}` to the `--{}` value.", stream, TASK_ID_PLACEHOLDER, stream);
            }
        }
    };
    if let JobType::Array(ref array) = job_type {
        let mut task_files = 0;
        let mut active_dirs = String::new();
        if is_path_some(opts.stdout.as_ref()) {
            task_files += array.id_count();
            active_dirs.push_str(" stdout");
            check_path(opts.stdout.as_ref(), "stdout");
        }
        if is_path_some(opts.stderr.as_ref()) {
            task_files += array.id_count();
            active_dirs.push_str(" stderr");
            check_path(opts.stderr.as_ref(), "stderr");
        }
        if task_files > SUBMIT_ARRAY_LIMIT && opts.log.is_none() {
            log::warn!(
                "The job will create {} files for{}. \
                You may consider using --log option to stream all outputs into one file",
                task_files,
                active_dirs
            );
        }
    }
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct ResubmitOpts {
    job_id: JobId,

    /// Filter only tasks in a given state
    #[clap(long)]
    status: Option<StatusList>,
}

pub async fn resubmit_computation(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    opts: ResubmitOpts,
) -> anyhow::Result<()> {
    let message = FromClientMessage::Resubmit(ResubmitRequest {
        job_id: opts.job_id,
        status: opts.status.map(|x| x.to_vec()),
    });
    let response = rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    print_job_detail(
        gsettings,
        response.job,
        true,
        false,
        get_worker_map(connection).await?,
    );
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::ArgEnvironmentVar;

    #[test]
    fn test_parse_env_empty() {
        let env: Result<ArgEnvironmentVar, _> = FromStr::from_str("");
        assert!(env.is_err());
    }

    #[test]
    fn test_parse_env_key() {
        let env: ArgEnvironmentVar = FromStr::from_str("key").unwrap();
        assert_eq!(env.key, "key");
        assert!(env.value.is_empty());
    }

    #[test]
    fn test_parse_env_empty_value() {
        let env: ArgEnvironmentVar = FromStr::from_str("key=").unwrap();
        assert_eq!(env.key, "key");
        assert!(env.value.is_empty());
    }

    #[test]
    fn test_parse_env_key_value() {
        let env: ArgEnvironmentVar = FromStr::from_str("key=value").unwrap();
        assert_eq!(env.key, "key");
        assert_eq!(env.value, "value");
    }

    #[test]
    fn test_parse_env_multiple_equal_signs() {
        let env: ArgEnvironmentVar = FromStr::from_str("key=value=value2").unwrap();
        assert_eq!(env.key, "key");
        assert_eq!(env.value, "value=value2");
    }
}
