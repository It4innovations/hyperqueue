use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs, io};

use anyhow::anyhow;
use bstr::BString;
use clap::Parser;
use tako::common::resources::{CpuRequest, GenericResourceAmount};
use tako::messages::common::{ProgramDefinition, StdioDef};
use tako::messages::gateway::{GenericResourceRequest, ResourceRequest};

use crate::client::commands::wait::{wait_for_jobs, wait_for_jobs_with_progress};
use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::resources::{parse_cpu_request, parse_resource_request};
use crate::client::status::StatusList;
use crate::common::arraydef::IntArray;
use crate::common::placeholders::{
    parse_resolvable_string, StringPart, CWD_PLACEHOLDER, JOB_ID_PLACEHOLDER,
    SUBMIT_DIR_PLACEHOLDER, TASK_ID_PLACEHOLDER,
};
use crate::common::timeutils::ArgDuration;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, JobDescription, ResubmitRequest, Selector, SubmitRequest, TaskDescription,
    ToClientMessage,
};
use crate::{rpc_call, JobTaskCount, Map};

const SUBMIT_ARRAY_LIMIT: JobTaskCount = 999;

// Keep in sync with tests/util/job.py::default_task_output
const DEFAULT_STDOUT_PATH: &str = const_format::concatcp!(
    "%{",
    SUBMIT_DIR_PLACEHOLDER,
    "}",
    "/",
    "job-",
    "%{",
    JOB_ID_PLACEHOLDER,
    "}",
    "/",
    "%{",
    TASK_ID_PLACEHOLDER,
    "}",
    ".stdout"
);
const DEFAULT_STDERR_PATH: &str = const_format::concatcp!(
    "%{",
    SUBMIT_DIR_PLACEHOLDER,
    "}",
    "/",
    "job-",
    "%{",
    JOB_ID_PLACEHOLDER,
    "}",
    "/",
    "%{",
    TASK_ID_PLACEHOLDER,
    "}",
    ".stderr"
);

crate::arg_wrapper!(ArgCpuRequest, CpuRequest, parse_cpu_request);
crate::arg_wrapper!(
    ArgNamedResourceRequest,
    (String, GenericResourceAmount),
    parse_resource_request
);

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

#[derive(Parser)]
pub struct SubmitOpts {
    command: String,
    args: Vec<String>,

    /// Number and placement of CPUs for each job
    #[clap(long, default_value = "1")]
    cpus: ArgCpuRequest,

    /// Generic resource request in form <NAME>=<AMOUNT>
    #[clap(long, setting = clap::ArgSettings::MultipleOccurrences)]
    resource: Vec<ArgNamedResourceRequest>,

    /// Minimal lifetime of the worker needed to start the job
    #[clap(long, default_value = "0ms")]
    time_request: ArgDuration,

    /// Name of the job
    #[clap(long)]
    name: Option<String>,

    /// Pin the job to the cores specified in `--cpus`.
    #[clap(long)]
    pin: bool,

    /// Working directory for the submitted job.
    /// The path must be accessible from worker nodes
    #[clap(long, default_value("%{SUBMIT_DIR}"))]
    cwd: PathBuf,

    /// Path where the standard output of the job will be stored.
    /// The path must be accessible from worker nodes
    #[clap(long)]
    stdout: Option<StdioArg>,

    /// Path where the standard error of the job will be stored.
    /// The path must be accessible from worker nodes
    #[clap(long)]
    stderr: Option<StdioArg>,

    /// Specify additional environment variable for the job.
    /// You can pass this flag multiple times to pass multiple variables
    ///
    /// `--env=KEY=VAL` - set an environment variable named `KEY` with the value `VAL`
    #[clap(long, multiple_occurrences(true))]
    env: Vec<ArgEnvironmentVar>,

    // Parameters for creating array jobs
    /// Create a task array where a task will be created for each line of the given file.
    /// The corresponding line will be passed to the task in environment variable `HQ_ENTRY`.
    #[clap(long, conflicts_with("array"), value_hint = clap::ValueHint::FilePath)]
    each_line: Option<PathBuf>,

    /// Create a task array where a task will be created for each item of a JSON array stored in
    /// the given file.
    /// The corresponding item from the array will be passed as a JSON string to the task in
    /// environment variable `HQ_ENTRY`.
    #[clap(long, conflicts_with("array"), conflicts_with("each-line"), value_hint = clap::ValueHint::FilePath)]
    from_json: Option<PathBuf>,

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

    /// Wait for the job to finish.
    #[clap(long, conflicts_with("progress"))]
    wait: bool,

    /// Interactively observe the progress of the submitted job.
    #[clap(long, conflicts_with("wait"))]
    progress: bool,

    /// Stream the output of tasks into this log file.
    #[clap(long)]
    log: Option<PathBuf>,
}

impl SubmitOpts {
    fn resource_request(&self) -> ResourceRequest {
        let generic_resources = self
            .resource
            .iter()
            .map(|gr| {
                let rq = gr.get().clone();
                GenericResourceRequest {
                    resource: rq.0,
                    amount: rq.1,
                }
            })
            .collect();

        ResourceRequest {
            cpus: self.cpus.get().clone(),
            min_time: *self.time_request.get(),
            generic: generic_resources,
        }
    }
}

fn create_stdio(arg: Option<StdioArg>, log: &Option<PathBuf>, default: &str) -> StdioDef {
    arg.map(|x| x.0).unwrap_or_else(|| {
        if log.is_none() {
            StdioDef::File(default.into())
        } else {
            StdioDef::Pipe
        }
    })
}

pub async fn submit_computation(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    opts: SubmitOpts,
) -> anyhow::Result<()> {
    let resources = opts.resource_request();
    let (ids, entries) = get_ids_and_entries(&opts)?;
    let task_count = ids.id_count();

    check_suspicious_options(&opts, task_count)?;

    let SubmitOpts {
        command,
        args,
        cpus: _,
        resource: _,
        time_request: _,
        name,
        pin,
        cwd,
        stdout,
        stderr,
        env,
        each_line: _,
        from_json: _,
        array: _,
        max_fails,
        priority,
        time_limit,
        wait,
        progress,
        log,
    } = opts;

    let name = if let Some(name) = name {
        validate_name(name)?
    } else {
        PathBuf::from(&command)
            .file_name()
            .and_then(|t| t.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "job".to_string())
    };

    let mut args: Vec<BString> = args.iter().map(|x| BString::from(x.as_str())).collect();
    args.insert(0, command.into());

    let cwd = Some(cwd);
    let stdout = create_stdio(stdout, &log, DEFAULT_STDOUT_PATH);
    let stderr = create_stdio(stderr, &log, DEFAULT_STDERR_PATH);

    let env_count = env.len();
    let env: Map<_, _> = env.into_iter().map(|env| (env.key, env.value)).collect();

    if env.len() != env_count {
        log::warn!(
            "Some environment variables were ignored. Check if you haven't used duplicate keys."
        )
    }

    let program_def = ProgramDefinition {
        args,
        env,
        stdout,
        stderr,
        cwd,
    };
    let task_desc = TaskDescription {
        program: program_def,
        resources,
        pin,
        priority,
        time_limit: time_limit.map(|x| x.unpack()),
    };

    let job_desc = JobDescription::Array {
        ids,
        entries,
        task_desc,
    };

    let message = FromClientMessage::Submit(SubmitRequest {
        job_desc,
        name,
        max_fails,
        submit_dir: std::env::current_dir().expect("Cannot get current working directory"),
        log,
    });

    let response = rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    let info = response.job.info.clone();

    gsettings.printer().print_job_submitted(response.job);
    if wait {
        wait_for_jobs(
            gsettings,
            connection,
            Selector::Specific(IntArray::from_id(info.id.into())),
        )
        .await?;
    } else if progress {
        wait_for_jobs_with_progress(connection, vec![info]).await?;
    }
    Ok(())
}

fn get_ids_and_entries(opts: &SubmitOpts) -> anyhow::Result<(IntArray, Option<Vec<BString>>)> {
    let entries = if let Some(ref filename) = opts.each_line {
        Some(read_lines(filename)?)
    } else if let Some(ref filename) = opts.from_json {
        Some(make_entries_from_json(filename)?)
    } else {
        None
    };

    let ids = if let Some(ref entries) = entries {
        IntArray::from_range(0, entries.len() as JobTaskCount)
    } else if let Some(ref array) = opts.array {
        array.clone()
    } else {
        IntArray::from_id(0)
    };

    Ok((ids, entries))
}

/// Warns the user that an array job might produce too many files.
fn warn_array_task_count(opts: &SubmitOpts, task_count: u32) {
    if task_count < 2 {
        return;
    }

    let is_path_some =
        |path: Option<&StdioArg>| -> bool { path.map_or(true, |x| !matches!(x.0, StdioDef::Null)) };

    let mut task_files = 0;
    let mut active_dirs = Vec::new();
    if is_path_some(opts.stdout.as_ref()) {
        task_files += task_count;
        active_dirs.push("stdout");
    }
    if is_path_some(opts.stderr.as_ref()) {
        task_files += task_count;
        active_dirs.push("stderr");
    }
    if task_files > SUBMIT_ARRAY_LIMIT && opts.log.is_none() {
        log::warn!(
            "The job will create {} files for{}. \
            Consider using the `--log` option to stream all outputs into a single file",
            task_files,
            active_dirs.join(" and ")
        );
    }
}

/// Warns the user that an array job does not contain task ID within stdout/stderr path.
fn warn_missing_task_id(opts: &SubmitOpts, task_count: u32) {
    let check_path = |path: Option<&StdioArg>, stream: &str| {
        let path = path.and_then(|stdio| match &stdio.0 {
            StdioDef::File(path) => path.to_str(),
            _ => None,
        });
        if let Some(path) = path {
            let placeholders = parse_resolvable_string(path);
            if !placeholders.contains(&StringPart::Placeholder(TASK_ID_PLACEHOLDER)) {
                log::warn!("You have submitted an array job, but the `{}` path does not contain the task ID placeholder.\n\
        Individual tasks might thus overwrite the file. Consider adding `%{{{}}}` to the `--{}` value.", stream, TASK_ID_PLACEHOLDER, stream);
            }
        }
    };

    if task_count > 1 {
        check_path(opts.stdout.as_ref(), "stdout");
        check_path(opts.stderr.as_ref(), "stderr");
    }
}

/// Returns an error if working directory contains the CWD placeholder.
fn check_valid_cwd(opts: &SubmitOpts) -> anyhow::Result<()> {
    let placeholders = parse_resolvable_string(opts.cwd.to_str().unwrap());
    if placeholders.contains(&StringPart::Placeholder(CWD_PLACEHOLDER)) {
        return Err(anyhow!(
            "Working directory path cannot contain the working directory placeholder `%{{{}}}`.",
            CWD_PLACEHOLDER
        ));
    }

    Ok(())
}

/// Warn user about suspicious submit parameters
fn check_suspicious_options(opts: &SubmitOpts, task_count: u32) -> anyhow::Result<()> {
    warn_array_task_count(opts, task_count);
    warn_missing_task_id(opts, task_count);
    check_valid_cwd(opts)?;
    Ok(())
}

#[derive(Parser)]
pub struct ResubmitOpts {
    job_id: u32,

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
        job_id: opts.job_id.into(),
        status: opts.status.map(|x| x.to_vec()),
    });
    let response = rpc_call!(connection, message, ToClientMessage::SubmitResponse(r) => r).await?;
    gsettings
        .printer()
        .print_job_detail(response.job, false, get_worker_map(connection).await?);
    Ok(())
}

fn validate_name(name: String) -> anyhow::Result<String> {
    match name {
        name if name.contains('\n') || name.contains('\t') => {
            Err(anyhow!("name cannot have a newline or a tab"))
        }
        name if name.len() > 40 => Err(anyhow!("name cannot be more than 40 characters")),
        name => Ok(name),
    }
}

// We need to read it as bytes, because not all our users use UTF-8
fn read_lines(filename: &Path) -> anyhow::Result<Vec<BString>> {
    log::info!("Reading file: {}", filename.display());
    if fs::metadata(filename)?.len() > 100 << 20 {
        log::warn!("Reading file bigger than 100MB");
    };
    let file = std::fs::File::open(filename)?;
    let results: Result<Vec<BString>, std::io::Error> = io::BufReader::new(file)
        .split(b'\n')
        .map(|x| x.map(BString::from))
        .collect();
    Ok(results?)
}

fn make_entries_from_json(filename: &Path) -> anyhow::Result<Vec<BString>> {
    log::info!("Reading json file: {}", filename.display());
    if fs::metadata(filename)?.len() > 100 << 20 {
        log::warn!("Reading file bigger then 100MB");
    };
    let file = std::fs::File::open(filename)?;
    let root = serde_json::from_reader(file)?;

    if let serde_json::Value::Array(values) = root {
        values
            .iter()
            .map(|element| {
                serde_json::to_string(element)
                    .map(BString::from)
                    .map_err(|e| e.into())
            })
            .collect()
    } else {
        anyhow::bail!(
            "{}: The top element of the provided JSON file has to be an array",
            filename.display()
        )
    }
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
