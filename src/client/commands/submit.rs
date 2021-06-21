use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs, io};

use anyhow::anyhow;
use bstr::BString;
use clap::Clap;
use hashbrown::HashMap;
use tako::common::resources::{CpuRequest, ResourceRequest};
use tako::messages::common::ProgramDefinition;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::print_job_detail;
use crate::client::resources::parse_cpu_request;
use crate::common::arraydef::ArrayDef;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobType, SubmitRequest, ToClientMessage};
use crate::{rpc_call, JobTaskCount};

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
struct OptionalPath(Option<PathBuf>);

impl FromStr for OptionalPath {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let path = match s {
            "none" => None,
            _ => Some(s.into()),
        };
        Ok(OptionalPath(path))
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

    /// Working directory for the submitted job
    /// The path must be accessible from a worker node
    #[clap(long, default_value("%{SUBMIT_DIR}"))]
    cwd: PathBuf,

    /// Path where the standard output of the job will be stored
    /// The path must be accessible from a worker node
    #[clap(long, default_value("stdout.%{JOB_ID}.%{TASK_ID}"))]
    stdout: OptionalPath,

    /// Path where the standard error of the job will be stored
    /// The path must be accessible from a worker node
    #[clap(long, default_value("stderr.%{JOB_ID}.%{TASK_ID}"))]
    stderr: OptionalPath,

    /// Specify additional environment variable for the job
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

    #[clap(long)]
    /// Create a task array where a task will be created for each number in the specified number range.
    /// Each task will be passed an environment variable `HQ_TASK_ID`.
    ///
    /// `--array=5` - create task array with one job with task ID 5
    ///
    /// `--array=3-5` - create task array with three jobs with task IDs 3, 4, 5
    array: Option<ArrayDef>,

    #[clap(long)]
    max_fails: Option<JobTaskCount>,
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

    let submit_cwd = opts.cwd;
    if !fs::metadata(&submit_cwd)
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        log::warn!(
            "{:?} is not a valid directory on the current node",
            submit_cwd
        )
    }

    let stdout = opts.stdout.0;
    let stderr = opts.stderr.0;

    let env_count = opts.env.len();
    let env: HashMap<_, _> = opts
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
            cwd: Some(submit_cwd),
        },
        resources,
        pin: opts.pin,
        entries,
        max_fails: opts.max_fails,
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
