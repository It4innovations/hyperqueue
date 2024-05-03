use std::collections::BTreeSet;
use std::io::{BufRead, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use std::{fs, io};

use anyhow::{anyhow, bail};
use bstr::BString;
use chumsky::primitive::{filter, just};
use chumsky::text::TextParser;
use chumsky::Parser as ChumskyParser;
use clap::{ArgMatches, Parser};
use smallvec::smallvec;
use tako::gateway::{
    ResourceRequest, ResourceRequestEntries, ResourceRequestEntry, ResourceRequestVariants,
};
use tako::program::{FileOnCloseBehavior, ProgramDefinition, StdioDef};
use tako::resources::{AllocationRequest, NumOfNodes, ResourceAmount, CPU_RESOURCE_NAME};

use super::directives::parse_hq_directives;
use crate::client::commands::submit::directives::parse_hq_directives_from_file;
use crate::client::commands::wait::{wait_for_jobs, wait_for_jobs_with_progress};
use crate::client::globalsettings::GlobalSettings;
use crate::client::resources::{parse_allocation_request, parse_resource_request};
use crate::common::arraydef::IntArray;
use crate::common::cli::OptsWithMatches;
use crate::common::parser2::{all_consuming, CharParser, ParseError};
use crate::common::placeholders::{
    get_unknown_placeholders, parse_resolvable_string, StringPart, CWD_PLACEHOLDER,
    JOB_ID_PLACEHOLDER, TASK_ID_PLACEHOLDER,
};
use crate::common::utils::fs::get_current_dir;
use crate::common::utils::str::pluralize;
use crate::common::utils::time::parse_human_time;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDescription, PinMode, SubmitRequest, TaskDescription,
    TaskKind, TaskKindProgram, ToClientMessage,
};
use crate::{rpc_call, JobTaskCount, Map};

const SUBMIT_ARRAY_LIMIT: JobTaskCount = 999;
pub const DEFAULT_CRASH_LIMIT: u32 = 5;

// Keep in sync with `tests/util/job.py::default_task_output` and `pyhq/python/hyperqueue/output.py`
pub const DEFAULT_STDOUT_PATH: &str = const_format::concatcp!(
    "%{",
    CWD_PLACEHOLDER,
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
pub const DEFAULT_STDERR_PATH: &str = const_format::concatcp!(
    "%{",
    CWD_PLACEHOLDER,
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

#[derive(Debug, Clone)]
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

#[derive(Clone, Debug, PartialEq)]
enum StdioDefInput {
    None,
    File {
        path: Option<PathBuf>,
        on_close: FileOnCloseBehavior,
    },
}

impl StdioDefInput {
    fn path(&self) -> Option<&Path> {
        match self {
            StdioDefInput::None => None,
            StdioDefInput::File { path, .. } => path.as_deref(),
        }
    }
}

impl FromStr for StdioDefInput {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(StdioDefInput::None),
            _ => Ok(parse_stdio_def(s).map_err(|error| format!("Cannot parse stream: {error:?}"))?),
        }
    }
}

fn stdio_def_parser() -> impl CharParser<StdioDefInput> {
    let path = filter(|&c| c != ':')
        .repeated()
        .at_least(1)
        .padded()
        .collect::<String>()
        .labelled("File path");
    let separator = just(':');
    let mode = just("rm-if-finished")
        .to(FileOnCloseBehavior::RmIfFinished)
        .labelled("rm-if-finished");

    let mode_complete = separator.ignore_then(mode);
    let path_complete = path.or_not().then(mode_complete.or_not());

    path_complete.try_map(|(path, mode), span| {
        if path.is_none() && mode.is_none() {
            return Err(ParseError::custom(span, "Input cannot be empty"));
        }

        Ok(StdioDefInput::File {
            path: path.map(PathBuf::from),
            on_close: mode.unwrap_or(FileOnCloseBehavior::None),
        })
    })
}

fn parse_stdio_def(input: &str) -> anyhow::Result<StdioDefInput> {
    all_consuming(stdio_def_parser()).parse_text(input)
}

#[derive(clap::ValueEnum, Clone)]
enum PinModeArg {
    #[value(name = "taskset")]
    TaskSet,
    #[value(name = "omp")]
    OpenMP,
}

impl From<PinModeArg> for PinMode {
    fn from(arg: PinModeArg) -> Self {
        match arg {
            PinModeArg::TaskSet => PinMode::TaskSet,
            PinModeArg::OpenMP => PinMode::OpenMP,
        }
    }
}

/* This is a special kind of parser because some arguments may be configured
 * through #HQ directives.
 *
 * When this is updated, also update the `overwrite` method!!!
*/
#[derive(Parser)]
pub struct SubmitJobConfOpts {
    /* Other resource configurations is not yet supported in combination of nodes,
      remove conflict_with as support is done
    */
    /// Number of nodes; 0
    #[arg(
        long,
        conflicts_with("pin"),
        conflicts_with("cpus"),
        default_value_t = 0
    )]
    nodes: NumOfNodes,

    /// Number and placement of CPUs for each job
    #[arg(long, value_parser = parse_allocation_request)]
    cpus: Option<AllocationRequest>,

    /// Generic resource request in the form <NAME>=<AMOUNT>
    #[arg(long, action = clap::ArgAction::Append, value_parser = parse_resource_request)]
    resource: Vec<(String, AllocationRequest)>,

    /// Minimal lifetime of the worker needed to start the job
    #[arg(long, value_parser = parse_human_time, default_value = "0ms")]
    time_request: Duration,

    /// Name of the job
    #[arg(long)]
    name: Option<String>,

    /// Pin the job to the cores specified in `--cpus`.
    #[arg(long, value_enum)]
    pin: Option<PinModeArg>,

    /// Working directory for the submitted job.
    /// The path must be accessible from worker nodes
    /// [default: %{SUBMIT_DIR}]
    #[arg(long)]
    cwd: Option<PathBuf>,

    /// Path where the standard output of the job will be stored.
    /// The path must be accessible from worker nodes
    #[arg(long)]
    stdout: Option<StdioDefInput>,

    /// Path where the standard error of the job will be stored.
    /// The path must be accessible from worker nodes
    #[arg(long)]
    stderr: Option<StdioDefInput>,

    /// Specify additional environment variable for the job.
    /// You can pass this flag multiple times to pass multiple variables
    ///
    /// `--env=KEY=VAL` - set an environment variable named `KEY` with the value `VAL`
    #[arg(long, action = clap::ArgAction::Append)]
    env: Vec<ArgEnvironmentVar>,

    // Parameters for creating array jobs
    /// Create a task array where a task will be created for each line of the given file.
    /// The corresponding line will be passed to the task in environment variable `HQ_ENTRY`.
    #[arg(long, value_hint = clap::ValueHint::FilePath)]
    each_line: Option<PathBuf>,

    /// Create a task array where a task will be created for each item of a JSON array stored in
    /// the given file.
    /// The corresponding item from the array will be passed as a JSON string to the task in
    /// environment variable `HQ_ENTRY`.
    #[arg(long, conflicts_with("each_line"), value_hint = clap::ValueHint::FilePath)]
    from_json: Option<PathBuf>,

    /// Create a task array where a task will be created for each number in the specified number range.
    /// Each task will be passed an environment variable `HQ_TASK_ID`.
    ///
    /// `--array=5` - create task array with one job with task ID 5
    ///
    /// `--array=3-5` - create task array with three jobs with task IDs 3, 4, 5
    #[arg(long)]
    array: Option<IntArray>,

    /// Maximum number of permitted task failures.
    /// If this limit is reached, the job will fail immediately.
    #[arg(long)]
    max_fails: Option<JobTaskCount>,

    /// Priority of each task
    #[arg(long, default_value_t = 0)]
    priority: tako::Priority,

    #[arg(long, value_parser = parse_human_time)]
    /// Time limit per task. E.g. --time-limit=10min
    time_limit: Option<Duration>,

    /// Stream the output of tasks into this log file.
    #[arg(long)]
    log: Option<PathBuf>,

    /// Create a temporary directory for task, path is provided in HQ_TASK_DIR
    /// The directory is automatically deleted when task is finished
    #[arg(long)]
    task_dir: bool,

    /// Limits how many times may task be in a running state while worker is lost.
    /// If the limit is reached, the task is marked as failed. If the limit is zero,
    /// the limit is disabled.
    #[arg(long, default_value_t = 5)]
    crash_limit: u32,
}

impl OptsWithMatches<SubmitJobConfOpts> {
    /// Overwrite options in `other` with values from `self`.
    pub fn overwrite(self, other: OptsWithMatches<SubmitJobConfOpts>) -> SubmitJobConfOpts {
        let (opts, self_matches) = self.into_inner();
        let (mut other_opts, other_matches) = other.into_inner();

        let mut env = opts.env;
        env.append(&mut other_opts.env);

        let mut resource = opts.resource;
        resource.append(&mut other_opts.resource);

        let (each_line, from_json, array) =
            if opts.each_line.is_some() || opts.from_json.is_some() || opts.array.is_some() {
                (opts.each_line, opts.from_json, opts.array)
            } else {
                (other_opts.each_line, other_opts.from_json, other_opts.array)
            };

        SubmitJobConfOpts {
            nodes: get_or_default(&self_matches, &other_matches, "nodes"),
            cpus: opts.cpus.or(other_opts.cpus),
            resource,
            time_request: get_or_default(&self_matches, &other_matches, "time_request"),
            name: opts.name.or(other_opts.name),
            pin: opts.pin.or(other_opts.pin),
            task_dir: opts.task_dir || other_opts.task_dir,
            cwd: opts.cwd.or(other_opts.cwd),
            stdout: opts.stdout.or(other_opts.stdout),
            stderr: opts.stderr.or(other_opts.stderr),
            env,
            each_line,
            from_json,
            array,
            max_fails: opts.max_fails.or(other_opts.max_fails),
            priority: get_or_default(&self_matches, &other_matches, "priority"),
            time_limit: opts.time_limit.or(other_opts.time_limit),
            log: opts.log.or(other_opts.log),
            crash_limit: get_or_default(&self_matches, &other_matches, "crash_limit"),
        }
    }
}

/// Returns true if the given parameter has been specified explicitly.
fn has_parameter(matches: &ArgMatches, id: &str) -> bool {
    if let Ok(true) = matches.try_contains_id(id) {
        matches!(
            matches.value_source(id),
            Some(clap::parser::ValueSource::CommandLine)
        )
    } else if let Some((_, subcmd)) = matches.subcommand() {
        has_parameter(subcmd, id)
    } else {
        false
    }
}

/// Get the value of the given parameter from the arg matches.
/// Searches recursively for values from subcommands.
fn get_arg_value<T: Clone + Send + Sync + 'static>(matches: &ArgMatches, id: &str) -> T {
    if let Ok(true) = matches.try_contains_id(id) {
        matches.get_one::<T>(id).expect("Missing argument").clone()
    } else if let Some((_, subcmd)) = matches.subcommand() {
        get_arg_value(subcmd, id)
    } else {
        panic!("Argument {id} was not found");
    }
}

/// Returns `self_value` if the given `id` has been specified explicitly or if it hasn't been
/// specified in `other`.
///
/// This method has to be used for arguments with a default value!
fn get_or_default<T: Clone + Send + Sync + 'static>(
    matches: &ArgMatches,
    other: &ArgMatches,
    id: &str,
) -> T {
    if has_parameter(matches, id) || !has_parameter(other, id) {
        get_arg_value(matches, id)
    } else {
        get_arg_value(other, id)
    }
}

#[derive(clap::ValueEnum, Clone, Eq, PartialEq)]
pub enum DirectivesMode {
    Auto,
    File,
    Stdin,
    Off,
}

#[derive(Parser)]
pub struct JobSubmitOpts {
    /// Command that should be executed by each task
    #[arg(required = true, trailing_var_arg(true))]
    commands: Vec<String>,

    #[clap(flatten)]
    conf: SubmitJobConfOpts,

    /// Wait for the job to finish.
    #[arg(long, conflicts_with("progress"))]
    wait: bool,

    /// Interactively observe the progress of the submitted job.
    #[arg(long, conflicts_with("wait"))]
    progress: bool,

    /// Capture stdin and start the task with the given stdin;
    /// the job will be submitted when the stdin is closed.
    #[arg(long)]
    stdin: bool,

    /// Select directives parsing mode.
    ///
    /// `auto`: Directives will be parsed if the suffix of the first command is ".sh".{n}
    /// `file`: Directives will be parsed regardless of the first command extension.{n}
    /// `stdin`: Directives will be parsed from standard input passed to `hq submit` instead
    ///  from the submitted command.{n}
    /// `off`: Directives will not be parsed.{n}
    ///
    /// If enabled, HQ will parse `#HQ` directives from a file located in the first entered command.
    /// Parameters following the `#HQ` prefix will be used as parameters for `hq submit`.
    ///
    /// Example (script.sh):{n}
    /// #!/bin/bash{n}
    /// #HQ --name my-job{n}
    /// #HQ --cpus=2{n}
    /// {n}
    /// program --foo=bar{n}
    #[arg(long, default_value_t = DirectivesMode::Auto, value_enum)]
    directives: DirectivesMode,
}

impl JobSubmitOpts {
    fn resource_request(&self) -> anyhow::Result<ResourceRequest> {
        let mut resources: ResourceRequestEntries = self
            .conf
            .resource
            .iter()
            .map(|r| {
                let r = r.clone();
                ResourceRequestEntry {
                    resource: r.0,
                    policy: r.1,
                }
            })
            .collect();

        let has_cpus = resources.iter().any(|r| r.resource == CPU_RESOURCE_NAME);

        if let Some(cpus) = &self.conf.cpus {
            if has_cpus {
                anyhow::bail!("--cpus and --resource cpus=... cannot be combined");
            }
            resources.insert(
                0,
                ResourceRequestEntry {
                    resource: CPU_RESOURCE_NAME.to_string(),
                    policy: cpus.clone(),
                },
            )
        } else if !has_cpus {
            resources.insert(
                0,
                ResourceRequestEntry {
                    resource: CPU_RESOURCE_NAME.to_string(),
                    policy: AllocationRequest::Compact(ResourceAmount::new_units(1)),
                },
            )
        }
        let request = ResourceRequest {
            n_nodes: self.conf.nodes,
            min_time: self.conf.time_request,
            resources,
        };
        request.validate()?;
        Ok(request)
    }
}

fn create_stdio(arg: Option<StdioDefInput>, log: &Option<PathBuf>, default: &str) -> StdioDef {
    match arg {
        Some(arg) => match arg {
            StdioDefInput::None => StdioDef::Null,
            StdioDefInput::File { path, on_close } => StdioDef::File {
                path: path.unwrap_or_else(|| PathBuf::from(default)),
                on_close,
            },
        },
        None => {
            if log.is_none() {
                StdioDef::File {
                    path: default.into(),
                    on_close: FileOnCloseBehavior::None,
                }
            } else {
                StdioDef::Pipe
            }
        }
    }
}

fn handle_directives(
    opts: OptsWithMatches<JobSubmitOpts>,
    stdin: Option<&[u8]>,
) -> anyhow::Result<JobSubmitOpts> {
    let parse_file = |command: &String| {
        parse_hq_directives_from_file(&PathBuf::from(command)).map_err(|error| {
            anyhow::anyhow!("Could not parse directives from {}: {:?}", command, error)
        })
    };

    let stdin_present = stdin.is_some();
    let command = opts.opts.commands[0].clone();

    let (mut opts, matches) = opts.into_inner();
    let conf = OptsWithMatches::new(opts.conf, matches);

    opts.conf = if let Some((parsed_opts, shebang)) = match opts.directives {
        DirectivesMode::Auto if command.ends_with(".sh") => Some(parse_file(&command)?),
        DirectivesMode::File => Some(parse_file(&command)?),
        DirectivesMode::Stdin => Some(parse_hq_directives(
            stdin.expect("Stdin directives is not present"),
        )?),
        DirectivesMode::Auto | DirectivesMode::Off => None,
    } {
        if let Some(shebang) = shebang {
            if !stdin_present {
                opts.commands = shebang.modify_commands(opts.commands);
            }
        }
        conf.overwrite(parsed_opts)
    } else {
        conf.into_inner().0
    };

    Ok(opts)
}

pub async fn submit_computation(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    opts: OptsWithMatches<JobSubmitOpts>,
) -> anyhow::Result<()> {
    let stdin = if opts.opts.stdin {
        let mut buf = Vec::new();
        println!("Reading data from stdin. In the interactive mode press Ctrl-D to submit.");
        std::io::stdin().lock().read_to_end(&mut buf)?;
        log::debug!("{} bytes read from stdin", buf.len());
        Some(buf)
    } else {
        None
    };

    if opts.opts.directives == DirectivesMode::Stdin && stdin.is_none() {
        bail!("You have to use `--stdin` when you specify `--directives=stdin`.");
    }

    let opts = handle_directives(opts, stdin.as_deref())?;

    let resources = opts.resource_request()?;
    let (ids, entries) = get_ids_and_entries(&opts)?;
    let task_count = ids.id_count();

    check_suspicious_options(&opts, task_count)?;

    let JobSubmitOpts {
        commands,
        wait,
        progress,
        stdin: _,
        directives: _,
        conf:
            SubmitJobConfOpts {
                nodes: _,
                cpus: _,
                resource: _,
                time_request: _,
                name,
                pin,
                task_dir,
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
                log,
                crash_limit,
            },
    } = opts;

    let name = if let Some(name) = name {
        validate_name(name)?
    } else {
        PathBuf::from(&commands[0])
            .file_name()
            .and_then(|t| t.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| "job".to_string())
    };

    let args: Vec<BString> = commands.into_iter().map(|arg| arg.into()).collect();

    let stdout = create_stdio(stdout, &log, DEFAULT_STDOUT_PATH);
    let stderr = create_stdio(stderr, &log, DEFAULT_STDERR_PATH);
    let cwd = cwd.unwrap_or_else(|| PathBuf::from("%{SUBMIT_DIR}"));

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
        stdin: stdin.unwrap_or_default(),
    };

    // Force task_dir for multi node tasks (for a place where to create node file)
    let task_dir = if resources.n_nodes > 0 {
        true
    } else {
        task_dir
    };

    let task_kind = TaskKind::ExternalProgram(TaskKindProgram {
        program: program_def,
        pin_mode: pin.map(|arg| arg.into()).unwrap_or(PinMode::None),
        task_dir,
    });
    let task_desc = TaskDescription {
        kind: task_kind,
        resources: ResourceRequestVariants::new(smallvec![resources]),
        priority,
        time_limit,
        crash_limit,
    };

    let job_desc = JobDescription::Array {
        ids,
        entries,
        task_desc,
    };

    let request = SubmitRequest {
        job_desc,
        name,
        max_fails,
        submit_dir: get_current_dir(),
        log,
    };

    send_submit_request(gsettings, session, request, wait, progress).await
}

pub(crate) async fn send_submit_request(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    request: SubmitRequest,
    wait: bool,
    progress: bool,
) -> anyhow::Result<()> {
    let message = FromClientMessage::Submit(request);

    let response =
        rpc_call!(session.connection(), message, ToClientMessage::SubmitResponse(r) => r).await?;
    let info = response.job.info.clone();

    gsettings.printer().print_job_submitted(response.job);
    if wait {
        wait_for_jobs(
            gsettings,
            session,
            IdSelector::Specific(IntArray::from_id(info.id.into())),
        )
        .await?;
    } else if progress {
        wait_for_jobs_with_progress(session, &[info]).await?;
    }
    Ok(())
}

fn get_ids_and_entries(opts: &JobSubmitOpts) -> anyhow::Result<(IntArray, Option<Vec<BString>>)> {
    let mut entries = if let Some(ref filename) = opts.conf.each_line {
        Some(read_lines(filename)?)
    } else if let Some(ref filename) = opts.conf.from_json {
        Some(make_entries_from_json(filename)?)
    } else {
        None
    };

    let ids = if let Some(array) = &opts.conf.array {
        if let Some(es) = entries {
            let id_set: BTreeSet<u32> = array
                .iter()
                .filter(|id| (*id as usize) < es.len())
                .collect();
            entries = Some(
                es.into_iter()
                    .enumerate()
                    .filter_map(|(id, value)| {
                        if id_set.contains(&(id as u32)) {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .collect(),
            );
            IntArray::from_sorted_ids(id_set.into_iter())
        } else {
            array.clone()
        }
    } else if let Some(ref entries) = entries {
        IntArray::from_range(0, entries.len() as JobTaskCount)
    } else {
        IntArray::from_id(0)
    };

    Ok((ids, entries))
}

/// Warns the user that an array job might produce too many files.
fn warn_array_task_count(opts: &JobSubmitOpts, task_count: u32) {
    if task_count < 2 {
        return;
    }

    let is_path_some =
        |path: Option<&StdioDefInput>| -> bool { path.map_or(true, |x| x.path().is_some()) };

    let mut task_files = 0;
    let mut active_dirs = Vec::new();
    if is_path_some(opts.conf.stdout.as_ref()) {
        task_files += task_count;
        active_dirs.push("stdout");
    }
    if is_path_some(opts.conf.stderr.as_ref()) {
        task_files += task_count;
        active_dirs.push("stderr");
    }
    if task_files > SUBMIT_ARRAY_LIMIT && opts.conf.log.is_none() {
        log::warn!(
            "The job will create {} files for {}. \
            Consider using the `--log` option to stream all outputs into a single file",
            task_files,
            active_dirs.join(" and ")
        );
    }
}

/// Warns the user that an array job does not contain task ID within stdout/stderr path.
fn warn_missing_task_id(opts: &JobSubmitOpts, task_count: u32) {
    let cwd_has_task_id = opts
        .conf
        .cwd
        .as_ref()
        .and_then(|p| p.to_str())
        .map(|p| parse_resolvable_string(p).contains(&StringPart::Placeholder(CWD_PLACEHOLDER)))
        .unwrap_or(false);

    let check_path = |path: Option<&StdioDefInput>, stream: &str| {
        let path = path.and_then(|stdio| {
            stdio
                .path()
                .map(|path| opts.conf.cwd.clone().unwrap_or_default().join(path))
        });
        if let Some(path) = path.as_ref().and_then(|p| p.to_str()) {
            let placeholders = parse_resolvable_string(path);
            // Either the path has to contain TASK_ID, or it has to contain CWD that itself contains
            // TASK_ID.
            let path_has_task_id =
                placeholders.contains(&StringPart::Placeholder(TASK_ID_PLACEHOLDER));
            let path_has_cwd = placeholders.contains(&StringPart::Placeholder(CWD_PLACEHOLDER));
            if !path_has_task_id && (!path_has_cwd || !cwd_has_task_id) {
                log::warn!("You have submitted an array job, but the `{}` path does not contain the task ID placeholder.\n\
        Individual tasks might thus overwrite the file. Consider adding `%{{{}}}` to the `--{}` value.", stream, TASK_ID_PLACEHOLDER, stream);
            }
        }
    };

    if task_count > 1 {
        check_path(opts.conf.stdout.as_ref(), "stdout");
        check_path(opts.conf.stderr.as_ref(), "stderr");
    }
}

/// Warns about unknown placeholders in various paths.
fn warn_unknown_placeholders(opts: &JobSubmitOpts) {
    let check = |path: Option<&Path>, context: &str| {
        if let Some(path) = path {
            let unknown = get_unknown_placeholders(path.to_str().unwrap());
            if !unknown.is_empty() {
                let placeholder_str = pluralize("placeholder", unknown.len());
                log::warn!(
                    "Found unknown {} `{}` in {}",
                    placeholder_str,
                    unknown.join(", "),
                    context
                );
            }
        }
    };

    check(
        opts.conf.stdout.as_ref().and_then(|arg| arg.path()),
        "stdout path",
    );
    check(
        opts.conf.stderr.as_ref().and_then(|arg| arg.path()),
        "stderr path",
    );
    check(opts.conf.log.as_deref(), "log path");
    check(opts.conf.cwd.as_deref(), "working directory path");
}

/// Returns an error if working directory contains the CWD placeholder.
fn check_valid_cwd(opts: &JobSubmitOpts) -> anyhow::Result<()> {
    if let Some(cwd) = &opts.conf.cwd {
        let placeholders = parse_resolvable_string(cwd.to_str().unwrap());
        if placeholders.contains(&StringPart::Placeholder(CWD_PLACEHOLDER)) {
            return Err(anyhow!(
                "Working directory path cannot contain the working directory placeholder `%{{{}}}`.",
                CWD_PLACEHOLDER
            ));
        }
    }
    Ok(())
}

/// Warn user about suspicious submit parameters
fn check_suspicious_options(opts: &JobSubmitOpts, task_count: u32) -> anyhow::Result<()> {
    warn_array_task_count(opts, task_count);
    warn_missing_task_id(opts, task_count);
    warn_unknown_placeholders(opts);
    check_valid_cwd(opts)?;
    Ok(())
}

fn validate_name(name: String) -> anyhow::Result<String> {
    match name {
        name if name.contains('\n') || name.contains('\t') => {
            Err(anyhow!("name cannot have a newline or a tab"))
        }
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
    use crate::client::commands::submit::command::{
        parse_stdio_def, stdio_def_parser, StdioDefInput,
    };
    use crate::common::parser2::all_consuming;
    use crate::tests::utils::expect_parser_error;
    use std::path::PathBuf;
    use std::str::FromStr;
    use tako::program::FileOnCloseBehavior;

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

    #[test]
    fn stdio_empty() {
        insta::assert_snapshot!(expect_parser_error(all_consuming(stdio_def_parser()), ""), @r###"
        Unexpected end of input found, expected something else:
        (the input was empty)
        "###);
    }

    #[test]
    fn stdio_just_path() {
        assert_eq!(
            parse_stdio_def("foo/bar/baz.txt").unwrap(),
            StdioDefInput::File {
                path: Some(PathBuf::from("foo/bar/baz.txt")),
                on_close: FileOnCloseBehavior::None
            }
        )
    }

    #[test]
    fn stdio_path_and_mode() {
        assert_eq!(
            parse_stdio_def("foo/bar/baz.txt:rm-if-finished").unwrap(),
            StdioDefInput::File {
                path: Some(PathBuf::from("foo/bar/baz.txt")),
                on_close: FileOnCloseBehavior::RmIfFinished
            }
        )
    }

    #[test]
    fn stdio_just_mode() {
        assert_eq!(
            parse_stdio_def(":rm-if-finished").unwrap(),
            StdioDefInput::File {
                path: None,
                on_close: FileOnCloseBehavior::RmIfFinished
            }
        )
    }

    #[test]
    fn stdio_missing_mode() {
        insta::assert_snapshot!(expect_parser_error(all_consuming(stdio_def_parser()), "foo:"), @r###"
        Unexpected end of input found while attempting to parse rm-if-finished, expected r:
          foo:
              |
              --- Unexpected end of input
        "###);
    }

    #[test]
    fn stdio_invalid_mode() {
        insta::assert_snapshot!(expect_parser_error(all_consuming(stdio_def_parser()), "foo:bar"), @r###"
        Unexpected token found while attempting to parse rm-if-finished, expected r:
          foo:bar
              |
              --- Unexpected token `b`
        "###);
    }

    #[test]
    fn stdio_only_colon() {
        insta::assert_snapshot!(expect_parser_error(all_consuming(stdio_def_parser()), ":"), @r###"
        Unexpected end of input found, expected something else:
          :
          |
          --- Input cannot be empty
        "###);
    }
}
