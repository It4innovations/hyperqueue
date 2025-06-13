use std::path::PathBuf;
use std::str::FromStr;

use clap::{ArgMatches, Parser};
use clap_complete::Shell;

use tako::WorkerId;

use crate::client::commands::autoalloc::AutoAllocOpts;
use crate::client::commands::data::DataOpts;
use crate::client::commands::doc::DocOpts;
use crate::client::commands::job::{
    JobCancelOpts, JobCatOpts, JobCloseOpts, JobForgetOpts, JobInfoOpts, JobListOpts,
    JobTaskIdsOpts,
};
use crate::client::commands::journal::JournalOpts;
use crate::client::commands::outputlog::OutputLogOpts;
use crate::client::commands::server::ServerOpts;
use crate::client::commands::submit::command::SubmitJobConfOpts;
use crate::client::commands::submit::{JobSubmitFileOpts, JobSubmitOpts};
use crate::client::commands::worker::{WorkerFilter, WorkerStartOpts};
use crate::client::output::outputs::Outputs;
use crate::client::status::Status;
use crate::client::task::TaskOpts;
use crate::common::arraydef::IntArray;
use crate::transfer::messages::{
    IdSelector, SingleIdSelector, TaskIdSelector, TaskSelector, TaskStatusSelector,
};

#[derive(Parser)]
pub struct TaskSelectorArg {
    /// Filter task(s) by ID.
    #[arg(long)]
    pub tasks: Option<IntArray>,

    /// Filter task(s) by status.
    /// You can use multiple states separated by a comma.
    #[arg(long, value_delimiter(','), value_enum)]
    pub task_status: Vec<Status>,
}

pub fn get_task_selector(opt_task_selector_arg: Option<TaskSelectorArg>) -> Option<TaskSelector> {
    opt_task_selector_arg.map(|arg| TaskSelector {
        id_selector: get_task_id_selector(arg.tasks),
        status_selector: get_task_status_selector(arg.task_status),
    })
}

pub fn get_task_id_selector(id_selector_arg: Option<IntArray>) -> TaskIdSelector {
    id_selector_arg
        .map(TaskIdSelector::Specific)
        .unwrap_or(TaskIdSelector::All)
}

fn get_task_status_selector(status_selector_arg: Vec<Status>) -> TaskStatusSelector {
    if status_selector_arg.is_empty() {
        TaskStatusSelector::All
    } else {
        TaskStatusSelector::Specific(status_selector_arg)
    }
}

/// Selects items with the following options:
/// - "last" => only the last item
/// - "all" => all items
/// - "<integer-range>" => see [`crate::common::arraydef::parse_array`]
pub fn parse_last_all_range(value: &str) -> anyhow::Result<IdSelector> {
    match value {
        "last" => Ok(IdSelector::LastN(1)),
        "all" => Ok(IdSelector::All),
        _ => Ok(IdSelector::Specific(IntArray::from_str(value)?)),
    }
}

/// Selects items with the following options:
/// - "last" => only the last item
/// - "<integer-range>" => see [`crate::common::arraydef::parse_array`]
pub fn parse_last_range(value: &str) -> anyhow::Result<IdSelector> {
    match value {
        "last" => Ok(IdSelector::LastN(1)),
        _ => Ok(IdSelector::Specific(IntArray::from_str(value)?)),
    }
}

/// Selects an item with the following options:
/// - "last" => only the last item
/// - "<id>" => item with the specified ID
pub fn parse_last_single_id(value: &str) -> anyhow::Result<SingleIdSelector> {
    match value {
        "last" => Ok(SingleIdSelector::Last),
        _ => Ok(SingleIdSelector::Specific(u32::from_str(value)?)),
    }
}

pub struct OptsWithMatches<Opts> {
    pub opts: Opts,
    pub matches: ArgMatches,
}

impl<Opts> OptsWithMatches<Opts> {
    pub fn new(opts: Opts, matches: ArgMatches) -> Self {
        Self { opts, matches }
    }

    pub fn into_inner(self) -> (Opts, ArgMatches) {
        (self.opts, self.matches)
    }
}

#[derive(clap::ValueEnum, Clone)]
pub enum ColorPolicy {
    /// Use colors if the stdout is detected to be a terminal.
    Auto,
    /// Always use colors.
    Always,
    /// Never use colors.
    Never,
}

// Common CLI options
#[derive(Parser)]
pub struct CommonOpts {
    /// The path where access files are stored
    #[arg(
    long,
    value_hint = clap::ValueHint::DirPath,
    global = true,
    env = "HQ_SERVER_DIR",
    help_heading("GLOBAL OPTIONS"),
    hide_short_help(true)
    )]
    pub server_dir: Option<PathBuf>,

    /// Sets console color policy
    #[arg(
        long,
        default_value_t = ColorPolicy::Auto,
        value_enum,
        global = true,
        help_heading("GLOBAL OPTIONS"),
        hide_short_help(true)
    )]
    pub colors: ColorPolicy,

    /// Sets output formatting
    #[arg(
        long,
        env = "HQ_OUTPUT_MODE",
        default_value_t = Outputs::CLI,
        value_enum,
        global = true,
        help_heading("GLOBAL OPTIONS"),
        hide_short_help(true)
    )]
    pub output_mode: Outputs,

    /// Enables more detailed log output
    #[arg(
        long,
        env = "HQ_DEBUG",
        global = true,
        help_heading("GLOBAL OPTIONS"),
        hide_short_help(true)
    )]
    pub debug: bool,
}

// Root CLI options
#[derive(Parser)]
#[command(
    author,
    about,
    version(crate::HQ_VERSION),
    disable_help_subcommand(true),
    help_expected(true)
)]
pub struct RootOptions {
    #[clap(flatten)]
    pub common: CommonOpts,

    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
pub enum SubCommand {
    /// Commands for the server
    Server(ServerOpts),
    /// Commands for jobs
    Job(JobOpts),
    /// Commands for tasks
    Task(TaskOpts),
    /// Submit a new job
    Submit(JobSubmitOpts),
    /// Commands for workers
    Worker(WorkerOpts),
    /// Operations for streaming logs
    OutputLog(OutputLogOpts),
    /// Automatic allocation management
    #[command(name = "alloc")]
    AutoAlloc(AutoAllocOpts),
    /// Journal management
    Journal(JournalOpts),
    /// Data object management inside a task
    Data(DataOpts),
    /// Starts CLI dashboard
    #[cfg(feature = "dashboard")]
    Dashboard(DashboardOpts),
    /// Show documentation.
    Doc(DocOpts),
    /// Generate shell completion script
    GenerateCompletion(GenerateCompletionOpts),
}

/// Dashboard
#[derive(Parser)]
pub struct DashboardOpts {
    #[clap(subcommand)]
    pub subcmd: Option<DashboardCommand>,
}

#[derive(Parser, Default)]
pub enum DashboardCommand {
    /// Stream events from a server
    ///
    /// Note: It will replay all events from the currently active
    /// journal file before new events are streamed.
    #[default]
    Stream,
    /// Replay events from a journal file
    Replay {
        /// Path to a journal file
        ///
        /// New journal file can be created by `hq server start --journal=<path>`.
        journal: PathBuf,
    },
}

// Worker CLI options

#[derive(Parser)]
pub struct WorkerOpts {
    #[clap(subcommand)]
    pub subcmd: WorkerCommand,
}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
pub enum WorkerCommand {
    /// Start a worker
    Start(WorkerStartOpts),
    /// Stop a worker
    Stop(WorkerStopOpts),
    /// Display information about workers
    ///
    /// By default, only running workers will be displayed.
    List(WorkerListOpts),
    /// Perform hardware detection
    ///
    /// This serves to test how a worker sees the hardware
    /// on a machine where the command is invoked.
    #[command(name = "hwdetect")]
    HwDetect(HwDetectOpts),
    /// Display information about a worker
    Info(WorkerInfoOpts),
    /// Display worker's hostname
    Address(WorkerAddressOpts),
    /// Waits on the connection of worker(s)
    Wait(WorkerWaitOpts),
    /// Deploy a set of workers using SSH.
    ///
    /// Note that to use this command, an OpenSSH-compatible `ssh` binary
    /// has to be available on the local node.
    DeploySsh(DeploySshOpts),
}

#[derive(Parser)]
pub struct WorkerStopOpts {
    /// Selects worker(s) to stop
    #[arg(value_parser = parse_last_all_range)]
    pub selector_arg: IdSelector,
}

#[derive(Parser)]
pub struct WorkerListOpts {
    /// Display all workers
    #[arg(long, conflicts_with("filter"))]
    pub all: bool,

    /// Select only workers in the given state
    #[arg(long, value_enum)]
    pub filter: Option<WorkerFilter>,
}

#[derive(Parser)]
pub struct WorkerAddressOpts {
    /// Worker ID
    pub worker_id: WorkerId,
}

#[derive(Parser)]
pub struct WorkerInfoOpts {
    /// Worker ID
    pub worker_id: WorkerId,
}

#[derive(Parser)]
pub struct WorkerWaitOpts {
    /// The number of worker(s) to wait on
    pub worker_count: u32,
}

#[derive(Parser)]
pub struct HwDetectOpts {
    /// Detect only physical cores
    #[arg(long)]
    pub no_hyper_threading: bool,
}

#[derive(Parser, Debug)]
pub struct DeploySshOpts {
    /// Show log output of the spawned worker(s)
    #[arg(long)]
    pub show_output: bool,

    /// Path to a file with target hostnames
    ///
    /// Path to a file that contains the hostnames to which should
    /// workers be deployed to.
    /// Each line in the file should correspond to one hostname address.
    pub hostfile: PathBuf,
    // This is essentially [WorkerStartOpts], but we parse it in an opaque way
    // so that we can forward it verbatim to the SSH command. See `deploy_ssh_workers`
    // for more information
    /// Arguments passed to `worker start` at the remote node.
    ///
    /// To display possible options, use `deploy-ssh -- --help`.
    #[arg(trailing_var_arg(true), allow_hyphen_values = true)]
    pub worker_start_args: Vec<String>,
}

// Job CLI options

#[derive(Parser)]
pub struct JobOpts {
    #[clap(subcommand)]
    pub subcmd: JobCommand,
}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
pub enum JobCommand {
    /// Display information about jobs
    ///
    /// By default, only queued or running jobs will be displayed.
    List(JobListOpts),
    /// Display a summary with the number of jobs.
    Summary,
    /// Display detailed information of a job
    Info(JobInfoOpts),
    /// Cancel a job
    ///
    /// This will cancel all job's tasks, stopping them from being computed.
    Cancel(JobCancelOpts),
    /// Forget a job
    ///
    /// This will remove the job from the server's memory, forgetting it completely and reducing
    /// the server's memory usage.
    ///
    /// You can only forget jobs that have been completed, i.e. that are not running or waiting to
    /// be executed. If they are not completed, you have to cancel these jobs first.
    Forget(JobForgetOpts),
    /// Shows task(s) stdout and stderr
    Cat(JobCatOpts),
    /// Submit a new job
    ///
    /// This is the same as `hq submit`
    Submit(JobSubmitOpts),
    /// Submit a job by a job definition file
    SubmitFile(JobSubmitFileOpts),
    /// Waits until a job is finished
    Wait(JobWaitOpts),
    /// Shows a progressbar with tasks/jobs
    Progress(JobProgressOpts),
    /// Prints task ids for a job
    TaskIds(JobTaskIdsOpts),
    /// Open a new job (without attaching any tasks yet)
    Open(SubmitJobConfOpts),
    /// Close an open job
    Close(JobCloseOpts),
}

#[derive(Parser)]
pub struct JobWaitOpts {
    /// Job(s) to wait for
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,

    /// Waits until all tasks are completed, even if the job is still open
    #[clap(long, action)]
    pub without_close: bool,
}

#[derive(Parser)]
pub struct JobProgressOpts {
    /// Job(s) to observe
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,
}

#[derive(Parser)]
pub struct GenerateCompletionOpts {
    /// Shell flavour for which the completion script should be generated
    #[arg(value_enum)]
    pub shell: Shell,
}

#[cfg(test)]
mod tests {
    use crate::common::cli::RootOptions;

    #[test]
    fn verify_root_cli() {
        use clap::CommandFactory;
        RootOptions::command().debug_assert()
    }
}
