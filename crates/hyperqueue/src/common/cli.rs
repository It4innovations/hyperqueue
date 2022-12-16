use crate::client::commands::autoalloc::AutoAllocOpts;
use crate::client::commands::event::EventLogOpts;
use crate::client::commands::job::{JobCancelOpts, JobCatOpts, JobInfoOpts, JobListOpts};
use crate::client::commands::log::LogOpts;
use crate::client::commands::server::ServerOpts;
use crate::client::commands::submit::{JobResubmitOpts, JobSubmitOpts};
use crate::client::commands::worker::{WorkerFilter, WorkerStartOpts};
use crate::client::output::outputs::Outputs;
use crate::client::status::Status;
use crate::client::task::TaskOpts;
use crate::common::arraydef::IntArray;
use crate::transfer::messages::{
    IdSelector, SingleIdSelector, TaskIdSelector, TaskSelector, TaskStatusSelector,
};
use clap::Parser;
use clap_complete::Shell;
use std::path::PathBuf;
use std::str::FromStr;
use tako::WorkerId;

#[derive(Clone)]
pub enum IdSelectorArg {
    All,
    Last,
    Id(IntArray),
}

impl FromStr for IdSelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(IdSelectorArg::Last),
            "all" => Ok(IdSelectorArg::All),
            _ => Ok(IdSelectorArg::Id(IntArray::from_str(s)?)),
        }
    }
}

impl From<IdSelectorArg> for IdSelector {
    fn from(id_selector_arg: IdSelectorArg) -> Self {
        match id_selector_arg {
            IdSelectorArg::Id(array) => IdSelector::Specific(array),
            IdSelectorArg::Last => IdSelector::LastN(1),
            IdSelectorArg::All => IdSelector::All,
        }
    }
}

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

#[derive(Clone)]
pub enum JobSelectorArg {
    Last,
    Id(IntArray),
}

impl FromStr for JobSelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(JobSelectorArg::Last),
            _ => Ok(JobSelectorArg::Id(IntArray::from_str(s)?)),
        }
    }
}

pub fn get_id_selector(job_selector_arg: JobSelectorArg) -> IdSelector {
    match job_selector_arg {
        JobSelectorArg::Last => IdSelector::LastN(1),
        JobSelectorArg::Id(ids) => IdSelector::Specific(ids),
    }
}

#[derive(Clone)]
pub enum SingleIdSelectorArg {
    Last,
    Id(u32),
}

impl FromStr for SingleIdSelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(SingleIdSelectorArg::Last),
            _ => Ok(SingleIdSelectorArg::Id(u32::from_str(s)?)),
        }
    }
}

impl From<SingleIdSelectorArg> for SingleIdSelector {
    fn from(id_selector_arg: SingleIdSelectorArg) -> Self {
        match id_selector_arg {
            SingleIdSelectorArg::Last => SingleIdSelector::Last,
            SingleIdSelectorArg::Id(id) => SingleIdSelector::Specific(id),
        }
    }
}

#[derive(clap::ValueEnum, Clone)]
pub enum ColorPolicy {
    Auto,
    Always,
    Never,
}

// Common CLI options
#[derive(Parser)]
pub struct CommonOpts {
    /// Path to a directory that stores HyperQueue access files
    #[arg(
    long,
    value_hint = clap::ValueHint::DirPath,
    global = true,
    env = "HQ_SERVER_DIR",
    help_heading("GLOBAL OPTIONS"),
    hide_short_help(true)
    )]
    pub server_dir: Option<PathBuf>,

    /// Console color policy.
    #[arg(
        long,
        default_value = "auto",
        value_enum,
        global = true,
        help_heading("GLOBAL OPTIONS"),
        hide_short_help(true)
    )]
    pub colors: ColorPolicy,

    /// How should the output of the command be formatted.
    #[arg(
        long,
        env = "HQ_OUTPUT_MODE",
        default_value = "cli",
        value_enum,
        global = true,
        help_heading("GLOBAL OPTIONS"),
        hide_short_help(true)
    )]
    pub output_mode: Outputs,

    /// Turn on a more detailed log output
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
    version(option_env!("HQ_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))),
    disable_help_subcommand(true),
    help_expected(true)
)]
pub struct RootOptions {
    #[clap(flatten)]
    pub common: CommonOpts,

    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

/// HyperQueue Dashboard
#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
pub struct DashboardOpts {}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
pub enum SubCommand {
    /// Commands for controlling the HyperQueue server
    Server(ServerOpts),
    /// Commands for controlling HyperQueue jobs
    Job(JobOpts),
    /// Commands for displaying task(s)
    Task(TaskOpts),
    /// Submit a job to HyperQueue
    Submit(JobSubmitOpts),
    /// Commands for controlling HyperQueue workers
    Worker(WorkerOpts),
    /// Operations with log
    Log(LogOpts),
    /// Automatic allocation management
    #[command(name = "alloc")]
    AutoAlloc(AutoAllocOpts),
    /// Event log management
    EventLog(EventLogOpts),
    /// Commands for the dashboard
    Dashboard(DashboardOpts),
    /// Generate shell completion script
    GenerateCompletion(GenerateCompletionOpts),
}

// Worker CLI options

#[derive(Parser)]
pub struct WorkerOpts {
    #[clap(subcommand)]
    pub subcmd: WorkerCommand,
}

#[derive(Parser)]
pub enum WorkerCommand {
    /// Start worker
    Start(WorkerStartOpts),
    /// Stop worker
    Stop(WorkerStopOpts),
    /// Display information about workers.
    /// By default, only running workers will be displayed.
    List(WorkerListOpts),
    /// Hwdetect
    #[command(name = "hwdetect")]
    HwDetect(HwDetectOpts),
    /// Display information about a specific worker
    Info(WorkerInfoOpts),
    /// Display worker's hostname
    Address(WorkerAddressOpts),
    /// Waits on the connection of worker(s)
    Wait(WorkerWaitOpts),
}

#[derive(Parser)]
pub struct WorkerStopOpts {
    /// Select worker(s) to stop
    pub selector_arg: IdSelectorArg,
}

#[derive(Parser)]
pub struct WorkerListOpts {
    /// Display all workers.
    #[arg(long, conflicts_with("filter"))]
    pub all: bool,

    /// Select only workers with the given state.
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
    /// Number of worker(s) to wait on
    pub worker_count: u32,
}

#[derive(Parser)]
pub struct HwDetectOpts {
    /// Detect only physical cores
    #[arg(long)]
    pub no_hyper_threading: bool,
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
    /// Display information about jobs.
    /// By default, only queued or running jobs will be displayed.
    List(JobListOpts),
    /// Display detailed information of the selected job
    Info(JobInfoOpts),
    /// Cancel a specific job
    Cancel(JobCancelOpts),
    /// Shows task(s) streams(stdout, stderr) of a specific job
    Cat(JobCatOpts),
    /// Submit a job to HyperQueue
    Submit(JobSubmitOpts),
    /// Resubmits tasks of a job
    Resubmit(JobResubmitOpts),
    /// Waits until a job is finished
    Wait(JobWaitOpts),
    /// Interactively observe the execution of a job
    Progress(JobProgressOpts),
}

#[derive(Parser)]
pub struct JobWaitOpts {
    /// Select job(s) to wait for
    pub selector_arg: IdSelectorArg,
}

#[derive(Parser)]
pub struct JobProgressOpts {
    /// Select job(s) to observe
    pub selector_arg: IdSelectorArg,
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
