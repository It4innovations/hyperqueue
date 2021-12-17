use std::path::PathBuf;
use std::str::FromStr;

use clap::{Parser, ValueHint};
use cli_table::ColorChoice;

use hyperqueue::client::commands::autoalloc::{command_autoalloc, AutoAllocOpts};
use hyperqueue::client::commands::jobs::{cancel_job, output_job_detail, output_job_list};
use hyperqueue::client::commands::log::{command_log, LogOpts};
use hyperqueue::client::commands::stats::print_server_stats;
use hyperqueue::client::commands::stop::stop_server;
use hyperqueue::client::commands::submit::{
    resubmit_computation, submit_computation, ResubmitOpts, SubmitOpts,
};
use hyperqueue::client::commands::wait::{wait_for_jobs, wait_for_jobs_with_progress};
use hyperqueue::client::commands::worker::{
    get_worker_info, get_worker_list, start_hq_worker, stop_worker, WorkerStartOpts,
};
use hyperqueue::client::globalsettings::GlobalSettings;
use hyperqueue::client::output::cli::CliOutput;
use hyperqueue::client::output::json::JsonOutput;
use hyperqueue::client::output::outputs::{Output, Outputs};
use hyperqueue::client::output::quiet::Quiet;
use hyperqueue::client::status::Status;
use hyperqueue::common::arraydef::IntArray;
use hyperqueue::common::fsutils::absolute_path;
use hyperqueue::common::setup::setup_logging;
use hyperqueue::common::timeutils::ArgDuration;
use hyperqueue::dashboard::state::DashboardState;
use hyperqueue::dashboard::ui_loop::start_ui_loop;
use hyperqueue::server::bootstrap::{
    get_client_connection, init_hq_server, print_server_info, ServerConfig,
};
use hyperqueue::transfer::messages::{
    FromClientMessage, JobInfoRequest, Selector, ToClientMessage,
};
use hyperqueue::worker::hwdetect::{detect_cpus, detect_cpus_no_ht, detect_generic_resource};
use hyperqueue::WorkerId;
use tako::common::resources::ResourceDescriptor;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Common CLI options
#[derive(Parser)]
struct CommonOpts {
    /// Path to a directory that stores HyperQueue access files
    #[clap(long, global = true, value_hint = ValueHint::DirPath)]
    server_dir: Option<PathBuf>,

    /// Console color policy.
    #[clap(long, default_value = "auto", possible_values = &["auto", "always", "never"])]
    colors: ColorPolicy,

    /// Output selection
    #[clap(long, default_value = "cli", possible_values = &["cli", "json", "quiet"])]
    output_type: Outputs,
}

// Root CLI options
#[derive(Parser)]
#[clap(author, about, version(option_env!("HQ_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))))]
struct Opts {
    #[clap(flatten)]
    common: CommonOpts,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

///HyperQueue Dashboard
#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
#[clap(setting = clap::AppSettings::Hidden)]
struct DashboardOpts {}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
enum SubCommand {
    /// Commands for controlling the HyperQueue server
    Server(ServerOpts),
    /// Display information about all jobs
    Jobs(JobListOpts),
    /// Display detailed information about a specific job
    Job(JobDetailOpts),
    /// Submit a job to HyperQueue
    Submit(SubmitOpts),
    /// Cancel a specific job
    Cancel(CancelOpts),
    /// Commands for controlling HyperQueue workers
    Worker(WorkerOpts),
    /// Resubmits all filtered tasks within a job
    Resubmit(ResubmitOpts),
    /// Waits until a job is ended
    Wait(WaitOpts),
    /// Interactively observe the execution of a job
    Progress(ProgressOpts),
    /// Operations with log
    Log(LogOpts),
    /// Automatic allocation management
    #[clap(name = "alloc")]
    AutoAlloc(AutoAllocOpts),
    ///Commands for the dashboard
    Dashboard(DashboardOpts),
}

// Server CLI options
#[derive(Parser)]
struct ServerStartOpts {
    /// Hostname/IP of the machine under which is visible to others, default: hostname
    #[clap(long)]
    host: Option<String>,

    /// Duration after which will an idle worker automatically stop
    #[clap(long)]
    idle_timeout: Option<ArgDuration>,

    /// How often should the auto allocator perform its actions
    #[clap(long)]
    autoalloc_interval: Option<ArgDuration>,

    /// Port for client connections (used e.g. for `hq submit`)
    #[clap(long)]
    client_port: Option<u16>,

    /// Port for worker connections
    #[clap(long)]
    worker_port: Option<u16>,

    /// The maximum number of events tako server will store in memory
    #[clap(long, default_value = "1000000")]
    event_store_size: usize,
}

#[derive(Parser)]
struct ServerStopOpts {}

#[derive(Parser)]
struct ServerInfoOpts {
    /// Show internal internal state of server
    #[clap(long)]
    stats: bool,
}

#[derive(Parser)]
struct ServerOpts {
    #[clap(subcommand)]
    subcmd: ServerCommand,
}

#[derive(Parser)]
enum ServerCommand {
    /// Start the HyperQueue server
    Start(ServerStartOpts),
    /// Stop the HyperQueue server, if it is running
    Stop(ServerStopOpts),
    /// Show info of running HyperQueue server
    Info(ServerInfoOpts),
}

enum SelectorArg {
    All,
    Last,
    Id(IntArray),
}

impl FromStr for SelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(SelectorArg::Last),
            "all" => Ok(SelectorArg::All),
            _ => Ok(SelectorArg::Id(IntArray::from_str(s)?)),
        }
    }
}

impl From<SelectorArg> for Selector {
    fn from(selector_arg: SelectorArg) -> Self {
        match selector_arg {
            SelectorArg::Id(array) => Selector::Specific(array),
            SelectorArg::Last => Selector::LastN(1),
            SelectorArg::All => Selector::All,
        }
    }
}

#[derive(Parser)]
pub struct WaitOpts {
    /// Select job(s) to wait for
    selector_arg: SelectorArg,
}

#[derive(Parser)]
pub struct ProgressOpts {
    /// Select job(s) to observe
    selector_arg: SelectorArg,
}

// Worker CLI options
#[derive(Parser)]
struct WorkerStopOpts {
    /// Select worker(s) to stop
    selector_arg: SelectorArg,
}

#[derive(Parser)]
struct WorkerListOpts {
    /// Include offline workers in the list
    #[clap(long)]
    all: bool,
}

#[derive(Parser)]
struct WorkerAddressOpts {
    worker_id: WorkerId,
}

#[derive(Parser)]
struct WorkerOpts {
    #[clap(subcommand)]
    subcmd: WorkerCommand,
}

#[derive(Parser)]
struct WorkerInfoOpts {
    worker_id: WorkerId,
}

#[derive(Parser)]
struct HwDetectOpts {
    /// Detect only physical cores
    #[clap(long)]
    no_hyperthreading: bool,
}

#[derive(Parser)]
enum WorkerCommand {
    /// Start worker
    Start(WorkerStartOpts),
    /// Stop worker
    Stop(WorkerStopOpts),
    /// Display information about workers
    List(WorkerListOpts),
    /// Hwdetect
    #[clap(name = "hwdetect")]
    HwDetect(HwDetectOpts),
    /// Display information about a specific worker
    Info(WorkerInfoOpts),
    /// Display worker's hostname
    Address(WorkerAddressOpts),
}

// Job CLI options

#[derive(Parser)]
struct JobListOpts {
    job_filters: Vec<Status>,
}

#[derive(Parser)]
struct JobDetailOpts {
    /// Single ID, ID range or `last` to display the most recently submitted job
    selector_arg: SelectorArg,

    /// Include detailed task information in the output
    #[clap(long)]
    tasks: bool,
}

#[derive(Parser)]
struct CancelOpts {
    /// Select job(s) to cancel
    selector_arg: SelectorArg,
}

// Commands
async fn command_server_start(
    gsettings: &GlobalSettings,
    opts: ServerStartOpts,
) -> anyhow::Result<()> {
    let server_cfg = ServerConfig {
        host: opts
            .host
            .unwrap_or_else(|| gethostname::gethostname().into_string().unwrap()),
        idle_timeout: opts.idle_timeout.map(|x| x.unpack()),
        autoalloc_interval: opts.autoalloc_interval.map(|x| x.unpack()),
        client_port: opts.client_port,
        worker_port: opts.worker_port,
        event_store_size: opts.event_store_size,
    };

    init_hq_server(gsettings, server_cfg).await
}

async fn command_server_stop(
    gsettings: &GlobalSettings,
    _opts: ServerStopOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    stop_server(&mut connection).await?;
    Ok(())
}

async fn command_server_info(
    gsettings: &GlobalSettings,
    opts: ServerInfoOpts,
) -> anyhow::Result<()> {
    if opts.stats {
        let mut connection = get_client_connection(gsettings.server_directory()).await?;
        print_server_stats(gsettings, &mut connection).await
    } else {
        print_server_info(gsettings).await
    }
}

async fn command_job_list(gsettings: &GlobalSettings, opts: JobListOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    output_job_list(gsettings, &mut connection, opts.job_filters).await
}

async fn command_job_detail(gsettings: &GlobalSettings, opts: JobDetailOpts) -> anyhow::Result<()> {
    if matches!(opts.selector_arg, SelectorArg::All) {
        log::warn!("Job detail doesn't support the `all` selector, did you mean to use `hq jobs`?");
        return Ok(());
    }

    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    output_job_detail(
        gsettings,
        &mut connection,
        opts.selector_arg.into(),
        opts.tasks,
    )
    .await
}

async fn command_submit(gsettings: &GlobalSettings, opts: SubmitOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    submit_computation(gsettings, &mut connection, opts).await
}

async fn command_cancel(gsettings: &GlobalSettings, opts: CancelOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    cancel_job(gsettings, &mut connection, opts.selector_arg.into()).await
}

async fn command_worker_start(
    gsettings: &GlobalSettings,
    opts: WorkerStartOpts,
) -> anyhow::Result<()> {
    start_hq_worker(gsettings, opts).await
}

async fn command_worker_stop(
    gsettings: &GlobalSettings,
    opts: WorkerStopOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    stop_worker(&mut connection, opts.selector_arg.into()).await?;
    Ok(())
}

async fn command_worker_list(
    gsettings: &GlobalSettings,
    opts: WorkerListOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    let workers = get_worker_list(&mut connection, opts.all).await?;
    gsettings.printer().print_worker_list(workers);
    Ok(())
}

async fn command_worker_info(
    gsettings: &GlobalSettings,
    opts: WorkerInfoOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    let response = get_worker_info(&mut connection, opts.worker_id).await?;

    if let Some(worker) = response {
        gsettings.printer().print_worker_info(worker);
    } else {
        log::error!("Worker {} not found", opts.worker_id);
    }
    Ok(())
}

async fn command_resubmit(gsettings: &GlobalSettings, opts: ResubmitOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    resubmit_computation(gsettings, &mut connection, opts).await
}

fn command_worker_hwdetect(gsettings: &GlobalSettings, opts: HwDetectOpts) -> anyhow::Result<()> {
    let cpus = if opts.no_hyperthreading {
        detect_cpus_no_ht()?
    } else {
        detect_cpus()?
    };
    let generic = detect_generic_resource()?;
    gsettings
        .printer()
        .print_hw(&ResourceDescriptor::new(cpus, generic));
    Ok(())
}

async fn command_worker_address(
    gsettings: &GlobalSettings,
    opts: WorkerAddressOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    let response = get_worker_info(&mut connection, opts.worker_id).await?;

    match response {
        Some(info) => println!("{}", info.configuration.hostname),
        None => anyhow::bail!("Worker {} not found", opts.worker_id),
    }

    Ok(())
}

async fn command_wait(gsettings: &GlobalSettings, opts: WaitOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    wait_for_jobs(gsettings, &mut connection, opts.selector_arg.into()).await
}

async fn command_progress(gsettings: &GlobalSettings, opts: ProgressOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    let selector = opts.selector_arg.into();
    let response = hyperqueue::rpc_call!(
        connection,
        FromClientMessage::JobInfo(JobInfoRequest {
            selector,
        }),
        ToClientMessage::JobInfoResponse(r) => r
    )
    .await?;
    wait_for_jobs_with_progress(&mut connection, response.jobs).await
}

///Starts the hq Dashboard
async fn command_dashboard_start(
    gsettings: &GlobalSettings,
    _opts: DashboardOpts,
) -> anyhow::Result<()> {
    start_ui_loop(DashboardState::default(), gsettings).await?;
    Ok(())
}

pub enum ColorPolicy {
    Auto,
    Always,
    Never,
}

impl FromStr for ColorPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "auto" => Self::Auto,
            "always" => Self::Always,
            "never" => Self::Never,
            _ => anyhow::bail!("Invalid color policy"),
        })
    }
}

fn make_global_settings(opts: CommonOpts) -> GlobalSettings {
    fn default_server_directory_path() -> PathBuf {
        let mut home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
        home.push(".hq-server");
        home
    }

    let server_dir = absolute_path(
        opts.server_dir
            .unwrap_or_else(default_server_directory_path),
    );

    let color_policy = match opts.colors {
        ColorPolicy::Always => ColorChoice::AlwaysAnsi,
        ColorPolicy::Auto => {
            if atty::is(atty::Stream::Stdout) {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
        ColorPolicy::Never => ColorChoice::Never,
    };

    // Create Printer
    let printer: Box<dyn Output> = match opts.output_type {
        Outputs::CLI => {
            // Set colored settings for CLI
            match color_policy {
                ColorChoice::Always | ColorChoice::AlwaysAnsi => {
                    colored::control::set_override(true)
                }
                ColorChoice::Never => colored::control::set_override(false),
                _ => {}
            }

            Box::new(CliOutput::new(color_policy))
        }
        Outputs::JSON => Box::new(JsonOutput::default()),
        Outputs::Quiet => Box::new(Quiet::default()),
    };

    GlobalSettings::new(server_dir, printer)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    let top_opts: Opts = Opts::parse();
    setup_logging();

    let gsettings = make_global_settings(top_opts.common);

    let result = match top_opts.subcmd {
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Start(opts),
        }) => command_server_start(&gsettings, opts).await,
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Stop(opts),
        }) => command_server_stop(&gsettings, opts).await,
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Info(opts),
        }) => command_server_info(&gsettings, opts).await,

        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Start(opts),
        }) => command_worker_start(&gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Stop(opts),
        }) => command_worker_stop(&gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::List(opts),
        }) => command_worker_list(&gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Info(opts),
        }) => command_worker_info(&gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::HwDetect(opts),
        }) => command_worker_hwdetect(&gsettings, opts),
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Address(opts),
        }) => command_worker_address(&gsettings, opts).await,
        SubCommand::Jobs(opts) => command_job_list(&gsettings, opts).await,
        SubCommand::Job(opts) => command_job_detail(&gsettings, opts).await,
        SubCommand::Submit(opts) => command_submit(&gsettings, opts).await,
        SubCommand::Cancel(opts) => command_cancel(&gsettings, opts).await,
        SubCommand::Resubmit(opts) => command_resubmit(&gsettings, opts).await,
        SubCommand::Dashboard(opts) => command_dashboard_start(&gsettings, opts).await,
        SubCommand::Wait(opts) => command_wait(&gsettings, opts).await,
        SubCommand::Progress(opts) => command_progress(&gsettings, opts).await,
        SubCommand::Log(opts) => command_log(&gsettings, opts),
        SubCommand::AutoAlloc(opts) => command_autoalloc(&gsettings, opts).await,
    };

    if let Err(e) = result {
        gsettings.printer().print_error(e);
        std::process::exit(1);
    }

    Ok(())
}
