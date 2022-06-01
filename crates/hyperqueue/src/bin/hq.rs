use std::io;
use std::path::PathBuf;

use clap::{IntoApp, Parser, ValueHint};
use clap_complete::{generate, Shell};
use cli_table::ColorChoice;

use hyperqueue::client::commands::autoalloc::{command_autoalloc, AutoAllocOpts};
use hyperqueue::client::commands::event::{command_event_log, EventLogOpts};
use hyperqueue::client::commands::job::{
    cancel_job, output_job_cat, output_job_detail, output_job_list, JobCancelOpts, JobCatOpts,
    JobInfoOpts, JobListOpts,
};
use hyperqueue::client::commands::log::{command_log, LogOpts};
use hyperqueue::client::commands::server::{command_server, ServerOpts};
use hyperqueue::client::commands::submit::{
    resubmit_computation, submit_computation, JobResubmitOpts, JobSubmitOpts,
};
use hyperqueue::client::commands::wait::{wait_for_jobs, wait_for_jobs_with_progress};
use hyperqueue::client::commands::worker::WorkerFilter;
use hyperqueue::client::commands::worker::{
    get_worker_info, get_worker_list, start_hq_worker, stop_worker, WorkerStartOpts,
};
use hyperqueue::client::default_server_directory_path;
use hyperqueue::client::globalsettings::GlobalSettings;
use hyperqueue::client::output::cli::CliOutput;
use hyperqueue::client::output::json::JsonOutput;
use hyperqueue::client::output::outputs::{Output, Outputs};
use hyperqueue::client::output::quiet::Quiet;
use hyperqueue::client::status::Status;
use hyperqueue::client::task::{output_job_tasks, TaskCommand, TaskListOpts, TaskOpts};
use hyperqueue::common::cli::{get_id_selector, get_task_selector, IdSelectorArg};
use hyperqueue::common::setup::setup_logging;
use hyperqueue::common::utils::fs::absolute_path;
use hyperqueue::dashboard::ui_loop::start_ui_loop;
use hyperqueue::server::bootstrap::get_client_session;
use hyperqueue::transfer::messages::{FromClientMessage, JobInfoRequest, ToClientMessage};
use hyperqueue::worker::hwdetect::{detect_cpus, detect_cpus_no_ht, detect_generic_resources};
use hyperqueue::WorkerId;
use tako::resources::ResourceDescriptor;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// Common CLI options
#[derive(Parser)]
struct CommonOpts {
    /// Path to a directory that stores HyperQueue access files
    #[clap(long, value_hint = ValueHint::DirPath)]
    #[clap(
        global = true,
        env = "HQ_SERVER_DIR",
        help_heading("GLOBAL OPTIONS"),
        hide_short_help(true)
    )]
    server_dir: Option<PathBuf>,

    /// Console color policy.
    #[clap(long, default_value = "auto", arg_enum)]
    #[clap(global = true, help_heading("GLOBAL OPTIONS"), hide_short_help(true))]
    colors: ColorPolicy,

    /// How should the output of the command be formatted.
    #[clap(long, env = "HQ_OUTPUT_MODE", default_value = "cli", arg_enum)]
    #[clap(global = true, help_heading("GLOBAL OPTIONS"), hide_short_help(true))]
    output_mode: Outputs,

    /// Turn on a more detailed log output
    #[clap(long, env = "HQ_DEBUG")]
    #[clap(global = true, help_heading("GLOBAL OPTIONS"), hide_short_help(true))]
    debug: bool,
}

// Root CLI options
#[derive(Parser)]
#[clap(author, about, version(option_env!("HQ_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))))]
#[clap(disable_help_subcommand(true))]
#[clap(help_expected(true))]
struct Opts {
    #[clap(flatten)]
    common: CommonOpts,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

/// HyperQueue Dashboard
#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
struct DashboardOpts {}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
enum SubCommand {
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
    #[clap(name = "alloc")]
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
struct WorkerOpts {
    #[clap(subcommand)]
    subcmd: WorkerCommand,
}

#[derive(Parser)]
enum WorkerCommand {
    /// Start worker
    Start(WorkerStartOpts),
    /// Stop worker
    Stop(WorkerStopOpts),
    /// Display information about workers.
    /// By default, only running workers will be displayed.
    List(WorkerListOpts),
    /// Hwdetect
    #[clap(name = "hwdetect")]
    HwDetect(HwDetectOpts),
    /// Display information about a specific worker
    Info(WorkerInfoOpts),
    /// Display worker's hostname
    Address(WorkerAddressOpts),
}

#[derive(Parser)]
struct WorkerStopOpts {
    /// Select worker(s) to stop
    selector_arg: IdSelectorArg,
}

#[derive(Parser)]
struct WorkerListOpts {
    /// Display all workers.
    #[clap(long, conflicts_with("filter"))]
    all: bool,

    /// Select only workers with the given state.
    #[clap(long, arg_enum)]
    filter: Option<WorkerFilter>,
}

#[derive(Parser)]
struct WorkerAddressOpts {
    /// Worker ID
    worker_id: WorkerId,
}

#[derive(Parser)]
struct WorkerInfoOpts {
    /// Worker ID
    worker_id: WorkerId,
}

#[derive(Parser)]
struct HwDetectOpts {
    /// Detect only physical cores
    #[clap(long)]
    no_hyperthreading: bool,
}

// Job CLI options

#[derive(Parser)]
struct JobOpts {
    #[clap(subcommand)]
    subcmd: JobCommand,
}

#[allow(clippy::large_enum_variant)]
#[derive(Parser)]
enum JobCommand {
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
    selector_arg: IdSelectorArg,
}

#[derive(Parser)]
pub struct JobProgressOpts {
    /// Select job(s) to observe
    selector_arg: IdSelectorArg,
}

#[derive(Parser)]
struct GenerateCompletionOpts {
    /// Shell flavour for which the completion script should be generated
    #[clap(possible_values = Shell::possible_values().collect::<Vec<_>>())]
    shell: Shell,
}

// Commands

async fn command_submit(gsettings: &GlobalSettings, opts: JobSubmitOpts) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    submit_computation(gsettings, &mut session, opts).await
}

async fn command_job_list(gsettings: &GlobalSettings, opts: JobListOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;

    let filter = if opts.filter.is_empty() {
        if opts.all {
            vec![]
        } else {
            vec![Status::Waiting, Status::Running]
        }
    } else {
        opts.filter
    };

    output_job_list(gsettings, &mut connection, filter).await
}

async fn command_job_detail(gsettings: &GlobalSettings, opts: JobInfoOpts) -> anyhow::Result<()> {
    if matches!(opts.selector_arg, IdSelectorArg::All) {
        log::warn!(
            "Job detail doesn't support the `all` selector, did you mean to use `hq job list --all`?"
        );
        return Ok(());
    }

    let mut session = get_client_session(gsettings.server_directory()).await?;
    output_job_detail(gsettings, &mut session, opts.selector_arg.into()).await
}

async fn command_job_cat(gsettings: &GlobalSettings, opts: JobCatOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    output_job_cat(
        gsettings,
        &mut connection,
        opts.job_selector,
        get_task_selector(Some(opts.task_selector)),
        opts.stream,
        opts.print_task_header,
    )
    .await
}

async fn command_job_cancel(gsettings: &GlobalSettings, opts: JobCancelOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    cancel_job(gsettings, &mut connection, opts.selector_arg.into()).await
}

async fn command_job_resubmit(
    gsettings: &GlobalSettings,
    opts: JobResubmitOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    resubmit_computation(gsettings, &mut connection, opts).await
}

async fn command_job_wait(gsettings: &GlobalSettings, opts: JobWaitOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    wait_for_jobs(gsettings, &mut connection, opts.selector_arg.into()).await
}

async fn command_job_progress(
    gsettings: &GlobalSettings,
    opts: JobProgressOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    let selector = opts.selector_arg.into();
    let response = hyperqueue::rpc_call!(
        session.connection(),
        FromClientMessage::JobInfo(JobInfoRequest {
            selector,
        }),
        ToClientMessage::JobInfoResponse(r) => r
    )
    .await?;
    wait_for_jobs_with_progress(&mut session, response.jobs).await
}

async fn command_task_list(gsettings: &GlobalSettings, opts: TaskListOpts) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    output_job_tasks(
        gsettings,
        &mut session,
        get_id_selector(opts.job_selector),
        get_task_selector(Some(opts.task_selector)),
    )
    .await
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
    let mut session = get_client_session(gsettings.server_directory()).await?;
    stop_worker(&mut session, opts.selector_arg.into()).await?;
    Ok(())
}

async fn command_worker_list(
    gsettings: &GlobalSettings,
    opts: WorkerListOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;

    let filter = opts.filter.or({
        if opts.all {
            None
        } else {
            Some(WorkerFilter::Running)
        }
    });

    let workers = get_worker_list(&mut session, filter).await?;
    gsettings.printer().print_worker_list(workers);
    Ok(())
}

async fn command_worker_info(
    gsettings: &GlobalSettings,
    opts: WorkerInfoOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    let response = get_worker_info(&mut session, opts.worker_id).await?;

    if let Some(worker) = response {
        gsettings.printer().print_worker_info(worker);
    } else {
        log::error!("Worker {} not found", opts.worker_id);
    }
    Ok(())
}

fn command_worker_hwdetect(gsettings: &GlobalSettings, opts: HwDetectOpts) -> anyhow::Result<()> {
    let cpus = if opts.no_hyperthreading {
        detect_cpus_no_ht()?
    } else {
        detect_cpus()?
    };
    let generic = detect_generic_resources()?;
    gsettings
        .printer()
        .print_hw(&ResourceDescriptor::new(cpus, generic));
    Ok(())
}

async fn command_worker_address(
    gsettings: &GlobalSettings,
    opts: WorkerAddressOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    let response = get_worker_info(&mut session, opts.worker_id).await?;

    match response {
        Some(info) => println!("{}", info.configuration.hostname),
        None => anyhow::bail!("Worker {} not found", opts.worker_id),
    }

    Ok(())
}

///Starts the hq Dashboard
async fn command_dashboard_start(
    gsettings: &GlobalSettings,
    _opts: DashboardOpts,
) -> anyhow::Result<()> {
    start_ui_loop(gsettings).await?;
    Ok(())
}

#[derive(clap::ArgEnum, Clone)]
pub enum ColorPolicy {
    Auto,
    Always,
    Never,
}

fn make_global_settings(opts: CommonOpts) -> GlobalSettings {
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
    let printer: Box<dyn Output> = match opts.output_mode {
        Outputs::CLI => {
            // Set colored public for CLI
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

fn generate_completion(opts: GenerateCompletionOpts) -> anyhow::Result<()> {
    let generator = opts.shell;

    let mut app = Opts::command();
    eprintln!("Generating completion file for {}...", generator);
    generate(generator, &mut app, "hq".to_string(), &mut io::stdout());
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    let top_opts: Opts = Opts::parse();
    setup_logging(top_opts.common.debug);

    let gsettings = make_global_settings(top_opts.common);

    let result = match top_opts.subcmd {
        SubCommand::Server(opts) => command_server(&gsettings, opts).await,
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
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::List(opts),
        }) => command_job_list(&gsettings, opts).await,
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::Info(opts),
        }) => command_job_detail(&gsettings, opts).await,
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::Cat(opts),
        }) => command_job_cat(&gsettings, opts).await,
        SubCommand::Submit(opts)
        | SubCommand::Job(JobOpts {
            subcmd: JobCommand::Submit(opts),
        }) => command_submit(&gsettings, opts).await,
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::Cancel(opts),
        }) => command_job_cancel(&gsettings, opts).await,
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::Resubmit(opts),
        }) => command_job_resubmit(&gsettings, opts).await,
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::Wait(opts),
        }) => command_job_wait(&gsettings, opts).await,
        SubCommand::Job(JobOpts {
            subcmd: JobCommand::Progress(opts),
        }) => command_job_progress(&gsettings, opts).await,
        SubCommand::Task(TaskOpts {
            subcmd: TaskCommand::List(opts),
        }) => command_task_list(&gsettings, opts).await,
        SubCommand::Dashboard(opts) => command_dashboard_start(&gsettings, opts).await,
        SubCommand::Log(opts) => command_log(&gsettings, opts),
        SubCommand::AutoAlloc(opts) => command_autoalloc(&gsettings, opts).await,
        SubCommand::EventLog(opts) => command_event_log(opts),
        SubCommand::GenerateCompletion(opts) => generate_completion(opts),
    };

    if let Err(e) = result {
        gsettings.printer().print_error(e);
        std::process::exit(1);
    }

    Ok(())
}
