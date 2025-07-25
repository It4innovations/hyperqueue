use clap::{CommandFactory, FromArgMatches};
use clap_complete::generate;
use cli_table::ColorChoice;
use colored::Colorize;
use hyperqueue::HQ_VERSION;
use hyperqueue::client::commands::autoalloc::command_autoalloc;
use hyperqueue::client::commands::data::command_task_data;
use hyperqueue::client::commands::doc::command_doc;
use hyperqueue::client::commands::job::{
    JobCancelOpts, JobCatOpts, JobCloseOpts, JobForgetOpts, JobInfoOpts, JobListOpts,
    JobTaskIdsOpts, cancel_job, close_job, forget_job, output_job_cat, output_job_detail,
    output_job_list, output_job_summary,
};
use hyperqueue::client::commands::journal::command_journal;
use hyperqueue::client::commands::outputlog::command_reader;
use hyperqueue::client::commands::server::{ServerCommand, ServerOpts, command_server};
use hyperqueue::client::commands::submit::command::{SubmitJobConfOpts, open_job};
use hyperqueue::client::commands::submit::{
    JobSubmitFileOpts, JobSubmitOpts, submit_computation, submit_computation_from_job_file,
};
use hyperqueue::client::commands::wait::{wait_for_jobs, wait_for_jobs_with_progress};
use hyperqueue::client::commands::worker::{
    WorkerFilter, WorkerStartOpts, deploy_ssh_workers, get_worker_info, get_worker_list,
    start_hq_worker, stop_worker, wait_for_workers,
};
use hyperqueue::client::default_server_directory_path;
use hyperqueue::client::globalsettings::GlobalSettings;
use hyperqueue::client::output::cli::CliOutput;
use hyperqueue::client::output::json::JsonOutput;
use hyperqueue::client::output::outputs::{Output, Outputs};
use hyperqueue::client::output::quiet::Quiet;
use hyperqueue::client::status::Status;
use hyperqueue::client::task::{
    TaskCommand, TaskExplainOpts, TaskInfoOpts, TaskListOpts, output_job_task_explain,
    output_job_task_ids, output_job_task_info, output_job_task_list,
};
use hyperqueue::common::cli::{
    ColorPolicy, CommonOpts, DeploySshOpts, GenerateCompletionOpts, HwDetectOpts, JobCommand,
    JobProgressOpts, JobWaitOpts, OptsWithMatches, RootOptions, SubCommand, WorkerAddressOpts,
    WorkerCommand, WorkerInfoOpts, WorkerListOpts, WorkerOpts, WorkerStopOpts, WorkerWaitOpts,
    get_task_id_selector, get_task_selector,
};
use hyperqueue::common::setup::setup_logging;
use hyperqueue::common::utils::fs::absolute_path;
use hyperqueue::server::bootstrap::get_client_session;
use hyperqueue::transfer::messages::{
    FromClientMessage, IdSelector, JobInfoRequest, ToClientMessage,
};
use hyperqueue::worker::hwdetect::{
    detect_additional_resources, detect_cpus, prune_hyper_threading,
};
use nix::sys::signal::{SigHandler, Signal};
use std::io;
use std::io::IsTerminal;
use std::panic::PanicHookInfo;
use tako::resources::{CPU_RESOURCE_NAME, ResourceDescriptor, ResourceDescriptorItem};

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

async fn command_job_submit(
    gsettings: &GlobalSettings,
    opts: OptsWithMatches<JobSubmitOpts>,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    submit_computation(gsettings, &mut session, opts).await
}

async fn command_job_open(
    gsettings: &GlobalSettings,
    opts: SubmitJobConfOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    open_job(gsettings, &mut session, opts).await
}

async fn command_submit_job_file(
    gsettings: &GlobalSettings,
    opts: JobSubmitFileOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    submit_computation_from_job_file(gsettings, &mut session, opts).await
}

async fn command_job_list(gsettings: &GlobalSettings, opts: JobListOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;

    let (filter, show_open) = if opts.filter.is_empty() {
        if opts.all {
            (vec![], true)
        } else {
            (vec![Status::Waiting, Status::Running, Status::Opened], true)
        }
    } else {
        (opts.filter, false)
    };

    output_job_list(gsettings, &mut connection, filter, show_open).await
}

async fn command_job_summary(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;

    output_job_summary(gsettings, &mut connection).await
}

async fn command_job_detail(gsettings: &GlobalSettings, opts: JobInfoOpts) -> anyhow::Result<()> {
    if matches!(opts.selector, IdSelector::All) {
        log::warn!(
            "Job detail doesn't support the `all` selector, did you mean to use `hq job list --all`?"
        );
        return Ok(());
    }

    let mut session = get_client_session(gsettings.server_directory()).await?;
    output_job_detail(gsettings, &mut session, opts.selector).await
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
    cancel_job(gsettings, &mut connection, opts.selector).await
}

async fn command_job_close(gsettings: &GlobalSettings, opts: JobCloseOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    close_job(gsettings, &mut connection, opts.selector).await
}

async fn command_job_delete(gsettings: &GlobalSettings, opts: JobForgetOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    forget_job(gsettings, &mut connection, opts).await
}

async fn command_job_task_ids(
    gsettings: &GlobalSettings,
    opts: JobTaskIdsOpts,
) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    output_job_task_ids(gsettings, &mut connection, opts).await
}

async fn command_job_wait(gsettings: &GlobalSettings, opts: JobWaitOpts) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    wait_for_jobs(
        gsettings,
        &mut connection,
        opts.selector,
        !opts.without_close,
    )
    .await
}

async fn command_job_progress(
    gsettings: &GlobalSettings,
    opts: JobProgressOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    let response = hyperqueue::rpc_call!(
        session.connection(),
        FromClientMessage::JobInfo(JobInfoRequest {
            selector: opts.selector,
        }),
        ToClientMessage::JobInfoResponse(r) => r
    )
    .await?;
    wait_for_jobs_with_progress(&mut session, &response.jobs).await
}

async fn command_task_list(gsettings: &GlobalSettings, opts: TaskListOpts) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    output_job_task_list(
        gsettings,
        &mut session,
        opts.job_selector,
        get_task_selector(Some(opts.task_selector)),
        opts.verbosity.into(),
    )
    .await
}

async fn command_task_info(gsettings: &GlobalSettings, opts: TaskInfoOpts) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    output_job_task_info(
        gsettings,
        &mut session,
        opts.job_selector,
        get_task_id_selector(Some(opts.task_selector)),
        opts.verbosity.into(),
    )
    .await
}

async fn command_task_explain(
    gsettings: &GlobalSettings,
    opts: TaskExplainOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    output_job_task_explain(gsettings, &mut session, opts).await
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
    stop_worker(&mut session, opts.selector_arg).await?;
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
    let response = get_worker_info(&mut session, opts.worker_id, true).await?;

    if let Some(worker) = response {
        gsettings.printer().print_worker_info(worker);
    } else {
        log::error!("Worker {} not found", opts.worker_id);
    }
    Ok(())
}

async fn command_worker_wait(
    gsettings: &GlobalSettings,
    opts: WorkerWaitOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    wait_for_workers(&mut session, opts.worker_count).await
}

async fn command_worker_deploy_ssh(
    _gsettings: &GlobalSettings,
    opts: DeploySshOpts,
) -> anyhow::Result<()> {
    deploy_ssh_workers(opts).await
}

fn command_worker_hwdetect(gsettings: &GlobalSettings, opts: HwDetectOpts) -> anyhow::Result<()> {
    let mut cpus = detect_cpus()?;
    if opts.no_hyper_threading {
        cpus = prune_hyper_threading(&cpus)?;
    }
    let mut resources = vec![ResourceDescriptorItem {
        name: CPU_RESOURCE_NAME.to_string(),
        kind: cpus,
    }];
    detect_additional_resources(&mut resources)?;
    gsettings
        .printer()
        .print_hw(&ResourceDescriptor::new(resources, Default::default()));
    Ok(())
}

async fn command_worker_address(
    gsettings: &GlobalSettings,
    opts: WorkerAddressOpts,
) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    let response = get_worker_info(&mut session, opts.worker_id, false).await?;

    match response {
        Some(info) => println!("{}", info.configuration.hostname),
        None => anyhow::bail!("Worker {} not found", opts.worker_id),
    }

    Ok(())
}

#[cfg(feature = "dashboard")]
/// Starts the hq dashboard
async fn command_dashboard_start(
    gsettings: &GlobalSettings,
    opts: hyperqueue::common::cli::DashboardOpts,
) -> anyhow::Result<()> {
    use hyperqueue::dashboard::preload_dashboard_events;
    use hyperqueue::dashboard::start_ui_loop;

    let cmd = opts.subcmd.unwrap_or_default();
    let events = preload_dashboard_events(gsettings, cmd).await?;
    start_ui_loop(events).await
}

fn make_global_settings(opts: CommonOpts) -> GlobalSettings {
    let server_dir = absolute_path(
        opts.server_dir
            .unwrap_or_else(default_server_directory_path),
    );

    let color_policy = match opts.colors {
        ColorPolicy::Always => ColorChoice::AlwaysAnsi,
        ColorPolicy::Auto => {
            if io::stdout().is_terminal() {
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
        Outputs::JSON => Box::<JsonOutput>::default(),
        Outputs::Quiet => Box::<Quiet>::default(),
    };

    GlobalSettings::new(server_dir, printer)
}

fn generate_completion(opts: GenerateCompletionOpts) -> anyhow::Result<()> {
    let generator = opts.shell;

    let mut app = RootOptions::command();
    eprintln!("Generating completion file for {generator}...");
    generate(generator, &mut app, "hq".to_string(), &mut io::stdout());
    Ok(())
}

fn hq_panic_hook(_info: &PanicHookInfo) {
    let message = format!(
        r#"Oops, HyperQueue has crashed. This is a bug, sorry for that.
If you would be so kind, please report this issue at the HQ issue tracker: https://github.com/It4innovations/hyperqueue/issues/new?title=HQ%20crashes
Please include the above error (starting from "thread ... panicked ...") and the stack backtrace in the issue contents, along with the following information:

HyperQueue version: {HQ_VERSION}

You can also re-run HyperQueue server (and its workers) with the `RUST_LOG=hq=debug,tako=debug`
environment variable, and attach the logs to the issue, to provide us more information.
"#
    );

    if io::stdout().is_terminal() {
        eprintln!("{}", message.red());
    } else {
        eprintln!("{message}");
    };
}

#[cfg(unix)]
fn reset_sigpipe() {
    unsafe {
        nix::sys::signal::signal(Signal::SIGPIPE, SigHandler::SigDfl)
            .expect("cannot reset sigpipe");
    }
}

#[cfg(not(unix))]
fn reset_sigpipe() {}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    // Augment panics - first print the error and backtrace like normally,
    // and then print our own custom error message.
    let std_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info: &PanicHookInfo| {
        std_panic(info);
        hq_panic_hook(info);
    }));

    // Also enable backtraces by default.
    // This enables backtraces when panicking, but also for normal anyhow errors.
    // SAFETY: we are at the beginning of the program, no other threads that could set
    // environment variables should be executing.
    unsafe {
        std::env::set_var("RUST_BACKTRACE", "full");
    }

    // This further disables backtraces for normal anyhow errors.
    // They should not be printed to users in release mode.
    #[cfg(not(debug_assertions))]
    {
        unsafe {
            std::env::set_var("RUST_LIB_BACKTRACE", "0");
        }
    }

    let matches = RootOptions::command().get_matches();
    let top_opts = match RootOptions::from_arg_matches(&matches) {
        Ok(opts) => opts,
        Err(error) => error.exit(),
    };

    setup_logging(top_opts.common.debug);

    let gsettings = make_global_settings(top_opts.common);

    let is_cli_like = match &top_opts.subcmd {
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Start(_),
        }) => false,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Start(_),
        }) => false,
        #[cfg(feature = "dashboard")]
        SubCommand::Dashboard(_) => false,
        _ => true,
    };

    if is_cli_like {
        // When our stdout is attached to a pipe and the pipe is closed,
        // it manifests as an I/O error, because the Rust runtime ignores
        // SIGPIPE by default.
        // This in turn causes `println!` to panic, which is not ideal,
        // because it crashes HQ when used with Unix CLI utilities (such as `head`).
        // Therefore, we reset SIGPIPE to its default behavior (terminate the process)
        // to avoid the panics.
        // See https://github.com/It4innovations/hyperqueue/issues/851.
        // However, we only do this for client commands, which are short running and
        // designed to be combined with other CLI tools.
        // Enabling this for server and workers has unintended consequences, for example
        // when a worker writes stdin to a task and the task has closed its stdin, then
        // this would terminate the worker.
        reset_sigpipe();
    }

    let is_long_running = matches!(
        top_opts.subcmd,
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Start(_),
        }) | SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Start(_),
        })
    );
    if is_long_running {
        log::info!("HyperQueue {HQ_VERSION}");
    }

    let result = match top_opts.subcmd {
        SubCommand::Server(opts) => command_server(&gsettings, opts).await,
        SubCommand::Worker(opts) => match opts.subcmd {
            WorkerCommand::Start(opts) => command_worker_start(&gsettings, opts).await,
            WorkerCommand::Stop(opts) => command_worker_stop(&gsettings, opts).await,
            WorkerCommand::List(opts) => command_worker_list(&gsettings, opts).await,
            WorkerCommand::HwDetect(opts) => command_worker_hwdetect(&gsettings, opts),
            WorkerCommand::Info(opts) => command_worker_info(&gsettings, opts).await,
            WorkerCommand::Address(opts) => command_worker_address(&gsettings, opts).await,
            WorkerCommand::Wait(opts) => command_worker_wait(&gsettings, opts).await,
            WorkerCommand::DeploySsh(opts) => command_worker_deploy_ssh(&gsettings, opts).await,
        },
        SubCommand::Job(opts) => match opts.subcmd {
            JobCommand::List(opts) => command_job_list(&gsettings, opts).await,
            JobCommand::Summary => command_job_summary(&gsettings).await,
            JobCommand::Info(opts) => command_job_detail(&gsettings, opts).await,
            JobCommand::Cancel(opts) => command_job_cancel(&gsettings, opts).await,
            JobCommand::Forget(opts) => command_job_delete(&gsettings, opts).await,
            JobCommand::Cat(opts) => command_job_cat(&gsettings, opts).await,
            JobCommand::Submit(opts) => {
                command_job_submit(&gsettings, OptsWithMatches::new(opts, matches)).await
            }
            JobCommand::SubmitFile(opts) => command_submit_job_file(&gsettings, opts).await,
            JobCommand::Wait(opts) => command_job_wait(&gsettings, opts).await,
            JobCommand::Progress(opts) => command_job_progress(&gsettings, opts).await,
            JobCommand::TaskIds(opts) => command_job_task_ids(&gsettings, opts).await,
            JobCommand::Open(opts) => command_job_open(&gsettings, opts).await,
            JobCommand::Close(opts) => command_job_close(&gsettings, opts).await,
        },
        SubCommand::Submit(opts) => {
            command_job_submit(&gsettings, OptsWithMatches::new(opts, matches)).await
        }
        SubCommand::Task(opts) => match opts.subcmd {
            TaskCommand::List(opts) => command_task_list(&gsettings, opts).await,
            TaskCommand::Info(opts) => command_task_info(&gsettings, opts).await,
            TaskCommand::Explain(opts) => command_task_explain(&gsettings, opts).await,
        },
        SubCommand::Data(opts) => command_task_data(&gsettings, opts).await,
        #[cfg(feature = "dashboard")]
        SubCommand::Dashboard(opts) => command_dashboard_start(&gsettings, opts).await,
        SubCommand::OutputLog(opts) => command_reader(&gsettings, opts),
        SubCommand::AutoAlloc(opts) => command_autoalloc(&gsettings, opts).await,
        SubCommand::Journal(opts) => command_journal(&gsettings, opts).await,
        SubCommand::GenerateCompletion(opts) => generate_completion(opts),
        SubCommand::Doc(opts) => command_doc(opts),
    };

    if let Err(e) = result {
        gsettings.printer().print_error(e);
        std::process::exit(1);
    }

    Ok(())
}
