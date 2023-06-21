use std::time::Duration;

use clap::Parser;

use crate::client::commands::worker::{ArgServerLostPolicy, SharedWorkerStartOpts};
use crate::client::globalsettings::GlobalSettings;
use crate::common::format::server_lost_policy_to_str;
use crate::common::manager::info::ManagerType;
use crate::common::utils::time::parse_hms_or_human_time;
use crate::rpc_call;
use crate::server::autoalloc::{Allocation, AllocationState, QueueId};
use crate::server::bootstrap::get_client_session;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    AllocationQueueParams, AutoAllocRequest, AutoAllocResponse, FromClientMessage, ToClientMessage,
};

#[derive(Parser)]
pub struct AutoAllocOpts {
    #[clap(subcommand)]
    subcmd: AutoAllocCommand,
}

#[derive(Parser)]
enum AutoAllocCommand {
    /// Displays allocation queues
    List,
    /// Display allocations of the specified allocation queue
    Info(AllocationsOpts),
    /// Add new allocation queue
    Add(AddQueueOpts),
    /// Pause an existing allocation queue.
    /// Paused queues do not submit new allocations.
    Pause(PauseQueueOpts),
    /// Resume a previously paused allocation queue.
    Resume(ResumeQueueOpts),
    /// Try to submit an allocation to test allocation parameters
    DryRun(DryRunOpts),
    /// Removes an allocation queue with the given ID
    Remove(RemoveQueueOpts),
}

#[derive(Parser)]
struct AddQueueOpts {
    #[clap(subcommand)]
    subcmd: AddQueueCommand,
}

#[derive(Parser)]
struct RemoveQueueOpts {
    /// ID of the allocation queue that should be removed
    queue_id: QueueId,

    /// Remove the queue even if there are currently running jobs.
    /// The running jobs will be canceled.
    #[arg(long, num_args(0))]
    force: bool,
}

#[derive(Parser)]
enum AddQueueCommand {
    /// Create a PBS allocation queue
    Pbs(SharedQueueOpts),
    /// Create a SLURM allocation queue
    Slurm(SharedQueueOpts),
}

fn parse_backlog(value: &str) -> Result<u32, anyhow::Error> {
    let value: u32 = value.parse()?;
    if value == 0 {
        Err(anyhow::anyhow!("Backlog has to be at least 1"))
    } else {
        Ok(value)
    }
}

#[derive(Parser)]
struct SharedQueueOpts {
    /// How many jobs should be waiting in the queue to be started
    #[arg(long, short, default_value_t = 1, value_parser = parse_backlog)]
    backlog: u32,

    /// Time limit (walltime) of PBS/Slurm allocations
    #[arg(long, short('t'), value_parser = parse_hms_or_human_time)]
    time_limit: Duration,

    /// How many workers (nodes) should be spawned in each allocation
    #[arg(long, short, default_value_t = 1)]
    workers_per_alloc: u32,

    /// Maximum number of workers that can be queued or running at any given time in this queue
    #[arg(long)]
    max_worker_count: Option<u32>,

    /// Name of the allocation queue (for debug purposes only)
    #[arg(long, short)]
    name: Option<String>,

    #[clap(flatten)]
    worker_args: SharedWorkerStartOpts,

    /// Behavior when a connection to a server is lost
    #[arg(long, default_value_t = ArgServerLostPolicy::FinishRunning, value_enum)]
    on_server_lost: ArgServerLostPolicy,

    /// Disables dry-run, which submits an allocation with the specified parameters to verify
    /// whether the parameters are correct.
    // This flag currently cannot be in [`AddQueueOpts`] because of a bug in clap:
    // https://github.com/clap-rs/clap/issues/1570.
    #[arg(long, global = true)]
    no_dry_run: bool,

    /// Shell command that will be executed on each allocated node, before a worker is created on
    /// that node.
    #[arg(long)]
    worker_start_cmd: Option<String>,

    /// Shell command that will be executed on each allocated node, just before the allocation ends.
    /// Note that this execution is best-effort. It is not guaranteed that the script will always
    /// be executed.
    #[arg(long)]
    worker_stop_cmd: Option<String>,

    /// Additional arguments passed to the submit command
    #[arg(trailing_var_arg(true))]
    additional_args: Vec<String>,
}

#[derive(Parser)]
struct DryRunOpts {
    #[clap(subcommand)]
    subcmd: DryRunCommand,
}

#[derive(Parser)]
enum DryRunCommand {
    /// Try to create a PBS allocation
    Pbs(SharedQueueOpts),
    /// Try to create a SLURM allocation
    Slurm(SharedQueueOpts),
}

#[derive(Parser)]
struct EventsOpts {
    /// ID of the allocation queue
    queue: u32,
}

#[derive(Parser)]
struct AllocationsOpts {
    /// ID of the allocation queue
    queue: u32,

    /// Display only allocations with the given state
    #[arg(long, value_enum)]
    filter: Option<AllocationStateFilter>,
}

#[derive(clap::ValueEnum, Clone, Eq, PartialEq)]
enum AllocationStateFilter {
    Queued,
    Running,
    Finished,
    Failed,
}

#[derive(Parser)]
struct PauseQueueOpts {
    /// ID of the allocation queue that should be paused
    queue_id: QueueId,
}

#[derive(Parser)]
struct ResumeQueueOpts {
    /// ID of the allocation queue that should be resumed
    queue_id: QueueId,
}

pub async fn command_autoalloc(
    gsettings: &GlobalSettings,
    opts: AutoAllocOpts,
) -> anyhow::Result<()> {
    let session = get_client_session(gsettings.server_directory()).await?;

    match opts.subcmd {
        AutoAllocCommand::List => {
            print_allocation_queues(gsettings, session).await?;
        }
        AutoAllocCommand::Add(opts) => {
            add_queue(session, opts).await?;
        }
        AutoAllocCommand::Info(opts) => {
            print_allocations(gsettings, session, opts).await?;
        }
        AutoAllocCommand::Remove(opts) => {
            remove_queue(session, opts.queue_id, opts.force).await?;
        }
        AutoAllocCommand::DryRun(opts) => {
            dry_run_command(session, opts).await?;
        }
        AutoAllocCommand::Pause(opts) => {
            pause_queue(session, opts).await?;
        }
        AutoAllocCommand::Resume(opts) => {
            resume_queue(session, opts).await?;
        }
    }
    Ok(())
}

fn args_to_params(args: SharedQueueOpts) -> AllocationQueueParams {
    let SharedQueueOpts {
        backlog,
        time_limit,
        workers_per_alloc,
        max_worker_count,
        name,
        worker_args,
        worker_start_cmd,
        worker_stop_cmd,
        additional_args,
        on_server_lost,
        no_dry_run: _,
    } = args;

    if let Some(ref idle_timeout) = worker_args.idle_timeout {
        if *idle_timeout > Duration::from_secs(60 * 10) {
            log::warn!(
                "You have set an idle timeout longer than 10 minutes. This can result in \
wasted allocation duration."
            );
        }
    }

    let SharedWorkerStartOpts {
        cpus,
        resource,
        group,
        no_detect_resources,
        no_hyper_threading,
        idle_timeout,
        overview_interval,
    } = worker_args;

    let mut worker_args = vec![];
    if let Some(cpus) = cpus {
        worker_args.extend(["--cpus".to_string(), cpus.into_original_input()]);
    }
    for arg in resource {
        worker_args.extend([
            "--resource".to_string(),
            format!("\"{}\"", arg.into_original_input()),
        ]);
    }
    if let Some(group) = group {
        worker_args.extend(["--group".to_string(), group]);
    }
    if no_hyper_threading {
        worker_args.push("--no-hyper-threading".to_string());
    }
    if no_detect_resources {
        worker_args.push("--no-detect-resources".to_string());
    }
    if let Some(overview_interval) = overview_interval {
        worker_args.extend([
            "--overview-interval".to_string(),
            overview_interval.into_original_input(),
        ]);
    }
    worker_args.extend([
        "--on-server-lost".to_string(),
        server_lost_policy_to_str(&on_server_lost.into()).to_string(),
    ]);

    AllocationQueueParams {
        workers_per_alloc,
        backlog,
        timelimit: time_limit,
        name,
        additional_args,
        worker_start_cmd,
        worker_stop_cmd,
        max_worker_count,
        worker_args,
        idle_timeout,
    }
}

async fn dry_run_command(mut session: ClientSession, opts: DryRunOpts) -> anyhow::Result<()> {
    let (manager, parameters) = match opts.subcmd {
        DryRunCommand::Pbs(params) => (ManagerType::Pbs, args_to_params(params)),
        DryRunCommand::Slurm(params) => (ManagerType::Slurm, args_to_params(params)),
    };
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::DryRun {
        manager,
        parameters,
    });

    rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::DryRunSuccessful) => ()
    )
    .await?;

    log::info!(
        "A trial allocation was submitted successfully. It was immediately canceled to avoid \
wasting resources."
    );
    Ok(())
}

async fn add_queue(mut session: ClientSession, opts: AddQueueOpts) -> anyhow::Result<()> {
    let (manager, parameters, dry_run) = match opts.subcmd {
        AddQueueCommand::Pbs(params) => {
            let no_dry_run = params.no_dry_run;
            (ManagerType::Pbs, args_to_params(params), !no_dry_run)
        }
        AddQueueCommand::Slurm(params) => {
            let no_dry_run = params.no_dry_run;
            (ManagerType::Slurm, args_to_params(params), !no_dry_run)
        }
    };

    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::AddQueue {
        manager,
        parameters,
        dry_run,
    });

    let queue_id = rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueCreated(id)) => id
    )
    .await?;

    if dry_run {
        log::info!(
            "A trial allocation was submitted successfully. It was immediately canceled to avoid \
wasting resources."
        );
    }

    log::info!("Allocation queue {queue_id} successfully created");
    Ok(())
}

async fn remove_queue(
    mut session: ClientSession,
    queue_id: QueueId,
    force: bool,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::RemoveQueue { queue_id, force });

    rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueRemoved(_)) => ()
    )
    .await?;

    log::info!("Allocation queue {queue_id} successfully removed");
    Ok(())
}

async fn print_allocation_queues(
    gsettings: &GlobalSettings,
    mut session: ClientSession,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::List);
    let response = rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::List(r)) => r
    )
    .await?;

    gsettings.printer().print_autoalloc_queues(response);
    Ok(())
}

async fn print_allocations(
    gsettings: &GlobalSettings,
    mut session: ClientSession,
    opts: AllocationsOpts,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Info {
        queue_id: opts.queue,
    });
    let mut allocations = rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(allocs)) => allocs
    )
    .await?;
    filter_allocations(&mut allocations, opts.filter);
    gsettings.printer().print_allocations(allocations);
    Ok(())
}

async fn pause_queue(mut session: ClientSession, opts: PauseQueueOpts) -> anyhow::Result<()> {
    let PauseQueueOpts { queue_id } = opts;
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::PauseQueue { queue_id });

    rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueuePaused(_)) => ()
    )
    .await?;

    log::info!("Allocation queue {queue_id} successfully paused");

    Ok(())
}

async fn resume_queue(mut session: ClientSession, opts: ResumeQueueOpts) -> anyhow::Result<()> {
    let ResumeQueueOpts { queue_id } = opts;
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::ResumeQueue { queue_id });

    rpc_call!(session.connection(), message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueResumed(_)) => ()
    )
    .await?;

    log::info!("Allocation queue {queue_id} successfully paused");

    Ok(())
}

fn filter_allocations(allocations: &mut Vec<Allocation>, filter: Option<AllocationStateFilter>) {
    if let Some(filter) = filter {
        allocations.retain(|allocation| {
            let status = &allocation.status;
            match filter {
                AllocationStateFilter::Queued => matches!(status, AllocationState::Queued),
                AllocationStateFilter::Running => {
                    matches!(status, AllocationState::Running { .. })
                }
                AllocationStateFilter::Finished => {
                    matches!(status, AllocationState::Finished { .. })
                }
                AllocationStateFilter::Failed => {
                    matches!(status, AllocationState::Finished { .. }) && status.is_failed()
                }
            }
        })
    }
}
