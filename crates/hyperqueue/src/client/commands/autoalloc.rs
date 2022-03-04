use clap::Parser;

use crate::client::commands::worker::ArgServerLostPolicy;
use crate::client::globalsettings::GlobalSettings;
use crate::client::utils::PassThroughArgument;
use crate::common::manager::info::ManagerType;
use crate::common::timeutils::ExtendedArgDuration;
use crate::rpc_call;
use crate::server::autoalloc::{Allocation, AllocationState, QueueId};
use crate::server::bootstrap::get_client_connection;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    AllocationQueueParams, AutoAllocRequest, AutoAllocResponse, FromClientMessage, ToClientMessage,
};
use crate::worker::parser::{ArgCpuDefinition, ArgGenericResourceDef};

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
    #[clap(long, takes_value = false)]
    force: bool,
}

#[derive(Parser)]
enum AddQueueCommand {
    /// Create a PBS allocation queue
    Pbs(SharedQueueOpts),
    /// Create a SLURM allocation queue
    Slurm(SharedQueueOpts),
}

#[derive(Parser)]
#[clap(setting = clap::AppSettings::TrailingVarArg)]
struct SharedQueueOpts {
    /// How many jobs should be waiting in the queue to be started
    #[clap(long, short, default_value = "1")]
    backlog: u32,

    /// Time limit (walltime) of PBS/Slurm allocations
    #[clap(long, short('t'))]
    time_limit: ExtendedArgDuration,

    /// How many workers (nodes) should be spawned in each allocation
    #[clap(long, short, default_value = "1")]
    workers_per_alloc: u32,

    /// Maximum number of workers that can be queued or running at any given time in this queue
    #[clap(long)]
    max_worker_count: Option<u32>,

    /// Name of the allocation queue (for debug purposes only)
    #[clap(long, short)]
    name: Option<String>,

    /// How many cores should be allocated for workers spawned inside allocations
    #[clap(long)]
    cpus: Option<PassThroughArgument<ArgCpuDefinition>>,

    /// What resources should the workers spawned inside allocations contain
    #[clap(long, setting = clap::ArgSettings::MultipleOccurrences)]
    resource: Vec<PassThroughArgument<ArgGenericResourceDef>>,

    /// Behavior when a connection to a server is lost
    #[clap(long, default_value = "finish-running", arg_enum)]
    on_server_lost: ArgServerLostPolicy,

    /// Disables dry-run, which submits an allocation with the specified parameters to verify
    /// whether the parameters are correct.
    // This flag currently cannot be in [`AddQueueOpts`] because of a bug in clap:
    // https://github.com/clap-rs/clap/issues/1570.
    #[clap(long, global = true)]
    no_dry_run: bool,

    /// Additional arguments passed to the submit command
    #[clap()]
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
    #[clap(long, arg_enum)]
    filter: Option<AllocationStateFilter>,
}

#[derive(clap::ArgEnum, Clone, Eq, PartialEq)]
enum AllocationStateFilter {
    Queued,
    Running,
    Finished,
    Failed,
}

pub async fn command_autoalloc(
    gsettings: &GlobalSettings,
    opts: AutoAllocOpts,
) -> anyhow::Result<()> {
    let connection = get_client_connection(gsettings.server_directory()).await?;

    match opts.subcmd {
        AutoAllocCommand::List => {
            print_allocation_queues(gsettings, connection).await?;
        }
        AutoAllocCommand::Add(opts) => {
            add_queue(connection, opts).await?;
        }
        AutoAllocCommand::Info(opts) => {
            print_allocations(gsettings, connection, opts).await?;
        }
        AutoAllocCommand::Remove(opts) => {
            remove_queue(connection, opts.queue_id, opts.force).await?;
        }
        AutoAllocCommand::DryRun(opts) => {
            dry_run_command(connection, opts).await?;
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
        cpus,
        resource,
        additional_args,
        on_server_lost,
        no_dry_run: _,
    } = args;

    AllocationQueueParams {
        workers_per_alloc,
        backlog,
        timelimit: time_limit.unpack(),
        name,
        additional_args,
        worker_cpu_arg: cpus.map(|v| v.into()),
        worker_resources_args: resource.into_iter().map(|v| v.into()).collect(),
        max_worker_count,
        on_server_lost: on_server_lost.into(),
    }
}

async fn dry_run_command(mut connection: ClientConnection, opts: DryRunOpts) -> anyhow::Result<()> {
    let (manager, parameters) = match opts.subcmd {
        DryRunCommand::Pbs(params) => (ManagerType::Pbs, args_to_params(params)),
        DryRunCommand::Slurm(params) => (ManagerType::Slurm, args_to_params(params)),
    };
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::DryRun {
        manager,
        parameters,
    });

    rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::DryRunSuccessful) => ()
    )
    .await?;

    log::info!(
        "A trial allocation was submitted successfully. It was immediately canceled to avoid \
wasting resources."
    );
    Ok(())
}

async fn add_queue(mut connection: ClientConnection, opts: AddQueueOpts) -> anyhow::Result<()> {
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

    let queue_id = rpc_call!(connection, message,
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
    mut connection: ClientConnection,
    queue_id: QueueId,
    force: bool,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::RemoveQueue { queue_id, force });

    rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueRemoved(_)) => ()
    )
    .await?;

    log::info!("Allocation queue {queue_id} successfully removed");
    Ok(())
}

async fn print_allocation_queues(
    gsettings: &GlobalSettings,
    mut connection: ClientConnection,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::List);
    let response = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::List(r)) => r
    )
    .await?;

    gsettings.printer().print_autoalloc_queues(response);
    Ok(())
}

async fn print_allocations(
    gsettings: &GlobalSettings,
    mut connection: ClientConnection,
    opts: AllocationsOpts,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Info {
        queue_id: opts.queue,
    });
    let mut allocations = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(allocs)) => allocs
    )
    .await?;
    filter_allocations(&mut allocations, opts.filter);
    gsettings.printer().print_allocations(allocations);
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
