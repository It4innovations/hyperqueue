use crate::client::globalsettings::GlobalSettings;
use crate::common::timeutils::ArgDuration;
use crate::rpc_call;
use crate::server::autoalloc::{Allocation, AllocationStatus, DescriptorId};
use crate::server::bootstrap::get_client_connection;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    AddQueueParams, AddQueueRequest, AutoAllocRequest, AutoAllocResponse, FromClientMessage,
    ToClientMessage,
};
use clap::Parser;
use std::str::FromStr;

#[derive(Parser)]
pub struct AutoAllocOpts {
    #[clap(subcommand)]
    subcmd: AutoAllocCommand,
}

#[derive(Parser)]
enum AutoAllocCommand {
    /// Displays allocation queues
    List,
    /// Display event log for a specified allocation queue
    Events(EventsOpts),
    /// Display allocations of the specified allocation queue
    Info(AllocationsOpts),
    /// Add new allocation queue
    Add(AddQueueOpts),
    /// Removes an allocation queue with the given ID
    Remove(RemoveQueueOpts),
}

#[derive(Parser)]
pub struct AddQueueOpts {
    #[clap(subcommand)]
    subcmd: AddQueueCommand,
}

#[derive(Parser)]
pub struct RemoveQueueOpts {
    /// ID of the allocation queue that should be removed
    queue_id: DescriptorId,
}

#[derive(Parser)]
pub enum AddQueueCommand {
    /// Create a PBS allocation queue
    Pbs(SharedQueueOpts),
    /// Create a SLURM allocation queue
    Slurm(SharedQueueOpts),
}

#[derive(Parser)]
#[clap(setting = clap::AppSettings::TrailingVarArg)]
pub struct SharedQueueOpts {
    /// How many jobs should be waiting in the queue to be started
    #[clap(long, short, default_value = "4")]
    backlog: u32,

    /// Time limit (walltime) of PBS allocations
    #[clap(long, short('t'))]
    time_limit: Option<ArgDuration>,

    /// How many workers (nodes) should be spawned in each allocation
    #[clap(long, short, default_value = "1")]
    workers_per_alloc: u32,

    /// Name of the allocation queue (for debug purposes only)
    #[clap(long, short)]
    name: Option<String>,

    /// Additional arguments passed to the submit command
    #[clap()]
    additional_args: Vec<String>,
}

#[derive(Parser)]
pub struct EventsOpts {
    /// ID of the allocation queue
    queue: u32,
}

#[derive(Parser)]
pub struct AllocationsOpts {
    /// ID of the allocation queue
    queue: u32,

    /// Display only allocations with the given state
    #[clap(long, possible_values = &["queued", "running", "finished", "failed"])]
    filter: Option<AllocationStateFilter>,
}

enum AllocationStateFilter {
    Queued,
    Running,
    Finished,
    Failed,
}

impl FromStr for AllocationStateFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(AllocationStateFilter::Queued),
            "running" => Ok(AllocationStateFilter::Running),
            "finished" => Ok(AllocationStateFilter::Finished),
            "failed" => Ok(AllocationStateFilter::Failed),
            _ => Err(anyhow::anyhow!("Invalid allocation state filter")),
        }
    }
}

pub async fn command_autoalloc(
    gsettings: GlobalSettings,
    opts: AutoAllocOpts,
) -> anyhow::Result<()> {
    let connection = get_client_connection(gsettings.server_directory()).await?;
    match opts.subcmd {
        AutoAllocCommand::List => {
            print_allocation_queues(&gsettings, connection).await?;
        }
        AutoAllocCommand::Add(opts) => {
            add_queue(connection, opts).await?;
        }
        AutoAllocCommand::Events(opts) => {
            print_event_log(&gsettings, connection, opts).await?;
        }
        AutoAllocCommand::Info(opts) => {
            print_allocations(&gsettings, connection, opts).await?;
        }
        AutoAllocCommand::Remove(descriptor_id) => {
            remove_queue(connection, descriptor_id.queue_id).await?;
        }
    }
    Ok(())
}

fn args_to_params(args: SharedQueueOpts) -> AddQueueParams {
    AddQueueParams {
        workers_per_alloc: args.workers_per_alloc,
        backlog: args.backlog,
        timelimit: args.time_limit.map(|v| v.into()),
        name: args.name,
        additional_args: args.additional_args,
    }
}

async fn add_queue(mut connection: ClientConnection, opts: AddQueueOpts) -> anyhow::Result<()> {
    let AddQueueOpts { subcmd } = opts;

    let message = match subcmd {
        AddQueueCommand::Pbs(params) => FromClientMessage::AutoAlloc(AutoAllocRequest::AddQueue(
            AddQueueRequest::Pbs(args_to_params(params)),
        )),
        AddQueueCommand::Slurm(params) => FromClientMessage::AutoAlloc(AutoAllocRequest::AddQueue(
            AddQueueRequest::Slurm(args_to_params(params)),
        )),
    };

    let queue_id = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueCreated(id)) => id
    )
    .await?;

    log::info!("Allocation queue {} successfully created", queue_id);
    Ok(())
}

async fn remove_queue(
    mut connection: ClientConnection,
    descriptor_id: DescriptorId,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::RemoveQueue(descriptor_id));

    rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueRemoved(_)) => ()
    )
    .await?;

    log::info!("Allocation queue {} successfully removed", descriptor_id);
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

async fn print_event_log(
    gsettings: &GlobalSettings,
    mut connection: ClientConnection,
    opts: EventsOpts,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Events {
        descriptor: opts.queue,
    });
    let response = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Events(logs)) => logs
    )
    .await?;

    gsettings.printer().print_event_log(response);
    Ok(())
}

async fn print_allocations(
    gsettings: &GlobalSettings,
    mut connection: ClientConnection,
    opts: AllocationsOpts,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Info {
        descriptor: opts.queue,
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
                AllocationStateFilter::Queued => matches!(status, AllocationStatus::Queued),
                AllocationStateFilter::Running => {
                    matches!(status, AllocationStatus::Running { .. })
                }
                AllocationStateFilter::Finished => {
                    matches!(status, AllocationStatus::Finished { .. })
                }
                AllocationStateFilter::Failed => matches!(status, AllocationStatus::Failed { .. }),
            }
        })
    }
}
