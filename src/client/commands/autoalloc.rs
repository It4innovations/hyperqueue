use crate::client::globalsettings::GlobalSettings;
use crate::common::timeutils::ArgDuration;
use crate::rpc_call;
use crate::server::autoalloc::{Allocation, AllocationStatus};
use crate::server::bootstrap::get_client_connection;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    AddQueueParams, AddQueueRequest, AutoAllocRequest, AutoAllocResponse, FromClientMessage,
    ToClientMessage,
};
use clap::Clap;
use std::str::FromStr;

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(setting = clap::AppSettings::Hidden)] // TODO: remove once autoalloc is ready to be used
pub struct AutoAllocOpts {
    #[clap(subcommand)]
    subcmd: AutoAllocCommand,
}

#[derive(Clap)]
enum AutoAllocCommand {
    /// Displays allocation queues
    List,
    /// Display event log for a specified allocation queue
    Events(EventsOpts),
    /// Display information about allocations
    Allocations(AllocationsOpts),
    /// Add new allocation queue
    Add(AddQueueOpts),
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct AddQueueOpts {
    #[clap(subcommand)]
    subcmd: AddQueueCommand,
}

#[derive(Clap)]
pub enum AddQueueCommand {
    /// Create a PBS allocation queue
    Pbs(AddPbsQueueOpts),
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct AddPbsQueueOpts {
    /// Name of the allocation queue
    #[clap(long)]
    name: String,

    /// PBS queue into which the allocations will be queued
    #[clap(long)]
    queue: String,

    /// How many workers should be kept active in this queue
    #[clap(long("workers"))]
    target_worker_count: u32,

    /// Time limit (walltime) of PBS allocations
    #[clap(long)]
    time_limit: Option<ArgDuration>,

    /// How many workers at most can be allocated in a single allocation
    #[clap(long, default_value = "1")]
    max_workers_per_alloc: u32,
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct EventsOpts {
    /// Name of the allocation queue
    name: String,
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct AllocationsOpts {
    /// Name of the allocation queue
    name: String,

    /// Display only allocations with the given state
    #[clap(long)]
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
        AutoAllocCommand::Allocations(opts) => {
            print_allocations(&gsettings, connection, opts).await?;
        }
    }
    Ok(())
}

async fn add_queue(mut connection: ClientConnection, opts: AddQueueOpts) -> anyhow::Result<()> {
    let AddQueueOpts { subcmd } = opts;

    let (message, name) = match subcmd {
        AddQueueCommand::Pbs(params) => {
            let name = params.name;
            let msg = FromClientMessage::AutoAlloc(AutoAllocRequest::AddQueue(
                AddQueueRequest::Pbs(AddQueueParams {
                    name: name.clone(),
                    max_workers_per_alloc: params.max_workers_per_alloc,
                    target_worker_count: params.target_worker_count,
                    queue: params.queue,
                    timelimit: params.time_limit.map(|v| v.into_duration()),
                }),
            ));
            (msg, name)
        }
    };

    rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Ok) => ()
    )
    .await?;

    log::info!("Allocation queue {} was successfully created", name);
    Ok(())
}

async fn print_allocation_queues(
    gsettings: &GlobalSettings,
    mut connection: ClientConnection,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Info);
    let response = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(r)) => r
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
        descriptor: opts.name,
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
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Allocations {
        descriptor: opts.name,
    });
    let mut allocations = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Allocations(allocs)) => allocs
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
