use crate::client::globalsettings::GlobalSettings;
use crate::common::timeutils::ArgDuration;
use crate::rpc_call;
use crate::server::autoalloc::{AllocationEvent, AllocationEventHolder};
use crate::server::bootstrap::get_client_connection;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    AddQueueParams, AddQueueRequest, AutoAllocRequest, AutoAllocResponse, FromClientMessage,
    ToClientMessage,
};
use clap::Clap;
use cli_table::{print_stdout, Cell, CellStruct, Color, Style, Table};

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
#[clap(setting = clap::AppSettings::Hidden)] // TODO: remove once autoalloc is ready to be used
pub struct AutoAllocOpts {
    #[clap(subcommand)]
    subcmd: AutoAllocCommand,
}

#[derive(Clap)]
enum AutoAllocCommand {
    /// Display information about autoalloc state
    Info,
    /// Display event log for a specified allocation queue
    Events(EventsOpts),
    /// Add new allocation queue
    Add(AddQueueOpts),
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct AddQueueOpts {
    /// Name of the allocation queue
    name: String,

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
    /// PBS queue into which the allocations will be queued
    queue: String,

    /// How many workers should be kept active in this queue
    target_worker_count: u32,

    /// Maximum timelimit of allocated jobs
    #[clap(long)]
    timelimit: Option<ArgDuration>,

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

pub async fn command_autoalloc(
    gsettings: GlobalSettings,
    opts: AutoAllocOpts,
) -> anyhow::Result<()> {
    let connection = get_client_connection(gsettings.server_directory()).await?;
    match opts.subcmd {
        AutoAllocCommand::Info => {
            print_info(gsettings, connection).await?;
        }
        AutoAllocCommand::Add(opts) => {
            add_queue(connection, opts).await?;
        }
        AutoAllocCommand::Events(opts) => {
            print_event_log(gsettings, connection, opts).await?;
        }
    }
    Ok(())
}

async fn add_queue(mut connection: ClientConnection, opts: AddQueueOpts) -> anyhow::Result<()> {
    let AddQueueOpts { name, subcmd } = opts;

    let message = match subcmd {
        AddQueueCommand::Pbs(params) => FromClientMessage::AutoAlloc(AutoAllocRequest::AddQueue(
            AddQueueRequest::Pbs(AddQueueParams {
                name: name.clone(),
                max_workers_per_alloc: params.max_workers_per_alloc,
                target_worker_count: params.target_worker_count,
                queue: params.queue,
                timelimit: params.timelimit.map(|v| v.into_duration()),
            }),
        )),
    };

    rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Ok) => ()
    )
    .await?;

    log::info!("Allocation queue {} was successfully created", name);
    Ok(())
}

async fn print_info(
    gsettings: GlobalSettings,
    mut connection: ClientConnection,
) -> anyhow::Result<()> {
    let message = FromClientMessage::AutoAlloc(AutoAllocRequest::Info);
    let response = rpc_call!(connection, message,
        ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(r)) => r
    )
    .await?;

    let rows = vec![vec![
        "Refresh interval".cell().bold(true),
        humantime::format_duration(response.refresh_interval).cell(),
    ]];
    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());

    let mut rows = vec![vec![
        "Name".cell().bold(true),
        "Target worker count".cell().bold(true),
        "Max workers per allocation".cell().bold(true),
        "Queue".cell().bold(true),
        "Timelimit".cell().bold(true),
    ]];

    let mut descriptors: Vec<_> = response.descriptors.into_iter().collect();
    descriptors.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    rows.extend(descriptors.into_iter().map(|(name, data)| {
        vec![
            name.cell(),
            data.info.target_worker_count().cell(),
            data.info.max_workers_per_alloc().cell(),
            data.info.queue().cell(),
            data.info
                .timelimit()
                .map(|d| humantime::format_duration(d).to_string())
                .unwrap_or_else(|| "N/A".to_string())
                .cell(),
        ]
    }));

    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
    Ok(())
}

async fn print_event_log(
    gsettings: GlobalSettings,
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

    let event_name = |event: &AllocationEventHolder| -> CellStruct {
        match event.event {
            AllocationEvent::AllocationQueued(..) => "Allocation queued"
                .cell()
                .foreground_color(Some(Color::Yellow)),
            AllocationEvent::AllocationStarted(..) => "Allocation started"
                .cell()
                .foreground_color(Some(Color::Green)),
            AllocationEvent::AllocationFinished(..) => "Allocation finished"
                .cell()
                .foreground_color(Some(Color::Blue)),
            AllocationEvent::AllocationFailed(..) => "Allocation failed"
                .cell()
                .foreground_color(Some(Color::Red)),
            AllocationEvent::AllocationDisappeared(..) => "Allocation disappeared"
                .cell()
                .foreground_color(Some(Color::Red)),
            AllocationEvent::QueueFail { .. } => "Allocation submission failed"
                .cell()
                .foreground_color(Some(Color::Red)),
            AllocationEvent::StatusFail { .. } => "Allocation status check failed"
                .cell()
                .foreground_color(Some(Color::Red)),
        }
    };

    let event_message = |event: &AllocationEventHolder| -> CellStruct {
        match &event.event {
            AllocationEvent::AllocationQueued(id)
            | AllocationEvent::AllocationStarted(id)
            | AllocationEvent::AllocationFailed(id)
            | AllocationEvent::AllocationFinished(id)
            | AllocationEvent::AllocationDisappeared(id) => id.cell(),
            AllocationEvent::QueueFail { error } | AllocationEvent::StatusFail { error } => {
                error.cell()
            }
        }
    };

    let mut rows = vec![vec![
        "Event".cell().bold(true),
        "Time".cell().bold(true),
        "Message".cell().bold(true),
    ]];
    rows.extend(response.into_iter().map(|event| {
        vec![
            event_name(&event),
            humantime::format_rfc3339_seconds(event.date).cell(),
            event_message(&event),
        ]
    }));

    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
    Ok(())
}
