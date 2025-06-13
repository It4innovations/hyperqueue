mod output;

use crate::client::commands::journal::output::format_event;
use crate::client::globalsettings::GlobalSettings;
use crate::common::utils::str::pluralize;
use crate::rpc_call;
use crate::server::bootstrap::get_client_session;
use crate::server::event::journal::JournalReader;
use crate::transfer::messages::{FromClientMessage, StreamEvents, ToClientMessage};
use anyhow::anyhow;
use clap::{Parser, ValueHint};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser)]
pub struct JournalOpts {
    /// Manage events and journal data.
    #[clap(subcommand)]
    command: JournalCommand,
}

#[derive(Parser)]
enum JournalCommand {
    /// Export events from a journal file
    ///
    /// Events are exported as NDJSON on `stdout`.
    Export(ExportOpts),

    /// Replays all old events, then streams new events
    ///
    /// The command blocks and waits for new live events until
    /// the server is not stopped.
    ///
    /// Events are exported as NDJSON on `stdout`.
    Stream,

    /// Replays old events, then terminate
    ///
    /// Events are exported as NDJSON on `stdout`.
    Replay,

    /// Prune a journal of a running server
    ///
    /// Connects to a server and removes completed tasks and non-active workers from the journal.
    Prune,

    /// Forces to flush its journal of a running server
    Flush,
}

#[derive(Parser)]
struct ExportOpts {
    /// Path to a journal
    ///
    /// It had to be created with `hq server start --journal=<PATH>`.
    #[arg(value_hint = ValueHint::FilePath)]
    journal: PathBuf,
}

pub async fn command_journal(gsettings: &GlobalSettings, opts: JournalOpts) -> anyhow::Result<()> {
    match opts.command {
        JournalCommand::Export(opts) => export_json(opts),
        JournalCommand::Replay => stream_json(gsettings, false).await,
        JournalCommand::Stream => stream_json(gsettings, true).await,
        JournalCommand::Prune => prune_journal(gsettings).await,
        JournalCommand::Flush => flush_journal(gsettings).await,
    }
}

async fn stream_json(gsettings: &GlobalSettings, live_events: bool) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    connection
        .connection()
        .send(FromClientMessage::StreamEvents(StreamEvents {
            live_events,
            enable_worker_overviews: false,
        }))
        .await?;
    let stdout = std::io::stdout();
    let stdout = stdout.lock();
    let mut stdout = BufWriter::new(stdout);
    while let Some(event) = connection.connection().receive().await {
        let event = event?;
        match event {
            ToClientMessage::Event(e) => {
                writeln!(stdout, "{}", format_event(e))?;
                stdout.flush()?;
            }
            ToClientMessage::EventLiveBoundary => { /* Do nothing */ }
            _ => {
                anyhow::bail!("Invalid message receive, not ToClientMessage::Event");
            }
        }
    }
    Ok(())
}

fn export_json(opts: ExportOpts) -> anyhow::Result<()> {
    let mut file = JournalReader::open(&opts.journal).map_err(|error| {
        anyhow!(
            "Cannot open event log file at `{}`: {error:?}",
            opts.journal.display()
        )
    })?;

    let stdout = std::io::stdout();
    let stdout = stdout.lock();
    let mut stdout = BufWriter::new(stdout);

    let mut count = 0;
    for event in &mut file {
        match event {
            Ok(event) => {
                writeln!(stdout, "{}", format_event(event))?;
                count += 1;
            }
            Err(error) => {
                log::error!(
                    "Encountered an error while reading event log file: {error:?}.\n
The file might have been incomplete."
                )
            }
        }
    }

    log::info!("Outputted {count} {}", pluralize("event", count));

    stdout.flush()?;
    Ok(())
}

async fn prune_journal(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    rpc_call!(
        session.connection(),
        FromClientMessage::PruneJournal,
        ToClientMessage::Finished
    )
    .await?;
    Ok(())
}

async fn flush_journal(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    rpc_call!(
        session.connection(),
        FromClientMessage::FlushJournal,
        ToClientMessage::Finished
    )
    .await?;
    Ok(())
}
