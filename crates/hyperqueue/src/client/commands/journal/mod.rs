mod output;

use crate::client::commands::journal::output::format_event;
use crate::client::globalsettings::GlobalSettings;
use crate::common::utils::str::pluralize;
use crate::rpc_call;
use crate::server::bootstrap::get_client_session;
use crate::server::event::journal::JournalReader;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
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
    /// Export events from a journal to NDJSON (line-delimited JSON).
    /// Events will be exported to `stdout`, you can redirect it e.g. to a file.
    Export(ExportOpts),

    /// Live stream events from the server.
    Stream,

    /// Connect to a server and remove completed tasks and non-active workers from journal
    Prune,
}

#[derive(Parser)]
struct ExportOpts {
    /// Path to a journal.
    /// It had to be created with `hq server start --journal=<PATH>`.
    #[arg(value_hint = ValueHint::FilePath)]
    journal: PathBuf,
}

pub async fn command_journal(gsettings: &GlobalSettings, opts: JournalOpts) -> anyhow::Result<()> {
    match opts.command {
        JournalCommand::Export(opts) => export_json(opts),
        JournalCommand::Stream => stream_json(gsettings).await,
        JournalCommand::Prune => prune_journal(gsettings).await,
    }
}

async fn stream_json(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    let mut connection = get_client_session(gsettings.server_directory()).await?;
    connection
        .connection()
        .send(FromClientMessage::StreamEvents)
        .await?;
    let stdout = std::io::stdout();
    let stdout = stdout.lock();
    let mut stdout = BufWriter::new(stdout);
    while let Some(event) = connection.connection().receive().await {
        let event = event?;
        if let ToClientMessage::Event(e) = event {
            writeln!(stdout, "{}", format_event(e))?;
            stdout.flush()?;
        } else {
            anyhow::bail!("Invalid message receive, not ToClientMessage::Event");
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