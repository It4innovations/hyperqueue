mod output;

use crate::client::commands::event::output::format_event;
use crate::client::globalsettings::GlobalSettings;
use crate::common::error::HqError;
use crate::common::utils::str::pluralize;
use crate::rpc_call;
use crate::server::bootstrap::get_client_session;
use crate::server::event::log::EventLogReader;
use crate::transfer::messages::{FromClientMessage, IdSelector, JobInfoRequest, ToClientMessage};
use anyhow::anyhow;
use clap::{Parser, ValueHint};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser)]
pub struct EventLogOpts {
    /// Manage event log files.
    #[clap(subcommand)]
    command: EventCommand,
}

#[derive(Parser)]
enum EventCommand {
    /// Export events from a log file to NDJSON (line-delimited JSON).
    /// Events will be exported to `stdout`, you can redirect it e.g. to a file.
    Export(ExportOpts),

    /// Live stream events from server
    Stream,
}

#[derive(Parser)]
struct ExportOpts {
    /// Path to a file containing the event log.
    /// The file had to be created with `hq server start --event-log-path=<PATH>`.
    #[arg(value_hint = ValueHint::FilePath)]
    logfile: PathBuf,
}

pub async fn command_event_log(
    gsettings: &GlobalSettings,
    opts: EventLogOpts,
) -> anyhow::Result<()> {
    match opts.command {
        EventCommand::Export(opts) => export_json(opts),
        EventCommand::Stream => stream_json(gsettings).await,
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
    let mut file = EventLogReader::open(&opts.logfile).map_err(|error| {
        anyhow!(
            "Cannot open event log file at `{}`: {error:?}",
            opts.logfile.display()
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
