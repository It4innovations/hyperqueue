mod output;

use crate::client::commands::event::output::format_event;
use crate::common::strutils::pluralize;
use crate::event::log::EventLogReader;
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
}

#[derive(Parser)]
struct ExportOpts {
    /// Path to a file containing the event log.
    /// The file had to be created with `hq server start --event-log-path=<PATH>`.
    #[clap(value_hint = ValueHint::FilePath)]
    logfile: PathBuf,
}

pub fn command_event_log(opts: EventLogOpts) -> anyhow::Result<()> {
    match opts.command {
        EventCommand::Export(opts) => export_json(opts),
    }
}

fn export_json(opts: ExportOpts) -> anyhow::Result<()> {
    let file = EventLogReader::open(&opts.logfile).map_err(|error| {
        anyhow!(
            "Cannot open event log file at `{}`: {error:?}",
            opts.logfile.display()
        )
    })?;

    let stdout = std::io::stdout();
    let stdout = stdout.lock();
    let mut stdout = BufWriter::new(stdout);

    let mut count = 0;
    for event in file {
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
