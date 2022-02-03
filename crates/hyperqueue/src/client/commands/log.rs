use crate::client::globalsettings::GlobalSettings;
use crate::common::arraydef::IntArray;
use crate::stream::reader::logfile::LogFile;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub struct LogOpts {
    /// Path of log file
    filename: PathBuf,

    /// Operation with log file
    #[clap(subcommand)]
    command: LogCommand,
}

#[derive(Parser)]
pub struct SummaryOpts {}

#[derive(Parser)]
pub struct ShowOpts {
    /// Filter only specific channel
    #[clap(long, arg_enum)]
    pub channel: Option<Channel>,

    /// Show close message even for tasks with empty stream
    #[clap(long)]
    pub show_empty: bool,
}

#[derive(Parser)]
pub struct CatOpts {
    /// Channel name: "stdout" or "stderr"
    #[clap(arg_enum)]
    pub channel: Channel,

    /// Print only the specified task(s) output. You can use the array syntax to specify multiple tasks.
    #[clap(long)]
    pub task: Option<IntArray>,

    /// Allow unfinished channel
    #[clap(long)]
    pub allow_unfinished: bool,
}

#[derive(Parser)]
pub enum LogCommand {
    /// Prints summary of log file
    Summary(SummaryOpts),

    /// Prints content of log ordered by time
    Show(ShowOpts),

    /// Prints a raw content of one channel
    Cat(CatOpts),
}

#[derive(clap::ArgEnum, Clone)]
pub enum Channel {
    Stdout,
    Stderr,
}

pub fn command_log(gsettings: &GlobalSettings, opts: LogOpts) -> anyhow::Result<()> {
    let mut log_file = LogFile::open(&opts.filename)?;
    match opts.command {
        LogCommand::Summary(_) => {
            gsettings
                .printer()
                .print_summary(&opts.filename, log_file.summary());
        }
        LogCommand::Show(show_opts) => {
            log_file.show(&show_opts)?;
        }
        LogCommand::Cat(cat_opts) => {
            log_file.cat(&cat_opts)?;
        }
    }

    Ok(())
}
