use crate::client::globalsettings::GlobalSettings;
use crate::common::arraydef::IntArray;
use crate::stream::reader::outputlog::OutputLog;
use clap::Parser;
use std::path::PathBuf;
use tako::JobId;

#[derive(Parser)]
pub struct OutputLogOpts {
    /// Path of the log file
    path: PathBuf,

    /// Filter files for the given server instance
    #[arg(long)]
    pub server_uid: Option<String>,

    /// Operations with log files
    #[clap(subcommand)]
    command: StreamCommand,
}

#[derive(Parser)]
pub struct SummaryOpts {}

#[derive(Parser)]
pub struct ShowOpts {
    /// JobId
    #[arg(long)]
    pub job: Option<JobId>,

    /// Show only the specific channel
    #[arg(long, value_enum)]
    pub channel: Option<Channel>,
}

#[derive(Parser)]
pub struct CatOpts {
    /// JobId
    pub job: JobId,

    /// Channel name: "stdout" or "stderr"
    #[arg(value_enum)]
    pub channel: Channel,

    /// Prints only outputs of the selected tasks
    ///
    /// You can use the array syntax to specify multiple tasks.
    #[arg(long)]
    pub task: Option<IntArray>,

    /// Allow unfinished channels
    #[arg(long)]
    pub allow_unfinished: bool,
}

#[derive(Parser)]
pub struct ExportOpts {
    /// Job to export
    pub job: JobId,

    /// Exports only output of the selected tasks
    ///
    /// You can use the array syntax to specify multiple tasks.
    #[arg(long)]
    pub task: Option<IntArray>,
}

#[derive(Parser)]
pub enum StreamCommand {
    /// Prints summary of the log file
    Summary(SummaryOpts),

    /// Prints job ids in the stream
    Jobs,

    /// Prints the stream content ordered by time
    Show(ShowOpts),

    /// Prints the content of a stream's channel
    Cat(CatOpts),

    /// Exports stream into JSON
    Export(ExportOpts),
}

#[derive(clap::ValueEnum, Clone)]
pub enum Channel {
    Stdout,
    Stderr,
}

pub fn command_reader(gsettings: &GlobalSettings, opts: OutputLogOpts) -> anyhow::Result<()> {
    let mut stream_dir = OutputLog::open(&opts.path, opts.server_uid.as_deref())?;
    match opts.command {
        StreamCommand::Summary(_) => {
            gsettings
                .printer()
                .print_summary(&opts.path, stream_dir.summary());
        }
        StreamCommand::Show(show_opts) => {
            stream_dir.show(&show_opts)?;
        }
        StreamCommand::Cat(cat_opts) => {
            stream_dir.cat(&cat_opts)?;
        }
        StreamCommand::Export(export_opts) => {
            stream_dir.export(&export_opts)?;
        }
        StreamCommand::Jobs => {
            stream_dir.jobs()?;
        }
    }

    Ok(())
}
