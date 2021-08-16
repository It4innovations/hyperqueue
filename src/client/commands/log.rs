use crate::client::globalsettings::GlobalSettings;
use crate::common::arraydef::IntArray;
use crate::common::format::human_size;
use crate::stream::reader::logfile::{LogFile, Summary};
use clap::Clap;
use cli_table::{print_stdout, Cell, Style, Table};
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct LogOpts {
    /// Path of log file
    filename: PathBuf,

    /// Operation with log file
    #[clap(subcommand)]
    command: LogCommand,
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct SummaryOpts {}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct ShowOpts {
    /// Filter only specific channel
    #[clap(long)]
    pub channel: Option<Channel>,

    /// Show close message even for tasks with empty stream
    #[clap(long)]
    pub show_empty: bool,
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct CatOpts {
    /// Channel name: "stdout" or "stderr"
    pub channel: Channel,

    /// Print only the specified task(s) output. You can use the array syntax to specify multiple tasks.
    #[clap(long)]
    pub task: Option<IntArray>,

    /// Allow unfinished channel
    #[clap(long)]
    pub allow_unfinished: bool,
}

#[derive(Clap)]
pub enum LogCommand {
    /// Prints summary of log file
    Summary(SummaryOpts),

    /// Prints content of log ordered by time
    Show(ShowOpts),

    /// Prints a raw content of one channel
    Cat(CatOpts),
}

#[derive(Clap)]
pub enum Channel {
    Stdout,
    Stderr,
}

impl FromStr for Channel {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "stdout" => Ok(Channel::Stdout),
            "stderr" => Ok(Channel::Stderr),
            _ => Err("Invalid channel"),
        }
    }
}

fn print_summary(gsettings: &GlobalSettings, filename: &Path, summary: Summary) {
    let rows = vec![
        vec!["Filename".cell().bold(true), filename.display().cell()],
        vec![
            "Tasks".cell().bold(true),
            summary.n_tasks.to_string().cell(),
        ],
        vec![
            "Opened streams".cell().bold(true),
            summary.n_opened.to_string().cell(),
        ],
        vec![
            "Stdout/stderr size".cell().bold(true),
            format!(
                "{} / {}",
                human_size(summary.stdout_size),
                human_size(summary.stderr_size)
            )
            .cell(),
        ],
        vec![
            "Superseded streams".cell().bold(true),
            summary.n_superseded.to_string().cell(),
        ],
        vec![
            "Superseded stdout/stderr size".cell().bold(true),
            format!(
                "{} / {}",
                human_size(summary.superseded_stdout_size),
                human_size(summary.superseded_stderr_size)
            )
            .cell(),
        ],
    ];
    let table = rows.table().color_choice(gsettings.color_policy());
    assert!(print_stdout(table).is_ok());
}

pub fn command_log(gsettings: GlobalSettings, opts: LogOpts) -> anyhow::Result<()> {
    let mut log_file = LogFile::open(&opts.filename)?;
    match opts.command {
        LogCommand::Summary(_) => {
            print_summary(&gsettings, &opts.filename, log_file.summary());
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
