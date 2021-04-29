use std::path::PathBuf;

use clap::Clap;

use cli_table::ColorChoice;
use hyperqueue::client::commands::jobs::{get_job_detail, get_job_list};
use hyperqueue::client::commands::stop::stop_server;
use hyperqueue::client::commands::submit::submit_computation;
use hyperqueue::client::commands::worker::{get_worker_list, stop_worker};
use hyperqueue::client::globalsettings::GlobalSettings;
use hyperqueue::client::utils::OutputStyle;
use hyperqueue::common::error::error;
use hyperqueue::common::fsutils::absolute_path;
use hyperqueue::common::setup::setup_logging;
use hyperqueue::server::bootstrap::{get_client_connection, init_hq_server};
use hyperqueue::server::job::JobId;
use hyperqueue::worker::start::{start_hq_worker, WorkerStartOpts};
use hyperqueue::WorkerId;
use std::str::FromStr;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub type Connection =
    tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LengthDelimitedCodec>;

#[derive(Clap)]
struct CommonOpts {
    #[clap(long)]
    server_dir: Option<PathBuf>,

    #[clap(long, default_value = "auto")]
    colors: ColorPolicy,

    #[clap(long)]
    plain_output: bool,
}

#[derive(Clap)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(setting = clap::AppSettings::ColoredHelp)]
struct Opts {
    #[clap(flatten)]
    common: CommonOpts,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
struct ServerStartOpts {}

#[derive(Clap)]
struct ServerStopOpts {}

#[derive(Clap)]
struct JobListOpts {}

#[derive(Clap)]
struct JobDetailOpts {
    job_id: JobId,
}

#[derive(Clap)]
struct SubmitOpts {
    commands: Vec<String>,
}

#[derive(Clap)]
enum SubCommand {
    Server(ServerOpts),
    Jobs(JobListOpts),
    Job(JobDetailOpts),
    Submit(SubmitOpts),
    Worker(WorkerOpts),
}

#[derive(Clap)]
struct ServerOpts {
    #[clap(subcommand)]
    subcmd: ServerCommand,
}

#[derive(Clap)]
enum ServerCommand {
    Start(ServerStartOpts),
    Stop(ServerStopOpts),
}

#[derive(Clap)]
struct WorkersOpts {}

#[derive(Clap)]
struct WorkerOpts {
    #[clap(subcommand)]
    subcmd: WorkerCommand,
}

#[derive(Clap)]
struct WorkerStopOpts {
    worker_id: WorkerId,
}

#[derive(Clap)]
struct WorkerListOpts {}

#[derive(Clap)]
struct WorkerInfoOpts {
    worker_id: WorkerId,
}

#[derive(Clap)]
enum WorkerCommand {
    Start(WorkerStartOpts),
    Stop(WorkerStopOpts),
    List(WorkerListOpts),
    Info(WorkerInfoOpts),
}

async fn command_server_start(
    gsettings: GlobalSettings,
    opts: ServerStartOpts,
) -> hyperqueue::Result<()> {
    init_hq_server(&gsettings).await
}

async fn command_server_stop(
    gsettings: GlobalSettings,
    opts: ServerStopOpts,
) -> hyperqueue::Result<()> {
    let mut connection = get_client_connection(&gsettings.server_directory()).await?;
    stop_server(&mut connection).await
}

async fn command_job_list(gsettings: GlobalSettings, opts: JobListOpts) -> hyperqueue::Result<()> {
    let mut connection = get_client_connection(&gsettings.server_directory()).await?;
    get_job_list(&gsettings, &mut connection).await
}

async fn command_job_detail(
    gsettings: GlobalSettings,
    opts: JobDetailOpts,
) -> hyperqueue::Result<()> {
    let mut connection = get_client_connection(&gsettings.server_directory()).await?;
    get_job_detail(&gsettings, &mut connection, opts.job_id).await
}

async fn command_submit(gsettings: GlobalSettings, opts: SubmitOpts) -> hyperqueue::Result<()> {
    let mut connection = get_client_connection(&gsettings.server_directory()).await?;
    submit_computation(&gsettings, &mut connection, opts.commands).await
}

async fn command_worker_start(
    gsettings: GlobalSettings,
    opts: WorkerStartOpts,
) -> hyperqueue::Result<()> {
    start_hq_worker(&gsettings, opts).await
}

async fn command_worker_stop(
    gsettings: GlobalSettings,
    opts: WorkerStopOpts,
) -> hyperqueue::Result<()> {
    let mut connection = get_client_connection(&gsettings.server_directory()).await?;
    stop_worker(&mut connection, opts.worker_id).await
}

fn default_server_directory_path() -> PathBuf {
    let mut home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
    home.push(".hq-server");
    home
}

async fn command_worker_list(
    gsettings: GlobalSettings,
    opts: WorkerListOpts,
) -> hyperqueue::Result<()> {
    let mut connection = get_client_connection(&gsettings.server_directory()).await?;
    get_worker_list(&mut connection, &gsettings).await
}

pub enum ColorPolicy {
    Auto,
    Always,
    Never,
}

impl FromStr for ColorPolicy {
    type Err = hyperqueue::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "auto" => Self::Auto,
            "always" => Self::Always,
            "never" => Self::Never,
            _ => return error("Invalid color policy".to_string()),
        })
    }
}

fn make_global_settings(opts: CommonOpts) -> GlobalSettings {
    let color_policy = match opts.colors {
        ColorPolicy::Always => ColorChoice::AlwaysAnsi,
        ColorPolicy::Auto => {
            if atty::is(atty::Stream::Stdout) {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
        ColorPolicy::Never => ColorChoice::Never,
    };

    let server_dir = absolute_path(
        opts.server_dir
            .clone()
            .unwrap_or_else(default_server_directory_path),
    );

    GlobalSettings::new(server_dir, color_policy)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    let top_opts: Opts = Opts::parse();
    setup_logging();

    let gsettings = make_global_settings(top_opts.common);

    let result = match top_opts.subcmd {
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Start(opts),
        }) => command_server_start(gsettings, opts).await,
        SubCommand::Server(ServerOpts {
            subcmd: ServerCommand::Stop(opts),
        }) => command_server_stop(gsettings, opts).await,

        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Start(opts),
        }) => command_worker_start(gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Stop(opts),
        }) => command_worker_stop(gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::List(opts),
        }) => command_worker_list(gsettings, opts).await,
        SubCommand::Worker(WorkerOpts {
            subcmd: WorkerCommand::Info(_),
        }) => {
            todo!()
        }

        SubCommand::Jobs(opts) => command_job_list(gsettings, opts).await,
        SubCommand::Job(opts) => command_job_detail(gsettings, opts).await,
        SubCommand::Submit(opts) => command_submit(gsettings, opts).await,
    };
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }

    Ok(())
}
