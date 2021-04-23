use std::path::PathBuf;

use clap::Clap;

use hyperqueue::client::commands::stats::get_server_stats;
use hyperqueue::client::commands::stop::stop_server;
use hyperqueue::client::commands::submit::submit_computation;
use hyperqueue::common::setup::setup_logging;
use hyperqueue::server::bootstrap::init_hq_server;
use hyperqueue::utils::absolute_path;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub type Connection = tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LengthDelimitedCodec>;

#[derive(Clap)]
struct CommonOpts {
    #[clap(long)]
    rundir: Option<PathBuf>,
}


#[derive(Clap)]
#[clap(version = "0.1")]
#[clap(setting = clap::AppSettings::ColoredHelp)]
struct Opts {
    #[clap(flatten)]
    common: CommonOpts,

    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl CommonOpts {
    fn get_rundir(&self) -> PathBuf {
        absolute_path(self.rundir.clone().unwrap_or_else(default_rundir))
    }
}

#[derive(Clap)]
struct StartOpts {
}

#[derive(Clap)]
struct StopOpts {
}

#[derive(Clap)]
struct StatsOpts {
}

#[derive(Clap)]
struct SubmitOpts {
    commands: Vec<String>,
}

#[derive(Clap)]
enum SubCommand {
    Server(ServerOpts),
    Worker(WorkerOpts),
    Stats(StatsOpts),
    Submit(SubmitOpts),
}

#[derive(Clap)]
struct ServerOpts {

    #[clap(subcommand)]
    subcmd: ServerCommand,
}

#[derive(Clap)]
enum ServerCommand {
    Start(StartOpts),
    Stop(StopOpts),
}

#[derive(Clap)]
struct WorkerOpts {

}


async fn command_start(common: CommonOpts, opts: StartOpts) -> hyperqueue::Result<()> {
    let rundir_path = common.get_rundir();
    init_hq_server(rundir_path).await
}

async fn command_stop(common: CommonOpts, opts: StopOpts) -> hyperqueue::Result<()> {
    stop_server(common.get_rundir()).await
}

async fn command_stats(common: CommonOpts, opts: StatsOpts) -> hyperqueue::Result<()> {
    get_server_stats(common.get_rundir()).await
}

async fn command_submit(common: CommonOpts, opts: SubmitOpts) -> hyperqueue::Result<()> {
    submit_computation(common.get_rundir(), opts.commands).await
}

fn default_rundir() -> PathBuf {
    let mut home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
    home.push(".hq-rundir");
    home
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    let top_opts: Opts = Opts::parse();
    setup_logging();

    let result = match top_opts.subcmd {
        SubCommand::Server(ServerOpts { subcmd: ServerCommand::Start(opts) }) => command_start(top_opts.common, opts).await,
        SubCommand::Server(ServerOpts { subcmd: ServerCommand::Stop(opts) }) => command_stop(top_opts.common, opts).await,
        SubCommand::Worker(opts) => { todo!() },
        SubCommand::Stats(opts) => command_stats(top_opts.common, opts).await,
        SubCommand::Submit(opts) => command_submit(top_opts.common, opts).await
    };
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }

    Ok(())
}
