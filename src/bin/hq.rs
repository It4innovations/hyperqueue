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
#[clap(version = "1.0")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
struct CommonOpts {
    #[clap(long)]
    rundir: Option<PathBuf>,
}

impl CommonOpts {
    fn get_rundir(&self) -> PathBuf {
        absolute_path(self.rundir.clone().unwrap_or_else(|| default_rundir()))
    }
}

#[derive(Clap)]
struct StartOpts {
    #[clap(flatten)]
    common: CommonOpts,
    #[clap(long)]
    no_auth: bool,
}

#[derive(Clap)]
struct StopOpts {
    #[clap(flatten)]
    common: CommonOpts,
}

#[derive(Clap)]
struct StatsOpts {
    #[clap(flatten)]
    common: CommonOpts,
}

#[derive(Clap)]
struct SubmitOpts {
    #[clap(flatten)]
    common: CommonOpts,
    commands: Vec<String>,
}

#[derive(Clap)]
enum SubCommand {
    Start(StartOpts),
    Stop(StopOpts),
    Stats(StatsOpts),
    Submit(SubmitOpts),
}

async fn command_start(opts: StartOpts) -> hyperqueue::Result<()> {
    let rundir_path = opts.common.get_rundir();
    init_hq_server(rundir_path, !opts.no_auth).await
}

async fn command_stop(opts: StopOpts) -> hyperqueue::Result<()> {
    stop_server(opts.common.get_rundir()).await
}

async fn command_stats(opts: StatsOpts) -> hyperqueue::Result<()> {
    get_server_stats(opts.common.get_rundir()).await
}

async fn command_submit(opts: SubmitOpts) -> hyperqueue::Result<()> {
    submit_computation(opts.common.get_rundir(), opts.commands).await
}

fn default_rundir() -> PathBuf {
    let mut home = dirs::home_dir().unwrap_or_else(|| std::env::temp_dir());
    home.push(".hq-rundir");
    home
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> hyperqueue::Result<()> {
    let opts: Opts = Opts::parse();
    setup_logging();

    let result = match opts.subcmd {
        SubCommand::Start(opts) => command_start(opts).await,
        SubCommand::Stop(opts) => command_stop(opts).await,
        SubCommand::Stats(opts) => command_stats(opts).await,
        SubCommand::Submit(opts) => command_submit(opts).await
    };
    if let Err(e) = result {
        eprintln!("{}", e);
        std::process::exit(1);
    }

    Ok(())
}
