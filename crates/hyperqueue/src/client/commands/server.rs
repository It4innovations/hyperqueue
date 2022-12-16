use crate::client::globalsettings::GlobalSettings;
use crate::client::server::client_stop_server;
use crate::common::utils::network::get_hostname;
use crate::common::utils::time::ArgDuration;
use crate::rpc_call;
use crate::server::bootstrap::{
    get_client_session, init_hq_server, print_server_info, ServerConfig,
};
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{FromClientMessage, StatsResponse, ToClientMessage};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
pub struct ServerOpts {
    #[clap(subcommand)]
    subcmd: ServerCommand,
}

#[derive(Parser)]
enum ServerCommand {
    /// Start the HyperQueue server
    Start(ServerStartOpts),
    /// Stop the HyperQueue server, if it is running
    Stop(ServerStopOpts),
    /// Show info of running HyperQueue server
    Info(ServerInfoOpts),
}

#[derive(Parser)]
struct ServerStartOpts {
    /// Hostname/IP of the machine under which is visible to others, default: hostname
    #[arg(long)]
    host: Option<String>,

    /// Duration after which will an idle worker automatically stop
    #[arg(long)]
    idle_timeout: Option<ArgDuration>,

    /// Port for client connections (used e.g. for `hq submit`)
    #[arg(long)]
    client_port: Option<u16>,

    /// Port for worker connections
    #[arg(long)]
    worker_port: Option<u16>,

    /// The maximum number of events tako server will store in memory
    #[arg(long, default_value_t = 1000000)]
    event_store_size: usize,

    /// Path to a log file where events will be stored.
    #[arg(long, hide(true))]
    event_log_path: Option<PathBuf>,
}

#[derive(Parser)]
struct ServerStopOpts {}

#[derive(Parser)]
struct ServerInfoOpts {
    /// Show internal internal state of server
    #[arg(long)]
    stats: bool,
}

pub async fn command_server(gsettings: &GlobalSettings, opts: ServerOpts) -> anyhow::Result<()> {
    match opts.subcmd {
        ServerCommand::Start(opts) => start_server(gsettings, opts).await,
        ServerCommand::Stop(opts) => stop_server(gsettings, opts).await,
        ServerCommand::Info(opts) => command_server_info(gsettings, opts).await,
    }
}

async fn start_server(gsettings: &GlobalSettings, opts: ServerStartOpts) -> anyhow::Result<()> {
    let server_cfg = ServerConfig {
        host: get_hostname(opts.host),
        idle_timeout: opts.idle_timeout.map(|x| x.unpack()),
        client_port: opts.client_port,
        worker_port: opts.worker_port,
        event_buffer_size: opts.event_store_size,
        event_log_path: opts.event_log_path,
    };

    init_hq_server(gsettings, server_cfg).await
}

async fn stop_server(gsettings: &GlobalSettings, _opts: ServerStopOpts) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    client_stop_server(session.connection()).await?;
    Ok(())
}

async fn command_server_info(
    gsettings: &GlobalSettings,
    opts: ServerInfoOpts,
) -> anyhow::Result<()> {
    if opts.stats {
        let mut session = get_client_session(gsettings.server_directory()).await?;
        print_server_stats(gsettings, &mut session).await
    } else {
        print_server_info(gsettings).await
    }
}

async fn print_server_stats(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
) -> anyhow::Result<()> {
    let response: StatsResponse = rpc_call!(
        session.connection(),
        FromClientMessage::Stats,
        ToClientMessage::StatsResponse(r) => r
    )
    .await?;

    gsettings.printer().print_server_stats(response);
    Ok(())
}
