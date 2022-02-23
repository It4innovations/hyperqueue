use crate::client::globalsettings::GlobalSettings;
use crate::client::server::client_stop_server;
use crate::common::env::is_inside_test_mode;
use crate::common::timeutils::ArgDuration;
use crate::rpc_call;
use crate::server::bootstrap::{
    get_client_connection, init_hq_server, print_server_info, ServerConfig,
};
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, StatsResponse, ToClientMessage};
use anyhow::anyhow;
use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;

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
    #[clap(long)]
    host: Option<String>,

    /// Duration after which will an idle worker automatically stop
    #[clap(long)]
    idle_timeout: Option<ArgDuration>,

    /// How often should the auto allocator perform its actions
    #[clap(long, default_value = "10m")]
    autoalloc_interval: ArgDuration,

    /// Port for client connections (used e.g. for `hq submit`)
    #[clap(long)]
    client_port: Option<u16>,

    /// Port for worker connections
    #[clap(long)]
    worker_port: Option<u16>,

    /// The maximum number of events tako server will store in memory
    #[clap(long, default_value = "1000000")]
    event_store_size: usize,

    /// Path to a log file where events will be stored.
    #[clap(long, hide(true))]
    event_log_path: Option<PathBuf>,
}

#[derive(Parser)]
struct ServerStopOpts {}

#[derive(Parser)]
struct ServerInfoOpts {
    /// Show internal internal state of server
    #[clap(long)]
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
    let autoalloc_interval = opts.autoalloc_interval.unpack();
    if !is_inside_test_mode() && autoalloc_interval < Duration::from_secs(60) {
        return Err(anyhow!(
            "Autoalloc interval cannot be shorter than one minute"
        ));
    }

    let server_cfg = ServerConfig {
        host: opts
            .host
            .unwrap_or_else(|| gethostname::gethostname().into_string().unwrap()),
        idle_timeout: opts.idle_timeout.map(|x| x.unpack()),
        autoalloc_interval,
        client_port: opts.client_port,
        worker_port: opts.worker_port,
        event_buffer_size: opts.event_store_size,
        event_log_path: opts.event_log_path,
    };

    init_hq_server(gsettings, server_cfg).await
}

async fn stop_server(gsettings: &GlobalSettings, _opts: ServerStopOpts) -> anyhow::Result<()> {
    let mut connection = get_client_connection(gsettings.server_directory()).await?;
    client_stop_server(&mut connection).await?;
    Ok(())
}

async fn command_server_info(
    gsettings: &GlobalSettings,
    opts: ServerInfoOpts,
) -> anyhow::Result<()> {
    if opts.stats {
        let mut connection = get_client_connection(gsettings.server_directory()).await?;
        print_server_stats(gsettings, &mut connection).await
    } else {
        print_server_info(gsettings).await
    }
}

async fn print_server_stats(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
) -> anyhow::Result<()> {
    let response: StatsResponse = rpc_call!(
        connection,
        FromClientMessage::Stats,
        ToClientMessage::StatsResponse(r) => r
    )
    .await?;

    gsettings.printer().print_server_stats(response);
    Ok(())
}
