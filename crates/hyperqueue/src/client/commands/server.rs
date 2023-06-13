use crate::client::globalsettings::GlobalSettings;
use crate::client::server::client_stop_server;
use crate::common::serverdir::{store_access_record, FullAccessRecord};
use crate::common::utils::network::get_hostname;
use crate::common::utils::time::parse_human_time;
use crate::rpc_call;
use crate::server::bootstrap::{
    generate_server_uid, get_client_session, init_hq_server, ServerConfig,
};
use crate::transfer::auth::generate_key;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{FromClientMessage, StatsResponse, ToClientMessage};
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
pub struct ServerOpts {
    #[clap(subcommand)]
    subcmd: ServerCommand,
}

#[derive(Parser)]
pub struct GenerateAccessOpts {
    /// Target filename of the full access file that will be generated
    access_file: PathBuf,

    /// Target filename of the access file for worker that will be generated
    #[arg(long)]
    client_file: Option<PathBuf>,

    /// Target filename of the access file for worker that will be generated
    #[arg(long)]
    worker_file: Option<PathBuf>,

    /// Override target host name, otherwise local hostname is used
    #[arg(long)]
    host: Option<String>,

    /// Port for connecting client
    #[arg(long)]
    client_port: u16,

    /// Port for connecting workers
    #[arg(long)]
    worker_port: u16,
}

#[derive(Parser)]
enum ServerCommand {
    /// Start the HyperQueue server
    Start(ServerStartOpts),
    /// Stop the HyperQueue server, if it is running
    Stop(ServerStopOpts),
    /// Show info of running HyperQueue server
    Info(ServerInfoOpts),
    /// Generate access file without starting server
    GenerateAccess(GenerateAccessOpts),
}

#[derive(Parser)]
struct ServerStartOpts {
    /// Hostname/IP of the machine under which is visible to others, default: hostname
    #[arg(long)]
    host: Option<String>,

    /// Duration after which will an idle worker automatically stop
    #[arg(long, value_parser = parse_human_time)]
    idle_timeout: Option<Duration>,

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

    /// Path to access file that is used for configuration of secret keys and ports
    #[arg(long, hide(true))]
    access_file: Option<PathBuf>,
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
        ServerCommand::GenerateAccess(opts) => command_server_generate_access(gsettings, opts),
    }
}

async fn start_server(gsettings: &GlobalSettings, opts: ServerStartOpts) -> anyhow::Result<()> {
    let access_file: Option<FullAccessRecord> = if let Some(path) = opts.access_file {
        let file = File::open(path)?;
        Some(serde_json::from_reader(file)?)
    } else {
        None
    };

    let host = opts
        .host
        .or_else(|| access_file.as_ref().map(|a| a.worker_host().to_string()))
        .unwrap_or_else(|| get_hostname(None));

    let worker_port = opts
        .worker_port
        .or(access_file.as_ref().map(|a| a.worker_port()));
    let client_port = opts
        .client_port
        .or(access_file.as_ref().map(|a| a.client_port()));

    let server_cfg = ServerConfig {
        host,
        idle_timeout: opts.idle_timeout,
        client_port,
        worker_port,
        event_buffer_size: opts.event_store_size,
        event_log_path: opts.event_log_path,
        worker_secret_key: access_file.as_ref().map(|a| a.worker_key().clone()),
        client_secret_key: access_file.as_ref().map(|a| a.client_key().clone()),
        server_uid: access_file.as_ref().map(|a| a.server_uid().to_string()),
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

pub async fn print_server_info(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    let mut session = get_client_session(gsettings.server_directory()).await?;
    let response = crate::rpc_call!(
        session.connection(),
        FromClientMessage::ServerInfo,
        ToClientMessage::ServerInfo(r) => r
    )
    .await?;

    gsettings
        .printer()
        // We are not using gsettings.server_directory() as it is local one
        .print_server_description(None, &response);
    Ok(())
}

fn command_server_generate_access(
    _gsettings: &GlobalSettings,
    opts: GenerateAccessOpts,
) -> anyhow::Result<()> {
    let server_uid = generate_server_uid();
    let worker_key = Arc::new(generate_key());
    let client_key = Arc::new(generate_key());
    let host = get_hostname(opts.host);

    let record = FullAccessRecord::new(
        host,
        server_uid,
        opts.client_port,
        opts.worker_port,
        client_key,
        worker_key,
    );

    store_access_record(&record, &opts.access_file)?;

    let (client_record, worker_record) = record.split();
    if let Some(path) = opts.client_file {
        store_access_record(&client_record, path)?;
    }
    if let Some(path) = opts.worker_file {
        store_access_record(&worker_record, path)?;
    }
    Ok(())
}
