use crate::client::commands::duration_doc;
use crate::client::globalsettings::GlobalSettings;
use crate::client::server::client_stop_server;
use crate::common::serverdir::{
    ConnectAccessRecordPart, FullAccessRecord, load_access_record, store_access_record,
};
use crate::common::utils::network::get_hostname;
use crate::common::utils::time::parse_hms_or_human_time;
use crate::server::bootstrap::{
    ServerConfig, generate_server_uid, get_client_session, init_hq_server,
};
use crate::transfer::auth::generate_key;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
pub struct ServerOpts {
    #[clap(subcommand)]
    pub subcmd: ServerCommand,
}

#[derive(Parser)]
pub struct GenerateAccessOpts {
    /// The filename of the generated full access file
    access_file: PathBuf,

    /// The filename of the generated client's access file
    #[arg(long)]
    client_file: Option<PathBuf>,

    /// The filename of the generated worker's access file
    #[arg(long)]
    worker_file: Option<PathBuf>,

    /// Override the target host name
    ///
    /// If not set, the local hostname is used
    #[arg(long)]
    host: Option<String>,

    /// Override target host name for clients
    #[arg(long)]
    client_host: Option<String>,

    /// Override target host name for workers
    #[arg(long)]
    worker_host: Option<String>,

    /// The port for connecting client
    #[arg(long)]
    client_port: u16,

    /// The port for connecting workers
    #[arg(long)]
    worker_port: u16,
}

#[derive(Parser)]
pub enum ServerCommand {
    /// Start the server
    Start(ServerStartOpts),
    /// Stop the server
    Stop(ServerStopOpts),
    /// Show info of a running server
    Info(ServerInfoOpts),
    /// Generate an access file without starting the server
    GenerateAccess(GenerateAccessOpts),
}

#[derive(Parser)]
pub struct ServerStartOpts {
    /// Hostname/IP of the machine under which is visible to others
    ///
    /// Default: hostname
    #[arg(long)]
    host: Option<String>,

    #[arg(
        long,
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("Duration after which will an idle worker automatically stop.")
    )]
    idle_timeout: Option<Duration>,

    /// The port for client connections
    #[arg(long)]
    client_port: Option<u16>,

    /// The port for worker connections
    #[arg(long)]
    worker_port: Option<u16>,

    /// The path to a journal file
    ///
    /// If the file already exists, the file is first used to restore the server state.
    #[arg(long)]
    journal: Option<PathBuf>,

    #[arg(
        long,
        value_parser = parse_hms_or_human_time,
        default_value = "30s",
        help = duration_doc!("Configure how often should be the journal written.")
    )]
    journal_flush_period: Duration,

    /// The path to an access file
    #[arg(long)]
    access_file: Option<PathBuf>,

    /// If set, the client connection will NOT be AUTHENTICATED and ENCRYPTED.
    ///
    /// ANYONE CAN CONNECT TO THE SERVER AS CLIENT!
    /// USE AT YOUR OWN RISK.
    #[arg(long)]
    disable_client_authentication_and_encryption: bool,

    /// If set, the worker connection will NOT be AUTHENTICATED and ENCRYPTED.
    ///
    /// ANYONE CAN CONNECT TO THE SERVER AS WORKER!
    /// USE AT YOUR OWN RISK.
    #[arg(long)]
    disable_worker_authentication_and_encryption: bool,
}

#[derive(Parser)]
pub struct ServerStopOpts {}

#[derive(Parser)]
pub struct ServerInfoOpts {}

pub async fn command_server(gsettings: &GlobalSettings, opts: ServerOpts) -> anyhow::Result<()> {
    match opts.subcmd {
        ServerCommand::Start(opts) => start_server(gsettings, opts).await,
        ServerCommand::Stop(opts) => stop_server(gsettings, opts).await,
        ServerCommand::Info(opts) => command_server_info(gsettings, opts).await,
        ServerCommand::GenerateAccess(opts) => command_server_generate_access(gsettings, opts),
    }
}

async fn start_server(gsettings: &GlobalSettings, opts: ServerStartOpts) -> anyhow::Result<()> {
    let access_file: Option<FullAccessRecord> = opts
        .access_file
        .map(|path| load_access_record(path.as_path()))
        .transpose()?;

    let worker_host = opts
        .host
        .clone()
        .or_else(|| access_file.as_ref().map(|a| a.worker_host().to_string()))
        .unwrap_or_else(|| get_hostname(None));

    let client_host = opts
        .host
        .or_else(|| access_file.as_ref().map(|a| a.client_host().to_string()))
        .unwrap_or_else(|| get_hostname(None));

    let worker_port = opts
        .worker_port
        .or(access_file.as_ref().map(|a| a.worker_port()));
    let client_port = opts
        .client_port
        .or(access_file.as_ref().map(|a| a.client_port()));

    let server_cfg = ServerConfig {
        client_host,
        worker_host,
        idle_timeout: opts.idle_timeout,
        client_port,
        worker_port,
        journal_path: opts.journal,
        journal_flush_period: opts.journal_flush_period,
        worker_secret_key: access_file
            .as_ref()
            .map(|a| a.worker_key().cloned())
            .unwrap_or_else(|| {
                if opts.disable_worker_authentication_and_encryption {
                    None
                } else {
                    Some(Arc::new(generate_key()))
                }
            }),
        client_secret_key: access_file
            .as_ref()
            .map(|a| a.client_key().cloned())
            .unwrap_or_else(|| {
                if opts.disable_client_authentication_and_encryption {
                    None
                } else {
                    Some(Arc::new(generate_key()))
                }
            }),
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
    _opts: ServerInfoOpts,
) -> anyhow::Result<()> {
    print_server_info(gsettings).await
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
        .print_server_info(None, &response);
    Ok(())
}

fn command_server_generate_access(
    _gsettings: &GlobalSettings,
    opts: GenerateAccessOpts,
) -> anyhow::Result<()> {
    let server_uid = generate_server_uid();
    let worker_key = Arc::new(generate_key());
    let client_key = Arc::new(generate_key());
    let client_host = get_hostname(opts.client_host.or_else(|| opts.host.clone()));
    let worker_host = get_hostname(opts.worker_host.or(opts.host));

    let record = FullAccessRecord::new(
        ConnectAccessRecordPart {
            host: client_host,
            port: opts.client_port,
            secret_key: Some(client_key),
        },
        ConnectAccessRecordPart {
            host: worker_host,
            port: opts.worker_port,
            secret_key: Some(worker_key),
        },
        server_uid,
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
