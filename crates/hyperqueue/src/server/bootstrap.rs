use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::LocalSet;

use crate::client::globalsettings::GlobalSettings;
use crate::common::serverdir::{default_server_directory, AccessRecord, ServerDir, SYMLINK_PATH};
use crate::server::autoalloc::create_autoalloc_service;
use crate::server::event::log::start_event_streaming;
use crate::server::event::log::EventLogWriter;
use crate::server::event::storage::EventStorage;
use crate::server::rpc::Backend;
use crate::server::state::StateRef;
use crate::transfer::auth::generate_key;
use crate::transfer::connection::{ClientConnection, HqConnection};
use std::time::Duration;

enum ServerStatus {
    Offline(AccessRecord),
    Online(AccessRecord),
}

pub struct ServerConfig {
    pub host: String,
    pub idle_timeout: Option<Duration>,
    pub client_port: Option<u16>,
    pub worker_port: Option<u16>,
    pub event_buffer_size: usize,
    pub event_log_path: Option<PathBuf>,
}

/// This function initializes the HQ server.
///
/// It takes a path to a directory and tries to find metadata of a running HQ server either directly
/// inside the directory or in a path linked to by a symlink named [`SYMLINK_PATH`].
///
/// If no metadata is found or the metadata links to a server that seems to be offline, a new
/// server will be started.
///
/// If an already running server is found, an error will be returned.
pub async fn init_hq_server(
    gsettings: &GlobalSettings,
    server_cfg: ServerConfig,
) -> anyhow::Result<()> {
    match get_server_status(gsettings.server_directory()).await {
        Err(_) | Ok(ServerStatus::Offline(_)) => {
            log::info!("No online server found, starting a new server");
            start_server(gsettings, server_cfg).await
        }
        Ok(ServerStatus::Online(_)) => anyhow::bail!(
            "Server at {0} is already online, please stop it first using \
            `hq server stop --server-dir {0}`",
            gsettings.server_directory().display()
        ),
    }
}

pub async fn get_client_connection(server_directory: &Path) -> anyhow::Result<ClientConnection> {
    let default_home = default_server_directory();
    let sd = ServerDir::open(server_directory).context("Invalid server directory")?;
    let server_dir_msg = if default_home != server_directory {
        format!(" --server-dir {}", server_directory.to_str().unwrap())
    } else {
        String::new()
    };
    let access_record = sd.read_access_record().with_context(|| {
        format!(
            "No running instance of HQ found at {:?}.\n\
            Try to start the server: `hq server start{}` or use a different server directory.",
            sd.access_filename(),
            server_dir_msg,
        )
    })?;

    let connection = HqConnection::connect_to_server(&access_record)
        .await
        .with_context(|| {
            format!(
                "Access token found but HQ server {}:{} is unreachable.\n\
                Try to (re)start the server using `hq server start{}`",
                access_record.host(),
                access_record.server_port(),
                server_dir_msg,
            )
        })?;

    Ok(connection)
}

async fn get_server_status(server_directory: &Path) -> crate::Result<ServerStatus> {
    let record = ServerDir::open(server_directory).and_then(|sd| sd.read_access_record())?;

    if HqConnection::connect_to_server(&record).await.is_err() {
        return Ok(ServerStatus::Offline(record));
    }

    Ok(ServerStatus::Online(record))
}

pub async fn initialize_server(
    gsettings: &GlobalSettings,
    server_cfg: ServerConfig,
) -> anyhow::Result<(impl Future<Output = anyhow::Result<()>>, Arc<Notify>)> {
    let server_directory = gsettings.server_directory();

    let client_listener = TcpListener::bind(SocketAddr::new(
        Ipv4Addr::UNSPECIFIED.into(),
        server_cfg.client_port.unwrap_or(0),
    ))
    .await
    .with_context(|| "Cannot create HQ server socket".to_string())?;
    let server_port = client_listener.local_addr()?.port();

    let hq_secret_key = Arc::new(generate_key());
    let tako_secret_key = Arc::new(generate_key());

    let (event_storage, event_stream_fut) = prepare_event_management(&server_cfg).await?;
    let state_ref = StateRef::new(event_storage);
    let (autoalloc_service, autoalloc_process) = create_autoalloc_service(state_ref.clone());
    // TODO: remove this hack
    state_ref.get_mut().autoalloc_service = Some(autoalloc_service);

    let (tako_server, tako_future) = Backend::start(
        state_ref.clone(),
        tako_secret_key.clone(),
        server_cfg.idle_timeout,
        server_cfg.worker_port,
    )
    .await?;

    let record = AccessRecord::new(
        server_cfg.host,
        server_port,
        tako_server.worker_port(),
        hq_secret_key.clone(),
        tako_secret_key.clone(),
    );

    let server_dir = ServerDir::create(server_directory, &record)?;
    gsettings
        .printer()
        .print_server_record(server_directory, &record);

    let end_flag = Arc::new(Notify::new());
    let end_flag_check = end_flag.clone();
    let end_flag_ret = end_flag.clone();

    let stop_check = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                log::info!("Received SIGINT, attempting to stop");
            }
            _ = end_flag_check.notified() => {
                log::info!("Received Stop command from client");
            }
        };
        log::info!("Stopping server");
    };

    let key = hq_secret_key;
    let fut = async move {
        tokio::pin! {
            let autoalloc_process = autoalloc_process;
        };

        let result = tokio::select! {
            _ = stop_check => {
                Ok(())
            }
            _ = crate::server::client::handle_client_connections(
                state_ref.clone(),
                tako_server,
                server_dir,
                client_listener,
                end_flag,
                key
            ) => { Ok(()) }
            _ = &mut autoalloc_process => { Ok(()) }
            r = tako_future => { r.map_err(|e| e.into()) }
        };

        log::debug!("Shutting down auto allocation");
        state_ref.get_mut().stop_autoalloc();
        autoalloc_process.await;

        log::debug!("Shutting down event streaming");

        // StateRef needs to be dropped at this moment, because it may contain a sender
        // that has to be shut-down for `event_stream_fut` to resolve.
        drop(state_ref);
        event_stream_fut.await;

        result
    };
    Ok((fut, end_flag_ret))
}

async fn prepare_event_management(
    server_cfg: &ServerConfig,
) -> anyhow::Result<(EventStorage, Pin<Box<dyn Future<Output = ()>>>)> {
    Ok(if let Some(ref log_path) = server_cfg.event_log_path {
        let writer = EventLogWriter::create(log_path).await.map_err(|error| {
            anyhow!(
                "Cannot create event log file at `{}`: {error:?}",
                log_path.display()
            )
        })?;

        let (tx, stream_fut) = start_event_streaming(writer);
        (
            EventStorage::new(server_cfg.event_buffer_size, Some(tx)),
            Box::pin(stream_fut),
        )
    } else {
        (
            EventStorage::new(server_cfg.event_buffer_size, None),
            Box::pin(futures::future::ready(())),
        )
    })
}

async fn start_server(
    gsettings: &GlobalSettings,
    server_config: ServerConfig,
) -> anyhow::Result<()> {
    let (fut, _) = initialize_server(gsettings, server_config).await?;
    let local_set = LocalSet::new();
    local_set.run_until(fut).await?;

    // Delete symlink to mark that the server is no longer active
    std::fs::remove_file(gsettings.server_directory().join(SYMLINK_PATH)).ok();

    Ok(())
}

pub async fn print_server_info(gsettings: &GlobalSettings) -> anyhow::Result<()> {
    match get_server_status(gsettings.server_directory()).await {
        Err(_) | Ok(ServerStatus::Offline(_)) => anyhow::bail!("No online server found"),
        Ok(ServerStatus::Online(record)) => gsettings
            .printer()
            .print_server_record(gsettings.server_directory(), &record),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use tokio::sync::Notify;

    use crate::common::serverdir::{store_access_record, AccessRecord, ServerDir, SYMLINK_PATH};
    use crate::server::bootstrap::{
        get_client_connection, get_server_status, initialize_server, ServerConfig,
    };

    use super::ServerStatus;
    use crate::client::globalsettings::GlobalSettings;
    use crate::client::output::cli::CliOutput;
    use crate::client::server::client_stop_server;
    use crate::tests::utils::run_concurrent;
    use cli_table::ColorChoice;
    use std::future::Future;
    use std::path::Path;
    use std::sync::Arc;

    pub async fn init_test_server(
        tmp_dir: &Path,
    ) -> (impl Future<Output = anyhow::Result<()>>, Arc<Notify>) {
        // Create global settings with CliOutput
        let gsettings = GlobalSettings::new(
            tmp_dir.to_path_buf(),
            Box::new(CliOutput::new(ColorChoice::Never)),
        );
        let server_cfg = ServerConfig {
            host: "localhost".to_string(),
            idle_timeout: None,
            client_port: None,
            worker_port: None,
            event_buffer_size: 1_000_000,
            event_log_path: None,
        };
        initialize_server(&gsettings, server_cfg).await.unwrap()
    }

    #[tokio::test]
    async fn test_status_empty_directory() {
        let tmp_dir = TempDir::new("foo").unwrap();
        assert!(get_server_status(&tmp_dir.into_path()).await.is_err());
    }

    #[tokio::test]
    async fn test_status_directory_with_access_file() {
        let tmp_dir = TempDir::new("foo").unwrap();
        let tmp_path = tmp_dir.into_path();
        let server_dir = ServerDir::open(&tmp_path).unwrap();
        let record =
            AccessRecord::new("foo".into(), 42, 43, Default::default(), Default::default());
        store_access_record(&record, server_dir.access_filename()).unwrap();

        let res = get_server_status(&tmp_path).await.unwrap();
        assert!(matches!(res, ServerStatus::Offline(_)));
    }

    #[tokio::test]
    async fn test_status_directory_with_symlink() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();
        let actual_dir = tmp_dir.join("server-dir");
        std::fs::create_dir(&actual_dir).unwrap();
        std::os::unix::fs::symlink(&actual_dir, tmp_dir.join(SYMLINK_PATH)).unwrap();

        let server_dir = ServerDir::open(&actual_dir).unwrap();
        let record =
            AccessRecord::new("foo".into(), 42, 43, Default::default(), Default::default());
        store_access_record(&record, server_dir.access_filename()).unwrap();

        //let server_dir = ServerDir::open(&tmp_dir).unwrap();
        let res = get_server_status(&tmp_dir).await.unwrap();
        assert!(matches!(res, ServerStatus::Offline(_)));
    }

    #[tokio::test]
    async fn test_start_stop() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();
        let (fut, _) = init_test_server(&tmp_dir).await;
        let (set, handle) = run_concurrent(fut, async {
            let mut connection = get_client_connection(&tmp_dir).await.unwrap();
            client_stop_server(&mut connection).await.unwrap();
        })
        .await;
        set.run_until(handle).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_stop_on_end_condition() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();
        let (fut, notify) = init_test_server(&tmp_dir).await;
        notify.notify_one();
        fut.await.unwrap();
    }
}
