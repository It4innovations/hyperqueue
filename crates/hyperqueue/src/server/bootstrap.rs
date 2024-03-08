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
use crate::common::error::HqError;
use crate::common::serverdir::{
    default_server_directory, ClientAccessRecord, ConnectAccessRecordPart, FullAccessRecord,
    ServerDir, SYMLINK_PATH,
};
use crate::server::autoalloc::create_autoalloc_service;
use crate::server::event::log::start_event_streaming;
use crate::server::event::log::EventLogWriter;
use crate::server::event::storage::EventStorage;
use crate::server::restore::StateRestorer;
use crate::server::rpc::Backend;
use crate::server::state::StateRef;
use crate::transfer::auth::generate_key;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::ServerInfo;
use crate::HQ_VERSION;
use chrono::Utc;
use orion::kdf::SecretKey;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::time::Duration;
use tako::gateway::{FromGatewayMessage, ToGatewayMessage};
use tako::WorkerId;

enum ServerStatus {
    Offline(ClientAccessRecord),
    Online(ClientAccessRecord),
}

pub struct ServerConfig {
    pub client_host: String,
    pub worker_host: String,
    pub idle_timeout: Option<Duration>,
    pub client_port: Option<u16>,
    pub worker_port: Option<u16>,
    pub event_buffer_size: usize,
    pub journal_path: Option<PathBuf>,
    pub worker_secret_key: Option<Arc<SecretKey>>,
    pub client_secret_key: Option<Arc<SecretKey>>,
    pub server_uid: Option<String>,
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

pub async fn get_client_session(server_directory: &Path) -> anyhow::Result<ClientSession> {
    let default_home = default_server_directory();
    let sd = ServerDir::open(server_directory).context("Invalid server directory")?;
    let server_dir_msg = if default_home != server_directory {
        format!(" --server-dir {}", server_directory.to_str().unwrap())
    } else {
        String::new()
    };
    let access_record_r = sd.read_client_access_record();

    if let Err(HqError::VersionError(msg)) = access_record_r {
        anyhow::bail!(msg);
    }
    let access_record = access_record_r.with_context(|| {
        format!(
            "No running instance of HQ found at {:?}.\n\
            Try to start the server: `hq server start{}` or use a different server directory.",
            sd.access_filename(),
            server_dir_msg,
        )
    })?;

    let session = ClientSession::connect_to_server(&access_record)
        .await
        .with_context(|| {
            format!(
                "Access token found but HQ server {}:{} is unreachable.\n\
                Try to (re)start the server using `hq server start{}`",
                access_record.client.host, access_record.client.port, server_dir_msg,
            )
        })?;

    Ok(session)
}

async fn get_server_status(server_directory: &Path) -> crate::Result<ServerStatus> {
    let record = ServerDir::open(server_directory).and_then(|sd| sd.read_client_access_record())?;

    if ClientSession::connect_to_server(&record).await.is_err() {
        return Ok(ServerStatus::Offline(record));
    }

    Ok(ServerStatus::Online(record))
}

pub fn generate_server_uid() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect()
}

pub async fn initialize_server(
    gsettings: &GlobalSettings,
    mut server_cfg: ServerConfig,
    worker_id_initial_value: WorkerId,
) -> anyhow::Result<(
    impl Future<Output = anyhow::Result<()>>,
    Arc<Notify>,
    StateRef,
    Backend,
)> {
    let server_directory = gsettings.server_directory();

    let client_listener = TcpListener::bind(SocketAddr::new(
        Ipv4Addr::UNSPECIFIED.into(),
        server_cfg.client_port.unwrap_or(0),
    ))
    .await
    .with_context(|| "Cannot create HQ server socket".to_string())?;
    let client_port = client_listener.local_addr()?.port();

    let server_uid = server_cfg
        .server_uid
        .take()
        .unwrap_or_else(generate_server_uid);
    let worker_key = server_cfg
        .worker_secret_key
        .take()
        .unwrap_or_else(|| Arc::new(generate_key()));
    let client_key = server_cfg
        .client_secret_key
        .take()
        .unwrap_or_else(|| Arc::new(generate_key()));

    let (event_storage, event_stream_fut) = prepare_event_management(&server_cfg).await?;
    let state_ref = StateRef::new(
        event_storage,
        ServerInfo {
            version: HQ_VERSION.to_string(),
            server_uid: server_uid.clone(),
            client_host: server_cfg.client_host.clone(),
            worker_host: server_cfg.worker_host.clone(),
            client_port,
            worker_port: 0, // Will be set later
            pid: std::process::id(),
            start_date: Utc::now(),
        },
    );

    let (autoalloc_service, autoalloc_process) = create_autoalloc_service(state_ref.clone());
    // TODO: remove this hack
    state_ref.get_mut().autoalloc_service = Some(autoalloc_service);

    let (tako_server, tako_future) = Backend::start(
        state_ref.clone(),
        worker_key.clone(),
        server_cfg.idle_timeout,
        server_cfg.worker_port,
        worker_id_initial_value,
    )
    .await?;

    let worker_port = tako_server.worker_port();

    state_ref.get_mut().set_worker_port(worker_port);

    let record = FullAccessRecord::new(
        ConnectAccessRecordPart {
            host: server_cfg.client_host,
            port: client_port,
            secret_key: client_key.clone(),
        },
        ConnectAccessRecordPart {
            host: server_cfg.worker_host,
            port: worker_port,
            secret_key: worker_key.clone(),
        },
        server_uid,
    );

    let server_dir = ServerDir::create(server_directory, &record)?;

    gsettings
        .printer()
        .print_server_description(Some(server_directory), state_ref.get().server_info());

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

    let key = client_key;
    let state_ref2 = state_ref.clone();
    let tako_server2 = tako_server.clone();

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

        state_ref.get_mut().event_storage_mut().close_stream();
        // StateRef needs to be dropped at this moment, because it may contain a sender
        // that has to be shut-down for `event_stream_fut` to resolve.
        drop(state_ref);
        event_stream_fut.await;

        result
    };
    Ok((fut, end_flag_ret, state_ref2, tako_server2))
}

async fn prepare_event_management(
    server_cfg: &ServerConfig,
) -> anyhow::Result<(EventStorage, Pin<Box<dyn Future<Output = ()>>>)> {
    Ok(if let Some(ref log_path) = server_cfg.journal_path {
        let writer = EventLogWriter::create_or_append(log_path)
            .await
            .map_err(|error| {
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

async fn start_server(gsettings: &GlobalSettings, server_cfg: ServerConfig) -> anyhow::Result<()> {
    let restorer = if let Some(path) = &server_cfg.journal_path {
        if path.exists() {
            let mut restorer = StateRestorer::default();
            restorer.load_event_file(path)?;
            Some(restorer)
        } else {
            None
        }
    } else {
        None
    };

    let (fut, _, state_ref, tako_ref) = initialize_server(
        gsettings,
        server_cfg,
        restorer
            .as_ref()
            .map(|r| r.worker_id_counter())
            .unwrap_or(WorkerId::new(0)),
    )
    .await?;
    let new_tasks = if let Some(restorer) = restorer {
        // This is early state recovery, we restore jobs later as we start futures because restoring
        // jobs already needs a running Tako
        let mut state = state_ref.get_mut();
        state.restore_state(&restorer);
        Some(restorer.restore_jobs(&mut state)?)
    } else {
        None
    };
    let local_set = LocalSet::new();

    if let Some(new_tasks) = new_tasks {
        local_set.spawn_local(async move {
            log::debug!("Restoring old tasks into Tako");
            for msg in new_tasks {
                match tako_ref
                    .send_tako_message(FromGatewayMessage::NewTasks(msg))
                    .await
                    .unwrap()
                {
                    ToGatewayMessage::NewTasksResponse(_) => { /* Ok */ }
                    r => panic!("Invalid response: {r:?}"),
                };
            }
            log::debug!("Restoration of old tasks is completed");
        });
    };
    local_set.run_until(fut).await?;

    // Delete symlink to mark that the server is no longer active
    std::fs::remove_file(gsettings.server_directory().join(SYMLINK_PATH)).ok();

    Ok(())
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use tokio::sync::Notify;

    use crate::common::serverdir::{
        store_access_record, ConnectAccessRecordPart, FullAccessRecord, ServerDir, SYMLINK_PATH,
    };
    use crate::server::bootstrap::{
        get_client_session, get_server_status, initialize_server, ServerConfig,
    };

    use super::ServerStatus;
    use crate::client::globalsettings::GlobalSettings;
    use crate::client::output::cli::CliOutput;
    use crate::client::server::client_stop_server;
    use crate::tests::utils::run_concurrent;
    use crate::transfer::auth::generate_key;
    use cli_table::ColorChoice;
    use std::future::Future;
    use std::path::Path;
    use std::sync::Arc;

    pub async fn init_test_server(
        tmp_dir: &Path,
    ) -> (impl Future<Output = anyhow::Result<()>>, Arc<Notify>) {
        // Create global public with CliOutput
        let gsettings = GlobalSettings::new(
            tmp_dir.to_path_buf(),
            Box::new(CliOutput::new(ColorChoice::Never)),
        );
        let server_cfg = ServerConfig {
            worker_host: "localhost".to_string(),
            client_host: "localhost".to_string(),
            idle_timeout: None,
            client_port: None,
            worker_port: None,
            event_buffer_size: 1_000_000,
            journal_path: None,
            worker_secret_key: None,
            client_secret_key: None,
            server_uid: None,
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
        let record = FullAccessRecord::new(
            ConnectAccessRecordPart {
                host: "foo".into(),
                port: 42,
                secret_key: Arc::new(generate_key()),
            },
            ConnectAccessRecordPart {
                host: "bar".into(),
                port: 42,
                secret_key: Arc::new(generate_key()),
            },
            "testHQ".into(),
        );
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
        let record = FullAccessRecord::new(
            ConnectAccessRecordPart {
                host: "foo".into(),
                port: 42,
                secret_key: Arc::new(generate_key()),
            },
            ConnectAccessRecordPart {
                host: "bar".into(),
                port: 42,
                secret_key: Arc::new(generate_key()),
            },
            "testHQ".into(),
        );
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
            let mut session = get_client_session(&tmp_dir).await.unwrap();
            client_stop_server(session.connection()).await.unwrap();
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
