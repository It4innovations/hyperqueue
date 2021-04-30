use std::future::Future;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use cli_table::{print_stdout, Cell, Style, Table};
use futures::TryFutureExt;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::LocalSet;

use crate::client::globalsettings::GlobalSettings;
use crate::common::error::error;
use crate::common::error::HqError::GenericError;
use crate::common::serverdir::{AccessRecord, ServerDir};
use crate::common::setup::setup_interrupt;
use crate::server::rpc::TakoServer;
use crate::server::state::StateRef;
use crate::transfer::auth::generate_key;
use crate::transfer::connection::{ClientConnection, HqConnection};

enum ServerStatus {
    Offline(AccessRecord),
    Online(AccessRecord),
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
pub async fn init_hq_server(gsettings: &GlobalSettings) -> crate::Result<()> {
    match get_server_status(gsettings.server_directory()).await {
        Err(_) | Ok(ServerStatus::Offline(_)) => {
            log::info!("No online server found, starting a new server");
            start_server(gsettings.server_directory()).await
        }
        Ok(ServerStatus::Online(_)) => error(format!(
            "Server at {0} is already online, please stop it first using \
            `hq stop --server-dir {0}`",
            gsettings.server_directory().display()
        )),
    }
}

pub async fn get_client_connection(server_directory: &Path) -> crate::Result<ClientConnection> {
    match ServerDir::open(server_directory).and_then(|sd| sd.read_access_record()) {
        Ok(record) => Ok(HqConnection::connect_to_server(&record).await?),
        Err(e) => error(format!("No running instance of HQ found: {}", e)),
    }
}

async fn get_server_status(server_directory: &Path) -> crate::Result<ServerStatus> {
    let record = ServerDir::open(server_directory).and_then(|sd| sd.read_access_record())?;

    if HqConnection::connect_to_server(&record).await.is_err() {
        return Ok(ServerStatus::Offline(record));
    }

    Ok(ServerStatus::Online(record))
}

async fn initialize_server(
    server_directory: &Path,
    end_flag: Arc<Notify>,
) -> crate::Result<impl Future<Output = crate::Result<()>>> {
    let client_listener = TcpListener::bind("0.0.0.0:0")
        .map_err(|e| GenericError(format!("Cannot create HQ server socket: {}", e)))
        .await?;
    let server_port = client_listener.local_addr()?.port();

    let hq_secret_key = Arc::new(generate_key());
    let tako_secret_key = Arc::new(generate_key());

    let state_ref = StateRef::new();
    let (tako_server, tako_future) =
        TakoServer::start(state_ref.clone(), tako_secret_key.clone()).await?;

    let record = AccessRecord::new(
        gethostname::gethostname().into_string().unwrap(),
        server_port,
        tako_server.worker_port(),
        hq_secret_key.clone(),
        tako_secret_key.clone(),
    );

    ServerDir::create(server_directory, &record)?;
    print_access_record(server_directory, &record);

    let stop_notify = Rc::new(Notify::new());
    let stop_cloned = stop_notify.clone();

    let key = hq_secret_key;
    let fut = async move {
        tokio::select! {
            _ = end_flag.notified() => {
                log::info!("Received SIGINT");
                Ok(())
            },
            _ = stop_notify.notified() => {
                log::info!("Stopping after Stop command from client");
                Ok(())
            },
            () = crate::server::client::handle_client_connections(
                state_ref,
                tako_server,
                client_listener,
                stop_cloned,
                key
            ) => { Ok(()) }
            r = tako_future => { r }
        }
    };
    Ok(fut)
}

async fn start_server(directory: &Path) -> crate::Result<()> {
    let end_flag = setup_interrupt();
    let fut = initialize_server(&directory, end_flag).await?;
    let local_set = LocalSet::new();
    local_set.run_until(fut).await
}

pub fn print_access_record(server_dir: &Path, record: &AccessRecord) {
    let rows = vec![
        vec![
            "Server directory".cell().bold(true),
            server_dir.display().cell(),
        ],
        vec!["Hostname".cell().bold(true), record.hostname().cell()],
        vec!["Pid".cell().bold(true), record.pid().cell()],
        vec!["HQ port".cell().bold(true), record.server_port().cell()],
        vec![
            "Workers port".cell().bold(true),
            record.worker_port().cell(),
        ],
        vec![
            "Start date".cell().bold(true),
            record.start_date().format("%F %T %Z").cell(),
        ],
        vec!["Version".cell().bold(true), record.version().cell()],
    ];
    let table = rows.table();
    assert!(print_stdout(table).is_ok());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempdir::TempDir;
    use tokio::sync::Notify;

    use crate::client::commands::stop::stop_server;
    use crate::common::fsutils::test_utils::run_concurrent;
    use crate::common::serverdir::{store_access_record, AccessRecord, ServerDir, SYMLINK_PATH};
    use crate::server::bootstrap::{get_client_connection, get_server_status, initialize_server};

    use super::ServerStatus;

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
        let fut = initialize_server(&tmp_dir, Default::default())
            .await
            .unwrap();
        let (set, handle) = run_concurrent(fut, async {
            let mut connection = get_client_connection(&tmp_dir).await.unwrap();
            stop_server(&mut connection).await.unwrap();
        })
        .await;
        set.run_until(handle).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_stop_on_end_condition() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();

        let notify = Arc::new(Notify::new());
        let fut = initialize_server(&tmp_dir, notify.clone()).await.unwrap();
        notify.notify_one();
        fut.await.unwrap();
    }
}
