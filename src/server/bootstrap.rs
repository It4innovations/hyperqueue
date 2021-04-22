use std::future::Future;
use std::path::{Path, PathBuf};

use futures::TryFutureExt;
use tokio::net::TcpListener;
use tokio::task::LocalSet;

use crate::common::error::error;
use crate::common::error::HqError::GenericError;
use crate::common::rundir::{load_runfile, RunDirectory, Runfile, store_runfile};
use crate::common::setup::setup_interrupt;
use crate::server::rpc::TakoServer;
use crate::server::state::StateRef;
use crate::transfer::auth::{generate_key};
use crate::transfer::connection::{HqConnection, ClientConnection};
use tokio::sync::Notify;
use std::rc::Rc;
use std::sync::Arc;

const SYMLINK_PATH: &str = "hq-active-dir";

enum ServerStatus {
    Offline(Runfile),
    Online(Runfile),
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
pub async fn init_hq_server(rundir_path: PathBuf) -> crate::Result<()> {
    let directory = resolve_active_directory(rundir_path.clone());
    std::fs::create_dir_all(&directory)?;

    let rundir = RunDirectory::new(directory.clone())?;

    match get_server_status(&rundir).await {
        Err(_) | Ok(ServerStatus::Offline(_)) => {
            log::info!("No online server found, starting a new server");
            start_server(rundir_path).await
        }
        Ok(ServerStatus::Online(_)) => {
            error(format!("Server at {0} is already online, please stop it first using \
            `hq stop --rundir {0}`", rundir_path.display()))
        }
    }
}

pub async fn get_client_connection(rundir_path: PathBuf) -> crate::Result<ClientConnection> {
    match get_runfile(rundir_path.clone()) {
        Ok(runfile) => Ok(HqConnection::connect_to_server(&runfile).await?),
        Err(e) => error(format!("No running instance of HQ found: {}", e))
    }
}

/// Returns either `path` if it doesn't contain `SYMLINK_PATH` or the target of `SYMLINK_PATH`.
fn resolve_active_directory(path: PathBuf) -> PathBuf {
    let symlink_path = path.join(SYMLINK_PATH);
    std::fs::canonicalize(symlink_path)
        .ok()
        .filter(|p| p.is_dir())
        .unwrap_or(path)
}

fn get_runfile(directory: PathBuf) -> crate::Result<Runfile> {
    let directory = resolve_active_directory(directory);
    let rundir = RunDirectory::new(directory)?;
    let runfile = load_runfile(rundir.runfile())?;
    Ok(runfile)
}

async fn get_server_status(rundir: &RunDirectory) -> crate::Result<ServerStatus> {
    let runfile = get_runfile(rundir.directory().clone())?;

    if HqConnection::connect_to_server(&runfile).await.is_err() {
        return Ok(ServerStatus::Offline(runfile));
    }

    Ok(ServerStatus::Online(runfile))
}

async fn initialize_server(
    directory: PathBuf,
    end_flag: Arc<Notify>
) -> crate::Result<impl Future<Output=crate::Result<()>>> {
    let client_listener = TcpListener::bind("0.0.0.0:0")
        .map_err(|e| GenericError(format!("Cannot create HQ server socket: {}", e)))
        .await?;
    let server_port = client_listener.local_addr()?.port();

    let hq_secret_key = Arc::new(generate_key());
    let tako_secret_key = Arc::new(generate_key());

    let state_ref = StateRef::new();
    let (tako_server, tako_future) = TakoServer::start(
        state_ref.clone(),
        tako_secret_key.clone(),
    ).await?;

    let runfile = Runfile::new(
        gethostname::gethostname().into_string().unwrap(),
        server_port,
        tako_server.worker_port(),
        hq_secret_key.clone(),
        tako_secret_key.clone(),
    );
    initialize_directory(&directory, &runfile)?;

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

async fn start_server(directory: PathBuf) -> crate::Result<()> {
    let end_flag = setup_interrupt();
    let fut = initialize_server(directory, end_flag).await?;
    let local_set = LocalSet::new();
    local_set.run_until(fut).await
}

fn initialize_directory(directory: &Path, runfile: &Runfile) -> crate::Result<()> {
    let rundir_path = directory.join(runfile.start_date().format("%Y-%m-%d-%H-%M-%S").to_string());
    std::fs::create_dir_all(&rundir_path)?;
    let rundir = RunDirectory::new(rundir_path.clone())?;

    log::info!("Storing runfile to {:?}", rundir.runfile());
    store_runfile(&runfile, rundir.runfile())?;

    create_symlink(&directory.join(SYMLINK_PATH), &rundir_path)
}

fn create_symlink(symlink_path: &Path, target: &Path) -> crate::Result<()> {
    if symlink_path.exists() {
        std::fs::remove_file(symlink_path)?;
    }
    std::os::unix::fs::symlink(target, symlink_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::common::rundir::{RunDirectory, Runfile, store_runfile};
    use crate::server::bootstrap::{get_server_status, initialize_server, resolve_active_directory, SYMLINK_PATH};
    use crate::utils::test_utils::run_concurrent;

    use super::ServerStatus;
    use crate::client::commands::stop::stop_server;
    use tokio::sync::Notify;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_status_empty_directory() {
        let tmp_dir = TempDir::new("foo").unwrap();
        assert!(get_server_status(&RunDirectory::new(tmp_dir.into_path()).unwrap()).await.is_err());
    }

    #[tokio::test]
    async fn test_status_directory_with_runfile() {
        let tmp_dir = TempDir::new("foo").unwrap();
        let rundir = RunDirectory::new(tmp_dir.into_path()).unwrap();
        let runfile = Runfile::new(
            "foo".into(),
            42,
            43,
            Default::default(),
            Default::default(),
        );
        store_runfile(&runfile, rundir.runfile()).unwrap();

        let res = get_server_status(&rundir).await.unwrap();
        assert!(matches!(res, ServerStatus::Offline(_)));
    }

    #[tokio::test]
    async fn test_status_directory_with_symlink() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();
        let actual_dir = tmp_dir.join("rundir");
        std::fs::create_dir(&actual_dir).unwrap();
        std::os::unix::fs::symlink(&actual_dir, tmp_dir.join(SYMLINK_PATH)).unwrap();

        let rundir = RunDirectory::new(actual_dir.clone()).unwrap();
        let runfile = Runfile::new(
            "foo".into(),
            42,
            43,
            Default::default(),
            Default::default(),
        );
        store_runfile(&runfile, rundir.runfile()).unwrap();

        let rundir = RunDirectory::new(resolve_active_directory(tmp_dir)).unwrap();
        let res = get_server_status(&rundir).await.unwrap();
        assert!(matches!(res, ServerStatus::Offline(_)));
    }

    #[tokio::test]
    async fn test_start_stop() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();
        let fut = initialize_server(tmp_dir.clone(), Default::default()).await.unwrap();
        let (set, handle) = run_concurrent(fut, async {
            stop_server(tmp_dir).await.unwrap();
        }).await;
        set.run_until(handle).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_stop_on_end_condition() {
        let tmp_dir = TempDir::new("foo").unwrap().into_path();

        let notify = Arc::new(Notify::new());
        let fut = initialize_server(tmp_dir.clone(), notify.clone()).await.unwrap();
        notify.notify_one();
        fut.await.unwrap();
    }
}
