use std::path::{Path, PathBuf};

use tokio::net::{TcpStream, TcpListener};
use tokio::task::LocalSet;

use crate::common::error::HqError::GenericError;
use crate::common::rundir::{load_runfile, RunDirectory, Runfile, store_runfile};
use crate::common::setup::setup_interrupt;
use crate::server::rpc::TakoServer;
use crate::server::state::StateRef;
use futures::TryFutureExt;
use crate::common::error::error;

const SYMLINK_PATH: &str = "hq-active-dir";

enum ServerStatus {
    None,
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
/// If an already running server is found, TODO.
pub async fn hyperqueue_start(rundir_path: PathBuf) -> crate::Result<()> {
    let directory = resolve_active_directory(rundir_path.clone());
    std::fs::create_dir_all(&directory)?;

    let rundir = RunDirectory::new(directory.clone())?;

    match get_server_status(&rundir).await.unwrap_or(ServerStatus::None) {
        ServerStatus::None | ServerStatus::Offline(_) => {
            log::info!("No online server found, starting a new server");
            start_server(rundir_path).await
        }
        ServerStatus::Online(_) => {
            // TODO: stop? stop if --forced?
            error(format!("Server at {} is already online, please stop it first", rundir_path.display()))
        }
    }
}

/// Returns either `path` if it doesn't contain `SYMLINK_PATH` or the target of `SYMLINK_PATH`.
fn resolve_active_directory(path: PathBuf) -> PathBuf {
    let symlink_path = path.join(SYMLINK_PATH);
    match std::fs::canonicalize(symlink_path) {
        Ok(p) => if p.is_dir() {
            return p;
        }
        _ => {}
    };
    path
}

async fn get_server_status(rundir: &RunDirectory) -> crate::Result<ServerStatus> {
    if !rundir.runfile().is_file() {
        return Ok(ServerStatus::None);
    }
    let runfile = load_runfile(rundir.runfile())?;

    if let Err(_) = TcpStream::connect(format!("{}:{}", runfile.hostname(), runfile.server_port())).await {
        return Ok(ServerStatus::Offline(runfile));
    }

    Ok(ServerStatus::Online(runfile))
}

async fn start_server(directory: PathBuf) -> crate::Result<()> {
    let mut end_rx = setup_interrupt();
    let end_flag = async move {
        end_rx.recv().await;
    };

    let client_listener = TcpListener::bind("0.0.0.0:0")
        .map_err(|e| GenericError(format!("Cannot create HQ server socket: {}", e)))
        .await?;
    let server_port = client_listener.local_addr()?.port();

    let state_ref = StateRef::new();
    let (tako_server, tako_future) = TakoServer::start(state_ref.clone()).await?;

    let runfile = Runfile::new(
        gethostname::gethostname().into_string().unwrap(),
        server_port,
        tako_server.worker_port()
    );
    initialize_directory(&directory, &runfile)?;

    let local_set = LocalSet::new();
    local_set.run_until(async move {
        tokio::select! {
            _ = end_flag => {},
            () = crate::server::client::handle_client_connections(state_ref, tako_server, client_listener) => {}
            r = tako_future => { r.expect("Tako has failed") }
        };
        Ok(())
    }).await
}

fn initialize_directory(directory: &PathBuf, runfile: &Runfile) -> crate::Result<()> {
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
    use crate::server::bootstrap::{get_server_status, resolve_active_directory, SYMLINK_PATH};
    use crate::common::rundir::{RunDirectory, Runfile, store_runfile};
    use super::ServerStatus;

    #[tokio::test]
    async fn test_status_empty_directory() {
        let tmp_dir = TempDir::new("foo").unwrap();
        let res = get_server_status(&RunDirectory::new(tmp_dir.into_path()).unwrap()).await.unwrap();
        assert!(matches!(res, ServerStatus::None));
    }

    #[tokio::test]
    async fn test_status_directory_with_runfile() {
        let tmp_dir = TempDir::new("foo").unwrap();
        let rundir = RunDirectory::new(tmp_dir.into_path()).unwrap();
        let runfile = Runfile::new(
            "foo".into(),
            42,
            43
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
            43
        );
        store_runfile(&runfile, rundir.runfile()).unwrap();

        let rundir = RunDirectory::new(resolve_active_directory(tmp_dir)).unwrap();
        let res = get_server_status(&rundir).await.unwrap();
        assert!(matches!(res, ServerStatus::Offline(_)));
    }
}
