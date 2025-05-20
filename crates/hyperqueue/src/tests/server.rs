use crate::client::globalsettings::GlobalSettings;
use crate::client::output::cli::CliOutput;
use crate::server::bootstrap::{ServerConfig, get_client_session, initialize_server};
use crate::transfer::connection::ClientSession;
use cli_table::ColorChoice;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::LocalSet;
use tokio::time::timeout;

pub struct RunningHqServer {
    dir: PathBuf,
    notify_quit: bool,
}

impl RunningHqServer {
    /// Do not explicitly stop the server, it will be stopped by the test itself.
    pub fn do_not_notify(&mut self) {
        self.notify_quit = false;
    }

    pub async fn client(&self) -> ClientSession {
        get_client_session(&self.dir)
            .await
            .expect("Cannot connect to server")
    }
}

/// Start the whole HQ server, including almost all bells and whistles,
/// and then run a future that will perform the actual test on it.
/// After the future finishes, the server will be shutted down.
///
/// If you need to configure the server, add parameters (or some builder) here.
pub async fn run_hq_test<F, Fut>(test_fn: F)
where
    // We pass the server by value and return it because of problematic lifetimes
    // with the closure + future combination. Async closures should fix this.
    F: FnOnce(RunningHqServer) -> Fut,
    Fut: Future<Output = anyhow::Result<RunningHqServer>>,
{
    let tmp_dir = TempDir::with_prefix("hq-test").unwrap();

    let gsettings = GlobalSettings::new(
        tmp_dir.path().to_path_buf(),
        Box::new(CliOutput::new(ColorChoice::Never)),
    );
    let server_cfg = ServerConfig {
        worker_host: "localhost".to_string(),
        client_host: "localhost".to_string(),
        idle_timeout: None,
        client_port: None,
        worker_port: None,
        journal_path: None,
        journal_flush_period: Duration::from_secs(30),
        worker_secret_key: None,
        client_secret_key: None,
        server_uid: None,
    };
    let (fut, notify, _state, _senders) =
        initialize_server(&gsettings, server_cfg, 1.into(), 1, None)
            .await
            .unwrap();
    let localset = LocalSet::new();

    // Run the server in the background, concurrently with the testing future
    let server_fut = localset.spawn_local(fut);

    let server = RunningHqServer {
        dir: tmp_dir.path().to_path_buf(),
        notify_quit: true,
    };
    // Run the test itself. If it fails, we still try to finish the server itself,
    // for better error propagation.
    let test_error = localset.run_until(test_fn(server)).await;
    let notify_quit = test_error.as_ref().map(|s| s.notify_quit).unwrap_or(true);

    // Tell the server to quit if the test didn't opt out
    if notify_quit {
        notify.notify_one();
    }

    // Wait for it to quit. Panics will be propagated from `run_until`.
    let server_error = localset
        .run_until(async move {
            match timeout(Duration::from_secs(5), server_fut).await {
                Ok(res) => res.unwrap(),
                Err(_) => {
                    panic!("The server has not finished in 5 seconds. Maybe there is a deadlock?")
                }
            }
        })
        .await;

    // Wait for any background processes to quit
    localset.await;

    // Propagate the errors
    if server_error.is_err() && test_error.is_err() {
        eprintln!("{server_error:?}");
        test_error.expect("Test failed");
    } else {
        test_error.expect("Test failed");
        server_error.expect("Server failed");
    }
}
