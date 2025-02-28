use hyperqueue::client::globalsettings::GlobalSettings;
use hyperqueue::client::output::cli::CliOutput;
use hyperqueue::common::utils::network::get_hostname;
use hyperqueue::server::bootstrap::{ServerConfig, initialize_server};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::LocalSet;

pub struct RunningServer {
    thread: JoinHandle<()>,
    stop_flag: Arc<Notify>,
}

impl RunningServer {
    pub fn start(server_dir: PathBuf) -> anyhow::Result<Self> {
        let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<Arc<Notify>>>();

        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Cannot create Tokio runtime");

            let settings = GlobalSettings::new(
                server_dir,
                Box::new(CliOutput::new(termcolor::ColorChoice::Never)),
            );
            let host = get_hostname(None);
            let config = ServerConfig {
                client_host: host.clone(),
                worker_host: host,
                idle_timeout: None,
                client_port: None,
                worker_port: None,
                journal_path: None,
                client_secret_key: None,
                worker_secret_key: None,
                server_uid: None,
                journal_flush_period: Duration::from_secs(30),
            };

            let main_future = async move {
                let set = LocalSet::new();

                set.run_until(async move {
                    match initialize_server(&settings, config, 0.into(), 1, None).await {
                        Ok((future, end_flag, _, _)) => {
                            tx.send(Ok(end_flag)).unwrap();
                            if let Err(error) = future.await {
                                log::error!("HyperQueue server ended with error: {error:?}");
                            }
                        }
                        Err(error) => {
                            tx.send(Err(error)).unwrap();
                        }
                    }
                })
                .await;
            };
            runtime.block_on(main_future);
        });
        let stop_flag = rx
            .blocking_recv()
            .expect("Could not receive response from server")?;
        Ok(Self { thread, stop_flag })
    }

    pub fn stop(self) {
        log::info!("Attempting to stop server");
        self.stop_flag.notify_one();
        self.thread.join().expect("Server did not stop correctly");
    }
}
