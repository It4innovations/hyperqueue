use std::path::Path;
use std::thread::JoinHandle;
use std::time::Duration;

use hyperqueue::common::utils::network::get_hostname;
use tokio::task::LocalSet;

use hyperqueue::worker::bootstrap::{finalize_configuration, initialize_worker};
use tako::resources::ResourceDescriptor;
use tako::worker::ServerLostPolicy;
use tako::worker::WorkerConfiguration;

pub struct RunningWorker {
    #[allow(unused)]
    thread: JoinHandle<()>,
}

impl RunningWorker {
    pub fn start(server_dir: &Path, cores: usize) -> anyhow::Result<Self> {
        let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<()>>();

        let server_dir = server_dir.to_path_buf();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Cannot create Tokio runtime");

            let worker_dir = server_dir.join("worker");
            let work_dir = worker_dir.join("workdir");
            let log_dir = worker_dir.join("logs");
            let mut configuration = WorkerConfiguration {
<<<<<<< HEAD
                resources: ResourceDescriptor {
                    cpus: vec![(0..cores).map(|id| CpuId::from(id as u32)).collect()],
=======
                /*resources: ResourceDescriptor {
                    cpus: vec![vec![CpuId::from(cores as u32)]],
>>>>>>> 5a2a70b5 (Unified resource management)
                    generic: vec![],
                },*/
                resources: todo!(),
                listen_address: Default::default(),
                hostname: get_hostname(None),
                work_dir,
                log_dir,
                heartbeat_interval: Duration::from_secs(10),
                send_overview_interval: None,
                idle_timeout: None,
                time_limit: None,
                on_server_lost: ServerLostPolicy::Stop,
                extra: Default::default(),
            };
            finalize_configuration(&mut configuration);

            log::info!("Starting worker: {configuration:?}");

            let main_future = async move {
                let set = LocalSet::new();
                set.run_until(async move {
                    match initialize_worker(&server_dir, configuration).await {
                        Ok((future, _)) => {
                            tx.send(Ok(())).unwrap();
                            if let Err(error) = future.await {
                                log::error!("HyperQueue worker ended with error: {error:?}");
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
        rx.blocking_recv()
            .expect("Could not receive response from worker")?;
        Ok(Self { thread })
    }
}
