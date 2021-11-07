use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tempdir::TempDir;

use crate::common::error::DsError;
use crate::common::resources::ResourceDescriptor;
use crate::messages::common::{ProgramDefinition, WorkerConfiguration};
use crate::server::core::CoreRef;
use derive_builder::Builder;
use orion::auth::SecretKey;

use crate::worker::launcher::command_from_definitions;
use crate::worker::rpc::run_worker;
use crate::worker::state::WorkerState;
use crate::worker::task::TaskRef;
use crate::worker::taskenv::{StopReason, TaskResult};
use crate::WorkerId;

pub enum WorkerSecretKey {
    Server,
    Custom(Option<SecretKey>),
}

impl Default for WorkerSecretKey {
    fn default() -> Self {
        Self::Server
    }
}

#[derive(Builder, Default)]
#[builder(pattern = "owned")]
pub struct WorkerConfig {
    #[builder(default)]
    idle_timeout: Option<Duration>,
    #[builder(default)]
    hw_state_poll_interval: Option<Duration>,
    #[builder(default)]
    secret_key: WorkerSecretKey,
}

pub(super) fn create_worker_configuration(
    builder: WorkerConfigBuilder,
) -> (WorkerConfiguration, WorkerSecretKey) {
    let config: WorkerConfig = builder.build().unwrap();
    let secret_key = config.secret_key;

    (
        WorkerConfiguration {
            resources: ResourceDescriptor {
                cpus: vec![vec![1]],
                generic: vec![],
            },
            listen_address: "".to_string(),
            hostname: "".to_string(),
            work_dir: Default::default(),
            log_dir: Default::default(),
            heartbeat_interval: Duration::from_secs(1),
            hw_state_poll_interval: config.hw_state_poll_interval,
            idle_timeout: config.idle_timeout,
            time_limit: None,
            extra: Default::default(),
        },
        secret_key,
    )
}

/// This data is available for tests
pub struct WorkerHandle {
    pub workdir: PathBuf,
    pub logdir: PathBuf,
    pub id: WorkerId,
}

/// This data has to live as long as the worker lives
pub struct WorkerContext {
    #[allow(dead_code)]
    tmpdir: TempDir,
    end_flag: tokio::sync::oneshot::Sender<()>,
    thread_handle: JoinHandle<()>,
}

impl WorkerContext {
    pub(super) async fn abort(self) {
        self.end_flag.send(()).unwrap();
        self.thread_handle.join().unwrap();
    }
}

pub(super) async fn start_worker(
    core_ref: CoreRef,
    server_secret_key: Option<Arc<SecretKey>>,
    config: WorkerConfigBuilder,
) -> anyhow::Result<(WorkerHandle, WorkerContext)> {
    let port = core_ref.get().get_worker_listen_port();
    let (mut configuration, worker_secret_key) = create_worker_configuration(config);
    let tmpdir = TempDir::new("tako").unwrap();
    let workdir = tmpdir.path().to_path_buf().join("work");
    let logdir = tmpdir.path().to_path_buf().join("logs");

    configuration.work_dir = workdir.clone();
    configuration.log_dir = logdir.clone();

    let server_address: SocketAddr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);

    let (id_tx, id_rx) = tokio::sync::oneshot::channel();
    let (end_tx, end_rx) = tokio::sync::oneshot::channel();
    let secret_key = match worker_secret_key {
        WorkerSecretKey::Server => server_secret_key,
        WorkerSecretKey::Custom(key) => key.map(Arc::new),
    };

    let thread_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let future = async move {
            let result = run_worker(
                server_address,
                configuration,
                secret_key,
                Box::new(launcher),
            )
            .await;
            match result {
                Ok(((worker_id, _), worker_future)) => {
                    id_tx.send(Ok(worker_id)).unwrap();
                    tokio::select! {
                        _ = worker_future => {},
                        _ = end_rx => {}
                    }
                }
                Err(e) => {
                    id_tx.send(Err(e)).unwrap();
                }
            }
        };
        runtime.block_on(future);
    });
    let worker_id = id_rx.await??;

    let handle = WorkerHandle {
        workdir,
        logdir,
        id: worker_id,
    };
    let ctx = WorkerContext {
        tmpdir,
        thread_handle,
        end_flag: end_tx,
    };
    Ok((handle, ctx))
}

async fn launcher_main(task_ref: TaskRef) -> crate::Result<()> {
    log::debug!(
        "Starting program launcher {} {:?} {:?}",
        task_ref.get().id,
        &task_ref.get().configuration.resources,
        task_ref.get().resource_allocation()
    );

    let program: ProgramDefinition = {
        let task = task_ref.get();
        rmp_serde::from_slice(&task.configuration.body)?
    };

    let mut command = command_from_definitions(&program)?;
    let status = command.status().await?;
    if !status.success() {
        let code = status.code().unwrap_or(-1);
        return crate::Result::Err(DsError::GenericError(format!(
            "Program terminated with exit code {}",
            code
        )));
    }
    Ok(())
}

fn launcher(
    _state: &WorkerState,
    task_ref: &TaskRef,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> Pin<Box<dyn Future<Output = crate::Result<TaskResult>> + 'static>> {
    let task_ref = task_ref.clone();

    Box::pin(async move {
        tokio::select! {
            biased;
                r = end_receiver => {
                    Ok(r.unwrap().into())
                }
                r = launcher_main(task_ref) => {
                    r?;
                    Ok(TaskResult::Finished)
                }
        }
    })
}
