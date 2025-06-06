use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use derive_builder::Builder;
use orion::auth::SecretKey;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::{JoinHandle, LocalSet};
use tokio::time::timeout;

use super::worker::WorkerConfigBuilder;
use crate::control::ServerRef;
use crate::events::EventProcessor;
use crate::gateway::{LostWorkerReason, SharedTaskConfiguration, TaskConfiguration, TaskSubmit};
use crate::internal::common::{Map, Set};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::tests::integration::utils::api::{WaitResult, wait_for_tasks};
use crate::internal::tests::integration::utils::worker::{
    WorkerContext, WorkerHandle, start_worker,
};
use crate::task::SerializedTaskContext;
use crate::tests::integration::utils::api::wait_for_worker_connected;
use crate::worker::{WorkerConfiguration, WorkerOverview};
use crate::{InstanceId, TaskId, WorkerId, WrappedRcRefCell};

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

pub enum ServerSecretKey {
    AutoGenerate,
    Custom(Option<SecretKey>),
}

impl Default for ServerSecretKey {
    fn default() -> Self {
        Self::AutoGenerate
    }
}

#[derive(Builder, Default)]
#[builder(pattern = "owned")]
pub struct ServerConfig {
    #[builder(default = "Duration::from_millis(20)")]
    msd: Duration,
    #[builder(default)]
    panic_on_worker_lost: bool,
    #[builder(default)]
    idle_timeout: Option<Duration>,
    #[builder(default)]
    secret_key: ServerSecretKey,
}

pub struct ServerHandle {
    pub server_ref: ServerRef,
    pub client: AsyncTestClientProcessorRef,
    secret_key: Option<Arc<SecretKey>>,
    workers: Map<WorkerId, WorkerContext>,
}

impl ServerHandle {
    pub async fn start_worker(
        &mut self,
        config: WorkerConfigBuilder,
    ) -> anyhow::Result<WorkerHandle> {
        let (handle, ctx) = start_worker(
            self.server_ref.get_worker_listen_port(),
            self.secret_key.clone(),
            config,
        )
        .await?;
        assert!(self.workers.insert(handle.id, ctx).is_none());
        Ok(handle)
    }

    pub async fn start_workers<InitFn: Fn() -> WorkerConfigBuilder>(
        &mut self,
        init_fn: InitFn,
        count: usize,
    ) -> anyhow::Result<Vec<WorkerHandle>> {
        let mut handles = vec![];
        for _ in 0..count {
            handles.push(self.start_worker(init_fn()).await);
        }
        handles.into_iter().collect()
    }

    pub async fn kill_worker(&mut self, id: WorkerId) {
        let ctx = self.workers.remove(&id).unwrap();
        ctx.kill().await;
    }

    pub async fn stop_worker(&mut self, worker_id: WorkerId) {
        self.server_ref.stop_worker(worker_id).unwrap();
    }

    pub async fn submit(
        &mut self,
        tasks_and_confs: (Vec<TaskConfiguration>, Vec<SharedTaskConfiguration>),
    ) -> Vec<TaskId> {
        let (tasks, configurations) = tasks_and_confs;
        let ids: Vec<TaskId> = tasks.iter().map(|t| t.id).collect();
        assert_eq!(ids.iter().collect::<Set<_>>().len(), ids.len());
        self.server_ref
            .add_new_tasks(TaskSubmit {
                tasks,
                shared_data: configurations,
                adjust_instance_id_and_crash_counters: Default::default(),
            })
            .unwrap();
        ids
    }

    pub async fn wait<T: Into<TaskId> + Copy>(&mut self, tasks: &[T]) -> WaitResult {
        timeout(WAIT_TIMEOUT, wait_for_tasks(self, tasks))
            .await
            .unwrap()
    }
}

#[derive(Clone)]
pub enum TestTaskState {
    Running(WorkerId),
    Finished(Option<WorkerId>),
    Failed(Option<WorkerId>, TaskFailInfo),
}

impl TestTaskState {
    pub fn is_terminated(&self) -> bool {
        match self {
            TestTaskState::Running(..) => false,
            TestTaskState::Finished(..) | TestTaskState::Failed(..) => true,
        }
    }

    pub fn assert_error_message(&self, message: &str) {
        assert!(matches!(self, TestTaskState::Failed(_, info) if info.message == message))
    }

    pub fn worker_id(&self) -> Option<WorkerId> {
        match self {
            TestTaskState::Running(worker_id) => Some(*worker_id),
            TestTaskState::Finished(worker_id) | TestTaskState::Failed(worker_id, _) => *worker_id,
        }
    }
}

#[derive(Debug)]
pub struct TestWorkerState {
    pub configuration: WorkerConfiguration,
    pub lost_reason: Option<LostWorkerReason>,
}

pub struct AsyncTestClientProcessor {
    pub notify: Rc<Notify>,
    pub task_state: Map<TaskId, TestTaskState>,
    pub overviews: Map<WorkerId, Box<WorkerOverview>>,
    pub worker_state: Map<WorkerId, TestWorkerState>,
}

pub type AsyncTestClientProcessorRef = WrappedRcRefCell<AsyncTestClientProcessor>;

impl AsyncTestClientProcessorRef {
    fn update_state(&self, task_id: TaskId, state: TestTaskState) {
        let mut inner = self.get_mut();
        inner.task_state.insert(task_id, state);
        inner.notify.notify_one();
    }

    pub fn clear(&self) {
        let mut inner = self.get_mut();
        inner.task_state.clear();
        inner.overviews.clear();
    }
}

impl EventProcessor for AsyncTestClientProcessorRef {
    fn on_task_finished(&mut self, task_id: TaskId) {
        let worker_id = self
            .get()
            .task_state
            .get(&task_id)
            .and_then(|s| s.worker_id());
        self.update_state(task_id, TestTaskState::Finished(worker_id))
    }

    fn on_task_started(
        &mut self,
        task_id: TaskId,
        _instance_id: InstanceId,
        worker_ids: &[WorkerId],
        _context: SerializedTaskContext,
    ) {
        self.update_state(task_id, TestTaskState::Running(worker_ids[0]));
    }

    fn on_task_error(
        &mut self,
        task_id: TaskId,
        _consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) -> Vec<TaskId> {
        let worker_id = self
            .get()
            .task_state
            .get(&task_id)
            .and_then(|s| s.worker_id());
        self.update_state(task_id, TestTaskState::Failed(worker_id, error_info));
        Vec::new()
    }

    fn on_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        self.get_mut().worker_state.insert(
            worker_id,
            TestWorkerState {
                configuration: configuration.clone(),
                lost_reason: None,
            },
        );
    }

    fn on_worker_lost(
        &mut self,
        worker_id: WorkerId,
        _running_tasks: &[TaskId],
        reason: LostWorkerReason,
    ) {
        self.get_mut()
            .worker_state
            .get_mut(&worker_id)
            .unwrap()
            .lost_reason = Some(reason);
        self.get().notify.notify_one();
    }

    fn on_worker_overview(&mut self, overview: Box<WorkerOverview>) {
        self.get_mut().overviews.insert(overview.id, overview);
        self.get().notify.notify_one();
    }
}

async fn create_handle(
    builder: ServerConfigBuilder,
) -> (ServerHandle, impl Future<Output = crate::Result<()>>) {
    let config: ServerConfig = builder.build().unwrap();

    let listen_address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let listener = TcpListener::bind(listen_address).await.unwrap();

    let secret_key = match config.secret_key {
        ServerSecretKey::AutoGenerate => Some(Arc::new(Default::default())),
        ServerSecretKey::Custom(key) => key.map(Arc::new),
    };

    let (server_ref, server_future) = crate::control::server_start(
        listener,
        secret_key.clone(),
        config.msd,
        config.panic_on_worker_lost,
        config.idle_timeout,
        None,
        "testuid".to_string(),
        1.into(),
    )
    .expect("Could not start server");

    let client_ref = WrappedRcRefCell::wrap(AsyncTestClientProcessor {
        notify: Rc::new(Notify::new()),
        task_state: Default::default(),
        overviews: Default::default(),
        worker_state: Default::default(),
    });
    server_ref.set_client_events(Box::new(client_ref.clone()));
    (
        ServerHandle {
            server_ref,
            secret_key,
            client: client_ref,
            workers: Default::default(),
        },
        server_future,
    )
}

pub struct ServerCompletion {
    set: LocalSet,
    server_handle: JoinHandle<crate::Result<()>>,
}

impl ServerCompletion {
    /// Finish the main server future.
    pub async fn finish(self) {
        timeout(WAIT_TIMEOUT, async move {
            self.set
                .run_until(self.server_handle)
                .await
                .unwrap()
                .unwrap();
            self.set.await;
        })
        .await
        .unwrap();
    }
}

pub async fn run_server_test<
    CreateTestFut: FnOnce(ServerHandle) -> TestFut,
    TestFut: Future<Output = ()>,
>(
    builder: ServerConfigBuilder,
    create_fut: CreateTestFut,
) -> ServerCompletion {
    let (handle, server_future) = create_handle(builder).await;
    let test_future = create_fut(handle);

    let set = tokio::task::LocalSet::new();
    let server_handle = set.spawn_local(server_future);
    set.run_until(test_future).await;

    ServerCompletion { server_handle, set }
}
