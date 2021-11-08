use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use derive_builder::Builder;
use orion::auth::SecretKey;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle, LocalSet};
use tokio::time::timeout;

use crate::common::Map;
use crate::messages::gateway::{
    FromGatewayMessage, NewTasksMessage, NewTasksResponse, ObserveTasksMessage, TaskDef,
    ToGatewayMessage,
};
use crate::server::client::process_client_message;
use crate::server::comm::CommSenderRef;
use crate::server::core::CoreRef;
use crate::tests::integration::utils::api::{wait_for_tasks, TaskWaitResultMap};
use crate::tests::integration::utils::worker::{start_worker, WorkerContext, WorkerHandle};
use crate::{TaskId, WorkerId};

use super::macros::wait_for_msg;
use super::worker::WorkerConfigBuilder;

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
    server_to_client: UnboundedReceiver<ToGatewayMessage>,
    client_sender: UnboundedSender<ToGatewayMessage>,
    pub(super) core_ref: CoreRef,
    comm_ref: CommSenderRef,
    pub(super) secret_key: Option<Arc<SecretKey>>,
    workers: Map<WorkerId, WorkerContext>,
}

impl ServerHandle {
    pub async fn send(&self, msg: FromGatewayMessage) {
        assert_eq!(self.send_with_response(msg).await, None);
    }
    pub async fn send_with_response(&self, msg: FromGatewayMessage) -> Option<String> {
        process_client_message(&self.core_ref, &self.comm_ref, &self.client_sender, msg).await
    }
    pub async fn recv(&mut self) -> ToGatewayMessage {
        match timeout(WAIT_TIMEOUT, self.server_to_client.recv()).await {
            Ok(result) => result.expect("Expected message to be received"),
            Err(_) => panic!("Timeout reached when receiving a message"),
        }
    }

    /// Receive messages until timeout is reached.
    /// When check_fn returns `Some`, the loop will end.
    ///
    /// Use this to wait for a specific message to be received.
    pub async fn recv_msg<CheckMsg: FnMut(ToGatewayMessage) -> Option<T>, T>(
        &mut self,
        mut check_fn: CheckMsg,
    ) -> T {
        let fut = async move {
            loop {
                let msg = self.recv().await;
                let formatted = format!("{:?}", msg);
                if let Some(result) = check_fn(msg) {
                    break result;
                }
                println!("Received out-of-band message {}", formatted);
            }
        };
        match timeout(WAIT_TIMEOUT, fut).await {
            Ok(result) => result,
            Err(_) => panic!("Timeout reached when waiting for a specific message"),
        }
    }

    pub async fn start_worker(
        &mut self,
        config: WorkerConfigBuilder,
    ) -> anyhow::Result<WorkerHandle> {
        let (handle, ctx) =
            start_worker(self.core_ref.clone(), self.secret_key.clone(), config).await?;
        assert!(self.workers.insert(handle.id, ctx).is_none());
        Ok(handle)
    }

    pub async fn kill_worker(&mut self, id: WorkerId) {
        let ctx = self.workers.remove(&id).unwrap();
        ctx.abort().await;
    }

    pub async fn submit(&mut self, tasks: Vec<TaskDef>) -> Vec<TaskId> {
        let ids = tasks.iter().map(|t| t.id).collect();
        let msg = NewTasksMessage { tasks };
        self.send(FromGatewayMessage::NewTasks(msg)).await;
        wait_for_msg!(self, ToGatewayMessage::NewTasksResponse(NewTasksResponse { .. }) => ());
        ids
    }

    pub async fn wait(&mut self, tasks: &[TaskId]) -> TaskWaitResultMap {
        let msg = ObserveTasksMessage {
            tasks: tasks.iter().map(|&v| v).collect(),
        };
        self.send(FromGatewayMessage::ObserveTasks(msg)).await;
        timeout(WAIT_TIMEOUT, wait_for_tasks(self, tasks.to_vec()))
            .await
            .unwrap()
    }
}

async fn create_handle(
    builder: ServerConfigBuilder,
) -> (ServerHandle, impl Future<Output = crate::Result<()>>) {
    let config: ServerConfig = builder.build().unwrap();

    let listen_address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let secret_key = match config.secret_key {
        ServerSecretKey::AutoGenerate => Some(Arc::new(Default::default())),
        ServerSecretKey::Custom(key) => key.map(Arc::new),
    };

    let (client_sender, client_receiver) = unbounded_channel::<ToGatewayMessage>();

    let (core_ref, comm_ref, server_future) = crate::server::server_start(
        listen_address,
        secret_key.clone(),
        config.msd,
        client_sender.clone(),
        config.panic_on_worker_lost,
        config.idle_timeout,
        None,
    )
    .await
    .expect("Could not start server");

    (
        ServerHandle {
            server_to_client: client_receiver,
            client_sender,
            core_ref,
            comm_ref,
            secret_key,
            workers: Default::default(),
        },
        server_future,
    )
}

pub struct ServerCompletion {
    core_ref: CoreRef,
    set: LocalSet,
    server_handle: JoinHandle<crate::Result<()>>,
}

impl ServerCompletion {
    /// Waits until all RPC connections (workers, custom connections) are finished.
    pub async fn finish_rpc(&mut self) {
        timeout(WAIT_TIMEOUT, async move {
            let handles = self.core_ref.get_mut().take_rpc_handles();
            for handle in handles {
                self.set.run_until(handle).await.unwrap();
            }
        })
        .await
        .unwrap();
    }

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

pub async fn run_test<
    CreateTestFut: FnOnce(ServerHandle) -> TestFut,
    TestFut: Future<Output = ()>,
>(
    builder: ServerConfigBuilder,
    create_fut: CreateTestFut,
) -> ServerCompletion {
    let (handle, server_future) = create_handle(builder).await;
    let core_ref = handle.core_ref.clone();
    let test_future = create_fut(handle);

    let set = tokio::task::LocalSet::new();
    let server_handle = set.spawn_local(server_future);
    set.run_until(test_future).await;

    ServerCompletion {
        core_ref,
        server_handle,
        set,
    }
}
