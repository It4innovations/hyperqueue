use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use derive_builder::Builder;
use orion::auth::SecretKey;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle, LocalSet};
use tokio::time::timeout;

use crate::gateway::{
    FromGatewayMessage, NewTasksMessage, NewTasksResponse, ObserveTasksMessage,
    SharedTaskConfiguration, StopWorkerRequest, TaskConfiguration, ToGatewayMessage,
};
use crate::internal::common::{Map, Set};
use crate::internal::server::client::process_client_message;
use crate::internal::server::comm::CommSenderRef;
use crate::internal::server::core::CoreRef;
use crate::internal::tests::integration::utils::api::{wait_for_tasks, TaskWaitResultMap};
use crate::internal::tests::integration::utils::worker::{
    start_worker, WorkerContext, WorkerHandle,
};
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
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    secret_key: Option<Arc<SecretKey>>,
    workers: Map<WorkerId, WorkerContext>,
    out_of_band_messages: Vec<ToGatewayMessage>,
}

pub enum MessageFilter<T> {
    Expected(T),
    Unexpected(ToGatewayMessage),
}

impl<T> From<ToGatewayMessage> for MessageFilter<T> {
    fn from(msg: ToGatewayMessage) -> Self {
        Self::Unexpected(msg)
    }
}

impl ServerHandle {
    /// Removes currently received out-of-band messages, so that the next `recv_msg` call will
    /// receive the latest data.
    pub fn flush_messages(&mut self) {
        self.out_of_band_messages.clear();
    }

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
    pub async fn recv_msg<CheckMsg: FnMut(ToGatewayMessage) -> MessageFilter<T>, T>(
        &mut self,
        check_fn: CheckMsg,
    ) -> T {
        self.recv_msg_with_timeout(check_fn, WAIT_TIMEOUT)
            .await
            .expect("Timeout reached when waiting for a specific message")
    }

    pub async fn recv_msg_with_timeout<CheckMsg: FnMut(ToGatewayMessage) -> MessageFilter<T>, T>(
        &mut self,
        mut check_fn: CheckMsg,
        max_duration: Duration,
    ) -> Option<T> {
        let mut found_oob = None;
        self.out_of_band_messages = std::mem::take(&mut self.out_of_band_messages)
            .into_iter()
            .filter_map(|msg| {
                if found_oob.is_none() {
                    match check_fn(msg) {
                        MessageFilter::Expected(response) => {
                            found_oob = Some(response);
                            None
                        }
                        MessageFilter::Unexpected(msg) => Some(msg),
                    }
                } else {
                    Some(msg)
                }
            })
            .collect();
        if let Some(found) = found_oob {
            return Some(found);
        }

        let fut = async move {
            loop {
                let msg = self.recv().await;
                match check_fn(msg) {
                    MessageFilter::Expected(response) => {
                        break response;
                    }
                    MessageFilter::Unexpected(msg) => {
                        println!("Received out-of-band message {:?}", msg);
                        self.out_of_band_messages.push(msg);
                    }
                }
            }
        };
        match timeout(max_duration, fut).await {
            Ok(result) => Some(result),
            Err(_) => None,
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
        self.send(FromGatewayMessage::StopWorker(StopWorkerRequest {
            worker_id,
        }))
        .await;
    }

    pub async fn submit(
        &mut self,
        tasks_and_confs: (Vec<TaskConfiguration>, Vec<SharedTaskConfiguration>),
    ) -> Vec<TaskId> {
        let (tasks, configurations) = tasks_and_confs;
        let ids: Vec<TaskId> = tasks.iter().map(|t| t.id).collect();
        assert_eq!(ids.iter().collect::<Set<_>>().len(), ids.len());
        let msg = NewTasksMessage {
            tasks,
            shared_data: configurations,
        };
        self.send(FromGatewayMessage::NewTasks(msg)).await;
        wait_for_msg!(self, ToGatewayMessage::NewTasksResponse(NewTasksResponse { .. }) => ());
        ids
    }

    pub async fn wait<T: Into<TaskId> + Copy>(&mut self, tasks: &[T]) -> TaskWaitResultMap {
        let msg = ObserveTasksMessage {
            tasks: tasks.iter().map(|&v| v.into()).collect(),
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

    let (server_ref, server_future) = crate::server::server_start(
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

    let (core_ref, comm_ref) = server_ref.split();

    (
        ServerHandle {
            server_to_client: client_receiver,
            client_sender,
            core_ref,
            comm_ref,
            secret_key,
            workers: Default::default(),
            out_of_band_messages: Default::default(),
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

pub async fn run_test<
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
