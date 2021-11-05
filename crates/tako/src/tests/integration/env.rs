use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use derive_builder::Builder;
use orion::auth::SecretKey;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle, LocalSet};

use crate::messages::gateway::{FromGatewayMessage, OverviewRequest, ToGatewayMessage};
use crate::server::client::process_client_message;
use crate::server::comm::CommSenderRef;
use crate::server::core::CoreRef;

#[derive(Builder, Default)]
pub struct ServerConfig {
    #[builder(default = "Duration::from_millis(20)")]
    msd: Duration,
    #[builder(default)]
    panic_on_worker_lost: bool,
    #[builder(default)]
    idle_timeout: Option<Duration>,
}

pub struct ServerHandle {
    server_to_client: UnboundedReceiver<ToGatewayMessage>,
    client_sender: UnboundedSender<ToGatewayMessage>,
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
}

const RECV_TIMEOUT: Duration = Duration::from_secs(5);

impl ServerHandle {
    pub async fn send(&self, msg: FromGatewayMessage) {
        assert_eq!(self.send_with_response(msg).await, None);
    }
    pub async fn send_with_response(&self, msg: FromGatewayMessage) -> Option<String> {
        process_client_message(&self.core_ref, &self.comm_ref, &self.client_sender, msg).await
    }
    pub async fn recv(&mut self) -> ToGatewayMessage {
        match tokio::time::timeout(RECV_TIMEOUT, self.server_to_client.recv()).await {
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
                if let Some(result) = check_fn(msg) {
                    break result;
                }
            }
        };
        match tokio::time::timeout(RECV_TIMEOUT, fut).await {
            Ok(result) => result,
            Err(_) => panic!("Timeout reached when waiting for a specific message"),
        }
    }
}

async fn create_handle(
    builder: ServerConfigBuilder,
) -> (ServerHandle, impl Future<Output = crate::Result<()>>) {
    let env: ServerConfig = builder.build().unwrap();

    let listen_address = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 0);
    let secret_key = Some(Arc::new(SecretKey::default()));

    let (client_sender, client_receiver) = unbounded_channel::<ToGatewayMessage>();

    let (core_ref, comm_ref, server_future) = crate::server::server_start(
        listen_address,
        secret_key,
        env.msd,
        client_sender.clone(),
        env.panic_on_worker_lost,
        env.idle_timeout,
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
        },
        server_future,
    )
}

pub async fn run_test<
    CreateTestFut: FnOnce(ServerHandle) -> TestFut,
    TestFut: Future<Output = ()>,
>(
    builder: ServerConfigBuilder,
    create_fut: CreateTestFut,
) -> (LocalSet, JoinHandle<crate::Result<()>>) {
    let (handle, server_future) = create_handle(builder).await;
    let test_future = create_fut(handle);

    let set = tokio::task::LocalSet::new();
    let handle = set.spawn_local(server_future);
    set.run_until(test_future).await;
    (set, handle)
}

/// Helper macro that simplifies the usage of [`ServerHandle::recv_msg`]
macro_rules! wait_for_msg {
    ($handler: expr, $matcher:pat $(=> $result:expr)?) => {
        $handler.recv_msg(|msg| match msg {
            $matcher => ::std::option::Option::Some(($($result),*)),
            _ => None,
        }).await
    };
}
pub(super) use wait_for_msg;

/// Helper utilities for common client messages
pub async fn request_get_overview(handler: &ServerHandle) {
    handler
        .send(FromGatewayMessage::GetOverview(OverviewRequest {
            enable_hw_overview: false,
        }))
        .await;
}
