use std::collections::VecDeque;
use std::future::Future;

use tako::messages::gateway::{ErrorResponse, FromGatewayMessage, ToGatewayMessage};
use tako::server::client::process_client_message;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;

use crate::common::error::error;
use crate::common::WrappedRcRefCell;
use crate::server::state::StateRef;
use tokio::time::Duration;
use orion::kdf::SecretKey;

struct Inner {
    sender: UnboundedSender<FromGatewayMessage>,
    responses: VecDeque<oneshot::Sender<ToGatewayMessage>>,
    worker_port: u16,
}

#[derive(Clone)]
pub struct TakoServer {
    inner: WrappedRcRefCell<Inner>
}

impl TakoServer {
    pub fn worker_port(&self) -> u16 {
        self.inner.get().worker_port
    }

    pub async fn send_message(&self, message: FromGatewayMessage) -> crate::Result<ToGatewayMessage> {
        let (sx, rx) = oneshot::channel::<ToGatewayMessage>();
        {
            let mut inner = self.inner.get_mut();
            inner.responses.push_back(sx);
            inner.sender.send(message).unwrap();
        }
        Ok(rx.await.unwrap())
    }

    pub async fn start(state_ref: StateRef, key: Option<SecretKey>) -> crate::Result<(TakoServer, impl Future<Output=crate::Result<()>>)> {
        let msd = Duration::from_millis(20);

        let (from_tako_sender, mut from_tako_receiver) = unbounded_channel::<ToGatewayMessage>();
        let (to_tako_sender, mut to_tako_receiver) = unbounded_channel::<FromGatewayMessage>();
        let (core_ref, comm_ref, server_future) = tako::server::server_start(
            "0.0.0.0:0".parse().unwrap(),
            key,
            msd,
            from_tako_sender.clone(),
            false,
        ).await?;

        let server = TakoServer {
            inner: WrappedRcRefCell::wrap(Inner {
                sender: to_tako_sender,
                responses: Default::default(),
                worker_port: core_ref.get().get_worker_listen_port(),
            })
        };
        let server2 = server.clone();

        let future = async move {
            let tako_msg_reader = async move {
                while let Some(message) = from_tako_receiver.recv().await {
                    match message {
                        ToGatewayMessage::TaskUpdate(msg) => state_ref.get_mut().process_task_update(msg),
                        ToGatewayMessage::TaskFailed(msg) => state_ref.get_mut().process_task_failed(msg),
                        m => {
                            let response = server2.inner.get_mut().responses.pop_front().unwrap();
                            response.send(m).unwrap();
                        }
                    }
                }
                error("Tako receive stream terminated".into()) as crate::Result<()>
            };
            let tako_msg_sender = async move {
                while let Some(message) = to_tako_receiver.recv().await {
                    let error = process_client_message(&core_ref, &comm_ref, &from_tako_sender, message).await;
                    if let Some(message) = error {
                        from_tako_sender.send(ToGatewayMessage::Error(ErrorResponse {
                            message
                        })).unwrap();
                    }
                }

                error("Tako send stream terminated".into()) as crate::Result<()>
            };

            tokio::select! {
                r = tako_msg_reader => { r },
                r = tako_msg_sender => { r }
                r = server_future => { r.map_err(|e| e.into()) }
            }
        };

        Ok((server, future))
    }
}

#[cfg(test)]
mod tests {
    use tako::messages::gateway::{FromGatewayMessage, ServerInfo, ToGatewayMessage};
    use tokio::net::TcpStream;

    use crate::server::rpc::TakoServer;
    use crate::server::state::StateRef;
    use crate::utils::test_utils::run_concurrent;

    #[tokio::test]
    async fn test_server_connect_worker() {
        let state = StateRef::new();
        let (server, _fut) = TakoServer::start(state, None).await.unwrap();
        TcpStream::connect(format!("127.0.0.1:{}", server.worker_port())).await.unwrap();
    }

    #[tokio::test]
    async fn test_server_server_info() {
        let state = StateRef::new();
        let (server, fut) = TakoServer::start(state, None).await.unwrap();
        run_concurrent(fut, async move {
            assert!(matches!(server.send_message(FromGatewayMessage::ServerInfo).await.unwrap(),
                ToGatewayMessage::ServerInfo(ServerInfo { worker_listen_port })
                if worker_listen_port == server.worker_port()
            ));
        }).await;
    }
}
