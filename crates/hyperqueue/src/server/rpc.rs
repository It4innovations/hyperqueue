use std::collections::VecDeque;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use orion::kdf::SecretKey;
use tako::gateway::{FromGatewayMessage, ToGatewayMessage};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::Duration;

use crate::common::error::error;
use crate::server::state::StateRef;
use crate::stream::server::control::StreamServerControlMessage;
use crate::stream::server::rpc::start_stream_server;
use crate::WrappedRcRefCell;

struct Inner {
    tako_sender: UnboundedSender<FromGatewayMessage>,
    tako_responses: VecDeque<oneshot::Sender<ToGatewayMessage>>,
    stream_server_control: UnboundedSender<StreamServerControlMessage>,

    worker_port: u16,
}

#[derive(Clone)]
pub struct Backend {
    inner: WrappedRcRefCell<Inner>,
}

impl Backend {
    pub fn worker_port(&self) -> u16 {
        self.inner.get().worker_port
    }

    pub async fn send_tako_message(
        &self,
        message: FromGatewayMessage,
    ) -> crate::Result<ToGatewayMessage> {
        let (sx, rx) = oneshot::channel::<ToGatewayMessage>();
        {
            let mut inner = self.inner.get_mut();
            inner.tako_responses.push_back(sx);
            inner.tako_sender.send(message).unwrap();
        }
        Ok(rx.await.unwrap())
    }

    pub fn send_stream_control(&self, message: StreamServerControlMessage) {
        assert!(self.inner.get().stream_server_control.send(message).is_ok())
    }

    pub async fn start(
        state_ref: StateRef,
        key: Arc<SecretKey>,
        idle_timeout: Option<Duration>,
        worker_port: Option<u16>,
    ) -> crate::Result<(Backend, impl Future<Output = crate::Result<()>>)> {
        let msd = Duration::from_millis(20);

        let (from_tako_sender, mut from_tako_receiver) = unbounded_channel::<ToGatewayMessage>();
        let (to_tako_sender, to_tako_receiver) = unbounded_channel::<FromGatewayMessage>();

        let stream_server_control = start_stream_server();
        let stream_server_control2 = stream_server_control.clone();

        let (server_ref, server_future) = tako::server::server_start(
            SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), worker_port.unwrap_or(0)),
            Some(key),
            msd,
            from_tako_sender.clone(),
            false,
            idle_timeout,
            Some(Box::new(move |connection| {
                assert!(stream_server_control2
                    .send(StreamServerControlMessage::AddConnection(connection))
                    .is_ok());
            })),
        )
        .await?;

        let server = Backend {
            inner: WrappedRcRefCell::wrap(Inner {
                tako_sender: to_tako_sender,
                tako_responses: Default::default(),
                worker_port: server_ref.get_worker_listen_port(),
                stream_server_control,
            }),
        };
        let server2 = server.clone();

        let future = async move {
            let tako_msg_reader = async move {
                while let Some(message) = from_tako_receiver.recv().await {
                    match message {
                        ToGatewayMessage::TaskUpdate(msg) => {
                            state_ref.get_mut().process_task_update(msg, &server2)
                        }
                        ToGatewayMessage::TaskFailed(msg) => {
                            state_ref
                                .get_mut()
                                .process_task_failed(&state_ref, &server2, msg);
                        }
                        ToGatewayMessage::NewWorker(msg) => {
                            state_ref.get_mut().process_worker_new(msg)
                        }
                        ToGatewayMessage::LostWorker(msg) => state_ref
                            .get_mut()
                            .process_worker_lost(&state_ref, &server2, msg),
                        ToGatewayMessage::WorkerOverview(overview) => {
                            state_ref
                                .get_mut()
                                .event_storage_mut()
                                .on_overview_received(overview);
                        }
                        ToGatewayMessage::NewTasksResponse(_)
                        | ToGatewayMessage::CancelTasksResponse(_)
                        | ToGatewayMessage::TaskInfo(_)
                        | ToGatewayMessage::Error(_)
                        | ToGatewayMessage::ServerInfo(_)
                        | ToGatewayMessage::WorkerStopped
                        | ToGatewayMessage::NewWorkerAllocationQueryResponse(_) => {
                            let response =
                                server2.inner.get_mut().tako_responses.pop_front().unwrap();
                            response.send(message).unwrap();
                        }
                    }
                }
                error("Tako receive stream terminated".into()) as crate::Result<()>
            };
            let tako_msg_sender = async move {
                /*while let Some(message) = to_tako_receiver.recv().await {
                    let error =
                        process_client_message(&core_ref, &comm_ref, &from_tako_sender, message)
                            .await;
                    if let Some(message) = error {
                        from_tako_sender
                            .send(ToGatewayMessage::Error(ErrorResponse { message }))
                            .unwrap();
                    }
                }*/
                server_ref
                    .process_messages(to_tako_receiver, from_tako_sender)
                    .await;

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
    use tako::gateway::{FromGatewayMessage, ServerInfo, ToGatewayMessage};
    use tokio::net::TcpStream;

    use crate::server::rpc::Backend;
    use crate::tests::utils::{create_hq_state, run_concurrent};

    #[tokio::test]
    async fn test_server_connect_worker() {
        let state = create_hq_state();
        let (server, _fut) = Backend::start(state, Default::default(), None, None)
            .await
            .unwrap();
        TcpStream::connect(format!("127.0.0.1:{}", server.worker_port()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_server_info() {
        let state = create_hq_state();
        let (server, fut) = Backend::start(state, Default::default(), None, None)
            .await
            .unwrap();
        run_concurrent(fut, async move {
            assert!(
                matches!(server.send_tako_message(FromGatewayMessage::ServerInfo).await.unwrap(),
                    ToGatewayMessage::ServerInfo(ServerInfo { worker_listen_port })
                    if worker_listen_port == server.worker_port()
                )
            );
        })
        .await;
    }
}
