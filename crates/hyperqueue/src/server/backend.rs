use std::collections::VecDeque;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use orion::kdf::SecretKey;
use tako::WorkerId;
use tako::gateway::{FromGatewayMessage, ToGatewayMessage};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio::time::Duration;

use crate::WrappedRcRefCell;
use crate::common::error::error;
use crate::server::Senders;
use crate::server::autoalloc::AutoAllocService;
use crate::server::event::streamer::EventStreamer;
use crate::server::state::StateRef;

struct InnerBackend {
    tako_sender: UnboundedSender<FromGatewayMessage>,
    tako_responses: VecDeque<oneshot::Sender<ToGatewayMessage>>,
    worker_port: u16,
}

#[derive(Clone)]
pub struct Backend {
    inner: WrappedRcRefCell<InnerBackend>,
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

    pub fn send_tako_message_no_wait(&self, message: FromGatewayMessage) {
        let _ = self.inner.get_mut().tako_sender.send(message);
    }

    pub async fn start(
        state_ref: StateRef,
        events: EventStreamer,
        autoalloc: AutoAllocService,
        key: Option<Arc<SecretKey>>,
        idle_timeout: Option<Duration>,
        worker_port: Option<u16>,
        worker_id_initial_value: WorkerId,
    ) -> crate::Result<(Backend, impl Future<Output = crate::Result<()>>)> {
        let msd = Duration::from_millis(20);

        let (from_tako_sender, mut from_tako_receiver) = unbounded_channel::<ToGatewayMessage>();
        let (to_tako_sender, to_tako_receiver) = unbounded_channel::<FromGatewayMessage>();

        let server_uid = state_ref.get().server_info().server_uid.clone();
        let (server_ref, server_future) = tako::server::server_start(
            SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), worker_port.unwrap_or(0)),
            key,
            msd,
            from_tako_sender.clone(),
            false,
            idle_timeout,
            None,
            server_uid,
            worker_id_initial_value,
        )
        .await?;

        let backend = Backend {
            inner: WrappedRcRefCell::wrap(InnerBackend {
                tako_sender: to_tako_sender,
                tako_responses: Default::default(),
                worker_port: server_ref.get_worker_listen_port(),
            }),
        };

        let senders = Senders {
            backend: backend.clone(),
            events,
            autoalloc,
        };

        let future = async move {
            let tako_msg_reader = async move {
                while let Some(message) = from_tako_receiver.recv().await {
                    match message {
                        ToGatewayMessage::TaskUpdate(msg) => {
                            state_ref.get_mut().process_task_update(msg, &senders)
                        }
                        ToGatewayMessage::TaskFailed(msg) => {
                            state_ref
                                .get_mut()
                                .process_task_failed(&state_ref, &senders, msg);
                        }
                        ToGatewayMessage::NewWorker(msg) => {
                            state_ref.get_mut().process_worker_new(msg, &senders);
                        }
                        ToGatewayMessage::LostWorker(msg) => state_ref
                            .get_mut()
                            .process_worker_lost(&state_ref, &senders, msg),
                        ToGatewayMessage::WorkerOverview(overview) => {
                            senders.events.on_overview_received(overview);
                        }
                        ToGatewayMessage::NewTasksResponse(_)
                        | ToGatewayMessage::CancelTasksResponse(_)
                        | ToGatewayMessage::TaskInfo(_)
                        | ToGatewayMessage::Error(_)
                        | ToGatewayMessage::ServerInfo(_)
                        | ToGatewayMessage::WorkerStopped
                        | ToGatewayMessage::NewWorkerAllocationQueryResponse(_) => {
                            let response = senders
                                .backend
                                .inner
                                .get_mut()
                                .tako_responses
                                .pop_front()
                                .unwrap();
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

        Ok((backend, future))
    }
}

#[cfg(test)]
mod tests {
    use crate::server::autoalloc::test_alloc_service;
    use tako::gateway::{FromGatewayMessage, ServerInfo, ToGatewayMessage};
    use tokio::net::TcpStream;

    use crate::server::backend::Backend;
    use crate::server::event::streamer::EventStreamer;
    use crate::tests::utils::{create_hq_state, run_concurrent};

    #[tokio::test]
    async fn test_server_connect_worker() {
        let state = create_hq_state();
        let (alloc, _) = test_alloc_service();
        let s = EventStreamer::new(None);
        let (server, _fut) =
            Backend::start(state, s, alloc, Default::default(), None, None, 1.into())
                .await
                .unwrap();
        TcpStream::connect(format!("127.0.0.1:{}", server.worker_port()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_server_info() {
        let state = create_hq_state();
        let (alloc, _) = test_alloc_service();
        let s = EventStreamer::new(None);
        let (server, fut) =
            Backend::start(state, s, alloc, Default::default(), None, None, 1.into())
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
