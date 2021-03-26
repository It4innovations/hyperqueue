use crate::common::WrappedRcRefCell;
use std::sync::WaitTimeoutResult;
use std::collections::VecDeque;
use std::path::Path;
use std::future::Future;
use std::fs::File;
use std::process::Stdio;
use tokio::time::{Instant, Duration};
use tokio::net::UnixStream;
use crate::common::protocol::make_protocol_builder;
use crate::common::sink::{forward_queue_to_sink};
use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;
use futures::StreamExt;
use crate::common::error::HqError;
use crate::server::state::StateRef;
use crate::messages::{FromClientMessage, ToClientMessage};
use tokio::sync::oneshot;
use crate::tako::gateway::{FromGatewayMessage, ToGatewayMessage};


struct Inner {
    sender: UnboundedSender<Bytes>,
    responses: VecDeque<oneshot::Sender<ToGatewayMessage>>,
}

#[derive(Clone)]
pub struct TakoServer {
    inner: WrappedRcRefCell<Inner>
}

impl TakoServer {

    pub async fn send_message(&self, message: FromGatewayMessage) -> ToGatewayMessage {
        let send_data = rmp_serde::to_vec_named(&message).unwrap();
        let (sx, rx) = oneshot::channel::<ToGatewayMessage>();
        self.inner.get_mut().responses.push_back(sx);
        assert!(self.inner.get_mut().sender.send(send_data.into()).is_ok());
        rx.await.unwrap()
    }

    pub fn start<'a>(state_ref: StateRef, socket_filename: &'a Path, log_file: Option<File>, worker_port: u16) -> (TakoServer, impl Future<Output=crate::Result<()>> + 'a) {
        let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();

        let server = TakoServer {
            inner: WrappedRcRefCell::wrap(Inner {
                sender: queue_sender,
                responses: Default::default()
            })
        };
        let server2 = server.clone();

        let future = async move {
            let (stdout, stderr) = if let Some(f) = log_file {
                let f2 = f.try_clone()?;
                (Stdio::from(f), Stdio::from(f2))
            } else {
                (Stdio::null(), Stdio::null())
            };

            let command = tokio::process::Command::new("tako-server")
                .arg(socket_filename)
                .arg("--port")
                .arg(&worker_port.to_string())
                .stdout(stdout)
                .stderr(stderr)
                .kill_on_drop(true)
                .status();

            let receiver = async move {
                tokio::time::delay_for(Duration::from_millis(250)).await;
                let socket = UnixStream::connect(socket_filename).await?;
                let (writer, mut reader) =  make_protocol_builder().new_framed(socket).split();
                let forward = forward_queue_to_sink(queue_receiver, writer);

                let reader = async move {
                    while let Some(data) = reader.next().await {
                        let data = data.unwrap();
                        let message : ToGatewayMessage = rmp_serde::from_slice(&data).unwrap();
                        let response = server.inner.get_mut().responses.pop_front().unwrap();
                        response.send(message);
                    }
                    Err(HqError::GenericError("Tako stream terminated".to_string()))
                };
                tokio::select! {
                    r = forward => { r.map_err(|e| HqError::from(e)) },
                    r = reader => { r }
                }
            };

            tokio::select! {
                r = command => {
                    Result::Err(crate::Error::GenericError(match r {
                        Ok(status) => format!("Taco server terminated, exit_code {}", status.code().unwrap_or(-1)),
                        Err(e) => format!("Cannot start Tako server: {}", e)
                    }))
                },
                r = receiver => { r }
            }
        };
        (server2, future)
    }

}