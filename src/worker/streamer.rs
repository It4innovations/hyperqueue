use crate::common::WrappedRcRefCell;
use crate::transfer::stream::{
    ChannelId, DataMsg, EndTaskStreamMsg, FromStreamerMessage, StartTaskStreamMsg,
    StreamRegistration, ToStreamerMessage,
};
use crate::{JobId, JobTaskId, Map};
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use orion::aead::streaming::StreamOpener;
use orion::aead::SecretKey;
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tako::messages::worker::ConnectionRegistration;
use tako::server::rpc::ConnectionDescriptor;
use tako::transfer::auth::{open_message, seal_message, serialize};
use tako::worker::rpc::connect_to_server_and_authenticate;
use tako::InstanceId;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::spawn_local;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const STREAMER_BUFFER_SIZE: usize = 128;

/*
    Streamer is connection manager that holds a connections to stream server.
    In the current version, each job establishes own connection.
    Tasks in the same job uses the same connection.
*/

struct StreamInfo {
    sender: Sender<FromStreamerMessage>,
    responses: Map<JobTaskId, oneshot::Sender<tako::Result<()>>>,
}

pub struct Streamer {
    streams: Map<JobId, StreamInfo>,
    server_address: SocketAddr,
    secret_key: Arc<SecretKey>,
}

async fn stream_receiver(
    streamer_ref: StreamerRef,
    job_id: JobId,
    mut receiver: SplitStream<Framed<tokio::net::TcpStream, LengthDelimitedCodec>>,
    mut opener: Option<StreamOpener>,
) -> crate::Result<()> {
    while let Some(data) = receiver.next().await {
        let message = data?;
        let msg: ToStreamerMessage = open_message(&mut opener, &message)?;
        match msg {
            ToStreamerMessage::EndResponse(r) => {
                let mut streamer = streamer_ref.get_mut();
                streamer.close_task_stream(job_id, r.task);
            }
            ToStreamerMessage::Error(e) => return Err(e.into()),
        }
    }
    Ok(())
}

impl Streamer {
    /*
        Sends an error message to each task stream that uses the same connection
    */
    pub fn send_error(&mut self, job_id: JobId, error: String) {
        let info = self.streams.remove(&job_id).unwrap();
        for (_, response_sender) in info.responses {
            let _ = response_sender.send(Err(error.as_str().into()));
        }
    }

    /*
        Closes a task stream, if it is the last stream that uses a connection,
        then the connection is closed
    */
    pub fn close_task_stream(&mut self, job_id: JobId, task_id: JobTaskId) {
        let info = self.streams.get_mut(&job_id).unwrap();
        let _ = info.responses.remove(&task_id).unwrap().send(Ok(()));
        if info.responses.is_empty() {
            self.streams.remove(&job_id);
        }
    }

    /* Get tasm stream input end, if a connection to a stream server is not established,
      the new one is created

      response_sender - error and finish confirmation mechanism. If an error occurs,
      response_sender is fired with an error,
      if a stream asked for termination (via StreamSender::close()) then response_sender is fired with
      Ok(()) -- it is a confirmation that all data was written into a file in the stream server.
    */
    pub fn get_stream(
        &mut self,
        streamer_ref: &StreamerRef,
        job_id: JobId,
        job_task_id: JobTaskId,
        instance_id: InstanceId,
        response_sender: oneshot::Sender<tako::Result<()>>,
    ) -> StreamSender {
        log::debug!("New stream for {}/{}", job_id, job_task_id);
        if let Some(ref mut info) = self.streams.get_mut(&job_id) {
            assert!(info
                .responses
                .insert(job_task_id, response_sender)
                .is_none());
            StreamSender {
                task_id: job_task_id,
                instance_id,
                sender: info.sender.clone(),
                finished: Cell::new(false),
            }
        } else {
            log::debug!("Starting a new stream connection for job_id = {}", job_id);
            let (queue_sender, mut queue_receiver) = channel(STREAMER_BUFFER_SIZE);
            let mut responses = Map::new();
            responses.insert(job_task_id, response_sender);
            self.streams.insert(
                job_id,
                StreamInfo {
                    sender: queue_sender.clone(),
                    responses,
                },
            );
            let streamer_ref = streamer_ref.clone();
            spawn_local(async move {
                let (server_address, secret_key) = {
                    let streamer = streamer_ref.get();
                    (streamer.server_address, streamer.secret_key.clone())
                };
                let ConnectionDescriptor {
                    mut sender,
                    receiver,
                    opener,
                    mut sealer,
                    ..
                } = match connect_to_server_and_authenticate(server_address, &Some(secret_key))
                    .await
                {
                    Ok(connection) => connection,
                    Err(e) => {
                        streamer_ref.get_mut().send_error(job_id, e.to_string());
                        return;
                    }
                };

                if let Err(e) = sender
                    .send(seal_message(
                        &mut sealer,
                        serialize(&ConnectionRegistration::Custom).unwrap().into(),
                    ))
                    .await
                {
                    streamer_ref.get_mut().send_error(job_id, e.to_string());
                    return;
                }

                if let Err(e) = sender
                    .send(seal_message(
                        &mut sealer,
                        serialize(&StreamRegistration { job: job_id })
                            .unwrap()
                            .into(),
                    ))
                    .await
                {
                    streamer_ref.get_mut().send_error(job_id, e.to_string());
                    return;
                }

                let send_loop = async {
                    while let Some(data) = queue_receiver.recv().await {
                        if let Err(e) = sender
                            .send(seal_message(&mut sealer, serialize(&data).unwrap().into()))
                            .await
                        {
                            log::debug!("Forwarding from queue failed");
                            return Err(e.into());
                        }
                    }
                    Ok(())
                };

                let r = tokio::select! {
                    r = send_loop => r,
                    r = stream_receiver(streamer_ref.clone(), job_id, receiver, opener) => r,
                };
                log::debug!("Stream for job {} terminated {:?}", job_id, r);
                if let Err(e) = r {
                    streamer_ref.get_mut().send_error(job_id, e.to_string())
                } else {
                    streamer_ref.get_mut().streams.remove(&job_id);
                }
            });
            StreamSender {
                task_id: job_task_id,
                sender: queue_sender,
                instance_id,
                finished: Cell::new(false),
            }
        }
    }
}

pub type StreamerRef = WrappedRcRefCell<Streamer>;

impl StreamerRef {
    pub fn start(
        stream_clean_interval: Duration,
        server_address: SocketAddr,
        secret_key: Arc<SecretKey>,
    ) -> (StreamerRef, impl Future<Output = ()>) {
        let streamer_ref = WrappedRcRefCell::wrap(Streamer {
            streams: Default::default(),
            secret_key,
            server_address,
        });

        let streamer_ref2 = streamer_ref.clone();
        let streamer_future = async move {
            let mut it = tokio::time::interval(stream_clean_interval);
            loop {
                it.tick().await;
                let mut streamer = streamer_ref2.get_mut();
                streamer
                    .streams
                    .retain(|_, info| !info.responses.is_empty());
            }
        };
        (streamer_ref, streamer_future)
    }
}

pub struct StreamSender {
    sender: Sender<FromStreamerMessage>,
    task_id: JobTaskId,
    instance_id: InstanceId,
    finished: Cell<bool>,
}

impl Drop for StreamSender {
    fn drop(&mut self) {
        if !self.finished.get() {
            log::debug!(
                "Clean up a unfinished StreamSender job_task_id={}",
                self.task_id
            );
            let sender = self.sender.clone();
            let task = self.task_id;
            let instance = self.instance_id;

            tokio::task::spawn_local(async move {
                log::debug!(
                    "Clean up task of a unfinished StreamSender job_task_id={}",
                    task
                );
                if sender
                    .send(FromStreamerMessage::End(EndTaskStreamMsg {
                        task,
                        instance,
                    }))
                    .await
                    .is_err()
                {
                    log::debug!("Closing stream in drop failed");
                }
            });
        }
    }
}

impl StreamSender {
    pub async fn close(&self) -> tako::Result<()> {
        self.finished.set(true);
        if self
            .sender
            .send(FromStreamerMessage::End(EndTaskStreamMsg {
                task: self.task_id,
                instance: self.instance_id,
            }))
            .await
            .is_err()
        {
            return Err("Sending close message failed".into());
        }
        Ok(())
    }

    pub async fn send_data(&self, channel: ChannelId, data: Vec<u8>) -> tako::Result<()> {
        if self
            .sender
            .send(FromStreamerMessage::Data(DataMsg {
                task: self.task_id,
                instance: self.instance_id,
                channel,
                data,
            }))
            .await
            .is_err()
        {
            return Err("Sending streamer message failed".into());
        }
        Ok(())
    }

    pub async fn send_stream_start(&self) -> tako::Result<()> {
        if self
            .sender
            .send(FromStreamerMessage::Start(StartTaskStreamMsg {
                task: self.task_id,
                instance: self.instance_id,
            }))
            .await
            .is_err()
        {
            return Err("Sending streamer message failed".into());
        }
        Ok(())
    }
}
