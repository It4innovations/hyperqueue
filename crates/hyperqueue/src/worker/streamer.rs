use crate::transfer::stream::{
    ChannelId, DataMsg, EndTaskStreamMsg, FromStreamerMessage, StartTaskStreamMsg,
    StreamRegistration, ToStreamerMessage,
};
use crate::WrappedRcRefCell;
use crate::{JobId, JobTaskId, Map};
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use orion::aead::streaming::StreamOpener;
use orion::aead::SecretKey;
use std::net::SocketAddr;
use std::sync::Arc;
use tako::comm::connect_to_server_and_authenticate;
use tako::comm::ConnectionRegistration;
use tako::comm::{open_message, seal_message, serialize};
use tako::server::ConnectionDescriptor;
use tako::{define_wrapped_type, InstanceId};
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

pub struct StreamInfo {
    job_id: JobId,
    sender: Option<Sender<FromStreamerMessage>>,
    responses: Map<JobTaskId, oneshot::Sender<tako::Result<()>>>,
}

type StreamInfoRef = WrappedRcRefCell<StreamInfo>;

pub struct Streamer {
    streams: Map<JobId, StreamInfoRef>,
    server_addresses: Vec<SocketAddr>,
    secret_key: Arc<SecretKey>,
}

impl StreamInfo {
    /*
        Sends an error message to each task stream that uses the same connection
    */
    pub fn send_error(&mut self, error: String) {
        log::debug!("Broadcasting stream error: {}", error);
        for (_, response_sender) in std::mem::take(&mut self.responses) {
            let _ = response_sender.send(Err(error.as_str().into()));
        }
    }

    /*
        Closes a task stream, if it is the last stream that uses a connection
        If streamer has no usafe, return true
    */
    pub fn close_task_stream(&mut self, task_id: JobTaskId) -> bool {
        log::debug!("Closing task stream task_id={}", task_id);
        let _ = self.responses.remove(&task_id).unwrap().send(Ok(()));
        self.responses.is_empty()
    }
}

async fn stream_receiver(
    streamer_ref: StreamerRef,
    streaminfo_ref: StreamInfoRef,
    mut receiver: SplitStream<Framed<tokio::net::TcpStream, LengthDelimitedCodec>>,
    mut opener: Option<StreamOpener>,
) -> crate::Result<()> {
    while let Some(data) = receiver.next().await {
        let message = data?;
        let msg: ToStreamerMessage = open_message(&mut opener, &message)?;
        match msg {
            ToStreamerMessage::EndResponse(r) => {
                let is_finished = streaminfo_ref.get_mut().close_task_stream(r.task);
                if is_finished {
                    let mut streamer = streamer_ref.get_mut();
                    streamer.remove(&streaminfo_ref);
                }
            }
            ToStreamerMessage::Error(e) => return Err(e.into()),
        }
    }
    log::debug!(
        "Server closed streaming connection for job_id={}",
        streaminfo_ref.get().job_id
    );
    Ok(())
}

impl Streamer {
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
            let mut info = info.get_mut();
            assert!(info
                .responses
                .insert(job_task_id, response_sender)
                .is_none());
            StreamSender {
                task_id: job_task_id,
                instance_id,
                sender: info.sender.as_ref().unwrap().clone(),
            }
        } else {
            log::debug!("Starting a new stream connection for job_id = {}", job_id);
            let (queue_sender, mut queue_receiver) = channel(STREAMER_BUFFER_SIZE);
            let mut responses = Map::new();
            responses.insert(job_task_id, response_sender);
            let streaminfo_ref = WrappedRcRefCell::wrap(StreamInfo {
                job_id,
                sender: Some(queue_sender.clone()),
                responses,
            });
            self.streams.insert(job_id, streaminfo_ref.clone());
            let streamer_ref = streamer_ref.clone();
            spawn_local(async move {
                let (server_addresses, secret_key) = {
                    let streamer = streamer_ref.get();
                    (
                        streamer.server_addresses.clone(),
                        streamer.secret_key.clone(),
                    )
                };
                let ConnectionDescriptor {
                    mut sender,
                    receiver,
                    opener,
                    mut sealer,
                    ..
                } = match connect_to_server_and_authenticate(&server_addresses, &Some(secret_key))
                    .await
                {
                    Ok(connection) => connection,
                    Err(e) => {
                        streamer_ref
                            .get_mut()
                            .send_error(&streaminfo_ref, e.to_string());
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
                    streamer_ref
                        .get_mut()
                        .send_error(&streaminfo_ref, e.to_string());
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
                    streamer_ref
                        .get_mut()
                        .send_error(&streaminfo_ref, e.to_string());
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
                    log::debug!("Stream send loop terminated for job {}", job_id);
                    Ok(())
                };

                let r = tokio::select! {
                    r = send_loop => r,
                    r = stream_receiver(streamer_ref.clone(), streaminfo_ref.clone(), receiver, opener) => r,
                };
                log::debug!("Stream for job {} terminated {:?}", job_id, r);
                if let Err(e) = r {
                    streamer_ref
                        .get_mut()
                        .send_error(&streaminfo_ref, e.to_string())
                } else {
                    streamer_ref.get_mut().remove(&streaminfo_ref);
                }
            });
            StreamSender {
                task_id: job_task_id,
                sender: queue_sender,
                instance_id,
            }
        }
    }

    pub fn send_error(&mut self, streaminfo_ref: &StreamInfoRef, message: String) {
        streaminfo_ref.get_mut().send_error(message);
        self.remove(streaminfo_ref);
    }

    pub fn remove(&mut self, streaminfo_ref: &StreamInfoRef) {
        let job_id = streaminfo_ref.get().job_id;
        streaminfo_ref.get_mut().sender = None;
        log::debug!("Removing stream {}", job_id);
        if self
            .streams
            .get(&job_id)
            .map(|x| x == streaminfo_ref)
            .unwrap_or(false)
        {
            self.streams.remove(&job_id);
        } else {
            log::debug!("Stream already replaced");
        }
    }
}

define_wrapped_type!(StreamerRef, Streamer, pub);

impl StreamerRef {
    pub fn start(server_addresses: &[SocketAddr], secret_key: Arc<SecretKey>) -> StreamerRef {
        let streamer_ref = WrappedRcRefCell::wrap(Streamer {
            streams: Default::default(),
            secret_key,
            server_addresses: server_addresses.to_vec(),
        });

        Self(streamer_ref)
    }
}

pub struct StreamSender {
    sender: Sender<FromStreamerMessage>,
    task_id: JobTaskId,
    instance_id: InstanceId,
}

impl StreamSender {
    pub async fn close(&self) -> tako::Result<()> {
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
