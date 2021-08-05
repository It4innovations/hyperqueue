use std::path::PathBuf;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::SplitStream;
use futures::StreamExt;
use orion::aead::streaming::StreamOpener;
use tako::server::rpc::ConnectionDescriptor;
use tako::transfer::auth::{forward_queue_to_sealed_sink, open_message};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::task::LocalSet;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::control::StreamServerControlMessage;

use crate::common::WrappedRcRefCell;
use crate::stream::reader::logfile::{
    BLOCK_STREAM_CHUNK, BLOCK_STREAM_END, BLOCK_STREAM_START, HQ_LOG_HEADER, HQ_LOG_VERSION,
};
use crate::transfer::messages::StreamStats;
use crate::transfer::stream::{
    EndTaskStreamResponseMsg, FromStreamerMessage, StreamRegistration, ToStreamerMessage,
};
use crate::{JobId, Map, Set};
use tokio::io::BufWriter;

const STREAM_BUFFER_SIZE: usize = 32;

enum StreamMessage {
    Message(FromStreamerMessage, UnboundedSender<Bytes>),
    Close,
}

struct StreamServerState {
    streams: Map<JobId, Sender<StreamMessage>>,
    registrations: Map<JobId, PathBuf>,
    connections: Set<String>,
}

impl StreamServerState {
    fn get_stream(&mut self, job_id: JobId) -> anyhow::Result<Sender<StreamMessage>> {
        if let Some(s) = self.streams.get(&job_id) {
            Ok(s.clone())
        } else if let Some(path) = self.registrations.get(&job_id) {
            log::debug!("Starting new stream for job {}", job_id);
            let (sender, mut receiver) = channel(STREAM_BUFFER_SIZE);
            self.streams.insert(job_id, sender.clone());
            let path = path.clone();
            tokio::task::spawn_local(async move {
                if let Err(e) = file_writer(&mut receiver, path).await {
                    error_state(receiver, e.to_string()).await;
                }
            });
            Ok(sender)
        } else {
            anyhow::bail!("Job {} is not registered for streaming", job_id);
        }
    }
}

type StreamServerStateRef = WrappedRcRefCell<StreamServerState>;

impl StreamServerStateRef {
    fn new() -> Self {
        WrappedRcRefCell::wrap(StreamServerState {
            streams: Default::default(),
            registrations: Default::default(),
            connections: Default::default(),
        })
    }
}

fn send_error(sender: UnboundedSender<Bytes>, message: String) {
    let msg = ToStreamerMessage::Error(message);
    let data = tako::transfer::auth::serialize(&msg).unwrap();
    if sender.send(data.into()).is_err() {
        log::debug!("Sendiding stream error failed");
    }
}

async fn error_state(mut receiver: Receiver<StreamMessage>, message: String) {
    while let Some(msg) = receiver.recv().await {
        match msg {
            StreamMessage::Message(_, response_sender) => {
                send_error(response_sender, message.clone());
            }
            StreamMessage::Close => break,
        }
    }
}

async fn file_writer(receiver: &mut Receiver<StreamMessage>, path: PathBuf) -> anyhow::Result<()> {
    let mut file = BufWriter::new(File::create(&path).await?);
    let mut buffer = BytesMut::with_capacity(24);
    buffer.put_slice(HQ_LOG_HEADER);
    buffer.put_u32(HQ_LOG_VERSION);
    buffer.put_u64(0); // Reserved bytes
    buffer.put_u64(0); // Reserved bytes
    file.write_all(&buffer).await?;
    file.flush().await?; // Make sure that header is written to avoid empty files for long time

    while let Some(msg) = receiver.recv().await {
        buffer.clear();
        match msg {
            StreamMessage::Message(FromStreamerMessage::Start(s), response_sender) => {
                buffer.put_u8(BLOCK_STREAM_START);
                buffer.put_u32(s.task);
                buffer.put_u32(s.instance);
                if let Err(e) = file.write_all(&buffer).await {
                    send_error(response_sender, e.to_string());
                    return Err(e.into());
                }
            }
            StreamMessage::Message(FromStreamerMessage::Data(s), response_sender) => {
                buffer.put_u8(BLOCK_STREAM_CHUNK);
                buffer.put_u32(s.task);
                buffer.put_u32(s.instance);
                buffer.put_u32(s.channel);
                buffer.put_u32(s.data.len() as u32);
                if let Err(e) = file.write_all(&buffer).await {
                    send_error(response_sender, e.to_string());
                    return Err(e.into());
                }
                if let Err(e) = file.write_all(&s.data).await {
                    send_error(response_sender, e.to_string());
                    return Err(e.into());
                }
            }
            StreamMessage::Message(FromStreamerMessage::End(s), response_sender) => {
                buffer.put_u8(BLOCK_STREAM_END);
                buffer.put_u32(s.task);
                buffer.put_u32(s.instance);
                if let Err(e) = file.write_all(&buffer).await {
                    send_error(response_sender, e.to_string());
                    return Err(e.into());
                }
                if let Err(e) = file.flush().await {
                    send_error(response_sender, e.to_string());
                    return Err(e.into());
                }
                let msg = ToStreamerMessage::EndResponse(EndTaskStreamResponseMsg { task: s.task });
                let data = tako::transfer::auth::serialize(&msg).unwrap();
                let _ = response_sender.send(data.into());
            }
            StreamMessage::Close => break,
        }
    }
    Ok(())
}

pub fn start_stream_server() -> UnboundedSender<StreamServerControlMessage> {
    let (sender, receiver) = unbounded_channel();
    std::thread::spawn(|| {
        let local_set = LocalSet::new();
        local_set.spawn_local(stream_server_main(receiver));

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(local_set)
    });
    sender
}

async fn receive_loop(
    state_ref: &StreamServerStateRef,
    mut receiver: SplitStream<Framed<tokio::net::TcpStream, LengthDelimitedCodec>>,
    mut opener: Option<StreamOpener>,
    response_sender: &UnboundedSender<Bytes>,
) -> anyhow::Result<()> {
    let register: StreamRegistration = if let Some(data) = receiver.next().await {
        open_message(&mut opener, &data?)?
    } else {
        log::debug!("Stream closed without registration");
        return Ok(());
    };
    log::debug!("Streamer for job {} connected", register.job);
    let stream = state_ref.get_mut().get_stream(register.job)?;
    while let Some(data) = receiver.next().await {
        let message: FromStreamerMessage = open_message(&mut opener, &data?)?;
        if stream
            .send(StreamMessage::Message(message, response_sender.clone()))
            .await
            .is_err()
        {
            anyhow::bail!("Fail to process streamed message");
        }
    }
    Ok(())
}

async fn handle_connection(state_ref: &StreamServerStateRef, connection: ConnectionDescriptor) {
    let (sender, receiver) = unbounded_channel();

    let snd_loop = forward_queue_to_sealed_sink(receiver, connection.sender, connection.sealer);

    tokio::select! {
        r = snd_loop => { log::debug!("Send queue for stream closed {:?}", r); },
        r = receive_loop(state_ref, connection.receiver, connection.opener, &sender) => {
            log::debug!("Connection for stream closed {:?}", r);
            if let Err(e) = r {
                send_error(sender, e.to_string());
            };
        },
    }
}

async fn stream_server_main(mut control_receiver: UnboundedReceiver<StreamServerControlMessage>) {
    /*let mut registrations: Map<StreamId, PathBuf> = Map::new();
    let mut streams: Map<StreamId, Sender<FromStreamerMessage>>;*/
    let state_ref = StreamServerStateRef::new();

    while let Some(msg) = control_receiver.recv().await {
        match msg {
            StreamServerControlMessage::RegisterStream {
                job_id,
                path,
                response,
            } => {
                log::debug!("Registering stream {}: {}", job_id, path.display());
                let mut state = state_ref.get_mut();
                assert!(state.registrations.insert(job_id, path).is_none());
                let _ = response.send(());
            }
            StreamServerControlMessage::UnregisterStream(job_id) => {
                log::debug!("Unregistering stream {}", job_id);
                let stream = {
                    let mut state = state_ref.get_mut();
                    assert!(state.registrations.remove(&job_id).is_some());
                    state.streams.remove(&job_id)
                };
                if let Some(stream) = stream {
                    log::debug!("Sending close to streamer");
                    let _ = stream.send(StreamMessage::Close).await;
                }
            }
            StreamServerControlMessage::AddConnection(connection) => {
                log::debug!("New connection for stream server");
                let state_ref = state_ref.clone();
                tokio::task::spawn_local(async move {
                    let address = connection.address.to_string();
                    state_ref.get_mut().connections.insert(address.clone());
                    handle_connection(&state_ref, connection).await;
                    state_ref.get_mut().connections.remove(&address);
                });
            }
            StreamServerControlMessage::Stats(response) => {
                log::debug!("Stream stat requested");
                let state = state_ref.get();
                let _ = response.send(StreamStats {
                    connections: state.connections.iter().cloned().collect(),
                    registrations: state
                        .registrations
                        .iter()
                        .map(|(job_id, path)| (*job_id, path.clone()))
                        .collect(),
                });
            }
        }
    }
}
