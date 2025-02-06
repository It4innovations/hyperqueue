use crate::common::error::HqError;
use crate::common::serialization::SerializationConfig;
use crate::stream::StreamSerializationConfig;
use crate::transfer::stream::{ChannelId, StreamChunkHeader};
use crate::WrappedRcRefCell;
use crate::{JobId, JobTaskId, Map};
use bincode::Options;
use chrono::Utc;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use tako::{define_wrapped_type, InstanceId, WorkerId};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::oneshot;
use tokio::task::spawn_local;

const STREAMER_BUFFER_SIZE: usize = 128;
pub const STREAM_FILE_HEADER: &[u8] = b"hqsf0000";
pub const STREAM_FILE_SUFFIX: &str = "hqs";

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct StreamFileHeader<'a> {
    pub server_uid: Cow<'a, String>,
    pub worker_id: WorkerId,
}

pub(crate) enum StreamerMessage {
    Write {
        header: StreamChunkHeader,
        data: Vec<u8>,
    },
    Flush(oneshot::Sender<()>),
}

pub(crate) struct StreamDescriptor {
    sender: Sender<StreamerMessage>,
}

pub struct Streamer {
    streams: Map<PathBuf, StreamDescriptor>,
    server_uid: String,
    worker_id: WorkerId,
}

impl Streamer {
    /* Get tasm stream input end, if a connection to a stream server is not established,
      the new one is created
    */
    pub fn get_stream(
        &mut self,
        streamer_ref: &StreamerRef,
        stream_path: &Path,
        job_id: JobId,
        job_task_id: JobTaskId,
        instance_id: InstanceId,
    ) -> crate::Result<StreamSender> {
        log::debug!(
            "New stream for {}/{} ({})",
            job_id,
            job_task_id,
            stream_path.display()
        );
        let sender = if let Some(ref mut info) = self.streams.get_mut(stream_path) {
            info.sender.clone()
        } else {
            log::debug!(
                "Starting a new stream instance for job_id = {}, stream_path = {}",
                job_id,
                stream_path.display()
            );
            if !stream_path.is_dir() {
                std::fs::create_dir_all(stream_path).map_err(|error| {
                    HqError::GenericError(format!(
                        "Cannot create stream directory {}: {error:?}",
                        stream_path.display()
                    ))
                })?;
            }
            let (queue_sender, queue_receiver) = channel(STREAMER_BUFFER_SIZE);
            let stream = StreamDescriptor {
                sender: queue_sender.clone(),
            };
            self.streams.insert(stream_path.to_path_buf(), stream);
            let stream_path = stream_path.to_path_buf();
            let streamer_ref = streamer_ref.clone();
            spawn_local(async move {
                if let Err(e) = stream_writer(&streamer_ref, &stream_path, queue_receiver).await {
                    log::error!("Stream failed: {e}")
                }
                let mut streamer = streamer_ref.get_mut();
                assert!(streamer.streams.remove(&stream_path).is_some());
            });
            queue_sender
        };
        Ok(StreamSender {
            job_id,
            job_task_id,
            instance_id,
            sender,
        })
    }
}

define_wrapped_type!(StreamerRef, Streamer, pub);

impl StreamerRef {
    pub fn new(server_uid: &str, worker_id: WorkerId) -> StreamerRef {
        let streamer_ref = WrappedRcRefCell::wrap(Streamer {
            streams: Default::default(),
            server_uid: server_uid.to_string(),
            worker_id,
        });
        Self(streamer_ref)
    }
}

#[derive(Clone)]
pub struct StreamSender {
    sender: Sender<StreamerMessage>,
    job_id: JobId,
    job_task_id: JobTaskId,
    instance_id: InstanceId,
}

impl StreamSender {
    pub async fn flush(&self) -> tako::Result<()> {
        let (sender, receiver) = oneshot::channel();
        if self
            .sender
            .send(StreamerMessage::Flush(sender))
            .await
            .is_err()
        {
            return Err("Sending flush message failed".into());
        }
        receiver.await.map_err(|_| {
            tako::Error::GenericError("Stream failed while flushing stream".to_string())
        })?;
        Ok(())
    }

    pub async fn send_data(&self, channel: ChannelId, data: Vec<u8>) -> tako::Result<()> {
        if self
            .sender
            .send(StreamerMessage::Write {
                header: StreamChunkHeader {
                    time: Utc::now(),
                    job: self.job_id,
                    task: self.job_task_id,
                    instance: self.instance_id,
                    channel,
                    size: data.len() as u64,
                },
                data,
            })
            .await
            .is_err()
        {
            return Err("Sending streamer message failed".into());
        }
        Ok(())
    }
}

async fn stream_writer(
    streamer_ref: &StreamerRef,
    path: &Path,
    mut receiver: Receiver<StreamerMessage>,
) -> crate::Result<()> {
    let uid: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect::<String>();
    let mut path = path.to_path_buf();
    path.push(format!("{uid}.{STREAM_FILE_SUFFIX}"));
    log::debug!("Opening stream file {}", path.display());
    let mut file = BufWriter::new(File::create(path).await?);
    file.write_all(STREAM_FILE_HEADER).await?;
    let mut buffer: Vec<u8> = Vec::new();
    {
        let streamer = streamer_ref.get();
        let header = StreamFileHeader {
            server_uid: Cow::Borrowed(&streamer.server_uid),
            worker_id: streamer.worker_id,
        };
        StreamSerializationConfig::config().serialize_into(&mut buffer, &header)?;
    };
    file.write_all(&buffer).await?;
    while let Some(message) = receiver.recv().await {
        match message {
            StreamerMessage::Write { header, data } => {
                log::debug!("Waiting data chunk into stream file");
                buffer.clear();
                StreamSerializationConfig::config().serialize_into(&mut buffer, &header)?;
                file.write_all(&buffer).await?;
                if !data.is_empty() {
                    file.write_all(&data).await?
                }
            }
            StreamerMessage::Flush(callback) => {
                log::debug!("Waiting for flush of stream file");
                file.flush().await?;
                if callback.send(()).is_err() {
                    log::debug!("Flush callback failed")
                }
            }
        }
    }
    log::debug!("Stream receiver is closed");
    Ok(())
}
