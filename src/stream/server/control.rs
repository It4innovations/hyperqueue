use crate::transfer::messages::StreamStats;
use crate::JobId;
use std::path::PathBuf;
use tako::server::rpc::ConnectionDescriptor;
use tokio::sync::oneshot;

pub enum StreamServerControlMessage {
    RegisterStream {
        job_id: JobId,
        path: PathBuf,
        response: oneshot::Sender<()>,
    },
    UnregisterStream(JobId),
    AddConnection(ConnectionDescriptor),
    Stats(oneshot::Sender<StreamStats>),
}
