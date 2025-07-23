use crate::datasrv::DataObjectId;
use crate::internal::datasrv::messages::{
    FromLocalDataClientMessageDown, ToLocalDataClientMessageUp,
};
use crate::internal::worker::state::WorkerStateRef;
use crate::TaskId;
use bstr::BString;
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Serialize, Deserialize)]
pub struct Notification {
    pub message: Box<[u8]>,
}

#[derive(Serialize, Deserialize)]
pub enum NotificationResponse {
    Ok,
}

pub(crate) async fn notify_local_connection_handler(
    worker_ref: WorkerStateRef,
    mut rx: impl Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    mut tx: (impl Sink<Bytes> + Unpin),
    task_id: TaskId,
) -> crate::Result<()> {
    while let Some(data) = rx.next().await {
        let data = data?;
        let notify_message: Notification = bincode::deserialize(&data)?;
        worker_ref
            .get_mut()
            .send_notify(task_id, notify_message.message);
        let data = bincode::serialize(&NotificationResponse::Ok)?;
        let _ = tx.send(data.into()).await;
    }
    Ok(())
}
