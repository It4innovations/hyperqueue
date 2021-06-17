use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, StopWorkerMessage, ToClientMessage, WorkerInfo,
};
use crate::WorkerId;

pub async fn get_worker_list(
    connection: &mut ClientConnection,
    get_online: bool,
    get_offline: bool,
) -> crate::Result<Vec<WorkerInfo>> {
    let mut msg = rpc_call!(
        connection,
        FromClientMessage::WorkerList,
        ToClientMessage::WorkerListResponse(r) => r
    )
    .await?;

    msg.workers.sort_unstable_by_key(|w| w.id);
    msg.workers.retain(|w| {
        if w.ended.is_none() {
            get_online
        } else {
            get_offline
        }
    });

    Ok(msg.workers)
}

pub async fn stop_worker(
    connection: &mut ClientConnection,
    worker_id: WorkerId,
) -> crate::Result<()> {
    let message = FromClientMessage::StopWorker(StopWorkerMessage { worker_id });
    rpc_call!(connection, message, ToClientMessage::StopWorkerResponse).await
}
