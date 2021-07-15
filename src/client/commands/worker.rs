use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, StopWorkerMessage, StopWorkerResponse, ToClientMessage, WorkerInfo,
    WorkerInfoRequest, WorkerSelector,
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

pub async fn get_worker_info(
    connection: &mut ClientConnection,
    worker_id: WorkerId,
) -> crate::Result<Option<WorkerInfo>> {
    let msg = rpc_call!(
        connection,
        FromClientMessage::WorkerInfo(WorkerInfoRequest {
            worker_id,
        }),
        ToClientMessage::WorkerInfoResponse(r) => r
    )
    .await?;

    Ok(msg)
}

pub async fn stop_worker(
    connection: &mut ClientConnection,
    selector: WorkerSelector,
) -> crate::Result<()> {
    let message = FromClientMessage::StopWorker(StopWorkerMessage { selector });
    let mut responses =
        rpc_call!(connection, message, ToClientMessage::StopWorkerResponse(r) => r).await?;

    responses.sort_unstable_by_key(|x| x.0);
    for (id, response) in responses {
        match response {
            StopWorkerResponse::Failed(e) => {
                log::error!("Stopping worker {} failed; {}", id, e.to_string());
            }
            StopWorkerResponse::InvalidWorker => {
                log::error!("Stopping worker {} failed; worker not found", id);
            }
            StopWorkerResponse::AlreadyStopped => {
                log::warn!("Stopping worker {} failed; worker is already stopped", id);
            }
            StopWorkerResponse::Stopped => {
                log::info!("Worker {} stopped", id)
            }
        }
    }

    Ok(())
}
