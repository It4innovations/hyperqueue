use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, Selector, StopWorkerMessage, StopWorkerResponse, ToClientMessage,
    WorkerInfo, WorkerInfoRequest,
};
use crate::WorkerId;

pub async fn get_worker_list(
    connection: &mut ClientConnection,
    include_offline: bool,
) -> crate::Result<Vec<WorkerInfo>> {
    let mut msg = rpc_call!(
        connection,
        FromClientMessage::WorkerList,
        ToClientMessage::WorkerListResponse(r) => r
    )
    .await?;

    msg.workers.sort_unstable_by_key(|w| w.id);

    if !include_offline {
        msg.workers.retain(|w| w.ended.is_none());
    }

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
    selector: Selector,
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
