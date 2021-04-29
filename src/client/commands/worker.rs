use crate::client::globalsettings::GlobalSettings;
use crate::client::worker::print_worker_info;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, StopWorkerMessage, ToClientMessage};
use crate::WorkerId;

pub async fn get_worker_list(
    connection: &mut ClientConnection,
    gsettings: &GlobalSettings,
) -> crate::Result<()> {
    let mut msg = rpc_call!(
        connection,
        FromClientMessage::WorkerList,
        ToClientMessage::WorkerListResponse(r) => r
    )
    .await?;

    msg.workers.sort_unstable_by_key(|w| w.id);
    print_worker_info(msg.workers, gsettings);
    Ok(())
}

pub async fn stop_worker(
    connection: &mut ClientConnection,
    worker_id: WorkerId,
) -> crate::Result<()> {
    let message = FromClientMessage::StopWorker(StopWorkerMessage { worker_id });
    rpc_call!(connection, message, ToClientMessage::StopWorkerResponse).await
}
