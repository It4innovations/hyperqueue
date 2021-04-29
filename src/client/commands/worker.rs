use crate::client::globalsettings::GlobalSettings;
use crate::client::utils::handle_message;
use crate::client::utils::OutputStyle;
use crate::client::worker::print_worker_info;
use crate::common::error::error;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, StopWorkerMessage, ToClientMessage};
use crate::WorkerId;

pub async fn get_worker_list(
    connection: &mut ClientConnection,
    gsettings: &GlobalSettings,
) -> crate::Result<()> {
    match handle_message(
        connection
            .send_and_receive(FromClientMessage::WorkerList)
            .await,
    )? {
        ToClientMessage::WorkerListResponse(mut msg) => {
            msg.workers.sort_unstable_by_key(|w| w.id);
            print_worker_info(msg.workers, gsettings);
        }
        msg => return error(format!("Received an invalid message {:?}", msg)),
    }
    Ok(())
}

pub async fn stop_worker(
    connection: &mut ClientConnection,
    worker_id: WorkerId,
) -> crate::Result<()> {
    match handle_message(
        connection
            .send_and_receive(FromClientMessage::StopWorker(StopWorkerMessage { worker_id }))
            .await,
    )? {
        ToClientMessage::StopWorkerResponse => {}
        msg => return error(format!("Received an invalid message {:?}", msg)),
    }
    Ok(())
}
