use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use crate::{Map, WorkerId};

/// Maps worker IDs to hostnames.
pub type WorkerMap = Map<WorkerId, String>;

pub async fn get_worker_map(connection: &mut ClientConnection) -> anyhow::Result<WorkerMap> {
    let message = FromClientMessage::WorkerList;
    let response =
        rpc_call!(connection, message, ToClientMessage::WorkerListResponse(r) => r).await?;
    let map = response
        .workers
        .into_iter()
        .map(|w| (w.id, w.configuration.hostname))
        .collect();
    Ok(map)
}
