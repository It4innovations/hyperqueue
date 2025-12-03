use crate::rpc_call;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{FromClientMessage, ResourceRequestMap, ToClientMessage};
use tako::{Map, WorkerId};

/// Maps worker IDs to hostnames.
pub type WorkerMap = Map<WorkerId, String>;

pub async fn get_remote_maps(
    session: &mut ClientSession,
    workers: bool,
    requests: bool,
) -> anyhow::Result<(WorkerMap, ResourceRequestMap)> {
    let message = FromClientMessage::GetList { workers, requests };
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::GetListResponse(r) => r).await?;
    let map = response
        .workers
        .into_iter()
        .map(|w| (w.id, w.configuration.hostname))
        .collect();
    Ok((map, response.requests))
}
