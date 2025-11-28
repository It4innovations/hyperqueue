use crate::rpc_call;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{FromClientMessage, GetListResponse, ToClientMessage};
use orion::kex::SessionKeys;
use tako::{Map, WorkerId};

/// Maps worker IDs to hostnames.
pub type WorkerMap = Map<WorkerId, String>;

pub async fn get_remote_lists(
    session: &mut ClientSession,
    workers: bool,
) -> anyhow::Result<GetListResponse> {
    let message = FromClientMessage::GetList { workers };
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::GetListResponse(r) => r).await?;
    Ok(response)
}

pub async fn get_worker_map(session: &mut ClientSession) -> anyhow::Result<WorkerMap> {
    let response = get_remote_lists(session, true).await?;
    Ok(response
        .workers
        .into_iter()
        .map(|w| (w.id, w.configuration.hostname))
        .collect())
}
