use crate::common::serverdir::ServerDir;
use crate::server::autoalloc::try_submit_allocation;
use crate::server::state::StateRef;
use crate::transfer::messages::{
    AutoAllocListResponse, AutoAllocRequest, AutoAllocResponse, ToClientMessage,
};

pub async fn handle_autoalloc_message(
    server_dir: &ServerDir,
    state_ref: &StateRef,
    request: AutoAllocRequest,
) -> ToClientMessage {
    match request {
        AutoAllocRequest::List => {
            let queues = state_ref.get().autoalloc().get_queues();
            ToClientMessage::AutoAllocResponse(AutoAllocResponse::List(AutoAllocListResponse {
                queues: queues.await,
            }))
        }
        AutoAllocRequest::DryRun {
            manager,
            parameters,
        } => {
            if let Err(e) = try_submit_allocation(manager, parameters).await {
                ToClientMessage::Error(e.to_string())
            } else {
                ToClientMessage::AutoAllocResponse(AutoAllocResponse::DryRunSuccessful)
            }
        }
        AutoAllocRequest::AddQueue {
            manager,
            parameters,
            dry_run,
        } => {
            if dry_run {
                if let Err(e) = try_submit_allocation(manager.clone(), parameters.clone()).await {
                    return ToClientMessage::Error(e.to_string());
                }
            }

            let result = state_ref
                .get()
                .autoalloc()
                .add_queue(server_dir, manager, parameters);
            match result.await {
                Ok(queue_id) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueCreated(queue_id))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::Info { queue_id } => {
            let result = state_ref.get().autoalloc().get_allocations(queue_id);
            match result.await {
                Ok(allocations) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(allocations))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::RemoveQueue { queue_id, force } => {
            let result = state_ref.get().autoalloc().remove_queue(queue_id, force);
            match result.await {
                Ok(_) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueRemoved(queue_id))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
    }
}
