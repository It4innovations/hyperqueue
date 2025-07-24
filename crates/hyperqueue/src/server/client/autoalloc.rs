use crate::common::serverdir::ServerDir;
use crate::server::Senders;
use crate::server::autoalloc::try_submit_allocation;
use crate::transfer::messages::{
    AutoAllocListQueuesResponse, AutoAllocRequest, AutoAllocResponse, ToClientMessage,
};

pub async fn handle_autoalloc_message(
    server_dir: &ServerDir,
    senders: &Senders,
    request: AutoAllocRequest,
) -> ToClientMessage {
    match request {
        AutoAllocRequest::ListQueues => {
            let queues = senders.autoalloc.get_queues();
            ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueList(
                AutoAllocListQueuesResponse {
                    queues: queues.await,
                },
            ))
        }
        AutoAllocRequest::DryRun { parameters } => {
            if let Err(e) = try_submit_allocation(parameters).await {
                ToClientMessage::Error(e.to_string())
            } else {
                ToClientMessage::AutoAllocResponse(AutoAllocResponse::DryRunSuccessful)
            }
        }
        AutoAllocRequest::AddQueue {
            parameters,
            dry_run,
        } => {
            if dry_run {
                if let Err(e) = try_submit_allocation(parameters.clone()).await {
                    return ToClientMessage::Error(e.to_string());
                }
            }

            let result =
                senders
                    .autoalloc
                    .add_queue(server_dir.directory(), parameters, None, None);
            match result.await {
                Ok(queue_id) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueCreated(queue_id))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::GetQueueInfo { queue_id } => {
            let result = senders.autoalloc.get_allocations(queue_id);
            match result.await {
                Ok(allocations) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueInfo(allocations))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::RemoveQueue { queue_id, force } => {
            let result = senders.autoalloc.remove_queue(queue_id, force);
            match result.await {
                Ok(_) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueRemoved(queue_id))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::PauseQueue { queue_id } => {
            let result = senders.autoalloc.pause_queue(queue_id);
            match result.await {
                Ok(_) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueuePaused(queue_id))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::ResumeQueue { queue_id } => {
            let result = senders.autoalloc.resume_queue(queue_id);
            match result.await {
                Ok(_) => {
                    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueResumed(queue_id))
                }
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
        AutoAllocRequest::GetAllocationInfo { allocation_id } => {
            let result = senders.autoalloc.get_allocation_by_id(allocation_id);
            match result.await {
                Ok(allocation) => ToClientMessage::AutoAllocResponse(
                    AutoAllocResponse::AllocationInfo(allocation),
                ),
                Err(error) => ToClientMessage::Error(error.to_string()),
            }
        }
    }
}
