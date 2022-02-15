use crate::common::manager::info::ManagerType;
use crate::common::serverdir::ServerDir;
use crate::server::autoalloc::{
    prepare_descriptor_cleanup, DescriptorId, PbsHandler, QueueDescriptor, QueueHandler, QueueInfo,
    RateLimiter, SlurmHandler,
};
use crate::server::state::StateRef;
use crate::transfer::messages::{
    AllocationQueueParams, AutoAllocListResponse, AutoAllocRequest, AutoAllocResponse,
    QueueDescriptorData, ToClientMessage,
};
use std::path::PathBuf;
use std::time::Duration;

pub async fn handle_autoalloc_message(
    server_dir: &ServerDir,
    state_ref: &StateRef,
    request: AutoAllocRequest,
) -> ToClientMessage {
    match request {
        AutoAllocRequest::List => {
            let state = state_ref.get();
            let autoalloc = state.get_autoalloc_state();
            ToClientMessage::AutoAllocResponse(AutoAllocResponse::List(AutoAllocListResponse {
                descriptors: autoalloc
                    .descriptors()
                    .map(|(id, descriptor)| {
                        (
                            id,
                            QueueDescriptorData {
                                info: descriptor.descriptor.info().clone(),
                                name: descriptor.descriptor.name().map(|v| v.to_string()),
                                manager_type: descriptor.descriptor.manager().clone(),
                            },
                        )
                    })
                    .collect(),
            }))
        }
        AutoAllocRequest::AddQueue {
            manager,
            parameters,
        } => create_queue(server_dir, state_ref, manager, parameters),
        AutoAllocRequest::Events { descriptor } => get_event_log(state_ref, descriptor),
        AutoAllocRequest::Info { descriptor } => get_allocations(state_ref, descriptor),
        AutoAllocRequest::RemoveQueue { descriptor, force } => {
            remove_queue(state_ref, descriptor, force).await
        }
    }
}

async fn remove_queue(state_ref: &StateRef, id: DescriptorId, force: bool) -> ToClientMessage {
    let remove_alloc_fut = {
        let mut server_state = state_ref.get_mut();
        let descriptor_state = server_state
            .get_autoalloc_state_mut()
            .get_descriptor_mut(id);

        let fut = match descriptor_state {
            Some(state) => {
                let has_running_allocations =
                    state.all_allocations().any(|alloc| alloc.is_running());
                if has_running_allocations && !force {
                    return ToClientMessage::Error(
                        "Allocation queue has running jobs, so it will \
not be removed. Use `--force` if you want to remove the queue anyway"
                            .to_string(),
                    );
                }

                prepare_descriptor_cleanup(state)
            }
            None => return ToClientMessage::Error("Allocation queue not found".to_string()),
        };

        server_state.get_autoalloc_state_mut().remove_descriptor(id);
        fut
    };

    for (result, allocation_id) in futures::future::join_all(remove_alloc_fut).await {
        match result {
            Ok(_) => log::info!("Allocation {} was removed", allocation_id),
            Err(e) => log::error!("Failed to remove allocation {}: {:?}", allocation_id, e),
        }
    }

    state_ref
        .get_mut()
        .get_event_storage_mut()
        .on_allocation_queue_removed(id);

    ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueRemoved(id))
}

fn get_allocations(state_ref: &StateRef, descriptor: DescriptorId) -> ToClientMessage {
    let state = state_ref.get();
    let autoalloc = state.get_autoalloc_state();

    match autoalloc.get_descriptor(descriptor) {
        Some(descriptor) => ToClientMessage::AutoAllocResponse(AutoAllocResponse::Info(
            descriptor.all_allocations().cloned().collect(),
        )),
        None => ToClientMessage::Error(format!("Descriptor {} not found", descriptor)),
    }
}

fn get_event_log(state_ref: &StateRef, descriptor: DescriptorId) -> ToClientMessage {
    let state = state_ref.get();
    let autoalloc = state.get_autoalloc_state();

    match autoalloc.get_descriptor(descriptor) {
        Some(descriptor) => ToClientMessage::AutoAllocResponse(AutoAllocResponse::Events(
            descriptor.get_events().iter().cloned().collect(),
        )),
        None => ToClientMessage::Error(format!("Descriptor {} not found", descriptor)),
    }
}

// The code doesn't compile if the Box closures are removed
#[allow(clippy::redundant_closure)]
pub fn create_allocation_handler(
    manager: &ManagerType,
    name: Option<String>,
    directory: PathBuf,
) -> anyhow::Result<Box<dyn QueueHandler>> {
    match manager {
        ManagerType::Pbs => {
            let handler = PbsHandler::new(directory, name);
            handler.map::<Box<dyn QueueHandler>, _>(|handler| Box::new(handler))
        }
        ManagerType::Slurm => {
            let handler = SlurmHandler::new(directory, name);
            handler.map::<Box<dyn QueueHandler>, _>(|handler| Box::new(handler))
        }
    }
}

pub fn create_queue_info(params: AllocationQueueParams) -> QueueInfo {
    let AllocationQueueParams {
        name: _name,
        workers_per_alloc,
        backlog,
        timelimit,
        additional_args,
        worker_cpu_arg,
        worker_resources_args,
        max_worker_count,
        on_server_lost,
        max_kept_directories: _,
    } = params;
    QueueInfo::new(
        backlog,
        workers_per_alloc,
        timelimit,
        on_server_lost,
        additional_args,
        worker_cpu_arg,
        worker_resources_args,
        max_worker_count,
    )
}

fn create_rate_limiter() -> RateLimiter {
    RateLimiter::new(
        vec![
            Duration::ZERO,
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_secs(5 * 60),
            Duration::from_secs(10 * 60),
            Duration::from_secs(15 * 60),
            Duration::from_secs(30 * 60),
            Duration::from_secs(60 * 60),
        ],
        50,
        10,
    )
}

fn create_queue(
    server_dir: &ServerDir,
    state_ref: &StateRef,
    manager: ManagerType,
    params: AllocationQueueParams,
) -> ToClientMessage {
    let server_directory = server_dir.directory().to_path_buf();
    let name = params.name.clone();
    let max_kept_directories = params.max_kept_directories;
    let handler = create_allocation_handler(&manager, name.clone(), server_directory);
    let queue_info = create_queue_info(params.clone());

    match handler {
        Ok(handler) => {
            let descriptor =
                QueueDescriptor::new(manager, queue_info, name, handler, max_kept_directories);
            let id = {
                let mut state = state_ref.get_mut();
                let id = state.get_autoalloc_state_mut().create_id();

                state.get_autoalloc_state_mut().add_descriptor(
                    id,
                    descriptor,
                    create_rate_limiter(),
                );
                state
                    .get_event_storage_mut()
                    .on_allocation_queue_created(id, params);
                id
            };

            ToClientMessage::AutoAllocResponse(AutoAllocResponse::QueueCreated(id))
        }
        Err(err) => ToClientMessage::Error(format!("Could not create autoalloc queue: {}", err)),
    }
}
