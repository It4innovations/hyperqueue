use crate::server::autoalloc::{AllocationId, DescriptorId};
use crate::server::event::events::MonitoringEventPayload;
use crate::server::event::MonitoringEvent;
use crate::transfer::messages::AllocationQueueParams;
use crate::Map;
use std::time::SystemTime;

pub struct AllocationQueueInfo {
    pub queue_params: AllocationQueueParams,
    pub creation_time: SystemTime,
    pub removal_time: Option<SystemTime>,

    pub allocations: Map<AllocationId, AllocationInfo>,
}

#[derive(Copy, Clone)]
pub struct AllocationInfo {
    pub worker_count: u64,
    pub queued_time: SystemTime,
    pub start_time: Option<SystemTime>,
    pub finish_time: Option<SystemTime>,
}

#[derive(Copy, Clone)]
pub enum AllocationStatus {
    Queued,
    Running,
    Finished,
}

pub fn get_allocation_status(info: &AllocationInfo, query_time: SystemTime) -> AllocationStatus {
    let has_started = info.start_time.is_some() && info.start_time.unwrap() < query_time;
    let has_finished = match info.finish_time {
        None => false,
        Some(finish_time) => finish_time < query_time,
    };
    match has_started && !has_finished {
        true => (AllocationStatus::Running),
        false => match has_finished {
            true => AllocationStatus::Finished,
            false => AllocationStatus::Queued,
        },
    }
}
/// Stores the state of different allocation queues and their allocations
#[derive(Default)]
pub struct AllocationTimeline {
    queue_timelines: Map<DescriptorId, AllocationQueueInfo>,
}

impl AllocationQueueInfo {
    // Add a new allocation that has been queued.
    pub fn add_queued_allocation(
        &mut self,
        allocation_id: AllocationId,
        worker_count: u64,
        queued_time: SystemTime,
    ) {
        self.allocations.insert(
            allocation_id,
            AllocationInfo {
                worker_count,
                queued_time,
                start_time: None,
                finish_time: None,
            },
        );
    }

    // Update the state of an existing allocation in the queue.
    fn update_allocation_state(
        &mut self,
        allocation_id: &AllocationId,
        new_state: AllocationStatus,
        at_time: SystemTime,
    ) {
        let state = self
            .allocations
            .iter_mut()
            .find(|(id, _)| id == &allocation_id)
            .map(|(_, state)| state)
            .unwrap();
        match new_state {
            AllocationStatus::Running => {
                state.start_time = Some(at_time);
            }
            AllocationStatus::Finished => {
                state.finish_time = Some(at_time);
            }
            _ => {}
        }
    }
}

impl AllocationTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[MonitoringEvent]) {
        for event in events {
            match &event.payload {
                MonitoringEventPayload::AllocationQueueCreated(id, params) => {
                    self.queue_timelines.insert(
                        *id,
                        AllocationQueueInfo {
                            queue_params: *params.clone(),
                            creation_time: event.time,
                            removal_time: None,
                            allocations: Default::default(),
                        },
                    );
                }
                MonitoringEventPayload::AllocationQueueRemoved(descriptor_id) => {
                    let queue_state = self.queue_timelines.get_mut(descriptor_id).unwrap();
                    queue_state.removal_time = Some(event.time);
                }
                MonitoringEventPayload::AllocationQueued {
                    descriptor_id,
                    allocation_id,
                    worker_count,
                } => {
                    let queue_state = self.queue_timelines.get_mut(descriptor_id).unwrap();
                    queue_state.add_queued_allocation(
                        allocation_id.clone(),
                        *worker_count,
                        event.time,
                    );
                }
                MonitoringEventPayload::AllocationStarted(descriptor_id, allocation_id) => {
                    let queue_state = self.queue_timelines.get_mut(descriptor_id).unwrap();
                    queue_state.update_allocation_state(
                        allocation_id,
                        AllocationStatus::Running,
                        event.time,
                    );
                }
                MonitoringEventPayload::AllocationFinished(descriptor_id, allocation_id) => {
                    let queue_state = self.queue_timelines.get_mut(descriptor_id).unwrap();
                    queue_state.update_allocation_state(
                        allocation_id,
                        AllocationStatus::Finished,
                        event.time,
                    );
                }
                _ => {}
            }
        }
    }

    pub fn get_queue_infos_at(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (&DescriptorId, &AllocationQueueInfo)> + '_ {
        self.queue_timelines
            .iter()
            .filter(move |(_, info)| info.creation_time <= time)
    }

    pub fn get_queue_params_for(
        &self,
        descriptor_id: &DescriptorId,
    ) -> Option<&AllocationQueueParams> {
        self.queue_timelines
            .get(descriptor_id)
            .map(|queue_info| &queue_info.queue_params)
    }

    pub fn get_allocations_for_queue(
        &self,
        descriptor_id: DescriptorId,
        time: SystemTime,
    ) -> Option<impl Iterator<Item = (&AllocationId, &AllocationInfo)> + '_> {
        self.get_queue_infos_at(time)
            .find(|(id, _)| **id == descriptor_id)
            .map(move |(_, info)| {
                info.allocations
                    .iter()
                    .filter(move |(_, info)| info.queued_time <= time)
            })
    }
}
