use crate::server::autoalloc::{AllocationId, QueueId};
use crate::server::event::Event;
use crate::server::event::payload::EventPayload;
use crate::transfer::messages::AllocationQueueParams;
use std::time::SystemTime;
use tako::Map;

pub struct AllocationQueueInfo {
    pub queue_params: AllocationQueueParams,
    pub creation_time: SystemTime,
    pub removal_time: Option<SystemTime>,

    pub allocations: Map<AllocationId, AllocationInfo>,
}

#[derive(Copy, Clone, Debug)]
pub struct AllocationInfo {
    pub worker_count: u64,
    pub queued_time: SystemTime,
    pub start_time: Option<SystemTime>,
    pub finish_time: Option<SystemTime>,
}

/// Status of an allocation at a given point in time.
#[derive(Copy, Clone)]
pub enum AllocationStatus {
    /// The allocation did not exist at this time.
    Missing,
    /// The allocation was queued.
    Queued,
    /// The allocation was running.
    Running,
    /// The allocation was finished.
    Finished,
}

pub fn get_allocation_status(info: &AllocationInfo, time: SystemTime) -> AllocationStatus {
    if time < info.queued_time {
        return AllocationStatus::Missing;
    }

    if let Some(finish_time) = info.finish_time {
        if finish_time < time {
            return AllocationStatus::Finished;
        }
    }

    if let Some(start_time) = info.start_time {
        if start_time < time {
            return AllocationStatus::Running;
        }
    }
    AllocationStatus::Queued
}
/// Stores the state of different allocation queues and their allocations
#[derive(Default)]
pub struct AllocationTimeline {
    queue_timelines: Map<QueueId, AllocationQueueInfo>,
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
    pub fn handle_new_events(&mut self, events: &[Event]) {
        for event in events {
            match &event.payload {
                EventPayload::AllocationQueueCreated(id, params) => {
                    self.queue_timelines.insert(
                        *id,
                        AllocationQueueInfo {
                            queue_params: *params.clone(),
                            creation_time: event.time.into(),
                            removal_time: None,
                            allocations: Default::default(),
                        },
                    );
                }
                EventPayload::AllocationQueueRemoved(queue_id) => {
                    let queue_state = self.queue_timelines.get_mut(queue_id).unwrap();
                    queue_state.removal_time = Some(event.time.into());
                }
                EventPayload::AllocationQueued {
                    queue_id,
                    allocation_id,
                    worker_count,
                } => {
                    let queue_state = self.queue_timelines.get_mut(queue_id).unwrap();
                    queue_state.add_queued_allocation(
                        allocation_id.clone(),
                        *worker_count,
                        event.time.into(),
                    );
                }
                EventPayload::AllocationStarted(queue_id, allocation_id) => {
                    let queue_state = self.queue_timelines.get_mut(queue_id).unwrap();
                    queue_state.update_allocation_state(
                        allocation_id,
                        AllocationStatus::Running,
                        event.time.into(),
                    );
                }
                EventPayload::AllocationFinished(queue_id, allocation_id) => {
                    let queue_state = self.queue_timelines.get_mut(queue_id).unwrap();
                    queue_state.update_allocation_state(
                        allocation_id,
                        AllocationStatus::Finished,
                        event.time.into(),
                    );
                }
                _ => {}
            }
        }
    }

    pub fn get_queue_infos_at(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (&QueueId, &AllocationQueueInfo)> + '_ {
        self.queue_timelines
            .iter()
            .filter(move |(_, info)| info.creation_time <= time)
    }

    pub fn get_queue_params_for(&self, queue_id: &QueueId) -> Option<&AllocationQueueParams> {
        self.queue_timelines
            .get(queue_id)
            .map(|queue_info| &queue_info.queue_params)
    }

    pub fn get_allocations_for_queue(
        &self,
        queue_id: QueueId,
    ) -> Option<impl Iterator<Item = (&AllocationId, &AllocationInfo)> + '_> {
        self.queue_timelines
            .get(&queue_id)
            .map(move |info| info.allocations.iter())
    }
}
