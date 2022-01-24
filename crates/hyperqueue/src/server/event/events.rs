use crate::server::autoalloc::DescriptorId;
use crate::transfer::messages::AllocationQueueParams;
use crate::WorkerId;
use serde::{Deserialize, Serialize};
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::LostWorkerReason;
use tako::messages::worker::WorkerOverview;
use tako::static_assert_size;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MonitoringEventPayload {
    // Workers
    WorkerConnected(WorkerId, Box<WorkerConfiguration>),
    WorkerLost(WorkerId, LostWorkerReason),
    WorkerOverviewReceived(WorkerOverview),
    // Allocations
    AllocationQueueCreated(DescriptorId, Box<AllocationQueueParams>),
    AllocationQueueRemoved(DescriptorId),
}

// Keep the size of the event structure in check
static_assert_size!(MonitoringEventPayload, 136);
