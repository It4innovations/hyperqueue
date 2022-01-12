use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::common::idcounter::IdCounter;
use crate::server::autoalloc::descriptor::QueueDescriptor;
use crate::Map;

/// Maximum number of autoalloc events stored in memory
const MAX_EVENT_QUEUE_LENGTH: usize = 100;

/// Maximum number of unsubmitted allocation directories kept on disk
pub(super) const MAX_UNSUBMITTED_DIRECTORIES_KEPT: usize = 16;

pub type DescriptorId = u32;

pub struct AutoAllocState {
    /// How often should the auto alloc process be invoked?
    refresh_interval: Duration,
    descriptors: Map<DescriptorId, DescriptorState>,
    descriptor_id_counter: IdCounter,
}

impl AutoAllocState {
    pub fn new(refresh_interval: Duration) -> AutoAllocState {
        Self {
            refresh_interval,
            descriptors: Default::default(),
            descriptor_id_counter: IdCounter::new(1),
        }
    }

    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    pub fn create_id(&mut self) -> DescriptorId {
        self.descriptor_id_counter.increment()
    }

    pub fn add_descriptor(&mut self, id: DescriptorId, descriptor: QueueDescriptor) {
        assert!(self.descriptors.insert(id, descriptor.into()).is_none());
    }
    pub fn remove_descriptor(&mut self, id: DescriptorId) {
        assert!(self.descriptors.remove(&id).is_some());
    }

    pub fn get_descriptor(&self, key: DescriptorId) -> Option<&DescriptorState> {
        self.descriptors.get(&key)
    }

    pub fn get_descriptor_mut(&mut self, key: DescriptorId) -> Option<&mut DescriptorState> {
        self.descriptors.get_mut(&key)
    }

    pub fn descriptor_ids(&self) -> impl Iterator<Item = DescriptorId> + '_ {
        self.descriptors.keys().copied()
    }

    pub fn descriptors(&self) -> impl Iterator<Item = (DescriptorId, &DescriptorState)> {
        self.descriptors.iter().map(|(k, v)| (*k, v))
    }
}

/// Represents the state of a single allocation queue.
pub struct DescriptorState {
    pub descriptor: QueueDescriptor,
    /// Active allocations.
    allocations: Map<AllocationId, Allocation>,
    /// Records events that have occurred on this queue.
    events: VecDeque<AllocationEventHolder>,
    /// Directories of allocations that have failed to be submitted.
    unsubmitted_allocation_directories: VecDeque<PathBuf>,
}

impl From<QueueDescriptor> for DescriptorState {
    fn from(descriptor: QueueDescriptor) -> Self {
        Self {
            descriptor,
            allocations: Default::default(),
            events: Default::default(),
            unsubmitted_allocation_directories: Default::default(),
        }
    }
}

impl DescriptorState {
    pub fn add_event<T: Into<AllocationEventHolder>>(&mut self, event: T) {
        self.events.push_back(event.into());
        if self.events.len() > MAX_EVENT_QUEUE_LENGTH {
            self.events.pop_front();
        }
    }

    pub fn get_events(&self) -> &VecDeque<AllocationEventHolder> {
        &self.events
    }

    pub fn get_allocation(&self, id: &str) -> Option<&Allocation> {
        self.allocations.get(id)
    }

    pub fn get_allocation_mut(&mut self, id: &str) -> Option<&mut Allocation> {
        self.allocations.get_mut(id)
    }

    pub fn active_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.allocations.values().filter(|alloc| alloc.is_active())
    }

    pub fn queued_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.allocations
            .values()
            .filter(|alloc| matches!(alloc.status, AllocationStatus::Queued))
    }

    pub fn all_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.allocations.values()
    }

    pub fn add_allocation(&mut self, allocation: Allocation) {
        if let Some(allocation) = self.allocations.insert(allocation.id.clone(), allocation) {
            log::warn!("Duplicate allocation detected: {}", allocation.id);
        }
    }

    pub fn remove_allocation(&mut self, key: &str) {
        if self.allocations.remove(key).is_none() {
            log::warn!("Trying to remove non-existent allocation {}", key);
        }
    }

    /// Stores the directory of an unsubmitted allocation that should be later deleted.
    ///
    /// The directory is not deleted right away to allow the user to debug potential failures.
    pub fn store_unsubmitted_directory(&mut self, directory: PathBuf) {
        self.unsubmitted_allocation_directories.push_back(directory);
    }

    /// Returns directories of allocations that have failed to be submitted that are scheduled for
    /// removal.
    pub fn get_submission_directories_for_removal(&mut self) -> Vec<PathBuf> {
        let mut to_remove = Vec::new();
        while self.unsubmitted_allocation_directories.len() > MAX_UNSUBMITTED_DIRECTORIES_KEPT {
            let directory = self.unsubmitted_allocation_directories.pop_front().unwrap();
            to_remove.push(directory);
        }
        to_remove
    }
}

pub type AllocationId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    pub id: AllocationId,
    pub worker_count: u64,
    pub queued_at: SystemTime,
    pub status: AllocationStatus,
    pub working_dir: PathBuf,
}

impl Allocation {
    /// Returns true if the allocation is currently in queue or running
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            AllocationStatus::Queued | AllocationStatus::Running { .. }
        )
    }

    /// Returns true if the allocation is currently running
    pub fn is_running(&self) -> bool {
        matches!(self.status, AllocationStatus::Running { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationStatus {
    Queued,
    Running {
        started_at: SystemTime,
    },
    Finished {
        started_at: SystemTime,
        finished_at: SystemTime,
    },
    Failed {
        started_at: SystemTime,
        finished_at: SystemTime,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationEventHolder {
    pub date: SystemTime,
    pub event: AllocationEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationEvent {
    AllocationQueued(AllocationId),
    AllocationStarted(AllocationId),
    AllocationFinished(AllocationId),
    AllocationFailed(AllocationId),
    AllocationDisappeared(AllocationId),
    QueueFail { error: String },
    StatusFail { error: String },
}

impl From<AllocationEvent> for AllocationEventHolder {
    fn from(event: AllocationEvent) -> Self {
        Self {
            date: SystemTime::now(),
            event,
        }
    }
}
