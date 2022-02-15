use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::common::idcounter::IdCounter;
use crate::common::timeutils::now_monotonic;
use crate::server::autoalloc::descriptor::QueueDescriptor;
use crate::Map;

/// Maximum number of autoalloc events stored in memory
const MAX_EVENT_QUEUE_LENGTH: usize = 100;

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

    pub fn add_descriptor(
        &mut self,
        id: DescriptorId,
        descriptor: QueueDescriptor,
        limiter: RateLimiter,
    ) {
        let state = DescriptorState::new(descriptor, limiter);
        assert!(self.descriptors.insert(id, state).is_none());
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
    /// Directories of allocations that are no longer active and serve only for debugging purposes.
    inactive_allocation_directories: VecDeque<PathBuf>,
    rate_limiter: RateLimiter,
}

impl DescriptorState {
    pub fn new(descriptor: QueueDescriptor, rate_limiter: RateLimiter) -> Self {
        Self {
            descriptor,
            allocations: Default::default(),
            events: Default::default(),
            inactive_allocation_directories: Default::default(),
            rate_limiter,
        }
    }

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

    /// Stores the directory of an inactive allocation that should be later deleted.
    ///
    /// The directory is not deleted right away to allow the user to debug potential failures.
    pub fn add_inactive_directory(&mut self, directory: PathBuf) {
        self.inactive_allocation_directories.push_back(directory);
    }

    /// Returns directories of inactive allocations that are scheduled for removal.
    pub fn get_directories_for_removal(&mut self) -> Vec<PathBuf> {
        let mut to_remove = Vec::new();
        while self.inactive_allocation_directories.len() > self.descriptor.max_kept_directories() {
            let directory = self.inactive_allocation_directories.pop_front().unwrap();
            to_remove.push(directory);
        }
        to_remove
    }

    pub fn get_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }
    pub fn get_limiter_mut(&mut self) -> &mut RateLimiter {
        &mut self.rate_limiter
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

#[derive(Debug)]
pub enum RateLimiterStatus {
    Ok,
    Wait,
    TooManyFailedSubmissions,
    TooManyFailedAllocations,
}

/// Limits how often does the automatic allocation subsystem submits new submissions.
/// Each descriptor has its own rate limiter.
///
/// The limiter uses a Vec of delays which are used to block off submission.
/// When an allocation or a submission fails, the delay is increased, until it reaches the last
/// element of the Vec.
///
/// When the maximum number of (successive) allocation or submission failures is reached, the `status`
/// method will return [`RateLimiterStatus::TooManyFailedSubmissions`] or [`RateLimiterStatus::TooManyFailedAllocations`].
pub struct RateLimiter {
    delays: Vec<Duration>,
    /// Which delay rate to currently use.
    /// Index into `delays`.
    current_delay: usize,
    last_check: Instant,
    /// How many times has an allocation failed in a row.
    allocation_fails: usize,
    max_allocation_fails: usize,
    /// How many times has the submission failed in a row.
    submission_fails: usize,
    max_submission_fails: usize,
}

impl RateLimiter {
    pub fn new(
        delays: Vec<Duration>,
        max_submission_fails: usize,
        max_allocation_fails: usize,
    ) -> Self {
        assert!(!delays.is_empty());
        Self {
            delays,
            current_delay: 0,
            last_check: now_monotonic(),
            allocation_fails: 0,
            max_allocation_fails,
            submission_fails: 0,
            max_submission_fails,
        }
    }

    /// An allocation was submitted successfully into the target system.
    pub fn on_submission_success(&mut self) {
        self.submission_fails = 0;
        if self.allocation_fails == 0 {
            self.current_delay = 0;
        }
    }
    /// An allocation was not submitted successfully, it was rejected by the target system.
    pub fn on_submission_fail(&mut self) {
        self.submission_fails += 1;
        self.increase_delay();
    }
    /// An allocation has finished its execution time and exited with a success exit code.
    pub fn on_allocation_success(&mut self) {
        self.allocation_fails = 0;
        self.current_delay = 0;
    }
    /// An allocation has finished with an error.
    pub fn on_allocation_fail(&mut self) {
        self.allocation_fails += 1;
        self.increase_delay();
    }

    /// Submission will be attempted, reset the limiter timer.
    pub fn on_submission_attempt(&mut self) {
        self.last_check = now_monotonic();
    }

    pub fn status(&self) -> RateLimiterStatus {
        if self.allocation_fails >= self.max_allocation_fails {
            return RateLimiterStatus::TooManyFailedAllocations;
        }
        if self.submission_fails >= self.max_submission_fails {
            return RateLimiterStatus::TooManyFailedSubmissions;
        }
        let time = now_monotonic();
        let duration = time.duration_since(self.last_check);
        let delay = self.delays[self.current_delay];

        if duration < delay {
            RateLimiterStatus::Wait
        } else {
            RateLimiterStatus::Ok
        }
    }

    fn increase_delay(&mut self) {
        if self.current_delay < self.delays.len() - 1 {
            self.current_delay += 1;
        }
    }
}
