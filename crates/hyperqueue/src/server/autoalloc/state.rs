use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use std::time::Instant;
use tako::common::Set;
use tako::messages::gateway::LostWorkerReason;
use tako::WorkerId;

use crate::common::idcounter::IdCounter;
use crate::common::manager::info::ManagerType;
use crate::common::timeutils::now_monotonic;
use crate::server::autoalloc::config::MAX_KEPT_DIRECTORIES;
use crate::server::autoalloc::queue::QueueHandler;
use crate::server::autoalloc::QueueInfo;
use crate::Map;

// Main state holder
pub struct AutoAllocState {
    allocation_to_queue: Map<AllocationId, QueueId>,
    queues: Map<QueueId, QueueState>,
    inactive_allocation_directories: VecDeque<PathBuf>,
    max_kept_directories: usize,
    queue_id_counter: IdCounter,
}

impl AutoAllocState {
    pub fn new() -> Self {
        Self {
            allocation_to_queue: Default::default(),
            queues: Default::default(),
            inactive_allocation_directories: Default::default(),
            max_kept_directories: MAX_KEPT_DIRECTORIES,
            queue_id_counter: IdCounter::new(1),
        }
    }

    fn create_id(&mut self) -> QueueId {
        self.queue_id_counter.increment()
    }

    pub fn add_queue(&mut self, queue: QueueState) -> QueueId {
        let id = self.create_id();
        assert!(self.queues.insert(id, queue).is_none());
        id
    }
    pub fn remove_queue(&mut self, id: QueueId) {
        let queue = self
            .queues
            .remove(&id)
            .unwrap_or_else(|| panic!("Queue {id} not found"));
        for (alloc_id, _) in queue.allocations {
            assert!(self.allocation_to_queue.remove(&alloc_id).is_some());
        }
    }

    pub fn add_allocation(&mut self, allocation: Allocation, queue_id: QueueId) {
        self.allocation_to_queue
            .insert(allocation.id.to_string(), queue_id);
        self.queues
            .get_mut(&queue_id)
            .expect("Could not find queue")
            .add_allocation(allocation);
    }

    pub fn get_queue_id_by_allocation(&self, allocation_id: &str) -> Option<QueueId> {
        self.allocation_to_queue.get(allocation_id).copied()
    }

    pub fn get_queue(&self, id: QueueId) -> Option<&QueueState> {
        self.queues.get(&id)
    }

    pub fn get_queue_mut(&mut self, key: QueueId) -> Option<&mut QueueState> {
        self.queues.get_mut(&key)
    }

    pub fn queue_ids(&self) -> impl Iterator<Item = QueueId> + '_ {
        self.queues.keys().copied()
    }

    pub fn queues(&self) -> impl Iterator<Item = (QueueId, &QueueState)> {
        self.queues.iter().map(|(k, v)| (*k, v))
    }

    pub fn add_inactive_directory(&mut self, directory: PathBuf) {
        self.inactive_allocation_directories.push_back(directory);
    }

    /// Returns directories of inactive allocations that are scheduled for removal.
    pub fn get_directories_for_removal(&mut self) -> Vec<PathBuf> {
        let mut to_remove = Vec::new();
        while self.inactive_allocation_directories.len() > self.max_kept_directories {
            let directory = self.inactive_allocation_directories.pop_front().unwrap();
            to_remove.push(directory);
        }
        to_remove
    }

    #[cfg(test)]
    pub(crate) fn set_max_kept_directories(&mut self, count: usize) {
        self.max_kept_directories = count;
    }
    #[cfg(test)]
    pub(crate) fn set_inactive_allocation_directories(&mut self, dirs: VecDeque<PathBuf>) {
        self.inactive_allocation_directories = dirs;
    }
}

// Queue
pub type QueueId = u32;

/// Represents the state of a single allocation queue.
pub struct QueueState {
    allocations: Map<AllocationId, Allocation>,
    manager: ManagerType,
    info: QueueInfo,
    name: Option<String>,
    handler: Box<dyn QueueHandler>,
    rate_limiter: RateLimiter,
}

impl QueueState {
    pub fn new(
        manager: ManagerType,
        info: QueueInfo,
        name: Option<String>,
        handler: Box<dyn QueueHandler>,
        rate_limiter: RateLimiter,
    ) -> Self {
        Self {
            manager,
            info,
            name,
            handler,
            allocations: Default::default(),
            rate_limiter,
        }
    }

    pub fn manager(&self) -> &ManagerType {
        &self.manager
    }

    pub fn info(&self) -> &QueueInfo {
        &self.info
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn handler(&self) -> &dyn QueueHandler {
        self.handler.as_ref()
    }

    pub fn handler_mut(&mut self) -> &mut dyn QueueHandler {
        self.handler.as_mut()
    }

    pub fn limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    pub fn limiter_mut(&mut self) -> &mut RateLimiter {
        &mut self.rate_limiter
    }

    pub fn add_allocation(&mut self, allocation: Allocation) {
        assert!(self
            .allocations
            .insert(allocation.id.clone(), allocation)
            .is_none());
    }

    pub fn get_allocation_mut(&mut self, id: &str) -> Option<&mut Allocation> {
        self.allocations.get_mut(id)
    }

    pub fn all_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.allocations.values()
    }

    pub fn queued_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.all_allocations()
            .filter(|alloc| matches!(alloc.status, AllocationState::Queued))
    }
    pub fn active_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.all_allocations().filter(|alloc| alloc.is_active())
    }
}

// Allocation
pub type AllocationId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    pub id: AllocationId,
    pub target_worker_count: u64,
    pub queued_at: SystemTime,
    pub status: AllocationState,
    pub working_dir: PathBuf,
}

impl Allocation {
    pub fn new(id: AllocationId, target_worker_count: u64, working_dir: PathBuf) -> Self {
        Self {
            id,
            target_worker_count,
            queued_at: SystemTime::now(),
            status: AllocationState::Queued,
            working_dir,
        }
    }

    /// Returns true if the allocation is currently in queue or running
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            AllocationState::Queued | AllocationState::Running { .. }
        )
    }

    /// Returns true if the allocation is currently running
    pub fn is_running(&self) -> bool {
        matches!(self.status, AllocationState::Running { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationState {
    Queued,
    Running {
        started_at: SystemTime,
        connected_workers: Set<WorkerId>,
        disconnected_workers: Map<WorkerId, LostWorkerReason>,
    },
    // The allocation has finished by connecting and disconnecting the expected amount of workers.
    Finished {
        started_at: SystemTime,
        finished_at: SystemTime,
        disconnected_workers: Map<WorkerId, LostWorkerReason>,
    },
    // Zero (or not enough) workers have connected, but the allocation has been finished.
    Invalid {
        connected_workers: Set<WorkerId>,
        disconnected_workers: Map<WorkerId, LostWorkerReason>,
        started_at: Option<SystemTime>,
        finished_at: SystemTime,
        failed: bool,
    },
}

// Rate limiter
#[derive(Debug)]
pub enum RateLimiterStatus {
    Ok,
    Wait,
    TooManyFailedSubmissions,
    TooManyFailedAllocations,
}

/// Limits how often does the automatic allocation subsystem submits new submissions.
/// Each queue has its own rate limiter.
///
/// The limiter uses a Vec of delays which are used to block off submission.
/// When an allocation or a submission fails, the delay is increased, until it reaches the last
/// element of the Vec.
///
/// When the maximum number of (successive) allocation or submission failures is reached, the `status`
/// method will return [`RateLimiterStatus::TooManyFailedSubmissions`] or [`RateLimiterStatus::TooManyFailedAllocations`].
#[derive(Debug)]
pub struct RateLimiter {
    submission_delays: Vec<Duration>,
    /// Which delay rate to currently use.
    /// Index into `delays`.
    current_delay: usize,
    /// Time when a submission (e.g. qsub) was last attempted.
    last_submission: Instant,
    /// Time when a status check (e.g. qstat) was last attempted.
    last_status: Instant,
    /// How often can status be checked.
    status_delay: Duration,
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
        status_delay: Duration,
    ) -> Self {
        assert!(!delays.is_empty());
        Self {
            submission_delays: delays,
            current_delay: 0,
            last_submission: now_monotonic(),
            last_status: now_monotonic(),
            status_delay,
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

    pub fn on_status_attempt(&mut self) {
        self.last_status = now_monotonic();
    }

    pub fn can_perform_status_check(&self) -> bool {
        now_monotonic().duration_since(self.last_status) >= self.status_delay
    }

    /// Submission will be attempted, reset the limiter timer.
    pub fn on_submission_attempt(&mut self) {
        self.last_submission = now_monotonic();
    }

    pub fn submission_status(&self) -> RateLimiterStatus {
        if self.allocation_fails >= self.max_allocation_fails {
            return RateLimiterStatus::TooManyFailedAllocations;
        }
        if self.submission_fails >= self.max_submission_fails {
            return RateLimiterStatus::TooManyFailedSubmissions;
        }
        let time = now_monotonic();
        let duration = time.duration_since(self.last_submission);
        let delay = self.submission_delays[self.current_delay];

        if duration < delay {
            RateLimiterStatus::Wait
        } else {
            RateLimiterStatus::Ok
        }
    }

    fn increase_delay(&mut self) {
        if self.current_delay < self.submission_delays.len() - 1 {
            self.current_delay += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::manager::info::ManagerType;
    use crate::server::autoalloc::queue::{
        AllocationStatusMap, AllocationSubmissionResult, QueueHandler, SubmitMode,
    };
    use crate::server::autoalloc::state::{AutoAllocState, QueueState, RateLimiter};
    use crate::server::autoalloc::{Allocation, AutoAllocResult, QueueId, QueueInfo};
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;
    use tako::worker::state::ServerLostPolicy;

    struct NullHandler;
    impl QueueHandler for NullHandler {
        fn submit_allocation(
            &mut self,
            _queue_id: QueueId,
            _queue_info: &QueueInfo,
            _worker_count: u64,
            _mode: SubmitMode,
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
            unreachable!()
        }

        fn get_status_of_allocations(
            &self,
            _allocations: &[&Allocation],
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>> {
            unreachable!()
        }

        fn remove_allocation(
            &self,
            _allocation: &Allocation,
        ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
            unreachable!()
        }
    }

    #[test]
    fn remove_allocations_from_map_when_removing_queue() {
        let mut state = AutoAllocState::new();
        let _id = state.create_id();
        let id = state.add_queue(QueueState::new(
            ManagerType::Pbs,
            QueueInfo::new(
                1,
                1,
                Duration::from_secs(1),
                ServerLostPolicy::FinishRunning,
                vec![],
                None,
                vec![],
                None,
            ),
            None,
            Box::new(NullHandler),
            RateLimiter::new(vec![Duration::from_secs(1)], 1, 1, Duration::from_secs(1)),
        ));
        state.add_allocation(Allocation::new("1".to_string(), 1, Default::default()), id);
        state.add_allocation(Allocation::new("2".to_string(), 1, Default::default()), id);
        assert_eq!(state.allocation_to_queue.len(), 2);
        state.remove_queue(id);
        assert_eq!(state.allocation_to_queue.len(), 0);
    }
}
