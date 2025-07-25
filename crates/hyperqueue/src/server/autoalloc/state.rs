use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use std::time::Instant;
use tako::Set;
use tako::WorkerId;
use tako::gateway::LostWorkerReason;

use crate::common::idcounter::IdCounter;
use crate::common::manager::info::ManagerType;
use crate::common::utils::time::{AbsoluteTime, now_monotonic};
use crate::server::autoalloc::config::MAX_KEPT_DIRECTORIES;
use crate::server::autoalloc::queue::QueueHandler;
use crate::server::autoalloc::{LostWorkerDetails, QueueInfo};
use tako::Map;
use tako::resources::ResourceDescriptor;
use tako::worker::WorkerConfiguration;

// Main state holder
pub struct AutoAllocState {
    allocation_to_queue: Map<AllocationId, QueueId>,
    queues: Map<QueueId, AllocationQueue>,
    inactive_allocation_directories: VecDeque<AllocationWorkdir>,
    max_kept_directories: usize,
    queue_id_counter: IdCounter,
}

impl AutoAllocState {
    pub fn new(queue_id_counter: u32) -> Self {
        Self {
            allocation_to_queue: Default::default(),
            queues: Default::default(),
            inactive_allocation_directories: Default::default(),
            max_kept_directories: MAX_KEPT_DIRECTORIES,
            queue_id_counter: IdCounter::new(queue_id_counter),
        }
    }

    fn create_id(&mut self) -> QueueId {
        self.queue_id_counter.increment()
    }

    pub fn add_queue(&mut self, queue: AllocationQueue, queue_id: Option<QueueId>) -> QueueId {
        let id = queue_id.unwrap_or_else(|| self.create_id());
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

    pub fn get_allocation_by_id(&self, allocation_id: &str) -> Option<Allocation> {
        self.allocation_to_queue
            .get(allocation_id)
            .and_then(|queue_id| self.queues.get(queue_id))
            .and_then(|queue| queue.allocations.get(allocation_id))
            .cloned()
    }

    pub fn get_queue(&self, id: QueueId) -> Option<&AllocationQueue> {
        self.queues.get(&id)
    }

    pub fn get_queue_mut(&mut self, key: QueueId) -> Option<&mut AllocationQueue> {
        self.queues.get_mut(&key)
    }

    pub fn queue_ids(&self) -> impl Iterator<Item = QueueId> + '_ {
        self.queues.keys().copied()
    }

    pub fn queues(&self) -> impl Iterator<Item = (QueueId, &AllocationQueue)> {
        self.queues.iter().map(|(k, v)| (*k, v))
    }

    pub fn queues_mut(&mut self) -> impl Iterator<Item = (QueueId, &mut AllocationQueue)> {
        self.queues.iter_mut().map(|(k, v)| (*k, v))
    }

    pub fn add_inactive_directory(&mut self, directory: AllocationWorkdir) {
        self.inactive_allocation_directories.push_back(directory);
    }

    /// Returns directories of inactive allocations that are scheduled for removal.
    pub fn get_directories_for_removal(&mut self) -> Vec<AllocationWorkdir> {
        let mut to_remove = Vec::new();
        while self.inactive_allocation_directories.len() > self.max_kept_directories {
            let directory = self.inactive_allocation_directories.pop_front().unwrap();
            to_remove.push(directory);
        }
        to_remove
    }

    pub fn has_active_queues(&self) -> bool {
        self.queues.values().any(|q| q.state().is_active())
    }

    #[cfg(test)]
    pub fn set_max_kept_directories(&mut self, count: usize) {
        self.max_kept_directories = count;
    }
    #[cfg(test)]
    pub fn set_inactive_allocation_directories(&mut self, dirs: VecDeque<PathBuf>) {
        self.inactive_allocation_directories = dirs.into_iter().map(|d| d.into()).collect();
    }
}

// Queue
pub type QueueId = u32;

#[derive(Debug, PartialEq)]
pub enum AllocationQueueState {
    /// The queue is being processed as normal.
    Active,
    /// The queue is paused. Its allocations are still being refreshed, but no new allocations
    /// will be submitted until the queue is resumed.
    ///
    /// A queue can be paused manually by a user, or automatically by autoalloc itself, after too
    /// many submission/allocation failures.
    Paused,
}

impl AllocationQueueState {
    pub fn is_active(&self) -> bool {
        matches!(self, AllocationQueueState::Active)
    }
}

/// Represents the state of a single allocation queue.
pub struct AllocationQueue {
    state: AllocationQueueState,
    allocations: Map<AllocationId, Allocation>,
    info: QueueInfo,
    name: Option<String>,
    handler: Box<dyn QueueHandler>,
    rate_limiter: RateLimiter,
    /// Worker resources from a worker that connected from an allocation
    /// attached to this queue.
    worker_resources: Option<ResourceDescriptor>,
}

impl AllocationQueue {
    pub fn new(
        info: QueueInfo,
        name: Option<String>,
        handler: Box<dyn QueueHandler>,
        rate_limiter: RateLimiter,
        worker_resources: Option<ResourceDescriptor>,
    ) -> Self {
        Self {
            state: AllocationQueueState::Active,
            info,
            name,
            handler,
            allocations: Default::default(),
            rate_limiter,
            worker_resources,
        }
    }

    pub fn state(&self) -> &AllocationQueueState {
        &self.state
    }

    pub fn pause(&mut self) {
        self.state = AllocationQueueState::Paused;
    }

    pub fn resume(&mut self) {
        self.state = AllocationQueueState::Active;
    }

    pub fn manager(&self) -> &ManagerType {
        self.info.manager()
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

    #[cfg(test)]
    pub fn set_handler(&mut self, handler: Box<dyn QueueHandler>) {
        self.handler = handler;
    }

    pub fn limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    pub fn limiter_mut(&mut self) -> &mut RateLimiter {
        &mut self.rate_limiter
    }

    pub fn add_allocation(&mut self, allocation: Allocation) {
        assert!(
            self.allocations
                .insert(allocation.id.clone(), allocation)
                .is_none()
        );
    }

    pub fn get_allocation_mut(&mut self, id: &str) -> Option<&mut Allocation> {
        self.allocations.get_mut(id)
    }

    pub fn all_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.allocations.values()
    }

    pub fn queued_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.all_allocations()
            .filter(|alloc| matches!(alloc.status, AllocationState::Queued { .. }))
    }

    pub fn active_allocations(&self) -> impl Iterator<Item = &Allocation> {
        self.all_allocations().filter(|alloc| alloc.is_active())
    }
    pub fn active_allocations_mut(&mut self) -> impl Iterator<Item = &mut Allocation> {
        self.allocations
            .values_mut()
            .filter(|alloc| alloc.is_active())
    }

    pub fn register_worker_resources(&mut self, config: WorkerConfiguration) {
        // Always overwrite the config with the latest one that we have seen
        self.worker_resources = Some(config.resources);
    }

    pub fn get_worker_resources(&self) -> Option<&ResourceDescriptor> {
        self.worker_resources.as_ref()
    }

    /// How many workers are currently queued or running?
    pub fn active_worker_count(&self) -> u32 {
        self.active_allocations()
            .map(|allocation| allocation.target_worker_count as u32)
            .sum::<u32>()
    }

    /// Returns true only if at least a single allocation could be added to this queue.
    pub fn has_space_for_submit(&self) -> bool {
        if self.queued_allocations().count() >= self.info.backlog() as usize {
            return false;
        }
        if let Some(max_worker_count) = self.info.max_worker_count() {
            if self.active_worker_count() >= max_worker_count {
                return false;
            }
        }
        true
    }
}

// Allocation
pub type AllocationId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationWorkdir(PathBuf);

impl AllocationWorkdir {
    pub fn stdout(&self) -> PathBuf {
        self.0.join("stdout")
    }

    pub fn stderr(&self) -> PathBuf {
        self.0.join("stderr")
    }
}

impl From<PathBuf> for AllocationWorkdir {
    fn from(value: PathBuf) -> Self {
        Self(value)
    }
}

impl AsRef<Path> for AllocationWorkdir {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    pub id: AllocationId,
    pub target_worker_count: u64,
    pub queued_at: AbsoluteTime,
    pub status: AllocationState,
    pub working_dir: AllocationWorkdir,
}

impl Allocation {
    pub fn new(id: AllocationId, target_worker_count: u64, working_dir: AllocationWorkdir) -> Self {
        Self {
            id,
            target_worker_count,
            queued_at: AbsoluteTime::now(),
            status: AllocationState::Queued {
                status_error_count: 0,
            },
            working_dir,
        }
    }

    /// Returns true if the allocation is currently in queue or running
    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            AllocationState::Queued { .. } | AllocationState::Running { .. }
        )
    }

    /// Returns true if the allocation is currently running
    pub fn is_running(&self) -> bool {
        matches!(self.status, AllocationState::Running { .. })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DisconnectedWorkers {
    workers: Map<WorkerId, LostWorkerDetails>,
}

impl DisconnectedWorkers {
    pub fn add_lost_worker(&mut self, worker_id: WorkerId, details: LostWorkerDetails) {
        self.workers.insert(worker_id, details);
    }
    pub fn count(&self) -> u64 {
        self.workers.len() as u64
    }

    /// Returns true if all workers that have disconnected have crashed.
    /// A worker is considered to be failed if it has lost connection to the server very soon
    /// after it has connected.
    pub fn all_crashed(&self) -> bool {
        self.workers.values().all(|details| {
            matches!(
                details.reason,
                LostWorkerReason::ConnectionLost | LostWorkerReason::HeartbeatLost
            ) && details.lifetime <= Duration::from_secs(60)
        })
    }
}

impl IntoIterator for DisconnectedWorkers {
    type Item = (WorkerId, LostWorkerDetails);
    type IntoIter = <Map<WorkerId, LostWorkerDetails> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.workers.into_iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationState {
    /// The allocation was submitted, and it is waiting in the queue.
    /// Once a worker connects to the allocation, it will be switched away from the Queued state.
    /// However, it is possible that a worker never connects, and we encounter refresh errors that
    /// will prevent this state to be ever changed, thus blocking other allocations.
    Queued { status_error_count: u32 },
    /// The same thing can happen for the running state, because it might never end if not enough
    /// workers ever connect to it.
    Running {
        started_at: AbsoluteTime,
        connected_workers: Set<WorkerId>,
        disconnected_workers: DisconnectedWorkers,
        status_error_count: u32,
    },
    /// The allocation has finished by connecting and disconnecting the expected amount of workers.
    Finished {
        started_at: AbsoluteTime,
        finished_at: AbsoluteTime,
        disconnected_workers: DisconnectedWorkers,
    },
    /// Zero (or not enough) workers have connected, but the allocation has been finished.
    FinishedUnexpectedly {
        connected_workers: Set<WorkerId>,
        disconnected_workers: DisconnectedWorkers,
        started_at: Option<AbsoluteTime>,
        finished_at: AbsoluteTime,
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
    last_submission: Option<Instant>,
    /// How many times has an allocation failed in a row.
    allocation_fails: u64,
    max_allocation_fails: u64,
    /// How many times has the submission failed in a row.
    submission_fails: u64,
    max_submission_fails: u64,
}

impl RateLimiter {
    pub fn new(
        submission_delays: Vec<Duration>,
        max_submission_fails: u64,
        max_allocation_fails: u64,
    ) -> Self {
        assert!(!submission_delays.is_empty());
        Self {
            submission_delays,
            current_delay: 0,
            last_submission: None,
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
        self.last_submission = Some(now_monotonic());
    }

    pub fn submission_status(&self) -> RateLimiterStatus {
        if self.allocation_fails >= self.max_allocation_fails {
            return RateLimiterStatus::TooManyFailedAllocations;
        }
        if self.submission_fails >= self.max_submission_fails {
            return RateLimiterStatus::TooManyFailedSubmissions;
        }

        match self.last_submission {
            Some(last_submission) => {
                let time = now_monotonic();
                let duration = time.duration_since(last_submission);
                let delay = self.submission_delays[self.current_delay];

                if duration < delay {
                    RateLimiterStatus::Wait
                } else {
                    RateLimiterStatus::Ok
                }
            }
            None => RateLimiterStatus::Ok,
        }
    }

    fn increase_delay(&mut self) {
        if self.current_delay < self.submission_delays.len() - 1 {
            self.current_delay += 1;
        }
    }

    #[cfg(test)]
    pub fn allocation_fail_count(&self) -> u64 {
        self.allocation_fails
    }
}

#[cfg(test)]
mod tests {
    use crate::common::manager::info::ManagerType;
    use crate::server::autoalloc::queue::{
        AllocationStatusMap, AllocationSubmissionResult, QueueHandler, SubmitMode,
    };
    use crate::server::autoalloc::state::{AllocationQueue, AutoAllocState, RateLimiter};
    use crate::server::autoalloc::{
        Allocation, AutoAllocResult, QueueId, QueueInfo, QueueParameters,
    };
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::time::Duration;

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
        let mut state = AutoAllocState::new(1);
        let _id = state.create_id();
        let id = state.add_queue(
            AllocationQueue::new(
                QueueInfo::new(QueueParameters {
                    manager: ManagerType::Pbs,
                    max_workers_per_alloc: 1,
                    backlog: 1,
                    timelimit: Duration::from_secs(1),
                    name: None,
                    max_worker_count: None,
                    min_utilization: None,
                    additional_args: vec![],
                    worker_start_cmd: None,
                    worker_stop_cmd: None,
                    cli_resource_descriptor: None,
                    worker_args: vec![],
                    idle_timeout: None,
                }),
                None,
                Box::new(NullHandler),
                RateLimiter::new(vec![Duration::from_secs(1)], 1, 1),
                None,
            ),
            None,
        );
        state.add_allocation(
            Allocation::new("1".to_string(), 1, PathBuf::default().into()),
            id,
        );
        state.add_allocation(
            Allocation::new("2".to_string(), 1, PathBuf::default().into()),
            id,
        );
        assert_eq!(state.allocation_to_queue.len(), 2);
        state.remove_queue(id);
        assert_eq!(state.allocation_to_queue.len(), 0);
    }
}
