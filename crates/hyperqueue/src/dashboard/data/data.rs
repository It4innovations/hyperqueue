use crate::dashboard::data::time_interval::TimeMode;
use crate::dashboard::data::timelines::alloc_timeline::{
    AllocationInfo, AllocationQueueInfo, AllocationTimeline,
};
use crate::dashboard::data::timelines::job_timeline::{DashboardJobInfo, JobTimeline, TaskInfo};
use crate::dashboard::data::timelines::worker_timeline::WorkerTimeline;
use crate::dashboard::data::{Time, TimeRange};
use crate::server::autoalloc::{AllocationId, QueueId};
use crate::server::event::Event;
use crate::transfer::messages::AllocationQueueParams;
use std::time::{Duration, SystemTime};
use tako::WorkerId;
use tako::{JobId, JobTaskId};

const MIN_TIME_RANGE_DURATION: Duration = Duration::from_secs(60);

pub struct DashboardData {
    /// Tracks worker connection and loss events
    worker_timeline: WorkerTimeline,
    /// Tracks job related events
    job_timeline: JobTimeline,
    /// Tracks the automatic allocator events
    alloc_timeline: AllocationTimeline,
    /// Determines the active time range
    time_mode: TimeMode,
    /// Is streaming from a live server enabled?
    stream_enabled: bool,
    /// Warning that should be displayed to the user.
    warning: Option<String>,
}

impl DashboardData {
    pub fn new(time_mode: TimeMode, stream_enabled: bool) -> Self {
        Self {
            worker_timeline: Default::default(),
            job_timeline: Default::default(),
            alloc_timeline: Default::default(),
            time_mode,
            stream_enabled,
            warning: None,
        }
    }

    pub fn get_warning(&self) -> Option<&str> {
        self.warning.as_deref()
    }
    pub fn set_warning(&mut self, warning: Option<&str>) {
        self.warning = warning.map(String::from);
    }

    pub fn push_new_events(&mut self, events: Vec<Event>) {
        // Update data views
        self.worker_timeline.handle_new_events(&events);
        self.job_timeline.handle_new_events(&events);
        self.alloc_timeline.handle_new_events(&events);
    }

    pub fn query_job_info_for_job(&self, job_id: JobId) -> Option<&DashboardJobInfo> {
        self.job_timeline.get_job_info_for_job(job_id)
    }

    /// Gets the list of jobs that were created before `time`
    pub fn query_jobs_created_before(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (JobId, &DashboardJobInfo)> + '_ {
        self.job_timeline.get_jobs_created_before(time)
    }

    /// Get the list of tasks for a job where the tasks have started before `time`
    pub fn query_task_history_for_job(
        &self,
        job_id: JobId,
        time: SystemTime,
    ) -> impl Iterator<Item = (JobTaskId, &TaskInfo)> + '_ {
        self.job_timeline.get_job_task_history(job_id, time)
    }

    /// Gets the list of tasks that have run on a worker.
    pub fn query_task_history_for_worker(
        &self,
        worker_id: WorkerId,
    ) -> impl Iterator<Item = (JobTaskId, &TaskInfo)> + '_ {
        self.job_timeline
            .get_worker_task_history(worker_id, SystemTime::now())
    }

    /// Gets an iterator over the list of different allocation queues created before `time`.
    pub fn query_allocation_queues_at(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (&QueueId, &AllocationQueueInfo)> + '_ {
        self.alloc_timeline.get_queue_infos_at(time)
    }

    /// Gets the information about a given allocation queue.
    pub fn query_allocation_params(&self, queue_id: QueueId) -> Option<&AllocationQueueParams> {
        self.alloc_timeline.get_queue_params_for(&queue_id)
    }

    /// The Queued and Running allocations at `time` for a queue.
    pub fn query_allocations_info(
        &self,
        queue_id: QueueId,
    ) -> Option<impl Iterator<Item = (&AllocationId, &AllocationInfo)> + '_> {
        self.alloc_timeline.get_allocations_for_queue(queue_id)
    }

    pub fn set_time_range(&mut self, range: TimeRange) {
        // Make sure that the range doesn't go into the future, and that it is not too small
        let end_cutoff = match self.time_mode {
            TimeMode::Live(_) => SystemTime::now(),
            TimeMode::Fixed(range) => range.end(),
        };
        let end = range.end().min(end_cutoff);

        let start = match end.duration_since(range.start()) {
            Ok(duration) if duration < MIN_TIME_RANGE_DURATION => end - MIN_TIME_RANGE_DURATION,
            Err(_) => end - MIN_TIME_RANGE_DURATION,
            _ => range.start(),
        };

        let range = TimeRange::new(start, end);
        self.time_mode = TimeMode::Fixed(range);
    }

    pub fn set_live_time_mode(&mut self, duration: Duration) {
        self.time_mode = TimeMode::Live(duration);
    }

    pub fn stream_enabled(&self) -> bool {
        self.stream_enabled
    }

    pub fn is_live_time_mode(&self) -> bool {
        matches!(self.time_mode, TimeMode::Live(_))
    }

    pub fn current_time(&self) -> Time {
        self.time_mode.get_current_time()
    }

    pub fn current_time_range(&self) -> TimeRange {
        self.time_mode.get_time_range()
    }

    pub fn workers(&self) -> &WorkerTimeline {
        &self.worker_timeline
    }
}
