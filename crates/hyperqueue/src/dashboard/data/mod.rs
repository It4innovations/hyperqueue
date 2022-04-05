use crate::dashboard::data::worker_timeline::WorkerTimeline;
use std::time::{Duration, SystemTime};
use tako::common::WrappedRcRefCell;
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::MonitoringEventRequest;
use tako::messages::worker::WorkerOverview;

use crate::dashboard::data::alloc_timeline::{
    AllocationInfo, AllocationQueueInfo, AllocationTimeline,
};
use crate::dashboard::data::job_timeline::{DashboardJobInfo, JobTimeline, TaskInfo};
use crate::server::autoalloc::{AllocationId, QueueId};
use crate::server::event::MonitoringEvent;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{AllocationQueueParams, FromClientMessage, ToClientMessage};
use crate::{rpc_call, JobId, JobTaskId, WorkerId};

pub mod alloc_timeline;
pub mod job_timeline;
pub mod worker_timeline;

#[derive(Default)]
pub struct DashboardData {
    /// The event_id until which the data has already been fetched for
    fetched_until: Option<u32>,
    /// Tracks worker connection and loss events
    worker_timeline: WorkerTimeline,
    /// Tracks job related events
    job_timeline: JobTimeline,
    /// Tracks the automatic allocator events
    alloc_timeline: AllocationTimeline,
}

impl DashboardData {
    pub fn last_fetched_id(&self) -> Option<u32> {
        self.fetched_until
    }

    pub fn update_data(&mut self, mut events: Vec<MonitoringEvent>) {
        events.sort_unstable_by_key(|e| e.time());

        // Update maximum event ID
        self.fetched_until = events
            .iter()
            .map(|event| event.id())
            .max()
            .or(self.fetched_until);

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
    ) -> impl Iterator<Item = (&JobId, &DashboardJobInfo)> + '_ {
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
    pub fn query_allocations_info_at(
        &self,
        queue_id: QueueId,
        time: SystemTime,
    ) -> Option<impl Iterator<Item = (&AllocationId, &AllocationInfo)> + '_> {
        self.alloc_timeline
            .get_allocations_for_queue(queue_id, time)
    }

    /// Gets the `WorkerConfiguration` for specified `worker_id` if the worker has ever connected.
    pub fn query_worker_info_for(&self, worker_id: &WorkerId) -> Option<&WorkerConfiguration> {
        self.worker_timeline.get_worker_info_for(worker_id)
    }

    /// Calculates the number of workers connected to the cluster at the specified `time`.
    pub fn query_connected_worker_ids(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = WorkerId> + '_ {
        self.worker_timeline.get_connected_worker_ids(time)
    }

    /// Gets the last `WorkerOverview` for a `worker_id` at a specific `time`
    pub fn query_worker_overview_at(
        &self,
        worker_id: WorkerId,
        time: SystemTime,
    ) -> Option<&WorkerOverview> {
        self.worker_timeline.get_worker_overview_at(worker_id, time)
    }

    /// Gets the last received worker overviews from all workers that have sent an overview.
    pub fn query_last_received_overviews(&self, time: SystemTime) -> Vec<&WorkerOverview> {
        self.worker_timeline.get_last_received_overviews(time)
    }
}

pub async fn create_data_fetch_process(
    refresh_interval: Duration,
    data: WrappedRcRefCell<DashboardData>,
    mut connection: ClientConnection,
) -> anyhow::Result<()> {
    let mut tick_duration = tokio::time::interval(refresh_interval);
    loop {
        let fetched_until = data.get().last_fetched_id();
        let events = fetch_events_after(&mut connection, fetched_until).await?;
        data.get_mut().update_data(events);
        tick_duration.tick().await;
    }
}

/// Gets the events from the server after the event_id specified
async fn fetch_events_after(
    connection: &mut ClientConnection,
    after_id: Option<u32>,
) -> crate::Result<Vec<MonitoringEvent>> {
    let response = rpc_call!(
        connection,
        FromClientMessage::MonitoringEvents (
            MonitoringEventRequest {
                after_id,
            }),
        ToClientMessage::MonitoringEventsResponse(response) => response
    )
    .await;
    response
}
