use crate::server::event::events::{JobInfo, MonitoringEventPayload};
use crate::server::event::log::EventLogReader;
use crate::server::job::Job;
use crate::server::state::State;
use crate::{JobId, Map};
use std::path::Path;
use tako::ItemId;

struct RestorerJob {
    job_info: JobInfo,
}

impl RestorerJob {
    pub fn restore(self, job_id: JobId, state: &mut State) {
        let task_count = self.job_info.job_desc.task_desc.task_count();
        let tako_base_id = state.new_task_id(task_count);
        let job = Job::new(self.job_info.job_desc, job_id, tako_base_id);
        todo!()
    }
}

impl RestorerJob {
    pub fn new(job_info: JobInfo) -> Self {
        RestorerJob { job_info }
    }
}

#[derive(Default)]
pub(crate) struct StateRestorer {
    jobs: Map<JobId, RestorerJob>,
    max_job_id: <JobId as ItemId>::IdType,
}

impl StateRestorer {
    pub fn job_id_counter(&self) -> <JobId as ItemId>::IdType {
        self.max_job_id + 1
    }

    pub fn restore_jobs(&self, state: &mut State) {}

    pub fn load_event_file(&mut self, path: &Path) -> crate::Result<()> {
        let event_reader = EventLogReader::open(path)?;
        for event in event_reader {
            let event = event?;
            match event.payload {
                MonitoringEventPayload::WorkerConnected(_, _) => {}
                MonitoringEventPayload::WorkerLost(_, _) => {}
                MonitoringEventPayload::WorkerOverviewReceived(_) => {}
                MonitoringEventPayload::JobCreated(job_id, job_info) => {
                    self.jobs.insert(job_id, RestorerJob::new(*job_info));
                    self.max_job_id = self.max_job_id.max(job_id.as_num());
                }
                MonitoringEventPayload::JobCompleted(job_id, _finish_time) => {
                    self.jobs.remove(&job_id);
                }
                MonitoringEventPayload::TaskStarted { .. } => {}
                MonitoringEventPayload::TaskFinished { .. } => {}
                MonitoringEventPayload::TaskFailed { .. } => {}
                MonitoringEventPayload::AllocationQueueCreated(_, _) => {}
                MonitoringEventPayload::AllocationQueueRemoved(_) => {}
                MonitoringEventPayload::AllocationQueued { .. } => {}
                MonitoringEventPayload::AllocationStarted(_, _) => {}
                MonitoringEventPayload::AllocationFinished(_, _) => {}
            }
        }
        Ok(())
    }
}
