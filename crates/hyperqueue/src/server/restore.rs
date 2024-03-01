use crate::server::event::events::{JobInfo, MonitoringEventPayload};
use crate::server::event::log::EventLogReader;
use crate::server::job::Job;
use crate::server::state::State;
use crate::{JobId, JobTaskCount, Map};
use serde_json::ser::State::Rest;
use std::path::Path;
use tako::ItemId;

struct RestorerJob {
    job_info: JobInfo,
}

impl RestorerJob {
    pub fn restore(self, job_id: JobId, state: &mut State) {
        let tako_base_id = state.new_task_id(self.job_info.task_ids.len() as JobTaskCount);
        let job = Job::new(
            self.job_info.job_desc,
            job_id,
            tako_base_id,
            self.job_info.name,
            self.job_info.max_fails,
            self.job_info.log.clone(),
            submit_dir,
        );
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
                MonitoringEventPayload::TaskFinished(_) => {}
                MonitoringEventPayload::TaskFailed(_) => {}
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
