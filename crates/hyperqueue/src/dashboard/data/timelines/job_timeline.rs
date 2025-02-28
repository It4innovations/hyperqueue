use crate::server::event::Event;
use crate::server::event::payload::EventPayload;
use crate::transfer::messages::{JobDescription, JobSubmitDescription};
use crate::{JobId, JobTaskId, WorkerId};
use chrono::{DateTime, Utc};
use std::time::SystemTime;
use tako::Map;

pub struct DashboardJobInfo {
    pub job: JobDescription,
    pub submit_data: JobSubmitDescription,
    pub job_creation_time: SystemTime,
    pub completion_date: Option<DateTime<Utc>>,

    job_tasks_info: Map<JobTaskId, TaskInfo>,
}

pub struct TaskInfo {
    pub worker_id: WorkerId,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    task_end_state: Option<DashboardTaskState>,
}

#[derive(Default)]
pub struct JobTimeline {
    job_timeline: Map<JobId, DashboardJobInfo>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum DashboardTaskState {
    Running,
    Finished,
    Failed,
}

impl TaskInfo {
    pub fn set_end_time_and_status(&mut self, end_time: &SystemTime, status: DashboardTaskState) {
        self.end_time = Some(*end_time);
        self.task_end_state = Some(status);
    }

    /// Returns the state of the task at given time. Time must be after start_time of the task.
    pub fn get_task_state_at(&self, time: SystemTime) -> Option<DashboardTaskState> {
        match self.end_time {
            None => Some(DashboardTaskState::Running),
            Some(end_time) if end_time > time => Some(DashboardTaskState::Running),
            _ => self.task_end_state,
        }
    }
}

impl JobTimeline {
    /// Assumes that `events` are sorted by time.
    pub fn handle_new_events(&mut self, events: &[Event]) {
        for event in events {
            match &event.payload {
                EventPayload::Submit {
                    job_id,
                    closed_job: _,
                    serialized_desc,
                } => {
                    let submit = serialized_desc
                        .deserialize()
                        .expect("Invalid serialized submit");
                    self.job_timeline.insert(
                        *job_id,
                        DashboardJobInfo {
                            job: submit.job_desc,
                            submit_data: submit.submit_desc,
                            job_tasks_info: Default::default(),
                            job_creation_time: event.time.into(),
                            completion_date: None,
                        },
                    );
                }

                EventPayload::JobCompleted(job_id) => {
                    if let Some(job_info) = self.job_timeline.get_mut(job_id) {
                        job_info.completion_date = Some(event.time);
                    }
                }

                EventPayload::TaskStarted {
                    job_id,
                    task_id,
                    instance_id: _,
                    workers,
                } => {
                    if let Some(info) = self.job_timeline.get_mut(job_id) {
                        info.job_tasks_info.insert(
                            *task_id,
                            TaskInfo {
                                worker_id: workers[0],
                                start_time: event.time.into(),
                                end_time: None,
                                task_end_state: None,
                            },
                        );
                    }
                }
                EventPayload::TaskFinished { job_id, task_id } => {
                    update_task_status(
                        &mut self.job_timeline,
                        *job_id,
                        *task_id,
                        DashboardTaskState::Finished,
                        event.time,
                    );
                }
                EventPayload::TaskFailed {
                    job_id,
                    task_id,
                    error: _,
                } => {
                    update_task_status(
                        &mut self.job_timeline,
                        *job_id,
                        *task_id,
                        DashboardTaskState::Failed,
                        event.time,
                    );
                }
                _ => {}
            }
        }
    }

    pub fn get_job_task_history(
        &self,
        job_id: JobId,
        time: SystemTime,
    ) -> impl Iterator<Item = (JobTaskId, &TaskInfo)> + '_ {
        self.job_timeline
            .get(&job_id)
            .into_iter()
            .flat_map(|job| job.job_tasks_info.iter())
            .filter(move |(_, info)| info.start_time < time)
            .map(|(id, info)| (*id, info))
    }

    pub fn get_worker_task_history(
        &self,
        worker_id: WorkerId,
        at_time: SystemTime,
    ) -> impl Iterator<Item = (JobTaskId, &TaskInfo)> + '_ {
        self.get_jobs_created_before(at_time)
            .flat_map(|(_, info)| info.job_tasks_info.iter())
            .filter(move |(_, task_info)| {
                task_info.worker_id == worker_id && task_info.start_time <= at_time
            })
            .map(|(id, info)| (*id, info))
    }

    pub fn get_job_info_for_job(&self, job_id: JobId) -> Option<&DashboardJobInfo> {
        self.job_timeline.get(&job_id)
    }

    pub fn get_jobs_created_before(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (JobId, &DashboardJobInfo)> + '_ {
        self.job_timeline
            .iter()
            .filter(move |(_, info)| info.job_creation_time <= time)
            .map(|(id, info)| (*id, info))
    }
}

fn update_task_status(
    job_timeline: &mut Map<JobId, DashboardJobInfo>,
    job_id: JobId,
    task_id: JobTaskId,
    task_status: DashboardTaskState,
    at_time: DateTime<Utc>,
) {
    if let Some(job_info) = job_timeline.get_mut(&job_id) {
        if let Some(task_info) = job_info.job_tasks_info.get_mut(&task_id) {
            task_info.set_end_time_and_status(&at_time.into(), task_status);
        }
    }
}
