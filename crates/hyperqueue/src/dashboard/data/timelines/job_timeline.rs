use crate::server::event::payload::{EventPayload, JobInfo};
use crate::server::event::Event;
use crate::{JobId, JobTaskId, TakoTaskId, WorkerId};
use chrono::{DateTime, Utc};
use std::time::SystemTime;
use tako::Map;

pub struct DashboardJobInfo {
    pub job_info: JobInfo,
    pub job_tasks_info: Map<TakoTaskId, TaskInfo>,
    pub job_creation_time: SystemTime,

    pub completion_date: Option<DateTime<Utc>>,
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
                EventPayload::Submit(job_id, job_info) => {
                    self.job_timeline.insert(
                        *job_id,
                        DashboardJobInfo {
                            job_info: *job_info.clone(),
                            job_tasks_info: Default::default(),
                            job_creation_time: event.time,
                            completion_date: None,
                        },
                    );
                }

                EventPayload::JobCompleted(job_id, completion_date) => {
                    if let Some(job_info) = self.job_timeline.get_mut(job_id) {
                        job_info.completion_date = Some(*completion_date)
                    }
                }

                EventPayload::TaskStarted {
                    job_id,
                    task_id,
                    worker_id,
                } => {
                    todo!()
                    /*if let Some((_, info)) = self
                        .job_timeline
                        .iter_mut()
                        .find(|(_, info)| info.job_info.task_ids.contains(task_id))
                    {
                        info.job_tasks_info.insert(
                            *task_id,
                            TaskInfo {
                                worker_id: *worker_id,
                                start_time: event.time,
                                end_time: None,
                                task_end_state: None,
                            },
                        );
                    }*/
                }
                EventPayload::TaskFinished { job_id, task_id } => {
                    todo!()
                    /*update_task_status(
                        &mut self.job_timeline,
                        task_id,
                        DashboardTaskState::Finished,
                        &event.time,
                    );*/
                }
                EventPayload::TaskFailed { job_id, task_id } => {
                    todo!()
                    /*update_task_status(
                        &mut self.job_timeline,
                        task_id,
                        DashboardTaskState::Failed,
                        &event.time,
                    );*/
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
        // Implementation removed as it was wrong
        todo!();
        std::iter::empty()
    }

    pub fn get_worker_task_history(
        &self,
        worker_id: WorkerId,
        at_time: SystemTime,
    ) -> impl Iterator<Item = (JobTaskId, &TaskInfo)> + '_ {
        todo!();
        std::iter::empty()
        // self.get_jobs_created_before(at_time)
        //     .flat_map(move |(_, info)| {
        //         let base_id = info.job_info.base_task_id.as_num();
        //         info.job_tasks_info.iter().map(move |(task_id, info)| {
        //             (JobTaskId::new(task_id.as_num() - base_id + 1), info)
        //         })
        //     })
        //     .filter(move |(_, task_info)| {
        //         task_info.worker_id == worker_id && task_info.start_time <= at_time
        //     })
    }

    pub fn get_job_info_for_job(&self, job_id: JobId) -> Option<&DashboardJobInfo> {
        self.job_timeline.get(&job_id)
    }

    pub fn get_jobs_created_before(
        &self,
        time: SystemTime,
    ) -> impl Iterator<Item = (&JobId, &DashboardJobInfo)> + '_ {
        self.job_timeline
            .iter()
            .filter(move |(_, info)| info.job_creation_time <= time)
    }
}

fn update_task_status(
    job_timeline: &mut Map<JobId, DashboardJobInfo>,
    task_id: &TakoTaskId,
    task_status: DashboardTaskState,
    at_time: &SystemTime,
) {
    todo!()
    /*if let Some((_, job_info)) = job_timeline
        .iter_mut()
        .find(|(_, info)| info.job_info.task_ids.contains(task_id))
    {
        if let Some(task_info) = job_info.job_tasks_info.get_mut(task_id) {
            task_info.set_end_time_and_status(at_time, task_status);
        }
    };*/
}
