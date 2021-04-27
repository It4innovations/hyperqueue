use crate::server::job::JobStatus::Submitted;
use crate::transfer::messages::JobInfo;
use crate::transfer::messages::JobState;
use crate::TaskId;
use tako::messages::common::ProgramDefinition;

pub type JobId = TaskId;

pub enum JobStatus {
    Submitted,
    Finished,
    Failed(String),
}

pub struct Job {
    pub task_id: TaskId,
    pub status: JobStatus,
    pub name: String,
    pub program_def: ProgramDefinition,
}

impl Job {
    pub fn new(task_id: TaskId, name: String, program_def: ProgramDefinition) -> Self {
        Job {
            task_id,
            name,
            status: Submitted,
            program_def,
        }
    }

    pub fn make_job_info(&self, include_program_def: bool) -> JobInfo {
        let (state, error) = match &self.status {
            JobStatus::Submitted => (JobState::Waiting, None),
            JobStatus::Finished => (JobState::Finished, None),
            JobStatus::Failed(e) => (JobState::Failed, Some(e.clone())),
        };

        JobInfo {
            id: self.task_id,
            name: self.name.clone(),
            worker_id: None,
            state,
            error,
            spec: include_program_def.then(|| self.program_def.clone()),
        }
    }
}
