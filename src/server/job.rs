use crate::server::job::JobStatus::Submitted;
use crate::transfer::messages::{JobInfo};
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
    pub spec: ProgramDefinition,
}

impl Job {
    pub fn new(task_id: TaskId, name: String, program_def: ProgramDefinition) -> Self {
        Job {
            task_id,
            name,
            status: Submitted,
            spec: program_def
        }
    }

    pub fn make_job_info(&self) -> JobInfo {
        JobInfo {
            id: self.task_id,
            name: self.name.clone(),
            state:
                match self.status {
                    JobStatus::Submitted => JobState::Waiting,
                    JobStatus::Finished => JobState::Finished,
                    JobStatus::Failed(_) => JobState::Failed,
                }

        }
    }
}
