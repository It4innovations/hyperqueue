use crate::server::job::JobStatus::Submitted;
use crate::messages::{JobInfo};
use crate::messages::JobState;
use crate::TaskId;
use crate::tako::common::ProgramDefinition;
use crate::tako::gateway::TaskInfo;

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

    pub fn make_job_info(&self, task_info: Option<&TaskInfo>) -> JobInfo {
        JobInfo {
            id: self.task_id,
            name: self.name.clone(),
            state:
                match self.status {
                    Submitted => {
                       match task_info {
                           Some(_) => JobState::Waiting,
                           None => JobState::Finished,
                       }
                    },
                    JobStatus::Finished => JobState::Finished,
                    JobStatus::Failed(_) => JobState::Failed,
                }

        }
    }
}

