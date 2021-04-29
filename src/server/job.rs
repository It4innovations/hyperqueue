use crate::server::job::JobState::Waiting;
use crate::transfer::messages::JobInfo;
use crate::transfer::messages::JobStatus;
use crate::TaskId;
use tako::messages::common::ProgramDefinition;

pub type JobId = TaskId;

pub enum JobState {
    Waiting,
    Running,
    Finished,
    Failed(String),
    Canceled,
}

pub struct Job {
    pub task_id: TaskId,
    pub state: JobState,
    pub name: String,
    pub program_def: ProgramDefinition,
}

impl Job {
    pub fn new(task_id: TaskId, name: String, program_def: ProgramDefinition) -> Self {
        Job {
            task_id,
            name,
            state: Waiting,
            program_def,
        }
    }

    pub fn make_job_info(&self, include_program_def: bool) -> JobInfo {
        let (state, error) = match &self.state {
            JobState::Waiting => (JobStatus::Waiting, None),
            JobState::Finished => (JobStatus::Finished, None),
            JobState::Failed(e) => (JobStatus::Failed, Some(e.clone())),
            JobState::Running => (JobStatus::Running, None),
            JobState::Canceled => (JobStatus::Canceled, None),
        };

        JobInfo {
            id: self.task_id,
            name: self.name.clone(),
            worker_id: None,
            status: state,
            error,
            spec: include_program_def.then(|| self.program_def.clone()),
        }
    }
}
