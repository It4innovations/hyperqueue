use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::server::job::JobTaskState;
use crate::transfer::messages::JobInfo;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Status {
    Waiting,
    Running,
    Finished,
    Failed,
    Canceled,
}

impl FromStr for Status {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(match input {
            "waiting" => Self::Waiting,
            "running" => Self::Running,
            "finished" => Self::Finished,
            "failed" => Self::Failed,
            "canceled" => Self::Canceled,
            _ => {
                anyhow::bail!("Invalid job status, possible options: `waiting`, `running`, `finished`, `failed` or `canceled`.")
            }
        })
    }
}

pub fn job_status(info: &JobInfo) -> Status {
    let has_waiting = info.counters.n_waiting_tasks(info.n_tasks) > 0;

    if info.counters.n_running_tasks > 0 {
        Status::Running
    } else if has_waiting {
        Status::Waiting
    } else if info.counters.n_canceled_tasks > 0 {
        Status::Canceled
    } else if info.counters.n_failed_tasks > 0 {
        Status::Failed
    } else {
        assert_eq!(info.counters.n_finished_tasks, info.n_tasks);
        Status::Finished
    }
}

pub fn is_terminated(info: &JobInfo) -> bool {
    info.counters.n_running_tasks == 0 && info.counters.n_waiting_tasks(info.n_tasks) == 0
}

pub fn task_status(status: &JobTaskState) -> Status {
    match status {
        JobTaskState::Waiting => Status::Waiting,
        JobTaskState::Running { .. } => Status::Running,
        JobTaskState::Finished { .. } => Status::Finished,
        JobTaskState::Failed { .. } => Status::Failed,
        JobTaskState::Canceled { .. } => Status::Canceled,
    }
}
