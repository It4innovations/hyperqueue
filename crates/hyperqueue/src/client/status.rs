use std::str::FromStr;

use nom::branch::alt;
use nom::character::complete::space0;
use nom::combinator::map;
use nom::multi::separated_list1;
use nom::sequence::tuple;
use nom_supreme::tag::complete::tag;
use serde::Deserialize;
use serde::Serialize;

use crate::common::parser::{consume_all, NomResult};
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "waiting" => Self::Waiting,
            "running" => Self::Running,
            "finished" => Self::Finished,
            "failed" => Self::Failed,
            "canceled" => Self::Canceled,
            _ => anyhow::bail!("Invalid job status"),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StatusList(Vec<Status>);

impl StatusList {
    pub fn to_vec(self) -> Vec<Status> {
        self.0
    }
}

impl FromStr for StatusList {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_status_list(s).map(StatusList)
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

fn p_status(input: &str) -> NomResult<Status> {
    alt((
        map(tag("waiting"), |_| Status::Waiting),
        map(tag("running"), |_| Status::Running),
        map(tag("finished"), |_| Status::Finished),
        map(tag("failed"), |_| Status::Failed),
        map(tag("canceled"), |_| Status::Canceled),
    ))(input)
}

fn p_status_list(input: &str) -> NomResult<Vec<Status>> {
    separated_list1(tuple((tag(","), space0)), p_status)(input)
}

pub fn parse_status_list(input: &str) -> anyhow::Result<Vec<Status>> {
    consume_all(p_status_list, input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_parse_status_list() {
        assert_eq!(parse_status_list("waiting").unwrap(), vec![Status::Waiting]);
        assert_eq!(
            parse_status_list("canceled").unwrap(),
            vec![Status::Canceled]
        );
        assert_eq!(
            parse_status_list("running,failed,waiting").unwrap(),
            vec![Status::Running, Status::Failed, Status::Waiting]
        );
        assert_eq!(
            parse_status_list("running, failed, waiting").unwrap(),
            vec![Status::Running, Status::Failed, Status::Waiting]
        );
    }
}
