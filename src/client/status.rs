use crate::common::parser::{format_parse_error, NomResult};
use crate::server::job::JobTaskState;
use crate::transfer::messages::JobInfo;
use cli_table::{Cell, CellStruct, Color, Style};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::space0;
use nom::combinator::{all_consuming, map};
use nom::multi::separated_list1;
use nom::sequence::tuple;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;

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

pub fn task_status(status: &JobTaskState) -> Status {
    match status {
        JobTaskState::Waiting => Status::Waiting,
        JobTaskState::Running(..) => Status::Running,
        JobTaskState::Finished(..) => Status::Finished,
        JobTaskState::Failed(..) => Status::Failed,
        JobTaskState::Canceled => Status::Canceled,
    }
}

pub fn status_cell(status: Status) -> CellStruct {
    match status {
        Status::Waiting => "WAITING".cell().foreground_color(Some(Color::Cyan)),
        Status::Finished => "FINISHED".cell().foreground_color(Some(Color::Green)),
        Status::Failed => "FAILED".cell().foreground_color(Some(Color::Red)),
        Status::Running => "RUNNING".cell().foreground_color(Some(Color::Yellow)),
        Status::Canceled => "CANCELED".cell().foreground_color(Some(Color::Magenta)),
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

fn parse_status_list(input: &str) -> anyhow::Result<Vec<Status>> {
    all_consuming(p_status_list)(input)
        .map(|r| r.1)
        .map_err(format_parse_error)
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
