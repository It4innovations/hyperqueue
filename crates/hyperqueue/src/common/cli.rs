use crate::client::status::Status;
use crate::common::arraydef::IntArray;
use crate::transfer::messages::{
    IdSelector, SingleIdSelector, TaskIdSelector, TaskSelector, TaskStatusSelector,
};
use clap::Parser;
use std::str::FromStr;

pub enum IdSelectorArg {
    All,
    Last,
    Id(IntArray),
}

impl FromStr for IdSelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(IdSelectorArg::Last),
            "all" => Ok(IdSelectorArg::All),
            _ => Ok(IdSelectorArg::Id(IntArray::from_str(s)?)),
        }
    }
}

impl From<IdSelectorArg> for IdSelector {
    fn from(id_selector_arg: IdSelectorArg) -> Self {
        match id_selector_arg {
            IdSelectorArg::Id(array) => IdSelector::Specific(array),
            IdSelectorArg::Last => IdSelector::LastN(1),
            IdSelectorArg::All => IdSelector::All,
        }
    }
}

#[derive(Parser)]
pub struct TaskSelectorArg {
    /// Filter task(s) by ID.
    #[clap(long)]
    pub tasks: Option<IntArray>,

    /// Filter task(s) by status.
    /// You can use multiple states separated by a comma.
    #[clap(long, use_value_delimiter(true), arg_enum)]
    pub task_status: Vec<Status>,
}

pub fn get_task_selector(opt_task_selector_arg: Option<TaskSelectorArg>) -> Option<TaskSelector> {
    opt_task_selector_arg.map(|arg| TaskSelector {
        id_selector: get_task_id_selector(arg.tasks),
        status_selector: get_task_status_selector(arg.task_status),
    })
}

pub fn get_task_id_selector(id_selector_arg: Option<IntArray>) -> TaskIdSelector {
    id_selector_arg
        .map(TaskIdSelector::Specific)
        .unwrap_or(TaskIdSelector::All)
}

fn get_task_status_selector(status_selector_arg: Vec<Status>) -> TaskStatusSelector {
    if status_selector_arg.is_empty() {
        TaskStatusSelector::All
    } else {
        TaskStatusSelector::Specific(status_selector_arg)
    }
}

pub enum JobSelectorArg {
    Last,
    Id(IntArray),
}

impl FromStr for JobSelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(JobSelectorArg::Last),
            _ => Ok(JobSelectorArg::Id(IntArray::from_str(s)?)),
        }
    }
}

pub fn get_id_selector(job_selector_arg: JobSelectorArg) -> IdSelector {
    match job_selector_arg {
        JobSelectorArg::Last => IdSelector::LastN(1),
        JobSelectorArg::Id(ids) => IdSelector::Specific(ids),
    }
}

pub enum SingleIdSelectorArg {
    Last,
    Id(u32),
}

impl FromStr for SingleIdSelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(SingleIdSelectorArg::Last),
            _ => Ok(SingleIdSelectorArg::Id(u32::from_str(s)?)),
        }
    }
}

impl From<SingleIdSelectorArg> for SingleIdSelector {
    fn from(id_selector_arg: SingleIdSelectorArg) -> Self {
        match id_selector_arg {
            SingleIdSelectorArg::Last => SingleIdSelector::Last,
            SingleIdSelectorArg::Id(id) => SingleIdSelector::Specific(id),
        }
    }
}
