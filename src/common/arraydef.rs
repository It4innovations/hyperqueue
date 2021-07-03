use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::common::arrayparser::parse_array_def;
use crate::{JobTaskCount, JobTaskId, JobTaskStep};

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct TaskIdRange {
    pub start: JobTaskId,
    pub count: JobTaskCount,
    pub step: JobTaskStep,
}

impl TaskIdRange {
    pub fn new(start: JobTaskId, count: JobTaskCount, step: JobTaskStep) -> TaskIdRange {
        TaskIdRange { start, count, step }
    }

    pub fn iter(&self) -> impl Iterator<Item = JobTaskId> {
        (self.start..self.start + self.count).step_by(self.step as usize)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrayDef {
    ranges: Vec<TaskIdRange>,
}

impl ArrayDef {
    pub fn new(ranges: Vec<TaskIdRange>) -> ArrayDef {
        ArrayDef { ranges }
    }

    pub fn new_tasks(task_ids: Vec<JobTaskId>) -> ArrayDef {
        let mut ranges: Vec<TaskIdRange> = Vec::new();
        for task_id in task_ids {
            if let Some(pos) = ranges.iter().position(|x| task_id == (x.start + x.count)) {
                ranges[pos].count += 1;
            } else {
                ranges.push(TaskIdRange::new(task_id, 1, 1));
            }
        }
        ArrayDef { ranges }
    }

    pub fn simple_range(start: JobTaskId, count: JobTaskCount) -> Self {
        ArrayDef {
            ranges: vec![TaskIdRange {
                start,
                count,
                step: 1,
            }],
        }
    }

    pub fn task_count(&self) -> JobTaskCount {
        self.ranges.iter().map(|x| x.count).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = JobTaskId> + '_ {
        self.ranges.iter().flat_map(|x| x.iter())
    }
}

impl FromStr for ArrayDef {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_array_def(&s)
    }
}

impl fmt::Display for ArrayDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut str = String::new();
        for x in &self.ranges {
            if x.count == 1 {
                str.push_str(&*format!("{}, ", x.start));
            } else if x.step == 1 {
                str.push_str(&*format!("{}-{}, ", x.start, x.start + x.count - 1));
            } else {
                str.push_str(&*format!(
                    "{}-{}:{}, ",
                    x.start,
                    x.start + x.count - 1,
                    x.step
                ));
            }
        }
        write!(f, "{}", &str[0..str.len() - 2])
    }
}
