use crate::{JobTaskId, JobTaskCount};
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;

use crate::common::arrayparser::parse_array_def;
use std::fmt;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskIdRange {
    start: JobTaskId,
    count: JobTaskCount,
}

impl TaskIdRange {
    pub fn new(start: JobTaskId, count: JobTaskCount) -> TaskIdRange {
        TaskIdRange {
            start, count
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArrayDef {
    range: TaskIdRange,
}

impl ArrayDef {

    pub fn new(range: TaskIdRange) -> ArrayDef {
        ArrayDef { range }
    }
    
    #[cfg(test)]
    pub fn simple_range(start: JobTaskId, count: JobTaskCount) -> Self {
        ArrayDef {
            range: TaskIdRange { start, count }
        }
    }
    
    pub fn task_count(&self) -> JobTaskCount {
        self.range.count
    }

    pub fn iter(&self) -> impl Iterator<Item=JobTaskId> {
        self.range.start..self.range.start + self.range.count
    }
}

impl FromStr for ArrayDef {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_array_def(s)
    }
}

impl fmt::Display for ArrayDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.range.count == 1 {
            write!(f, "{}", self.range.start)
        } else {
            write!(f, "{}-{}", self.range.start, self.range.start + self.range.count - 1)
        }
    }
}