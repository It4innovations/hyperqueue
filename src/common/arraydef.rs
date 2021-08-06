use std::fmt;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::common::arrayparser::parse_array;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct IntRange {
    pub start: u32,
    pub count: u32,
    pub step: u32,
}

impl IntRange {
    pub fn new(start: u32, count: u32, step: u32) -> IntRange {
        IntRange { start, count, step }
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> {
        (self.start..self.start + self.count).step_by(self.step as usize)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IntArray {
    ranges: Vec<IntRange>,
}

impl IntArray {
    pub fn new(ranges: Vec<IntRange>) -> IntArray {
        IntArray { ranges }
    }

    pub fn from_ids(ids: Vec<u32>) -> IntArray {
        let mut ranges: Vec<IntRange> = Vec::new();
        for id in ids {
            if let Some(pos) = ranges.iter().position(|x| id == (x.start + x.count)) {
                ranges[pos].count += 1;
            } else {
                ranges.push(IntRange::new(id, 1, 1));
            }
        }
        IntArray { ranges }
    }

    pub fn from_range(start: u32, count: u32) -> Self {
        IntArray {
            ranges: vec![IntRange {
                start,
                count,
                step: 1,
            }],
        }
    }

    pub fn count_ids(&self) -> u32 {
        self.ranges.iter().map(|x| x.count).sum()
    }

    pub fn get_ids(&self) -> Vec<u32> {
        self.ranges.iter().flat_map(|x| x.iter()).collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.ranges.iter().flat_map(|x| x.iter())
    }
}

impl FromStr for IntArray {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_array(s)
    }
}

impl fmt::Display for IntArray {
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
