use crate::common::error::HqError;
use crate::common::utils::time::parse_human_time;
use crate::{JobTaskCount, JobTaskId};
use bstr::BString;
use serde::de::{EnumAccess, Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Formatter, Write};
use std::path::PathBuf;
use std::time::Duration;
use tako::Priority;

#[derive(Deserialize, Debug)]
pub struct JobDef {
    #[serde(default)]
    pub name: String,

    pub max_fails: Option<JobTaskCount>,

    #[serde(rename = "task")]
    pub tasks: Vec<TaskDef>,

    pub stream_log: Option<PathBuf>,
}

#[derive(Default, Debug, Deserialize)]
pub enum PinMode {
    #[default]
    #[serde(rename = "none")]
    None,
    #[serde(rename = "taskset")]
    TaskSet,
    #[serde(rename = "omp")]
    OpenMP,
}

impl PinMode {
    pub fn into(&self) -> crate::transfer::messages::PinMode {
        match self {
            PinMode::None => crate::transfer::messages::PinMode::None,
            PinMode::TaskSet => crate::transfer::messages::PinMode::TaskSet,
            PinMode::OpenMP => crate::transfer::messages::PinMode::OpenMP,
        }
    }
}

fn deserialize_human_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = Option::<String>::deserialize(deserializer)?;

    if let Some(b) = buf {
        parse_human_time(&b)
            .map(Some)
            .map_err(serde::de::Error::custom)
    } else {
        Ok(None)
    }
}

#[derive(Deserialize, Debug)]
pub struct TaskDef {
    pub id: Option<JobTaskId>,

    pub command: Vec<String>,

    #[serde(default)]
    pub env: crate::Map<BString, BString>,

    #[serde(default)]
    pub stdout: String,

    #[serde(default)]
    pub stderr: String,

    #[serde(default)]
    pub cwd: String,

    #[serde(default)]
    pub pin: PinMode,

    #[serde(default)]
    pub task_dir: bool,

    #[serde(default)]
    #[serde(deserialize_with = "deserialize_human_duration")]
    pub time_limit: Option<Duration>,

    #[serde(default)]
    pub priority: Priority,

    #[serde(default)]
    pub crash_limit: u32,
}

impl JobDef {
    pub fn parse(str: &str) -> crate::Result<JobDef> {
        let jdef: JobDef = toml::from_str(str)?;

        if jdef.tasks.is_empty() {
            return Err(HqError::DeserializationError("No tasks defined".into()));
        }

        Ok(jdef)
    }
}

#[cfg(test)]
mod test {
    use crate::client::commands::submit::defs::{JobDef, TaskDef};
    use crate::common::error::HqError;

    #[test]
    fn test_read_minimal_def() {
        let jdef = JobDef::parse(
            r#"
        [[task]]
        command = ["sleep", "1"]
        "#,
        )
        .unwrap();
        assert!(jdef.name.is_empty());
        assert_eq!(jdef.tasks.len(), 1);
        assert!(jdef.tasks[0].id.is_none());
        assert_eq!(
            jdef.tasks[0].command,
            vec!["sleep".to_string(), "1".to_string()]
        );
    }

    #[test]
    fn test_validate_no_task_def() {
        let r = JobDef::parse(
            r#"
        name = 'myjob'
        "#,
        );
        assert!(matches!(r, Err(HqError::DeserializationError(_))))
        //assert!(r.is_err());
    }
}
