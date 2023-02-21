use crate::common::error::HqError;
use crate::{JobTaskCount, JobTaskId};
use bstr::BString;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

#[derive(Deserialize, Debug)]
pub struct TaskDef {
    pub id: Option<JobTaskId>,

    #[serde(default)]
    pub pin: PinMode,

    pub command: Vec<String>,

    #[serde(default)]
    pub env: crate::Map<BString, BString>,

    #[serde(default)]
    pub stdout: String,

    #[serde(default)]
    pub stderr: String,

    #[serde(default)]
    pub cwd: String,
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
