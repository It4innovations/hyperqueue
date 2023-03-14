use crate::internal::common::Map;
use bstr::BString;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub enum StdioDef {
    #[default]
    Null,
    File(PathBuf),
    Pipe,
}

impl StdioDef {
    pub fn map_filename<F>(self, f: F) -> StdioDef
    where
        F: FnOnce(PathBuf) -> PathBuf,
    {
        match self {
            StdioDef::Null => StdioDef::Null,
            StdioDef::File(filename) => StdioDef::File(f(filename)),
            StdioDef::Pipe => StdioDef::Pipe,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProgramDefinition {
    pub args: Vec<BString>,

    #[serde(default)]
    pub env: Map<BString, BString>,

    #[serde(default)]
    pub stdout: StdioDef,

    #[serde(default)]
    pub stderr: StdioDef,

    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub stdin: Vec<u8>,

    #[serde(default)]
    pub cwd: PathBuf,
}
