use crate::internal::common::Map;
use bstr::BString;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// What should happen with a file, once its owning task finishes executing?
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum FileOnCloseBehavior {
    /// Don't do anything
    #[default]
    None,
    /// Remove the file if its task has finished successfully
    RmIfFinished,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub enum StdioDef {
    #[default]
    Null,
    File {
        path: PathBuf,
        on_close: FileOnCloseBehavior,
    },
    Pipe,
}

impl StdioDef {
    pub fn map_filename<F>(self, f: F) -> StdioDef
    where
        F: FnOnce(PathBuf) -> PathBuf,
    {
        match self {
            StdioDef::Null => StdioDef::Null,
            StdioDef::File { path, on_close } => StdioDef::File {
                path: f(path),
                on_close,
            },
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
