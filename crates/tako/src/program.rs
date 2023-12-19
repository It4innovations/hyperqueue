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

const MAX_SHORTENED_BSTRING: usize = 256;
const MAX_SHORTENED_ARGS: usize = 128;

impl ProgramDefinition {
    pub fn strip_large_data(&mut self) {
        self.stdin = Vec::new();
        if self.args.len() > MAX_SHORTENED_ARGS {
            self.args.truncate(MAX_SHORTENED_ARGS);
            self.args.shrink_to_fit();
        }
        for arg in &mut self.args {
            if arg.len() > MAX_SHORTENED_BSTRING {
                *arg = format!("<{} bytes>", arg.len()).into()
            }
        }
    }
}
