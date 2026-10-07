use std::path::PathBuf;

pub mod autoalloc;
pub mod commands;
pub mod globalsettings;
pub mod job;
pub mod output;
pub mod resources;
pub mod server;
pub mod status;
pub mod task;
pub mod utils;

pub fn default_server_directory_path() -> PathBuf {
    crate::common::serverdir::default_server_directory()
}
