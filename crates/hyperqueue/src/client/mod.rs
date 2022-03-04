use std::path::PathBuf;

pub mod autoalloc;
pub mod commands;
pub mod globalsettings;
pub mod job;
pub mod output;
pub mod resources;
pub mod server;
pub mod status;
pub mod utils;

pub fn default_server_directory_path() -> PathBuf {
    let mut home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
    home.push(".hq-server");
    home
}
