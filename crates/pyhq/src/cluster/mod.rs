use crate::cluster::server::RunningServer;
use std::path::{Path, PathBuf};
use tempdir::TempDir;

pub mod server;

pub struct Cluster {
    server: Option<RunningServer>,
    server_dir: PathBuf,
}

impl Cluster {
    pub fn start(server_dir: Option<PathBuf>) -> anyhow::Result<Self> {
        let server_dir = server_dir.unwrap_or_else(|| TempDir::new("hq").unwrap().into_path());
        let server = RunningServer::start(server_dir.clone())?;
        Ok(Self {
            server: Some(server),
            server_dir,
        })
    }

    pub fn server_dir(&self) -> &Path {
        &self.server_dir
    }

    pub fn stop(&mut self) {
        self.server
            .take()
            .expect("Attempting to stop an already stopped server")
            .stop();
    }
}
