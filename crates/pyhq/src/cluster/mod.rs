use crate::cluster::server::RunningServer;
use crate::cluster::worker::RunningWorker;
use anyhow::bail;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub mod server;
mod worker;

pub struct Cluster {
    server: Option<RunningServer>,
    workers: Vec<RunningWorker>,
    server_dir: PathBuf,
}

impl Cluster {
    pub fn start(server_dir: Option<PathBuf>) -> anyhow::Result<Self> {
        let server_dir =
            server_dir.unwrap_or_else(|| TempDir::with_prefix("hq").unwrap().into_path());
        let server = RunningServer::start(server_dir.clone())?;
        Ok(Self {
            server: Some(server),
            workers: Default::default(),
            server_dir,
        })
    }

    pub fn server_dir(&self) -> &Path {
        &self.server_dir
    }

    pub fn add_worker(&mut self, cores: usize) -> anyhow::Result<()> {
        if self.server.is_none() {
            bail!("Attempting to add worker to a stopped server");
        }

        let worker = RunningWorker::start(self.server_dir(), cores)?;
        self.workers.push(worker);
        Ok(())
    }

    pub fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(server) = self.server.take() {
            server.stop();
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Attempting to stop an already stopped server"
            ))
        }
    }
}
