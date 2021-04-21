use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use std::fs::{OpenOptions, File};
use std::os::unix::fs::OpenOptionsExt;
use crate::utils::absolute_path;
use crate::common::error::error;
use chrono::{DateTime, Utc};

#[derive(Clone)]
pub struct RunDirectory {
    path: PathBuf
}

impl RunDirectory {
    pub fn new(path: PathBuf) -> crate::Result<Self> {
        if !path.is_dir() {
            return error(format!("{:?} is not a directory", path));
        }

        Ok(Self { path: absolute_path(path) })
    }
    pub fn path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.path.join(path)
    }

    pub fn directory(&self) -> &PathBuf {
        &self.path
    }

    pub fn runfile(&self) -> PathBuf {
        self.path("runfile.json")
    }
}

/// This data structure represents information required to connect to a running instance of
/// HyperQueue.
///
/// It is stored on disk during `hq start` and loaded by both client operations (stats, submit) and
/// HyperQueue workers in order to connect to the server instance.
#[derive(Clone, Serialize, Deserialize)]
pub struct Runfile {
    /// Hostname of the HyperQueue server
    hostname: String,

    /// Port that you can connect to as a HyperQueue client
    server_port: u16,

    /// Port that you can connect to as a Tako worker
    worker_port: u16,

    start_date: DateTime<Utc>
    // TODO: add secret
}

impl Runfile {
    pub fn new(hostname: String, server_port: u16, worker_port: u16) -> Self {
        Self { hostname, server_port, worker_port, start_date: Utc::now() }
    }
    pub fn hostname(&self) -> &str {
        &self.hostname
    }
    pub fn server_port(&self) -> u16 {
        self.server_port
    }
    pub fn worker_port(&self) -> u16 {
        self.worker_port
    }
    pub fn start_date(&self) -> &DateTime<Utc> {
        &self.start_date
    }
}

pub fn store_runfile<P: AsRef<Path>>(runfile: &Runfile, path: P) -> crate::Result<()> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .mode(0o600); // Read/write for user, nothing for others

    let file = options.open(path)?;
    serde_json::to_writer_pretty(file, runfile)?;

    Ok(())
}

pub fn load_runfile<P: AsRef<Path>>(path: P) -> crate::Result<Runfile> {
    let file = File::open(path)?;
    let runfile = serde_json::from_reader(file)?;
    Ok(runfile)
}
