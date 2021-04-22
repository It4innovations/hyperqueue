use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use orion::kdf::SecretKey;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Error;

use crate::common::error::error;
use crate::transfer::auth::{deserialize_key, serialize_key};
use crate::utils::absolute_path;

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

fn serde_serialize_key<S: Serializer>(key: &Option<SecretKey>, serializer: S) -> Result<S::Ok, S::Error> {
    match key {
        Some(k) => {
            let str = serialize_key(&k);
            serializer.serialize_some(&str)
        }
        None => serializer.serialize_none()
    }
}

fn serde_deserialize_key<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<SecretKey>, D::Error> {
    let key: Option<String> = Deserialize::deserialize(deserializer)?;
    match key {
        Some(key) => {
            let key = deserialize_key(&key).map_err(|e| D::Error::custom(format!("Could not load secret key {}", e)))?;
            Ok(Some(key))
        }
        None => Ok(None)
    }
}

/// This data structure represents information required to connect to a running instance of
/// HyperQueue.
///
/// It is stored on disk during `hq start` and loaded by both client operations (stats, submit) and
/// HyperQueue workers in order to connect to the server instance.
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct Runfile {
    /// Hostname of the HyperQueue server
    hostname: String,

    /// Port that you can connect to as a HyperQueue client
    server_port: u16,

    /// Port that you can connect to as a Tako worker
    worker_port: u16,

    start_date: DateTime<Utc>,

    #[serde(serialize_with = "serde_serialize_key")]
    #[serde(deserialize_with = "serde_deserialize_key")]
    hq_secret_key: Option<SecretKey>,

    #[serde(serialize_with = "serde_serialize_key")]
    #[serde(deserialize_with = "serde_deserialize_key")]
    tako_secret_key: Option<SecretKey>,
}

impl Runfile {
    pub fn new(
        hostname: String,
        server_port: u16,
        worker_port: u16,
        hq_secret_key: Option<SecretKey>,
        tako_secret_key: Option<SecretKey>,
    ) -> Self {
        Self {
            hostname,
            server_port,
            worker_port,
            start_date: Utc::now(),
            hq_secret_key,
            tako_secret_key,
        }
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
    pub fn hq_secret_key(&self) -> &Option<SecretKey> {
        &self.hq_secret_key
    }
    pub fn tako_secret_key(&self) -> &Option<SecretKey> {
        &self.tako_secret_key
    }
}

pub fn store_runfile<P: AsRef<Path>>(runfile: &Runfile, path: P) -> crate::Result<()> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .mode(0o400); // Read for user, nothing for others

    let file = options.open(path)?;
    serde_json::to_writer_pretty(file, runfile)?;

    Ok(())
}

pub fn load_runfile<P: AsRef<Path>>(path: P) -> crate::Result<Runfile> {
    let file = File::open(path)?;
    let runfile = serde_json::from_reader(file)?;
    Ok(runfile)
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::common::rundir::{load_runfile, Runfile, store_runfile};

    #[test]
    fn test_roundtrip() {
        let runfile = Runfile::new(
            "foo".into(),
            42,
            43,
            Some(Default::default()),
            Some(Default::default()),
        );
        let path = TempDir::new("foo").unwrap().into_path().join("runfile.json");
        store_runfile(&runfile, path.clone()).unwrap();
        let loaded = load_runfile(path).unwrap();
        assert!(runfile == loaded);
    }
}
