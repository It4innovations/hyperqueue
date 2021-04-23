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
use std::sync::Arc;

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

    pub fn access_filename(&self) -> PathBuf {
        self.path("access.json")
    }
}

fn serde_serialize_key<S: Serializer>(key: &Arc<SecretKey>, serializer: S) -> Result<S::Ok, S::Error> {
    let str = serialize_key(&key);
    serializer.serialize_str(&str)
}

fn serde_deserialize_key<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Arc<SecretKey>, D::Error> {
    let key: String = Deserialize::deserialize(deserializer)?;
    deserialize_key(&key)
        .map(Arc::new)
        .map_err(|e| D::Error::custom(format!("Could not load secret key {}", e)))
}

/// This data structure represents information required to connect to a running instance of
/// HyperQueue.
///
/// It is stored on disk during `hq start` and loaded by both client operations (stats, submit) and
/// HyperQueue workers in order to connect to the server instance.
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct AccessRecord {
    /// Version of HQ
    version: String,

    /// Hostname of the HyperQueue server
    hostname: String,

    /// Server process pid
    pid: u32,

    /// Port that you can connect to as a HyperQueue client
    server_port: u16,

    /// Port that you can connect to as a Tako worker
    worker_port: u16,

    start_date: DateTime<Utc>,

    #[serde(serialize_with = "serde_serialize_key")]
    #[serde(deserialize_with = "serde_deserialize_key")]
    hq_secret_key: Arc<SecretKey>,

    #[serde(serialize_with = "serde_serialize_key")]
    #[serde(deserialize_with = "serde_deserialize_key")]
    tako_secret_key: Arc<SecretKey>,
}

impl AccessRecord {
    pub fn new(
        hostname: String,
        server_port: u16,
        worker_port: u16,
        hq_secret_key: Arc<SecretKey>,
        tako_secret_key: Arc<SecretKey>,
    ) -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname,
            server_port,
            worker_port,
            start_date: Utc::now(),
            hq_secret_key,
            tako_secret_key,
            pid: std::process::id(),
        }
    }
    pub fn version(&self) -> &str { &self.version }
    pub fn hostname(&self) -> &str {
        &self.hostname
    }
    pub fn pid(&self) -> u32 {
        self.pid
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
    pub fn hq_secret_key(&self) -> &Arc<SecretKey> {
        &self.hq_secret_key
    }
    pub fn tako_secret_key(&self) -> &Arc<SecretKey> {
        &self.tako_secret_key
    }
}

pub fn store_access_record<P: AsRef<Path>>(record: &AccessRecord, path: P) -> crate::Result<()> {
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .mode(0o400); // Read for user, nothing for others

    let file = options.open(path)?;
    serde_json::to_writer_pretty(file, record)?;

    Ok(())
}

pub fn load_access_file<P: AsRef<Path>>(path: P) -> crate::Result<AccessRecord> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}


#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::common::rundir::{load_access_file, AccessRecord, store_access_record};

    #[test]
    fn test_roundtrip() {
        let record = AccessRecord::new(
            "foo".into(),
            42,
            43,
            Default::default(),
            Default::default(),
        );
        let path = TempDir::new("foo").unwrap().into_path().join("access.json");
        store_access_record(&record, path.clone()).unwrap();
        let loaded = load_access_file(path).unwrap();
        assert!(record == loaded);
    }
}
