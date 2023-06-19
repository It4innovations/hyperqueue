use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use orion::kdf::SecretKey;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::common::error::error;
use crate::common::utils::fs::{absolute_path, create_symlink};
use crate::transfer::auth::{deserialize_key, serialize_key};
use crate::HQ_VERSION;

#[derive(Clone)]
pub struct ServerDir {
    path: PathBuf,
}

pub fn default_server_directory() -> PathBuf {
    let mut home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
    home.push(".hq-server");
    home
}

pub const SYMLINK_PATH: &str = "hq-current";
const ACCESS_FILE: &str = "access.json";

impl ServerDir {
    pub fn open(directory: &Path) -> crate::Result<Self> {
        let path = resolve_active_directory(directory);
        if !path.is_dir() {
            return error(format!("{path:?} is not a directory"));
        }
        Ok(Self {
            path: absolute_path(path),
        })
    }

    pub fn create(directory: &Path, record: &FullAccessRecord) -> crate::Result<ServerDir> {
        let dir_path = create_new_server_dir(directory)?;

        let server_dir = ServerDir::open(&dir_path)?;
        let access_file_path = server_dir.access_filename();
        store_access_record(record, access_file_path)?;

        let symlink = directory.join(SYMLINK_PATH);
        create_symlink(&symlink, &dir_path)?;
        Ok(server_dir)
    }

    pub fn path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.path.join(path)
    }

    pub fn directory(&self) -> &Path {
        &self.path
    }

    pub fn access_filename(&self) -> PathBuf {
        self.path(ACCESS_FILE)
    }

    pub fn read_worker_access_record(&self) -> crate::Result<WorkerAccessRecord> {
        let record = load_worker_access_file(self.access_filename())?;
        if record.version != HQ_VERSION {
            return error(format!(
                "Hyperqueue version mismatch detected.\nServer was started with version {}, \
                but the current version is {}.",
                record.version, HQ_VERSION
            ));
        }

        Ok(record)
    }

    pub fn read_client_access_record(&self) -> crate::Result<ClientAccessRecord> {
        let record = load_client_access_file(self.access_filename())?;
        if record.version != HQ_VERSION {
            return error(format!(
                "Hyperqueue version mismatch detected.\nServer was started with version {}, \
                but the current version is {}.",
                record.version, HQ_VERSION
            ));
        }

        Ok(record)
    }
}

/// Returns either `path` if it doesn't contain `SYMLINK_PATH` or the target of `SYMLINK_PATH`.
fn resolve_active_directory(path: &Path) -> PathBuf {
    let symlink_path = path.join(SYMLINK_PATH);
    std::fs::canonicalize(symlink_path)
        .ok()
        .filter(|p| p.is_dir())
        .unwrap_or_else(|| path.into())
}

fn serde_serialize_key<S: Serializer>(
    key: &Arc<SecretKey>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let str = serialize_key(key);
    serializer.serialize_str(&str)
}

fn serde_deserialize_key<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Arc<SecretKey>, D::Error> {
    let key: String = Deserialize::deserialize(deserializer)?;
    deserialize_key(&key)
        .map(Arc::new)
        .map_err(|e| D::Error::custom(format!("Could not load secret key {e}")))
}

/// Finds all child directories in the given directory.
/// For each directory, tries to parse its name as an integer.
/// Returns the maximum found successfully parsed integer or [`None`] if no integer was found.
fn find_max_id_in_dir(directory: &Path) -> Option<u64> {
    if let Ok(entries) = std::fs::read_dir(directory) {
        entries
            .filter_map(|entry| {
                entry.ok().and_then(|entry| {
                    match entry.metadata().ok().map(|m| m.is_dir()).unwrap_or(false) {
                        true => entry
                            .file_name()
                            .to_str()
                            .and_then(|f| f.parse::<u64>().ok()),
                        false => None,
                    }
                })
            })
            .max()
    } else {
        None
    }
}

fn create_new_server_dir(directory: &Path) -> crate::Result<PathBuf> {
    let max_id = find_max_id_in_dir(directory).unwrap_or(0);
    let new_id = max_id + 1;

    let dir_path = directory.join(format!("{new_id:03}"));
    std::fs::create_dir_all(&dir_path)?;
    Ok(dir_path)
}

/*
pub struct ServerDescription {
    pub record: FullAccessRecord,

    /// Server process pid
    pub pid: u32,

    pub start_date: DateTime<Utc>,
}*/

/// This data structure represents information required to connect to a running instance of
/// HyperQueue.
///
/// It is stored on disk during `hq start` and loaded by both client operations (stats, submit) and
/// HyperQueue workers in order to connect to the server instance.
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct FullAccessRecord {
    /// Version of HQ
    version: String,

    /// UID of the server instance
    #[serde(default)]
    server_uid: String,

    client: ConnectAccessRecordPart,

    worker: ConnectAccessRecordPart,
}

/// Subset of FullAccess record needed only by clients
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct ClientAccessRecord {
    /// Version of HQ
    pub version: String,

    pub client: ConnectAccessRecordPart,
}

/// Subset of FullAccess record needed only by workers
#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct WorkerAccessRecord {
    /// Version of HQ
    pub version: String,

    pub worker: ConnectAccessRecordPart,
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct ConnectAccessRecordPart {
    /// Hostname or IP address of the HyperQueue server
    pub host: String,

    pub port: u16,

    #[serde(serialize_with = "serde_serialize_key")]
    #[serde(deserialize_with = "serde_deserialize_key")]
    pub secret_key: Arc<SecretKey>,
}

impl FullAccessRecord {
    pub fn new(
        client: ConnectAccessRecordPart,
        worker: ConnectAccessRecordPart,
        server_uid: String,
    ) -> Self {
        Self {
            version: HQ_VERSION.to_string(),
            server_uid,
            client,
            worker,
        }
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn worker_host(&self) -> &str {
        &self.worker.host
    }
    pub fn client_host(&self) -> &str {
        &self.client.host
    }
    pub fn server_uid(&self) -> &str {
        &self.server_uid
    }
    pub fn client_port(&self) -> u16 {
        self.client.port
    }
    pub fn worker_port(&self) -> u16 {
        self.worker.port
    }
    pub fn client_key(&self) -> &Arc<SecretKey> {
        &self.client.secret_key
    }
    pub fn worker_key(&self) -> &Arc<SecretKey> {
        &self.worker.secret_key
    }

    pub fn split(self) -> (ClientAccessRecord, WorkerAccessRecord) {
        (
            ClientAccessRecord {
                version: self.version.clone(),
                client: self.client,
            },
            WorkerAccessRecord {
                version: self.version,
                worker: self.worker,
            },
        )
    }
}

pub fn load_access_record(path: &Path) -> crate::Result<FullAccessRecord> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

pub fn store_access_record<R: ?Sized + Serialize, P: AsRef<Path>>(
    record: &R,
    path: P,
) -> crate::Result<()> {
    log::info!("Storing access file as '{}'", path.as_ref().display());
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o400); // Read for user, nothing for others

    let file = options.open(path.as_ref())?;
    serde_json::to_writer_pretty(file, record)?;
    Ok(())
}

pub fn load_worker_access_file<P: AsRef<Path>>(path: P) -> crate::Result<WorkerAccessRecord> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

pub fn load_client_access_file<P: AsRef<Path>>(path: P) -> crate::Result<ClientAccessRecord> {
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

#[cfg(test)]
mod tests {
    use std::fs::DirEntry;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempdir::TempDir;

    use crate::common::serverdir::{
        create_new_server_dir, load_client_access_file, load_worker_access_file,
        store_access_record, ConnectAccessRecordPart, FullAccessRecord,
    };
    use crate::transfer::auth::generate_key;

    #[test]
    fn test_store_load() {
        let record = FullAccessRecord::new(
            ConnectAccessRecordPart {
                host: "foo".into(),
                port: 42,
                secret_key: Arc::new(generate_key()),
            },
            ConnectAccessRecordPart {
                host: "bar".into(),
                port: 42,
                secret_key: Arc::new(generate_key()),
            },
            "testHQ".into(),
        );
        let path = TempDir::new("foo").unwrap().into_path().join("access.json");
        store_access_record(&record, path.clone()).unwrap();
        let loaded = load_worker_access_file(&path).unwrap();
        assert_eq!(loaded.version, record.version);
        assert_eq!(loaded.worker.host, record.worker_host());
        assert_eq!(loaded.worker.port, record.worker_port());
        assert_eq!(&loaded.worker.secret_key, record.worker_key());

        let loaded = load_client_access_file(&path).unwrap();
        assert_eq!(loaded.version, record.version);
        assert_eq!(loaded.client.host, record.client_host());
        assert_eq!(loaded.client.port, record.client_port());
        assert_eq!(&loaded.client.secret_key, record.client_key());
    }

    #[test]
    fn test_server_dir_start_at_one() {
        let path = TempDir::new("foo").unwrap();
        create_new_server_dir(path.as_ref()).unwrap();
        let entry = std::fs::read_dir(&path).unwrap().next().unwrap().unwrap();
        assert_eq!(entry.file_name().to_str().unwrap(), "001");
    }

    #[test]
    fn test_server_dir_find_max_id() {
        let path = TempDir::new("foo").unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("001")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("002")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("003")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("004")).unwrap();

        let created = create_new_server_dir(path.as_ref()).unwrap();
        assert_eq!(created.file_name().unwrap().to_str().unwrap(), "005");

        let entry: Result<Vec<DirEntry>, _> = std::fs::read_dir(&path).unwrap().collect();
        assert!(entry
            .unwrap()
            .into_iter()
            .any(|p| p.file_name().to_str() == Some("005")));
    }

    #[test]
    fn test_server_dir_find_max_id_without_zero_padding() {
        let path = TempDir::new("foo").unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("1")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("3")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("8")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("136")).unwrap();

        let created = create_new_server_dir(path.as_ref()).unwrap();
        assert_eq!(created.file_name().unwrap().to_str().unwrap(), "137");
    }

    #[test]
    fn test_server_dir_find_max_id_no_number() {
        let path = TempDir::new("foo").unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("a")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("b")).unwrap();
        std::fs::create_dir_all(PathBuf::from(path.path()).join("c")).unwrap();

        let created = create_new_server_dir(path.as_ref()).unwrap();
        assert_eq!(created.file_name().unwrap().to_str().unwrap(), "001");
    }
}
