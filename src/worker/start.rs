use std::path::{PathBuf, Path};
use crate::server::bootstrap::{print_access_record};
use crate::common::serverdir::ServerDir;
use tako::messages::common::WorkerConfiguration;

pub async fn start_hq_worker(server_dir_path: &Path) -> crate::Result<()> {
    log::info!("Starting hyperqueue worker {}", env!("CARGO_PKG_VERSION"));
    let server_dir = ServerDir::open(server_dir_path).map_err(|e| format!("Server directory error: {}", e))?;
    let record = server_dir.read_access_record().map_err(|e| format!("Server is not running: {}", e))?;
    println!("Connecting to server:");
    print_access_record(&server_dir_path, &record);
    Ok(())
}

pub fn gather_configuration() -> WorkerConfiguration {
    WorkerConfiguration {
        n_cpus: 0,
        listen_address: "".to_string(),
        hostname: "".to_string(),
        work_dir: Default::default(),
        log_dir: Default::default(),
        heartbeat_interval: Default::default(),
        extra: vec![]
    }
} 
