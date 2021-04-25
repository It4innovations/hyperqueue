use std::path::{PathBuf, Path};
use crate::server::bootstrap::{print_access_record};
use crate::common::serverdir::ServerDir;
use tako::messages::common::WorkerConfiguration;
use tako::worker::rpc::run_worker;
use tako::messages::worker::FromWorkerMessage::Heartbeat;
use std::time::Duration;
use clap::Clap;
use tempdir::TempDir;
use crate::worker::output::print_worker_configuration;


#[derive(Clap)]
pub struct WorkerStartOpts {

    #[clap(long)]
    ncpus: Option<u32>,

    #[clap(long, default_value = "8000")]
    heartbeat: u32,
}


pub async fn start_hq_worker(server_dir_path: &Path, opts: WorkerStartOpts) -> crate::Result<()> {
    log::info!("Starting hyperqueue worker {}", env!("CARGO_PKG_VERSION"));
    let server_dir = ServerDir::open(server_dir_path).map_err(|e| format!("Server directory error: {}", e))?;
    let record = server_dir.read_access_record().map_err(|e| format!("Server is not running: {}", e))?;
    let server_address = format!("{}:{}", record.hostname(), record.worker_port());
    log::info!("Connecting to: {}", server_address);

    let configuration = gather_configuration(opts);
    let ((worker_id, configuration), worker_future) = run_worker(&server_address, configuration, Some(record.tako_secret_key().clone())).await?;
    print_worker_configuration(worker_id, configuration);
    worker_future.await;
    Ok(())
}

fn gather_configuration(opts: WorkerStartOpts) -> WorkerConfiguration {
    let hostname = gethostname::gethostname()
            .into_string()
            .expect("Invalid hostname");

    let n_cpus = opts.ncpus.unwrap_or_else(|| num_cpus::get() as u32);
    if n_cpus < 1 {
        panic!("Invalid number of cpus");
    };

    let heartbeat_interval = Duration::from_millis(opts.heartbeat as u64);

    let (work_dir, log_dir) = {
        let tmpdir = TempDir::new("hq-worker").unwrap().into_path();
        (tmpdir.join("work"),tmpdir.join("logs"))
    };

    WorkerConfiguration {
        n_cpus,
        listen_address: Default::default(), // Will be filled during init
        hostname,
        work_dir,
        log_dir,
        heartbeat_interval,
        extra: vec![]
    }
}