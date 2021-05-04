use std::str::FromStr;
use std::time::Duration;

use clap::Clap;
use tako::messages::common::WorkerConfiguration;
use tako::worker::rpc::run_worker;
use tempdir::TempDir;
use tokio::task::LocalSet;

use crate::client::globalsettings::GlobalSettings;
use crate::common::error::{error, HqError};
use crate::common::serverdir::ServerDir;
use crate::worker::output::print_worker_configuration;
use crate::Map;

#[derive(Clap)]
pub enum ManagerOpts {
    Detect,
    None,
    Pbs,
    Slurm,
}

impl FromStr for ManagerOpts {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "detect" => Self::Detect,
            "none" => Self::None,
            "pbs" => Self::Pbs,
            "slurm" => Self::Slurm,
            _ => {
                return error(
                    "Invalid manager value. Allowed values are 'detect', 'none', 'pbs', 'slurm'"
                        .to_string(),
                )
            }
        })
    }
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct WorkerStartOpts {
    /// How many cores should be allocated for the worker
    #[clap(long)]
    cpus: Option<u32>,

    /// How often should the worker announce its existence to the server. [ms]
    #[clap(long, default_value = "8000")]
    heartbeat: u32,

    /// What HPC job manager should be used by the worker.
    #[clap(long, default_value = "detect", possible_values = &["detect", "slurm", "pbs", "none"])]
    manager: ManagerOpts,
}

pub async fn start_hq_worker(
    gsettings: &GlobalSettings,
    opts: WorkerStartOpts,
) -> crate::Result<()> {
    log::info!("Starting hyperqueue worker {}", env!("CARGO_PKG_VERSION"));
    let server_dir = ServerDir::open(gsettings.server_directory())
        .map_err(|e| format!("Server directory error: {}", e))?;
    let record = server_dir
        .read_access_record()
        .map_err(|e| format!("Server is not running: {}", e))?;
    let server_address = format!("{}:{}", record.hostname(), record.worker_port());
    log::info!("Connecting to: {}", server_address);

    let configuration = gather_configuration(opts)?;
    let ((worker_id, configuration), worker_future) = run_worker(
        &server_address,
        configuration,
        Some(record.tako_secret_key().clone()),
    )
    .await?;
    print_worker_configuration(gsettings, worker_id, configuration);
    let local_set = LocalSet::new();
    local_set.run_until(worker_future).await;
    Ok(())
}

fn try_get_pbs_info() -> crate::Result<Map<String, String>> {
    log::debug!("Detecting PBS environment");

    std::env::var("PBS_ENVIRONMENT").map_err(|_| {
        HqError::GenericError(
            "PBS_ENVIRONMENT not found. The process is not running under PBS".to_string(),
        )
    })?;

    let manager_job_id = std::env::var("PBS_JOBID").unwrap_or_else(|_| "unknown".to_string());

    let mut result = Map::with_capacity(2);
    result.insert("MANAGER".to_string(), "PBS".to_string());
    result.insert("MANAGER_JOB_ID".to_string(), manager_job_id);

    // TODO: Run "qstat -f -F json $PBS_JOBID" to get walltime

    log::info!("PBS environment detected");
    Ok(result)
}

fn try_get_slurm_info() -> crate::Result<Map<String, String>> {
    log::debug!("Detecting SLURM environment");

    let manager_job_id = std::env::var("SLURM_JOB_ID")
        .or_else(|_| std::env::var("SLURM_JOBID"))
        .map_err(|_| {
            HqError::GenericError(
                "SLURM_JOB_ID/SLURM_JOBID not found. The process is not running under SLURM"
                    .to_string(),
            )
        })?;

    let mut result = Map::with_capacity(2);
    result.insert("MANAGER".to_string(), "SLURM".to_string());
    result.insert("MANAGER_JOB_ID".to_string(), manager_job_id);

    // TODO: Get walltime info

    log::info!("SLURM environment detected");
    Ok(result)
}

fn gather_manager_info(opts: ManagerOpts) -> crate::Result<Map<String, String>> {
    match opts {
        ManagerOpts::Detect => {
            log::debug!("Trying to detect manager");
            try_get_pbs_info()
                .or_else(|_| try_get_slurm_info())
                .or_else(|_| Ok(Map::new()))
        }
        ManagerOpts::None => {
            log::debug!("Manager detection disabled");
            Ok(Map::new())
        }
        ManagerOpts::Pbs => try_get_pbs_info(),
        ManagerOpts::Slurm => try_get_slurm_info(),
    }
}

fn gather_configuration(opts: WorkerStartOpts) -> crate::Result<WorkerConfiguration> {
    let hostname = gethostname::gethostname()
        .into_string()
        .expect("Invalid hostname");

    let n_cpus = opts.cpus.unwrap_or_else(|| num_cpus::get() as u32);
    if n_cpus < 1 {
        panic!("Invalid number of cpus");
    };

    let heartbeat_interval = Duration::from_millis(opts.heartbeat as u64);

    let (work_dir, log_dir) = {
        let tmpdir = TempDir::new("hq-worker").unwrap().into_path();
        (tmpdir.join("work"), tmpdir.join("logs"))
    };

    let extra = gather_manager_info(opts.manager)?;

    Ok(WorkerConfiguration {
        n_cpus,
        listen_address: Default::default(), // Will be filled during init
        hostname,
        work_dir,
        log_dir,
        heartbeat_interval,
        extra,
    })
}
