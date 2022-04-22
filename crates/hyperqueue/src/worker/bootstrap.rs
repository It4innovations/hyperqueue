use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio::net::lookup_host;
use tokio::sync::Notify;
use tokio::task::LocalSet;

use tako::common::error::DsError;
use tako::messages::common::WorkerConfiguration;
use tako::worker::rpc::run_worker;
use tako::WorkerId;

use crate::common::manager::info::{ManagerInfo, ManagerType};
use crate::common::manager::{pbs, slurm};
use crate::common::serverdir::ServerDir;
use crate::worker::start::{HqTaskLauncher, WORKER_EXTRA_PROCESS_PID};
use crate::worker::streamer::StreamerRef;

pub type WorkerStopFlag = Arc<Notify>;

pub struct InitializedWorker {
    pub id: WorkerId,
    pub configuration: WorkerConfiguration,
    pub stop_flag: WorkerStopFlag,
}

pub async fn initialize_worker(
    server_directory: &Path,
    configuration: WorkerConfiguration,
) -> anyhow::Result<(impl Future<Output = Result<(), DsError>>, InitializedWorker)> {
    log::info!("Starting hyperqueue worker {}", env!("CARGO_PKG_VERSION"));
    let server_dir = ServerDir::open(server_directory).context("Cannot load server directory")?;
    let record = server_dir.read_access_record().with_context(|| {
        format!(
            "Cannot load access record from {:?}",
            server_dir.access_filename()
        )
    })?;
    let server_address = format!("{}:{}", record.host(), record.worker_port());
    log::info!("Connecting to: {}", server_address);

    std::fs::create_dir_all(&configuration.work_dir)?;
    std::fs::create_dir_all(&configuration.log_dir)?;

    let server_addr = lookup_host(&server_address)
        .await?
        .next()
        .expect("Invalid server address");

    log::debug!("Starting streamer ...");
    let (streamer_ref, streamer_future) = StreamerRef::start(
        Duration::from_secs(10),
        server_addr,
        record.tako_secret_key().clone(),
    );

    log::debug!("Starting Tako worker ...");
    let ((worker_id, configuration), worker_future) = run_worker(
        server_addr,
        configuration,
        Some(record.tako_secret_key().clone()),
        Box::new(HqTaskLauncher::new(streamer_ref)),
    )
    .await?;

    let stop_flag = Arc::new(Notify::new());
    let worker = InitializedWorker {
        id: worker_id,
        configuration,
        stop_flag: stop_flag.clone(),
    };

    let future = async move {
        let local_set = LocalSet::new();
        local_set
            .run_until(async move {
                tokio::select! {
                    res = worker_future => res,
                    () = streamer_future => { Ok(()) },
                    () = stop_flag.notified() => { Ok(()) }
                }
            })
            .await
    };
    Ok((future, worker))
}

/// Utility function that adds common data to an already created worker configuration.
pub fn finalize_configuration(conf: &mut WorkerConfiguration) {
    conf.extra.insert(
        WORKER_EXTRA_PROCESS_PID.to_string(),
        std::process::id().to_string(),
    );
}

pub fn try_get_pbs_info() -> anyhow::Result<ManagerInfo> {
    log::debug!("Detecting PBS environment");

    std::env::var("PBS_ENVIRONMENT")
        .map_err(|_| anyhow!("PBS_ENVIRONMENT not found. The process is not running under PBS"))?;

    let manager_job_id =
        std::env::var("PBS_JOBID").expect("PBS_JOBID not found in environment variables");

    let time_limit = match pbs::get_remaining_timelimit(&manager_job_id) {
        Ok(time_limit) => Some(time_limit),
        Err(error) => {
            log::warn!("Cannot get time-limit from PBS: {error:?}");
            None
        }
    };

    log::info!("PBS environment detected");

    Ok(ManagerInfo::new(
        ManagerType::Pbs,
        manager_job_id,
        time_limit,
    ))
}

pub fn try_get_slurm_info() -> anyhow::Result<ManagerInfo> {
    log::debug!("Detecting SLURM environment");

    let manager_job_id = std::env::var("SLURM_JOB_ID")
        .or_else(|_| std::env::var("SLURM_JOBID"))
        .map_err(|_| {
            anyhow!("SLURM_JOB_ID/SLURM_JOBID not found. The process is not running under SLURM")
        })?;

    let duration = slurm::get_remaining_timelimit(&manager_job_id)
        .expect("Could not get remaining time from scontrol");

    log::info!("SLURM environment detected");

    Ok(ManagerInfo::new(
        ManagerType::Slurm,
        manager_job_id,
        Some(duration),
    ))
}
