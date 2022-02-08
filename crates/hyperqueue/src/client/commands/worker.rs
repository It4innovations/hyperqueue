use crate::client::globalsettings::GlobalSettings;
use crate::common::serverdir::ServerDir;
use crate::common::timeutils::ArgDuration;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, StopWorkerMessage, StopWorkerResponse, ToClientMessage,
    WorkerInfo, WorkerInfoRequest,
};
use crate::worker::parser::{ArgCpuDefinition, ArgGenericResourceDef};
use crate::worker::start;
use crate::worker::start::HqTaskLauncher;
use crate::worker::streamer::StreamerRef;
use crate::WorkerId;
use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;
use tako::worker::rpc::run_worker;
use tako::worker::state::ServerLostPolicy;
use tokio::net::lookup_host;
use tokio::task::LocalSet;

#[derive(clap::ArgEnum, Clone)]
pub enum WorkerFilter {
    Running,
    Offline,
}

#[derive(clap::ArgEnum, Clone)]
pub enum ManagerOpts {
    Detect,
    None,
    Pbs,
    Slurm,
}

#[derive(clap::ArgEnum, Clone)]
pub enum ArgServerLostPolicy {
    Stop,
    FinishRunning,
}

impl From<ArgServerLostPolicy> for ServerLostPolicy {
    fn from(policy: ArgServerLostPolicy) -> Self {
        match policy {
            ArgServerLostPolicy::Stop => ServerLostPolicy::Stop,
            ArgServerLostPolicy::FinishRunning => ServerLostPolicy::FinishRunning,
        }
    }
}

#[derive(Parser)]
pub struct WorkerStartOpts {
    /// How many cores should be allocated for the worker
    #[clap(long, default_value = "auto")]
    pub cpus: ArgCpuDefinition,

    /// Resources
    #[clap(long, setting = clap::ArgSettings::MultipleOccurrences)]
    pub resource: Vec<ArgGenericResourceDef>,

    #[clap(long = "no-detect-resources")]
    /// Disable auto-detection of resources
    pub no_detect_resources: bool,

    /// How often should the worker announce its existence to the server. (default: "8s")
    #[clap(long, default_value = "8s")]
    pub heartbeat: ArgDuration,

    /// Duration after which will an idle worker automatically stop
    #[clap(long)]
    pub idle_timeout: Option<ArgDuration>,

    /// Worker time limit. Worker exits after given time.
    #[clap(long)]
    pub time_limit: Option<ArgDuration>,

    /// What HPC job manager should be used by the worker.
    #[clap(long, default_value = "detect", arg_enum)]
    pub manager: ManagerOpts,

    /// Overwrite worker hostname
    #[clap(long)]
    pub hostname: Option<String>,

    /// Behavior when a connection to a server is lost
    #[clap(long, default_value = "stop", arg_enum)]
    pub on_server_lost: ArgServerLostPolicy,

    /// Working directory of a worker. Temp directory by default.
    /// It should *NOT* be placed on a network filesystem.
    #[clap(long)]
    pub work_dir: Option<PathBuf>,
}

pub async fn start_hq_worker(
    gsettings: &GlobalSettings,
    opts: WorkerStartOpts,
) -> anyhow::Result<()> {
    log::info!("Starting hyperqueue worker {}", env!("CARGO_PKG_VERSION"));
    let server_dir =
        ServerDir::open(gsettings.server_directory()).context("Cannot load server directory")?;
    let record = server_dir.read_access_record().with_context(|| {
        format!(
            "Cannot load access record from {:?}",
            server_dir.access_filename()
        )
    })?;
    let server_address = format!("{}:{}", record.host(), record.worker_port());
    log::info!("Connecting to: {}", server_address);

    let configuration = start::gather_configuration(opts)?;

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

    gsettings.printer().print_worker_info(WorkerInfo {
        id: worker_id,
        configuration,
        ended: None,
    });
    let local_set = LocalSet::new();
    local_set
        .run_until(async move {
            tokio::select! {
                () = worker_future => {}
                () = streamer_future => {}
            }
        })
        .await;
    Ok(())
}

pub async fn get_worker_list(
    connection: &mut ClientConnection,
    filter: Option<WorkerFilter>,
) -> crate::Result<Vec<WorkerInfo>> {
    let msg = rpc_call!(
        connection,
        FromClientMessage::WorkerList,
        ToClientMessage::WorkerListResponse(r) => r
    )
    .await?;

    let mut workers = msg.workers;

    if let Some(filter) = filter {
        match filter {
            WorkerFilter::Running => workers.retain(|w| w.ended.is_none()),
            WorkerFilter::Offline => workers.retain(|w| w.ended.is_some()),
        };
    }

    workers.sort_unstable_by_key(|w| w.id);
    Ok(workers)
}

pub async fn get_worker_info(
    connection: &mut ClientConnection,
    worker_id: WorkerId,
) -> crate::Result<Option<WorkerInfo>> {
    let msg = rpc_call!(
        connection,
        FromClientMessage::WorkerInfo(WorkerInfoRequest {
            worker_id,
        }),
        ToClientMessage::WorkerInfoResponse(r) => r
    )
    .await?;

    Ok(msg)
}

pub async fn stop_worker(
    connection: &mut ClientConnection,
    selector: IdSelector,
) -> crate::Result<()> {
    let message = FromClientMessage::StopWorker(StopWorkerMessage { selector });
    let mut responses =
        rpc_call!(connection, message, ToClientMessage::StopWorkerResponse(r) => r).await?;

    responses.sort_unstable_by_key(|x| x.0);
    for (id, response) in responses {
        match response {
            StopWorkerResponse::Failed(e) => {
                log::error!("Stopping worker {} failed; {}", id, e.to_string());
            }
            StopWorkerResponse::InvalidWorker => {
                log::error!("Stopping worker {} failed; worker not found", id);
            }
            StopWorkerResponse::AlreadyStopped => {
                log::warn!("Stopping worker {} failed; worker is already stopped", id);
            }
            StopWorkerResponse::Stopped => {
                log::info!("Worker {} stopped", id)
            }
        }
    }

    Ok(())
}
