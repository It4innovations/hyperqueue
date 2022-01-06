use crate::client::globalsettings::GlobalSettings;
use crate::common::error::error;
use crate::common::serverdir::ServerDir;
use crate::common::timeutils::ArgDuration;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    FromClientMessage, Selector, StopWorkerMessage, StopWorkerResponse, ToClientMessage,
    WorkerInfo, WorkerInfoRequest,
};
use crate::worker::parser::{ArgCpuDefinition, ArgGenericResourceDef};
use crate::worker::start;
use crate::worker::start::HqTaskLauncher;
use crate::worker::streamer::StreamerRef;
use crate::WorkerId;
use anyhow::Context;
use clap::Parser;
use std::str::FromStr;
use std::time::Duration;
use tako::worker::rpc::run_worker;
use tokio::net::lookup_host;
use tokio::task::LocalSet;

#[derive(Parser)]
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
                );
            }
        })
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
    #[clap(long, default_value = "detect", possible_values = &["detect", "slurm", "pbs", "none"])]
    pub manager: ManagerOpts,

    /// Overwrite worker hostname
    #[clap(long)]
    pub hostname: Option<String>,
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
    include_offline: bool,
) -> crate::Result<Vec<WorkerInfo>> {
    let mut msg = rpc_call!(
        connection,
        FromClientMessage::WorkerList,
        ToClientMessage::WorkerListResponse(r) => r
    )
    .await?;

    msg.workers.sort_unstable_by_key(|w| w.id);

    if !include_offline {
        msg.workers.retain(|w| w.ended.is_none());
    }

    Ok(msg.workers)
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
    selector: Selector,
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
