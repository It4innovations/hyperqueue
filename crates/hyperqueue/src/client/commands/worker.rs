use anyhow::bail;
use chrono::Utc;
use std::path::PathBuf;
use std::time::Duration;

use tako::resources::{
    ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, CPU_RESOURCE_NAME,
};
use tako::worker::{ServerLostPolicy, WorkerConfiguration};
use tako::{Map, Set};

use clap::Parser;
use tako::hwstats::GpuFamily;
use tako::internal::worker::configuration::OverviewConfiguration;
use tempdir::TempDir;
use tokio::time::sleep;

use crate::client::globalsettings::GlobalSettings;
use crate::client::utils::{passthrough_parser, PassThroughArgument};
use crate::common::manager::info::{ManagerInfo, WORKER_EXTRA_MANAGER_KEY};
use crate::common::utils::network::get_hostname;
use crate::common::utils::time::parse_human_time;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, StopWorkerMessage, StopWorkerResponse, ToClientMessage,
    WorkerInfo, WorkerInfoRequest,
};
use crate::worker::bootstrap::{
    finalize_configuration, initialize_worker, try_get_pbs_info, try_get_slurm_info,
};
use crate::worker::hwdetect::{
    detect_additional_resources, detect_cpus, prune_hyper_threading, GPU_ENVIRONMENTS,
};
use crate::worker::parser::{parse_cpu_definition, parse_resource_definition};
use crate::WorkerId;
use crate::{rpc_call, DEFAULT_WORKER_GROUP_NAME};

// How often to send overview status to the server
const DEFAULT_OVERVIEW_INTERVAL: Duration = Duration::from_secs(1);

#[derive(clap::ValueEnum, Clone)]
pub enum WorkerFilter {
    Running,
    Offline,
}

#[derive(clap::ValueEnum, Clone)]
pub enum ManagerOpts {
    Detect,
    None,
    Pbs,
    Slurm,
}

#[derive(clap::ValueEnum, Clone)]
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

// These options are shared by worker start and autoalloc.
#[derive(Parser, Debug)]
pub struct SharedWorkerStartOpts {
    /// How many cores should be allocated for the worker
    #[arg(long, value_parser = passthrough_parser(parse_cpu_definition))]
    pub cpus: Option<PassThroughArgument<ResourceDescriptorKind>>,

    /// Resources provided by the worker.
    ///
    /// Examples:{n}
    /// - `--resource gpus=[0,1,2,3]`{n}
    /// - `--resource "memory=sum(1024)"`
    #[arg(long, action = clap::ArgAction::Append, value_parser = passthrough_parser(parse_resource_definition))]
    pub resource: Vec<PassThroughArgument<ResourceDescriptorItem>>,

    /// Manual configuration of worker's group
    /// Workers from the same group are used for multi-node tasks
    #[arg(long)]
    pub group: Option<String>,

    #[clap(long)]
    /// Disable auto-detection of resources
    #[arg(long = "no-detect-resources")]
    pub no_detect_resources: bool,

    #[clap(long)]
    /// Ignore hyper-threading while detecting CPU cores
    #[arg(long = "no-hyper-threading")]
    pub no_hyper_threading: bool,

    /// Duration after which will an idle worker automatically stop
    #[arg(long, value_parser = parse_human_time)]
    pub idle_timeout: Option<Duration>,

    /// How often should the worker send its overview status (e.g. HW usage, task status)
    /// to the server for monitoring. Set to "0s" to disable overview updates.
    #[arg(long, value_parser = passthrough_parser(parse_human_time))]
    pub overview_interval: Option<PassThroughArgument<Duration>>,
}

#[derive(Parser)]
pub struct WorkerStartOpts {
    #[clap(flatten)]
    shared: SharedWorkerStartOpts,

    /// How often should the worker announce its existence to the server.
    #[arg(long, default_value = "8s", value_parser = parse_human_time)]
    pub heartbeat: Duration,

    /// Worker time limit. Worker exits after given time.
    #[arg(long, value_parser = parse_human_time)]
    pub time_limit: Option<Duration>,

    /// What HPC job manager should be used by the worker.
    #[arg(long, default_value_t = ManagerOpts::Detect, value_enum)]
    pub manager: ManagerOpts,

    /// Overwrite worker hostname
    #[arg(long)]
    pub hostname: Option<String>,

    /// Behavior when a connection to a server is lost
    #[arg(long, default_value_t = ArgServerLostPolicy::Stop, value_enum)]
    pub on_server_lost: ArgServerLostPolicy,

    /// Working directory of a worker. Temp directory by default.
    /// It should *NOT* be placed on a network filesystem.
    #[arg(long)]
    pub work_dir: Option<PathBuf>,
}

pub async fn start_hq_worker(
    gsettings: &GlobalSettings,
    opts: WorkerStartOpts,
) -> anyhow::Result<()> {
    let mut configuration = gather_configuration(opts)?;
    finalize_configuration(&mut configuration);

    let worker = initialize_worker(gsettings.server_directory(), configuration).await?;

    gsettings.printer().print_worker_info(WorkerInfo {
        id: worker.id,
        configuration: worker.configuration.clone(),
        started: Utc::now(),
        ended: None,
    });
    worker.run().await?;
    log::info!("Worker stopping");
    Ok(())
}

fn gather_configuration(opts: WorkerStartOpts) -> anyhow::Result<WorkerConfiguration> {
    log::debug!("Gathering worker configuration information");

    let WorkerStartOpts {
        shared:
            SharedWorkerStartOpts {
                cpus,
                resource,
                group,
                no_detect_resources,
                no_hyper_threading,
                idle_timeout,
                overview_interval,
            },
        heartbeat,
        time_limit,
        manager,
        hostname,
        on_server_lost,
        work_dir,
    } = opts;

    let hostname = get_hostname(hostname);

    let mut resources: Vec<_> = resource.into_iter().map(|x| x.into_parsed_arg()).collect();
    if !resources.iter().any(|x| x.name == CPU_RESOURCE_NAME) {
        resources.push(ResourceDescriptorItem {
            name: CPU_RESOURCE_NAME.to_string(),
            kind: if let Some(cpus) = cpus {
                cpus.into_parsed_arg()
            } else {
                detect_cpus()?
            },
        })
    } else if cpus.is_some() {
        bail!("Parameters --cpus and --resource cpus=... cannot be combined");
    }

    if no_hyper_threading {
        let cpus = resources
            .iter_mut()
            .find(|x| x.name == CPU_RESOURCE_NAME)
            .unwrap();
        cpus.kind = prune_hyper_threading(&cpus.kind)?;
    }

    let mut gpu_families = Set::new();
    if !no_detect_resources {
        gpu_families = detect_additional_resources(&mut resources)?;
    }
    for gpu_environment in GPU_ENVIRONMENTS {
        if resources
            .iter()
            .any(|r| r.name == gpu_environment.resource_name)
            && gpu_families.insert(gpu_environment.family)
        {
            log::info!(
                "Observing {} GPU usage because of manually passed resource `{}`",
                match gpu_environment.family {
                    GpuFamily::Nvidia => "nvidia",
                    GpuFamily::Amd => "amd",
                },
                gpu_environment.resource_name
            );
        }
    }

    let resources = ResourceDescriptor::new(resources);
    resources.validate()?;

    let (work_dir, log_dir) = {
        let tmpdir = TempDir::new("hq-worker").unwrap().into_path();
        (
            work_dir.unwrap_or_else(|| tmpdir.join("work")),
            tmpdir.join("logs"),
        )
    };

    let manager_info = gather_manager_info(manager)?;
    let mut extra = Map::new();

    if let Some(manager_info) = &manager_info {
        extra.insert(
            WORKER_EXTRA_MANAGER_KEY.to_string(),
            serde_json::to_string(&manager_info)?,
        );
    }

    let group = group.unwrap_or_else(|| {
        manager_info
            .as_ref()
            .map(|info| info.allocation_id.clone())
            .unwrap_or_else(|| DEFAULT_WORKER_GROUP_NAME.to_string())
    });

    let send_overview_interval = match overview_interval {
        Some(v) => {
            let duration = v.into_parsed_arg();
            if duration.is_zero() {
                None
            } else {
                Some(duration)
            }
        }
        None => Some(DEFAULT_OVERVIEW_INTERVAL),
    };
    let overview_configuration = send_overview_interval.map(|interval| OverviewConfiguration {
        send_interval: interval,
        gpu_families,
    });

    Ok(WorkerConfiguration {
        resources,
        listen_address: Default::default(), // Will be filled during init
        time_limit: time_limit.or_else(|| manager_info.and_then(|m| m.time_limit)),
        hostname,
        group,
        work_dir,
        log_dir,
        on_server_lost: on_server_lost.into(),
        heartbeat_interval: heartbeat,
        idle_timeout,
        overview_configuration,
        extra,
    })
}

fn gather_manager_info(opts: ManagerOpts) -> anyhow::Result<Option<ManagerInfo>> {
    match opts {
        ManagerOpts::Detect => {
            log::debug!("Trying to detect manager");
            Ok(try_get_pbs_info().or_else(|_| try_get_slurm_info()).ok())
        }
        ManagerOpts::None => {
            log::debug!("Manager detection disabled");
            Ok(None)
        }
        ManagerOpts::Pbs => Ok(Some(try_get_pbs_info()?)),
        ManagerOpts::Slurm => Ok(Some(try_get_slurm_info()?)),
    }
}

pub async fn get_worker_list(
    session: &mut ClientSession,
    filter: Option<WorkerFilter>,
) -> crate::Result<Vec<WorkerInfo>> {
    let msg = rpc_call!(
        session.connection(),
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
    session: &mut ClientSession,
    worker_id: WorkerId,
) -> crate::Result<Option<WorkerInfo>> {
    let msg = rpc_call!(
        session.connection(),
        FromClientMessage::WorkerInfo(WorkerInfoRequest {
            worker_id,
        }),
        ToClientMessage::WorkerInfoResponse(r) => r
    )
    .await?;

    Ok(msg)
}

pub async fn stop_worker(session: &mut ClientSession, selector: IdSelector) -> crate::Result<()> {
    let message = FromClientMessage::StopWorker(StopWorkerMessage { selector });
    let mut responses =
        rpc_call!(session.connection(), message, ToClientMessage::StopWorkerResponse(r) => r)
            .await?;

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

pub async fn wait_for_workers(
    session: &mut ClientSession,
    worker_count: u32,
) -> anyhow::Result<()> {
    async fn get_workers_status(session: &mut ClientSession) -> anyhow::Result<(u32, u32)> {
        let msg = rpc_call!(
            session.connection(),
            FromClientMessage::WorkerList,
            ToClientMessage::WorkerListResponse(r) => r
        )
        .await?;

        let mut online: u32 = 0;
        let mut offline: u32 = 0;
        for info in msg.workers {
            match info.ended {
                None => online += 1,
                Some(_) => offline += 1,
            }
        }
        Ok((online, offline))
    }

    loop {
        let (online, _) = get_workers_status(session).await?;
        if worker_count <= online {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
