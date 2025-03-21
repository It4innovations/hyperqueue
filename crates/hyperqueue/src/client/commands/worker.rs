use crate::client::commands::duration_doc;
use anyhow::{Context, bail};
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tako::resources::{
    CPU_RESOURCE_NAME, ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind,
};
use tako::worker::{ServerLostPolicy, WorkerConfiguration};
use tako::{Map, Set};

use clap::Parser;
use tako::hwstats::GpuFamily;
use tako::internal::worker::configuration::OverviewConfiguration;
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tokio::signal::ctrl_c;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::WorkerId;
use crate::client::globalsettings::GlobalSettings;
use crate::client::utils::{PassThroughArgument, passthrough_parser};
use crate::common::cli::DeploySshOpts;
use crate::common::manager::info::{ManagerInfo, WORKER_EXTRA_MANAGER_KEY};
use crate::common::utils::fs::get_hq_binary_path;
use crate::common::utils::network::get_hostname;
use crate::common::utils::time::parse_hms_or_human_time;
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
    GPU_ENVIRONMENTS, detect_additional_resources, detect_cpus, prune_hyper_threading,
};
use crate::worker::parser::{parse_cpu_definition, parse_resource_definition};
use crate::{DEFAULT_WORKER_GROUP_NAME, rpc_call};

// How often to send overview status to the server
const DEFAULT_OVERVIEW_INTERVAL: Duration = Duration::from_secs(5);

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
    #[arg(long, action = clap::ArgAction::Append, value_parser = passthrough_parser(parse_resource_definition)
    )]
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

    #[arg(
        long,
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("Duration after which will an idle worker automatically stop.")
    )]
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

    #[arg(
        long,
        default_value = "8s",
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("How often should the worker announce its existence to the server.")
    )]
    pub heartbeat: Duration,

    #[arg(
        long,
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("Worker time limit. Worker exits after given time.")
    )]
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
    let cpu_resources = resources.iter().find(|x| x.name == CPU_RESOURCE_NAME);
    let specified_cpu_resource = match cpu_resources {
        Some(item) => {
            if cpus.is_some() {
                bail!("Parameters --cpus and --resource cpus=... cannot be combined");
            }
            item.kind.clone()
        }
        None => {
            let kind = if let Some(cpus) = cpus {
                cpus.into_parsed_arg()
            } else {
                detect_cpus()?
            };
            resources.push(ResourceDescriptorItem {
                name: CPU_RESOURCE_NAME.to_string(),
                kind: kind.clone(),
            });
            kind
        }
    };

    if matches!(
        specified_cpu_resource,
        tako::resources::ResourceDescriptorKind::Sum { .. }
    ) {
        bail!("Resource kind `sum` cannot be used with CPUs. CPUs must have identity");
    }

    if no_hyper_threading {
        let cpus = resources
            .iter_mut()
            .find(|x| x.name == CPU_RESOURCE_NAME)
            .expect("No CPUs resource found");
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

    let work_dir = {
        let tmpdir = TempDir::with_prefix("hq-worker")?.into_path();
        work_dir.unwrap_or_else(|| tmpdir.join("work"))
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

pub async fn deploy_ssh_workers(opts: DeploySshOpts) -> anyhow::Result<()> {
    // We need to validate worker start options, but also get them as a list of arguments,
    // so that we can forward it to the SSH command.
    // We do that by using trailing var args, and then parse them manually as `WorkerStartOpts`
    // here.
    // We need to include a first argument so that `parse_from` works.
    // The name of the argument is chosen so that it renders well in `--help`.
    let mut fake_worker_args = vec!["hq worker deploy-ssh --".to_string()];
    fake_worker_args.extend(opts.worker_start_args.clone());

    // This actually crashes the program if the arguments are wrong.
    // It is not ideal, but if we returned Err, then the error message would not be as nice.
    let _worker_args = WorkerStartOpts::parse_from(fake_worker_args);

    let hostnames = load_hostnames(&opts.hostfile)?;
    if hostnames.is_empty() {
        return Err(anyhow::anyhow!("The provided hostname is empty"));
    }

    let ssh = get_ssh_binary()?;

    let binary = get_hq_binary_path()?;
    let mut args = vec![
        binary.to_str().unwrap().to_string(),
        "worker".to_string(),
        "start".to_string(),
    ];
    args.extend(opts.worker_start_args);

    let mut worker_futures = JoinSet::new();
    let mut worker_data = Map::new();

    // Start the workers on all nodes in the hostfile
    for hostname in hostnames {
        let mut child = start_ssh_worker_process(&ssh, &hostname, &args, opts.show_output)?;
        let handle = worker_futures.spawn(async move {
            let status = child.wait().await?;
            if !status.success() {
                Err(anyhow::anyhow!(
                    "Worker exited with code {}",
                    status.code().unwrap_or(-1)
                ))
            } else {
                Ok(())
            }
        });
        worker_data.insert(handle.id(), hostname);
    }

    let mut interrupt = std::pin::pin!(ctrl_c());
    let mut has_error = false;
    loop {
        tokio::select! {
            _ = &mut interrupt => {
                eprintln!("SIGINT received, closing workers");
                break;
            }
            result = worker_futures.join_next_with_id() => {
                let result = result.unwrap()?;
                let (id, result) = result;
                let hostname = &worker_data[&id];
                match result {
                    Ok(_) => {
                        log::info!("Worker at `{hostname}` has ended successfully");
                    }
                    Err(error) => {
                        log::error!("Worker at `{hostname}` has ended with an error: {error:?}");
                        has_error = true;
                    }
                }
                if worker_futures.is_empty() {
                    log::info!("All workers have finished");
                    break;
                }
            }
        }
    }
    worker_futures.shutdown().await;

    if has_error {
        Err(anyhow::anyhow!("Some workers have ended with an error"))
    } else {
        Ok(())
    }
}

fn start_ssh_worker_process(
    ssh: &Path,
    node: &str,
    args: &[String],
    show_output: bool,
) -> anyhow::Result<Child> {
    let mut cmd = Command::new(ssh);
    let cmd = cmd
        .arg(node)
        // double -t forces TTY allocation and propagates SIGINT to the remote process
        .arg("-t")
        .arg("-t")
        .arg("--")
        .args(args)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    if !show_output {
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
    }

    let child = cmd
        .spawn()
        .with_context(|| anyhow::anyhow!("Cannot start SSH command {cmd:?}"))?;
    log::info!("Started worker process at `{node}`");
    Ok(child)
}

fn load_hostnames(hostfile: &Path) -> anyhow::Result<Vec<String>> {
    let content = std::fs::read_to_string(hostfile)
        .with_context(|| anyhow::anyhow!("Cannot load hostfile from {}", hostfile.display()))?;
    Ok(content.lines().map(|s| s.to_string()).collect::<Vec<_>>())
}

fn get_ssh_binary() -> anyhow::Result<PathBuf> {
    which::which("ssh").context("Cannot find `ssh` binary. Make sure that OpenSSH is installed.")
}
