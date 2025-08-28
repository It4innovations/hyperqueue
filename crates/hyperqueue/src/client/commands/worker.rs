use crate::client::commands::duration_doc;
use anyhow::{Context, bail};
use chrono::Utc;
use clap::builder::{PossibleValue, TypedValueParser};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tako::Map;
use tako::resources::{
    CPU_RESOURCE_NAME, ResourceDescriptor, ResourceDescriptorCoupling, ResourceDescriptorItem,
    ResourceDescriptorKind,
};
use tako::worker::{ServerLostPolicy, WorkerConfiguration};

use clap::error::ErrorKind;
use clap::{Arg, Error, Parser, ValueEnum};
use tako::hwstats::GpuFamily;
use tako::internal::worker::configuration::OverviewConfiguration;
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tokio::signal::ctrl_c;
use tokio::task::JoinSet;
use tokio::time::sleep;

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
use crate::worker::parser::{
    parse_cpu_definition, parse_resource_coupling, parse_resource_definition,
};
use crate::{DEFAULT_WORKER_GROUP_NAME, rpc_call};
use tako::WorkerId;

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
    /// The cores assigned to the worker
    #[arg(long, value_parser = passthrough_parser(parse_cpu_definition))]
    pub cpus: Option<PassThroughArgument<ResourceDescriptorKind>>,

    /// Resources provided by the worker
    ///
    /// Examples:{n}
    /// - `--resource gpus=[0,1,2,3]`{n}
    /// - `--resource "memory=sum(1024)"`
    #[arg(long, action = clap::ArgAction::Append, value_parser = passthrough_parser(parse_resource_definition)
    )]
    pub resource: Vec<PassThroughArgument<ResourceDescriptorItem>>,

    /// Sets worker's group
    ///
    /// Workers from the same group are used for multi-node tasks.
    ///
    /// By default, the worker tries to detect allocation name from SLURM/PBS,
    /// if running outside a manager, then the group is empty.
    #[arg(long)]
    pub group: Option<String>,

    /// Resource coupling
    ///
    /// Examples:{n}
    /// - `--coupling cpus,gpus"
    #[arg(long, value_parser = passthrough_parser(parse_resource_coupling))]
    pub coupling: Option<PassThroughArgument<ResourceDescriptorCoupling>>,

    /// Determines which resources on the worker node will be automatically detected by HyperQueue.
    ///
    /// Except for `all` and `none`, you can combine multiple resources with
    /// a comma, e.g. `--detect-resources=cpus,mem,gpus/nvidia`.
    ///
    /// Note that if CPU auto-detection is turned off, you will need to provide `--cpus` explicitly.
    #[arg(long = "detect-resources", value_parser = passthrough_parser(DetectResourcesParser))]
    pub detect_resources: Option<PassThroughArgument<ResourceDetectComponents>>,

    /// Ignores hyper-threading while detecting CPU cores
    #[arg(long = "no-hyper-threading")]
    pub no_hyper_threading: bool,

    #[arg(
        long,
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("Duration after which will an idle worker automatically stop")
    )]
    pub idle_timeout: Option<Duration>,

    /// The period of reporting the overview to the server
    ///
    /// The overview contains information about HW usage.
    /// It serves only for the user's informing the user (e.g., in dashboard),
    /// the server scheduler is independent on worker's overviews.
    ///
    /// Set to "0s" to disable overview updates.
    /// By default, overview updates are disabled to reduce network bandwidth and event journal
    /// size.
    #[arg(long, value_parser = passthrough_parser(parse_human_time))]
    pub overview_interval: Option<PassThroughArgument<Duration>>,
}

/// Parses resource detection options (all, none or a comma-separated list of
/// [ResourceDetectComponent] values.
#[derive(Clone, Debug)]
pub struct DetectResourcesParser;

impl TypedValueParser for DetectResourcesParser {
    type Value = ResourceDetectComponents;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        _arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy();
        let value = value.as_ref();
        match value {
            "all" => Ok(ResourceDetectComponents::All),
            "none" => Ok(ResourceDetectComponents::None),
            values => {
                let mut parsed = HashSet::new();
                for value in values.split(",") {
                    match ResourceDetectComponent::from_str(value, false) {
                        Ok(component) => {
                            parsed.insert(component);
                        }
                        Err(error) => {
                            let error = Error::raw(ErrorKind::InvalidValue, error);
                            return Err(error.format(&mut cmd.clone()));
                        }
                    }
                }
                let mut parsed = parsed.into_iter().collect::<Vec<_>>();
                parsed.sort();
                Ok(ResourceDetectComponents::Selected(parsed))
            }
        }
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        let values = ResourceDetectComponent::value_variants()
            .iter()
            .filter_map(|v| v.to_possible_value());
        Some(Box::new(
            [
                PossibleValue::new("all").help("Detect all known resources"),
                PossibleValue::new("none").help("Do not detect any resources"),
            ]
            .into_iter()
            .chain(values),
        ))
    }
}

/// A single resource type that can be detected from the worker environment
#[derive(Debug, Clone, clap::ValueEnum, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceDetectComponent {
    /// Detect CPUs
    Cpus,
    /// Detect memory
    #[value(name = "mem")]
    Memory,
    /// Detect NVIDIA GPUs
    #[value(name = "gpus/nvidia")]
    GpusNvidia,
    /// Detect AMD ROC GPUs
    #[value(name = "gpus/amd")]
    GpusAmd,
}

#[derive(Clone, Debug, Default)]
pub enum ResourceDetectComponents {
    #[default]
    All,
    None,
    Selected(Vec<ResourceDetectComponent>),
}

impl ResourceDetectComponents {
    pub fn has(&self, component: ResourceDetectComponent) -> bool {
        match self {
            ResourceDetectComponents::All => true,
            ResourceDetectComponents::None => false,
            ResourceDetectComponents::Selected(selected) => selected.contains(&component),
        }
    }
}

#[derive(Parser)]
pub struct WorkerStartOpts {
    #[clap(flatten)]
    shared: SharedWorkerStartOpts,

    #[arg(
        long,
        default_value = "8s",
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("How often heartbeats are sent\n\nHeartbeats are used to detect worker's liveness. If the worker does not send a heartbeat for given time, then the worker is considered as lost.")
    )]
    pub heartbeat: Duration,

    #[arg(
        long,
        value_parser = parse_hms_or_human_time,
        help = duration_doc!("Worker time limit\n\nWorker exits after given time.")
    )]
    pub time_limit: Option<Duration>,

    /// Sets HPC job manager for the worker
    ///
    /// It modifies what variables are read from the environment
    #[arg(long, default_value_t = ManagerOpts::Detect, value_enum)]
    pub manager: ManagerOpts,

    /// Overwrites worker hostname
    #[arg(long)]
    pub hostname: Option<String>,

    /// The policy when a connection to a server is lost
    #[arg(long, default_value_t = ArgServerLostPolicy::Stop, value_enum)]
    pub on_server_lost: ArgServerLostPolicy,

    /// Sets the working directory for the worker
    ///
    /// It is a directory for internal usage of the worker
    /// and where temporal task directories are created.
    ///
    /// By default, it is placed in /tmp.
    /// It should *NOT* be placed on a network filesystem.
    #[arg(long)]
    pub work_dir: Option<PathBuf>,

    /// The maximal parallel downloads for data objects
    #[arg(long, default_value = "4")]
    pub max_parallel_downloads: u32,

    /// The maximal data object download tries
    ///
    /// Specifies how many times the worker tries to download a data object
    /// from the remote side before download is considered as failed.
    #[arg(long, default_value = "8")]
    pub max_download_tries: u32,

    #[arg(long,
          default_value = "1s", value_parser = parse_hms_or_human_time,
          help = duration_doc!("The delay between download attempts\n\nSets how long to wait between failed downloads of data object. This time is multiplied by the number of previous retries. Therefore between 4th and 5th retry it waits 4 * the given duration"),
          value_name = "TIME")
    ]
    pub wait_between_download_tries: Duration,
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
        runtime_info: None,
        last_task_started: None,
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
                coupling,
                group,
                detect_resources: detect_resources_cli,
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
        max_parallel_downloads,
        max_download_tries,
        wait_between_download_tries,
    } = opts;

    let detect_resources = detect_resources_cli
        .clone()
        .map(|p| p.into_parsed_arg())
        .unwrap_or_default();

    if max_download_tries == 0 {
        bail!("--max-download-tries cannot be zero");
    }

    if max_parallel_downloads == 0 {
        bail!("--max-parallel-downloads cannot be zero");
    }

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
            } else if detect_resources.has(ResourceDetectComponent::Cpus) {
                detect_cpus()?
            } else {
                return Err(anyhow::anyhow!(
                    "You have to specify --cpus explicitly when opting out of CPU automatic detection"
                ));
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

    let manager_info = gather_manager_info(manager)?;

    let mut gpu_families =
        detect_additional_resources(&mut resources, &detect_resources, manager_info.as_ref())?;
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
    let coupling: Option<_> = coupling.map(|x| x.into_parsed_arg());
    let resources = ResourceDescriptor::new(resources, coupling);
    resources.validate(true)?;

    let work_dir = {
        let tmpdir = TempDir::with_prefix("hq-worker")?.keep();
        work_dir.unwrap_or_else(|| tmpdir.join("work"))
    };

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

    let send_overview_interval = overview_interval.and_then(|v| {
        let duration = v.into_parsed_arg();
        if duration.is_zero() {
            None
        } else {
            Some(duration)
        }
    });
    let overview_configuration = OverviewConfiguration {
        send_interval: send_overview_interval,
        gpu_families,
    };

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
        max_parallel_downloads,
        max_download_tries,
        wait_between_download_tries,
    })
}

pub fn gather_manager_info(opts: ManagerOpts) -> anyhow::Result<Option<ManagerInfo>> {
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
    runtime_info: bool,
) -> crate::Result<Option<WorkerInfo>> {
    let msg = rpc_call!(
        session.connection(),
        FromClientMessage::WorkerInfo(WorkerInfoRequest {
            worker_id,
            runtime_info,
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
                log::error!("Stopping worker {id} failed; {e}");
            }
            StopWorkerResponse::InvalidWorker => {
                log::error!("Stopping worker {id} failed; worker not found");
            }
            StopWorkerResponse::AlreadyStopped => {
                log::warn!("Stopping worker {id} failed; worker is already stopped");
            }
            StopWorkerResponse::Stopped => {
                log::info!("Worker {id} stopped")
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
    hostname: &Hostname,
    args: &[String],
    show_output: bool,
) -> anyhow::Result<Child> {
    let mut cmd = Command::new(ssh);
    let cmd = cmd
        .arg(&hostname.host)
        // double -t forces TTY allocation and propagates SIGINT to the remote process
        .arg("-t")
        .arg("-t");
    if let Some(port) = hostname.port {
        cmd.arg("-p");
        cmd.arg(port.to_string());
    }

    cmd.arg("--")
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
    log::info!("Started worker process at `{hostname}`");
    Ok(child)
}

struct Hostname {
    host: String,
    port: Option<u16>,
}

impl Display for Hostname {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.host)?;
        if let Some(port) = self.port {
            write!(f, ":{port}")?;
        }
        Ok(())
    }
}

fn load_hostnames(hostfile: &Path) -> anyhow::Result<Vec<Hostname>> {
    let content = std::fs::read_to_string(hostfile)
        .with_context(|| anyhow::anyhow!("Cannot load hostfile from {}", hostfile.display()))?;
    content
        .lines()
        .map(|line| {
            let line = line.trim();
            if let Some((host, port)) = line.split_once(':') {
                let port = port
                    .parse::<u16>()
                    .map_err(|e| anyhow::anyhow!("Cannot parse port from {line}: {e:?}"))?;
                Ok(Hostname {
                    host: host.to_string(),
                    port: Some(port),
                })
            } else {
                Ok(Hostname {
                    host: line.to_string(),
                    port: None,
                })
            }
        })
        .collect()
}

fn get_ssh_binary() -> anyhow::Result<PathBuf> {
    which::which("ssh").context("Cannot find `ssh` binary. Make sure that OpenSSH is installed.")
}
