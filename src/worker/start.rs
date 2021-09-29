use std::future::Future;
use std::io;

use std::pin::Pin;
use std::process::ExitStatus;
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context};
use clap::Clap;
use futures::TryFutureExt;

use tako::common::error::DsError;
use tako::messages::common::WorkerConfiguration;
use tako::messages::common::{ProgramDefinition, StdioDef};
use tako::worker::launcher::{command_from_definitions, pin_program};
use tako::worker::rpc::run_worker;
use tako::worker::task::TaskRef;
use tako::worker::taskenv::{StopReason, TaskResult};
use tako::InstanceId;
use tempdir::TempDir;
use tokio::io::AsyncReadExt;
use tokio::net::lookup_host;
use tokio::sync::oneshot;
use tokio::task::LocalSet;

use crate::client::globalsettings::GlobalSettings;
use crate::common::env::{HQ_CPUS, HQ_INSTANCE_ID, HQ_PIN};
use crate::common::error::error;
use crate::common::manager::info::{ManagerInfo, ManagerType, WORKER_EXTRA_MANAGER_KEY};
use crate::common::manager::pbs;
use crate::common::manager::pbs::PbsContext;
use crate::common::placeholders::replace_placeholders_worker;
use crate::common::serverdir::ServerDir;
use crate::common::timeutils::ArgDuration;
use crate::transfer::messages::TaskBody;
use crate::transfer::stream::ChannelId;
use crate::worker::hwdetect::detect_resource;
use crate::worker::parser::parse_cpu_definition;
use crate::worker::streamer::StreamSender;
use crate::worker::streamer::StreamerRef;
use crate::Map;
use crate::{JobId, JobTaskId};

const STDIO_BUFFER_SIZE: usize = 16 * 1024; // 16kB

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
                );
            }
        })
    }
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct WorkerStartOpts {
    /// How many cores should be allocated for the worker
    #[clap(long)]
    cpus: Option<String>,

    /// How often should the worker announce its existence to the server. (default: "8s")
    #[clap(long, default_value = "8s")]
    heartbeat: ArgDuration,

    /// Duration after which will an idle worker automatically stop
    #[clap(long)]
    idle_timeout: Option<ArgDuration>,

    /// Worker time limit. Worker exits after given time.
    #[clap(long)]
    time_limit: Option<ArgDuration>,

    /// What HPC job manager should be used by the worker.
    #[clap(long, default_value = "detect", possible_values = & ["detect", "slurm", "pbs", "none"])]
    manager: ManagerOpts,

    /// Overwrite worker hostname
    #[clap(long)]
    hostname: Option<String>,
}

async fn resend_stdio(
    job_id: JobId,
    job_task_id: JobTaskId,
    channel: ChannelId,
    stdio: Option<impl tokio::io::AsyncRead + Unpin>,
    stream: Rc<StreamSender>,
) -> tako::Result<()> {
    if let Some(mut stdio) = stdio {
        log::debug!("Starting stream {}/{}/{}", job_id, job_task_id, channel);
        loop {
            let mut buffer = vec![0; STDIO_BUFFER_SIZE];
            let size = stdio.read(&mut buffer[..]).await?;
            if size == 0 {
                break;
            };
            buffer.truncate(size);
            stream.send_data(channel, buffer).await?;
        }
    }
    Ok(())
}

fn create_directory_if_needed(file: &StdioDef) -> io::Result<()> {
    if let StdioDef::File(path) = file {
        if let Some(path) = path.parent() {
            std::fs::create_dir_all(path)?;
        }
    }
    Ok(())
}

async fn launcher_main(
    streamer_ref: StreamerRef,
    task_ref: TaskRef,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> tako::Result<TaskResult> {
    log::debug!(
        "Starting program launcher {} {:?} {:?}",
        task_ref.get().id,
        &task_ref.get().configuration.resources,
        task_ref.get().resource_allocation()
    );

    let (program, job_id, job_task_id, instance_id): (
        ProgramDefinition,
        JobId,
        JobTaskId,
        InstanceId,
    ) = {
        let task = task_ref.get();
        let body: TaskBody = tako::transfer::auth::deserialize(&task.configuration.body)?;
        let allocation = task
            .resource_allocation()
            .expect("Missing resource allocation for running task");
        let mut program = body.program;

        if body.pin {
            pin_program(&mut program, allocation);
            program.env.insert(HQ_PIN.into(), "1".into());
        }

        program
            .env
            .insert(HQ_CPUS.into(), allocation.comma_delimited_cpu_ids().into());

        program
            .env
            .insert(HQ_INSTANCE_ID.into(), task.instance_id.to_string().into());

        replace_placeholders_worker(&mut program);

        create_directory_if_needed(&program.stdout)?;
        create_directory_if_needed(&program.stderr)?;

        (program, body.job_id, body.task_id, task.instance_id)
    };

    run_task(
        streamer_ref,
        &program,
        job_id,
        job_task_id,
        instance_id,
        end_receiver,
    )
    .await
}

/// Zero-worker mode measures pure overhead of HyperQueue.
/// In this mode the task is not executed at all.
#[cfg(feature = "zero-worker")]
async fn run_task(
    _streamer_ref: StreamerRef,
    _program: &ProgramDefinition,
    _job_id: JobId,
    _job_task_id: JobTaskId,
    _instance_id: InstanceId,
    _end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> tako::Result<TaskResult> {
    Ok(TaskResult::Finished)
}

#[cfg(not(feature = "zero-worker"))]
async fn run_task(
    streamer_ref: StreamerRef,
    program: &ProgramDefinition,
    job_id: JobId,
    job_task_id: JobTaskId,
    instance_id: InstanceId,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> tako::Result<TaskResult> {
    let mut command = command_from_definitions(program)?;

    let status_to_result = |status: ExitStatus| {
        if !status.success() {
            let code = status.code().unwrap_or(-1);
            return tako::Result::Err(DsError::GenericError(format!(
                "Program terminated with exit code {}",
                code
            )));
        } else {
            Ok(TaskResult::Finished)
        }
    };

    if matches!(program.stdout, StdioDef::Pipe) || matches!(program.stderr, StdioDef::Pipe) {
        let streamer_error =
            |e: DsError| DsError::GenericError(format!("Streamer: {:?}", e.to_string()));
        let mut child = command.spawn()?;
        let (close_sender, close_responder) = oneshot::channel();
        let stream = Rc::new(streamer_ref.get_mut().get_stream(
            &streamer_ref,
            job_id,
            job_task_id,
            instance_id,
            close_sender,
        ));

        stream.send_stream_start().await.map_err(streamer_error)?;

        let stream2 = stream.clone();

        let main_fut = async move {
            let stdout = child.stdout.take();
            let stderr = child.stderr.take();
            let response = tokio::try_join!(
                child.wait().map_err(DsError::from),
                resend_stdio(job_id, job_task_id, 0, stdout, stream2.clone())
                    .map_err(streamer_error),
                resend_stdio(job_id, job_task_id, 1, stderr, stream2).map_err(streamer_error),
            );
            status_to_result(response?.0)
        };

        let guard_fut = async move {
            let result = tokio::select! {
                biased;
                    r = end_receiver => {
                        Ok(r.unwrap().into())
                    }
                    r = main_fut => r
            };
            result
        };

        futures::pin_mut!(guard_fut);

        match futures::future::select(guard_fut, close_responder).await {
            futures::future::Either::Left((result, close_responder)) => {
                log::debug!("Waiting for stream termination");
                stream.close().await.map_err(streamer_error)?;
                close_responder
                    .await
                    .map_err(|_| {
                        DsError::GenericError("Connection to stream failed while closing".into())
                    })?
                    .map_err(streamer_error)?;
                result
            }
            futures::future::Either::Right((result, _)) => Err(match result {
                Ok(_) => streamer_error(DsError::GenericError(
                    "Internal error: Stream closed without calling close()".into(),
                )),
                Err(_) => streamer_error(DsError::GenericError(
                    "Connection to stream server closed".into(),
                )),
            }),
        }
    } else {
        tokio::select! {
            biased;
                r = end_receiver => {
                    Ok(r.unwrap().into())
                }
                r = command.status() => status_to_result(r?)
        }
    }
}

fn launcher(
    streamer_ref: &StreamerRef,
    task_ref: &TaskRef,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> Pin<Box<dyn Future<Output = tako::Result<TaskResult>> + 'static>> {
    let task_ref = task_ref.clone();
    let streamer_ref = streamer_ref.clone();
    Box::pin(async move { launcher_main(streamer_ref, task_ref, end_receiver).await })
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

    let configuration = gather_configuration(opts)?;

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
        Box::new(move |task_ref, end_receiver| launcher(&streamer_ref, task_ref, end_receiver)),
    )
    .await?;

    gsettings
        .printer()
        .print_worker_info(worker_id, configuration);
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

fn try_get_pbs_info() -> anyhow::Result<ManagerInfo> {
    log::debug!("Detecting PBS environment");

    std::env::var("PBS_ENVIRONMENT")
        .map_err(|_| anyhow!("PBS_ENVIRONMENT not found. The process is not running under PBS"))?;

    let manager_job_id =
        std::env::var("PBS_JOBID").expect("PBS_JOBID not found in environment variables");

    let pbs_context = PbsContext::create()?;
    let time_limit = pbs::get_remaining_timelimit(&pbs_context, &manager_job_id)
        .expect("Could not get PBS timelimit");

    log::info!("PBS environment detected");

    Ok(ManagerInfo::new(
        ManagerType::Pbs,
        manager_job_id,
        time_limit,
    ))
}

fn try_get_slurm_info() -> anyhow::Result<ManagerInfo> {
    log::debug!("Detecting SLURM environment");

    let manager_job_id = std::env::var("SLURM_JOB_ID")
        .or_else(|_| std::env::var("SLURM_JOBID"))
        .map_err(|_| {
            anyhow!("SLURM_JOB_ID/SLURM_JOBID not found. The process is not running under SLURM")
        })?;

    log::info!("SLURM environment detected");

    // TODO: Get walltime info
    let duration = Duration::from_secs(1);
    Ok(ManagerInfo::new(
        ManagerType::Slurm,
        manager_job_id,
        duration,
    ))
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

fn gather_configuration(opts: WorkerStartOpts) -> anyhow::Result<WorkerConfiguration> {
    log::debug!("Gathering worker configuration information");

    let hostname = opts.hostname.unwrap_or_else(|| {
        gethostname::gethostname()
            .into_string()
            .expect("Invalid hostname")
    });

    let resources = opts
        .cpus
        .map(|cpus| parse_cpu_definition(&cpus))
        .unwrap_or_else(detect_resource)?;

    let (work_dir, log_dir) = {
        let tmpdir = TempDir::new("hq-worker").unwrap().into_path();
        (tmpdir.join("work"), tmpdir.join("logs"))
    };

    let manager_info = gather_manager_info(opts.manager)?;
    let mut extra = Map::new();
    if let Some(manager_info) = manager_info {
        extra.insert(
            WORKER_EXTRA_MANAGER_KEY.to_string(),
            serde_json::to_string(&manager_info)?,
        );
    }

    Ok(WorkerConfiguration {
        resources,
        listen_address: Default::default(), // Will be filled during init
        time_limit: opts.time_limit.map(|x| x.into()),
        hostname,
        work_dir,
        log_dir,
        heartbeat_interval: opts.heartbeat.into(),
        idle_timeout: opts.idle_timeout.map(|x| x.into()),
        hw_state_poll_interval: Some(Duration::from_millis(1000)),
        extra,
    })
}
