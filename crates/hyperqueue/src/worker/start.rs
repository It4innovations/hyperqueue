use std::ffi::OsStr;
use std::fmt::Write;
use std::io;
use std::io::ErrorKind;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::rc::Rc;
use std::time::Duration;

use anyhow::anyhow;
use bstr::{BStr, ByteSlice};
use futures::TryFutureExt;
use tempdir::TempDir;
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

use tako::common::error::DsError;
use tako::common::resources::{ResourceAllocation, ResourceDescriptor};
use tako::messages::common::WorkerConfiguration;
use tako::messages::common::{ProgramDefinition, StdioDef};
use tako::transfer::auth::serialize;
use tako::worker::launcher::{command_from_definitions, TaskLaunchData, TaskLauncher};
use tako::worker::state::WorkerState;
use tako::worker::task::Task;
use tako::worker::taskenv::{StopReason, TaskResult};
use tako::{InstanceId, TaskId};

use crate::client::commands::worker::{ManagerOpts, WorkerStartOpts};
use crate::common::env::{HQ_CPUS, HQ_INSTANCE_ID, HQ_PIN, HQ_SUBMIT_DIR};
use crate::common::manager::info::{ManagerInfo, ManagerType, WORKER_EXTRA_MANAGER_KEY};
use crate::common::manager::{pbs, slurm};
use crate::common::placeholders::{
    fill_placeholders_in_paths, CompletePlaceholderCtx, ResolvablePaths,
};
use crate::transfer::messages::TaskBody;
use crate::transfer::stream::ChannelId;
use crate::worker::hwdetect::{detect_cpus, detect_cpus_no_ht, detect_generic_resource};
use crate::worker::parser::CpuDefinition;
use crate::worker::streamer::StreamSender;
use crate::worker::streamer::StreamerRef;
use crate::Map;
use crate::{JobId, JobTaskId};
use serde::{Deserialize, Serialize};

/// Data created when a task is started on a worker.
/// It can be accessed through the state of a running task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningTaskContext {
    pub instance_id: InstanceId,
}

pub struct HqTaskLauncher {
    streamer_ref: StreamerRef,
}

impl HqTaskLauncher {
    pub fn new(streamer_ref: StreamerRef) -> Self {
        Self { streamer_ref }
    }
}

impl TaskLauncher for HqTaskLauncher {
    fn build_task(
        &self,
        state: &WorkerState,
        task_id: TaskId,
        stop_receiver: Receiver<StopReason>,
    ) -> tako::Result<TaskLaunchData> {
        let (program, job_id, job_task_id, instance_id): (
            ProgramDefinition,
            JobId,
            JobTaskId,
            InstanceId,
        ) = {
            let task = state.get_task(task_id);
            let allocation = task
                .resource_allocation()
                .expect("Missing resource allocation for running task");

            log::debug!(
                "Starting program launcher task_id={} res={:?} alloc={:?} body_len={}",
                task.id,
                &task.resources,
                allocation,
                task.body.len(),
            );

            let body: TaskBody = tako::transfer::auth::deserialize(&task.body)?;
            let mut program = body.program;

            if body.pin {
                pin_program(&mut program, allocation);
                program.env.insert(HQ_PIN.into(), "1".into());
            }

            insert_resources_into_env(state, task, allocation, &mut program);

            let submit_dir: PathBuf = program.env[<&BStr>::from(HQ_SUBMIT_DIR)].to_string().into();
            program
                .env
                .insert(HQ_INSTANCE_ID.into(), task.instance_id.to_string().into());

            let ctx = CompletePlaceholderCtx {
                job_id: body.job_id,
                task_id: body.task_id,
                instance_id: task.instance_id,
                submit_dir: &submit_dir,
            };
            let paths = ResolvablePaths::from_program_def(&mut program);
            fill_placeholders_in_paths(paths, ctx);

            create_directory_if_needed(&program.stdout)?;
            create_directory_if_needed(&program.stderr)?;

            (program, body.job_id, body.task_id, task.instance_id)
        };

        let context = RunningTaskContext { instance_id };
        let serialized_context = serialize(&context)?;

        let task_future = run_task(
            self.streamer_ref.clone(),
            program,
            job_id,
            job_task_id,
            instance_id,
            stop_receiver,
        );

        Ok(TaskLaunchData::new(
            Box::pin(task_future),
            serialized_context,
        ))
    }
}

pub const WORKER_EXTRA_PROCESS_PID: &str = "ProcessPid";

const STDIO_BUFFER_SIZE: usize = 16 * 1024; // 16kB

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

fn insert_resources_into_env(
    state: &WorkerState,
    task: &Task,
    allocation: &ResourceAllocation,
    program: &mut ProgramDefinition,
) {
    program
        .env
        .insert(HQ_CPUS.into(), allocation.comma_delimited_cpu_ids().into());

    let resource_map = state.get_resource_map();

    for rq in task.resources.generic_requests() {
        let resource_name = resource_map.get_name(rq.resource).unwrap();
        program.env.insert(
            format!("HQ_RESOURCE_REQUEST_{}", resource_name).into(),
            rq.amount.to_string().into(),
        );
    }

    for alloc in &allocation.generic_allocations {
        let resource_name = resource_map.get_name(alloc.resource).unwrap();
        if let Some(indices) = alloc.value.to_comma_delimited_list() {
            if resource_name == "gpus" {
                /* Extra hack for GPUS */
                program
                    .env
                    .insert("CUDA_VISIBLE_DEVICES".into(), indices.clone().into());
                program
                    .env
                    .insert("CUDA_DEVICE_ORDER".into(), "PCI_BUS_ID".into());
            }
            program.env.insert(
                format!("HQ_RESOURCE_INDICES_{}", resource_name).into(),
                indices.into(),
            );
        }
    }
}

fn pin_program(program: &mut ProgramDefinition, allocation: &ResourceAllocation) {
    program.args.insert(0, "taskset".into());
    program.args.insert(1, "-c".into());
    program
        .args
        .insert(2, allocation.comma_delimited_cpu_ids().into());
}

/// Zero-worker mode measures pure overhead of HyperQueue.
/// In this mode the task is not executed at all.
#[cfg(feature = "zero-worker")]
async fn run_task(
    _streamer_ref: StreamerRef,
    _program: ProgramDefinition,
    _job_id: JobId,
    _job_task_id: JobTaskId,
    _instance_id: InstanceId,
    _end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> tako::Result<TaskResult> {
    Ok(TaskResult::Finished)
}

/// Provide a more detailed error message when a process fails to be spawned.
fn map_spawn_error(error: std::io::Error, program: &ProgramDefinition) -> tako::Error {
    let context = match &error.kind() {
        ErrorKind::NotFound => {
            let file = &program.args[0];
            let mut msg = format!(
                "\nThe program that you have tried to execute (`{}`) was not found.",
                file
            );

            let path = Path::new(OsStr::from_bytes(file.as_bytes()));
            if !path.is_absolute() && !path.starts_with(".") {
                let possible_path = program.cwd.join(path);
                if possible_path.is_file() {
                    msg.write_fmt(format_args!(
                        "\nThe file {:?} exists, maybe you have meant `./{}` instead?",
                        possible_path, file
                    ))
                    .unwrap();
                }
            }

            msg
        }
        _ => "".to_string(),
    };
    let message = format!(
        "Cannot execute {:?}: {}{}",
        program
            .args
            .iter()
            .map(|arg| arg.to_str_lossy())
            .collect::<Vec<_>>()
            .join(" "),
        error,
        context
    );

    tako::Error::GenericError(message)
}

#[cfg(not(feature = "zero-worker"))]
async fn run_task(
    streamer_ref: StreamerRef,
    program: ProgramDefinition,
    job_id: JobId,
    job_task_id: JobTaskId,
    instance_id: InstanceId,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> tako::Result<TaskResult> {
    let mut command = command_from_definitions(&program)?;

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

    log::trace!("Running command {:?}", command);

    let mut child = command
        .spawn()
        .map_err(|error| map_spawn_error(error, &program))?;

    if matches!(program.stdout, StdioDef::Pipe) || matches!(program.stderr, StdioDef::Pipe) {
        let streamer_error =
            |e: DsError| DsError::GenericError(format!("Streamer: {:?}", e.to_string()));
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
                r = child.wait() => status_to_result(r?)
        }
    }
}

fn try_get_pbs_info() -> anyhow::Result<ManagerInfo> {
    log::debug!("Detecting PBS environment");

    std::env::var("PBS_ENVIRONMENT")
        .map_err(|_| anyhow!("PBS_ENVIRONMENT not found. The process is not running under PBS"))?;

    let manager_job_id =
        std::env::var("PBS_JOBID").expect("PBS_JOBID not found in environment variables");

    let time_limit =
        pbs::get_remaining_timelimit(&manager_job_id).expect("Could not get PBS timelimit");

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

    let duration = slurm::get_remaining_timelimit(&manager_job_id)
        .expect("Could not get remaining time from scontrol");

    log::info!("SLURM environment detected");

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

pub fn gather_configuration(opts: WorkerStartOpts) -> anyhow::Result<WorkerConfiguration> {
    log::debug!("Gathering worker configuration information");

    let hostname = opts.hostname.unwrap_or_else(|| {
        gethostname::gethostname()
            .into_string()
            .expect("Invalid hostname")
    });

    let cpus = match opts.cpus.unpack() {
        CpuDefinition::Detect => detect_cpus()?,
        CpuDefinition::DetectNoHyperThreading => detect_cpus_no_ht()?,
        CpuDefinition::Custom(cpus) => cpus,
    };

    let mut generic = if opts.no_detect_resources {
        Vec::new()
    } else {
        detect_generic_resource()?
    };
    for def in opts.resource {
        let descriptor = def.unpack();
        generic.retain(|desc| desc.name != descriptor.name);
        generic.push(descriptor)
    }

    let resources = ResourceDescriptor::new(cpus, generic);

    resources.validate()?;

    let (work_dir, log_dir) = {
        let tmpdir = TempDir::new("hq-worker").unwrap().into_path();
        (tmpdir.join("work"), tmpdir.join("logs"))
    };

    let manager_info = gather_manager_info(opts.manager)?;
    let mut extra = Map::new();

    if let Some(manager_info) = &manager_info {
        extra.insert(
            WORKER_EXTRA_MANAGER_KEY.to_string(),
            serde_json::to_string(&manager_info)?,
        );
    }

    extra.insert(
        WORKER_EXTRA_PROCESS_PID.to_string(),
        std::process::id().to_string(),
    );

    Ok(WorkerConfiguration {
        resources,
        listen_address: Default::default(), // Will be filled during init
        time_limit: opts
            .time_limit
            .map(|x| x.unpack())
            .or_else(|| manager_info.map(|m| m.time_limit)),
        hostname,
        work_dir,
        log_dir,
        on_server_lost: opts.on_server_lost.unpack(),
        heartbeat_interval: opts.heartbeat.unpack(),
        idle_timeout: opts.idle_timeout.map(|x| x.unpack()),
        send_overview_interval: Some(Duration::from_millis(1000)),
        extra,
    })
}
