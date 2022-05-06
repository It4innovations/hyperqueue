use std::fs::File;
use std::io;
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::rc::Rc;

use bstr::{BStr, BString, ByteSlice};
use futures::TryFutureExt;
use tempdir::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

use tako::launcher::{
    command_from_definitions, LaunchContext, StopReason, TaskLaunchData, TaskLauncher, TaskResult,
};
use tako::InstanceId;

use crate::common::env::{
    HQ_CPUS, HQ_ERROR_FILENAME, HQ_INSTANCE_ID, HQ_NODE_FILE, HQ_PIN, HQ_SUBMIT_DIR, HQ_TASK_DIR,
};
use crate::common::placeholders::{
    fill_placeholders_in_paths, CompletePlaceholderCtx, ResolvablePaths,
};
use crate::common::utils::fs::{bytes_to_path, is_implicit_path, path_has_extension};
use crate::transfer::messages::{PinMode, TaskBody};
use crate::transfer::stream::ChannelId;
use crate::worker::streamer::StreamSender;
use crate::worker::streamer::StreamerRef;
use crate::{JobId, JobTaskId};
use serde::{Deserialize, Serialize};
use tako::comm::serialize;
use tako::program::{ProgramDefinition, StdioDef};
use tako::resources::ResourceAllocation;

const MAX_CUSTOM_ERROR_LENGTH: usize = 2048; // 2KiB

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
        launch_ctx: LaunchContext,
        stop_receiver: Receiver<StopReason>,
    ) -> tako::Result<TaskLaunchData> {
        let (mut program, job_id, job_task_id, instance_id, task_dir): (
            ProgramDefinition,
            JobId,
            JobTaskId,
            InstanceId,
            Option<TempDir>,
        ) = {
            log::debug!(
                "Starting program launcher task_id={} res={:?} alloc={:?} body_len={}",
                launch_ctx.task_id(),
                launch_ctx.resources(),
                launch_ctx.allocation(),
                launch_ctx.body().len(),
            );

            let body: TaskBody = tako::comm::deserialize(launch_ctx.body())?;
            let TaskBody {
                mut program,
                pin: pin_mode,
                task_dir,
                job_id,
                task_id,
            } = body;

            pin_program(&mut program, launch_ctx.allocation(), pin_mode);

            let task_dir = if task_dir {
                let task_dir = TempDir::new_in(&launch_ctx.worker_configuration().work_dir, "t")?;
                program.env.insert(
                    HQ_TASK_DIR.into(),
                    task_dir.path().to_string_lossy().to_string().into(),
                );
                program.env.insert(
                    HQ_ERROR_FILENAME.into(),
                    get_custom_error_filename(&task_dir)
                        .to_string_lossy()
                        .to_string()
                        .into(),
                );
                if !launch_ctx.node_list().is_empty() {
                    let filename = task_dir.path().join("hq-nodelist");
                    write_node_file(&launch_ctx, &filename)?;
                    program.env.insert(
                        HQ_NODE_FILE.into(),
                        filename.to_string_lossy().to_string().into(),
                    );
                }
                Some(task_dir)
            } else {
                None
            };

            // Do not insert resources for multi-node tasks, semantics has to be cleared
            if launch_ctx.node_list().is_empty() {
                insert_resources_into_env(&launch_ctx, &mut program);
            }

            let submit_dir: PathBuf = program.env[<&BStr>::from(HQ_SUBMIT_DIR)].to_string().into();
            program.env.insert(
                HQ_INSTANCE_ID.into(),
                launch_ctx.instance_id().to_string().into(),
            );

            let ctx = CompletePlaceholderCtx {
                job_id,
                task_id,
                instance_id: launch_ctx.instance_id(),
                submit_dir: &submit_dir,
            };
            let paths = ResolvablePaths::from_program_def(&mut program);
            fill_placeholders_in_paths(paths, ctx);

            create_directory_if_needed(&program.stdout)?;
            create_directory_if_needed(&program.stderr)?;

            (program, job_id, task_id, launch_ctx.instance_id(), task_dir)
        };

        let context = RunningTaskContext { instance_id };
        let serialized_context = serialize(&context)?;

        try_add_interpreter(&mut program);

        let task_future = run_task(
            self.streamer_ref.clone(),
            program,
            job_id,
            job_task_id,
            instance_id,
            stop_receiver,
            task_dir,
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

fn get_custom_error_filename(task_dir: &TempDir) -> PathBuf {
    task_dir.path().join("hq-error")
}

fn write_node_file(ctx: &LaunchContext, path: &Path) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut file = BufWriter::new(file);
    for worker_id in ctx.node_list() {
        file.write_all(ctx.worker_hostname(*worker_id).unwrap().as_bytes())?;
        file.write_all(b"\n")?;
    }
    file.flush()?;
    Ok(())
}

fn insert_resources_into_env(ctx: &LaunchContext, program: &mut ProgramDefinition) {
    let allocation = ctx.allocation();
    program
        .env
        .insert(HQ_CPUS.into(), allocation.comma_delimited_cpu_ids().into());

    if !program.env.contains_key(b"OMP_NUM_THREADS".as_bstr()) {
        program.env.insert(
            "OMP_NUM_THREADS".into(),
            allocation.cpus.len().to_string().into(),
        );
    }

    let resource_map = ctx.get_resource_map();

    for rq in ctx.resources().generic_requests() {
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

fn pin_program(
    program: &mut ProgramDefinition,
    allocation: &ResourceAllocation,
    pin_mode: PinMode,
) {
    match pin_mode {
        PinMode::TaskSet => {
            let mut args: Vec<BString> = vec![
                "taskset".into(),
                "-c".into(),
                allocation.comma_delimited_cpu_ids().into(),
            ];
            args.append(&mut program.args);

            program.args = args;
            program.env.insert(HQ_PIN.into(), "taskset".into());
        }
        PinMode::OpenMP => {
            // OMP_PLACES specifies on which cores should the OpenMP threads execute.
            // OMP_PROC_BIND makes sure that OpenMP will actually pin its threads
            // to the specified places.
            if !program.env.contains_key(b"OMP_PROC_BIND".as_bstr()) {
                program.env.insert("OMP_PROC_BIND".into(), "close".into());
            }
            if !program.env.contains_key(b"OMP_PLACES".as_bstr()) {
                let places = allocation.comma_delimited_cpu_ids();
                program
                    .env
                    .insert("OMP_PLACES".into(), format!("{{{places}}}").into());
            }
            program.env.insert(HQ_PIN.into(), "omp".into());
        }
        PinMode::None => {}
    }
}

#[allow(clippy::unused_io_amount)]
fn read_interpreter(path: &Path) -> io::Result<BString> {
    let mut file = File::open(path)?;
    let mut buffer = [0; 256];
    file.read(&mut buffer)?;

    let bytes: &BStr = buffer.as_ref().into();
    if let Some(line) = bytes.lines().next() {
        let line = line.trim();
        if line.starts_with(b"#!") {
            return Ok(BString::from(&line[2..]));
        }
    }
    Err(io::ErrorKind::NotFound.into())
}

fn looks_like_bash_script(path: &Path) -> bool {
    path_has_extension(path, "sh")
}

/// If the program's first argument is a shell script, this function will try to parse a shebang
/// from its first line.
///
/// If the parsing is successful, the parsed interpreter path will be prepended to the commands of
/// the program definition.
fn try_add_interpreter(program: &mut ProgramDefinition) {
    let command = &program.args[0];
    let path = bytes_to_path(command.as_ref());
    if looks_like_bash_script(path) {
        if let Ok(interpreter) = read_interpreter(path) {
            log::debug!("Adding interpreter {interpreter:?} to command {command:?}");
            program.args.insert(0, interpreter);
        }
    }
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
    _task_dir: Option<TempDir>,
) -> tako::Result<TaskResult> {
    Ok(TaskResult::Finished)
}

/// Provide a more detailed error message when a process fails to be spawned.
fn map_spawn_error(error: std::io::Error, program: &ProgramDefinition) -> tako::Error {
    use std::fmt::Write;
    let context = match &error.kind() {
        ErrorKind::NotFound => {
            let file = &program.args[0];
            let mut msg = format!(
                "\nThe program that you have tried to execute (`{}`) was not found.",
                file
            );

            let path = bytes_to_path(file.as_ref());
            if is_implicit_path(path) {
                let possible_path = program.cwd.join(path);
                if possible_path.is_file() {
                    msg.write_fmt(format_args!(
                        "\nThe file `{}` exists, maybe you have meant `./{}` instead?",
                        possible_path.display(),
                        path.display()
                    ))
                    .unwrap();
                }
            }

            msg
        }
        ErrorKind::PermissionDenied => {
            let file = bytes_to_path(program.args[0].as_ref());
            if looks_like_bash_script(file) {
                format!(
                    "\nThe script that you have tried to execute (`{}`) is not executable.
Try making it executable or add a shebang line to it.",
                    file.display()
                )
            } else {
                "".to_string()
            }
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

async fn write_stdin(mut stdin: tokio::process::ChildStdin, stdin_data: &[u8]) {
    log::debug!("Writing {} bytes on task stdin", stdin_data.len());
    if let Err(e) = stdin.write_all(stdin_data).await {
        log::debug!("Writing stdin data failed: {}", e);
    }
    drop(stdin);
    futures::future::pending::<()>().await;
}

async fn child_wait(
    mut child: tokio::process::Child,
    stdin_data: &[u8],
) -> Result<ExitStatus, std::io::Error> {
    if let Some(stdin) = child.stdin.take() {
        let r = tokio::select! {
            () = write_stdin(stdin, stdin_data) => { unreachable!() }
            r = child.wait() => r
        };
        Ok(r?)
    } else {
        Ok(child.wait().await?)
    }
}

#[inline(never)]
fn check_error_filename(task_dir: TempDir) -> Option<tako::Error> {
    let mut f = File::open(&get_custom_error_filename(&task_dir)).ok()?;
    let mut buffer = [0; MAX_CUSTOM_ERROR_LENGTH];
    let size = f
        .read(&mut buffer)
        .map_err(|e| log::debug!("Reading error file failed: {}", e))
        .ok()?;
    let msg = String::from_utf8_lossy(&buffer[..size]);
    Some(if size == 0 {
        tako::Error::GenericError("Task created an error file, but it is empty".to_string())
    } else if size == MAX_CUSTOM_ERROR_LENGTH {
        tako::Error::GenericError(format!("{}\n[The message was truncated]", msg))
    } else {
        tako::Error::GenericError(msg.to_string())
    })
}

#[cfg(not(feature = "zero-worker"))]
async fn run_task(
    streamer_ref: StreamerRef,
    program: ProgramDefinition,
    job_id: JobId,
    job_task_id: JobTaskId,
    instance_id: InstanceId,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
    task_dir: Option<TempDir>,
) -> tako::Result<TaskResult> {
    let mut command = command_from_definitions(&program)?;

    let status_to_result = |status: ExitStatus| {
        if !status.success() {
            let code = status.code().unwrap_or(-1);
            if let Some(dir) = task_dir {
                if let Some(e) = check_error_filename(dir) {
                    return tako::Result::Err(e);
                }
            }
            return tako::Result::Err(tako::Error::GenericError(format!(
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
            |e: tako::Error| tako::Error::GenericError(format!("Streamer: {:?}", e.to_string()));
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
                child_wait(child, &program.stdin).map_err(tako::Error::from),
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
                        tako::Error::GenericError(
                            "Connection to stream failed while closing".into(),
                        )
                    })?
                    .map_err(streamer_error)?;
                result
            }
            futures::future::Either::Right((result, _)) => Err(match result {
                Ok(_) => streamer_error(tako::Error::GenericError(
                    "Internal error: Stream closed without calling close()".into(),
                )),
                Err(_) => streamer_error(tako::Error::GenericError(
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
                r = child_wait(child, &program.stdin) => status_to_result(r?)
        }
    }
}
