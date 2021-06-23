use std::fs::File;
use std::process::Stdio;

use tokio::process::Command;
use tokio::sync::oneshot;

use crate::common::error::DsError;
use crate::common::resources::ResourceAllocation;
use crate::messages::common::{LauncherDefinition, ProgramDefinition, TaskFailInfo};
use crate::worker::state::WorkerStateRef;
use crate::worker::task::{Task, TaskRef};
use bstr::ByteSlice;
use std::path::PathBuf;

pub type LauncherSetup = Box<dyn Fn(&Task, LauncherDefinition) -> crate::Result<ProgramDefinition>>;

pub fn pin_program(program: &mut ProgramDefinition, allocation: &ResourceAllocation) {
    program.args.insert(0, "taskset".into());
    program.args.insert(1, "-c".into());
    program
        .args
        .insert(2, allocation.comma_delimited_cpu_ids().into());
}

/// Create an output stream file on the given path.
/// If the path is relative and `cwd` is specified, the file will be created relative to `cwd`.
fn create_output_stream(path: Option<&PathBuf>, cwd: Option<&PathBuf>) -> crate::Result<Stdio> {
    let stdio = match path {
        Some(path) => {
            let stream_path = match cwd {
                Some(cwd) => cwd.join(path),
                None => path.clone()
            };

            let file = File::create(stream_path)
                .map_err(|e| format!("Creating stream file failed: {}", e))?;
            Stdio::from(file)
        }
        None => Stdio::null(),
    };
    Ok(stdio)
}

fn command_from_definitions(definition: &ProgramDefinition) -> crate::Result<Command> {
    if definition.args.is_empty() {
        return Result::Err(crate::Error::GenericError(
            "No command arguments".to_string(),
        ));
    }

    let mut command = Command::new(definition.args[0].to_os_str_lossy());

    command.kill_on_drop(true);
    command.args(definition.args[1..].iter().map(|x| x.to_os_str_lossy()));

    if let Some(cwd) = &definition.cwd {
        std::fs::create_dir_all(cwd).expect("Could not create working directory");
        command.current_dir(cwd);
    }

    command.stdout(create_output_stream(
        definition.stdout.as_ref(),
        definition.cwd.as_ref(),
    )?);
    command.stderr(create_output_stream(
        definition.stderr.as_ref(),
        definition.cwd.as_ref(),
    )?);

    for (k, v) in definition.env.iter() {
        command.env(k.to_os_str_lossy(), v.to_os_str_lossy());
    }

    Ok(command)
}

async fn start_program_from_task(
    state_ref: &WorkerStateRef,
    task_ref: &TaskRef,
) -> crate::Result<()> {
    let program: ProgramDefinition = {
        let task = task_ref.get();
        let def: LauncherDefinition = rmp_serde::from_slice(&task.spec)?;
        (state_ref.get().launcher_setup)(&task, def)?
    };

    let mut command = command_from_definitions(&program)?;
    let status = command.status().await?;
    if !status.success() {
        let code = status.code().unwrap_or(-1);
        return Result::Err(DsError::GenericError(format!(
            "Program terminated with exit code {}",
            code
        )));
    }
    Ok(())
}

pub fn launch_program_from_task(
    state_ref: WorkerStateRef,
    task_ref: TaskRef,
) -> oneshot::Sender<()> {
    let (sender, receiver) = oneshot::channel();
    tokio::task::spawn_local(async move {
        if task_ref.get().is_removed() {
            // Task was canceled in between start of the task and this spawn_local
            return;
        }
        log::debug!(
            "Starting program launcher {} {:?} {:?}",
            task_ref.get().id,
            &task_ref.get().resources,
            task_ref.get().resource_allocation()
        );
        let program = start_program_from_task(&state_ref, &task_ref);
        tokio::select! {
            biased;
            _ = receiver => {
                let task_id = task_ref.get().id;
                log::debug!("Launcher cancelled id={}", task_id);
            }
            r = program => {
                match r {
                    Ok(()) => {
                        let mut state = state_ref.get_mut();
                        let task_id = task_ref.get().id;
                        log::debug!("Program launcher finished id={}", task_id);
                        state.finish_task(task_ref, 0);
                        /*
                        let data_ref = DataObjectRef::new(
                            task_id,
                            0,
                            DataObjectState::Local(LocalData {
                                serializer: SerializationType::None,
                                bytes: Default::default(),
                                subworkers: Default::default()
                            }),
                        );
                        state.add_data_object(data_ref);*/
                    }
                    Err(e) => {
                        log::debug!("Program launcher failed id={}", task_ref.get().id);
                        let mut state = state_ref.get_mut();
                        state.finish_task_failed(task_ref, TaskFailInfo::from_string(e.to_string()));
                    }
                }
            }
        }
    });
    sender
}
