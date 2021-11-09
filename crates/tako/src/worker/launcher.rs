use std::fs::File;
use std::process::Stdio;

use tokio::process::Command;
use tokio::sync::oneshot;

use crate::common::error::DsError;
use crate::messages::common::{ProgramDefinition, TaskFailInfo};
use crate::worker::state::WorkerStateRef;
use crate::worker::task::{Task, TaskRef};

pub type LauncherSetup = Box<dyn Fn(&Task, ProgramDefinition) -> crate::Result<ProgramDefinition>>;

fn command_from_definitions(definition: &ProgramDefinition) -> crate::Result<Command> {
    if definition.args.is_empty() {
        return Result::Err(crate::Error::GenericError(
            "No command arguments".to_string(),
        ));
    }

    let mut command = Command::new(&definition.args[0]);

    command.kill_on_drop(true);
    command.args(&definition.args[1..]);

    if let Some(cwd) = &definition.cwd {
        command.current_dir(cwd);
    }

    command.stdout(if let Some(filename) = &definition.stdout {
        Stdio::from(File::create(filename)?)
    } else {
        Stdio::null()
    });

    command.stderr(if let Some(filename) = &definition.stderr {
        Stdio::from(File::create(filename)?)
    } else {
        Stdio::null()
    });

    for (k, v) in definition.env.iter() {
        command.env(k, v);
    }

    Ok(command)
}

async fn start_program_from_task(
    state_ref: &WorkerStateRef,
    task_ref: &TaskRef,
) -> crate::Result<()> {
    let definition: ProgramDefinition = {
        let task = task_ref.get();
        let def = rmp_serde::from_slice(&task.spec)?;
        (state_ref.get().launcher_setup)(&task, def)?
    };

    let mut command = command_from_definitions(&definition)?;
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
