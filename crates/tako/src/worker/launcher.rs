use std::fs::File;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;

use bstr::ByteSlice;
use tokio::process::Command;

use crate::messages::common::{ProgramDefinition, StdioDef};
use crate::worker::state::WorkerStateRef;
use crate::worker::taskenv::{StopReason, TaskResult};
use crate::TaskId;

pub trait TaskLauncher {
    fn start_task(
        &self,
        state_ref: WorkerStateRef,
        task_id: TaskId,
        stop_receiver: tokio::sync::oneshot::Receiver<StopReason>,
    ) -> Pin<Box<dyn Future<Output = crate::Result<TaskResult>>>>;
}

/// Create an output stream file on the given path.
/// If the path is relative and `cwd` is specified, the file will be created relative to `cwd`.
fn create_output_stream(def: &StdioDef, cwd: Option<&PathBuf>) -> crate::Result<Stdio> {
    let stdio = match def {
        StdioDef::File(path) => {
            let stream_path = match cwd {
                Some(cwd) => cwd.join(path),
                None => path.clone(),
            };

            let file = File::create(stream_path)
                .map_err(|e| format!("Creating stream file failed: {}", e))?;
            Stdio::from(file)
        }
        StdioDef::Null => Stdio::null(),
        StdioDef::Pipe => Stdio::piped(),
    };
    Ok(stdio)
}

pub fn command_from_definitions(definition: &ProgramDefinition) -> crate::Result<Command> {
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
        &definition.stdout,
        definition.cwd.as_ref(),
    )?);
    command.stderr(create_output_stream(
        &definition.stderr,
        definition.cwd.as_ref(),
    )?);

    for (k, v) in definition.env.iter() {
        command.env(k.to_os_str_lossy(), v.to_os_str_lossy());
    }

    Ok(command)
}

/*pub fn launch_program_from_task(
    state_ref: WorkerStateRef,
    task_ref: TaskRef,
) -> oneshot::Sender<()> {
    let (sender, receiver) = oneshot::channel();
    tokio::task::spawn_local(async move {
        if task_ref.get().is_removed() {
            // Task was canceled in between start of the task and this spawn_local
            return;
        }

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
*/
