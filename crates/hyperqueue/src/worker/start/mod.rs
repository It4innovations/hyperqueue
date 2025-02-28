use bstr::BString;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::oneshot::Receiver;

use crate::transfer::messages::{TaskBuildDescription, TaskKind};
use crate::{JobId, JobTaskId};
use tako::InstanceId;
use tako::launcher::{StopReason, TaskBuildContext, TaskLaunchData, TaskLauncher};

use crate::worker::start::program::build_program_task;
use crate::worker::streamer::StreamerRef;

mod program;

pub const WORKER_EXTRA_PROCESS_PID: &str = "ProcessPid";

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
        build_ctx: TaskBuildContext,
        stop_receiver: Receiver<StopReason>,
    ) -> tako::Result<TaskLaunchData> {
        log::debug!(
            "Starting task launcher task_id={} res={:?} alloc={:?} body_len={}",
            build_ctx.task_id(),
            build_ctx.resources(),
            build_ctx.allocation(),
            build_ctx.body().len(),
        );

        let desc: TaskBuildDescription = tako::comm::deserialize(build_ctx.body())?;
        let shared = SharedTaskDescription {
            job_id: desc.job_id,
            task_id: desc.task_id,
            submit_dir: desc.submit_dir.into_owned(),
            stream_path: desc.stream_path.map(|x| x.into_owned()),
            entry: desc.entry,
        };
        match desc.task_kind.into_owned() {
            TaskKind::ExternalProgram(program) => build_program_task(
                build_ctx,
                stop_receiver,
                &self.streamer_ref,
                program,
                shared,
            ),
        }
    }
}

struct SharedTaskDescription {
    job_id: JobId,
    task_id: JobTaskId,
    submit_dir: PathBuf,
    stream_path: Option<PathBuf>,
    entry: Option<BString>,
}
