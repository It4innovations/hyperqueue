use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Receiver;

use tako::launcher::{StopReason, TaskBuildContext, TaskLaunchData, TaskLauncher};
use tako::InstanceId;

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
        build_program_task(build_ctx, stop_receiver, &self.streamer_ref)
    }
}
