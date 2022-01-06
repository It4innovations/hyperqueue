use crate::common::resources::ResourceAllocation;
use crate::messages::worker::{FromWorkerMessage, TaskRunningMsg};
use crate::worker::state::{WorkerState, WorkerStateRef};
use crate::worker::task::TaskState;
use crate::worker::taskenv::TaskEnv;
use crate::TaskId;

pub fn run_task(
    state: &mut WorkerState,
    state_ref: WorkerStateRef,
    task_id: TaskId,
    allocation: ResourceAllocation,
) {
    {
        log::debug!("Task={} assigned", task_id);

        state.send_message_to_server(FromWorkerMessage::TaskRunning(TaskRunningMsg {
            id: task_id,
        }));

        let mut task_env = TaskEnv::new();
        task_env.start_task(state, state_ref, task_id);

        let mut task = state.get_task_mut(task_id);
        task.state = TaskState::Running(task_env, allocation);
    }
    state.running_tasks.insert(task_id);
}
