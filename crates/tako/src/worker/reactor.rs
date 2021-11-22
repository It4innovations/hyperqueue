use crate::common::resources::ResourceAllocation;
use crate::messages::worker::{FromWorkerMessage, TaskRunningMsg};
use crate::worker::state::WorkerState;
use crate::worker::task::TaskState;
use crate::worker::taskenv::TaskEnv;
use crate::TaskId;

pub fn assign_task(state: &mut WorkerState, task_id: TaskId, allocation: ResourceAllocation) {
    {
        let mut task = state.get_task(task_id).get_mut();
        log::debug!("Task={} assigned", task.id);

        state.send_message_to_server(FromWorkerMessage::TaskRunning(TaskRunningMsg {
            id: task.id,
        }));

        let mut task_env = TaskEnv::new();
        task_env.start_task(state, &task);
        task.state = TaskState::Running(task_env, allocation);
    }
    state.running_tasks.insert(task_id);
}
