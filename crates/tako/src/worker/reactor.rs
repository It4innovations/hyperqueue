use crate::common::resources::ResourceAllocation;
use crate::messages::worker::{FromWorkerMessage, TaskRunningMsg};
use crate::worker::state::WorkerState;
use crate::worker::task::{TaskRef, TaskState};
use crate::worker::taskenv::TaskEnv;

pub fn assign_task(state: &mut WorkerState, task_ref: TaskRef, allocation: ResourceAllocation) {
    let mut task = task_ref.get_mut();
    log::debug!("Task={} assigned", task.id);

    state.send_message_to_server(FromWorkerMessage::TaskRunning(TaskRunningMsg {
        id: task.id,
    }));

    let mut task_env = TaskEnv::new();
    task_env.start_task(state, &task, &task_ref);
    task.state = TaskState::Running(task_env, allocation);
    state.running_tasks.insert(task_ref.clone());
}
