use smallvec::smallvec;

use crate::common::resources::ResourceAllocation;
use crate::messages::worker::{FromWorkerMessage, TaskRunningMsg, ToWorkerMessage};
use crate::worker::data::{DataObjectState, InSubworkersData, LocalDownloadingData, Subscriber};
use crate::worker::state::WorkerState;
use crate::worker::task::{Task, TaskRef, TaskState};
use crate::worker::taskenv::TaskEnv;
use crate::TaskId;

pub fn start_task(
    state: &mut WorkerState,
    task: &mut Task,
    task_ref: &TaskRef,
    mut task_env: TaskEnv,
    allocation: ResourceAllocation,
) {
    task_env.start_task(state, task, task_ref, &allocation);
    task.state = TaskState::Running(task_env, allocation);
    state.running_tasks.insert(task_ref.clone());
}

pub fn assign_task(state: &mut WorkerState, task_ref: TaskRef, allocation: ResourceAllocation) {
    let mut task = task_ref.get_mut();
    log::debug!("Task={} assigned", task.id);

    state.send_message_to_server(FromWorkerMessage::TaskRunning(TaskRunningMsg {
        id: task.id,
    }));

    let task_env = if let Some(env) = TaskEnv::create(state, &task) {
        env
    } else {
        return;
    };
    let mut waiting_count = 0;
    for data_ref in &task.deps {
        let mut data_obj = data_ref.get_mut();
        if task_env.is_uploaded(&data_obj) {
            /* Data is already in subworker */
            continue;
        }
        let data_id = data_obj.id;
        match &mut data_obj.state {
            DataObjectState::InSubworkers(insw_data) => {
                data_obj.state = start_local_download(
                    state,
                    data_id,
                    insw_data,
                    Subscriber::Task(task_ref.clone()),
                );
                waiting_count += 1;
            }
            DataObjectState::LocalDownloading(local_downloading) => {
                log::debug!("Subscribing to local download");
                local_downloading
                    .subscribers
                    .push(Subscriber::Task(task_ref.clone()));
                waiting_count += 1;
            }
            DataObjectState::Local(local_data) => {
                //local_data
                task_env.send_data(
                    data_id,
                    local_data.bytes.clone(),
                    local_data.serializer.clone(),
                )
            }
            DataObjectState::Remote(_) | DataObjectState::Removed => unreachable!(),
        }
    }

    if waiting_count == 0 {
        start_task(state, &mut task, &task_ref, task_env, allocation);
    } else {
        task.state = TaskState::Uploading(task_env, waiting_count, allocation)
    }
}

pub fn start_local_download(
    state: &mut WorkerState,
    data_id: TaskId,
    insw_data: &mut InSubworkersData,
    subscriber: Subscriber,
) -> DataObjectState {
    let subworkers = std::mem::take(&mut insw_data.subworkers);
    let source_sw = state.random_choice(&subworkers).clone();
    log::debug!(
        "Starting download of data={} from subworker={}",
        data_id,
        source_sw.get().id
    );
    source_sw.get().send_download_request(data_id);
    let ldd = LocalDownloadingData {
        subworkers,
        source: source_sw,
        subscribers: smallvec![subscriber],
    };
    DataObjectState::LocalDownloading(ldd)
}
