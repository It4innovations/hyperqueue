use crate::TaskId;
use crate::worker::data::{DataObjectState, InSubworkersData, LocalDownloadingData, Subscriber};
use crate::worker::state::WorkerState;
use crate::worker::subworker::{Subworker, SubworkerRef};
use crate::worker::task::{Task, TaskRef, TaskState};
use smallvec::smallvec;

fn choose_subworker(state: &mut WorkerState, task: &Task) -> SubworkerRef {
    let fsw = &state.free_subworkers;
    let len = fsw.len();
    if len == 1 || task.deps.is_empty() {
        return state.free_subworkers.pop().unwrap();
    }
    assert!(len > 0);
    let mut costs = Vec::with_capacity(len);
    costs.resize(len, u64::MAX);

    if task.deps.len() <= 32 {
        for dep in &task.deps {
            let obj = dep.get();
            for sw_ref in obj.get_placement().unwrap() {
                if let Some(p) = fsw.iter().position(|sw| sw == sw_ref) {
                    costs[p] -= obj.size;
                }
            }
        }
    } else {
        for idx in (0..task.deps.len()).step_by(task.deps.len() / 16) {
            let obj = task.deps[idx].get();
            for sw_ref in obj.get_placement().unwrap() {
                if let Some(p) = fsw.iter().position(|sw| sw == sw_ref) {
                    costs[p] -= obj.size;
                }
            }
        }
    }

    let pos: usize = costs
        .iter()
        .enumerate()
        .min_by_key(|x| x.1)
        .map(|x| x.0)
        .unwrap();
    state.free_subworkers.remove(pos)
}

pub fn start_task(subworker: &Subworker, subworker_ref: SubworkerRef, task: &mut Task) {
    task.state = TaskState::Running(subworker_ref);
    subworker.send_start_task(&task);
}

pub fn assign_task(state: &mut WorkerState, subworker_ref: &SubworkerRef, task_ref: &TaskRef) {
    let mut task = task_ref.get_mut();
    let mut sw = subworker_ref.get_mut();
    assert!(sw.running_task.is_none());
    sw.running_task = Some(task_ref.clone());

    log::debug!("Assigning task={} to subworker={}", task.id, sw.id,);

    let mut waiting_count = 0;
    for data_ref in &task.deps {
        let mut data_obj = data_ref.get_mut();
        if data_obj.is_in_subworker(&subworker_ref) {
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
                sw.send_data(
                    data_id,
                    local_data.bytes.clone(),
                    local_data.serializer.clone(),
                )
            }
            DataObjectState::Remote(_) | DataObjectState::Removed => unreachable!(),
        }
    }

    if waiting_count == 0 {
        task.state = TaskState::Running(subworker_ref.clone());
        start_task(&sw, subworker_ref.clone(), &mut task);
    } else {
        task.state = TaskState::Uploading(subworker_ref.clone(), waiting_count)
    }
}

pub fn try_assign_tasks(state: &mut WorkerState) {
    if state.free_subworkers.is_empty() {
        return;
    }
    while let Some((task_ref, _)) = state.ready_task_queue.pop() {
        {
            let subworker_ref = choose_subworker(state, &task_ref.get());
            assign_task(state, &subworker_ref, &task_ref);
        }
        if state.free_subworkers.is_empty() {
            return;
        }
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
