use crate::internal::server::task::Task;
use crate::internal::server::taskmap::TaskMap;
use crate::WorkerId;

pub(crate) fn task_transfer_cost(taskmap: &TaskMap, task: &Task, worker_id: WorkerId) -> u64 {
    // TODO: For large number of inputs, only sample inputs
    task.inputs
        .iter()
        .take(512)
        .map(|ti| {
            let t = taskmap.get_task(ti.task());
            let info = t.data_info().unwrap();
            if info.placement.contains(&worker_id) {
                0u64
            } else if info.future_placement.contains_key(&worker_id) {
                1u64
            } else {
                info.data_info.size
            }
        })
        .sum()
}
