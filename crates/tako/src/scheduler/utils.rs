use crate::server::task::Task;
use crate::WorkerId;

pub fn task_transfer_cost(task: &Task, worker_id: WorkerId) -> u64 {
    // TODO: For large number of inputs, only sample inputs
    task.inputs
        .iter()
        .take(512)
        .map(|ti| {
            let t = ti.task().get();
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
