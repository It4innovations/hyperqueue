use crate::server::task::Task;
use crate::WorkerId;

pub fn task_transfer_cost(task: &Task, worker_id: WorkerId) -> u64 {
    // TODO: For large number of inputs, only sample inputs
    task.inputs
        .iter()
        .take(512)
        .map(|tr| {
            let t = tr.get();
            if t.get_placement().unwrap().contains(&worker_id) {
                0u64
            } else if t.future_placement.contains_key(&worker_id) {
                1u64
            } else {
                t.data_info().unwrap().size
            }
        })
        .sum()
}
