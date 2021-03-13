use crate::scheduler::task::Task;
use crate::scheduler::worker::WorkerRef;

pub fn task_transfer_cost(task: &Task, worker_ref: &WorkerRef) -> u64 {
    // TODO: For large number of inputs, only sample inputs
    let hostname_id = worker_ref.get().hostname_id;
    task.inputs
        .iter()
        .take(512)
        .map(|tr| {
            let t = tr.get();
            if t.placement.contains(worker_ref) {
                0u64
            } else if t.future_placement.contains_key(worker_ref) {
                1u64
            } else if t
                .placement
                .iter()
                .take(32)
                .any(|w| w.get().hostname_id == hostname_id)
            {
                t.size / 2
            } else {
                t.size
            }
        })
        .sum()
}
