use crate::common::{Map, Set};
use crate::server::task::Task;
use crate::server::taskmap::TaskMap;
use crate::TaskId;

pub fn compute_b_level_metric(tasks: &mut TaskMap) {
    crawl(tasks, |t| t.get_consumers());
}

fn crawl<F1: Fn(&Task) -> &Set<TaskId>>(tasks: &mut TaskMap, predecessor_fn: F1) {
    let mut neighbours: Map<TaskId, u32> = Map::with_capacity(tasks.len());
    let mut stack: Vec<TaskId> = Vec::new();
    for task in tasks.iter_tasks() {
        let len = predecessor_fn(&task).len() as u32;
        if len == 0 {
            stack.push(task.id);
        } else {
            neighbours.insert(task.id, len);
        }
    }

    while let Some(task_id) = stack.pop() {
        let level = predecessor_fn(&tasks.get_task_ref(task_id))
            .iter()
            .map(|&pred_id| tasks.get_task_ref(pred_id).get_scheduler_priority())
            .max()
            .unwrap_or(0);

        let mut task = tasks.get_task_ref_mut(task_id);
        task.set_scheduler_priority(level + 1);

        for ti in &task.inputs {
            let input_id = ti.task();
            let v: &mut u32 = neighbours
                .get_mut(&input_id)
                .expect("Couldn't find task neighbour in level computation");
            if *v <= 1 {
                assert_eq!(*v, 1);
                stack.push(input_id);
            } else {
                *v -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduler::metrics::compute_b_level_metric;
    use crate::server::core::Core;
    use crate::tests::utils::workflows::submit_example_2;

    #[test]
    fn b_level_simple_graph() {
        let mut core = Core::default();
        submit_example_2(&mut core);
        compute_b_level_metric(core.get_task_map_mut());

        assert_eq!(
            core.get_task_by_id_or_panic(7.into())
                .get()
                .get_scheduler_priority(),
            1
        );
        assert_eq!(
            core.get_task_by_id_or_panic(6.into())
                .get()
                .get_scheduler_priority(),
            2
        );
        assert_eq!(
            core.get_task_by_id_or_panic(5.into())
                .get()
                .get_scheduler_priority(),
            1
        );
        assert_eq!(
            core.get_task_by_id_or_panic(4.into())
                .get()
                .get_scheduler_priority(),
            2
        );
        assert_eq!(
            core.get_task_by_id_or_panic(3.into())
                .get()
                .get_scheduler_priority(),
            3
        );
        assert_eq!(
            core.get_task_by_id_or_panic(2.into())
                .get()
                .get_scheduler_priority(),
            3
        );
        assert_eq!(
            core.get_task_by_id_or_panic(1.into())
                .get()
                .get_scheduler_priority(),
            4
        );
    }
}
