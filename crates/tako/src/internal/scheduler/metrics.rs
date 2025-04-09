use crate::TaskId;
use crate::internal::common::{Map, Set};
use crate::internal::server::task::Task;
use crate::internal::server::taskmap::TaskMap;

pub fn compute_b_level_metric(tasks: &mut TaskMap) {
    crawl(tasks, |t| t.get_consumers());
}

fn crawl<F1: Fn(&Task) -> &Set<TaskId>>(tasks: &mut TaskMap, predecessor_fn: F1) {
    let mut neighbours: Map<TaskId, u32> = Map::with_capacity(tasks.len());
    let mut stack: Vec<TaskId> = Vec::new();
    for task in tasks.tasks() {
        let len = predecessor_fn(task).len() as u32;
        if len == 0 {
            stack.push(task.id);
        } else {
            neighbours.insert(task.id, len);
        }
    }

    while let Some(task_id) = stack.pop() {
        let level = predecessor_fn(tasks.get_task(task_id))
            .iter()
            .map(|&pred_id| tasks.get_task(pred_id).get_scheduler_priority())
            .max()
            .unwrap_or(0);

        let task = tasks.get_task_mut(task_id);
        task.set_scheduler_priority(level + 1);

        for t in task.task_deps.iter() {
            if let Some(v) = neighbours.get_mut(t) {
                if *v <= 1 {
                    assert_eq!(*v, 1);
                    stack.push(*t);
                } else {
                    *v -= 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::TaskId;
    use crate::internal::common::index::ItemId;
    use crate::internal::scheduler::metrics::compute_b_level_metric;
    use crate::internal::server::core::Core;
    use crate::internal::tests::utils::workflows::submit_example_2;

    #[test]
    fn b_level_simple_graph() {
        let mut core = Core::default();
        submit_example_2(&mut core);
        compute_b_level_metric(core.task_map_mut());

        check_task_priority(&core, 7, 1);
        check_task_priority(&core, 6, 2);
        check_task_priority(&core, 5, 1);
        check_task_priority(&core, 4, 2);
        check_task_priority(&core, 3, 3);
        check_task_priority(&core, 2, 3);
        check_task_priority(&core, 1, 4);
    }

    fn check_task_priority(core: &Core, task_id: u64, priority: i32) {
        let task_id = TaskId::new(task_id as <TaskId as ItemId>::IdType);
        assert_eq!(core.get_task(task_id).get_scheduler_priority(), priority);
    }
}
