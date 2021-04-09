use crate::common::{Map, Set};
use crate::TaskId;
use crate::server::task::{TaskRef, Task};

pub fn compute_b_level_metric<'a>(tasks: &Map<TaskId, TaskRef>) {
    crawl(tasks, |t| t.get_consumers(), |t| &t.inputs);
}

fn crawl<'a, F1: Fn(&Task) -> &Set<TaskRef>, F2: Fn(&Task) -> &Vec<TaskRef>>(
    tasks: &Map<TaskId, TaskRef>,
    predecessor_fn: F1,
    successor_fn: F2,
) {
    let mut neighbours: Map<TaskRef, u32> = Map::with_capacity(tasks.len());
    let mut stack: Vec<TaskRef> = Vec::new();
    for (_, tref) in tasks.iter() {
        let len = predecessor_fn(&tref.get()).len() as u32;
        if len == 0 {
            stack.push(tref.clone());
        } else {
            neighbours.insert(tref.clone(), len);
        }
    }

    while let Some(tref) = stack.pop() {
        let mut task = tref.get_mut();

        let mut level = 0;
        for tr in predecessor_fn(&task) {
            level = level.max(tr.get().get_scheduler_priority());
        }
        task.set_scheduler_priority(level + 1);

        for inp in successor_fn(&task) {
            let v: &mut u32 = neighbours
                .get_mut(&inp)
                .expect("Couldn't find task neighbour in level computation");
            if *v <= 1 {
                assert_eq!(*v, 1);
                stack.push(inp.clone());
            } else {
                *v -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::server::core::{Core};
    use crate::server::test_util::submit_example_2;
    use crate::scheduler::metrics::compute_b_level_metric;

    #[test]
    fn b_level_simple_graph() {
        let mut core = Core::default();
        submit_example_2(&mut core);
        compute_b_level_metric(core.get_task_map());

        assert_eq!(core.get_task_by_id_or_panic(7).get().get_scheduler_priority(), 1);
        assert_eq!(core.get_task_by_id_or_panic(6).get().get_scheduler_priority(), 2);
        assert_eq!(core.get_task_by_id_or_panic(5).get().get_scheduler_priority(), 1);
        assert_eq!(core.get_task_by_id_or_panic(4).get().get_scheduler_priority(), 2);
        assert_eq!(core.get_task_by_id_or_panic(3).get().get_scheduler_priority(), 3);
        assert_eq!(core.get_task_by_id_or_panic(2).get().get_scheduler_priority(), 3);
        assert_eq!(core.get_task_by_id_or_panic(1).get().get_scheduler_priority(), 4);
    }

}
