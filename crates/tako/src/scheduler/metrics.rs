use crate::common::Map;
use crate::scheduler::task::{OwningTaskRef, Task, TaskRef};
use crate::TaskId;

pub trait NodeMetrics {
    /// -1 -> sort tasks in descending order
    /// 1 -> sort tasks in ascending order
    const SORT_MULTIPLIER: i32;

    fn assign_metric(tasks: &Map<TaskId, OwningTaskRef>);
}

#[derive(Default, Debug)]
pub struct BLevelMetric;

impl NodeMetrics for BLevelMetric {
    const SORT_MULTIPLIER: i32 = -1;

    fn assign_metric(tasks: &Map<TaskId, OwningTaskRef>) {
        crawl(tasks, |t| &t.consumers, |t| &t.inputs);
    }
}

#[derive(Default, Debug)]
pub struct TLevelMetric;

impl NodeMetrics for TLevelMetric {
    const SORT_MULTIPLIER: i32 = 1;

    fn assign_metric(tasks: &Map<TaskId, OwningTaskRef>) {
        crawl(tasks, |t| &t.inputs, |t| &t.consumers);
    }
}

fn crawl<F1: Fn(&Task) -> &Vec<TaskRef>, F2: Fn(&Task) -> &Vec<TaskRef>>(
    tasks: &Map<TaskId, OwningTaskRef>,
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
            level = level.max(tr.get().computed_metric);
        }
        task.computed_metric = level + 1;

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
    use crate::scheduler::graph::SchedulerGraph;
    use crate::scheduler::metrics::{BLevelMetric, NodeMetrics, TLevelMetric};
    use crate::scheduler::protocol::TaskInfo;

    #[test]
    fn b_level_simple_graph() {
        let graph = create_graph_simple();
        BLevelMetric::assign_metric(&graph.tasks);
        assert_eq!(graph.get_task(7).get().computed_metric, 1);
        assert_eq!(graph.get_task(6).get().computed_metric, 2);
        assert_eq!(graph.get_task(5).get().computed_metric, 1);
        assert_eq!(graph.get_task(4).get().computed_metric, 2);
        assert_eq!(graph.get_task(3).get().computed_metric, 3);
        assert_eq!(graph.get_task(2).get().computed_metric, 3);
        assert_eq!(graph.get_task(1).get().computed_metric, 4);
    }

    #[test]
    fn t_level_simple_graph() {
        let graph = create_graph_simple();
        TLevelMetric::assign_metric(&graph.tasks);
        assert_eq!(graph.get_task(1).get().computed_metric, 1);
        assert_eq!(graph.get_task(2).get().computed_metric, 2);
        assert_eq!(graph.get_task(3).get().computed_metric, 2);
        assert_eq!(graph.get_task(4).get().computed_metric, 3);
        assert_eq!(graph.get_task(5).get().computed_metric, 4);
        assert_eq!(graph.get_task(6).get().computed_metric, 3);
        assert_eq!(graph.get_task(7).get().computed_metric, 4);
    }

    /* Graph simple
         T1
        /  \
       T2   T3
       |  / |\
       T4   | T6
        \      \
         \ /   T7
          T5
    */
    fn create_graph_simple() -> SchedulerGraph {
        create_graph(vec![
            (1, vec![]),
            (2, vec![1]),
            (3, vec![1]),
            (4, vec![2, 3]),
            (5, vec![4]),
            (6, vec![3]),
            (7, vec![6]),
        ])
    }

    fn create_graph(tasks: Vec<(u64, Vec<u64>)>) -> SchedulerGraph {
        let mut graph = SchedulerGraph::default();
        for (id, inputs) in tasks {
            graph.add_task(TaskInfo { id, inputs });
        }
        graph
    }
}
