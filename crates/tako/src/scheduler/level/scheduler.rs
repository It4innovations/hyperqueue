use crate::scheduler::graph::{assign_task_to_worker, create_task_assignment, SchedulerGraph};
use crate::scheduler::metrics::NodeMetrics;
use crate::scheduler::protocol::SchedulerRegistration;
use crate::scheduler::task::{Task, TaskRef};
use crate::scheduler::utils::task_transfer_cost;
use crate::scheduler::worker::{Worker, WorkerRef};
use crate::scheduler::{Scheduler, TaskAssignment, ToSchedulerMessage};

#[derive(Default, Debug)]
pub struct LevelScheduler<Metric> {
    graph: SchedulerGraph,
    _phantom: std::marker::PhantomData<Metric>,
}

impl<Metric: NodeMetrics> Scheduler for LevelScheduler<Metric> {
    fn identify(&self) -> SchedulerRegistration {
        SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "blevel".into(),
            scheduler_version: "0.0".into(),
        }
    }

    fn handle_messages(&mut self, message: ToSchedulerMessage) -> bool {
        let mut schedule = false;
        match &message {
            ToSchedulerMessage::NewFinishedTask(..)
            | ToSchedulerMessage::NewTasks(..)
            | ToSchedulerMessage::NewWorker(..)
            | ToSchedulerMessage::TaskUpdate(..) => schedule = true,
            _ => {}
        }
        self.graph.handle_message(message);
        schedule
    }

    fn schedule(&mut self) -> Vec<TaskAssignment> {
        let mut assignments = vec![];

        if !self.graph.new_tasks.is_empty() {
            Metric::assign_metric(&self.graph.tasks);
            self.graph.new_tasks.clear();
        }

        if !self.graph.ready_to_assign.is_empty() {
            let mut underloaded_workers: Vec<_> = self
                .graph
                .workers
                .values()
                .filter(|w| w.get().is_underloaded())
                .collect();
            if !underloaded_workers.is_empty() {
                self.graph
                    .ready_to_assign
                    .sort_unstable_by_key(|t| Metric::SORT_MULTIPLIER * t.get().computed_metric);

                // TODO: handle multi-CPU workers
                let end =
                    std::cmp::min(self.graph.ready_to_assign.len(), underloaded_workers.len());
                for tref in self.graph.ready_to_assign.drain(..end) {
                    let mut task = tref.get_mut();
                    let (windex, worker) = underloaded_workers
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, w)| task_transfer_cost(&task, &w))
                        .unwrap();
                    assign::<Metric>(
                        &mut task,
                        tref.clone(),
                        &mut worker.get_mut(),
                        (*worker).clone(),
                        &mut assignments,
                    );
                    underloaded_workers.swap_remove(windex);
                }
            }
        }
        assignments
    }
}

fn assign<M: NodeMetrics>(
    task: &mut Task,
    task_ref: TaskRef,
    worker: &mut Worker,
    worker_ref: WorkerRef,
    assignments: &mut Vec<TaskAssignment>,
) {
    assign_task_to_worker(task, task_ref, worker, worker_ref);
    assignments.push(create_task_assignment::<M>(task, worker.id));
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use crate::scheduler::test_util::{connect_workers, finish_task, new_task, new_tasks};
    use crate::scheduler::{
        BLevelMetric, LevelScheduler, Scheduler, TLevelMetric, TaskAssignment, TaskId,
    };

    #[test]
    fn schedule_b_level() {
        let mut scheduler: LevelScheduler<BLevelMetric> = LevelScheduler::default();
        let workers = connect_workers(&mut scheduler, 1, 1);
        submit_graph_simple(&mut scheduler);
        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&1].priority, -4);
        finish_task(&mut scheduler, 1, workers[0], 0);

        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&2].priority, -3);
        finish_task(&mut scheduler, 2, workers[0], 0);

        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&3].priority, -3);
        finish_task(&mut scheduler, 3, workers[0], 0);

        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&4].priority, -2);
    }

    #[test]
    fn schedule_t_level() {
        let mut scheduler: LevelScheduler<TLevelMetric> = LevelScheduler::default();
        let workers = connect_workers(&mut scheduler, 1, 1);
        submit_graph_simple(&mut scheduler);
        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&1].priority, 1);
        finish_task(&mut scheduler, 1, workers[0], 0);

        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&2].priority, 2);
        finish_task(&mut scheduler, 2, workers[0], 0);

        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&3].priority, 2);
        finish_task(&mut scheduler, 3, workers[0], 0);

        let assignments = schedule(&mut scheduler);
        assert_eq!(assignments[&4].priority, 3);
    }

    fn schedule<S: Scheduler>(scheduler: &mut S) -> HashMap<TaskId, TaskAssignment> {
        scheduler
            .schedule()
            .into_iter()
            .map(|t| (t.task, t))
            .collect()
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
    fn submit_graph_simple<S: Scheduler>(scheduler: &mut S) {
        scheduler.handle_messages(new_tasks(vec![
            new_task(1, vec![]),
            new_task(2, vec![1]),
            new_task(3, vec![1]),
            new_task(4, vec![2, 3]),
            new_task(5, vec![4]),
            new_task(6, vec![3]),
            new_task(7, vec![6]),
        ]));
    }
}
