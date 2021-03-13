use rand::prelude::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use crate::common::{Map, Set};
use crate::scheduler::graph::{assign_task_to_worker, create_task_assignment, SchedulerGraph};
use crate::scheduler::metrics::BLevelMetric;
use crate::scheduler::metrics::NodeMetrics;
use crate::scheduler::protocol::{
    SchedulerRegistration, TaskStealResponse, TaskUpdate, TaskUpdateType,
};
use crate::scheduler::task::{SchedulerTaskState, Task, TaskRef};
use crate::scheduler::utils::task_transfer_cost;
use crate::scheduler::worker::{Worker, WorkerRef};
use crate::scheduler::{Scheduler, TaskAssignment, ToSchedulerMessage};
use std::cmp::Ordering;
use crate::WorkerId;

type DirtyTaskSet = Set<TaskRef>;

#[derive(Debug)]
pub struct WorkstealingScheduler {
    graph: SchedulerGraph,
    // The set is cached here to reduce allocations
    dirty_tasks: DirtyTaskSet,
    random: SmallRng,
}

impl Default for WorkstealingScheduler {
    fn default() -> Self {
        Self {
            graph: Default::default(),
            dirty_tasks: Default::default(),
            random: SmallRng::from_entropy(),
        }
    }
}

impl WorkstealingScheduler {
    /// Returns true if balancing is needed.
    fn schedule_available_tasks(&mut self) -> bool {
        if self.graph.workers.is_empty() {
            return false;
        }

        log::debug!("Scheduling started");
        if !self.graph.new_tasks.is_empty() {
            // TODO: utilize information and do not recompute all b-levels
            trace_time!("scheduler", "blevel", {
                BLevelMetric::assign_metric(&self.graph.tasks);
            });

            self.graph.new_tasks.clear();
        }

        for tr in self.graph.ready_to_assign.drain(..) {
            let mut task = tr.get_mut();
            assert!(task.is_waiting());
            task.state = SchedulerTaskState::AssignedFresh;
            let worker_ref = choose_worker_for_task(&task, &self.graph.workers, &mut self.random);
            let mut worker = worker_ref.get_mut();
            log::debug!("Task {} initially assigned to {}", task.id, worker.id);
            assert!(task.assigned_worker.is_none());
            assign(
                &mut self.dirty_tasks,
                &mut task,
                tr.clone(),
                &mut worker,
                worker_ref.clone(),
            );
        }

        let has_underload_workers = self.graph.workers.values().any(|wr| {
            let worker = wr.get();
            worker.is_underloaded()
        });

        log::debug!("Scheduling finished");
        has_underload_workers
    }

    fn balance(&mut self) {
        log::debug!("Balancing started");
        let mut balanced_tasks = Vec::new();
        for wr in self.graph.workers.values() {
            let worker = wr.get();
            let len = worker.tasks.len() as u32;
            if len > worker.ncpus {
                log::debug!("Worker {} offers {} tasks", worker.id, len);
                for tr in &worker.tasks {
                    tr.get_mut().take_flag = false;
                }
                balanced_tasks.extend(
                    worker
                        .tasks
                        .iter()
                        .filter(|tr| !tr.get().is_pinned())
                        .cloned(),
                );
            }
        }

        let mut underload_workers = Vec::new();
        for wr in self.graph.workers.values() {
            let worker = wr.get();
            let len = worker.tasks.len() as u32;
            if len < worker.ncpus {
                log::debug!("Worker {} is underloaded ({} tasks)", worker.id, len);
                let mut ts = balanced_tasks.clone();
                ts.sort_by_cached_key(|tr| {
                    let task = tr.get();
                    let mut cost = task_transfer_cost(&task, &wr);
                    if !task.is_fresh() {
                        cost += cost / 8;
                        cost += 10_000_000;
                    }
                    log::debug!(
                        "Transfer cost task={} -> worker={} is {}",
                        task.id,
                        worker.id,
                        cost
                    );
                    std::u64::MAX - cost
                });
                underload_workers.push((wr.clone(), ts));
            }
        }
        underload_workers.sort_by_key(|x| x.0.get().tasks.len());

        let mut n_tasks = underload_workers[0].0.get().tasks.len();
        loop {
            let mut change = false;
            for (wr, ts) in underload_workers.iter_mut() {
                let mut worker = wr.get_mut();
                if worker.tasks.len() > n_tasks {
                    break;
                }
                if ts.is_empty() {
                    continue;
                }
                while let Some(tr) = ts.pop() {
                    let mut task = tr.get_mut();
                    if task.take_flag {
                        continue;
                    }
                    task.take_flag = true;
                    let wid = {
                        let wr2 = task.assigned_worker.clone().unwrap();
                        let worker2 = wr2.get();
                        let worker2_n_tasks = worker2.tasks.len();
                        if worker2_n_tasks <= n_tasks || worker2_n_tasks <= worker2.ncpus as usize {
                            continue;
                        }
                        worker2.id
                    };
                    log::debug!(
                        "Changing assignment of task={} from worker={} to worker={}",
                        task.id,
                        wid,
                        worker.id
                    );
                    assign(
                        &mut self.dirty_tasks,
                        &mut task,
                        tr.clone(),
                        &mut worker,
                        wr.clone(),
                    );
                    break;
                }
                change = true;
            }
            if !change {
                break;
            }
            n_tasks += 1;
        }
        log::debug!("Balancing finished");
    }

    fn task_update(&mut self, tu: TaskUpdate) -> bool {
        match tu.state {
            TaskUpdateType::Finished => {
                let (mut invoke_scheduling, worker) =
                    self.graph.finish_task(tu.id, tu.worker, tu.size.unwrap());
                invoke_scheduling |= worker.get().is_underloaded();
                return invoke_scheduling;
            }
            TaskUpdateType::Placed => self.graph.place_task_on_worker(tu.id, tu.worker),
            TaskUpdateType::Removed => self.graph.remove_task_from_worker(tu.id, tu.worker),
        }
        false
    }

    fn rollback_steal(&mut self, response: TaskStealResponse) -> bool {
        let tref = self.graph.get_task(response.id);
        let mut task = tref.get_mut();
        let new_wref = self.graph.get_worker(response.to_worker);

        let need_balancing = {
            let wref = task.assigned_worker.as_ref().unwrap();
            if wref == new_wref {
                return false;
            }
            for tref in &task.inputs {
                let mut t = tref.get_mut();
                t.remove_future_placement(&wref);
                t.set_future_placement(new_wref.clone());
            }
            let mut worker = wref.get_mut();
            worker.tasks.remove(&tref);
            worker.is_underloaded()
        };
        task.state = SchedulerTaskState::AssignedPinned;
        task.assigned_worker = Some(new_wref.clone());
        let mut new_worker = new_wref.get_mut();
        new_worker.tasks.insert(tref.clone());
        need_balancing
    }

    pub fn sanity_check(&self) {
        self.graph.sanity_check()
    }
}

fn assign(
    dirty_tasks: &mut DirtyTaskSet,
    task: &mut Task,
    task_ref: TaskRef,
    worker: &mut Worker,
    worker_ref: WorkerRef,
) {
    dirty_tasks.insert(task_ref.clone());
    assign_task_to_worker(task, task_ref, worker, worker_ref);
}

impl Scheduler for WorkstealingScheduler {
    fn identify(&self) -> SchedulerRegistration {
        SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "workstealing-scheduler".into(),
            scheduler_version: "0.0".into(),
        }
    }

    fn handle_messages(&mut self, message: ToSchedulerMessage) -> bool {
        match message {
            ToSchedulerMessage::TaskUpdate(tu) => {
                return self.task_update(tu);
            }
            ToSchedulerMessage::TaskStealResponse(sr) => {
                if !sr.success {
                    return self.rollback_steal(sr);
                }
            }
            ToSchedulerMessage::NewTasks(ts) => {
                for ti in ts {
                    self.graph.add_task(ti);
                }
                return true;
            }
            ToSchedulerMessage::RemoveTask(task_id) => self.graph.remove_task(task_id),
            ToSchedulerMessage::NewFinishedTask(ti) => self.graph.add_finished_task(ti),
            ToSchedulerMessage::NewWorker(wi) => {
                self.graph.add_worker(wi);
                return true;
            }
            ToSchedulerMessage::NetworkBandwidth(nb) => {
                self.graph.network_bandwidth = nb;
            }
        };
        return false;
    }

    fn schedule(&mut self) -> Vec<TaskAssignment> {
        if self.schedule_available_tasks() {
            trace_time!("scheduler", "balance", self.balance());
        }
        self.dirty_tasks
            .drain()
            .map(|t| {
                let mut task = t.get_mut();
                let worker_id = task.assigned_worker.as_ref().unwrap().get().id;
                create_task_assignment::<BLevelMetric>(&mut task, worker_id)
            })
            .collect()
    }
}

fn choose_worker_for_task(
    task: &Task,
    worker_map: &Map<WorkerId, WorkerRef>,
    random: &mut SmallRng,
) -> WorkerRef {
    let mut costs = std::u64::MAX;
    let mut workers = Vec::new();
    for wr in worker_map.values() {
        let c = task_transfer_cost(task, wr);
        match c.cmp(&costs) {
            Ordering::Less => {
                costs = c;
                workers.clear();
                workers.push(wr.clone());
            }
            Ordering::Equal => workers.push(wr.clone()),
            Ordering::Greater => { /* Do nothing */ }
        }
    }
    if workers.len() == 1 {
        workers.pop().unwrap()
    } else {
        workers.choose(random).unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{Map, Set};
    use crate::scheduler::test_util::{
        assigned_worker, connect_workers, finish_task, new_task, new_tasks,
    };
    use crate::scheduler::{TaskId, WorkerId};

    use super::*;

    fn init() {
        let _ = env_logger::try_init().map_err(|_| {
            println!("Logging initialized failed");
        });
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
    fn submit_graph_simple(scheduler: &mut WorkstealingScheduler) {
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

    /* Graph reduce

        0   1   2   3   4 .. n-1
         \  \   |   /  /    /
          \--\--|--/--/----/
                |
                n
    */
    fn submit_graph_reduce(scheduler: &mut WorkstealingScheduler, size: usize) {
        let mut tasks: Vec<_> = (0..size)
            .map(|t| new_task(t as TaskId, Vec::new()))
            .collect();
        tasks.push(new_task(size as TaskId, (0..size as TaskId).collect()));
        scheduler.handle_messages(new_tasks(tasks));
    }

    /* Graph split

        0
        |\---\---\------\
        | \   \   \     \
        1  2   3   4  .. n-1
    */
    fn submit_graph_split(scheduler: &mut WorkstealingScheduler, size: usize) {
        let mut tasks = vec![new_task(0, Vec::new())];
        tasks.extend((1..=size).map(|t| new_task(t as TaskId, vec![0])));
        scheduler.handle_messages(new_tasks(tasks));
    }

    fn run_schedule(scheduler: &mut WorkstealingScheduler) -> Vec<TaskAssignment> {
        scheduler.schedule()
    }

    fn run_schedule_get_task_ids(scheduler: &mut WorkstealingScheduler) -> Set<TaskId> {
        run_schedule(scheduler).iter().map(|a| a.task).collect()
    }

    #[test]
    fn test_simple_w1_1() {
        let mut scheduler = WorkstealingScheduler::default();
        submit_graph_simple(&mut scheduler);
        connect_workers(&mut scheduler, 1, 1);
        scheduler.sanity_check();

        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();

        let w = assigned_worker(&scheduler.graph, 1);
        finish_task(&mut scheduler, 1, w, 1);
        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 2);
        assert!(n.contains(&2));
        assert!(n.contains(&3));
        let _t2 = scheduler.graph.tasks.get(&2).unwrap();
        let _t3 = scheduler.graph.tasks.get(&3).unwrap();
        assert_eq!(assigned_worker(&scheduler.graph, 2), 100);
        assert_eq!(assigned_worker(&scheduler.graph, 3), 100);
        scheduler.sanity_check();
    }

    #[test]
    fn test_simple_w2_1() {
        init();
        let mut scheduler = WorkstealingScheduler::default();
        submit_graph_simple(&mut scheduler);
        connect_workers(&mut scheduler, 2, 1);
        scheduler.sanity_check();

        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();

        let w = assigned_worker(&scheduler.graph, 1);
        finish_task(&mut scheduler, 1, w, 1);
        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 2);
        assert!(n.contains(&2));
        assert!(n.contains(&3));
        let _t2 = scheduler.graph.tasks.get(&2).unwrap();
        let _t3 = scheduler.graph.tasks.get(&3).unwrap();
        assert_ne!(
            assigned_worker(&scheduler.graph, 2),
            assigned_worker(&scheduler.graph, 3)
        );
        scheduler.sanity_check();
    }

    #[test]
    fn test_reduce_w5_1() {
        init();
        let mut scheduler = WorkstealingScheduler::default();
        submit_graph_reduce(&mut scheduler, 5000);
        connect_workers(&mut scheduler, 5, 1);
        scheduler.sanity_check();

        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 5000);
        scheduler.sanity_check();

        let mut wcount = Map::default();
        for t in 0..5000 {
            assert!(n.contains(&t));
            let wid = assigned_worker(&scheduler.graph, t);
            let c = wcount.entry(wid).or_insert(0);
            *c += 1;
        }

        dbg!(&wcount);
        assert_eq!(wcount.len(), 5);
        for v in wcount.values() {
            assert!(*v > 400);
        }
    }

    #[test]
    fn test_split_w5_1() {
        init();
        let mut scheduler = WorkstealingScheduler::default();
        submit_graph_split(&mut scheduler, 5000);
        connect_workers(&mut scheduler, 5, 1);
        scheduler.sanity_check();

        let _n = run_schedule_get_task_ids(&mut scheduler);
        let w = assigned_worker(&scheduler.graph, 0);
        finish_task(&mut scheduler, 0, w, 100_000_000);
        scheduler.sanity_check();

        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 5000);
        scheduler.sanity_check();

        let mut wcount = Map::default();
        for t in 1..=5000 {
            assert!(n.contains(&t));
            let wid = assigned_worker(&scheduler.graph, t);
            let c = wcount.entry(wid).or_insert(0);
            *c += 1;
        }

        dbg!(&wcount);
        assert_eq!(wcount.len(), 5);
        for v in wcount.values() {
            assert!(*v > 400);
        }
    }

    #[test]
    fn test_consecutive_submit() {
        init();
        let mut scheduler = WorkstealingScheduler::default();
        connect_workers(&mut scheduler, 5, 1);
        scheduler.handle_messages(new_tasks(vec![new_task(1, vec![])]));
        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&1));
        scheduler.sanity_check();
        finish_task(&mut scheduler, 1, 101, 1000_1000);
        scheduler.sanity_check();
        scheduler.handle_messages(new_tasks(vec![new_task(2, vec![1])]));
        let n = run_schedule_get_task_ids(&mut scheduler);
        assert_eq!(n.len(), 1);
        assert!(n.contains(&2));
        scheduler.sanity_check();
    }

    #[test]
    fn test_many_submits() {
        init();
        const WORKERS: u32 = 42;
        const TASKS: usize = 40;
        let mut scheduler = WorkstealingScheduler::default();
        connect_workers(&mut scheduler, WORKERS, 1);
        let mut workers: Set<WorkerId> = Set::new();
        for i in 0..TASKS {
            scheduler.handle_messages(new_tasks(vec![new_task((i + 1) as u64, vec![])]));
            let n = run_schedule(&mut scheduler);
            assert_eq!(n.len(), 1);
            let worker_id = n.iter().next().unwrap().worker;
            assert!(!workers.contains(&worker_id));
            workers.insert(worker_id);
        }
        assert_eq!(workers.len(), TASKS);
        scheduler.sanity_check();
    }
}
