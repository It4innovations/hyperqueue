use std::cmp::{Ordering, Reverse};
use std::rc::Rc;
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tokio::sync::Notify;
use tokio::time::sleep;

use crate::common::trace::trace_task_assign;
use crate::common::Map;
use crate::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::server::comm::{Comm, CommSenderRef};
use crate::server::core::{Core, CoreRef};
use crate::server::task::{Task, TaskRuntimeState};
use crate::server::taskmap::TaskMap;
use crate::server::worker::Worker;
use crate::server::worker_load::ResourceRequestLowerBound;
use crate::{TaskId, WorkerId};

use super::metrics::compute_b_level_metric;
use super::utils::task_transfer_cost;

// Long duration - 1 year
const LONG_DURATION: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

pub struct SchedulerState {
    // Which tasks has modified state, this map holds the original state
    dirty_tasks: Map<TaskId, TaskRuntimeState>,

    random: SmallRng,
}

/// Selects a worker that is able to execute the given task.
///
/// The `workers` Vec is passed from the outside as an optimization, to reuse its allocated buffer.
///
/// If no worker is available, returns `None`.
fn choose_worker_for_task(
    task: &Task,
    taskmap: &TaskMap,
    worker_map: &Map<WorkerId, Worker>,
    random: &mut SmallRng,
    workers: &mut Vec<WorkerId>,
    now: std::time::Instant,
) -> Option<WorkerId> {
    workers.clear();

    let mut costs = u64::MAX;
    for worker in worker_map.values() {
        if !worker.is_capable_to_run(&task.configuration.resources, now) {
            continue;
        }

        let c = task_transfer_cost(taskmap, task, worker.id);
        match c.cmp(&costs) {
            Ordering::Less => {
                costs = c;
                workers.clear();
                workers.push(worker.id);
            }
            Ordering::Equal => workers.push(worker.id),
            Ordering::Greater => { /* Do nothing */ }
        }
    }

    match workers.len() {
        1 => Some(workers.pop().unwrap()),
        0 => None,
        _ => Some(*workers.choose(random).unwrap()),
    }
}

pub async fn scheduler_loop(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    scheduler_wakeup: Rc<Notify>,
    minimum_delay: Duration,
) {
    let mut last_schedule = Instant::now() - minimum_delay * 2;
    let mut state = SchedulerState {
        dirty_tasks: Default::default(),
        random: SmallRng::from_entropy(),
    };
    loop {
        scheduler_wakeup.notified().await;
        let since_last_schedule = last_schedule.elapsed();
        if minimum_delay > since_last_schedule {
            sleep(minimum_delay - since_last_schedule).await;
        }
        let mut comm = comm_ref.get_mut();
        state.run_scheduling(&mut core_ref.get_mut(), &mut *comm);
        comm.reset_scheduling_flag();
        last_schedule = Instant::now();
    }
}

impl SchedulerState {
    #[allow(clippy::new_without_default)]
    pub fn new() -> SchedulerState {
        Self {
            dirty_tasks: Default::default(),
            random: SmallRng::from_seed([
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0,
            ]),
        }
    }

    pub fn run_scheduling(&mut self, core: &mut Core, comm: &mut impl Comm) {
        if self.schedule_available_tasks(core) {
            trace_time!("scheduler", "balance", self.balance(core));
        }
        self.finish_scheduling(core, comm);
    }

    pub(crate) fn finish_scheduling(&mut self, core: &mut Core, comm: &mut impl Comm) {
        let mut task_steals: Map<WorkerId, Vec<TaskId>> = Default::default();
        let mut task_computes: Map<WorkerId, Vec<TaskId>> = Default::default();
        for (task_id, old_state) in self.dirty_tasks.drain() {
            let task = core.get_task(task_id);
            match (&task.state, old_state) {
                (TaskRuntimeState::Assigned(w_id), TaskRuntimeState::Waiting(winfo)) => {
                    debug_assert_eq!(winfo.unfinished_deps, 0);
                    debug_assert!(task.is_fresh());
                    task_computes.entry(*w_id).or_default().push(task_id)
                }
                (TaskRuntimeState::Stealing(from_id, _to_id), TaskRuntimeState::Assigned(w_id)) => {
                    debug_assert_eq!(*from_id, w_id);
                    debug_assert!(!task.is_fresh());
                    task_steals.entry(w_id).or_default().push(task.id);
                }
                (
                    TaskRuntimeState::Stealing(from_w1, _to_w1),
                    TaskRuntimeState::Stealing(from_w2, _to_w2),
                ) => {
                    debug_assert!(!task.is_fresh());
                    debug_assert_eq!(*from_w1, from_w2);
                }
                (new, old) => {
                    panic!("Invalid task state, old = {:?}, new = {:?}", new, old);
                }
            }
        }

        for (worker_id, task_ids) in task_steals {
            comm.send_worker_message(
                worker_id,
                &ToWorkerMessage::StealTasks(TaskIdsMsg { ids: task_ids }),
            );
        }

        for (worker_id, mut task_ids) in task_computes {
            task_ids.sort_unstable_by_key(|&task_id| {
                let task = core.get_task(task_id);
                Reverse((task.configuration.user_priority, task.scheduler_priority))
            });
            let tasks = core.task_map_mut();
            for task_id in task_ids {
                {
                    let task = tasks.get_task_mut(task_id);
                    task.set_fresh_flag(false);
                }
                let task = tasks.get_task(task_id);
                trace_task_assign(task.id, worker_id);
                comm.send_worker_message(worker_id, &task.make_compute_message(tasks));
            }
        }
    }

    pub(crate) fn assign(&mut self, core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
        let (inputs, assigned_worker) = {
            let (tasks, workers) = core.split_tasks_workers_mut();
            let task = tasks.get_task(task_id);
            let assigned_worker = task.get_assigned_worker();
            if let Some(w_id) = assigned_worker {
                log::debug!(
                    "Changing assignment of task={} from worker={} to worker={}",
                    task.id,
                    w_id,
                    worker_id
                );
                assert_ne!(w_id, worker_id);
                workers.get_worker_mut(w_id).remove_task(task);
            } else {
                log::debug!(
                    "Fresh assignment of task={} to worker={}",
                    task.id,
                    worker_id
                );
            }
            (task.inputs.clone(), assigned_worker)
        };

        for ti in inputs {
            let input = core.get_task_mut(ti.task());
            if let Some(wr) = assigned_worker {
                input.remove_future_placement(wr);
            }
            input.set_future_placement(worker_id);
        }

        let (tasks, workers) = core.split_tasks_workers_mut();
        let task = tasks.get_task_mut(task_id);
        workers.get_worker_mut(worker_id).insert_task(task);
        let new_state = match task.state {
            TaskRuntimeState::Waiting(_) => TaskRuntimeState::Assigned(worker_id),
            TaskRuntimeState::Assigned(old_w) => {
                if task.is_fresh() {
                    TaskRuntimeState::Assigned(worker_id)
                } else {
                    TaskRuntimeState::Stealing(old_w, Some(worker_id))
                }
            }
            TaskRuntimeState::Stealing(from_w, _) => {
                TaskRuntimeState::Stealing(from_w, Some(worker_id))
            }
            TaskRuntimeState::Running(_)
            | TaskRuntimeState::Finished(_)
            | TaskRuntimeState::Released => {
                panic!("Invalid state {:?}", task.state);
            }
        };
        let old_state = std::mem::replace(&mut task.state, new_state);
        self.dirty_tasks.entry(task.id).or_insert(old_state);
    }

    /// Returns true if balancing is needed.
    fn schedule_available_tasks(&mut self, core: &mut Core) -> bool {
        if !core.has_workers() {
            return false;
        }

        log::debug!("Scheduling started");

        if core.check_has_new_tasks_and_reset() {
            // TODO: utilize information and do not recompute all b-levels
            trace_time!("scheduler", "blevel", {
                compute_b_level_metric(core.task_map_mut())
            });
        }

        let ready_tasks = core.take_ready_to_assign();
        if !ready_tasks.is_empty() {
            let now = std::time::Instant::now();
            let mut tmp_workers = Vec::with_capacity(core.get_worker_map().len());
            for task_id in ready_tasks.into_iter() {
                let worker_id = {
                    let configuration = {
                        let task = core.get_task(task_id);
                        if let TaskRuntimeState::Released = task.state {
                            continue;
                        }
                        assert!(task.is_waiting());
                        task.configuration.clone()
                    };
                    core.try_wakeup_parked_resources(&configuration.resources);

                    choose_worker_for_task(
                        core.get_task(task_id),
                        core.task_map(),
                        core.get_worker_map(),
                        &mut self.random,
                        &mut tmp_workers,
                        now,
                    )
                    //log::debug!("Task {} initially assigned to {}", task.id, worker_id);
                };
                if let Some(worker_id) = worker_id {
                    debug_assert!(core
                        .get_worker_map()
                        .get_worker(worker_id)
                        .is_capable_to_run(&core.get_task(task_id).configuration.resources, now));
                    self.assign(core, task_id, worker_id);
                } else {
                    core.add_sleeping_task(task_id);
                }
            }
        }

        let has_underload_workers = core
            .get_workers()
            .any(|w| !w.is_parked() && w.is_underloaded());

        log::debug!("Scheduling finished");
        has_underload_workers
    }

    pub(crate) fn balance(&mut self, core: &mut Core) {
        // This method is called only if at least one worker is underloaded

        log::debug!("Balancing started");

        let mut balanced_tasks: Vec<TaskId> = Vec::new();
        let mut min_resource = ResourceRequestLowerBound::new(core.resource_count());
        let now = std::time::Instant::now();

        {
            let (tasks, workers) = core.split_tasks_workers_mut();
            for worker in workers.values() {
                let mut offered = 0;
                let not_overloaded = !worker.is_overloaded();
                for &task_id in worker.tasks() {
                    let task = tasks.get_task_mut(task_id);
                    if task.is_running()
                        || (not_overloaded
                            && (task.is_fresh() || !task.inputs.is_empty())
                            && worker.has_time_to_run(task.configuration.resources.min_time(), now))
                    {
                        continue;
                    }
                    task.set_take_flag(false);
                    min_resource.include(&task.configuration.resources);
                    balanced_tasks.push(task_id);
                    offered += 1;
                }
                if offered > 0 {
                    log::debug!(
                        "Worker {} offers {}/{} tasks",
                        worker.id,
                        offered,
                        worker.tasks().len()
                    );
                }
            }
        }

        if balanced_tasks.is_empty() {
            log::debug!("No tasks to balance");
            return;
        }

        log::debug!("Min resources {:?}", min_resource);

        let mut underload_workers = Vec::new();
        let task_map = core.task_map();
        for worker in core.get_workers() {
            // We could here also testing park flag, but it is already solved in the next condition
            if worker.have_immediate_resources_for_lb(&min_resource) {
                log::debug!(
                    "Worker {} is underloaded ({} tasks)",
                    worker.id,
                    worker.tasks().len()
                );
                let mut ts = balanced_tasks.clone();
                // Here we want to sort task such that [t1, ... tN]
                // Where t1 is the task with the highest cost to schedule here
                // and tN is the lowest cost to schedule here
                ts.sort_by_cached_key(|&task_id| {
                    let task = task_map.get_task(task_id);
                    let mut cost = task_transfer_cost(task_map, task, worker.id);
                    if !task.is_fresh() && task.get_assigned_worker() != Some(worker.id) {
                        cost += 10_000_000;
                    }
                    log::debug!(
                        "Transfer cost task={} -> worker={} is {}",
                        task.id,
                        worker.id,
                        cost
                    );
                    (
                        u64::MAX - cost,
                        worker.resources.n_cpus(&task.configuration.resources),
                    )
                });
                let len = ts.len();
                underload_workers.push((
                    worker.id,
                    ts,
                    len,
                    worker.remaining_time(now).unwrap_or(LONG_DURATION),
                ));
            }
        }

        if underload_workers.is_empty() {
            log::debug!("No balancing possible");
            return;
        }

        /* Micro heuristic, give small priority to workers with less tasks and with less remaining time */
        underload_workers
            .sort_unstable_by_key(|(_, ts, _, remaining_time)| (ts.len(), *remaining_time));

        /* Iteration 1 - try to assign tasks so workers are not longer underutilized */
        /* This should be always relatively quick even with million of tasks and many workers,
          since we require that each task need at least one core and we expect that number
          of cores per worker is low
        */
        loop {
            let mut changed = false;
            for (worker_id, ts, pos, _) in underload_workers.iter_mut() {
                let worker = core.get_worker_map().get_worker(*worker_id);
                if !worker.have_immediate_resources_for_lb(&min_resource) {
                    continue;
                }
                while *pos > 0 {
                    *pos -= 1;
                    let (task_id, worker2_id) = {
                        let task_id = ts[*pos];
                        let task = core.get_task(task_id);
                        if task.is_taken() {
                            continue;
                        }
                        if !worker.has_time_to_run(task.configuration.resources.min_time(), now) {
                            continue;
                        }
                        if !worker.have_immediate_resources_for_rq(&task.configuration.resources) {
                            continue;
                        }
                        let worker2_id = task.get_assigned_worker().unwrap();
                        (task_id, worker2_id)
                    };
                    if *worker_id != worker2_id {
                        self.assign(core, task_id, *worker_id);
                    }
                    changed = true;
                    core.get_task_mut(task_id).set_take_flag(true);
                    break;
                }
            }
            if !changed {
                break;
            }
        }

        log::debug!("Balancing phase 2");

        /* Iteration 2 - balance number of tasks per node */
        /* After Iteration 1 we know that workers are not underutilized,
          or this state could not be achieved, so it does not make sense
          to examine detailed resource requirements and we do it only roughly
        */

        loop {
            let mut changed = false;
            for (worker_id, ts, _, _) in underload_workers.iter_mut() {
                let worker = core.get_worker_map().get_worker(*worker_id);
                while let Some(task_id) = ts.pop() {
                    let worker2_id = {
                        let task = core.get_task(task_id);
                        if task.is_taken() {
                            continue;
                        }
                        if !worker.is_capable_to_run(&task.configuration.resources, now) {
                            continue;
                        }
                        let worker2_id = task.get_assigned_worker().unwrap();
                        let worker2 = core.get_worker_by_id_or_panic(worker2_id);

                        if !worker2.is_overloaded() || !worker2.is_more_loaded_then(worker) {
                            continue;
                        }
                        worker2_id
                    };
                    if *worker_id != worker2_id {
                        self.assign(core, task_id, *worker_id);
                    }
                    changed = true;
                    core.get_task_mut(task_id).set_take_flag(true);
                    break;
                }
            }
            if !changed {
                break;
            }
        }

        if core.get_workers().any(|w| w.is_overloaded()) {
            // We have overloaded worker, so try to park underloaded workers
            // as it may be case the there are unschedulable tasks for them
            core.park_workers();
        }

        log::debug!("Balancing finished");
    }
}
