use std::cmp::{Ordering, Reverse};
use std::rc::Rc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::time::sleep;

//use crate::internal::common::trace::trace_time;
use crate::internal::common::Map;
use crate::internal::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::internal::server::comm::{Comm, CommSenderRef};
use crate::internal::server::core::{Core, CoreRef};
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::Worker;
use crate::internal::server::workerload::ResourceRequestLowerBound;
use crate::internal::server::workermap::WorkerMap;
use crate::{TaskId, WorkerId};

use super::metrics::compute_b_level_metric;
use super::utils::task_transfer_cost;

// Long duration - 1 year
const LONG_DURATION: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

// Knobs
const MAX_TASKS_FOR_TRY_PREV_WORKER_HEURISTICS: usize = 1000;

pub struct SchedulerState {
    // Which tasks has modified state, this map holds the original state
    dirty_tasks: Map<TaskId, TaskRuntimeState>,

    /// Use in choose_worker_for_task to reuse allocated buffer
    tmp_workers: Vec<WorkerId>,

    choose_counter: usize,
    now: std::time::Instant,
}

pub(crate) async fn scheduler_loop(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    scheduler_wakeup: Rc<Notify>,
    minimum_delay: Duration,
) {
    let mut last_schedule = Instant::now() - minimum_delay * 2;
    loop {
        scheduler_wakeup.notified().await;
        let mut now = Instant::now();
        let since_last_schedule = now - last_schedule;
        if minimum_delay > since_last_schedule {
            sleep(minimum_delay - since_last_schedule).await;
            now = Instant::now();
        }
        let mut comm = comm_ref.get_mut();
        let mut state = SchedulerState::new(now);
        let mut core = core_ref.get_mut();
        state.run_scheduling(&mut core, &mut *comm);
        comm.reset_scheduling_flag();
        last_schedule = Instant::now();
        comm.call_after_scheduling_callbacks(&mut core);
    }
}

impl SchedulerState {
    pub fn new(now: std::time::Instant) -> Self {
        SchedulerState {
            dirty_tasks: Map::new(),
            choose_counter: 0,
            tmp_workers: Vec::new(),
            now,
        }
    }

    fn get_last(&mut self) -> Option<WorkerId> {
        if !self.tmp_workers.is_empty() {
            Some(self.tmp_workers[self.choose_counter % self.tmp_workers.len()])
        } else {
            None
        }
    }

    fn pick_worker(&mut self) -> Option<WorkerId> {
        match self.tmp_workers.len() {
            1 => Some(self.tmp_workers.pop().unwrap()),
            0 => None,
            n => {
                self.choose_counter += 1;
                let worker_id = self.tmp_workers[self.choose_counter % n];
                Some(worker_id)
            }
        }
    }

    /// Selects a worker that is able to execute the given task.
    /// If no worker is available, returns `None`.
    fn choose_worker_for_task(
        &mut self,
        task: &Task,
        taskmap: &TaskMap,
        worker_map: &Map<WorkerId, Worker>,
        try_prev_worker: bool, // Enable heuristics that tries to fit tasks on fewer workers
    ) -> Option<WorkerId> {
        // Fast path
        if try_prev_worker && task.inputs.is_empty() {
            // Note: We are *not* using "is_capable_to_run" but "have_immediate_resources_for_rq",
            // because we want to enable fast path only if task can be directly executed
            // We want to avoid creation of overloaded
            if let Some(worker_id) = self.get_last() {
                let worker = &worker_map[&worker_id];
                if worker.has_time_to_run(task.configuration.resources.min_time(), self.now)
                    && worker.have_immediate_resources_for_rq(&task.configuration.resources)
                {
                    return Some(worker_id);
                }
            }
        }

        self.tmp_workers.clear(); // This has to be called AFTER fast path

        let mut costs = u64::MAX;
        for worker in worker_map.values() {
            if !worker.is_capable_to_run(&task.configuration.resources, self.now) {
                continue;
            }

            let c = task_transfer_cost(taskmap, task, worker.id);
            match c.cmp(&costs) {
                Ordering::Less => {
                    costs = c;
                    self.tmp_workers.clear();
                    self.tmp_workers.push(worker.id);
                }
                Ordering::Equal => self.tmp_workers.push(worker.id),
                Ordering::Greater => { /* Do nothing */ }
            }
        }
        self.pick_worker()
    }

    #[cfg(test)]
    pub fn run_scheduling_without_balancing(
        &mut self,
        core: &mut Core,
        comm: &mut impl Comm,
    ) -> bool {
        let need_balance = self.schedule_available_tasks(core);
        self.finish_scheduling(core, comm);
        need_balance
    }

    pub fn run_scheduling(&mut self, core: &mut Core, comm: &mut impl Comm) {
        if self.schedule_available_tasks(core) {
            trace_time!("scheduler", "balance", self.balance(core));
        }
        self.finish_scheduling(core, comm);
    }

    pub fn finish_scheduling(&mut self, core: &mut Core, comm: &mut impl Comm) {
        let mut task_steals: Map<WorkerId, Vec<TaskId>> = Default::default();
        let mut task_computes: Map<WorkerId, Vec<TaskId>> = Default::default();
        let (task_map, worker_map) = core.split_tasks_workers_mut();
        for (task_id, old_state) in self.dirty_tasks.drain() {
            let task = task_map.get_task(task_id);
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
                (TaskRuntimeState::RunningMultiNode(worker_ids), TaskRuntimeState::Waiting(_)) => {
                    comm.send_worker_message(
                        worker_ids[0],
                        &task.make_compute_message(worker_ids.clone()),
                    );
                    for worker_id in &worker_ids[1..] {
                        let worker = worker_map.get_worker_mut(*worker_id);
                        worker.set_reservation(true, comm);
                    }
                }
                (
                    TaskRuntimeState::Stealing(from_w1, _to_w1),
                    TaskRuntimeState::Stealing(from_w2, _to_w2),
                ) => {
                    debug_assert!(!task.is_fresh());
                    debug_assert_eq!(*from_w1, from_w2);
                }
                (new, old) => {
                    panic!("Invalid task state, new = {:?}, old = {:?}", new, old);
                }
            }
        }

        for (worker_id, task_ids) in task_steals {
            comm.send_worker_message(
                worker_id,
                &ToWorkerMessage::StealTasks(TaskIdsMsg { ids: task_ids }),
            );
        }

        let (task_map, worker_map) = core.split_tasks_workers_mut();
        for (worker_id, mut task_ids) in task_computes {
            task_ids.sort_by_cached_key(|&task_id| {
                let task = task_map.get_task(task_id);
                Reverse((task.configuration.user_priority, task.scheduler_priority))
            });
            for task_id in task_ids {
                let task = task_map.get_task_mut(task_id);
                task.set_fresh_flag(false);
                comm.send_worker_message(worker_id, &task.make_compute_message(Vec::new()));
            }
        }

        // Try unreserve workers
        for worker in worker_map.get_workers_mut() {
            if worker.is_reserved() {
                let unreserve = worker
                    .mn_task()
                    .map(|mn| {
                        let task = task_map.get_task(mn.task_id);
                        task.mn_root_worker().unwrap() == worker.id
                    })
                    .unwrap_or(true);
                if unreserve {
                    worker.set_reservation(false, comm);
                }
            }
        }
    }

    pub fn assign_multinode(
        &mut self,
        worker_map: &mut WorkerMap,
        task: &mut Task,
        workers: Vec<WorkerId>,
    ) {
        for worker_id in &workers {
            let worker = worker_map.get_worker_mut(*worker_id);
            worker.set_mn_task(task.id, worker_id != &workers[0]);
        }

        let old_state =
            std::mem::replace(&mut task.state, TaskRuntimeState::RunningMultiNode(workers));
        self.dirty_tasks.entry(task.id).or_insert(old_state);
    }

    pub fn assign(&mut self, core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
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
                workers.get_worker_mut(w_id).remove_sn_task(task);
            } else {
                log::debug!(
                    "Fresh assignment of task={} to worker={}",
                    task.id,
                    worker_id
                );
            }
            (task.inputs.clone(), assigned_worker)
        };

        for ti in inputs.into_iter() {
            let input = core.get_task_mut(ti.task());
            if let Some(wr) = assigned_worker {
                input.remove_future_placement(wr);
            }
            input.set_future_placement(worker_id);
        }

        let (tasks, workers) = core.split_tasks_workers_mut();
        let task = tasks.get_task_mut(task_id);
        workers.get_worker_mut(worker_id).insert_sn_task(task);
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
            TaskRuntimeState::Running { .. }
            | TaskRuntimeState::RunningMultiNode(_)
            | TaskRuntimeState::Finished(_) => {
                panic!("Invalid state {:?}", task.state);
            }
        };
        let old_state = std::mem::replace(&mut task.state, new_state);
        self.dirty_tasks.entry(task.id).or_insert(old_state);
    }

    // fn assign_multi_node_task(
    //     &mut self,
    //     comm: &mut impl Comm,
    //     task: &mut Task,
    //     worker_map: &mut WorkerMap,
    //     worker_ids: Vec<WorkerId>,
    // ) {
    //     // assert!(task.is_waiting());
    //     //
    //     // for worker_id in &worker_ids {
    //     //     let worker = worker_map.get_worker_mut(*worker_id);
    //     //     if *worker_id == worker_ids[0] {
    //     //         comm.send_worker_message(
    //     //             worker_ids[0],
    //     //             &task.make_compute_message(worker_ids.clone()),
    //     //         );
    //     //     } else {
    //     //         for worker_id in &worker_ids[1..] {
    //     //             if !worker.reservation_state() {
    //     //                 comm.send_worker_message(*worker_id, &ToWorkerMessage::ReservationOn);
    //     //             }
    //     //         }
    //     //     }
    //     // }
    // }

    fn try_start_multinode_tasks(&mut self, core: &mut Core) {
        let mut selected_workers = Vec::new();
        loop {
            // "while let" not used because of lifetime problems
            let (mn_queue, task_map, worker_map, worker_groups) = core.multi_node_queue_split_mut();
            if let Some((task_id, _)) = mn_queue.queue.peek() {
                let task_id = *task_id;
                let task = task_map.get_task_mut(task_id);
                let n_nodes = task.configuration.resources.n_nodes() as usize;
                assert!(n_nodes > 0);

                let mut found = false;
                let mut big_enough = false;
                'outer: for group in worker_groups.values() {
                    if group.size() < n_nodes {
                        continue;
                    }
                    big_enough = true;
                    selected_workers.clear();
                    for worker_id in group.worker_ids() {
                        let worker = worker_map.get_worker(worker_id);
                        if worker.is_free() {
                            selected_workers.push(worker_id);
                        }
                        if selected_workers.len() == n_nodes {
                            found = true;
                            break 'outer;
                        }
                    }
                }
                if found {
                    mn_queue.queue.pop();
                    self.assign_multinode(worker_map, task, std::mem::take(&mut selected_workers));
                    continue;
                } else if !big_enough {
                    log::debug!(
                        "Multi-node task {} put into sleep. (n_nodes={}, workers={})",
                        task_id,
                        n_nodes,
                        worker_map.len()
                    );
                    mn_queue.queue.pop();
                    core.add_sleeping_mn_task(task_id);
                    continue;
                } else {
                    return;
                }
            }
            return;
        }
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

            let (multi_node_queue, task_map, _, _) = core.multi_node_queue_split_mut();
            multi_node_queue.recompute_priorities(task_map);
        }

        let multi_node_ready_tasks = core.take_multi_node_ready_to_assign();
        if !multi_node_ready_tasks.is_empty() {
            let (multi_node_queue, task_map, _, _) = core.multi_node_queue_split_mut();
            for task_id in multi_node_ready_tasks {
                if let Some(task) = task_map.find_task(task_id) {
                    multi_node_queue.add_task(task)
                }
            }
        }

        self.try_start_multinode_tasks(core);

        let ready_tasks = core.take_single_node_ready_to_assign();
        if !ready_tasks.is_empty() {
            let try_prev_worker = ready_tasks.len() < MAX_TASKS_FOR_TRY_PREV_WORKER_HEURISTICS;
            let has_parked_resources = core.has_parked_resources();
            for task_id in ready_tasks.into_iter() {
                let worker_id = {
                    if has_parked_resources {
                        let wakeup = if let Some(task) = core.find_task(task_id) {
                            core.check_parked_resources(&task.configuration.resources)
                        } else {
                            continue;
                        };
                        if wakeup {
                            core.wakeup_parked_resources()
                        }
                    }

                    if let Some(task) = core.find_task(task_id) {
                        self.choose_worker_for_task(
                            task,
                            core.task_map(),
                            core.get_worker_map(),
                            try_prev_worker,
                        )
                    } else {
                        continue;
                    }
                    //log::debug!("Task {} initially assigned to {}", task.id, worker_id);
                };
                if let Some(worker_id) = worker_id {
                    debug_assert!(core
                        .get_worker_map()
                        .get_worker(worker_id)
                        .is_capable_to_run(
                            &core.get_task(task_id).configuration.resources,
                            self.now
                        ));
                    self.assign(core, task_id, worker_id);
                } else {
                    core.add_sleeping_sn_task(task_id);
                }
            }
        }

        let has_underload_workers = core
            .get_workers()
            .any(|w| !w.is_parked() && w.is_underloaded());

        log::debug!("Scheduling finished");
        has_underload_workers
    }

    pub fn balance(&mut self, core: &mut Core) {
        // This method is called only if at least one worker is underloaded

        log::debug!("Balancing started");

        let mut balanced_tasks: Vec<TaskId> = Vec::new();
        let mut min_resource = ResourceRequestLowerBound::new();
        let now = Instant::now();

        {
            let (tasks, workers) = core.split_tasks_workers_mut();
            for worker in workers.values() {
                let mut offered = 0;
                let not_overloaded = !worker.is_overloaded();
                for &task_id in worker.sn_tasks() {
                    let task = tasks.get_task_mut(task_id);
                    if task.is_sn_running()
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
                        worker.sn_tasks().len()
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
                    worker.sn_tasks().len()
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
                        worker
                            .resources
                            .difficulty_score(&task.configuration.resources),
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
            if core.get_workers().any(|w| w.is_overloaded()) {
                core.park_workers();
            }
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
                        let request = &task.configuration.resources;
                        if !worker.is_capable_to_run(request, now) {
                            continue;
                        }
                        let worker2_id = task.get_assigned_worker().unwrap();
                        let worker2 = core.get_worker_by_id_or_panic(worker2_id);

                        if !worker2.is_overloaded()
                            || worker.load_wrt_request(request) > worker2.load_wrt_request(request)
                        {
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
