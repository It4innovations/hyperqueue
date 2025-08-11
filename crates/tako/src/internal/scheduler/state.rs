use std::cell::{RefCell, RefMut};
use std::cmp::Reverse;
use std::rc::Rc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::time::sleep;

use crate::internal::common::Map;
use crate::internal::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::internal::scheduler::multinode::MultiNodeAllocator;
use crate::internal::server::comm::{Comm, CommSender, CommSenderRef};
use crate::internal::server::core::{Core, CoreRef};
use crate::internal::server::dataobjmap::DataObjectMap;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::worker::Worker;
use crate::internal::server::workerload::ResourceRequestLowerBound;
use crate::internal::server::workermap::WorkerMap;
use crate::{TaskId, WorkerId};

// Long duration - 1 year
const LONG_DURATION: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

// Knobs
const MAX_TASKS_FOR_IMMEDIATE_RUN_CHECK: usize = 600;

pub struct SchedulerState {
    // Which tasks has modified state, this map holds the original state
    dirty_tasks: Map<TaskId, TaskRuntimeState>,

    last_idx: usize,
    now: std::time::Instant,
}

pub(crate) async fn scheduler_loop(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    scheduler_wakeup: Rc<Notify>,
    minimum_delay: Duration,
) {
    let mut last_schedule = Instant::now().checked_sub(minimum_delay * 2).unwrap();
    loop {
        scheduler_wakeup.notified().await;
        if !comm_ref.get().get_scheduling_flag() {
            last_schedule = Instant::now();
            continue;
        }
        let mut now = Instant::now();
        let since_last_schedule = now - last_schedule;
        if minimum_delay > since_last_schedule {
            sleep(minimum_delay - since_last_schedule).await;
            now = Instant::now();
        }
        let mut comm = comm_ref.get_mut();
        if !comm.get_scheduling_flag() {
            last_schedule = now;
            continue;
        }
        let mut core = core_ref.get_mut();
        run_scheduling_now(&mut core, &mut comm, now);
        last_schedule = Instant::now();
    }
}

pub fn run_scheduling_now(core: &mut Core, comm: &mut CommSender, now: Instant) {
    let mut state = SchedulerState::new(now);
    state.run_scheduling(core, &mut *comm);
    comm.reset_scheduling_flag();
}

impl SchedulerState {
    pub fn new(now: std::time::Instant) -> Self {
        SchedulerState {
            dirty_tasks: Map::new(),
            now,
            last_idx: 0,
        }
    }

    /// Selects a worker that is able to execute the given task.
    /// If no worker is available, returns `None`.
    fn choose_worker_for_task<'a>(
        &mut self,
        task: &Task,
        workers: &'a [RefCell<&'a mut Worker>],
        dataobj_map: &DataObjectMap,
        try_immediate_check: bool,
    ) -> Option<RefMut<'a, &'a mut Worker>> {
        let no_data_deps = task.data_deps.is_empty();
        if no_data_deps && try_immediate_check {
            if workers[self.last_idx]
                .borrow_mut()
                .have_immediate_resources_for_rqv_now(&task.configuration.resources, self.now)
            {
                return Some(workers[self.last_idx].borrow_mut());
            }
            for (idx, worker) in workers.iter().enumerate() {
                if idx == self.last_idx {
                    continue;
                }
                if worker
                    .borrow()
                    .have_immediate_resources_for_rqv_now(&task.configuration.resources, self.now)
                {
                    self.last_idx = idx;
                    return Some(worker.borrow_mut());
                }
            }
        }
        let start_idx = self.last_idx + 1;
        if no_data_deps {
            for (idx, worker) in workers[start_idx..].iter().enumerate() {
                if worker
                    .borrow()
                    .is_capable_to_run_rqv(&task.configuration.resources, self.now)
                {
                    self.last_idx = idx + start_idx;
                    return Some(worker.borrow_mut());
                }
            }
            for (idx, worker) in workers[..start_idx].iter().enumerate() {
                if worker
                    .borrow()
                    .is_capable_to_run_rqv(&task.configuration.resources, self.now)
                {
                    self.last_idx = idx;
                    return Some(worker.borrow_mut());
                }
            }
            None
        } else {
            let mut best_worker = None;
            let mut best_cost = u64::MAX;
            let mut best_idx = 0;

            for (idx, worker) in workers[start_idx..].iter().enumerate() {
                let worker = worker.borrow_mut();
                if !worker.is_capable_to_run_rqv(&task.configuration.resources, self.now) {
                    continue;
                }
                let cost = compute_transfer_cost(dataobj_map, task, worker.id);
                if cost >= best_cost {
                    continue;
                }
                best_cost = cost;
                best_worker = Some(worker);
                best_idx = start_idx + idx;
            }
            for (idx, worker) in workers[..start_idx].iter().enumerate() {
                let worker = worker.borrow_mut();
                if !worker.is_capable_to_run_rqv(&task.configuration.resources, self.now) {
                    continue;
                }
                let cost = compute_transfer_cost(dataobj_map, task, worker.id);
                if cost >= best_cost {
                    continue;
                }
                best_cost = cost;
                best_worker = Some(worker);
                best_idx = idx;
            }
            if let Some(worker) = best_worker {
                self.last_idx = best_idx;
                Some(worker)
            } else {
                None
            }
        }
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
                        worker.set_reservation(true);
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
                    panic!("Invalid task state, new = {new:?}, old = {old:?}");
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
                    worker.set_reservation(false);
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

    // This function assumes that potential removal of an assigned is already done
    fn assign_into(&mut self, task: &mut Task, worker: &mut Worker) {
        worker.insert_sn_task(task);
        let new_state = match task.state {
            TaskRuntimeState::Waiting(_) => TaskRuntimeState::Assigned(worker.id),
            TaskRuntimeState::Assigned(old_w) => {
                if task.is_fresh() {
                    TaskRuntimeState::Assigned(worker.id)
                } else {
                    TaskRuntimeState::Stealing(old_w, Some(worker.id))
                }
            }
            TaskRuntimeState::Stealing(from_w, _) => {
                TaskRuntimeState::Stealing(from_w, Some(worker.id))
            }
            TaskRuntimeState::Running { .. }
            | TaskRuntimeState::RunningMultiNode(_)
            | TaskRuntimeState::Finished => {
                panic!("Invalid state {:?}", task.state);
            }
        };
        let old_state = std::mem::replace(&mut task.state, new_state);
        self.dirty_tasks.entry(task.id).or_insert(old_state);
    }

    pub fn assign(&mut self, core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
        let (tasks, workers) = core.split_tasks_workers_mut();
        let task = tasks.get_task_mut(task_id);
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
        self.assign_into(task, workers.get_worker_mut(worker_id));
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
        loop {
            // "while let" not used because of lifetime problems
            let (mn_queue, task_map, worker_map, worker_groups) = core.multi_node_queue_split_mut();
            let allocator =
                MultiNodeAllocator::new(mn_queue, task_map, worker_map, worker_groups, self.now);
            if let Some((task_id, workers)) = allocator.try_allocate_task() {
                let task = task_map.get_task_mut(task_id);
                self.assign_multinode(worker_map, task, workers);
                continue;
            } else {
                return;
            }
        }
    }

    /// Returns true if balancing is needed.
    fn schedule_available_tasks(&mut self, core: &mut Core) -> bool {
        if !core.has_workers() {
            return false;
        }
        log::debug!("Scheduling started");

        self.try_start_multinode_tasks(core);

        let ready_tasks = core.take_single_node_ready_to_assign();
        let mut sleeping_tasks = Vec::new();
        if !ready_tasks.is_empty() {
            if core.has_parked_resources() {
                for task_id in &ready_tasks {
                    let Some(task) = core.find_task(*task_id) else {
                        continue;
                    };
                    if core.check_parked_resources(&task.configuration.resources) {
                        core.wakeup_parked_resources();
                        break;
                    }
                }
            }
            let (tasks, workers, dataobjs) = core.split_tasks_workers_dataobjs_mut();
            let mut workers = workers
                .values_mut()
                .filter(|w| !w.is_parked())
                .map(RefCell::new)
                .collect::<Vec<_>>();
            workers.sort_unstable_by_key(|w| w.borrow().id);
            self.last_idx %= workers.len();
            for (idx, task_id) in ready_tasks.into_iter().enumerate() {
                let Some(task) = tasks.find_task_mut(task_id) else {
                    continue;
                };
                if let Some(mut worker) = self.choose_worker_for_task(
                    task,
                    &workers,
                    dataobjs,
                    idx < MAX_TASKS_FOR_IMMEDIATE_RUN_CHECK,
                ) {
                    self.assign_into(task, &mut worker);
                } else {
                    sleeping_tasks.push(task_id);
                }
            }
        }

        core.add_sleeping_sn_tasks(sleeping_tasks);

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
                if !worker.is_overloaded() {
                    continue;
                }
                for &task_id in worker.sn_tasks() {
                    let task = tasks.get_task_mut(task_id);
                    if task.is_sn_running() {
                        continue;
                    }
                    task.set_take_flag(false);
                    min_resource.include_rqv(&task.configuration.resources);
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

        log::debug!("Min resources {min_resource:?}");

        let mut underload_workers = Vec::new();
        let task_map = core.task_map();
        let dataobj_map = core.dataobj_map();
        for worker in core.get_workers() {
            // We could here also test park flag, but it is already solved in the next condition
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
                    let mut cost = compute_transfer_cost(dataobj_map, task, worker.id);
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
                            .difficulty_score_of_rqv(&task.configuration.resources),
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

        /* Micro heuristic, give small priority to workers with fewer tasks and with less remaining time */
        underload_workers
            .sort_unstable_by_key(|(_, ts, _, remaining_time)| (ts.len(), *remaining_time));

        /* Iteration 1 - try to assign tasks so workers are no longer underutilized */
        /* This should be always relatively quick even with millions of tasks and many workers,
          since we require that each task need at least one core, and we expect that number
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
                        if !worker.has_time_to_run_for_rqv(&task.configuration.resources, now) {
                            continue;
                        }
                        if !worker.have_immediate_resources_for_rqv(&task.configuration.resources) {
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
                        if !worker.is_capable_to_run_rqv(request, now) {
                            continue;
                        }
                        let worker2_id = task.get_assigned_worker().unwrap();
                        let worker2 = core.get_worker_by_id_or_panic(worker2_id);

                        if !worker2.is_overloaded()
                            || worker.load_wrt_rqv(request) > worker2.load_wrt_rqv(request)
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

fn compute_transfer_cost(dataobj_map: &DataObjectMap, task: &Task, worker_id: WorkerId) -> u64 {
    let mut cost = 0;
    // TODO: When task become ready we can sort data deps by size, first n-th task is really most relevant
    // but we need need remember the original order somewhere

    if task.data_deps.len() > 32 {
        for data_id in task.data_deps.iter().step_by(task.data_deps.len() / 16) {
            let data_obj = dataobj_map.get_data_object(*data_id);
            if !data_obj.placement().contains(&worker_id) {
                cost += data_obj.size();
            }
        }
    } else {
        for data_id in task.data_deps.iter() {
            let data_obj = dataobj_map.get_data_object(*data_id);
            if !data_obj.placement().contains(&worker_id) {
                cost += data_obj.size();
            }
        }
    }
    cost
}
