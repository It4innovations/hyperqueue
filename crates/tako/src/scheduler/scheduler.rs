use crate::server::core::{CoreRef, Core};
use crate::server::comm::{CommSenderRef, Comm};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use super::utils::task_transfer_cost;
use crate::server::task::{TaskRuntimeState, Task, TaskRef};
use std::cmp::Ordering;
use rand::seq::SliceRandom;
use rand::rngs::SmallRng;
use crate::common::Map;
use crate::{WorkerId, TaskId};
use crate::server::worker::Worker;
use super::metrics::compute_b_level_metric;
use rand::SeedableRng;
use crate::messages::worker::{ToWorkerMessage, TaskIdsMsg};
use crate::common::trace::trace_task_assign;
use tokio::sync::Notify;
use std::rc::Rc;

pub struct SchedulerState {

    // Which tasks has modified state, this map holds the original state
    dirty_tasks: Map<TaskRef, TaskRuntimeState>,

    random: SmallRng,
}

fn choose_worker_for_task(
    task: &Task,
    worker_map: &Map<WorkerId, Worker>,
    random: &mut SmallRng,
) -> WorkerId {
    let mut costs = std::u64::MAX;
    let mut workers = Vec::with_capacity(worker_map.len());
    for wr in worker_map.keys() {
        let c = task_transfer_cost(task, *wr);
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


pub async fn scheduler_loop(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    scheduler_wakeup: Rc<Notify>,
    minimum_delay: Duration
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
    fn run_scheduling(&mut self, core: &mut Core, comm: &mut impl Comm) {
        if self.schedule_available_tasks(core) {
            trace_time!("scheduler", "balance", self.balance(core));
        }
        self.finish_scheduling(comm);
    }

    pub fn finish_scheduling(&mut self, comm: &mut impl Comm) {
        let mut task_steals: Map<WorkerId, Vec<TaskId>> = Default::default();
        let mut task_computes: Map<WorkerId, Vec<TaskRef>> = Default::default();
        for (task_ref, old_state) in self.dirty_tasks.drain() {
            let task = task_ref.get();
            match (&task.state, old_state) {
                (TaskRuntimeState::Assigned(w_id), TaskRuntimeState::Waiting(winfo)) => {
                    debug_assert_eq!(winfo.unfinished_deps, 0);
                    debug_assert!(task.is_fresh());
                    task_computes.entry(*w_id).or_default().push(task_ref.clone())
                },
                (TaskRuntimeState::Stealing(from_id, to_id), TaskRuntimeState::Assigned(w_id)) => {
                    debug_assert_eq!(*from_id, w_id);
                    debug_assert!(!task.is_fresh());
                    task_steals.entry(w_id).or_default().push(task.id);
                }
                (TaskRuntimeState::Stealing(from_w1, to_w1), TaskRuntimeState::Stealing(from_w2, to_w2)) => {
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

    for (worker_id, mut task_refs) in task_computes {
        task_refs.sort_unstable_by_key(|tr| {
            let t = tr.get();
            (-t.user_priority, -t.scheduler_priority)
        });
        for task_ref in task_refs {
            let mut task = task_ref.get_mut();
            task.set_fresh_flag(false);
            trace_task_assign(task.id, worker_id);
            comm.send_worker_message(worker_id, &task.make_compute_message());
        }
    }

   }

    pub fn assign(
        &mut self,
        core: &mut Core,
        task: &mut Task,
        task_ref: TaskRef,
        worker_id: WorkerId,
    ) {
        //assign_task_to_worker(task, task_ref, worker, worker_ref);

        let assigned_worker = task.get_assigned_worker();
        if let Some(w_id) = assigned_worker {
            assert_ne!(w_id, worker_id);
            let previous_worker = core.get_worker_mut_by_id_or_panic(w_id);
            assert!(previous_worker.tasks.remove(&task_ref));
        }
        for tr in &task.inputs {
            let mut t = tr.get_mut();
            if let Some(wr) = assigned_worker {
                t.remove_future_placement(wr);
            }
            t.set_future_placement(worker_id);
        }
        core.get_worker_mut_by_id_or_panic(worker_id).tasks.insert(task_ref.clone());
        let new_state = match task.state {
            TaskRuntimeState::Waiting(_) => TaskRuntimeState::Assigned(worker_id),
            TaskRuntimeState::Assigned(old_w) => {
                if task.is_fresh() {
                    TaskRuntimeState::Assigned(worker_id)
                } else {
                    TaskRuntimeState::Stealing(old_w, worker_id)
                }
            },
            TaskRuntimeState::Stealing(from_w, to_w) => TaskRuntimeState::Stealing(from_w, worker_id),
            TaskRuntimeState::Running(_) |
            TaskRuntimeState::Finished(_) |
            TaskRuntimeState::Released => { panic!("Invalid state"); }
        };
        let old_state = std::mem::replace(&mut task.state, new_state);
        self.dirty_tasks.entry(task_ref).or_insert(old_state);
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
                compute_b_level_metric(core.get_task_map())
            });
        }

        for tr in core.take_ready_to_assign().drain(..) {
            let mut task = tr.get_mut();
            assert!(task.is_waiting());
            let worker_id = choose_worker_for_task(&task, core.get_worker_map(), &mut self.random);
            log::debug!("Task {} initially assigned to {}", task.id, worker_id);
            self.assign(
                core,
                &mut task,
                tr.clone(),
                worker_id
            );
        }

        let has_underload_workers = core.get_workers().any(|w| {
            w.is_underloaded()
        });

        log::debug!("Scheduling finished");
        has_underload_workers
    }

    pub fn balance(&mut self, core: &mut Core) {
        log::debug!("Balancing started");
        let mut balanced_tasks = Vec::new();
        for worker in core.get_workers() {
            let len = worker.tasks.len() as u32;
            if len > worker.ncpus {
                log::debug!("Worker {} offers {} tasks", worker.id, len);
                for tr in &worker.tasks {
                    let mut task = tr.get_mut();
                    if task.is_running() {
                        continue;
                    }
                    task.set_take_flag(false);
                    balanced_tasks.push(tr.clone());
                }
            }
        }

        let mut underload_workers = Vec::new();
        for worker in core.get_workers() {
            let len = worker.tasks.len() as u32;
            if len < worker.ncpus {
                log::debug!("Worker {} is underloaded ({} tasks)", worker.id, len);
                let mut ts = balanced_tasks.clone();
                ts.sort_by_cached_key(|tr| {
                    let task = tr.get();
                    let mut cost = task_transfer_cost(&task, worker.id);
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
                underload_workers.push((worker.id, worker.tasks.len(), ts));
            }
        }
        underload_workers.sort_by_key(|x| x.1);

        let mut n_tasks = core.get_worker_by_id_or_panic(underload_workers[0].0).tasks.len();
        loop {
            let mut change = false;
            for (worker_id, initial_n_tasks, ts) in underload_workers.iter_mut() {
                if *initial_n_tasks > n_tasks {
                    break;
                }
                if ts.is_empty() {
                    continue;
                }

                while let Some(tr) = ts.pop() {
                    let mut task = tr.get_mut();
                    if task.is_taken() {
                        continue;
                    }
                    task.set_take_flag(true);
                    let old_worker_id = {
                        let worker2_id = task.get_assigned_worker().unwrap();
                        let worker2 = core.get_worker_mut_by_id_or_panic(worker2_id);
                        let worker2_n_tasks = worker2.tasks.len();
                        if worker2_n_tasks <= n_tasks || worker2_n_tasks <= worker2.ncpus as usize {
                            continue;
                        }
                        worker2_id
                    };
                    log::debug!(
                        "Changing assignment of task={} from worker={} to worker={}",
                        task.id,
                        old_worker_id,
                        worker_id
                    );
                    self.assign(
                        core,
                        &mut task,
                        tr.clone(),
                        *worker_id,
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
}


#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn create_test_scheduler() -> SchedulerState {
        SchedulerState {
            dirty_tasks: Default::default(),
            random: SmallRng::from_seed([0, 0, 0, 0, 0, 0, 0, 0,0, 0, 0, 0,0, 0, 0, 0,]),
        }
    }

    use crate::server::core::{Core};
    use crate::server::test_util::{create_test_workers, task, submit_test_tasks, create_test_comm, finish_on_worker, submit_example_1, start_and_finish_on_worker};
    use crate::common::Set;
    use crate::server::reactor::on_steal_response;
    use crate::messages::worker::{StealResponseMsg, StealResponse};

    #[test]
    fn test_no_deps_distribute() {
        let mut core = Core::default();
        create_test_workers(&mut core, &[2, 2, 2]);

        assert_eq!(core.get_worker_map().len(), 3);
        for w in core.get_workers() {
            assert!(w.is_underloaded());
        }

        let mut active_ids : Set<TaskId> = (1..301).collect();
        let tasks : Vec<TaskRef> = (1..301).map(|i| task(i)).collect();
        let task_refs : Vec<&TaskRef> = tasks.iter().collect();
        submit_test_tasks(&mut core, &task_refs);

        let mut scheduler = create_test_scheduler();
        let mut comm = create_test_comm();
        scheduler.run_scheduling(&mut core, &mut comm);

        let m1 = comm.take_worker_msgs(100, 0);
        let m2 = comm.take_worker_msgs(101, 0);
        let m3 = comm.take_worker_msgs(102, 0);
        comm.emptiness_check();

        assert_eq!(m1.len() + m2.len() + m3.len(), 300);
        assert!(m1.len() >= 10);
        assert!(m2.len() >= 10);
        assert!(m3.len() >= 10);

        for w in core.get_workers() {
            assert!(!w.is_underloaded());
        }

        let mut finish_all = |core: &mut Core, msgs, worker_id| {
            for m in msgs {
                match m {
                   ToWorkerMessage::ComputeTask(cm) => {
                       assert!(active_ids.remove(&cm.id));
                       finish_on_worker(core, cm.id, worker_id, 1000);
                   },
                   _ => unreachable!()
                };
            }
        };

        finish_all(&mut core, m1, 100);
        finish_all(&mut core, m3, 102);

        assert!(core.get_worker_by_id_or_panic(100).is_underloaded());
        assert!(!core.get_worker_by_id_or_panic(101).is_underloaded());
        assert!(core.get_worker_by_id_or_panic(102).is_underloaded());

        scheduler.run_scheduling(&mut core, &mut comm);

        assert!(!core.get_worker_by_id_or_panic(100).is_underloaded());
        assert!(!core.get_worker_by_id_or_panic(101).is_underloaded());
        assert!(!core.get_worker_by_id_or_panic(102).is_underloaded());

        // TODO: Finish stealing

        let x1 = comm.take_worker_msgs(101, 1);

        let stealing = match &x1[0] {
                ToWorkerMessage::StealTasks(tasks) => { tasks.ids.clone() }
                _ => { unreachable!() }
        };

        comm.emptiness_check();

        on_steal_response(&mut core, &mut comm, 101, StealResponseMsg { responses: stealing.iter().map(|t| (*t, StealResponse::Ok)).collect() });

        let n1 = comm.take_worker_msgs(100, 0);
        let n3 = comm.take_worker_msgs(102, 0);

        assert!(n1.len() > 5);
        assert!(n3.len() > 5);
        assert_eq!(n1.len() + n3.len(), stealing.len());

        assert!(!core.get_worker_by_id_or_panic(100).is_underloaded());
        assert!(!core.get_worker_by_id_or_panic(101).is_underloaded());
        assert!(!core.get_worker_by_id_or_panic(102).is_underloaded());

        comm.emptiness_check();

        finish_all(&mut core, n1, 100);
        finish_all(&mut core, n3, 102);
        assert_eq!(active_ids.len(), core.get_worker_by_id_or_panic(101).tasks.len());

        comm.emptiness_check();
    }

    #[test]
    fn test_minimal_transfer_no_balance1() {
        /*11  12
           \  / \
           13   14

           11 - is big on W100
           12 - is small on W101
         */

        let mut core = Core::default();
        create_test_workers(&mut core, &[2, 2, 2]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100, 10000);
        start_and_finish_on_worker(&mut core, 12, 101, 1000);

        let mut scheduler = create_test_scheduler();
        let mut comm = create_test_comm();
        scheduler.run_scheduling(&mut core, &mut comm);

        let m1 = comm.take_worker_msgs(100, 1);
        let m2 = comm.take_worker_msgs(101, 1);

        assert_eq!(core.get_task_by_id_or_panic(13).get().get_assigned_worker().unwrap(), 100);
        assert_eq!(core.get_task_by_id_or_panic(14).get().get_assigned_worker().unwrap(), 101);

        comm.emptiness_check();
    }

    #[test]
    fn test_minimal_transfer_no_balance2() {
        /*11  12
           \  / \
           13   14

           11 - is small on W100
           12 - is big on W102
         */

        let mut core = Core::default();
        create_test_workers(&mut core, &[2, 2, 2]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100, 1000);
        start_and_finish_on_worker(&mut core, 12, 101, 10000);

        let mut scheduler = create_test_scheduler();
        let mut comm = create_test_comm();
        scheduler.run_scheduling(&mut core, &mut comm);

        comm.take_worker_msgs(101, 2);

        assert_eq!(core.get_task_by_id_or_panic(13).get().get_assigned_worker().unwrap(), 101);
        assert_eq!(core.get_task_by_id_or_panic(14).get().get_assigned_worker().unwrap(), 101);

        comm.emptiness_check();
    }


    #[test]
    fn test_minimal_transfer_after_balance() {
        /*11  12
           \  / \
           13   14

           11 - is on W100
           12 - is on W100
         */

        let mut core = Core::default();
        create_test_workers(&mut core, &[1, 1]);
        submit_example_1(&mut core);
        start_and_finish_on_worker(&mut core, 11, 100, 10000);
        start_and_finish_on_worker(&mut core, 12, 100, 10000);

        let mut scheduler = create_test_scheduler();
        let mut comm = create_test_comm();
        scheduler.run_scheduling(&mut core, &mut comm);

        dbg!(&comm);

        comm.take_worker_msgs(100, 1);
        comm.take_worker_msgs(101, 1);


        assert_eq!(core.get_task_by_id_or_panic(13).get().get_assigned_worker().unwrap(), 100);
        assert_eq!(core.get_task_by_id_or_panic(14).get().get_assigned_worker().unwrap(), 101);

        comm.emptiness_check();
    }

}