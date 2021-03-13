use crate::common::{Map, Set};
use crate::scheduler::metrics::NodeMetrics;
use crate::scheduler::protocol::{
    NewFinishedTaskInfo, TaskInfo, TaskUpdate, TaskUpdateType, WorkerInfo,
};
use crate::scheduler::task::{OwningTaskRef, SchedulerTaskState, Task, TaskRef};
use crate::scheduler::worker::{HostnameId, Worker, WorkerRef};
use crate::scheduler::{TaskAssignment, ToSchedulerMessage};
use crate::{WorkerId, TaskId};

#[derive(Debug)]
pub struct SchedulerGraph {
    pub network_bandwidth: f32,
    pub workers: Map<WorkerId, WorkerRef>,
    pub tasks: Map<TaskId, OwningTaskRef>,
    pub ready_to_assign: Vec<TaskRef>,
    pub new_tasks: Vec<TaskRef>,
    pub hostnames: Map<String, HostnameId>,
}

impl Default for SchedulerGraph {
    fn default() -> Self {
        Self {
            network_bandwidth: 100.0,
            workers: Default::default(),
            tasks: Default::default(),
            ready_to_assign: Default::default(),
            new_tasks: Default::default(),
            hostnames: Default::default(),
        }
    }
}

impl SchedulerGraph {
    pub fn get_task(&self, task_id: TaskId) -> &TaskRef {
        self.tasks
            .get(&task_id)
            .unwrap_or_else(|| panic!("Task {} not found", task_id))
    }
    pub fn get_worker(&self, worker_id: WorkerId) -> &WorkerRef {
        self.workers
            .get(&worker_id)
            .unwrap_or_else(|| panic!("Worker {} not found", worker_id))
    }

    pub fn handle_message(&mut self, message: ToSchedulerMessage) {
        match message {
            ToSchedulerMessage::TaskUpdate(tu) => self.update_task(tu),
            ToSchedulerMessage::NewWorker(wi) => self.add_worker(wi),
            ToSchedulerMessage::NewTasks(ts) => {
                for ti in ts {
                    self.add_task(ti);
                }
            }
            ToSchedulerMessage::NewFinishedTask(ti) => self.add_finished_task(ti),
            ToSchedulerMessage::RemoveTask(task_id) => self.remove_task(task_id),
            ToSchedulerMessage::NetworkBandwidth(bandwidth) => self.network_bandwidth = bandwidth,
            _ => { /* Ignore */ }
        }
    }

    pub fn add_task(&mut self, ti: TaskInfo) {
        log::debug!("New task {} #inputs={}", ti.id, ti.inputs.len());
        let task_id = ti.id;
        let inputs: Vec<_> = ti
            .inputs
            .iter()
            .map(|id| self.tasks.get(id).unwrap().clone())
            .collect();
        let task = OwningTaskRef::new(ti, inputs);
        if task.get().is_ready() {
            log::debug!("Task {} is ready", task_id);
            self.ready_to_assign.push(task.clone());
        }
        self.new_tasks.push(task.clone());
        assert!(self.tasks.insert(task_id, task).is_none());
    }

    pub fn add_finished_task(&mut self, tis: Vec<NewFinishedTaskInfo>) {
        for ti in tis {
            let placement: Set<WorkerRef> = ti
                .workers
                .iter()
                .map(|id| self.get_worker(*id).clone())
                .collect();
            let task_id = ti.id;
            let task = OwningTaskRef::new_finished(ti, placement);
            assert!(self.tasks.insert(task_id, task).is_none());
        }
    }

    pub fn remove_task(&mut self, task_id: TaskId) {
        {
            let task_ref = self.get_task(task_id);
            let mut task = task_ref.get_mut();

            let assigned_wr = task.assigned_worker.take();
            if let Some(wr) = assigned_wr {
                assert!(wr.get_mut().tasks.remove(&task_ref));
            }
            task.placement = Default::default();
            for tr in &task.consumers {
                let mut t = tr.get_mut();
                t.inputs = Default::default();
            }
            task.consumers = Default::default();
        }

        assert!(self.tasks.remove(&task_id).is_some());
    }
    pub fn update_task(&mut self, tu: TaskUpdate) {
        match tu.state {
            TaskUpdateType::Placed => self.place_task_on_worker(tu.id, tu.worker),
            TaskUpdateType::Removed => self.remove_task_from_worker(tu.id, tu.worker),
            TaskUpdateType::Finished => {
                self.finish_task(tu.id, tu.worker, tu.size.unwrap());
            }
        }
    }

    pub fn add_worker(&mut self, wi: WorkerInfo) {
        let hostname_id = self.get_hostname_id(&wi.hostname);
        assert!(self
            .workers
            .insert(wi.id, WorkerRef::new(wi, hostname_id),)
            .is_none());
    }

    /// Finishes a task.
    /// Returns the assigned worker of the task and a boolean which states if at least a single
    /// consumer changed to a ready state after this finish.
    pub fn finish_task(
        &mut self,
        task_id: TaskId,
        worker_id: WorkerId,
        size: u64,
    ) -> (bool, WorkerRef) {
        let tref = self.get_task(task_id).clone();
        let mut task = tref.get_mut();
        log::debug!("Task id={} is finished on worker={}", task.id, worker_id);

        assert!(!task.is_finished() && task.is_ready());
        task.state = SchedulerTaskState::Finished;
        task.size = size;
        let assigned_wr = {
            let assigned_wr = task.assigned_worker.take().unwrap();
            assert!(assigned_wr.get_mut().tasks.remove(&tref));
            assigned_wr
        };

        let worker = self.get_worker(worker_id).clone();
        for tref in &task.inputs {
            let mut t = tref.get_mut();
            t.placement.insert(worker.clone());
            t.remove_future_placement(&assigned_wr);
        }

        let mut ready_consumer = false;
        for tref in &task.consumers {
            let mut t = tref.get_mut();
            if t.unfinished_deps <= 1 {
                assert!(t.unfinished_deps > 0);
                assert!(t.is_waiting());
                t.unfinished_deps = 0;
                log::debug!("Task {} is ready", t.id);
                self.ready_to_assign.push(tref.clone());
                ready_consumer = true;
            } else {
                t.unfinished_deps -= 1;
            }
        }
        task.placement.insert(worker);
        (ready_consumer, assigned_wr)
    }

    pub fn place_task_on_worker(&mut self, task_id: TaskId, worker_id: WorkerId) {
        let tref = self.get_task(task_id).clone();
        let mut task = tref.get_mut();
        let worker = self.get_worker(worker_id).clone();
        assert!(task.is_finished());
        task.placement.insert(worker);
    }

    pub fn remove_task_from_worker(&mut self, task_id: TaskId, worker_id: WorkerId) {
        let mut task = self.get_task(task_id).get_mut();
        let worker = self.get_worker(worker_id);
        if !task.placement.remove(worker) {
            panic!(
                "Worker {} removes task {}, but it was not there.",
                worker.get().id,
                task.id
            );
        }
    }

    pub fn get_hostname_id(&mut self, hostname: &str) -> HostnameId {
        let new_id = self.hostnames.len() as HostnameId;
        *self.hostnames.entry(hostname.to_owned()).or_insert(new_id)
    }

    pub fn sanity_check(&self) {
        for (id, tr) in &self.tasks {
            let task = tr.get();
            assert_eq!(task.id, *id);
            task.sanity_check(&tr);
            if let Some(w) = &task.assigned_worker {
                assert!(self.workers.contains_key(&w.get().id));
            }
        }

        for wr in self.workers.values() {
            let worker = wr.get();
            worker.sanity_check(&wr);
        }
    }
}

pub fn create_task_assignment<M: NodeMetrics>(task: &mut Task, worker: WorkerId) -> TaskAssignment {
    if task.is_fresh() {
        /* Mainly because to not changed AssignedPinned to Assigned */
        task.state = SchedulerTaskState::Assigned;
    }
    TaskAssignment {
        task: task.id,
        worker,
        priority: M::SORT_MULTIPLIER * task.computed_metric,
    }
}
pub fn assign_task_to_worker(
    task: &mut Task,
    task_ref: TaskRef,
    worker: &mut Worker,
    worker_ref: WorkerRef,
) {
    let assigned_worker = &task.assigned_worker;
    if let Some(wr) = assigned_worker {
        assert!(!wr.eq(&worker_ref));
        let mut previous_worker = wr.get_mut();
        assert!(previous_worker.tasks.remove(&task_ref));
    }
    for tr in &task.inputs {
        let mut t = tr.get_mut();
        if let Some(wr) = assigned_worker {
            t.remove_future_placement(wr);
        }
        t.set_future_placement(worker_ref.clone());
    }
    task.assigned_worker = Some(worker_ref);
    assert!(worker.tasks.insert(task_ref));
}

#[cfg(test)]
mod tests {
    use crate::scheduler::graph::{assign_task_to_worker, SchedulerGraph};
    use crate::scheduler::protocol::{TaskInfo, WorkerInfo};
    use crate::scheduler::{TaskId, WorkerId};

    #[test]
    fn set_consumers_on_tasks() {
        let mut graph = SchedulerGraph::default();
        add_task(&mut graph, 0, vec![]);
        add_task(&mut graph, 1, vec![0]);
        add_task(&mut graph, 2, vec![0]);
        add_task(&mut graph, 3, vec![1, 2]);

        let get_consumers = |tid| -> Vec<TaskId> {
            let mut ids = graph
                .get_task(tid)
                .get()
                .consumers
                .iter()
                .map(|t| t.get().id)
                .collect::<Vec<_>>();
            ids.sort();
            ids
        };

        assert_eq!(get_consumers(0), vec!(1, 2));
        assert_eq!(get_consumers(1), vec!(3));
        assert_eq!(get_consumers(2), vec!(3));
        assert_eq!(get_consumers(3), vec!() as Vec<TaskId>);
    }

    #[test]
    #[should_panic]
    fn finish_unassigned_task() {
        let mut graph = SchedulerGraph::default();
        add_worker(&mut graph, 0);
        add_task(&mut graph, 0, vec![]);
        graph.finish_task(0, 0, 0);
    }

    #[test]
    fn reset_inputs_after_delete() {
        let mut graph = SchedulerGraph::default();
        add_worker(&mut graph, 0);
        add_task(&mut graph, 0, vec![]);
        add_task(&mut graph, 1, vec![]);
        add_task(&mut graph, 2, vec![]);

        assign(&mut graph, 0, 0);
        graph.finish_task(0, 0, 0);
        graph.remove_task(0);

        assert!(graph.get_task(1).get().inputs.is_empty());
        assert!(graph.get_task(2).get().inputs.is_empty());
    }

    #[test]
    fn assign_task() {
        let mut graph = SchedulerGraph::default();
        add_worker(&mut graph, 0);
        add_task(&mut graph, 0, vec![]);

        assign(&mut graph, 0, 0);
        assert_eq!(
            graph.get_task(0).get().assigned_worker,
            Some(graph.get_worker(0).clone())
        );
        assert_eq!(
            graph
                .get_worker(0)
                .get()
                .tasks
                .iter()
                .map(|t| t.get().id)
                .collect::<Vec<_>>(),
            vec!(0)
        );
    }

    #[test]
    fn reassign_task() {
        let mut graph = SchedulerGraph::default();
        add_worker(&mut graph, 0);
        add_worker(&mut graph, 1);
        add_task(&mut graph, 0, vec![]);

        assign(&mut graph, 0, 0);
        assert!(graph.get_worker(0).get().tasks.contains(graph.get_task(0)));
        assign(&mut graph, 0, 1);
        assert!(!graph.get_worker(0).get().tasks.contains(graph.get_task(0)));
        assert!(graph.get_worker(1).get().tasks.contains(graph.get_task(0)));
    }

    #[test]
    fn set_placement() {
        let mut graph = SchedulerGraph::default();
        add_worker(&mut graph, 0);
        add_task(&mut graph, 0, vec![]);
        add_task(&mut graph, 1, vec![0]);

        assign(&mut graph, 0, 0);
        graph.finish_task(0, 0, 0);
        assign(&mut graph, 1, 0);
        assert_eq!(
            graph
                .get_task(0)
                .get()
                .future_placement
                .get(graph.get_worker(0)),
            Some(&1)
        );
        graph.finish_task(1, 0, 0);
        assert!(graph
            .get_task(1)
            .get()
            .placement
            .contains(graph.get_worker(0)));
    }

    #[test]
    fn mark_assignable_tasks() {
        let mut graph = SchedulerGraph::default();
        add_worker(&mut graph, 0);
        add_task(&mut graph, 0, vec![]);
        add_task(&mut graph, 1, vec![0]);

        assert_eq!(graph.ready_to_assign, vec!(graph.get_task(0).clone()));
        graph.ready_to_assign.clear();
        assign(&mut graph, 0, 0);
        graph.finish_task(0, 0, 0);
        assert_eq!(graph.ready_to_assign, vec!(graph.get_task(1).clone()));
    }

    #[test]
    fn intern_hostnames() {
        let mut graph = SchedulerGraph::default();
        graph.add_worker(WorkerInfo {
            id: 0,
            n_cpus: 0,
            hostname: "foo".to_string(),
        });
        graph.add_worker(WorkerInfo {
            id: 1,
            n_cpus: 0,
            hostname: "foo".to_string(),
        });
        graph.add_worker(WorkerInfo {
            id: 2,
            n_cpus: 0,
            hostname: "bar".to_string(),
        });
        assert_eq!(
            graph.get_worker(0).get().hostname_id,
            graph.get_worker(1).get().hostname_id
        );
        assert_ne!(
            graph.get_worker(0).get().hostname_id,
            graph.get_worker(2).get().hostname_id
        );
    }

    fn assign(graph: &mut SchedulerGraph, task_id: TaskId, worker_id: WorkerId) {
        let tref = graph.get_task(task_id);
        let wref = graph.get_worker(worker_id);
        assign_task_to_worker(
            &mut tref.get_mut(),
            tref.clone(),
            &mut wref.get_mut(),
            wref.clone(),
        );
    }

    fn add_worker(graph: &mut SchedulerGraph, id: WorkerId) {
        graph.add_worker(WorkerInfo {
            id,
            n_cpus: 0,
            hostname: "".to_string(),
        });
    }
    fn add_task(graph: &mut SchedulerGraph, id: TaskId, inputs: Vec<TaskId>) {
        graph.add_task(TaskInfo { id, inputs });
    }
}
