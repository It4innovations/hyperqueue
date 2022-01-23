use std::sync::Arc;
use std::time::Duration;

use orion::aead::SecretKey;
use tokio::task::JoinHandle;

use crate::common::resources::map::{ResourceIdAllocator, ResourceMap};
use crate::common::resources::{GenericResourceId, ResourceRequest};
use crate::common::{Map, Set, WrappedRcRefCell};
use crate::messages::gateway::ServerInfo;
use crate::server::rpc::ConnectionDescriptor;
use crate::server::task::{Task, TaskRuntimeState};
use crate::server::taskmap::TaskMap;
use crate::server::worker::Worker;
use crate::server::worker_load::WorkerResources;
use crate::server::workermap::WorkerMap;
use crate::{TaskId, WorkerId};

pub type CustomConnectionHandler = Box<dyn Fn(ConnectionDescriptor)>;

#[derive(Default)]
pub struct Core {
    tasks: TaskMap,
    workers: WorkerMap,

    /* Scheduler items */
    parked_resources: Set<WorkerResources>, // Resources of workers that has flag NOTHING_TO_LOAD
    // TODO: benchmark and possibly replace with a set
    ready_to_assign: Vec<TaskId>,
    has_new_tasks: bool,

    sleeping_tasks: Vec<TaskId>, // Tasks that cannot be scheduled to any available worker

    maximal_task_id: TaskId,
    worker_id_counter: u32,
    resource_map: ResourceIdAllocator,
    worker_listen_port: u16,

    idle_timeout: Option<Duration>,

    secret_key: Option<Arc<SecretKey>>,

    custom_conn_handler: Option<CustomConnectionHandler>,

    #[cfg(test)]
    rpc_handles: Vec<JoinHandle<()>>,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl CoreRef {
    pub fn new(
        worker_listen_port: u16,
        secret_key: Option<Arc<SecretKey>>,
        idle_timeout: Option<Duration>,
        custom_conn_handler: Option<CustomConnectionHandler>,
    ) -> Self {
        CoreRef::wrap(Core {
            worker_listen_port,
            secret_key,
            idle_timeout,
            custom_conn_handler,
            ..Default::default()
        })
    }
}

impl Core {
    #[inline]
    pub fn split_tasks_workers(&self) -> (&TaskMap, &WorkerMap) {
        (&self.tasks, &self.workers)
    }

    #[inline]
    pub fn split_tasks_workers_mut(&mut self) -> (&mut TaskMap, &mut WorkerMap) {
        (&mut self.tasks, &mut self.workers)
    }

    pub fn new_worker_id(&mut self) -> WorkerId {
        self.worker_id_counter += 1;
        WorkerId::new(self.worker_id_counter)
    }

    #[inline]
    pub fn is_used_task_id(&self, task_id: TaskId) -> bool {
        task_id <= self.maximal_task_id
    }

    pub fn idle_timeout(&self) -> &Option<Duration> {
        &self.idle_timeout
    }

    pub fn park_workers(&mut self) {
        for worker in self.workers.values_mut() {
            if worker.is_underloaded()
                && worker
                    .tasks()
                    .iter()
                    .all(|&task_id| self.tasks.get_task(task_id).is_running())
            {
                log::debug!("Parking worker {}", worker.id);
                worker.set_parked_flag(true);
                self.parked_resources.insert(worker.resources.clone());
            }
        }
    }

    pub fn add_sleeping_task(&mut self, task_id: TaskId) {
        self.sleeping_tasks.push(task_id);
    }

    pub fn take_sleeping_tasks(&mut self) -> Vec<TaskId> {
        std::mem::take(&mut self.sleeping_tasks)
    }

    pub fn reset_waiting_resources(&mut self) {}

    pub fn get_server_info(&self) -> ServerInfo {
        ServerInfo {
            worker_listen_port: self.worker_listen_port,
        }
    }

    pub fn take_ready_to_assign(&mut self) -> Vec<TaskId> {
        std::mem::take(&mut self.ready_to_assign)
    }

    pub fn get_worker_listen_port(&self) -> u16 {
        self.worker_listen_port
    }

    pub fn update_max_task_id(&mut self, task_id: TaskId) {
        assert!(task_id >= self.maximal_task_id);
        self.maximal_task_id = task_id;
    }

    pub fn check_has_new_tasks_and_reset(&mut self) -> bool {
        let result = self.has_new_tasks;
        self.has_new_tasks = false;
        result
    }

    pub fn new_worker(&mut self, worker: Worker) {
        /* Wake up sleeping tasks */
        let mut sleeping_tasks = self.take_sleeping_tasks();
        self.ready_to_assign.append(&mut sleeping_tasks);

        let worker_id = worker.id;
        self.workers.insert(worker_id, worker);
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) -> Worker {
        self.workers.remove(&worker_id).unwrap()
    }

    pub fn get_worker_by_address(&self, address: &str) -> Option<&Worker> {
        self.workers
            .values()
            .find(|w| w.configuration.listen_address == address)
    }

    pub fn get_worker_addresses(&self) -> Map<WorkerId, String> {
        self.workers
            .values()
            .map(|w| (w.id, w.configuration.listen_address.clone()))
            .collect()
    }

    #[inline]
    pub fn get_worker_by_id(&self, id: WorkerId) -> Option<&Worker> {
        self.workers.get(&id)
    }

    #[inline]
    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &Worker {
        self.workers.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    #[inline]
    pub fn get_worker_mut_by_id_or_panic(&mut self, id: WorkerId) -> &mut Worker {
        self.workers.get_mut(&id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    #[inline]
    pub fn get_worker_mut(&mut self, id: WorkerId) -> Option<&mut Worker> {
        self.workers.get_mut(&id)
    }

    #[inline]
    pub fn get_workers(&self) -> impl Iterator<Item = &Worker> {
        self.workers.values()
    }

    #[inline]
    pub fn get_workers_mut(&mut self) -> impl Iterator<Item = &mut Worker> {
        self.workers.values_mut()
    }

    #[inline]
    pub fn get_worker_map(&self) -> &WorkerMap {
        &self.workers
    }

    #[inline]
    pub fn has_workers(&self) -> bool {
        !self.workers.is_empty()
    }

    pub fn add_task(&mut self, task: Task) {
        if task.is_ready() {
            self.add_ready_to_assign(task.id);
        }
        self.has_new_tasks = true;
        assert!(self.tasks.insert(task).is_none());
    }

    #[inline(never)]
    fn wakeup_parked_resources(&mut self) {
        log::debug!("Waking up parked resources");
        for worker in self.workers.values_mut() {
            worker.set_parked_flag(false);
        }
        self.parked_resources.clear();
    }

    #[inline]
    pub fn try_wakeup_parked_resources(&mut self, request: &ResourceRequest) {
        for res in &self.parked_resources {
            if res.is_capable_to_run(request) {
                self.wakeup_parked_resources();
                break;
            }
        }
    }

    #[inline]
    pub fn add_ready_to_assign(&mut self, task_id: TaskId) {
        self.ready_to_assign.push(task_id);
    }

    // TODO: move to TaskMap
    /// Removes a single task.
    /// It can still remain in [`ready_to_assign`], where it will remain until the scheduler picks
    /// it up.
    #[must_use]
    pub fn remove_task(&mut self, task_id: TaskId) -> TaskRuntimeState {
        let task = self
            .tasks
            .remove(task_id)
            .expect("Trying to remove non-existent task");
        assert!(!task.has_consumers());
        task.state
    }

    /// Removes multiple tasks at once, to reduce memory consumption
    pub fn remove_tasks_batched(&mut self, tasks: &Set<TaskId>) {
        for &task_id in tasks {
            let _ = self.remove_task(task_id);
        }
        self.ready_to_assign.retain(|t| !tasks.contains(t));
    }

    #[inline]
    pub fn task_map(&self) -> &TaskMap {
        &self.tasks
    }

    #[inline]
    pub fn task_map_mut(&mut self) -> &mut TaskMap {
        &mut self.tasks
    }

    #[inline]
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.get_task(task_id)
    }

    #[inline]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.get_task_mut(task_id)
    }

    #[inline]
    pub fn find_task(&self, task_id: TaskId) -> Option<&Task> {
        self.tasks.find_task(task_id)
    }

    #[inline]
    pub fn find_task_mut(&mut self, task_id: TaskId) -> Option<&mut Task> {
        self.tasks.find_task_mut(task_id)
    }

    pub fn custom_conn_handler(&self) -> &Option<CustomConnectionHandler> {
        &self.custom_conn_handler
    }

    pub fn sanity_check(&self) {
        let fw_check = |task: &Task| {
            for input in &task.inputs {
                assert!(self.tasks.get_task(input.task()).is_finished());
            }
            for &task_id in task.get_consumers() {
                assert!(self.tasks.get_task(task_id).is_waiting());
            }
        };

        let worker_check = |core: &Core, task_id: TaskId, wid: WorkerId| {
            for (worker_id, worker) in core.workers.iter() {
                if wid == *worker_id {
                    assert!(worker.tasks().contains(&task_id));
                } else {
                    assert!(!worker.tasks().contains(&task_id));
                }
            }
        };

        for (worker_id, worker) in self.workers.iter() {
            assert_eq!(worker.id, *worker_id);
            if worker.is_parked() {
                assert!(self.parked_resources.contains(&worker.resources));
            }
            worker.sanity_check(&self.tasks);
        }

        for task_id in self.tasks.task_ids() {
            let task = self.get_task(task_id);
            assert_eq!(task.id, task_id);
            match &task.state {
                TaskRuntimeState::Waiting(winfo) => {
                    let mut count = 0;
                    for ti in &task.inputs {
                        if !self.tasks.get_task(ti.task()).is_finished() {
                            count += 1;
                        }
                    }
                    for &task_id in task.get_consumers() {
                        assert!(self.tasks.get_task(task_id).is_waiting());
                    }
                    assert_eq!(winfo.unfinished_deps, count);
                    worker_check(self, task.id, 0.into());
                    assert!(task.is_fresh());
                }

                TaskRuntimeState::Assigned(wid)
                | TaskRuntimeState::Running { worker_id: wid, .. } => {
                    assert!(!task.is_fresh());
                    fw_check(task);
                    worker_check(self, task.id, *wid);
                }

                TaskRuntimeState::Stealing(_, target) => {
                    assert!(!task.is_fresh());
                    fw_check(task);
                    worker_check(self, task.id, target.unwrap_or(WorkerId::new(0)));
                }

                TaskRuntimeState::Finished(_) => {
                    for ti in &task.inputs {
                        assert!(self.tasks.get_task(ti.task()).is_finished());
                    }
                }
            }
        }
    }

    #[inline]
    pub fn get_or_create_generic_resource_id(&mut self, name: &str) -> GenericResourceId {
        self.resource_map.get_or_allocate_id(name)
    }

    #[inline]
    pub fn create_resource_map(&self) -> ResourceMap {
        self.resource_map.create_map()
    }

    #[inline]
    pub fn resource_count(&self) -> usize {
        self.resource_map.resource_count()
    }

    pub fn set_secret_key(&mut self, secret_key: Option<Arc<SecretKey>>) {
        self.secret_key = secret_key
    }

    pub fn secret_key(&self) -> &Option<Arc<SecretKey>> {
        &self.secret_key
    }

    /// Add a task handle to a connection RPC loop.
    #[cfg(test)]
    pub fn add_rpc_handle(&mut self, handle: JoinHandle<()>) {
        self.rpc_handles.push(handle);
    }

    #[cfg(not(test))]
    pub fn add_rpc_handle(&mut self, _handle: JoinHandle<()>) {}

    #[cfg(test)]
    pub fn take_rpc_handles(&mut self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut self.rpc_handles)
    }
}

#[cfg(test)]
mod tests {
    use crate::server::core::Core;
    use crate::server::task::Task;
    use crate::server::task::TaskRuntimeState;
    use crate::server::worker::Worker;
    use crate::tests::utils::task;
    use crate::{TaskId, WorkerId};

    impl Core {
        pub fn get_read_to_assign(&self) -> &[TaskId] {
            &self.ready_to_assign
        }

        pub fn remove_from_ready_to_assign(&mut self, task_id: TaskId) {
            self.ready_to_assign.retain(|&id| id != task_id);
        }

        pub fn assert_task_condition<T: Copy + Into<TaskId>, F: Fn(&Task) -> bool>(
            &self,
            task_ids: &[T],
            op: F,
        ) {
            for task_id in task_ids {
                let task_id: TaskId = task_id.clone().into();
                if !op(&self.get_task(task_id)) {
                    panic!("Task {} does not satisfy the condition", task_id);
                }
            }
        }

        pub fn assert_worker_condition<W: Copy + Into<WorkerId>, F: Fn(&Worker) -> bool>(
            &self,
            worker_ids: &[W],
            op: F,
        ) {
            for worker_id in worker_ids {
                let worker_id: WorkerId = worker_id.clone().into();
                if !op(&self.get_worker_by_id_or_panic(worker_id)) {
                    panic!("Worker {} does not satisfy the condition", worker_id);
                }
            }
        }

        pub fn assert_waiting<T: Copy + Into<TaskId>>(&self, task_ids: &[T]) {
            self.assert_task_condition(task_ids, |t| t.is_waiting());
        }

        pub fn assert_ready<T: Copy + Into<TaskId>>(&self, task_ids: &[T]) {
            self.assert_task_condition(task_ids, |t| t.is_ready());
        }

        pub fn assert_assigned<T: Copy + Into<TaskId>>(&self, task_ids: &[T]) {
            self.assert_task_condition(task_ids, |t| t.is_assigned());
        }

        pub fn assert_fresh<T: Copy + Into<TaskId>>(&self, task_ids: &[T]) {
            self.assert_task_condition(task_ids, |t| t.is_fresh());
        }

        pub fn assert_not_fresh<T: Copy + Into<TaskId>>(&self, task_ids: &[T]) {
            self.assert_task_condition(task_ids, |t| !t.is_fresh());
        }

        pub fn assert_running<T: Copy + Into<TaskId>>(&self, task_ids: &[T]) {
            self.assert_task_condition(task_ids, |t| t.is_running());
        }

        pub fn assert_underloaded<W: Copy + Into<WorkerId>>(&self, worker_ids: &[W]) {
            self.assert_worker_condition(worker_ids, |w| w.is_underloaded());
        }

        pub fn assert_not_underloaded<W: Copy + Into<WorkerId>>(&self, worker_ids: &[W]) {
            self.assert_worker_condition(worker_ids, |w| !w.is_underloaded());
        }

        pub fn task<T: Into<TaskId>>(&self, id: T) -> &Task {
            self.tasks.get_task(id.into())
        }
    }

    #[test]
    fn add_remove() {
        let mut core = Core::default();
        let t = task::task(101);
        core.add_task(t);
        assert!(match core.remove_task(101.into()) {
            TaskRuntimeState::Waiting(_) => true,
            _ => false,
        });
        assert_eq!(core.find_task(101.into()), None);
    }
}
