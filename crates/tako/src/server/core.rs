use std::sync::Arc;
use std::time::Duration;

use orion::aead::SecretKey;
use tokio::task::JoinHandle;

use crate::common::resources::map::{ResourceIdAllocator, ResourceMap};
use crate::common::resources::{GenericResourceId, ResourceRequest};
use crate::common::trace::trace_task_remove;
use crate::common::{Map, Set, WrappedRcRefCell};
use crate::messages::gateway::ServerInfo;
use crate::server::monitoring::EventStorage;
use crate::server::rpc::ConnectionDescriptor;
use crate::server::task::{Task, TaskRef, TaskRuntimeState};
use crate::server::worker::Worker;
use crate::server::worker_load::WorkerResources;
use crate::{TaskId, WorkerId};

pub type CustomConnectionHandler = Box<dyn Fn(ConnectionDescriptor)>;

#[derive(Default)]
pub struct Core {
    tasks: Map<TaskId, TaskRef>,
    workers: Map<WorkerId, Worker>,
    event_storage: EventStorage,

    /* Scheduler items */
    parked_resources: Set<WorkerResources>, // Resources of workers that has flag NOTHING_TO_LOAD
    // TODO: benchmark and possibly replace with a set
    ready_to_assign: Vec<TaskRef>,
    has_new_tasks: bool,

    sleeping_tasks: Vec<TaskRef>, // Tasks that cannot be scheduled to any available worker

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
        event_store_size: usize,
        custom_conn_handler: Option<CustomConnectionHandler>,
    ) -> Self {
        /*let mut core = Core::default();
        core.worker_listen_port = worker_listen_port;
        core.secret_key = secret_key;*/
        CoreRef::wrap(Core {
            worker_listen_port,
            secret_key,
            idle_timeout,
            custom_conn_handler,
            event_storage: EventStorage::new(event_store_size),
            ..Default::default()
        })
    }
}

impl Core {
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
            if worker.is_underloaded() && worker.tasks().iter().all(|t| t.get().is_running()) {
                log::debug!("Parking worker {}", worker.id);
                worker.set_parked_flag(true);
                self.parked_resources.insert(worker.resources.clone());
            }
        }
    }

    pub fn add_sleeping_task(&mut self, task_ref: TaskRef) {
        self.sleeping_tasks.push(task_ref);
    }

    pub fn take_sleeping_tasks(&mut self) -> Vec<TaskRef> {
        std::mem::take(&mut self.sleeping_tasks)
    }

    pub fn reset_waiting_resources(&mut self) {}

    pub fn get_event_storage(&mut self) -> &mut EventStorage {
        &mut self.event_storage
    }

    pub fn get_server_info(&self) -> ServerInfo {
        ServerInfo {
            worker_listen_port: self.worker_listen_port,
        }
    }

    pub fn take_ready_to_assign(&mut self) -> Vec<TaskRef> {
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
    pub fn get_worker_map(&self) -> &Map<WorkerId, Worker> {
        &self.workers
    }

    #[inline]
    pub fn has_workers(&self) -> bool {
        !self.workers.is_empty()
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let task_id = {
            let task = task_ref.get();
            if task.is_ready() {
                self.add_ready_to_assign(task_ref.clone());
            }
            task.id()
        };
        self.has_new_tasks = true;
        assert!(self.tasks.insert(task_id, task_ref).is_none());
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
    pub fn add_ready_to_assign(&mut self, task_ref: TaskRef) {
        self.ready_to_assign.push(task_ref);
    }

    /// Removes a single task.
    /// It can still remain in [`ready_to_assign`], where it will remain until the scheduler picks
    /// it up.
    #[must_use]
    pub fn remove_task(&mut self, task: &mut Task) -> TaskRuntimeState {
        trace_task_remove(task.id);
        assert!(!task.has_consumers());
        assert!(self.tasks.remove(&task.id).is_some());
        std::mem::replace(&mut task.state, TaskRuntimeState::Released)
    }

    /// Removes multiple tasks at once, to reduce memory consumption
    pub fn remove_tasks_batched(&mut self, tasks: &Set<TaskRef>) {
        for task in tasks {
            let _ = self.remove_task(&mut task.get_mut());
        }
        self.ready_to_assign.retain(|t| !tasks.contains(t));
    }

    pub fn get_tasks(&self) -> impl Iterator<Item = &TaskRef> {
        self.tasks.values()
    }

    pub fn get_task_map(&self) -> &Map<TaskId, TaskRef> {
        &self.tasks
    }

    #[inline]
    pub fn get_task_by_id_or_panic(&self, id: TaskId) -> &TaskRef {
        self.tasks.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", id);
        })
    }

    #[inline]
    pub fn get_task_by_id(&self, id: TaskId) -> Option<&TaskRef> {
        self.tasks.get(&id)
    }

    pub fn custom_conn_handler(&self) -> &Option<CustomConnectionHandler> {
        &self.custom_conn_handler
    }

    pub fn sanity_check(&self) {
        let fw_check = |task: &Task| {
            for t in &task.inputs {
                assert!(t.task().get().is_finished());
            }
            for t in task.get_consumers() {
                assert!(t.get().is_waiting());
            }
        };

        let worker_check = |core: &Core, tr: &TaskRef, wid: WorkerId| {
            for (worker_id, worker) in &core.workers {
                if wid == *worker_id {
                    assert!(worker.tasks().contains(tr));
                } else {
                    //dbg!(tr.get().id, wid, worker_id);
                    assert!(!worker.tasks().contains(tr));
                }
            }
        };

        for (worker_id, worker) in &self.workers {
            assert_eq!(worker.id, *worker_id);
            if worker.is_parked() {
                assert!(self.parked_resources.contains(&worker.resources));
            }
            worker.sanity_check();
        }

        for (task_id, task_ref) in &self.tasks {
            let task = task_ref.get();
            assert_eq!(task.id, *task_id);
            match &task.state {
                TaskRuntimeState::Waiting(winfo) => {
                    let mut count = 0;
                    for ti in &task.inputs {
                        if !ti.task().get().is_finished() {
                            count += 1;
                        }
                    }
                    for t in task.get_consumers() {
                        assert!(t.get().is_waiting());
                    }
                    assert_eq!(winfo.unfinished_deps, count);
                    worker_check(self, task_ref, 0.into());
                    assert!(task.is_fresh());
                }

                TaskRuntimeState::Assigned(wid) | TaskRuntimeState::Running(wid) => {
                    assert!(!task.is_fresh());
                    fw_check(&task);
                    worker_check(self, task_ref, *wid);
                }

                TaskRuntimeState::Stealing(_, target) => {
                    assert!(!task.is_fresh());
                    fw_check(&task);
                    worker_check(self, task_ref, target.unwrap_or(WorkerId::new(0)));
                }

                TaskRuntimeState::Finished(_) => {
                    for ti in &task.inputs {
                        assert!(ti.task().get().is_finished());
                    }
                }

                TaskRuntimeState::Released => {
                    unreachable!()
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
    use crate::server::task::{TaskRef, TaskRuntimeState};
    use crate::tests::utils::task;

    impl Core {
        pub fn get_read_to_assign(&self) -> &[TaskRef] {
            &self.ready_to_assign
        }

        pub fn remove_from_ready_to_assign(&mut self, task_ref: &TaskRef) {
            let p = self
                .ready_to_assign
                .iter()
                .position(|x| x == task_ref)
                .unwrap();
            self.ready_to_assign.remove(p);
        }

        pub fn assert_task_condition<F: Fn(&Task) -> bool>(&self, task_ids: &[u64], op: F) {
            for task_id in task_ids {
                if !op(&self.get_task_by_id_or_panic((*task_id).into()).get()) {
                    panic!("Task {} does not satisfy the condition", task_id);
                }
            }
        }

        pub fn assert_waiting(&self, task_ids: &[u64]) {
            self.assert_task_condition(task_ids, |t| t.is_waiting());
        }

        pub fn assert_ready(&self, task_ids: &[u64]) {
            self.assert_task_condition(task_ids, |t| t.is_ready());
        }

        pub fn assert_assigned(&self, task_ids: &[u64]) {
            self.assert_task_condition(task_ids, |t| t.is_assigned());
        }

        pub fn assert_fresh(&self, task_ids: &[u64]) {
            self.assert_task_condition(task_ids, |t| t.is_fresh());
        }

        pub fn assert_running(&self, task_ids: &[u64]) {
            self.assert_task_condition(task_ids, |t| t.is_running());
        }
    }

    #[test]
    fn add_remove() {
        let mut core = Core::default();
        let t = task::task(101);
        core.add_task(t.clone());
        assert_eq!(core.get_task_by_id(101.into()).unwrap(), &t);
        assert!(match core.remove_task(&mut t.get_mut()) {
            TaskRuntimeState::Waiting(_) => true,
            _ => false,
        });
        assert_eq!(core.get_task_by_id(101.into()), None);
    }
}
