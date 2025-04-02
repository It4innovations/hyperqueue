use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use orion::aead::SecretKey;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::SmallRng;

use crate::TaskId;
use crate::WorkerId;
use crate::internal::common::resources::Allocation;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::stablemap::StableMap;
use crate::internal::common::{Map, Set, WrappedRcRefCell};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    FromWorkerMessage, NewWorkerMsg, StealResponse, TaskFailedMsg, TaskFinishedMsg,
};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::internal::worker::resources::allocator::ResourceAllocator;
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::rqueue::ResourceWaitQueue;
use crate::internal::worker::task::{Task, TaskState};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::launcher::TaskLauncher;

pub type TaskMap = StableMap<TaskId, Task>;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    comm: WorkerComm,
    tasks: TaskMap,
    pub(crate) ready_task_queue: ResourceWaitQueue,
    pub(crate) running_tasks: Set<TaskId>,
    pub(crate) start_task_scheduled: bool,

    pub(crate) worker_id: WorkerId,
    pub(crate) worker_addresses: Map<WorkerId, String>,
    pub(crate) random: SmallRng,

    pub(crate) configuration: WorkerConfiguration,
    pub(crate) task_launcher: Box<dyn TaskLauncher>,
    //pub(crate) secret_key: Option<Arc<SecretKey>>,
    pub(crate) start_time: Instant,

    pub(crate) reservation: bool, // If true, idle timeout is blocked
    pub(crate) last_task_finish_time: Instant,

    resource_map: ResourceMap,
    resource_label_map: ResourceLabelMap,

    server_uid: String,
}

impl WorkerState {
    pub(crate) fn comm(&mut self) -> &mut WorkerComm {
        &mut self.comm
    }

    pub fn server_uid(&self) -> &str {
        &self.server_uid
    }

    #[inline]
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.get(&task_id)
    }

    #[inline]
    pub fn find_task(&self, task_id: TaskId) -> Option<&Task> {
        self.tasks.find(&task_id)
    }

    #[inline]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.get_mut(&task_id)
    }

    #[inline]
    pub fn borrow_tasks_and_queue(&mut self) -> (&TaskMap, &mut ResourceWaitQueue) {
        (&self.tasks, &mut self.ready_task_queue)
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn add_ready_task(&mut self, task: &Task) {
        self.ready_task_queue.add_task(task);
        self.schedule_task_start();
    }

    pub fn add_ready_tasks(&mut self, tasks: &[Task]) {
        for task in tasks {
            self.ready_task_queue.add_task(task);
        }
        self.schedule_task_start();
    }

    pub fn add_task(&mut self, task: Task) {
        if task.is_ready() {
            log::debug!("Task {} is directly ready", task.id);
            self.add_ready_task(&task);
        } else {
            log::debug!(
                "Task {} is blocked by {} remote objects",
                task.id,
                task.get_waiting()
            );
        }
        self.tasks.insert(task);
    }

    pub fn random_choice<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        if items.len() == 1 {
            &items[0]
        } else {
            items.choose(&mut self.random).unwrap()
        }
    }

    #[inline]
    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    pub fn reset_idle_timer(&mut self) {
        self.last_task_finish_time = Instant::now();
    }

    fn remove_task(&mut self, task_id: TaskId, just_finished: bool) {
        match self.tasks.remove(&task_id).unwrap().state {
            TaskState::Waiting(x) => {
                log::debug!("Removing waiting task id={}", task_id);
                assert!(!just_finished);
                if x == 0 {
                    self.ready_task_queue.remove_task(task_id);
                }
            }
            TaskState::Running(_, allocation) => {
                log::debug!("Removing running task id={}", task_id);
                assert!(just_finished);
                assert!(self.running_tasks.remove(&task_id));
                self.schedule_task_start();
                self.ready_task_queue.release_allocation(allocation);
            }
        }

        if self.tasks.is_empty() {
            self.comm.notify_worker_is_empty();
        }
        self.reset_idle_timer();
    }

    pub fn get_worker_address(&self, worker_id: WorkerId) -> Option<&String> {
        self.worker_addresses.get(&worker_id)
    }

    pub fn drop_non_running_tasks(&mut self) {
        log::debug!("Dropping non running tasks");
        let non_running_tasks: Vec<TaskId> = self
            .tasks
            .values()
            .filter_map(|t| if t.is_running() { None } else { Some(t.id) })
            .collect();
        for task_id in non_running_tasks {
            self.remove_task(task_id, false);
        }
    }

    pub fn cancel_task(&mut self, task_id: TaskId) {
        log::debug!("Canceling task {}", task_id);
        let was_waiting = match self.tasks.find_mut(&task_id) {
            None => {
                /* This may happen that task was computed or when work steal
                  was successful
                */
                log::debug!("Task not found");
                false
            }
            Some(task) => match task.state {
                TaskState::Running(ref mut env, _) => {
                    env.send_cancel_notification();
                    false
                }
                TaskState::Waiting(_) => true,
            },
        };
        if was_waiting {
            self.remove_task(task_id, false);
        }
    }

    pub fn steal_task(&mut self, task_id: TaskId) -> StealResponse {
        let response = match self.tasks.find(&task_id) {
            None => StealResponse::NotHere,
            Some(task) => match task.state {
                TaskState::Waiting(_) => StealResponse::Ok,
                TaskState::Running(_, _) => StealResponse::Running,
            },
        };
        if let StealResponse::Ok = &response {
            self.remove_task(task_id, false);
        }
        response
    }

    pub fn schedule_task_start(&mut self) {
        if self.start_task_scheduled {
            return;
        }
        self.start_task_scheduled = true;
        self.comm.notify_start_task();
    }

    pub fn start_task(
        &mut self,
        task_id: TaskId,
        task_comm: RunningTaskComm,
        allocation: Rc<Allocation>,
    ) {
        let task = self.get_task_mut(task_id);
        task.state = TaskState::Running(task_comm, allocation);
        self.running_tasks.insert(task_id);
    }

    pub fn finish_task(&mut self, task_id: TaskId) {
        self.remove_task(task_id, true);
        let message = FromWorkerMessage::TaskFinished(TaskFinishedMsg { id: task_id });
        self.comm.send_message_to_server(message);
    }

    pub fn finish_task_failed(&mut self, task_id: TaskId, info: TaskFailInfo) {
        self.remove_task(task_id, true);
        let message = FromWorkerMessage::TaskFailed(TaskFailedMsg { id: task_id, info });
        self.comm.send_message_to_server(message);
    }

    pub fn finish_task_cancel(&mut self, task_id: TaskId) {
        self.remove_task(task_id, true);
    }

    pub fn get_resource_map(&self) -> &ResourceMap {
        &self.resource_map
    }

    pub fn get_resource_label_map(&self) -> &ResourceLabelMap {
        &self.resource_label_map
    }

    pub fn worker_hostname(&self, worker_id: WorkerId) -> Option<&str> {
        if worker_id == self.worker_id {
            return Some(&self.configuration.hostname);
        }
        self.worker_addresses
            .get(&worker_id)
            .and_then(|address| address.split(':').next())
    }

    pub fn new_worker(&mut self, other_worker: NewWorkerMsg) {
        log::debug!(
            "New worker={} announced at {}",
            other_worker.worker_id,
            &other_worker.address
        );
        assert_ne!(self.worker_id, other_worker.worker_id); // We should not receive message about ourselves
        assert!(
            self.worker_addresses
                .insert(other_worker.worker_id, other_worker.address)
                .is_none()
        );

        let resources = WorkerResources::from_transport(other_worker.resources);
        self.ready_task_queue
            .new_worker(other_worker.worker_id, resources);
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        log::debug!("Lost worker={} announced", worker_id);
        assert!(self.worker_addresses.remove(&worker_id).is_some());
        self.ready_task_queue.remove_worker(worker_id);
    }
}

impl WorkerStateRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        comm: WorkerComm,
        worker_id: WorkerId,
        configuration: WorkerConfiguration,
        _secret_key: Option<Arc<SecretKey>>,
        resource_map: ResourceMap,
        task_launcher: Box<dyn TaskLauncher>,
        server_uid: String,
    ) -> Self {
        let resource_label_map = ResourceLabelMap::new(&configuration.resources, &resource_map);
        let allocator =
            ResourceAllocator::new(&configuration.resources, &resource_map, &resource_label_map);
        let ready_task_queue = ResourceWaitQueue::new(allocator);
        let now = Instant::now();

        Self::wrap(WorkerState {
            comm,
            worker_id,
            configuration,
            task_launcher,
            tasks: Default::default(),
            ready_task_queue,
            random: SmallRng::from_os_rng(),
            start_task_scheduled: false,
            running_tasks: Default::default(),
            start_time: now,
            resource_map,
            resource_label_map,
            last_task_finish_time: now,
            reservation: false,
            worker_addresses: Default::default(),
            server_uid,
        })
    }
}
