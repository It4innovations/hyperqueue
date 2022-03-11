use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use orion::aead::SecretKey;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use crate::common::resources::map::ResourceMap;
use crate::common::resources::ResourceAllocation;
use crate::common::stablemap::StableMap;
use crate::common::{Map, Set, WrappedRcRefCell};
use crate::messages::common::{TaskFailInfo, WorkerConfiguration};
use crate::messages::worker::{FromWorkerMessage, StealResponse, TaskFailedMsg, TaskFinishedMsg};
use crate::transfer::auth::serialize;
use crate::worker::launcher::TaskLauncher;
use crate::worker::rqueue::ResourceWaitQueue;
use crate::worker::task::{Task, TaskState};
use crate::worker::taskenv::TaskEnv;
use crate::TaskId;
use crate::WorkerId;
use serde::{Deserialize, Serialize};

pub type TaskMap = StableMap<TaskId, Task>;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerLostPolicy {
    Stop,
    FinishRunning,
}

pub struct WorkerState {
    sender: Option<UnboundedSender<Bytes>>,
    tasks: TaskMap,
    pub ready_task_queue: ResourceWaitQueue,
    pub running_tasks: Set<TaskId>,
    pub start_task_scheduled: bool,
    pub start_task_notify: Rc<Notify>,

    pub worker_id: WorkerId,
    pub worker_addresses: Map<WorkerId, String>,
    pub random: SmallRng,

    pub worker_is_empty_notify: Option<Rc<Notify>>,

    pub configuration: WorkerConfiguration,
    pub task_launcher: Box<dyn TaskLauncher>,
    pub secret_key: Option<Arc<SecretKey>>,

    pub start_time: std::time::Instant,
    pub last_task_finish_time: std::time::Instant,

    resource_map: ResourceMap,
}

impl WorkerState {
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

    pub fn send_message_to_server(&self, message: FromWorkerMessage) {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(serialize(&message).unwrap().into()).is_err() {
                log::debug!("Message could not be sent to server");
            }
        } else {
            log::debug!(
                "Attempting to send a message to server, but server has already disconnected"
            );
        }
    }
    pub fn drop_sender(&mut self) {
        self.sender = None;
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

    #[inline]
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
            if let Some(notify) = &self.worker_is_empty_notify {
                log::debug!("Notifying that worker is empty");
                notify.notify_one()
            }
        }
        self.last_task_finish_time = Instant::now();
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
                    env.cancel_task();
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
        self.start_task_notify.notify_one();
    }

    pub fn start_task(
        &mut self,
        task_id: TaskId,
        task_env: TaskEnv,
        allocation: ResourceAllocation,
    ) {
        let mut task = self.get_task_mut(task_id);
        task.state = TaskState::Running(task_env, allocation);
        self.running_tasks.insert(task_id);
    }

    pub fn finish_task(&mut self, task_id: TaskId, size: u64) {
        self.remove_task(task_id, true);
        let message = FromWorkerMessage::TaskFinished(TaskFinishedMsg { id: task_id, size });
        self.send_message_to_server(message);
    }

    pub fn finish_task_failed(&mut self, task_id: TaskId, info: TaskFailInfo) {
        self.remove_task(task_id, true);
        let message = FromWorkerMessage::TaskFailed(TaskFailedMsg { id: task_id, info });
        self.send_message_to_server(message);
    }

    pub fn finish_task_cancel(&mut self, task_id: TaskId) {
        self.remove_task(task_id, true);
    }

    pub fn get_resource_map(&self) -> &ResourceMap {
        &self.resource_map
    }
}

impl WorkerStateRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_id: WorkerId,
        configuration: WorkerConfiguration,
        secret_key: Option<Arc<SecretKey>>,
        sender: UnboundedSender<Bytes>,
        worker_addresses: Map<WorkerId, String>,
        resource_map: ResourceMap,
        task_launcher: Box<dyn TaskLauncher>,
    ) -> Self {
        let ready_task_queue = ResourceWaitQueue::new(&configuration.resources, &resource_map);
        let now = std::time::Instant::now();

        Self::wrap(WorkerState {
            worker_id,
            worker_addresses,
            sender: Some(sender),
            configuration,
            task_launcher,
            secret_key,
            tasks: Default::default(),
            ready_task_queue,
            random: SmallRng::from_entropy(),
            start_task_scheduled: false,
            start_task_notify: Rc::new(Notify::new()),
            running_tasks: Default::default(),
            start_time: now,
            resource_map,
            worker_is_empty_notify: None,
            last_task_finish_time: now,
        })
    }
}
