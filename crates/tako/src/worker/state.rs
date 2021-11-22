use std::rc::Rc;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use orion::aead::SecretKey;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use crate::common::data::SerializationType;
use crate::common::resources::map::ResourceMap;
use crate::common::stablemap::StableMap;
use crate::common::{Map, Set, WrappedRcRefCell};
use crate::messages::common::{TaskFailInfo, WorkerConfiguration};
use crate::messages::worker::{FromWorkerMessage, StealResponse, TaskFailedMsg, TaskFinishedMsg};
use crate::transfer::auth::serialize;
use crate::transfer::DataConnection;
use crate::worker::data::{DataObject, DataObjectRef, DataObjectState};
use crate::worker::hwmonitor::WorkerHwState;
use crate::worker::launcher::TaskLauncher;
use crate::worker::rqueue::ResourceWaitQueue;
use crate::worker::task::{Task, TaskState};
use crate::TaskId;
use crate::{PriorityTuple, WorkerId};

pub type TaskMap = StableMap<TaskId, Task>;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    pub sender: UnboundedSender<Bytes>,
    tasks: TaskMap,
    pub ready_task_queue: ResourceWaitQueue,
    pub data_objects: HashMap<TaskId, DataObjectRef>,
    pub running_tasks: Set<TaskId>,
    pub start_task_scheduled: bool,
    pub start_task_notify: Rc<Notify>,

    pub download_sender: tokio::sync::mpsc::UnboundedSender<(DataObjectRef, PriorityTuple)>,
    pub worker_id: WorkerId,
    pub worker_addresses: Map<WorkerId, String>,
    pub worker_connections: Map<WorkerId, Vec<DataConnection>>,
    pub random: SmallRng,

    pub self_ref: Option<WorkerStateRef>,

    pub configuration: WorkerConfiguration,
    pub task_launcher: TaskLauncher,
    pub secret_key: Option<Arc<SecretKey>>,

    pub start_time: std::time::Instant,
    pub hardware_state: WorkerHwState,

    resource_map: ResourceMap,
}

impl WorkerState {
    #[inline]
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.get(&task_id)
    }

    #[inline]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.get_mut(&task_id)
    }

    #[inline]
    pub fn borrow_tasks_and_queue(&mut self) -> (&TaskMap, &mut ResourceWaitQueue) {
        (&self.tasks, &mut self.ready_task_queue)
    }

    pub fn add_data_object(&mut self, data_ref: DataObjectRef) {
        let id = data_ref.get().id;
        self.data_objects.insert(id, data_ref);
    }

    pub fn send_message_to_server(&self, message: FromWorkerMessage) {
        self.sender
            .send(serialize(&message).unwrap().into())
            .unwrap();
    }

    pub fn on_data_downloaded(
        &mut self,
        _data_ref: DataObjectRef,
        _data: BytesMut,
        _serializer: SerializationType,
    ) {
        // TODO
        /*let new_ready = {
            let mut data_obj = data_ref.get_mut();
            log::debug!("Data {} downloaded ({} bytes)", data_obj.id, data.len());
            match data_obj.state {
                DataObjectState::Remote(_) => { /* This is ok */ }
                DataObjectState::Removed => {
                    /* download was completed, but we do not care about data */
                    log::debug!("Data is not needed any more");
                    return;
                }
                DataObjectState::Local(_) => {
                    log::debug!("Data clash, data is already in worker, ignoring download");
                    return;
                }
            }
            data_obj.state = DataObjectState::Local(LocalData {
                serializer,
                bytes: data.into(),
            });

            let message = FromWorkerMessage::DataDownloaded(DataDownloadedMsg { id: data_obj.id });
            self.send_message_to_server(message);

            /* We need to drop borrow before calling
              add_ready_task may start to borrow_mut this data_ref
            */
            let mut new_ready: SmallVec<[TaskRef; 2]> = smallvec![];
            for task_ref in &data_obj.consumers {
                let is_ready = task_ref.get_mut().decrease_waiting_count();
                if is_ready {
                    log::debug!("Task {} becomes ready", task_ref.get().id);
                    new_ready.push(task_ref.clone());
                }
            }
            new_ready
        };
        if !new_ready.is_empty() {
            self.add_ready_tasks(&new_ready);
        }*/
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

    pub fn add_dependency(
        &mut self,
        _task: &Task,
        _task_id: TaskId,
        _size: u64,
        _workers: Vec<WorkerId>,
    ) {
        todo!();
        /*let mut task = task_ref.get_mut();
        let mut is_remote = false;
        let data_ref = match self.data_objects.get(&task_id).cloned() {
            None => {
                let data_ref = DataObjectRef::new(
                    task_id,
                    size,
                    DataObjectState::Remote(RemoteData { workers }),
                );
                self.data_objects.insert(task_id, data_ref.clone());
                is_remote = true;
                data_ref
            }
            Some(data_ref) => {
                {
                    let mut data_obj = data_ref.get_mut();
                    match data_obj.state {
                        DataObjectState::Remote(_) => {
                            is_remote = true;
                            data_obj.state = DataObjectState::Remote(RemoteData { workers })
                        }
                        DataObjectState::Local(_) => { /* Do nothing */ }
                        DataObjectState::Removed => {
                            unreachable!();
                        }
                    };
                }
                data_ref
            }
        };
        data_ref.get_mut().consumers.insert(task_ref.clone());
        if is_remote {
            task.increase_waiting_count();
            let _ = self.download_sender.send((data_ref.clone(), task.priority));
        }
        task.deps.push(data_ref);*/
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
        self.tasks.insert(task.id, task);
    }

    pub fn remove_data_by_id(&mut self, task_id: TaskId) {
        log::debug!("Removing data object by id={}", task_id);
        if let Some(data_ref) = self.data_objects.remove(&task_id) {
            let mut data_obj = data_ref.get_mut();
            self.remove_data_helper(&mut data_obj);
        } else {
            log::debug!("Object not here");
        };
    }

    pub fn remove_data(&mut self, data_obj: &mut DataObject) {
        log::debug!("Removing data object {}", data_obj.id);
        assert!(self.data_objects.remove(&data_obj.id).is_some());
        self.remove_data_helper(data_obj);
    }

    fn remove_data_helper(&mut self, data_obj: &mut DataObject) {
        if !data_obj.consumers.is_empty() {
            todo!(); // What should happen when server removes data but there are tasks that needs it?
        }
        data_obj.state = DataObjectState::Removed;
    }

    pub fn random_choice<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        if items.len() == 1 {
            &items[0]
        } else {
            items.choose(&mut self.random).unwrap()
        }
    }

    fn remove_task(&mut self, task_id: TaskId, just_finished: bool) {
        let previous_state =
            { std::mem::replace(&mut self.tasks.get_mut(&task_id).state, TaskState::Removed) };

        match previous_state {
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
            TaskState::Removed => {
                unreachable!();
            }
        }

        assert!(self.tasks.remove(&task_id).is_some());
        /* TODO
        for data_ref in std::mem::take(&mut task.deps) {
            let mut data = data_ref.get_mut();
            assert!(data.consumers.remove(task_ref));
            if data.consumers.is_empty() {
                match data.state {
                    DataObjectState::Remote(_) => {
                        /* We are going to stop unnecessary download */
                        assert!(!just_finished);
                        self.remove_data(&mut data);
                    }
                    DataObjectState::Local(_) => { /* Do nothing */ }
                    DataObjectState::Removed => {
                        unreachable!()
                    }
                };
            }
        }*/
    }

    pub fn pop_worker_connection(&mut self, worker_id: WorkerId) -> Option<DataConnection> {
        self.worker_connections
            .get_mut(&worker_id)
            .and_then(|connections| connections.pop())
    }

    pub fn return_worker_connection(&mut self, worker_id: WorkerId, connection: DataConnection) {
        self.worker_connections
            .entry(worker_id)
            .or_default()
            .push(connection);
    }

    pub fn get_worker_address(&self, worker_id: WorkerId) -> Option<&String> {
        self.worker_addresses.get(&worker_id)
    }

    pub fn cancel_task(&mut self, task_id: TaskId) {
        log::debug!("Canceling task {}", task_id);
        let was_waiting = match self.tasks.find_mut(&task_id) {
            None => {
                /* This may happen that task was computed or when work steal
                  was successful
                */
                log::debug!("Task not found, try to remove object");
                self.remove_data_by_id(task_id);
                false
            }
            Some(task) => match task.state {
                TaskState::Running(ref mut env, _) => {
                    env.cancel_task();
                    false
                }
                TaskState::Waiting(_) => true,
                TaskState::Removed => unreachable!(),
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
                TaskState::Removed => unreachable!(),
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

    pub fn self_ref(&self) -> WorkerStateRef {
        self.self_ref.clone().unwrap()
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
        download_sender: tokio::sync::mpsc::UnboundedSender<(DataObjectRef, PriorityTuple)>,
        worker_addresses: Map<WorkerId, String>,
        resource_map: ResourceMap,
        task_launcher: TaskLauncher,
    ) -> Self {
        let ready_task_queue = ResourceWaitQueue::new(&configuration.resources, &resource_map);
        let self_ref = Self::wrap(WorkerState {
            worker_id,
            worker_addresses,
            sender,
            download_sender,
            configuration,
            task_launcher,
            secret_key,
            tasks: Default::default(),
            ready_task_queue,
            data_objects: Default::default(),
            random: SmallRng::from_entropy(),
            worker_connections: Default::default(),
            self_ref: None,
            start_task_scheduled: false,
            start_task_notify: Rc::new(Notify::new()),
            running_tasks: Default::default(),
            hardware_state: Default::default(),
            start_time: std::time::Instant::now(),
            resource_map,
        });
        self_ref.get_mut().self_ref = Some(self_ref.clone());
        self_ref
    }
}
