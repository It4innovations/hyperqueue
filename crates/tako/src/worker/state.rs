use std::rc::Rc;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use orion::aead::SecretKey;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use smallvec::{smallvec, SmallVec};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use crate::common::data::SerializationType;
use crate::common::{Map, Set, WrappedRcRefCell};
use crate::messages::common::{SubworkerDefinition, TaskFailInfo, WorkerConfiguration};
use crate::messages::worker::{
    DataDownloadedMsg, FromWorkerMessage, StealResponse, TaskFailedMsg, TaskFinishedMsg,
};
use crate::server::worker::WorkerId;
use crate::transfer::auth::serialize;
use crate::transfer::DataConnection;
use crate::worker::data::{DataObject, DataObjectRef, DataObjectState, LocalData, RemoteData};
use crate::worker::launcher::InnerTaskLauncher;
use crate::worker::rqueue::ResourceWaitQueue;
use crate::worker::subworker::{SubworkerId, SubworkerRef};
use crate::worker::task::{TaskRef, TaskState};
use crate::PriorityTuple;
use crate::TaskId;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    pub sender: UnboundedSender<Bytes>,
    pub subworkers: HashMap<SubworkerId, SubworkerRef>,
    pub free_subworkers: Vec<SubworkerRef>,
    pub tasks: HashMap<TaskId, TaskRef>,
    pub ready_task_queue: ResourceWaitQueue,
    pub data_objects: HashMap<TaskId, DataObjectRef>,
    pub running_tasks: Set<TaskRef>,
    pub start_task_scheduled: bool,
    pub start_task_notify: Rc<Notify>,

    pub download_sender: tokio::sync::mpsc::UnboundedSender<(DataObjectRef, PriorityTuple)>,
    pub worker_id: WorkerId,
    pub worker_addresses: Map<WorkerId, String>,
    pub worker_connections: Map<WorkerId, Vec<DataConnection>>,
    pub random: SmallRng,

    pub self_ref: Option<WorkerStateRef>,

    pub subworker_id_counter: SubworkerId,
    pub subworker_definitions: Map<SubworkerId, SubworkerDefinition>,

    pub configuration: WorkerConfiguration,
    pub inner_task_launcher: InnerTaskLauncher,
    pub secret_key: Option<Arc<SecretKey>>,
}

impl WorkerState {
    /*pub fn set_subworkers(&mut self, subworkers: Vec<SubworkerRef>) {
        assert!(self.subworkers.is_empty() && self.free_subworkers.is_empty());
        self.free_subworkers = subworkers.clone();
        self.subworkers = subworkers
            .iter()
            .map(|s| {
                let id = s.get().id;
                (id, s.clone())
            })
            .collect();
    }*/

    pub fn new_subworker_id(&mut self) -> SubworkerId {
        let id = self.subworker_id_counter;
        self.subworker_id_counter += 1;
        id
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
        data_ref: DataObjectRef,
        data: BytesMut,
        serializer: SerializationType,
    ) {
        let new_ready = {
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
                DataObjectState::InSubworkers(_) | DataObjectState::LocalDownloading(_) => {
                    log::debug!("Data is also in subworker, but not local, accepting download");
                }
            }
            data_obj.state = DataObjectState::Local(LocalData {
                serializer,
                bytes: data.into(),
                subworkers: Default::default(),
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
        }
    }

    pub fn add_ready_task(&mut self, task_ref: TaskRef) {
        self.ready_task_queue.add_task(task_ref);
        self.schedule_task_start();
    }

    pub fn add_ready_tasks(&mut self, task_refs: &[TaskRef]) {
        for task_ref in task_refs {
            self.ready_task_queue.add_task(task_ref.clone());
        }
        self.schedule_task_start();
    }

    pub fn add_dependancy(
        &mut self,
        task_ref: &TaskRef,
        task_id: TaskId,
        size: u64,
        workers: Vec<WorkerId>,
    ) {
        let mut task = task_ref.get_mut();
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
                        DataObjectState::Local(_)
                        | DataObjectState::InSubworkers(_)
                        | DataObjectState::LocalDownloading(_) => { /* Do nothing */ }
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
        task.deps.push(data_ref);
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let id = task_ref.get().id;
        if task_ref.get().is_ready() {
            log::debug!("Task {} is directly ready", id);
            self.add_ready_task(task_ref.clone());
        } else {
            let task = task_ref.get();
            log::debug!(
                "Task {} is blocked by {} remote objects",
                id,
                task.get_waiting()
            );
        }
        self.tasks.insert(id, task_ref);
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
        if let Some(sw_refs) = data_obj.get_placement() {
            for sw_ref in sw_refs {
                sw_ref.get().send_remove_data(data_obj.id);
            }
        }
    }

    pub fn random_choice<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        if items.len() == 1 {
            &items[0]
        } else {
            items.choose(&mut self.random).unwrap()
        }
    }

    pub fn remove_task(&mut self, task_ref: TaskRef, just_finished: bool) {
        let mut task = task_ref.get_mut();
        match std::mem::replace(&mut task.state, TaskState::Removed) {
            TaskState::Waiting(x) => {
                log::debug!("Removing waiting task id={}", task.id);
                assert!(!just_finished);
                if x == 0 {
                    self.ready_task_queue.remove_task(&task_ref);
                }
            }
            TaskState::Uploading(_, _, _) => {
                todo!()
                /* This should not happen in this version, but in case of need:
                   TODO: The following code
                   TODO: Free subworker
                   TODO: Try to schedule new task
                  for data_ref in std::mem::take(&mut task.deps) {
                    let data_obj = data_ref.get_mut();
                    if let DataObjectState::LocalDownloading(mut ld) = &mut data_obj.state {
                        let pos = ld.subscribers.iter().position(|s| {
                            match s {
                                Subscriber::Task(t_ref) => { t_ref == &task_ref }
                                _ => false
                            }
                        });
                        if let Some(p) = pos {
                            ld.subscribers.remove(p);
                        }
                    }
                }*/
            }
            TaskState::Running(mut env, allocation) => {
                log::debug!("Removing running task id={}", task.id);
                if !just_finished {
                    env.cancel_task();
                }
                assert!(self.running_tasks.remove(&task_ref));
                self.schedule_task_start();
                self.ready_task_queue.release_allocation(allocation);
            }
            TaskState::Removed => {
                unreachable!();
            }
        }

        assert!(self.tasks.remove(&task.id).is_some());
        for data_ref in std::mem::take(&mut task.deps) {
            let mut data = data_ref.get_mut();
            assert!(data.consumers.remove(&task_ref));
            if data.consumers.is_empty() {
                match data.state {
                    DataObjectState::Remote(_) => {
                        /* We are going to stop unnecessary download */
                        assert!(!just_finished);
                        self.remove_data(&mut data);
                    }
                    DataObjectState::InSubworkers(_)
                    | DataObjectState::Local(_)
                    | DataObjectState::LocalDownloading(_) => { /* Do nothing */ }
                    DataObjectState::Removed => {
                        unreachable!()
                    }
                };
            }
        }
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
        match self.tasks.get(&task_id).cloned() {
            None => {
                /* This may happen that task was computed or when work steal
                  was successful
                */
                log::debug!("Task not found, try to remove object");
                self.remove_data_by_id(task_id);
            }
            Some(task_ref) => {
                self.remove_task(task_ref, false);
            }
        }
    }

    pub fn steal_task(&mut self, task_id: TaskId) -> StealResponse {
        match self.tasks.get(&task_id).cloned() {
            None => StealResponse::NotHere,
            Some(task_ref) => {
                {
                    let task = task_ref.get_mut();
                    match task.state {
                        TaskState::Waiting(_) => { /* Continue */ }
                        TaskState::Running(_, _) | TaskState::Uploading(_, _, _) => {
                            return StealResponse::Running
                        }
                        TaskState::Removed => unreachable!(),
                    }
                }
                self.remove_task(task_ref, false);
                StealResponse::Ok
            }
        }
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

    pub fn finish_task(&mut self, task_ref: TaskRef, size: u64) {
        let id = task_ref.get().id;
        self.remove_task(task_ref, true);
        let message = FromWorkerMessage::TaskFinished(TaskFinishedMsg { id, size });
        self.send_message_to_server(message);
    }

    pub fn finish_task_failed(&mut self, task_ref: TaskRef, info: TaskFailInfo) {
        let id = task_ref.get().id;
        self.remove_task(task_ref, true);
        let message = FromWorkerMessage::TaskFailed(TaskFailedMsg { id, info });
        self.send_message_to_server(message);
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
        subworker_definitions: Vec<SubworkerDefinition>,
        inner_task_launcher: InnerTaskLauncher,
    ) -> Self {
        let ready_task_queue = ResourceWaitQueue::new(&configuration.resources);
        let self_ref = Self::wrap(WorkerState {
            worker_id,
            worker_addresses,
            sender,
            download_sender,
            configuration,
            inner_task_launcher,
            secret_key,
            subworker_definitions: subworker_definitions
                .into_iter()
                .map(|x| (x.id, x))
                .collect(),
            tasks: Default::default(),
            subworkers: Default::default(),
            free_subworkers: Default::default(),
            ready_task_queue,
            data_objects: Default::default(),
            random: SmallRng::from_entropy(),
            worker_connections: Default::default(),
            subworker_id_counter: 1, // 0 is reserved for "dummy" subworkers
            self_ref: None,
            start_task_scheduled: false,
            start_task_notify: Rc::new(Notify::new()),
            running_tasks: Default::default(),
        });
        self_ref.get_mut().self_ref = Some(self_ref.clone());
        self_ref
    }
}
