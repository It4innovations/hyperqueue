use crate::datasrv::DataObjectId;
use crate::internal::common::resources::map::{ResourceIdMap, ResourceRqMap};
use crate::internal::common::resources::{Allocation, ResourceRqId};
use crate::internal::common::stablemap::StableMap;
use crate::internal::common::{Map, Set, WrappedRcRefCell};
use crate::internal::datasrv::{DataObjectRef, DataStorage};
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    FromWorkerMessage, NewWorkerMsg, TaskFailedMsg, TaskFinishedMsg, TaskOutput, TaskUpdates,
    WorkerNotifyMessage,
};
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::configuration::WorkerConfiguration;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::internal::worker::data::download::WorkerDownloadManagerRef;
use crate::internal::worker::localcomm::LocalCommState;
use crate::internal::worker::resources::allocator::ResourceAllocator;
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::task::{Task, TaskState};
use crate::internal::worker::task_comm::RunningTaskComm;
use crate::launcher::TaskLauncher;
use crate::resources::{ResourceRequest, ResourceRequestVariants};
use crate::{Priority, TaskId};
use crate::{ResourceVariantId, WorkerId};
use orion::aead::SecretKey;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::SmallRng;
use tokio::sync::oneshot;

pub type TaskMap = StableMap<TaskId, Task>;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    comm: WorkerComm,
    tasks: TaskMap,
    pub(crate) allocator: ResourceAllocator,
    pub(crate) blocked_requests: Set<(ResourceRqId, ResourceVariantId)>,
    pub(crate) running_tasks: Set<TaskId>,

    pub(crate) worker_id: WorkerId,
    pub(crate) worker_addresses: Map<WorkerId, String>,

    pub(crate) configuration: WorkerConfiguration,
    /// If `Some`, forcefully overrides `configuration.overview_configuration.send_interval`.
    pub(crate) worker_overview_interval_override: Option<Duration>,
    pub(crate) task_launcher: Box<dyn TaskLauncher>,
    //pub(crate) secret_key: Option<Arc<SecretKey>>,
    pub(crate) start_time: Instant,

    pub(crate) lc_state: RefCell<LocalCommState>,
    pub(crate) data_storage: DataStorage,
    download_manager: Option<WorkerDownloadManagerRef>,
    tasks_waiting_for_data: Map<DataObjectId, Set<TaskId>>,
    placement_resolver: Map<DataObjectId, oneshot::Sender<Option<String>>>,

    pub resource_rq_map: ResourceRqMap,
    resource_id_map: ResourceIdMap,
    resource_label_map: ResourceLabelMap,

    state_ref: Option<WorkerStateRef>,

    secret_key: Option<Arc<SecretKey>>,
    server_uid: String,
}

impl WorkerState {
    pub(crate) fn comm(&mut self) -> &mut WorkerComm {
        &mut self.comm
    }

    pub(crate) fn state_ref(&self) -> WorkerStateRef {
        self.state_ref.as_ref().unwrap().clone()
    }

    pub fn server_uid(&self) -> &str {
        &self.server_uid
    }

    pub fn secret_key(&self) -> Option<&Arc<SecretKey>> {
        self.secret_key.as_ref()
    }

    pub fn allocator(&mut self) -> &mut ResourceAllocator {
        &mut self.allocator
    }

    pub fn process_resolved_placement(
        &mut self,
        data_id: DataObjectId,
        worker_id: Option<WorkerId>,
    ) {
        if let Some(sender) = self.placement_resolver.remove(&data_id) {
            let host = worker_id.and_then(|id| self.worker_addresses.get(&id).cloned());
            let _ = sender.send(host);
        }
    }

    pub(crate) fn set_download_manager(&mut self, dm_ref: WorkerDownloadManagerRef) {
        self.download_manager = Some(dm_ref);
    }

    #[inline]
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.get(&task_id)
    }

    #[inline]
    pub fn find_task(&self, task_id: TaskId) -> Option<&Task> {
        self.tasks.find(&task_id)
    }

    pub(crate) fn tasks_and_storage(&mut self) -> (&mut TaskMap, &mut DataStorage) {
        (&mut self.tasks, &mut self.data_storage)
    }

    pub(crate) fn remaining_time(&self) -> Option<Duration> {
        if let Some(limit) = self.configuration.time_limit {
            let life_time = Instant::now() - self.start_time;
            Some(limit - life_time)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.get_mut(&task_id)
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /*pub fn add_ready_task(&mut self, task: &Task) {
        self.ready_task_queue.add_task(&self.resource_rq_map, task);
        self.schedule_task_start();
    }

    pub fn add_ready_tasks(&mut self, resource_rq_map: &ResourceRqMap, tasks: &[Task]) {
        for task in tasks {
            self.ready_task_queue.add_task(resource_rq_map, task);
        }
        self.schedule_task_start();
    }*/

    pub fn add_task(&mut self, task: Task) {
        /*if task.is_ready() {
            log::debug!("Task {} is directly ready", task.id);
            self.add_ready_task(&task);
        } else {
            log::debug!(
                "Task {} is blocked by {} remote objects",
                task.id,
                task.get_waiting()
            );
        }*/
        self.tasks.insert(task);
    }

    #[inline]
    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    #[must_use]
    pub(crate) fn remove_task(&mut self, task_id: TaskId) -> Option<Task> {
        let task = self.tasks.remove(&task_id);
        if self.tasks.is_empty() {
            self.comm.notify_worker_is_empty();
        }
        task
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
            self.cancel_task(task_id);
        }
    }

    pub fn cancel_task(&mut self, task_id: TaskId) {
        log::debug!("Canceling task {task_id}");
        match self.tasks.find_mut(&task_id) {
            None => {
                /* This may happen that task was computed or when work steal
                  was successful
                */
                log::debug!("Task not found");
                return;
            }
            Some(task) => match &mut task.state {
                TaskState::Running { comm, .. } => {
                    comm.send_cancel_notification();
                    return;
                }
                TaskState::Waiting {
                    waiting_data_objects,
                } => {
                    let waiting_data_objects = *waiting_data_objects;
                    let task = self.remove_task(task_id).unwrap();
                    if waiting_data_objects > 0
                        && let Some(data_deps) = task.data_deps
                    {
                        todo!(); // FIXME: Just cancel finished downloads?
                        let mut dm = self.download_manager.as_ref().unwrap().get_mut();
                        for data_id in data_deps.iter() {
                            dm.cancel_download(*data_id);
                        }
                    };
                }
            },
        };
    }

    pub fn retract_task(&mut self, task_id: TaskId) -> bool {
        todo!()
        /*
        let response = match self.tasks.find(&task_id) {
            None => RetractResponse::NotHere,
            Some(task) => match task.state {
                TaskState::Waiting { .. } => RetractResponse::Ok,
                TaskState::Running { .. } => RetractResponse::Running,
            },
        };
        if let RetractResponse::Ok = &response {
            self.remove_task(task_id, false, false);
        }
        response*/
    }

    pub fn ask_for_data_placement(
        &mut self,
        data_id: DataObjectId,
        sender: oneshot::Sender<Option<String>>,
    ) {
        self.placement_resolver.insert(data_id, sender);
        self.comm
            .send_message_to_server(FromWorkerMessage::PlacementQuery(data_id))
    }

    /*pub fn finish_task(&mut self, task_id: TaskId) {
        let output_ids = self.remove_task(task_id, true, true);
        let message = FromWorkerMessage::TaskFinished(TaskFinishedMsg {
            task_id: task_id,
            outputs: output_ids,
        });
        self.comm.send_message_to_server(message);
    }

    pub fn finish_task_failed(&mut self, task_id: TaskId, info: TaskFailInfo) {
        self.remove_task(task_id, true, false);
        let message = FromWorkerMessage::TaskFailed(TaskFailedMsg {
            task_id: task_id,
            info,
        });
        self.comm.send_message_to_server(message);
    }

    pub fn finish_task_cancel(&mut self, task_id: TaskId) {
        self.remove_task(task_id, true, false);
    }*/

    #[inline]
    pub fn get_resource_map(&self) -> &ResourceIdMap {
        &self.resource_id_map
    }

    pub fn get_resource_maps(&self) -> (&ResourceIdMap, &ResourceRqMap) {
        (&self.resource_id_map, &self.resource_rq_map)
    }

    #[inline]
    pub fn get_resource_rq_map(&self) -> &ResourceRqMap {
        &self.resource_rq_map
    }

    #[inline]
    pub fn get_resource_rq(
        &self,
        rq_id: ResourceRqId,
        r_id: ResourceVariantId,
    ) -> &ResourceRequest {
        self.resource_rq_map.get(rq_id).get(r_id)
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
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        log::debug!("Lost worker={worker_id} announced");
        assert!(self.worker_addresses.remove(&worker_id).is_some());
    }

    pub fn send_notify(&mut self, task_id: TaskId, message: Box<[u8]>) {
        self.comm
            .send_message_to_server(FromWorkerMessage::Notify(WorkerNotifyMessage {
                task_id,
                message,
            }))
    }

    pub fn on_download_finished(&mut self, data_id: DataObjectId, data_ref: DataObjectRef) {
        todo!()
        /*
        self.data_storage.add_stats_remote_download(data_ref.size());
        self.data_storage.put_object(data_id, data_ref).unwrap();
        self.comm
            .send_message_to_server(FromWorkerMessage::NewPlacement(data_id));
        if let Some(tasks) = self.tasks_waiting_for_data.remove(&data_id) {
            let mut new_ready = false;
            for task_id in tasks {
                if let Some(task) = self.tasks.find_mut(&task_id) {
                    log::debug!("Task {} is directly ready", task.id);
                    if task.decrease_waiting_count() {
                        self.ready_task_queue.add_task(&self.resource_rq_map, task);
                        new_ready = true;
                    }
                }
            }
            if new_ready {
                self.schedule_task_start();
            }
        }*/
    }

    pub fn on_download_failed(&mut self, data_id: DataObjectId) {
        todo!()
        /*log::debug!("Data {data_id} download failed");
        if let Some(tasks) = self.tasks_waiting_for_data.remove(&data_id) {
            for task_id in tasks {
                if let Some(task) = self.tasks.find_mut(&task_id) {
                    log::debug!("Task {task_id} failed because of failed download");
                    let input_idx = task
                        .data_deps
                        .as_ref()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .find(|(_, id)| **id == data_id)
                        .unwrap()
                        .0;
                    self.remove_task(task_id, false, false);
                    let message = format!(
                        "Fails to download data object {data_id}; it has input index {input_idx}"
                    );
                    let message = FromWorkerMessage::TaskFailed(TaskFailedMsg {
                        task_id: task_id,
                        info: TaskFailInfo { message },
                    });
                    self.comm.send_message_to_server(message);
                }
            }
        }*/
    }

    pub fn register_resource_rq(&mut self, rqv: ResourceRequestVariants) -> ResourceRqId {
        self.resource_rq_map.insert(rqv)
    }

    pub fn download_object(&mut self, data_id: DataObjectId, task_id: TaskId, priority: Priority) {
        self.tasks_waiting_for_data
            .entry(data_id)
            .or_default()
            .insert(task_id);
        self.download_manager
            .as_ref()
            .unwrap()
            .get_mut()
            .download_object(data_id, priority);
    }
}

impl WorkerStateRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        comm: WorkerComm,
        worker_id: WorkerId,
        configuration: WorkerConfiguration,
        secret_key: Option<Arc<SecretKey>>,
        resource_map: ResourceIdMap,
        resource_rq_map: ResourceRqMap,
        task_launcher: Box<dyn TaskLauncher>,
        server_uid: String,
    ) -> Self {
        let resource_label_map = ResourceLabelMap::new(&configuration.resources, &resource_map);
        let allocator =
            ResourceAllocator::new(&configuration.resources, &resource_map, &resource_label_map);
        //let ready_task_queue = ResourceWaitQueue::new(allocator);
        let now = Instant::now();

        let state = Self::wrap(WorkerState {
            comm,
            worker_id,
            configuration,
            worker_overview_interval_override: None,
            task_launcher,
            server_uid,
            secret_key,
            tasks: Default::default(),
            allocator,
            blocked_requests: Set::new(),
            running_tasks: Default::default(),
            start_time: now,
            resource_id_map: resource_map,
            resource_rq_map,
            resource_label_map,
            worker_addresses: Default::default(),
            lc_state: RefCell::new(LocalCommState::new()),
            data_storage: DataStorage::new(),
            download_manager: None,
            tasks_waiting_for_data: Map::new(),
            placement_resolver: Map::new(),
            state_ref: None,
        });
        state.get_mut().state_ref = Some(state.clone());
        state
    }
}
