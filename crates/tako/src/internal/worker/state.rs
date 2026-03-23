use crate::internal::common::resources::ResourceRqId;
use crate::internal::common::resources::map::{ResourceIdMap, ResourceRqMap};
use crate::internal::common::stablemap::StableMap;
use crate::internal::common::{Map, Set, WrappedRcRefCell};
use crate::internal::messages::worker::{FromWorkerMessage, NewWorkerMsg, WorkerNotifyMessage};
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::configuration::WorkerConfiguration;
use std::cell::RefCell;
use std::time::{Duration, Instant};

use crate::TaskId;
use crate::internal::worker::localcomm::LocalCommState;
use crate::internal::worker::resources::allocator::ResourceAllocator;
use crate::internal::worker::resources::map::ResourceLabelMap;
use crate::internal::worker::task::{RunningTask, Task};
use crate::launcher::TaskLauncher;
use crate::resources::ResourceRequestVariants;
use crate::{ResourceVariantId, WorkerId};

pub(crate) type TaskMap = StableMap<TaskId, RunningTask>;

pub(crate) type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub(crate) struct WorkerState {
    comm: WorkerComm,
    pub(crate) allocator: ResourceAllocator,
    pub(crate) blocked_requests: Set<(ResourceRqId, ResourceVariantId)>,
    pub(crate) running_tasks: TaskMap,
    pub(crate) prefilled_tasks: Map<ResourceRqId, Vec<Task>>,

    pub(crate) worker_id: WorkerId,
    pub(crate) worker_addresses: Map<WorkerId, String>,

    pub(crate) configuration: WorkerConfiguration,
    /// If `Some`, forcefully overrides `configuration.overview_configuration.send_interval`.
    pub(crate) worker_overview_interval_override: Option<Duration>,
    pub(crate) task_launcher: Box<dyn TaskLauncher>,
    pub(crate) start_time: Instant,

    pub(crate) lc_state: RefCell<LocalCommState>,

    pub resource_rq_map: ResourceRqMap,
    resource_id_map: ResourceIdMap,
    resource_label_map: ResourceLabelMap,

    state_ref: Option<WorkerStateRef>,
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

    #[inline]
    #[cfg(test)]
    pub fn get_running_task(&self, task_id: TaskId) -> &RunningTask {
        self.running_tasks.get(&task_id)
    }

    #[inline]
    pub fn find_running_task(&self, task_id: TaskId) -> Option<&RunningTask> {
        self.running_tasks.find(&task_id)
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
    pub fn get_running_task_mut(&mut self, task_id: TaskId) -> &mut RunningTask {
        self.running_tasks.get_mut(&task_id)
    }

    pub fn is_empty(&self) -> bool {
        self.running_tasks.is_empty()
    }

    #[must_use]
    pub(crate) fn remove_running_task(&mut self, task_id: TaskId) -> Option<RunningTask> {
        let task = self.running_tasks.remove(&task_id);
        if self.running_tasks.is_empty() {
            self.comm.notify_worker_is_empty();
        }
        task
    }

    pub fn drop_non_running_tasks(&mut self) {
        self.prefilled_tasks = Default::default();
    }

    pub fn cancel_task(&mut self, task_id: TaskId) {
        log::debug!("Canceling task {task_id}");
        match self.running_tasks.find_mut(&task_id) {
            None => {
                /* This may happen that task was computed or when work steal
                  was successful
                */
                log::debug!("Task not found");
            }
            Some(task) => task.cancel(),
        }
    }

    pub fn retract_tasks(&mut self, task_ids: &[TaskId]) -> Vec<TaskId> {
        let task_set: Set<TaskId> = task_ids.iter().copied().collect();
        let mut out: Vec<TaskId> = Vec::with_capacity(task_set.len());
        self.prefilled_tasks.values_mut().for_each(|tasks| {
            tasks.retain(|t| {
                if task_set.contains(&t.id) {
                    out.push(t.id);
                    false
                } else {
                    true
                }
            })
        });
        out
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

    pub fn register_resource_rq(&mut self, rqv: ResourceRequestVariants) -> ResourceRqId {
        self.resource_rq_map.insert(rqv)
    }
}

impl WorkerStateRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        comm: WorkerComm,
        worker_id: WorkerId,
        configuration: WorkerConfiguration,
        resource_map: ResourceIdMap,
        resource_rq_map: ResourceRqMap,
        task_launcher: Box<dyn TaskLauncher>,
        server_uid: String,
    ) -> Self {
        let resource_label_map = ResourceLabelMap::new(&configuration.resources, &resource_map);
        let allocator =
            ResourceAllocator::new(&configuration.resources, &resource_map, &resource_label_map);
        let now = Instant::now();

        let state = Self::wrap(WorkerState {
            comm,
            worker_id,
            configuration,
            worker_overview_interval_override: None,
            task_launcher,
            server_uid,
            allocator,
            blocked_requests: Set::new(),
            running_tasks: Default::default(),
            prefilled_tasks: Default::default(),
            start_time: now,
            resource_id_map: resource_map,
            resource_rq_map,
            resource_label_map,
            worker_addresses: Default::default(),
            lc_state: RefCell::new(LocalCommState::new()),
            state_ref: None,
        });
        state.get_mut().state_ref = Some(state.clone());
        state
    }
}
