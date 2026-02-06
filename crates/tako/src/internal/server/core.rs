use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::internal::common::resources::map::{
    GlobalResourceMapping, ResourceIdMap, ResourceRqMap,
};
use crate::internal::common::resources::{ResourceId, ResourceRequestVariants, ResourceRqId};
use crate::internal::common::{Set, WrappedRcRefCell};
use crate::internal::scheduler::multinode::MultiNodeQueue;
use crate::internal::scheduler2::{TaskQueue, TaskQueues};
use crate::internal::server::dataobj::{DataObjectHandle, ObjsToRemoveFromWorkers};
use crate::internal::server::dataobjmap::DataObjectMap;
use crate::internal::server::rpc::ConnectionDescriptor;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::{Worker, WorkerAssignment};
use crate::internal::server::workergroup::WorkerGroup;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::server::workermap::WorkerMap;
use crate::{Map, TaskId, WorkerId};
use orion::aead::SecretKey;
use serde_json::json;

pub(crate) type CustomConnectionHandler = Box<dyn Fn(ConnectionDescriptor)>;

pub(crate) struct CoreSplitMut<'a> {
    pub task_map: &'a mut TaskMap,
    pub worker_map: &'a mut WorkerMap,
    pub request_map: &'a ResourceRqMap,
    pub task_queues: &'a mut TaskQueues,
    pub data_objects: &'a mut DataObjectMap,
    pub worker_groups: &'a mut Map<String, WorkerGroup>,
}

pub(crate) struct CoreSplit<'a> {
    pub task_map: &'a TaskMap,
    pub worker_map: &'a WorkerMap,
    pub request_map: &'a ResourceRqMap,
    pub task_queues: &'a TaskQueues,
    pub data_objects: &'a DataObjectMap,
    pub worker_groups: &'a Map<String, WorkerGroup>,
}

#[derive(Default)]
pub struct Core {
    tasks: TaskMap,
    workers: WorkerMap,
    resource_map: GlobalResourceMapping,
    task_queues: TaskQueues,
    data_objects: DataObjectMap,
    worker_groups: Map<String, WorkerGroup>,

    maximal_task_id: TaskId,
    worker_id_counter: u32,
    worker_listen_port: u16,

    idle_timeout: Option<Duration>,

    secret_key: Option<Arc<SecretKey>>,
    server_uid: String,
    custom_conn_handler: Option<CustomConnectionHandler>,

    // How many streaming clients currently want to receive worker overviews
    worker_overview_listeners: u64,
}

pub(crate) type CoreRef = WrappedRcRefCell<Core>;

impl CoreRef {
    pub fn new(
        worker_listen_port: u16,
        secret_key: Option<Arc<SecretKey>>,
        idle_timeout: Option<Duration>,
        custom_conn_handler: Option<CustomConnectionHandler>,
        server_uid: String,
        worker_id_initial_value: WorkerId,
    ) -> Self {
        CoreRef::wrap(Core {
            worker_listen_port,
            secret_key,
            idle_timeout,
            custom_conn_handler,
            server_uid,
            worker_id_counter: worker_id_initial_value.as_num(),
            ..Default::default()
        })
    }
}

impl Core {
    #[inline]
    pub fn split_mut(&mut self) -> CoreSplitMut {
        CoreSplitMut {
            task_map: &mut self.tasks,
            worker_map: &mut self.workers,
            request_map: self.resource_map.get_resource_rq_map(),
            task_queues: &mut self.task_queues,
            data_objects: &mut self.data_objects,
            worker_groups: &mut self.worker_groups,
        }
    }

    #[inline]
    pub fn split(&self) -> CoreSplit {
        CoreSplit {
            task_map: &self.tasks,
            worker_map: &self.workers,
            request_map: self.resource_map.get_resource_rq_map(),
            task_queues: &self.task_queues,
            data_objects: &self.data_objects,
            worker_groups: &self.worker_groups,
        }
    }

    pub fn task_queues_mut(&mut self) -> &mut TaskQueues {
        &mut self.task_queues
    }

    pub fn new_worker_id(&mut self) -> WorkerId {
        self.worker_id_counter += 1;
        WorkerId::new(self.worker_id_counter)
    }

    pub fn worker_counter(&self) -> u32 {
        self.worker_id_counter
    }

    pub fn worker_groups(&self) -> &Map<String, WorkerGroup> {
        &self.worker_groups
    }

    #[inline]
    pub fn is_used_task_id(&self, task_id: TaskId) -> bool {
        task_id <= self.maximal_task_id
    }

    pub fn idle_timeout(&self) -> &Option<Duration> {
        &self.idle_timeout
    }

    pub fn server_uid(&self) -> &str {
        &self.server_uid
    }

    pub fn worker_overview_listeners(&self) -> u64 {
        self.worker_overview_listeners
    }
    pub fn worker_overview_listeners_mut(&mut self) -> &mut u64 {
        &mut self.worker_overview_listeners
    }

    pub fn park_workers(&mut self) {
        todo!()
        /*for worker in self.workers.values_mut() {
            if worker.is_underloaded()
                && worker
                    .sn_tasks()
                    .iter()
                    .all(|&task_id| self.tasks.get_task(task_id).is_sn_running())
            {
                log::debug!("Parking worker {}", worker.id);
                worker.set_parked_flag(true);
                self.parked_resources.insert(worker.resources.clone());
            }
        }*/
    }

    pub fn get_worker_listen_port(&self) -> u16 {
        self.worker_listen_port
    }

    pub fn new_worker(&mut self, worker: Worker) {
        let worker_id = worker.id;
        if let Some(g) = self.worker_groups.get_mut(&worker.configuration.group) {
            g.new_worker(worker_id);
        } else {
            let mut worker_ids = Set::new();
            worker_ids.insert(worker_id);
            self.worker_groups.insert(
                worker.configuration.group.clone(),
                WorkerGroup::new(worker_ids),
            );
        }
        self.workers.insert(worker_id, worker);
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) -> Worker {
        let worker = self.workers.get_worker(worker_id);
        let group = self
            .worker_groups
            .get_mut(&worker.configuration.group)
            .unwrap();
        group.remove_worker(worker_id);
        if group.is_empty() {
            self.worker_groups.remove(&worker.configuration.group);
        }
        self.workers.remove(&worker_id).unwrap()
    }

    #[inline]
    pub fn get_worker_by_id(&self, id: WorkerId) -> Option<&Worker> {
        self.workers.get(&id)
    }

    #[inline]
    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &Worker {
        self.workers.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={id}");
        })
    }

    #[inline]
    pub fn get_worker_mut_by_id_or_panic(&mut self, id: WorkerId) -> &mut Worker {
        self.workers.get_mut(&id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={id}");
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

    pub(crate) fn add_data_object(&mut self, data_obj: DataObjectHandle) {
        self.data_objects.insert(data_obj);
    }

    #[inline(always)]
    pub fn dataobj_map(&self) -> &DataObjectMap {
        &self.data_objects
    }

    #[inline(always)]
    pub fn data_objects_mut(&mut self) -> &mut DataObjectMap {
        &mut self.data_objects
    }

    pub fn add_task(&mut self, task: Task) {
        if task.is_ready() {
            self.task_queues.get_mut(task.resource_rq_id).add(&task);
        }
        assert!(self.tasks.insert(task).is_none());
    }

    // TODO: move to TaskMap
    /// Removes a single task.
    /// It can still remain in [`ready_to_assign`], where it will remain until the scheduler picks
    /// it up.
    #[must_use]
    pub fn remove_task(
        &mut self,
        task_id: TaskId,
        objs_to_remove: &mut ObjsToRemoveFromWorkers,
    ) -> (TaskRuntimeState, ResourceRqId) {
        let task = self
            .tasks
            .remove(task_id)
            .expect("Trying to remove non-existent task");
        if matches!(&task.state, TaskRuntimeState::Waiting { unfinished_deps } if *unfinished_deps > 0)
        {
            for input_id in task.task_deps {
                if let Some(input) = self.find_task_mut(input_id) {
                    assert!(input.remove_consumer(task_id));
                }
            }
        }
        if !task.data_deps.is_empty() {
            for data_id in &task.data_deps {
                self.data_objects
                    .try_decrease_ref_count(*data_id, objs_to_remove);
            }
        }
        (task.state, task.resource_rq_id)
    }

    /// Removes multiple tasks at once, to reduce memory consumption
    pub fn remove_tasks_batched(
        &mut self,
        tasks: &Set<TaskId>,
        objs_to_remove: &mut ObjsToRemoveFromWorkers,
    ) {
        for &task_id in tasks {
            let resource_rq_id = self.remove_task(task_id, objs_to_remove).1;
            todo!() // REMOVE FROM TASKS QUEUES
            //self.task_queues[resource_rq_id.as_usize()].remove(task);
        }
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

    #[cfg(test)]
    pub fn sanity_check(&self) {
        let fw_check = |task: &Task| {
            for task_dep in &task.task_deps {
                assert!(self.tasks.find_task(*task_dep).is_none());
            }
            for &task_id in task.get_consumers() {
                assert!(self.tasks.get_task(task_id).is_waiting());
            }
        };

        let worker_check_sn = |core: &Core, task_id: TaskId, wid: WorkerId| {
            for (worker_id, worker) in core.workers.iter() {
                match worker.assignment() {
                    WorkerAssignment::Sn(s) => {
                        if wid == *worker_id {
                            assert!(s.assign_tasks.contains(&task_id));
                        } else {
                            assert!(!s.assign_tasks.contains(&task_id));
                        }
                    }
                    WorkerAssignment::Mn(m) => {
                        assert_ne!(m.task_id, task_id);
                    }
                }
            }
        };

        for (worker_id, worker) in self.workers.iter() {
            assert_eq!(worker.id, *worker_id);
            worker.sanity_check(&self.tasks, self.resource_map.get_resource_rq_map());
        }

        for data in self.data_objects.iter() {
            assert!(data.ref_count() > 0);
        }

        for task_id in self.tasks.task_ids() {
            let task = self.get_task(task_id);
            assert_eq!(task.id, task_id);
            match &task.state {
                TaskRuntimeState::Waiting { unfinished_deps } => {
                    let mut count = 0;
                    for task_dep in &task.task_deps {
                        if !self
                            .tasks
                            .find_task(*task_dep)
                            .is_none_or(|t| t.is_finished())
                        {
                            count += 1;
                        }
                    }
                    for &task_id in task.get_consumers() {
                        assert!(self.tasks.get_task(task_id).is_waiting());
                    }
                    assert_eq!(*unfinished_deps, count);
                    worker_check_sn(self, task.id, 0.into());
                }

                TaskRuntimeState::Assigned { worker_id, .. }
                | TaskRuntimeState::Running { worker_id, .. } => {
                    fw_check(task);
                    worker_check_sn(self, task.id, *worker_id);
                }
                /*TaskRuntimeState::Retracting { source: _ } => {
                    fw_check(task);
                    worker_check_sn(self, task.id, WorkerId::new(0));
                }

                TaskRuntimeState::Stealing {
                    source: _, target, ..
                } => {
                    fw_check(task);
                    worker_check_sn(self, task.id, *target);
                }*/
                TaskRuntimeState::Finished => {
                    for task_dep in &task.task_deps {
                        assert!(self.tasks.find_task(*task_dep).is_none());
                    }
                }
                TaskRuntimeState::RunningMultiNode(ws) => {
                    assert!(!ws.is_empty());
                    fw_check(task);
                    let mut set = Set::new();
                    for worker_id in ws {
                        assert!(self.workers.contains_key(worker_id));
                        assert!(set.insert(*worker_id));
                    }
                    for (worker_id, worker) in self.workers.iter() {
                        if set.contains(worker_id) {
                            let mn = worker.mn_assignment().unwrap();
                            assert_eq!(mn.task_id, task_id);
                            assert_eq!(ws[0] != *worker_id, mn.reservation_only);
                        } else {
                            match worker.assignment() {
                                WorkerAssignment::Sn(sn) => {
                                    assert!(!sn.assign_tasks.contains(&task_id));
                                }
                                WorkerAssignment::Mn(mn) => {
                                    assert_ne!(mn.task_id, task_id);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn get_or_create_resource_id(&mut self, name: &str) -> ResourceId {
        self.resource_map.get_or_create_resource_id(name)
    }

    pub fn convert_client_resource_rq(
        &mut self,
        resources: &crate::gateway::ResourceRequestVariants,
    ) -> ResourceRequestVariants {
        self.resource_map.convert_client_resource_rq(resources)
    }

    #[inline]
    pub fn resource_map(&self) -> &GlobalResourceMapping {
        &self.resource_map
    }

    #[inline]
    pub fn resource_map_mut(&mut self) -> &mut GlobalResourceMapping {
        &mut self.resource_map
    }

    #[inline]
    pub fn create_resource_map(&self) -> ResourceIdMap {
        self.resource_map.create_resource_id_map()
    }

    pub fn get_resource_rq_map(&self) -> &ResourceRqMap {
        self.resource_map.get_resource_rq_map()
    }

    #[inline]
    pub fn get_resource_rq(&self, rq_id: ResourceRqId) -> &ResourceRequestVariants {
        self.resource_map.get_resource_rq_map().get(rq_id)
    }

    pub fn secret_key(&self) -> Option<&Arc<SecretKey>> {
        self.secret_key.as_ref()
    }

    pub fn try_release_memory(&mut self) {
        self.tasks.shrink_to_fit();
        self.workers.shrink_to_fit();
        self.worker_groups.shrink_to_fit();
        self.task_queues.shrink_to_fit();
    }

    pub fn dump(&self, now: Instant) -> serde_json::Value {
        json!({
            "workers": self.workers.values().map(|w| w.dump(now)).collect::<Vec<_>>(),
            "worker_groups": self.worker_groups.iter().map(|(k, v)|
                json!({"name": k,
                       "workers": v.worker_ids().collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            "tasks": self.tasks.tasks().map(|t| t.dump()).collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::server::core::Core;
    use crate::internal::server::dataobj::ObjsToRemoveFromWorkers;
    use crate::internal::server::task::Task;
    use crate::internal::server::task::TaskRuntimeState;
    use crate::internal::server::worker::Worker;
    use crate::internal::server::workergroup::WorkerGroup;

    use crate::tests::utils::env::TestEnv;

    use crate::{TaskId, WorkerId};

    impl Core {
        pub fn worker_group(&self, group_name: &str) -> Option<&WorkerGroup> {
            self.worker_groups.get(group_name)
        }

        pub fn assert_task_condition<F: Fn(&Task) -> bool>(&self, task_ids: &[TaskId], op: F) {
            for task_id in task_ids {
                if !op(self.get_task(*task_id)) {
                    panic!("Task {task_id} does not satisfy the condition");
                }
            }
        }

        pub fn assert_worker_condition<F: Fn(&Worker) -> bool>(
            &self,
            worker_ids: &[WorkerId],
            op: F,
        ) {
            for worker_id in worker_ids {
                if !op(self.get_worker_by_id_or_panic(*worker_id)) {
                    panic!("Worker {worker_id} does not satisfy the condition");
                }
            }
        }

        pub fn assert_waiting(&self, task_ids: &[TaskId]) {
            self.assert_task_condition(task_ids, |t| t.is_waiting());
        }

        pub fn assert_ready(&self, task_ids: &[TaskId]) {
            self.assert_task_condition(task_ids, |t| t.is_ready());
        }

        pub fn assert_assigned(&self, task_ids: &[TaskId]) {
            self.assert_task_condition(task_ids, |t| t.is_assigned());
        }

        pub fn assert_running(&self, task_ids: &[TaskId]) {
            self.assert_task_condition(task_ids, |t| t.is_sn_running());
        }

        pub fn assert_underloaded(&self, worker_ids: &[WorkerId]) {
            todo!()
            //self.assert_worker_condition(worker_ids, |w| w.is_underloaded());
        }

        pub fn assert_not_underloaded(&self, worker_ids: &[WorkerId]) {
            todo!()
            //self.assert_worker_condition(worker_ids, |w| !w.is_underloaded());
        }

        pub fn remove_from_ready_queue(&mut self, task_id: TaskId) {
            let task = self.get_task(task_id);
            let resource_rq_id = task.resource_rq_id;
            let priority = task.priority();
            self.task_queues
                .get_mut(resource_rq_id)
                .remove(task_id, priority);
        }
    }

    #[test]
    fn add_remove() {
        let mut rt = TestEnv::new();
        let t = rt.new_task_default();
        let mut objs_to_remove = ObjsToRemoveFromWorkers::new();
        assert!(matches!(
            rt.core().remove_task(t, &mut objs_to_remove).0,
            TaskRuntimeState::Waiting { .. }
        ));
        assert_eq!(rt.core().find_task(t), None);
    }
}
