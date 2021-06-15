use orion::aead::SecretKey;

use crate::common::error::DsError;
use crate::common::resources::ResourceRequest;
use crate::common::trace::trace_task_remove;
use crate::common::{IdCounter, Map, Set, WrappedRcRefCell};
use crate::messages::common::SubworkerDefinition;
use crate::messages::gateway::ServerInfo;
use crate::server::task::{Task, TaskRef, TaskRuntimeState};
use crate::server::worker::Worker;
use crate::server::worker_load::WorkerResources;
use crate::{TaskId, WorkerId};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
pub struct Core {
    tasks: Map<TaskId, TaskRef>,
    workers: Map<WorkerId, Worker>,

    /* Scheduler items */
    parked_resources: Set<WorkerResources>, // Resources of workers that has flag NOTHING_TO_LOAD
    ready_to_assign: Vec<TaskRef>,
    has_new_tasks: bool,

    sleeping_tasks: Vec<TaskRef>, // Tasks that cannot be scheduled to any available worker

    maximal_task_id: TaskId,
    worker_id_counter: IdCounter,
    scatter_counter: usize,
    worker_listen_port: u16,

    idle_timeout: Option<Duration>,

    subworker_definitions: Vec<SubworkerDefinition>,
    secret_key: Option<Arc<SecretKey>>,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl CoreRef {
    pub fn new(
        worker_listen_port: u16,
        secret_key: Option<Arc<SecretKey>>,
        idle_timeout: Option<Duration>,
    ) -> Self {
        /*let mut core = Core::default();
        core.worker_listen_port = worker_listen_port;
        core.secret_key = secret_key;*/
        CoreRef::wrap(Core {
            worker_listen_port,
            secret_key,
            idle_timeout,
            ..Default::default()
        })
    }
}

impl Core {
    pub fn new_worker_id(&mut self) -> WorkerId {
        self.worker_id_counter.next_id()
    }

    #[inline]
    pub fn is_used_task_id(&self, task_id: TaskId) -> bool {
        task_id <= self.maximal_task_id
    }

    pub fn idle_timeout(&self) -> &Option<Duration> {
        &self.idle_timeout
    }

    pub fn get_and_move_scatter_counter(&mut self, size: usize) -> usize {
        let c = self.scatter_counter;
        self.scatter_counter += size;
        c
    }

    pub fn park_workers(&mut self) {
        for worker in self.workers.values_mut() {
            if worker.is_underloaded() && worker.tasks().iter().all(|t| t.get().is_running()) {
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

    pub fn get_subworker_definitions(&self) -> &Vec<SubworkerDefinition> {
        &self.subworker_definitions
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
        let task_id = task_ref.get().id();
        {
            let task = task_ref.get();
            if task.is_ready() {
                self.add_ready_to_assign(task_ref.clone());
            }
        }
        self.has_new_tasks = true;
        assert!(self.tasks.insert(task_id, task_ref).is_none());
    }

    #[inline(never)]
    fn wakeup_parked_resources(&mut self) {
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

    #[must_use]
    pub fn remove_task(&mut self, task: &mut Task) -> TaskRuntimeState {
        trace_task_remove(task.id);
        assert!(!task.has_consumers());
        let task_ref = self.tasks.remove(&task.id).unwrap();
        if task.is_ready() {
            self.ready_to_assign
                .iter()
                .position(|t| t == &task_ref)
                .map(|idx| self.ready_to_assign.remove(idx));
        }
        std::mem::replace(&mut task.state, TaskRuntimeState::Released)
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

    pub fn add_subworker_definition(
        &mut self,
        subworker_def: SubworkerDefinition,
    ) -> crate::Result<()> {
        if subworker_def.id == 0 {
            return Err(DsError::GenericError("Subworker id 0 is reserved".into()));
        }
        if self
            .subworker_definitions
            .iter()
            .any(|d| d.id == subworker_def.id)
        {
            return Err(DsError::GenericError(format!(
                "Subworker id {} is already reserved",
                subworker_def.id
            )));
        }
        self.subworker_definitions.push(subworker_def);
        Ok(())
    }

    pub fn sanity_check(&self) {
        let fw_check = |task: &Task| {
            for t in &task.inputs {
                assert!(t.get().is_finished());
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
            assert_eq!(
                self.parked_resources.contains(&worker.resources),
                worker.is_parked()
            );
            worker.sanity_check();
        }

        for (task_id, task_ref) in &self.tasks {
            let task = task_ref.get();
            assert_eq!(task.id, *task_id);
            assert!(
                task.type_id == 0
                    || self
                        .subworker_definitions
                        .iter()
                        .any(|d| d.id == task.type_id)
            );
            match &task.state {
                TaskRuntimeState::Waiting(winfo) => {
                    let mut count = 0;
                    for t in &task.inputs {
                        if !t.get().is_finished() {
                            count += 1;
                        }
                    }
                    for t in task.get_consumers() {
                        assert!(t.get().is_waiting());
                    }
                    assert_eq!(winfo.unfinished_deps, count);
                    worker_check(self, &task_ref, 0);
                    assert!(task.is_fresh());
                }

                TaskRuntimeState::Assigned(wid) | TaskRuntimeState::Running(wid) => {
                    assert!(!task.is_fresh());
                    fw_check(&task);
                    worker_check(self, &task_ref, *wid);
                }

                TaskRuntimeState::Stealing(_, target) => {
                    assert!(!task.is_fresh());
                    fw_check(&task);
                    worker_check(self, &task_ref, target.unwrap_or(0));
                }

                TaskRuntimeState::Finished(_) => {
                    for t in &task.inputs {
                        assert!(t.get().is_finished());
                    }
                }

                TaskRuntimeState::Released => {
                    unreachable!()
                }
            }
        }
    }

    pub fn set_secret_key(&mut self, secret_key: Option<Arc<SecretKey>>) {
        self.secret_key = secret_key
    }

    pub fn secret_key(&self) -> &Option<Arc<SecretKey>> {
        &self.secret_key
    }
}

/*
/// Returns task duration as specified by Dask.
/// Converts from UNIX in seconds to a microseconds.
fn get_task_duration(msg: &TaskFinishedMsg) -> (u64, u64) {
    msg.startstops
        .iter()
        .find(|map| map[b"action" as &[u8]].as_str().unwrap() == "compute")
        .map(|map| {
            (
                (map[b"start" as &[u8]].as_f64().unwrap() * 1_000_000f64) as u64,
                (map[b"stop" as &[u8]].as_f64().unwrap() * 1_000_000f64) as u64,
            )
        })
        .unwrap_or((0, 0))
}*/

#[cfg(test)]
mod tests {
    use crate::server::core::Core;
    use crate::server::task::{TaskRef, TaskRuntimeState};
    use crate::server::test_util::task;

    impl Core {
        pub fn remove_from_ready_to_assign(&mut self, task_ref: &TaskRef) {
            let p = self
                .ready_to_assign
                .iter()
                .position(|x| x == task_ref)
                .unwrap();
            self.ready_to_assign.remove(p);
        }
    }

    #[test]
    fn add_remove() {
        let mut core = Core::default();
        let t = task(101);
        core.add_task(t.clone());
        assert_eq!(core.get_task_by_id(101).unwrap(), &t);
        assert!(match core.remove_task(&mut t.get_mut()) {
            TaskRuntimeState::Waiting(_) => true,
            _ => false,
        });
        assert_eq!(core.get_task_by_id(101), None);
    }

    #[test]
    fn task_duration() {
        /*assert_eq!(
            get_task_duration(&TaskFinishedMsg {
                key: "null".into(),
                nbytes: 16,
                r#type: vec![1, 2, 3],
                startstops: vec!(
                    startstop_item("send", 100.0, 200.0),
                    startstop_item("compute", 200.134, 300.456)
                ),
            }),
            (200134000, 300456000)
        );*/
    }

    #[test]
    fn task_duration_missing() {
        /*assert_eq!(
            get_task_duration(&TaskFinishedMsg {
                key: "null".into(),
                nbytes: 16,
                r#type: vec![1, 2, 3],
                startstops: vec!(),
            }),
            (0, 0)
        );*/
    }
}
