use crate::common::{IdCounter, Map, WrappedRcRefCell};
use crate::{TaskId, WorkerId};
use crate::server::gateway::Gateway;

use crate::server::task::{Task, TaskRef, TaskRuntimeState};
use crate::common::trace::trace_task_remove;
use crate::server::worker::Worker;

#[derive(Default)]
pub struct Core {
    tasks: Map<TaskId, TaskRef>,
    workers: Map<WorkerId, Worker>,

    task_id_counter: IdCounter,
    worker_id_counter: IdCounter,

    scatter_counter: usize,
}

pub type CoreRef = WrappedRcRefCell<Core>;

impl CoreRef {
    pub fn new() -> Self {
        CoreRef::wrap(Core::default())
    }
}

impl Core {
    #[inline]
    pub fn new_worker_id(&mut self) -> WorkerId {
        self.worker_id_counter.next()
    }

    #[inline]
    pub fn new_task_id(&mut self) -> TaskId {
        self.task_id_counter.next()
    }

    #[inline]
    pub fn get_and_move_scatter_counter(&mut self, size: usize) -> usize {
        let c = self.scatter_counter;
        self.scatter_counter += size;
        c
    }

    #[inline]
    pub fn gateway(&self) -> &dyn Gateway {
        todo!()
        //self.gateway.as_ref()
    }

    pub fn new_worker(&mut self, worker: Worker) {
        let worker_id = worker.id;
        self.workers.insert(worker_id, worker);
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        assert!(self.workers.remove(&worker_id).is_some());
    }

    #[inline]
    pub fn get_worker_by_address(&self, address: &str) -> Option<&Worker> {
        self.workers.values().find(|w| w.address() == address)
    }

    pub fn get_worker_addresses(&self) -> Map<WorkerId, String> {
        self.workers
            .values()
            .map(|w| {
                (w.id, w.listen_address.clone())
            })
            .collect()
    }

    #[inline]
    pub fn get_worker_by_id_or_panic(&self, id: WorkerId) -> &Worker {
        self.workers.get(&id).unwrap_or_else(|| {
            panic!("Asking for invalid worker id={}", id);
        })
    }

    #[inline]
    pub fn get_workers(&self) -> impl Iterator<Item = &Worker> {
        self.workers.values()
    }

    #[inline]
    pub fn has_workers(&self) -> bool {
        !self.workers.is_empty()
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let task_id = task_ref.get().id();
        assert!(self.tasks.insert(task_id, task_ref).is_none());
    }

    #[must_use]
    pub fn remove_task(&mut self, task: &mut Task) -> TaskRuntimeState {
        trace_task_remove(task.id);
        assert!(!task.has_consumers());
        assert!(self.tasks.remove(&task.id).is_some());
        std::mem::replace(&mut task.state, TaskRuntimeState::Released)
    }

    pub fn get_tasks(&self) -> impl Iterator<Item = &TaskRef> {
        self.tasks.values()
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

    use crate::server::task::TaskRuntimeState;

    use crate::server::core::Core;
    use crate::test_util::task;

    #[test]
    fn add_remove() {
        let mut core = Core::default();
        let t = task(101);
        core.add_task(t.clone());
        assert_eq!(core.get_task_by_id(101).unwrap(), &t);
        assert!(match core.remove_task(&mut t.get_mut()) {
            TaskRuntimeState::Waiting => true,
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
