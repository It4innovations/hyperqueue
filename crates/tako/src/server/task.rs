use std::collections::HashSet;
use std::fmt;

use crate::common::{Set, WrappedRcRefCell};
use crate::{TaskId, TaskTypeId};
use crate::server::core::Core;
use crate::messages::worker::{ComputeTaskMsg, ToWorkerMessage};
use crate::PriorityValue;
use crate::WorkerId;



#[derive(Debug)]
pub struct DataInfo {
    pub size: u64,
}

pub enum TaskRuntimeState {
    Waiting,
    Scheduled(WorkerId),
    Assigned(WorkerId),
    Stealing(WorkerId, WorkerId), // (from, to)
    Finished(DataInfo, Set<WorkerId>),
    Released,
}

impl fmt::Debug for TaskRuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = match self {
            Self::Waiting => 'W',
            Self::Scheduled(_) => 'S',
            Self::Assigned(_) => 'A',
            Self::Stealing(_, _) => 'T',
            Self::Finished(_, _) => 'F',
            Self::Released => 'R',
        };
        write!(f, "{}", n)
    }
}

bitflags::bitflags! {
    pub struct TaskFlags: u32 {
        const KEEP = 0b00000001;
        const OBSERVE = 0b00000010;
    }
}


#[derive(Debug)]
pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    pub unfinished_inputs: u32,
    consumers: Set<TaskRef>,
    pub dependencies: Vec<TaskId>,
    pub flags: TaskFlags,

    pub type_id: TaskTypeId,
    pub spec: Vec<u8>, // Serialized TaskSpec

    pub user_priority: i32,
    pub scheduler_priority: i32,
    pub client_priority: i32,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.unfinished_inputs == 0
    }

    #[inline]
    pub fn id(&self) -> TaskId {
        self.id
    }

    #[inline]
    pub fn has_consumers(&self) -> bool {
        !self.consumers.is_empty()
    }
    #[inline]
    pub fn add_consumer(&mut self, consumer: TaskRef) -> bool {
        self.consumers.insert(consumer)
    }
    #[inline]
    pub fn remove_consumer(&mut self, consumer: &TaskRef) -> bool {
        self.consumers.remove(consumer)
    }
    #[inline]
    pub fn get_consumers(&self) -> &Set<TaskRef> {
        &self.consumers
    }

    #[inline]
    pub fn set_keep_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::KEEP, value);
    }

    #[inline]
    pub fn set_observed_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::OBSERVE, value);
    }

    #[inline]
    pub fn is_observed(&self) -> bool {
        self.flags.contains(TaskFlags::OBSERVE)
    }

    #[inline]
    pub fn is_keeped(&self) -> bool {
        self.flags.contains(TaskFlags::KEEP)
    }

    pub fn make_sched_info(&self) -> crate::scheduler::protocol::TaskInfo {
        crate::scheduler::protocol::TaskInfo {
            id: self.id,
            inputs: self.dependencies.clone(),
        }
    }

    #[inline]
    pub fn is_removable(&self) -> bool {
        self.consumers.is_empty() && !self.is_keeped() && self.is_finished()
    }

    pub fn collect_consumers(&self) -> HashSet<TaskRef> {
        let mut stack: Vec<_> = self.consumers.iter().cloned().collect();
        let mut result: HashSet<TaskRef> = stack.iter().cloned().collect();

        while let Some(task_ref) = stack.pop() {
            let task = task_ref.get();
            for t in &task.consumers {
                if result.insert(t.clone()) {
                    stack.push(t.clone());
                }
            }
        }
        result
    }

    pub fn make_compute_message(&self, core: &Core) -> ToWorkerMessage {
        let dep_info: Vec<_> = self
            .dependencies
            .iter()
            .map(|task_id| {
                let task_ref = core.get_task_by_id_or_panic(*task_id);
                let task = task_ref.get();
                let addresses: Vec<_> = task
                    .get_workers()
                    .unwrap()
                    .iter()
                    .copied()
                    .collect();
                (task.id, task.data_info().unwrap().size, addresses)
            })
            .collect();

        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id: self.id,
            type_id: self.type_id,
            dep_info,
            spec: self.spec.clone(),
            user_priority: self.user_priority,
            scheduler_priority: self.scheduler_priority,
        })
    }

    #[inline]
    pub fn is_waiting(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Waiting)
    }

    #[inline]
    pub fn is_scheduled(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Scheduled(_))
    }

    #[inline]
    pub fn is_assigned(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Assigned(_))
    }

    #[inline]
    pub fn is_assigned_or_stealed_from(&self, worker_id: WorkerId) -> bool {
        match &self.state {
            TaskRuntimeState::Assigned(w) | TaskRuntimeState::Stealing(w, _) => worker_id == *w,
            _ => false,
        }
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Finished(_, _))
    }

    #[inline]
    pub fn is_done(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Finished(_, _) | TaskRuntimeState::Released)
    }

    #[inline]
    pub fn data_info(&self) -> Option<&DataInfo> {
        match &self.state {
            TaskRuntimeState::Finished(data, _) => Some(data),
            _ => None,
        }
    }

    #[inline]
    pub fn get_workers(&self) -> Option<&Set<WorkerId>> {
        match &self.state {
            TaskRuntimeState::Finished(_, ws) => Some(ws),
            _ => None,
        }
    }
}

impl TaskRef {
    pub fn new(
        id: TaskId,
        type_id: TaskTypeId,
        spec: Vec<u8>,
        dependencies: Vec<TaskId>,
        user_priority: PriorityValue,
        client_priority: PriorityValue,
        keep: bool,
        observe: bool,
    ) -> Self {
        let mut flags = TaskFlags::empty();
        flags.set(TaskFlags::KEEP, keep);
        flags.set(TaskFlags::OBSERVE, observe);
        Self::wrap(Task {
            id,
            dependencies,
            unfinished_inputs: 0,
            flags,
            type_id,
            spec,
            user_priority,
            client_priority,
            scheduler_priority: Default::default(),
            state: TaskRuntimeState::Waiting,
            consumers: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;

    use crate::server::core::Core;
    use crate::test_util::{submit_test_tasks, task, task_with_deps};

    #[test]
    fn task_consumers_empty() {
        let a = task(0);
        assert_eq!(a.get().collect_consumers(), Default::default());
    }

    #[test]
    fn task_recursive_consumers() {
        let mut core = Core::default();
        let a = task(0);
        let b = task_with_deps(1, &[0]);
        let c = task_with_deps(2, &[1]);
        let d = task_with_deps(3, &[1]);
        let e = task_with_deps(4, &[2, 3]);

        submit_test_tasks(
            &mut core,
            &[a.clone(), b.clone(), c.clone(), d.clone(), e.clone()],
        );

        assert_eq!(
            a.get().collect_consumers(),
            vec!(b, c, d, e).into_iter().collect()
        );
    }
}
