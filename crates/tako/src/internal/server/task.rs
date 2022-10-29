use std::fmt;
use std::rc::Rc;
use std::time::Duration;
use thin_vec::ThinVec;

use crate::internal::common::stablemap::ExtractKey;
use crate::internal::common::{Map, Set};
use crate::internal::messages::worker::{ComputeTaskMsg, ToWorkerMessage};
use crate::internal::server::taskmap::TaskMap;
use crate::WorkerId;
use crate::{static_assert_size, TaskId};
use crate::{InstanceId, Priority};

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct DataInfo {
    pub size: u64,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct WaitingInfo {
    pub unfinished_deps: u32,
    // pub scheduler_metric: i32,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct FinishInfo {
    pub data_info: DataInfo,
    pub placement: Set<WorkerId>,
    pub future_placement: Map<WorkerId, u32>,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum TaskRuntimeState {
    Waiting(WaitingInfo),
    Assigned(WorkerId),
    Stealing(WorkerId, Option<WorkerId>), // (from, to)
    Running { worker_id: WorkerId },
    // The first worker is the root node where the command is executed, others are reserved
    RunningMultiNode(Vec<WorkerId>),
    Finished(FinishInfo),
}

impl fmt::Debug for TaskRuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Waiting(info) => write!(f, "W({})", info.unfinished_deps),
            Self::Assigned(w_id) => write!(f, "A({w_id})"),
            Self::Stealing(from_w, to_w) => write!(f, "S({from_w}, {to_w:?})"),
            Self::Running { worker_id, .. } => write!(f, "R({worker_id})"),
            Self::RunningMultiNode(ws) => write!(f, "M({ws:?})"),
            Self::Finished(_) => write!(f, "F"),
        }
    }
}

bitflags::bitflags! {
    pub struct TaskFlags: u32 {
        const KEEP    = 0b00000001;
        const OBSERVE = 0b00000010;

        const FRESH   = 0b00000100;

        // This is utilized inside scheduler, it has no meaning between scheduler calls
        const TAKE   = 0b00001000;
    }
}

#[derive(Clone)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct TaskInput {
    task: TaskId,
    output_id: u32, // MAX = pure dependency on task, not real output id
}

impl TaskInput {
    pub fn new(task: TaskId, output_id: u32) -> Self {
        TaskInput { task, output_id }
    }

    pub fn new_task_dependency(task: TaskId) -> Self {
        TaskInput {
            task,
            output_id: u32::MAX,
        }
    }

    pub fn task(&self) -> TaskId {
        self.task
    }

    pub fn output_id(&self) -> Option<u32> {
        if self.output_id == u32::MAX {
            None
        } else {
            Some(self.output_id)
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct TaskConfiguration {
    pub resources: crate::internal::common::resources::ResourceRequestVariants,
    pub user_priority: Priority,
    pub time_limit: Option<Duration>,
    pub n_outputs: u32,
    pub crash_limit: u32,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    consumers: Set<TaskId>,
    pub inputs: ThinVec<TaskInput>,
    pub flags: TaskFlags,
    pub configuration: Rc<TaskConfiguration>,
    pub scheduler_priority: Priority,
    pub instance_id: InstanceId,
    pub crash_counter: u32,
    pub body: Box<[u8]>,
}

// Task is a critical data structure, so we should keep its size in check
static_assert_size!(Task, 168);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        //let task_ids : Vec<_> = self.tasks.iter().map(|r| r.get().id.to_string()).collect();
        f.debug_struct("Task").field("id", &self.id).finish()
    }
}

impl Task {
    pub fn new(
        id: TaskId,
        inputs: ThinVec<TaskInput>,
        configuration: Rc<TaskConfiguration>,
        body: Box<[u8]>,
        keep: bool,
        observe: bool,
    ) -> Self {
        log::debug!("New task {} {:?}", id, &configuration.resources);

        let mut flags = TaskFlags::empty();
        flags.set(TaskFlags::KEEP, keep);
        flags.set(TaskFlags::OBSERVE, observe);
        flags.set(TaskFlags::FRESH, true);

        Self {
            id,
            inputs,
            flags,
            configuration,
            body,
            scheduler_priority: Default::default(),
            state: TaskRuntimeState::Waiting(WaitingInfo { unfinished_deps: 0 }),
            consumers: Default::default(),
            instance_id: InstanceId::new(0),
            crash_counter: 0,
        }
    }

    #[inline]
    pub(crate) fn is_ready(&self) -> bool {
        matches!(
            self.state,
            TaskRuntimeState::Waiting(WaitingInfo {
                unfinished_deps: 0,
                ..
            })
        )
    }

    #[inline]
    pub(crate) fn is_sn_running(&self) -> bool {
        matches!(self.state, TaskRuntimeState::Running { .. })
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn is_mn_running(&self) -> bool {
        matches!(self.state, TaskRuntimeState::RunningMultiNode { .. })
    }

    #[inline]
    pub(crate) fn decrease_unfinished_deps(&mut self) -> bool {
        match &mut self.state {
            TaskRuntimeState::Waiting(WaitingInfo {
                unfinished_deps, ..
            }) if *unfinished_deps > 0 => {
                *unfinished_deps -= 1;
                *unfinished_deps == 0
            }
            _ => panic!("Invalid state"),
        }
    }

    #[inline]
    pub fn id(&self) -> TaskId {
        self.id
    }

    #[inline]
    pub(crate) fn has_consumers(&self) -> bool {
        !self.consumers.is_empty()
    }

    #[inline]
    pub(crate) fn add_consumer(&mut self, consumer: TaskId) -> bool {
        self.consumers.insert(consumer)
    }
    #[inline]
    pub(crate) fn remove_consumer(&mut self, consumer: TaskId) -> bool {
        self.consumers.remove(&consumer)
    }
    #[inline]
    pub(crate) fn get_consumers(&self) -> &Set<TaskId> {
        &self.consumers
    }

    #[inline]
    pub(crate) fn set_keep_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::KEEP, value);
    }

    #[inline]
    pub(crate) fn set_take_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::TAKE, value);
    }

    #[inline]
    pub(crate) fn set_observed_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::OBSERVE, value);
    }

    #[inline]
    pub(crate) fn set_fresh_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::FRESH, value);
    }

    #[inline]
    pub(crate) fn is_observed(&self) -> bool {
        self.flags.contains(TaskFlags::OBSERVE)
    }

    #[inline]
    pub(crate) fn is_keeped(&self) -> bool {
        self.flags.contains(TaskFlags::KEEP)
    }

    #[inline]
    pub(crate) fn is_fresh(&self) -> bool {
        self.flags.contains(TaskFlags::FRESH)
    }

    #[inline]
    pub(crate) fn is_taken(&self) -> bool {
        self.flags.contains(TaskFlags::TAKE)
    }

    #[inline]
    pub(crate) fn is_removable(&self) -> bool {
        self.consumers.is_empty() && !self.is_keeped() && self.is_finished()
    }

    pub(crate) fn collect_consumers(&self, taskmap: &TaskMap) -> Set<TaskId> {
        let mut stack: Vec<_> = self.consumers.iter().copied().collect();
        let mut result: Set<TaskId> = stack.iter().copied().collect();

        while let Some(task_id) = stack.pop() {
            let task = taskmap.get_task(task_id);
            for &consumer_id in &task.consumers {
                if result.insert(consumer_id) {
                    stack.push(consumer_id);
                }
            }
        }
        result
    }

    pub(crate) fn increment_instance_id(&mut self) {
        self.instance_id = InstanceId(self.instance_id.as_num() + 1);
    }

    pub(crate) fn increment_crash_counter(&mut self) -> bool {
        self.crash_counter += 1;
        self.crash_counter >= self.configuration.crash_limit && self.configuration.crash_limit > 0
    }

    pub(crate) fn make_compute_message(&self, node_list: Vec<WorkerId>) -> ToWorkerMessage {
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id: self.id,
            instance_id: self.instance_id,
            user_priority: self.configuration.user_priority,
            scheduler_priority: self.scheduler_priority,
            resources: self.configuration.resources.clone(),
            time_limit: self.configuration.time_limit,
            n_outputs: self.configuration.n_outputs,
            node_list,
            body: self.body.clone(),
        })
    }

    pub(crate) fn mn_placement(&self) -> Option<&[WorkerId]> {
        match &self.state {
            TaskRuntimeState::RunningMultiNode(ws) => Some(ws),
            _ => None,
        }
    }

    pub(crate) fn mn_root_worker(&self) -> Option<WorkerId> {
        self.mn_placement().map(|ws| ws[0])
    }

    #[inline]
    pub(crate) fn is_waiting(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Waiting(_))
    }

    #[cfg(test)] // Used only tests, feel free to make it non-test-code
    #[inline]
    pub(crate) fn is_assigned(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Assigned(_))
    }

    #[inline]
    pub(crate) fn is_assigned_or_stolen_from(&self, worker_id: WorkerId) -> bool {
        match &self.state {
            TaskRuntimeState::Assigned(w)
            | TaskRuntimeState::Running { worker_id: w, .. }
            | TaskRuntimeState::Stealing(w, _) => worker_id == *w,
            TaskRuntimeState::RunningMultiNode(ws) => ws[0] == worker_id,
            _ => false,
        }
    }

    #[inline]
    pub(crate) fn is_finished(&self) -> bool {
        matches!(&self.state, TaskRuntimeState::Finished(_))
    }

    #[inline]
    pub(crate) fn is_done_or_running(&self) -> bool {
        matches!(
            &self.state,
            TaskRuntimeState::Finished(_) | TaskRuntimeState::Running { .. }
        )
    }

    #[inline]
    pub(crate) fn data_info(&self) -> Option<&FinishInfo> {
        match &self.state {
            TaskRuntimeState::Finished(finfo) => Some(finfo),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn get_assigned_worker(&self) -> Option<WorkerId> {
        match &self.state {
            TaskRuntimeState::Assigned(id)
            | TaskRuntimeState::Running { worker_id: id, .. }
            | TaskRuntimeState::Stealing(_, Some(id)) => Some(*id),
            TaskRuntimeState::Waiting(_)
            | TaskRuntimeState::Stealing(_, None)
            | TaskRuntimeState::RunningMultiNode(_)
            | TaskRuntimeState::Finished(_) => None,
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn get_placement(&self) -> Option<&Set<WorkerId>> {
        match &self.state {
            TaskRuntimeState::Finished(finfo) => Some(&finfo.placement),
            _ => None,
        }
    }

    pub(crate) fn remove_future_placement(&mut self, worker_id: WorkerId) {
        match &mut self.state {
            TaskRuntimeState::Finished(finfo) => {
                let count = finfo.future_placement.get_mut(&worker_id).unwrap();
                if *count <= 1 {
                    assert_ne!(*count, 0);
                    finfo.future_placement.remove(&worker_id);
                } else {
                    *count -= 1;
                }
            }
            _ => {
                unreachable!()
            }
        }
    }

    #[inline]
    pub(crate) fn set_future_placement(&mut self, worker_id: WorkerId) {
        match self.state {
            TaskRuntimeState::Finished(ref mut finfo) => {
                (*finfo.future_placement.entry(worker_id).or_insert(0)) += 1;
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    pub(crate) fn get_scheduler_priority(&self) -> i32 {
        /*match self.state {
            TaskRuntimeState::Waiting(winfo) => winfo.scheduler_metric,
            _ => unreachable!()
        }*/
        self.scheduler_priority
    }

    #[inline]
    pub(crate) fn set_scheduler_priority(&mut self, value: i32) {
        /*match &mut self.state {
            TaskRuntimeState::Waiting(WaitingInfo { ref mut scheduler_metric, ..}) => { *scheduler_metric = value },
            _ => unreachable!()
        }*/
        self.scheduler_priority = value;
    }
}

impl ExtractKey<TaskId> for Task {
    #[inline]
    fn extract_key(&self) -> TaskId {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;

    use crate::internal::server::core::Core;
    use crate::internal::server::task::{Task, TaskRuntimeState};
    use crate::internal::tests::utils::schedule::submit_test_tasks;
    use crate::internal::tests::utils::task;
    use crate::internal::tests::utils::task::task_with_deps;

    //use crate::test_util::{submit_test_tasks, task, task_with_deps};

    impl Task {
        pub fn get_unfinished_deps(&self) -> u32 {
            match &self.state {
                TaskRuntimeState::Waiting(winfo) => winfo.unfinished_deps,
                _ => panic!("Invalid state"),
            }
        }
    }

    #[test]
    fn task_consumers_empty() {
        let a = task::task(0);
        assert_eq!(a.collect_consumers(&Default::default()), Default::default());
    }

    #[test]
    fn task_recursive_consumers() {
        let mut core = Core::default();
        let a = task::task(0);
        let b = task_with_deps(1, &[&a], 1);
        let c = task_with_deps(2, &[&b], 1);
        let d = task_with_deps(3, &[&b], 1);
        let e = task_with_deps(4, &[&c, &d], 1);

        let expected_ids = vec![b.id, c.id, d.id, e.id];
        submit_test_tasks(&mut core, vec![a, b, c, d, e]);

        assert_eq!(
            core.get_task(0.into()).collect_consumers(core.task_map()),
            expected_ids.into_iter().collect()
        );
    }
}
