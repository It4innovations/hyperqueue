use std::fmt;
use std::rc::Rc;
use std::time::Duration;
use thin_vec::ThinVec;

use crate::WorkerId;
use crate::internal::common::Set;
use crate::internal::common::stablemap::ExtractKey;

use crate::gateway::{CrashLimit, EntryType, TaskDataFlags};
use crate::internal::datasrv::dataobj::DataObjectId;

use crate::internal::messages::worker::{ComputeTaskMsg, ToWorkerMessage};
use crate::internal::server::taskmap::TaskMap;
use crate::{InstanceId, Priority};
use crate::{TaskId, static_assert_size};

#[cfg_attr(test, derive(Eq, PartialEq, Clone))]
pub struct WaitingInfo {
    pub unfinished_deps: u32,
    // pub scheduler_metric: i32,
}

#[cfg_attr(test, derive(Eq, PartialEq, Clone))]
pub enum TaskRuntimeState {
    Waiting(WaitingInfo),
    Assigned(WorkerId),
    Stealing(WorkerId, Option<WorkerId>), // (from, to)
    Running { worker_id: WorkerId },
    // The first worker is the root node where the command is executed, others are reserved
    RunningMultiNode(Vec<WorkerId>),
    Finished,
}

impl fmt::Debug for TaskRuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Waiting(info) => write!(f, "W({})", info.unfinished_deps),
            Self::Assigned(w_id) => write!(f, "A({w_id})"),
            Self::Stealing(from_w, to_w) => write!(f, "S({from_w}, {to_w:?})"),
            Self::Running { worker_id, .. } => write!(f, "R({worker_id})"),
            Self::RunningMultiNode(ws) => write!(f, "M({ws:?})"),
            Self::Finished => write!(f, "F"),
        }
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct TaskFlags: u32 {
        const FRESH   = 0b00000001;

        // This is utilized inside scheduler, it has no meaning between scheduler calls
        const TAKE   = 0b00000010;
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct TaskConfiguration {
    pub resources: crate::internal::common::resources::ResourceRequestVariants,
    pub user_priority: Priority,
    pub time_limit: Option<Duration>,
    pub crash_limit: CrashLimit,
    pub data_flags: TaskDataFlags,
    pub body: Box<[u8]>,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct Task {
    pub id: TaskId,
    pub state: TaskRuntimeState,
    consumers: Set<TaskId>,
    pub task_deps: ThinVec<TaskId>,
    pub data_deps: ThinVec<DataObjectId>,
    pub flags: TaskFlags,
    pub configuration: Rc<TaskConfiguration>,
    pub scheduler_priority: Priority,
    pub instance_id: InstanceId,
    pub crash_counter: u32,
    pub entry: Option<EntryType>,
}

// Task is a critical data structure, so we should keep its size in check
static_assert_size!(Task, 112);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        //let task_ids : Vec<_> = self.tasks.iter().map(|r| r.get().id.to_string()).collect();
        f.debug_struct("Task").field("id", &self.id).finish()
    }
}

impl Task {
    pub fn new(
        id: TaskId,
        task_deps: ThinVec<TaskId>,
        dataobj_deps: ThinVec<DataObjectId>,
        entry: Option<EntryType>,
        configuration: Rc<TaskConfiguration>,
    ) -> Self {
        log::debug!(
            "New task {} {:?} {:?} {:?}",
            id,
            &configuration.resources,
            &task_deps,
            &dataobj_deps,
        );

        let mut flags = TaskFlags::empty();
        flags.set(TaskFlags::FRESH, true);

        Self {
            id,
            task_deps,
            data_deps: dataobj_deps,
            flags,
            configuration,
            entry,
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

    #[inline]
    #[cfg(test)]
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
    pub(crate) fn set_take_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::TAKE, value);
    }

    #[inline]
    pub(crate) fn set_fresh_flag(&mut self, value: bool) {
        self.flags.set(TaskFlags::FRESH, value);
    }

    #[inline]
    pub(crate) fn is_fresh(&self) -> bool {
        self.flags.contains(TaskFlags::FRESH)
    }

    #[inline]
    pub(crate) fn is_taken(&self) -> bool {
        self.flags.contains(TaskFlags::TAKE)
    }

    pub(crate) fn collect_recursive_consumers(&self, taskmap: &TaskMap, out: &mut Set<TaskId>) {
        for consumer in &self.consumers {
            out.insert(*consumer);
        }
        let mut stack: Vec<_> = self.consumers.iter().copied().collect();
        while let Some(task_id) = stack.pop() {
            let task = taskmap.get_task(task_id);
            for &consumer_id in &task.consumers {
                if out.insert(consumer_id) {
                    stack.push(consumer_id);
                }
            }
        }
    }

    pub(crate) fn increment_instance_id(&mut self) {
        self.instance_id = InstanceId::new(self.instance_id.as_num() + 1);
    }

    pub(crate) fn increment_crash_counter(&mut self) -> bool {
        self.crash_counter += 1;
        match self.configuration.crash_limit {
            CrashLimit::NeverRestart => true,
            CrashLimit::MaxCrashes(count) => self.crash_counter >= count as u32,
            CrashLimit::Unlimited => false,
        }
    }

    pub(crate) fn make_compute_message(&self, node_list: Vec<WorkerId>) -> ToWorkerMessage {
        ToWorkerMessage::ComputeTask(ComputeTaskMsg {
            id: self.id,
            instance_id: self.instance_id,
            user_priority: self.configuration.user_priority,
            scheduler_priority: self.scheduler_priority,
            resources: self.configuration.resources.clone(),
            time_limit: self.configuration.time_limit,
            node_list,
            data_deps: self.data_deps.iter().copied().collect(),
            data_flags: self.configuration.data_flags,
            body: self.configuration.body.clone(),
            entry: self.entry.clone(),
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
        matches!(&self.state, TaskRuntimeState::Finished)
    }

    #[inline]
    pub(crate) fn is_done_or_running(&self) -> bool {
        matches!(
            &self.state,
            TaskRuntimeState::Finished | TaskRuntimeState::Running { .. }
        )
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
            | TaskRuntimeState::Finished => None,
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
        let mut s = crate::Set::new();
        a.collect_recursive_consumers(&Default::default(), &mut s);
        assert!(s.is_empty());
    }

    #[test]
    fn task_recursive_consumers() {
        let mut core = Core::default();
        let a = task::task(0);
        let b = task_with_deps(1, &[&a]);
        let c = task_with_deps(2, &[&b]);
        let d = task_with_deps(3, &[&b]);
        let e = task_with_deps(4, &[&c, &d]);

        let expected_ids = vec![b.id, c.id, d.id, e.id];
        submit_test_tasks(&mut core, vec![a, b, c, d, e]);

        let mut s = crate::Set::new();
        core.get_task(0.into())
            .collect_recursive_consumers(core.task_map(), &mut s);
        assert_eq!(s, expected_ids.into_iter().collect());
    }
}
