use serde_json::json;
use std::fmt;
use std::rc::Rc;
use std::time::Duration;
use thin_vec::ThinVec;

use crate::internal::common::Set;
use crate::internal::common::stablemap::ExtractKey;
use crate::{MAX_FRAME_SIZE, Map, WorkerId};

use crate::gateway::{CrashLimit, EntryType, TaskDataFlags};
use crate::internal::datasrv::dataobj::DataObjectId;

use crate::internal::messages::worker::{
    ComputeTaskSeparateData, ComputeTaskSharedData, ComputeTasksMsg, ToWorkerMessage,
};
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

impl TaskRuntimeState {
    fn dump(&self) -> serde_json::Value {
        match self {
            Self::Waiting(info) => json!({
                "state": "Waiting",
                "unfinished_deps": info.unfinished_deps,
            }),
            Self::Assigned(w_id) => json!({
                "state": "Assigned",
                "worker_id": w_id,
            }),
            Self::Stealing(from_w, to_w) => json!({
                "state": "Stealing",
                "from_worker": from_w,
                "to_worker": to_w,
            }),
            Self::Running { worker_id, .. } => json!({
                "state": "Running",
                "worker_id": worker_id,
            }),
            Self::RunningMultiNode(ws) => json!({
                "state": "RunningMultiNode",
                "worker_ids": ws,
            }),
            Self::Finished => json!({
                "state": "Finished",
            }),
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

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TaskConfiguration {
    // Try to keep the fields ordered in a way so that the chance for finding a different field
    // between two different task configurations is as high as possible.
    // In other words, task configuration fields that are the same between most tasks should be
    // ordered last.
    pub resources: crate::internal::common::resources::ResourceRequestVariants,
    // Use Rc to avoid cloning the data when we serialize them
    pub body: Rc<[u8]>,
    pub user_priority: Priority,
    pub time_limit: Option<Duration>,
    pub crash_limit: CrashLimit,
    pub data_flags: TaskDataFlags,
}

impl TaskConfiguration {
    pub fn dump(&self) -> serde_json::Value {
        json!({
            "resources": self.resources,
            "user_priority": self.user_priority,
            "time_limit": self.time_limit,
            "crash_limit": self.crash_limit,
            "body_len": self.body.len(),
        })
    }
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
    pub fn dump(&self) -> serde_json::Value {
        json!({
            "id": self.id.to_string(),
            "state": self.state.dump(),
            "consumers": self.consumers,
            "task_deps": self.task_deps,
            "flags": self.flags.bits(),
            "scheduler_priority": self.scheduler_priority,
            "instance_id": self.instance_id,
            "crash_counter": self.crash_counter,
            "configuration": self.configuration.dump(),
        })
    }

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

/// Maximum size of data that we are willing to put into a single packet.
const MAX_TASK_MSG_SIZE: usize = MAX_FRAME_SIZE / 4;

/// Builder for [ToWorkerMessage::ComputeTasks], which tries to shared common data between
/// task instances to reduce network bandwidth.
///
/// The builder also handles fragmentation, if it receives too much data for a single packet
#[derive(Default)]
pub struct ComputeTasksBuilder {
    tasks: Vec<ComputeTaskSeparateData>,
    // TODO: if we could ensure that we intern all known task configurations in memory, and they
    // have a unique allocation, we could compare the configs here based on just pointer address,
    // not the contents of `TaskConfiguration`.
    configuration_index: Map<Rc<TaskConfiguration>, usize>,
    shared_data: Vec<ComputeTaskSharedData>,
    estimated_size: usize,
}

impl ComputeTasksBuilder {
    pub fn single_task(task: &Task, node_list: Vec<WorkerId>) -> ToWorkerMessage {
        // TODO: optimize this
        let mut builder = Self::default();
        if let Some(msg) = builder.add_task(task, node_list) {
            msg
        } else {
            builder.into_last_message().unwrap()
        }
    }

    /// Adds a task to the builder, and optionally generate a message if it has reached size limits.
    #[must_use]
    pub fn add_task(&mut self, task: &Task, node_list: Vec<WorkerId>) -> Option<ToWorkerMessage> {
        let conf = &task.configuration;
        let shared_index = *self
            .configuration_index
            .entry(conf.clone())
            .or_insert_with(|| {
                let shared = ComputeTaskSharedData {
                    user_priority: conf.user_priority,
                    resources: conf.resources.clone(),
                    time_limit: conf.time_limit,
                    data_flags: conf.data_flags,
                    body: conf.body.clone(),
                };
                let index = self.shared_data.len();
                self.estimated_size += estimate_shared_data_size(&shared);
                self.shared_data.push(shared);
                index
            });

        let task_data = ComputeTaskSeparateData {
            shared_index,
            id: task.id,
            instance_id: task.instance_id,
            scheduler_priority: task.scheduler_priority,
            node_list,
            data_deps: task.data_deps.iter().copied().collect(),
            entry: task.entry.clone(),
        };
        self.estimated_size += estimate_task_data_size(&task_data);
        self.tasks.push(task_data);

        self.create_message_on_overflow()
    }

    /// If there are any pending task data, materialize them into a new ComputeTasks message
    /// and push it into `self.messages`. Also resets any internal auxiliary data.
    fn create_message_on_overflow(&mut self) -> Option<ToWorkerMessage> {
        if self.estimated_size > MAX_TASK_MSG_SIZE {
            let msg = ComputeTasksMsg {
                tasks: std::mem::take(&mut self.tasks),
                shared_data: std::mem::take(&mut self.shared_data),
            };
            self.configuration_index.clear();
            self.estimated_size = 0;
            Some(ToWorkerMessage::ComputeTasks(msg))
        } else {
            None
        }
    }

    /// If there are any tasks in this builder, generate a message
    pub fn into_last_message(self) -> Option<ToWorkerMessage> {
        if !self.tasks.is_empty() {
            let msg = ComputeTasksMsg {
                tasks: self.tasks,
                shared_data: self.shared_data,
            };
            Some(ToWorkerMessage::ComputeTasks(msg))
        } else {
            None
        }
    }
}

/// Estimate how much data it will take to serialize this task data
fn estimate_task_data_size(data: &ComputeTaskSeparateData) -> usize {
    let ComputeTaskSeparateData {
        shared_index,
        id,
        instance_id,
        scheduler_priority,
        node_list,
        data_deps,
        entry,
    } = data;

    // We cound each field separately, because if we just used size_of on the whole struct, it would
    // count internal field of Vecs, which are not serialized.
    size_of_val(shared_index)
        + size_of_val(id)
        + size_of_val(instance_id)
        + size_of_val(scheduler_priority)
        + size_of_val(node_list.as_slice())
        + size_of_val(data_deps.as_slice())
        + entry.as_ref().map(|e| e.len()).unwrap_or_default()
}

/// Estimate how much data it will take to serialize this shared task data
fn estimate_shared_data_size(data: &ComputeTaskSharedData) -> usize {
    let ComputeTaskSharedData {
        user_priority,
        resources,
        time_limit,
        data_flags,
        body,
    } = data;
    size_of_val(user_priority)
        + size_of_val(resources.requests())
        + size_of_val(time_limit)
        + size_of_val(data_flags)
        + body.len()
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
