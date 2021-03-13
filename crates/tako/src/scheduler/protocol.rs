use serde::{Deserialize, Serialize};
use crate::{WorkerId, TaskId};

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub n_cpus: u32,
    pub hostname: String,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum TaskUpdateType {
    Finished,
    // Task was computed on a worker
    Placed,
    // Task data are available on worker
    Removed,
    // Task data are no available on worker (or running state is cancelled)
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: TaskId,
    pub inputs: Vec<TaskId>,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Serialize, Deserialize)]
pub struct NewFinishedTaskInfo {
    pub id: TaskId,
    pub workers: Vec<WorkerId>,
    pub size: u64,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub state: TaskUpdateType,
    pub worker: WorkerId,
    pub size: Option<u64>,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskStealResponse {
    pub id: TaskId,
    pub success: bool,
    pub from_worker: WorkerId,
    pub to_worker: WorkerId,
}

#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug, Serialize, Deserialize)]
pub enum ToSchedulerMessage {
    TaskUpdate(TaskUpdate),
    TaskStealResponse(TaskStealResponse),
    NewTasks(Vec<TaskInfo>),
    RemoveTask(TaskId),
    NewFinishedTask(Vec<NewFinishedTaskInfo>),
    NewWorker(WorkerInfo),
    NetworkBandwidth(f32),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchedulerRegistration {
    pub protocol_version: u32,
    pub scheduler_name: String,
    pub scheduler_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: TaskId,
    pub worker: WorkerId,
    pub priority: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FromSchedulerMessage {
    TaskAssignments(Vec<TaskAssignment>),
    Register(SchedulerRegistration),
}
