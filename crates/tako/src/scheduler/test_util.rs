#![cfg(test)]

use crate::scheduler::graph::SchedulerGraph;
use crate::scheduler::protocol::{TaskInfo, TaskUpdate, TaskUpdateType, WorkerInfo};
use crate::scheduler::{Scheduler, TaskId, ToSchedulerMessage, WorkerId};

pub fn assigned_worker(scheduler_graph: &SchedulerGraph, task_id: TaskId) -> WorkerId {
    scheduler_graph
        .tasks
        .get(&task_id)
        .expect("Unknown task")
        .get()
        .assigned_worker
        .as_ref()
        .expect("Worker not assigned")
        .get()
        .id
}

pub fn finish_task<S: Scheduler>(
    scheduler: &mut S,
    task_id: TaskId,
    worker_id: WorkerId,
    size: u64,
) {
    scheduler.handle_messages(ToSchedulerMessage::TaskUpdate(TaskUpdate {
        state: TaskUpdateType::Finished,
        id: task_id,
        worker: worker_id,
        size: Some(size),
    }));
}

pub fn connect_workers<S: Scheduler>(scheduler: &mut S, count: u32, n_cpus: u32) -> Vec<WorkerId> {
    let mut ids = vec![];
    for i in 0..count {
        let id = 100 + i as WorkerId;
        ids.push(id);
        scheduler.handle_messages(ToSchedulerMessage::NewWorker(WorkerInfo {
            id,
            n_cpus,
            hostname: "worker".into(),
        }));
    }
    ids
}

pub fn new_tasks(task_infos: Vec<TaskInfo>) -> ToSchedulerMessage {
    ToSchedulerMessage::NewTasks(task_infos)
}

pub fn new_task(id: TaskId, inputs: Vec<TaskId>) -> TaskInfo {
    TaskInfo { id, inputs }
}
