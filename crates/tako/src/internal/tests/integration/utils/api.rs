#![allow(unused)]

use crate::gateway::LostWorkerReason;
use crate::internal::common::Map;
use crate::internal::messages::worker::WorkerOverview;
use crate::internal::tests::integration::utils::server::{ServerHandle, TestTaskState};
use crate::worker::WorkerConfiguration;
use crate::{TaskId, WorkerId};

pub struct WaitResult {
    tasks: Map<TaskId, TestTaskState>,
}

impl WaitResult {
    pub fn assert_all_finished(&self) -> bool {
        self.tasks
            .values()
            .all(|s| matches!(s, TestTaskState::Finished(_)))
    }
    pub fn assert_all_failed(&self) -> bool {
        self.tasks
            .values()
            .all(|s| matches!(s, TestTaskState::Failed(..)))
    }

    pub fn get_state<T: Into<TaskId>>(&self, task: T) -> TestTaskState {
        let task_id = task.into();
        self.tasks.get(&task_id).cloned().unwrap()
    }
}

pub async fn wait_for_worker_overview(
    handle: &mut ServerHandle,
    worker_id: WorkerId,
) -> Box<WorkerOverview> {
    wait_for_workers_overview(handle, &[worker_id])
        .await
        .remove(&worker_id)
        .unwrap()
}
pub async fn wait_for_workers_overview(
    handle: &mut ServerHandle,
    worker_ids: &[WorkerId],
) -> Map<WorkerId, Box<WorkerOverview>> {
    let mut result = Map::new();
    let notify = handle.client.get().notify.clone();
    loop {
        for worker_id in worker_ids {
            if let Some(overview) = handle.client.get_mut().overviews.remove(worker_id) {
                result.insert(*worker_id, overview);
            }
        }
        if result.len() == worker_ids.len() {
            return result;
        }
        notify.notified().await;
    }
}

pub async fn wait_for_worker_connected(
    handle: &mut ServerHandle,
    worker_id: WorkerId,
) -> WorkerConfiguration {
    let notify = handle.client.get().notify.clone();
    loop {
        if let Some(s) = handle.client.get_mut().worker_state.get(&worker_id) {
            return s.configuration.clone();
        }
        notify.notified().await;
    }
}

pub async fn wait_for_worker_lost(
    handle: &mut ServerHandle,
    worker_id: WorkerId,
) -> LostWorkerReason {
    let notify = handle.client.get().notify.clone();
    loop {
        if let Some(s) = handle
            .client
            .get_mut()
            .worker_state
            .get(&worker_id)
            .and_then(|s| s.lost_reason.clone())
        {
            return s;
        }
        notify.notified().await;
    }
}

pub async fn wait_for_task_start<T: Into<TaskId>>(
    handle: &mut ServerHandle,
    task_id: T,
) -> WorkerId {
    let task_id: TaskId = task_id.into();
    let notify = handle.client.get().notify.clone();
    loop {
        {
            let client = handle.client.get();
            if let Some(worker_id) = client
                .task_state
                .get(&task_id)
                .map(|x| x.worker_id().unwrap())
            {
                return worker_id;
            }
        }
        notify.notified().await;
    }
}

pub async fn wait_for_tasks<T: Into<TaskId> + Copy>(
    handle: &mut ServerHandle,
    tasks: &[T],
) -> WaitResult {
    let notify = handle.client.get().notify.clone();
    loop {
        {
            let mut client = handle.client.get_mut();
            if tasks.iter().all(|t| {
                let task_id: TaskId = (*t).into();
                client
                    .task_state
                    .get(&task_id)
                    .is_some_and(|s| s.is_terminated())
            }) {
                return WaitResult {
                    tasks: tasks
                        .iter()
                        .map(|t| {
                            let task_id: TaskId = (*t).into();
                            (task_id, client.task_state.remove(&task_id).unwrap())
                        })
                        .collect(),
                };
            }
        }
        notify.notified().await;
    }
}
