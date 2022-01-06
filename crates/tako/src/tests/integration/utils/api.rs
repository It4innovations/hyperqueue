use super::macros::wait_for_msg;
use crate::common::{Map, Set};
use crate::messages::common::TaskFailInfo;
use crate::messages::gateway::{
    CancelTasks, CancelTasksResponse, FromGatewayMessage, MonitoringEventRequest, TaskState,
    TaskUpdate, ToGatewayMessage,
};
use crate::messages::worker::WorkerOverview;
use crate::server::monitoring::{MonitoringEvent, MonitoringEventId, MonitoringEventPayload};
use crate::tests::integration::utils::server::ServerHandle;
use crate::{TaskId, WorkerId};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::error::Elapsed;
use tokio::time::sleep;

/// Repeatedly gathers events until at least one overview per each worker is received.
/// Events created before calling this function will not be considered.
pub async fn get_latest_overview(
    handler: &mut ServerHandle,
    worker_ids: Vec<WorkerId>,
) -> HashMap<WorkerId, WorkerOverview> {
    let event_id = get_current_event_id(handler).await;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<MonitoringEvent>(100);
    let gather_fut = async move {
        let mut overview_map: HashMap<WorkerId, WorkerOverview> = HashMap::new();
        while let Some(event) = rx.recv().await {
            assert!(Some(event.id) > event_id);
            if let MonitoringEventPayload::OverviewUpdate(overview) = event.payload {
                if worker_ids.contains(&overview.id) && !overview_map.contains_key(&overview.id) {
                    overview_map.insert(overview.id, overview);
                }
            }
            if overview_map.len() >= worker_ids.len() {
                break;
            }
        }
        overview_map
    };

    event_stream(handler, tx, gather_fut, Duration::from_secs(5), event_id)
        .await
        .expect("Timed out while waiting for worker overviews")
}

pub async fn wait_for_worker_lost(
    handler: &mut ServerHandle,
    worker_id: WorkerId,
) -> MonitoringEvent {
    wait_for_event(
        handler,
        |event| {
            matches!(
                event.payload,
                MonitoringEventPayload::WorkerLost(id, _) if id == worker_id
            )
        },
        Duration::from_secs(3),
        None,
    )
    .await
    .unwrap()
}

pub async fn wait_for_task_start<T: Into<TaskId>>(
    handler: &mut ServerHandle,
    task_id: T,
) -> WorkerId {
    let task_id: TaskId = task_id.into();
    wait_for_msg!(handler, ToGatewayMessage::TaskUpdate(TaskUpdate {
        state: TaskState::Running(worker_id),
        id,
    }) if id == task_id => worker_id)
}

pub async fn wait_for_event<EventCondition: Fn(&MonitoringEvent) -> bool>(
    handler: &mut ServerHandle,
    condition: EventCondition,
    for_duration: Duration,
    start_event_id: Option<MonitoringEventId>,
) -> Option<MonitoringEvent> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let wait_fut = async move {
        while let Some(event) = rx.recv().await {
            if condition(&event) {
                return Some(event);
            }
        }
        None
    };

    event_stream(handler, tx, wait_fut, for_duration, start_event_id)
        .await
        .unwrap_or(None)
}

pub async fn get_current_event_id(handler: &mut ServerHandle) -> Option<MonitoringEventId> {
    let events = get_events_after(None, handler).await;
    events.into_iter().map(|event| event.id).max()
}

/// Repeatedly download events from the server and pass them to the provided queue.
async fn event_stream<F: Future>(
    handler: &mut ServerHandle,
    event_sender: Sender<MonitoringEvent>,
    reader_fut: F,
    timeout: Duration,
    start_event_id: Option<MonitoringEventId>,
) -> Result<F::Output, Elapsed> {
    let events_fut = async move {
        let mut last_id = start_event_id;
        loop {
            let mut events = get_events_after(last_id, handler).await;
            events.sort_unstable_by_key(|x| x.id);
            last_id = events.last().map(|event| event.id).or(last_id);

            for event in events {
                event_sender.send(event).await.unwrap();
            }

            sleep(Duration::from_millis(10)).await;
        }
    };
    let events_fut = tokio::time::timeout(timeout, events_fut);

    tokio::select! {
        result = reader_fut => Ok(result),
        err = events_fut => err
    }
}

/**
 * The event order is not guaranteed
 **/
pub async fn get_events_after(
    after_id: Option<u32>,
    handler: &mut ServerHandle,
) -> Vec<MonitoringEvent> {
    handler
        .send(FromGatewayMessage::GetMonitoringEvents(
            MonitoringEventRequest { after_id },
        ))
        .await;
    wait_for_msg!(handler, ToGatewayMessage::MonitoringEvents(events) => events)
}

// Waiting for tasks
#[derive(Debug)]
pub enum TaskResult {
    Update(TaskState),
    Fail {
        cancelled_tasks: Vec<TaskId>,
        info: TaskFailInfo,
    },
}

impl TaskResult {
    pub fn is_finished(&self) -> bool {
        matches!(self, TaskResult::Update(TaskState::Finished))
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, TaskResult::Update(TaskState::Invalid))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, TaskResult::Fail { .. })
    }
}

#[derive(Default, Debug)]
pub struct TaskWaitResult {
    events: Vec<TaskResult>,
}

impl TaskWaitResult {
    fn add(&mut self, result: TaskResult) {
        self.events.push(result);
    }

    fn is_finished(&self) -> bool {
        self.events.iter().any(|v| v.is_finished())
    }

    fn is_failed(&self) -> bool {
        self.events.iter().any(|v| v.is_failed())
    }

    pub fn is_invalid(&self) -> bool {
        self.events.iter().any(|v| v.is_invalid())
    }

    pub fn assert_error_message(&self, needle: &str) {
        for event in &self.events {
            match event {
                TaskResult::Fail { info, .. } => {
                    assert!(info.message.contains(needle));
                    return;
                }
                _ => {}
            }
        }
        panic!("Did not find error result for the current task");
    }
}

#[derive(Default)]
pub struct TaskWaitResultMap {
    tasks: Map<TaskId, TaskWaitResult>,
}

impl TaskWaitResultMap {
    pub fn assert_all_finished(&self) {
        for (id, task) in &self.tasks {
            if !task.is_finished() {
                panic!("Task {} has not finished: {:?}", id, task);
            }
        }
    }

    pub fn is_failed<T: Into<TaskId>>(&self, id: T) -> bool {
        self.tasks[&id.into()].is_failed()
    }

    pub fn get<T: Into<TaskId>>(&self, id: T) -> &TaskWaitResult {
        &self.tasks[&id.into()]
    }
}

pub async fn wait_for_tasks<T: Into<TaskId>>(
    handle: &mut ServerHandle,
    tasks: Vec<T>,
) -> TaskWaitResultMap {
    let mut tasks: Set<TaskId> = tasks.into_iter().map(|v| v.into()).collect();
    let tasks_orig = tasks.clone();
    let mut result = TaskWaitResultMap::default();

    while !tasks.is_empty() {
        match handle.recv().await {
            ToGatewayMessage::TaskUpdate(msg) => {
                if !tasks_orig.contains(&msg.id) {
                    continue;
                }
                if let TaskState::Finished | TaskState::Invalid = msg.state {
                    assert!(tasks.remove(&msg.id));
                }
                result
                    .tasks
                    .entry(msg.id)
                    .or_default()
                    .add(TaskResult::Update(msg.state));
            }
            ToGatewayMessage::TaskFailed(msg) => {
                if !tasks_orig.contains(&msg.id) {
                    continue;
                }
                assert!(tasks.remove(&msg.id));
                result
                    .tasks
                    .entry(msg.id)
                    .or_default()
                    .add(TaskResult::Fail {
                        cancelled_tasks: msg.cancelled_tasks,
                        info: msg.info,
                    });
            }
            ToGatewayMessage::Error(msg) => panic!(
                "Received error message {:?} while waiting for tasks",
                msg.message
            ),
            msg => println!("Received message {:?} while waiting for tasks", msg),
        };
    }
    result
}

// Cancellation
pub async fn cancel<T: Into<TaskId> + Copy>(
    handle: &mut ServerHandle,
    tasks: &[T],
) -> CancelTasksResponse {
    let msg = FromGatewayMessage::CancelTasks(CancelTasks {
        tasks: tasks.iter().map(|&id| id.into()).collect(),
    });
    handle.send(msg).await;
    wait_for_msg!(
        handle,
        ToGatewayMessage::CancelTasksResponse(msg) => msg
    )
}
