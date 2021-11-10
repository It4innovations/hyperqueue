use super::macros::wait_for_msg;
use crate::common::{Map, Set};
use crate::messages::common::TaskFailInfo;
use crate::messages::gateway::{
    CancelTasks, CancelTasksResponse, CollectedOverview, FromGatewayMessage, OverviewRequest,
    TaskState, ToGatewayMessage,
};
use crate::tests::integration::utils::server::ServerHandle;
use crate::TaskId;

// Worker info
pub async fn get_overview(handler: &mut ServerHandle) -> CollectedOverview {
    handler
        .send(FromGatewayMessage::GetOverview(OverviewRequest {
            enable_hw_overview: true,
        }))
        .await;
    wait_for_msg!(handler, ToGatewayMessage::Overview(overview) => overview)
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
