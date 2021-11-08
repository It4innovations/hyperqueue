use super::macros::wait_for_msg;
use crate::common::{Map, Set};
use crate::messages::common::TaskFailInfo;
use crate::messages::gateway::{
    CollectedOverview, FromGatewayMessage, OverviewRequest, TaskState, ToGatewayMessage,
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
    pub fn is_failed(&self, id: TaskId) -> bool {
        self.tasks[&id].is_failed()
    }
}

pub async fn wait_for_tasks(handle: &mut ServerHandle, tasks: Vec<TaskId>) -> TaskWaitResultMap {
    let mut tasks: Set<_> = tasks.into_iter().collect();
    let mut result = TaskWaitResultMap::default();

    while !tasks.is_empty() {
        match handle.recv().await {
            ToGatewayMessage::TaskUpdate(msg) => {
                if let TaskState::Finished = msg.state {
                    assert!(tasks.remove(&msg.id));
                }
                result
                    .tasks
                    .entry(msg.id)
                    .or_default()
                    .add(TaskResult::Update(msg.state));
            }
            ToGatewayMessage::TaskFailed(msg) => {
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
            msg => log::warn!("Received message {:?} while waiting for tasks", msg),
        };
    }
    result
}
