use crate::gateway::LostWorkerReason;
use crate::internal::messages::common::TaskFailInfo;
use crate::task::SerializedTaskContext;
use crate::worker::{WorkerConfiguration, WorkerOverview};
use crate::{InstanceId, TaskId, WorkerId};

pub trait EventProcessor {
    fn on_task_finished(&mut self, task_id: TaskId);
    fn on_task_started(
        &mut self,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: &[WorkerId],
        context: SerializedTaskContext,
    );
    fn on_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) -> Vec<TaskId>;
    fn on_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration);
    fn on_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: &[TaskId],
        reason: LostWorkerReason,
    );
    fn on_worker_overview(&mut self, overview: Box<WorkerOverview>);

    fn on_task_notify(&mut self, task_id: TaskId, worker_id: WorkerId, message: Box<[u8]>);
}
