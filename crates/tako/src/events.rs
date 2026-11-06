use crate::gateway::{DrainMode, LostWorkerReason};
use crate::internal::messages::common::TaskFailInfo;
use crate::task::SerializedTaskContext;
use crate::worker::{WorkerConfiguration, WorkerOverview};
use crate::{InstanceId, ResourceVariantId, TaskId, WorkerId};

pub trait EventProcessor {
    fn on_task_finished(&mut self, task_id: TaskId);
    fn on_task_started(
        &mut self,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: &[WorkerId],
        rv_id: ResourceVariantId,
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
    /// The worker was drained (`Some`) or undrained (`None`).
    /// Called only when the drain setting of the worker actually changes.
    fn on_worker_drain_changed(&mut self, worker_id: WorkerId, drain: Option<DrainMode>);

    fn on_task_notify(&mut self, task_id: TaskId, worker_id: WorkerId, message: Box<[u8]>);
}
