use crate::server::Senders;
use crate::server::state::StateRef;
use tako::events::EventProcessor;
use tako::gateway::LostWorkerReason;
use tako::internal::messages::common::TaskFailInfo;
use tako::task::SerializedTaskContext;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, TaskId, WorkerId};

pub(crate) struct UpstreamEventProcessor {
    state_ref: StateRef,
    senders: Senders,
}

impl UpstreamEventProcessor {
    pub fn new(state_ref: StateRef, senders: Senders) -> UpstreamEventProcessor {
        UpstreamEventProcessor { state_ref, senders }
    }
}

impl EventProcessor for UpstreamEventProcessor {
    fn on_task_finished(&self, task_id: TaskId) {
        self.state_ref
            .get_mut()
            .process_task_finished(&self.senders, task_id);
    }

    fn on_task_started(
        &self,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: &[WorkerId],
        context: SerializedTaskContext,
    ) {
        self.state_ref.get_mut().process_task_started(
            &self.senders,
            task_id,
            instance_id,
            worker_ids,
            context,
        );
    }

    fn on_task_error(&self, task_id: TaskId, consumers_id: Vec<TaskId>, error_info: TaskFailInfo) {
        self.state_ref.get_mut().process_task_failed(
            &self.senders,
            task_id,
            consumers_id,
            error_info,
        )
    }

    fn on_worker_new(&self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        self.state_ref
            .get_mut()
            .process_worker_new(&self.senders, worker_id, configuration)
    }

    fn on_worker_lost(
        &self,
        worker_id: WorkerId,
        running_tasks: &[TaskId],
        reason: LostWorkerReason,
    ) {
        self.state_ref.get_mut().process_worker_lost(
            &self.senders,
            worker_id,
            &running_tasks,
            reason,
        )
    }

    fn on_worker_overview(&self, overview: Box<WorkerOverview>) {
        todo!()
    }
}
