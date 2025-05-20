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
    fn on_task_finished(&mut self, task_id: TaskId) {
        self.state_ref
            .get_mut()
            .process_task_finished(&self.senders, task_id);
    }

    fn on_task_started(
        &mut self,
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

    fn on_task_error(
        &mut self,
        task_id: TaskId,
        consumers_id: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) -> Vec<TaskId> {
        self.state_ref.get_mut().process_task_failed(
            &self.senders,
            task_id,
            consumers_id,
            error_info,
        )
    }

    fn on_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        self.state_ref
            .get_mut()
            .process_worker_new(&self.senders, worker_id, configuration)
    }

    fn on_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: &[TaskId],
        reason: LostWorkerReason,
    ) {
        self.state_ref.get_mut().process_worker_lost(
            &self.senders,
            worker_id,
            running_tasks,
            reason,
        )
    }

    fn on_worker_overview(&mut self, overview: Box<WorkerOverview>) {
        let state = self.state_ref.get();
        if let Some(worker) = state.get_worker(overview.id) {
            // We only want to persist worker overviews for workers that have
            // the overview interval explicitly enabled. If the overviews are
            // sent temporarily (because a client, e.g. a dashboard, is
            // streaming events), we only want to stream the event, but not
            // persist it.
            let persist_event = worker
                .configuration
                .overview_configuration
                .is_overview_enabled();
            self.senders
                .events
                .on_overview_received(overview, persist_event);
        }
    }
}
