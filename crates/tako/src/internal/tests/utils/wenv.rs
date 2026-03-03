use crate::internal::messages::worker::ComputeTasksMsg;
use crate::internal::worker::comm::WorkerComm;
use crate::internal::worker::state::{WorkerState, WorkerStateRef};
use crate::launcher::{StopReason, TaskBuildContext, TaskLaunchData, TaskLauncher, TaskResult};
use crate::resources::{ResourceIdMap, ResourceRqMap};
use crate::tests::utils::env::TestComm;
use crate::tests::utils::task::TaskBuilder;
use crate::tests::utils::worker::WorkerBuilder;
use crate::tests::utils::worker_comm::TestWorkerComm;
use crate::{ResourceVariantId, TaskId, WorkerId};
use tokio::sync::oneshot::Receiver;

pub struct WorkerTestEnv {
    state: WorkerStateRef,
}

impl WorkerTestEnv {
    pub fn new(builder: &WorkerBuilder) -> WorkerTestEnv {
        let config = builder.build_config(WorkerId::new(321));
        let resource_map = ResourceIdMap::from_vec(
            config
                .resources
                .resources
                .iter()
                .map(|x| x.name.clone())
                .collect(),
        );
        let state = WorkerStateRef::new(
            WorkerComm::new_test_comm(),
            WorkerId::from(100),
            config,
            None,
            resource_map,
            ResourceRqMap::default(),
            Box::new(TestLauncher),
            "testuid".to_string(),
        );
        WorkerTestEnv { state }
    }

    pub fn compute_msg<V: Into<ResourceVariantId>>(
        &self,
        task_id: TaskId,
        variant: V,
        task: &TaskBuilder,
    ) -> ComputeTasksMsg {
        let mut state = self.state.get_mut();
        task.build_compute_msg(task_id, variant.into(), &mut state.resource_rq_map)
    }

    pub(crate) fn state(&mut self) -> &mut WorkerStateRef {
        &mut self.state
    }
}

#[derive(Default)]
struct TestLauncher;

impl TaskLauncher for TestLauncher {
    fn build_task(
        &self,
        _ctx: TaskBuildContext,
        _stop_receiver: Receiver<StopReason>,
    ) -> crate::Result<TaskLaunchData> {
        Ok(TaskLaunchData {
            task_future: Box::pin(async { Ok(TaskResult::Finished) }),
            task_context: vec![],
        })
    }
}
