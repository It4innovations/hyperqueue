use crate::common::resources::ResourceDescriptor;
use crate::messages::common::WorkerConfiguration;
use crate::messages::worker::TaskFinishedMsg;
use crate::scheduler::state::SchedulerState;
use crate::server::core::Core;
use crate::server::reactor::{on_new_tasks, on_new_worker, on_task_finished, on_task_running};
use crate::server::task::TaskRef;
use crate::server::worker::Worker;
use crate::tests::utils::env::TestComm;
use crate::{TaskId, WorkerId};
use std::time::Duration;

pub fn create_test_workers(core: &mut Core, cpus: &[u32]) {
    for (i, c) in cpus.iter().enumerate() {
        let worker_id = WorkerId::new((100 + i) as u32);

        let wcfg = WorkerConfiguration {
            resources: ResourceDescriptor::simple(*c),
            listen_address: format!("1.1.1.{}:123", i),
            hostname: format!("test{}", i),
            work_dir: Default::default(),
            log_dir: Default::default(),
            heartbeat_interval: Duration::from_millis(1000),
            hw_state_poll_interval: Some(Duration::from_millis(1000)),
            idle_timeout: None,
            time_limit: None,
            extra: Default::default(),
        };

        let worker = Worker::new(worker_id, wcfg, Default::default());
        on_new_worker(core, &mut TestComm::default(), worker);
    }
}

pub fn submit_test_tasks(core: &mut Core, tasks: &[&TaskRef]) {
    on_new_tasks(
        core,
        &mut TestComm::default(),
        tasks.iter().map(|&tr| tr.clone()).collect(),
    );
}

pub(crate) fn force_assign<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    scheduler: &mut SchedulerState,
    task_id: T,
    worker_id: W,
) {
    let task_ref = core.get_task_by_id_or_panic(task_id.into()).clone();
    core.remove_from_ready_to_assign(&task_ref);
    let mut task = task_ref.get_mut();
    scheduler.assign(core, &mut task, task_ref.clone(), worker_id.into());
}

pub fn start_on_worker<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
) {
    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign(core, &mut scheduler, task_id.into(), worker_id.into());
    scheduler.finish_scheduling(&mut comm);
}

pub fn start_on_worker_running<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
) {
    let task_id = task_id.into();
    let worker_id = worker_id.into();

    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign(core, &mut scheduler, task_id, worker_id);
    scheduler.finish_scheduling(&mut comm);
    on_task_running(core, &mut comm, worker_id, task_id);
}

pub fn finish_on_worker<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
    size: u64,
) {
    let mut comm = TestComm::default();
    on_task_finished(
        core,
        &mut comm,
        worker_id.into(),
        TaskFinishedMsg {
            id: task_id.into(),
            size,
        },
    );
}

pub fn start_and_finish_on_worker<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
    size: u64,
) {
    let task_id = task_id.into();
    let worker_id = worker_id.into();

    start_on_worker(core, task_id, worker_id);
    finish_on_worker(core, task_id, worker_id, size);
}

pub(crate) fn create_test_scheduler() -> SchedulerState {
    SchedulerState::new()
}

impl SchedulerState {
    pub(crate) fn test_assign(&mut self, core: &mut Core, task_ref: &TaskRef, worker_id: WorkerId) {
        let mut task = task_ref.get_mut();
        self.assign(core, &mut task, task_ref.clone(), worker_id);
    }
}
