use crate::internal::messages::worker::{TaskFinishedMsg, TaskOutput};
use crate::internal::scheduler::state::SchedulerState;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::{on_new_tasks, on_task_finished, on_task_running};
use crate::internal::server::task::Task;
use crate::internal::tests::utils::env::TestComm;
use crate::internal::tests::utils::task::task_running_msg;
use crate::{TaskId, WorkerId};
use std::time::Instant;

pub fn submit_test_tasks(core: &mut Core, tasks: Vec<Task>) {
    on_new_tasks(core, &mut TestComm::default(), tasks);
}

pub(crate) fn force_assign(
    core: &mut Core,
    scheduler: &mut SchedulerState,
    task_id: TaskId,
    worker_id: WorkerId,
) {
    core.remove_from_ready_to_assign(task_id);
    scheduler.assign(core, task_id, worker_id.into());
}

pub(crate) fn force_assign_mn(
    core: &mut Core,
    scheduler: &mut SchedulerState,
    workers: Vec<WorkerId>,
    task_id: TaskId,
) {
    let (task_map, worker_map) = core.split_tasks_workers_mut();
    let task = task_map.get_task_mut(task_id);
    scheduler.assign_multinode(worker_map, task, workers);
}

pub(crate) fn start_mn_task_on_worker(core: &mut Core, task_id: TaskId, worker_ids: Vec<WorkerId>) {
    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign_mn(
        core,
        &mut scheduler,
        worker_ids.into_iter().collect(),
        task_id,
    );
    scheduler.finish_scheduling(core, &mut comm);
}

pub fn set_as_running(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    let mut comm = TestComm::default();
    on_task_running(core, &mut comm, worker_id, task_running_msg(task_id));
}

pub fn assign_to_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign(core, &mut scheduler, task_id, worker_id);
    scheduler.finish_scheduling(core, &mut comm);
}

pub fn start_on_worker_running(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    let worker_id = worker_id.into();

    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign(core, &mut scheduler, task_id, worker_id);
    scheduler.finish_scheduling(core, &mut comm);
    on_task_running(core, &mut comm, worker_id, task_running_msg(task_id));
}

pub fn finish_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    finish_on_worker_with_data(core, task_id, worker_id, Vec::new());
}

pub fn finish_on_worker_with_data(
    core: &mut Core,
    task_id: TaskId,
    worker_id: WorkerId,
    outputs: Vec<TaskOutput>,
) {
    let mut comm = TestComm::default();
    on_task_finished(
        core,
        &mut comm,
        worker_id.into(),
        TaskFinishedMsg {
            id: task_id.into(),
            outputs,
        },
    );
}

pub fn start_and_finish_on_worker(core: &mut Core, task_id: TaskId, worker_id: WorkerId) {
    start_and_finish_on_worker_with_data(core, task_id, worker_id, Vec::new());
}

pub fn start_and_finish_on_worker_with_data(
    core: &mut Core,
    task_id: TaskId,
    worker_id: WorkerId,
    outputs: Vec<TaskOutput>,
) {
    let task_id = task_id.into();
    let worker_id = worker_id.into();

    assign_to_worker(core, task_id, worker_id);
    finish_on_worker_with_data(core, task_id, worker_id, outputs);
}

pub(crate) fn create_test_scheduler() -> SchedulerState {
    SchedulerState::new(Instant::now())
}
