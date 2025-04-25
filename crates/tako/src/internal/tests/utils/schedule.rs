use crate::internal::common::resources::ResourceDescriptor;
use crate::internal::messages::worker::{TaskFinishedMsg, TaskOutput};
use crate::internal::scheduler::state::SchedulerState;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::{
    on_new_tasks, on_new_worker, on_task_finished, on_task_running,
};
use crate::internal::server::task::Task;
use crate::internal::server::worker::Worker;
use crate::internal::tests::utils::env::TestComm;
use crate::internal::tests::utils::task::task_running_msg;
use crate::internal::worker::configuration::{
    DEFAULT_MAX_DOWNLOAD_TRIES, DEFAULT_MAX_PARALLEL_DOWNLOADS,
    DEFAULT_WAIT_BETWEEN_DOWNLOAD_TRIES, OverviewConfiguration,
};
use crate::resources::ResourceMap;
use crate::worker::{ServerLostPolicy, WorkerConfiguration};
use crate::{TaskId, WorkerId};
use std::process::Output;
use std::time::{Duration, Instant};

pub fn create_test_worker_config(
    worker_id: WorkerId,
    resources: ResourceDescriptor,
) -> WorkerConfiguration {
    WorkerConfiguration {
        resources,
        listen_address: format!("1.1.1.{}:123", worker_id),
        hostname: format!("test{}", worker_id),
        group: "default".to_string(),
        work_dir: Default::default(),
        heartbeat_interval: Duration::from_millis(1000),
        overview_configuration: OverviewConfiguration {
            send_interval: Some(Duration::from_millis(1000)),
            gpu_families: Default::default(),
        },
        idle_timeout: None,
        time_limit: None,
        on_server_lost: ServerLostPolicy::Stop,
        max_parallel_downloads: DEFAULT_MAX_PARALLEL_DOWNLOADS,
        max_download_tries: DEFAULT_MAX_DOWNLOAD_TRIES,
        wait_between_download_tries: DEFAULT_WAIT_BETWEEN_DOWNLOAD_TRIES,
        extra: Default::default(),
    }
}

pub fn new_test_worker(
    core: &mut Core,
    worker_id: WorkerId,
    configuration: WorkerConfiguration,
    resource_map: ResourceMap,
) {
    let worker = Worker::new(worker_id, configuration, resource_map);
    on_new_worker(core, &mut TestComm::default(), worker);
}

pub fn create_test_worker(core: &mut Core, worker_id: WorkerId, cpus: u32) {
    let wcfg = create_test_worker_config(worker_id, ResourceDescriptor::simple(cpus));
    new_test_worker(
        core,
        worker_id,
        wcfg,
        ResourceMap::from_vec(vec!["cpus".to_string()]),
    );
}

pub fn create_test_workers(core: &mut Core, cpus: &[u32]) {
    for (i, c) in cpus.iter().enumerate() {
        let worker_id = WorkerId::new((100 + i) as u32);
        create_test_worker(core, worker_id, *c);
    }
}

pub fn submit_test_tasks(core: &mut Core, tasks: Vec<Task>) {
    on_new_tasks(core, &mut TestComm::default(), tasks);
}

pub(crate) fn force_assign<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    scheduler: &mut SchedulerState,
    task_id: T,
    worker_id: W,
) {
    let task_id = task_id.into();
    core.remove_from_ready_to_assign(task_id);
    scheduler.assign(core, task_id, worker_id.into());
}

pub(crate) fn force_assign_mn(
    core: &mut Core,
    scheduler: &mut SchedulerState,
    workers: Vec<WorkerId>,
    task_id: TaskId,
) {
    core.remove_from_ready_to_assign(task_id);
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

pub fn start_on_worker<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
) {
    let mut scheduler = create_test_scheduler();
    let mut comm = TestComm::default();
    force_assign(core, &mut scheduler, task_id.into(), worker_id.into());
    scheduler.finish_scheduling(core, &mut comm);
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
    scheduler.finish_scheduling(core, &mut comm);
    on_task_running(core, &mut comm, worker_id, task_running_msg(task_id));
}

pub fn finish_on_worker<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
) {
    finish_on_worker_with_data(core, task_id, worker_id, Vec::new());
}

pub fn finish_on_worker_with_data<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
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

pub fn start_and_finish_on_worker<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
) {
    start_and_finish_on_worker_with_data(core, task_id, worker_id, Vec::new());
}

pub fn start_and_finish_on_worker_with_data<W: Into<WorkerId>, T: Into<TaskId>>(
    core: &mut Core,
    task_id: T,
    worker_id: W,
    outputs: Vec<TaskOutput>,
) {
    let task_id = task_id.into();
    let worker_id = worker_id.into();

    start_on_worker(core, task_id, worker_id);
    finish_on_worker_with_data(core, task_id, worker_id, outputs);
}

pub(crate) fn create_test_scheduler() -> SchedulerState {
    SchedulerState::new(Instant::now())
}
