use crate::common::index::ItemId;
use crate::common::resources::descriptor::cpu_descriptor_from_socket_size;
use crate::common::resources::{GenericResourceDescriptor, NumOfCpus, ResourceDescriptor};
use crate::common::Map;
use crate::messages::common::{TaskFailInfo, WorkerConfiguration};
use crate::messages::gateway::LostWorkerReason;
use crate::messages::worker::{ToWorkerMessage, WorkerOverview};
use crate::scheduler::state::SchedulerState;
use crate::server::comm::Comm;
use crate::server::core::Core;
use crate::server::reactor::on_new_worker;
use crate::server::task::{SerializedTaskContext, Task};
use crate::server::worker::Worker;
use crate::server::worker_load::WorkerLoad;
use crate::tests::utils;
use crate::tests::utils::resources::cpus_compact;
use crate::tests::utils::schedule;
use crate::tests::utils::task::TaskBuilder;
use crate::transfer::auth::{deserialize, serialize};
use crate::worker::state::ServerLostPolicy;
use crate::{TaskId, WorkerId};
use std::time::Duration;

pub struct TestEnv {
    core: Core,
    scheduler: SchedulerState,
    pub task_id_counter: <TaskId as ItemId>::IdType,
    worker_id_counter: <WorkerId as ItemId>::IdType,
}

impl TestEnv {
    pub fn new() -> TestEnv {
        TestEnv {
            core: Default::default(),
            scheduler: schedule::create_test_scheduler(),
            task_id_counter: 10,
            worker_id_counter: 100,
        }
    }

    pub fn core(&mut self) -> &mut Core {
        &mut self.core
    }

    pub fn task(&self, task_id: TaskId) -> &Task {
        self.core.get_task(task_id)
    }

    pub fn new_task(&mut self, builder: TaskBuilder) -> &Task {
        let task = builder.build();
        let task_id = task.id;
        schedule::submit_test_tasks(&mut self.core, vec![task]);
        self.task(task_id)
    }

    pub fn new_generic_resource(&mut self, count: usize) {
        for i in 0..count {
            self.core
                .get_or_create_generic_resource_id(&format!("Res{}", i));
        }
    }

    pub fn new_task_assigned<W: Into<WorkerId>>(&mut self, builder: TaskBuilder, worker_id: W) {
        let task = builder.build();
        let task_id = task.id();
        schedule::submit_test_tasks(&mut self.core, vec![task]);
        schedule::start_on_worker(&mut self.core, task_id, worker_id.into());
    }

    pub fn new_task_running<W: Into<WorkerId>>(&mut self, builder: TaskBuilder, worker_id: W) {
        let task = builder.build();
        let task_id = task.id();
        schedule::submit_test_tasks(&mut self.core, vec![task]);
        schedule::start_on_worker_running(&mut self.core, task_id, worker_id.into());
    }

    pub fn worker<W: Into<WorkerId>>(&self, worker_id: W) -> &Worker {
        self.core.get_worker_by_id_or_panic(worker_id.into())
    }

    pub fn new_workers_ext(
        &mut self,
        defs: &[(u32, Option<Duration>, Vec<GenericResourceDescriptor>)],
    ) {
        for (i, (c, time_limit, grds)) in defs.iter().enumerate() {
            let worker_id = WorkerId::new(self.worker_id_counter);
            self.worker_id_counter += 1;

            let cpus = cpu_descriptor_from_socket_size(1, *c);
            let rd = ResourceDescriptor::new(cpus, grds.clone());

            let wcfg = WorkerConfiguration {
                resources: rd,
                listen_address: format!("1.1.1.{}:123", i),
                hostname: format!("test{}", i),
                work_dir: Default::default(),
                log_dir: Default::default(),
                heartbeat_interval: Duration::from_millis(1000),
                send_overview_interval: Some(Duration::from_millis(1000)),
                idle_timeout: None,
                time_limit: time_limit.clone(),
                on_server_lost: ServerLostPolicy::Stop,
                extra: Default::default(),
            };

            let worker = Worker::new(worker_id, wcfg, self.core.create_resource_map());
            on_new_worker(&mut self.core, &mut TestComm::default(), worker);
        }
    }

    pub fn new_workers(&mut self, cpus: &[u32]) {
        let defs: Vec<_> = cpus.iter().map(|c| (*c, None, Vec::new())).collect();
        self.new_workers_ext(&defs);
    }

    pub fn new_ready_tasks_cpus(&mut self, tasks: &[NumOfCpus]) -> Vec<TaskId> {
        let tasks: Vec<_> = tasks
            .iter()
            .map(|n_cpus| {
                let task_id = self.task_id_counter;
                self.task_id_counter += 1;
                TaskBuilder::new(task_id)
                    .resources(cpus_compact(*n_cpus))
                    .build()
            })
            .collect();
        let task_ids: Vec<_> = tasks.iter().map(|t| t.id).collect();
        schedule::submit_test_tasks(&mut self.core, tasks);
        task_ids
    }

    pub fn _test_assign(&mut self, task_id: TaskId, worker_id: WorkerId) {
        self.scheduler.assign(&mut self.core, task_id, worker_id);
        self.core.remove_from_ready_to_assign(task_id);
    }

    pub fn test_assign<T: Into<TaskId>, W: Into<WorkerId>>(&mut self, task_id: T, worker_id: W) {
        self._test_assign(task_id.into(), worker_id.into());
    }

    pub fn new_assigned_tasks_cpus(&mut self, tasks: &[&[NumOfCpus]]) {
        for (i, tdefs) in tasks.iter().enumerate() {
            let w_id = WorkerId::new(100 + i as u32);
            let task_ids = self.new_ready_tasks_cpus(tdefs);
            for task_id in task_ids {
                self._test_assign(task_id, w_id);
            }
        }
    }

    pub fn check_worker_tasks<W: Into<WorkerId>, T: Into<TaskId> + Copy>(
        &self,
        worker_id: W,
        tasks: &[T],
    ) {
        let worker_id = worker_id.into();
        let ids = utils::sorted_vec(
            self.core
                .get_worker_by_id_or_panic(worker_id)
                .tasks()
                .iter()
                .copied()
                .collect(),
        );
        assert_eq!(
            ids,
            utils::sorted_vec(tasks.iter().map(|&id| id.into()).collect())
        );
    }

    pub fn worker_load<W: Into<WorkerId>>(&self, worker_id: W) -> &WorkerLoad {
        &self.core.get_worker_by_id_or_panic(worker_id.into()).load
    }

    pub fn check_worker_load_lower_bounds(&self, cpus: &[NumOfCpus]) {
        let found_cpus: Vec<NumOfCpus> = utils::sorted_vec(
            self.core
                .get_workers()
                .map(|w| w.load.get_n_cpus())
                .collect(),
        );
        for (c, f) in cpus.iter().zip(found_cpus.iter()) {
            assert!(c <= f);
        }
    }

    pub fn finish_scheduling(&mut self) {
        let mut comm = create_test_comm();
        self.scheduler.finish_scheduling(&mut self.core, &mut comm);
        self.core.sanity_check();
        println!("-------------");
        for worker in self.core.get_workers() {
            println!(
                "Worker {} ({}) {}",
                worker.id,
                worker.load.get_n_cpus(),
                worker
                    .tasks()
                    .iter()
                    .map(|&task_id| format!(
                        "{}:{:?}",
                        task_id,
                        self.core.get_task(task_id).configuration.resources
                    ))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
        }
    }

    pub fn schedule(&mut self) {
        let mut comm = create_test_comm();
        self.scheduler.run_scheduling(&mut self.core, &mut comm);
        self.core.sanity_check();
    }

    pub fn balance(&mut self) {
        self.scheduler.balance(&mut self.core);
        self.finish_scheduling();
    }
}

#[derive(Default, Debug)]
pub struct TestComm {
    pub worker_msgs: Map<WorkerId, Vec<ToWorkerMessage>>,
    pub broadcast_msgs: Vec<ToWorkerMessage>,

    pub client_task_finished: Vec<TaskId>,
    pub client_task_running: Vec<TaskId>,
    pub client_task_errors: Vec<(TaskId, Vec<TaskId>, TaskFailInfo)>,

    pub new_workers: Vec<(WorkerId, WorkerConfiguration)>,
    pub lost_workers: Vec<(WorkerId, Vec<TaskId>)>,
    pub worker_overviews: Vec<WorkerOverview>,

    pub need_scheduling: bool,
}

impl TestComm {
    pub fn take_worker_msgs<T: Into<WorkerId>>(
        &mut self,
        worker_id: T,
        len: usize,
    ) -> Vec<ToWorkerMessage> {
        let worker_id: WorkerId = worker_id.into();
        let msgs = match self.worker_msgs.remove(&worker_id) {
            None => {
                panic!("No messages for worker {}", worker_id)
            }
            Some(x) => x,
        };
        if len != 0 {
            assert_eq!(msgs.len(), len);
        }
        msgs
    }

    pub fn take_broadcasts(&mut self, len: usize) -> Vec<ToWorkerMessage> {
        assert_eq!(self.broadcast_msgs.len(), len);
        std::mem::take(&mut self.broadcast_msgs)
    }

    pub fn take_client_task_finished(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.client_task_finished.len(), len);
        std::mem::take(&mut self.client_task_finished)
    }

    pub fn take_client_task_running(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.client_task_running.len(), len);
        std::mem::take(&mut self.client_task_running)
    }

    pub fn take_client_task_errors(
        &mut self,
        len: usize,
    ) -> Vec<(TaskId, Vec<TaskId>, TaskFailInfo)> {
        assert_eq!(self.client_task_errors.len(), len);
        std::mem::take(&mut self.client_task_errors)
    }

    pub fn take_new_workers(&mut self) -> Vec<(WorkerId, WorkerConfiguration)> {
        std::mem::take(&mut self.new_workers)
    }

    pub fn take_lost_workers(&mut self) -> Vec<(WorkerId, Vec<TaskId>)> {
        std::mem::take(&mut self.lost_workers)
    }

    pub fn check_need_scheduling(&mut self) {
        assert!(self.need_scheduling);
        self.need_scheduling = false;
    }

    pub fn emptiness_check(&self) {
        if !self.worker_msgs.is_empty() {
            let ids: Vec<_> = self.worker_msgs.keys().collect();
            panic!("Unexpected worker messages for workers: {:?}", ids);
        }
        assert!(self.broadcast_msgs.is_empty());

        assert!(self.client_task_finished.is_empty());
        assert!(self.client_task_running.is_empty());
        assert!(self.client_task_errors.is_empty());

        assert!(self.new_workers.is_empty());
        assert!(self.lost_workers.is_empty());

        assert!(!self.need_scheduling);
    }
}

impl Comm for TestComm {
    fn send_worker_message(&mut self, worker_id: WorkerId, message: &ToWorkerMessage) {
        let data = serialize(&message).unwrap();
        let message = deserialize(&data).unwrap();
        self.worker_msgs.entry(worker_id).or_default().push(message);
    }

    fn broadcast_worker_message(&mut self, message: &ToWorkerMessage) {
        let data = serialize(&message).unwrap();
        let message = deserialize(&data).unwrap();
        self.broadcast_msgs.push(message);
    }

    fn ask_for_scheduling(&mut self) {
        self.need_scheduling = true;
    }

    fn send_client_task_finished(&mut self, task_id: TaskId) {
        self.client_task_finished.push(task_id);
    }

    fn send_client_task_started(
        &mut self,
        task_id: TaskId,
        _worker_id: WorkerId,
        _context: SerializedTaskContext,
    ) {
        self.client_task_running.push(task_id);
    }

    fn send_client_task_error(
        &mut self,
        task_id: TaskId,
        consumers: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) {
        self.client_task_errors
            .push((task_id, consumers, error_info));
    }

    fn send_client_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        self.new_workers.push((worker_id, configuration.clone()));
    }

    fn send_client_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: Vec<TaskId>,
        _reason: LostWorkerReason,
    ) {
        self.lost_workers.push((worker_id, running_tasks));
    }

    fn send_client_worker_overview(&mut self, overview: WorkerOverview) {
        self.worker_overviews.push(overview);
    }
}

pub fn create_test_comm() -> TestComm {
    TestComm::default()
}
