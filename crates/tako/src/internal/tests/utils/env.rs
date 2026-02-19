use crate::events::EventProcessor;
use crate::gateway::LostWorkerReason;
use crate::internal::common::Map;
use crate::internal::common::index::ItemId;
use crate::internal::common::resources::ResourceId;
use crate::internal::common::utils::format_comma_delimited;
use crate::internal::messages::common::TaskFailInfo;
use crate::internal::messages::worker::{
    TaskFinishedMsg, TaskOutput, TaskRunningMsg, ToWorkerMessage, WorkerOverview,
};
use crate::internal::scheduler::{
    TaskBatch, WorkerTaskMapping, create_task_batches, create_task_mapping, run_scheduling,
    run_scheduling_inner, run_scheduling_solver,
};
use crate::internal::server::comm::Comm;
use crate::internal::server::core::{Core, CoreSplitMut};
use crate::internal::server::reactor::{
    on_new_tasks, on_new_worker, on_task_finished, on_task_running,
};
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::worker::Worker;
use crate::internal::server::workerload::WorkerLoad;
use crate::internal::tests::utils;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::internal::transfer::auth::{deserialize, serialize};
use crate::resources::{ResourceAmount, ResourceUnits};
use crate::task::SerializedTaskContext;
use crate::tests::utils::task::task_running_msg;
use crate::tests::utils::worker::WorkerBuilder;
use crate::worker::WorkerConfiguration;
use crate::{InstanceId, JobId, JobTaskId, ResourceVariantId, Set, TaskId, WorkerId};
use std::time::Instant;

pub struct TestEnv {
    core: Core,
    job_id: JobId,
    task_id_counter: u32,
    worker_id_counter: <WorkerId as ItemId>::IdType,
    now: Instant,
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

impl TestEnv {
    pub fn new() -> TestEnv {
        TestEnv {
            core: Default::default(),
            now: Instant::now(),
            job_id: JobId::new(1),
            task_id_counter: 1,
            worker_id_counter: 50,
        }
    }

    pub fn set_job<J: Into<JobId>>(&mut self, job_id: J, task_id_counter: u32) {
        self.job_id = job_id.into();
        self.task_id_counter = task_id_counter;
    }

    pub fn core(&mut self) -> &mut Core {
        &mut self.core
    }

    pub fn task(&self, task_id: TaskId) -> &Task {
        self.core.get_task(task_id)
    }

    pub fn task_exists(&self, task_id: TaskId) -> bool {
        self.core.find_task(task_id).is_some()
    }

    pub(crate) fn task_map(&self) -> &TaskMap {
        self.core.task_map()
    }

    pub(crate) fn sanity_check(&self) {
        self.core.sanity_check();
    }

    pub fn new_task(&mut self, builder: &TaskBuilder) -> TaskId {
        let task_id = TaskId::new(self.job_id, JobTaskId::new(self.task_id_counter));
        self.task_id_counter += 1;
        let task = builder.build(task_id, &mut self.core);
        on_new_tasks(&mut self.core, &mut TestComm::default(), vec![task]);
        task_id
    }

    pub fn new_task_cpus(&mut self, cpus: u32) -> TaskId {
        self.new_task(&TaskBuilder::new().cpus(cpus))
    }

    pub fn new_task_default(&mut self) -> TaskId {
        self.new_task(&TaskBuilder::new())
    }

    pub fn new_generic_resource(&mut self, count: usize) {
        for i in 0..count {
            self.core.get_or_create_resource_id(&format!("Res{i}"));
        }
    }

    pub fn new_named_resource(&mut self, name: &str) -> ResourceId {
        self.core.get_or_create_resource_id(name)
    }

    pub fn new_tasks(&mut self, n: usize, task_builder: &TaskBuilder) -> Vec<TaskId> {
        (0..n).map(|_| self.new_task(task_builder)).collect()
    }

    pub fn new_task_assigned(&mut self, builder: &TaskBuilder, worker_id: WorkerId) -> TaskId {
        let task_id = self.new_task(builder);
        self.assign_task(task_id, worker_id);
        task_id
    }

    pub fn new_task_running(&mut self, builder: &TaskBuilder, worker_id: WorkerId) -> TaskId {
        let task_id = self.new_task_assigned(builder, worker_id);
        self.start_task(task_id, ResourceVariantId::new(0));
        task_id
    }

    pub fn new_tasks_cpus(&mut self, tasks: &[ResourceUnits]) -> Vec<TaskId> {
        tasks
            .iter()
            .map(|n_cpus| self.new_task_cpus(*n_cpus))
            .collect()
    }

    pub fn worker(&self, worker_id: WorkerId) -> &Worker {
        self.core.get_worker_by_id_or_panic(worker_id)
    }

    pub fn worker_tasks(&self, worker_id: WorkerId) -> &Set<TaskId> {
        &self.worker(worker_id).sn_assignment().unwrap().assign_tasks
    }

    pub fn new_worker(&mut self, builder: &WorkerBuilder) -> WorkerId {
        let worker_id = WorkerId::new(self.worker_id_counter);
        self.worker_id_counter += 1;
        let resource_id_map = self.core.create_resource_map();
        let worker = builder.build(worker_id, &&resource_id_map, Instant::now());
        on_new_worker(&mut self.core, &mut TestComm::default(), worker);
        worker_id
    }

    pub fn new_workers(&mut self, n: usize, builder: &WorkerBuilder) -> Vec<WorkerId> {
        (0..n).map(|_| self.new_worker(builder)).collect()
    }

    pub fn new_worker_cpus(&mut self, cpus: u32) -> WorkerId {
        self.new_worker(&WorkerBuilder::new(cpus))
    }

    pub fn new_workers_cpus(&mut self, cpus: &[u32]) -> Vec<WorkerId> {
        cpus.iter()
            .map(|c| self.new_worker(&WorkerBuilder::new(*c)))
            .collect()
    }

    pub fn check_worker_tasks(&self, worker_id: WorkerId, tasks: &[TaskId]) {
        let ids: Vec<TaskId> = self.worker_tasks(worker_id).iter().copied().collect();
        assert_eq!(
            ids,
            utils::sorted_vec(tasks.iter().map(|&id| id.into()).collect())
        );
    }

    pub fn start_and_finish_task(&mut self, task_id: TaskId, worker_id: WorkerId) {
        self.assign_and_start_task(task_id, worker_id, 0);
        self.finish_task(task_id, worker_id);
    }

    pub fn finish_task(&mut self, task_id: TaskId, worker_id: WorkerId) {
        self.finish_task_with_data(task_id, worker_id, Vec::new());
    }

    pub fn finish_task_with_data(
        &mut self,
        task_id: TaskId,
        worker_id: WorkerId,
        outputs: Vec<TaskOutput>,
    ) {
        let mut comm = TestComm::new();
        on_task_finished(
            &mut self.core,
            &mut comm,
            worker_id.into(),
            TaskFinishedMsg {
                id: task_id.into(),
                outputs,
            },
        );
    }

    pub fn assign_task(&mut self, task_id: TaskId, worker_id: WorkerId) {
        let CoreSplitMut {
            task_map,
            worker_map,
            request_map,
            ..
        } = self.core.split_mut();
        let task = task_map.get_task_mut(task_id);
        match &task.state {
            TaskRuntimeState::Waiting { unfinished_deps } => {
                if *unfinished_deps > 0 {
                    panic!("Task {} is not ready", task_id);
                }
                task.state = TaskRuntimeState::Assigned {
                    worker_id,
                    rv_id: 0.into(),
                };
            }

            _ => {
                panic!("Task {} is not waiting", task_id);
            }
        }
        let w = worker_map.get_worker_mut(worker_id.into());
        let rq = request_map
            .get(task.resource_rq_id)
            .get(ResourceVariantId::new(0));
        w.insert_sn_task(task_id, rq);
        self.core.remove_from_ready_queue(task_id);
    }

    pub fn start_task<V: Into<ResourceVariantId>>(&mut self, task_id: TaskId, variant: V) {
        let task = self.core.get_task(task_id);
        let worker_id = match task.state {
            TaskRuntimeState::Assigned { worker_id, .. } => worker_id,
            _ => panic!("Task {} is not assigned", task_id),
        };
        let mut comm = TestComm::default();
        on_task_running(
            &mut self.core,
            &mut comm,
            worker_id,
            TaskRunningMsg {
                id: task_id,
                rv_id: variant.into(),
                context: Default::default(),
            },
        );
    }

    pub fn assign_and_start_task<V: Into<ResourceVariantId>>(
        &mut self,
        task_id: TaskId,
        worker_id: WorkerId,
        variant: V,
    ) {
        self.assign_task(task_id, worker_id);
        self.start_task(task_id, variant);
    }

    pub fn schedule(&mut self) {
        let mut comm = TestComm::new();
        self.schedule_with_comm(&mut comm);
        self.core.sanity_check();
    }

    pub fn schedule_with_comm(&mut self, comm: &mut TestComm) {
        run_scheduling_inner(&mut self.core, comm, self.now);
    }

    pub fn schedule_mapping(&mut self) -> WorkerTaskMapping {
        let batches = create_task_batches(&mut self.core, self.now, &[]);
        let solution = run_scheduling_solver(&mut self.core, self.now, &batches, &[]);
        create_task_mapping(&mut self.core, solution)
    }
}

#[derive(Default, Debug)]
pub struct TestClientProcessor {
    pub task_finished: Vec<TaskId>,
    pub task_running: Vec<TaskId>,
    pub task_errors: Vec<(TaskId, Vec<TaskId>, TaskFailInfo)>,

    pub new_workers: Vec<(WorkerId, WorkerConfiguration)>,
    pub lost_workers: Vec<(WorkerId, Vec<TaskId>)>,
    pub worker_overviews: Vec<Box<WorkerOverview>>,
}

impl TestClientProcessor {
    pub fn take_task_finished(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.task_finished.len(), len);
        std::mem::take(&mut self.task_finished)
    }

    pub fn take_task_running(&mut self, len: usize) -> Vec<TaskId> {
        assert_eq!(self.task_running.len(), len);
        std::mem::take(&mut self.task_running)
    }

    pub fn take_task_errors(&mut self, len: usize) -> Vec<(TaskId, Vec<TaskId>, TaskFailInfo)> {
        assert_eq!(self.task_errors.len(), len);
        std::mem::take(&mut self.task_errors)
    }

    pub fn take_new_workers(&mut self) -> Vec<(WorkerId, WorkerConfiguration)> {
        std::mem::take(&mut self.new_workers)
    }

    pub fn take_lost_workers(&mut self) -> Vec<(WorkerId, Vec<TaskId>)> {
        std::mem::take(&mut self.lost_workers)
    }

    pub fn emptiness_check(&self) {
        assert!(self.task_finished.is_empty());
        assert!(self.task_running.is_empty());
        assert!(self.task_errors.is_empty());

        assert!(self.new_workers.is_empty());
        assert!(self.lost_workers.is_empty());
    }
}

#[derive(Default, Debug)]
pub struct TestComm {
    pub worker_msgs: Map<WorkerId, Vec<ToWorkerMessage>>,
    pub broadcast_msgs: Vec<ToWorkerMessage>,
    pub client: TestClientProcessor,
    pub need_scheduling: bool,
}

impl TestComm {
    pub fn take_worker_msgs(&mut self, worker_id: WorkerId, len: usize) -> Vec<ToWorkerMessage> {
        let msgs = self.worker_msgs.remove(&worker_id).unwrap_or_default();
        if len != 0 {
            assert_eq!(msgs.len(), len);
        }
        msgs
    }

    pub fn take_broadcasts(&mut self, len: usize) -> Vec<ToWorkerMessage> {
        assert_eq!(self.broadcast_msgs.len(), len);
        std::mem::take(&mut self.broadcast_msgs)
    }

    pub fn check_need_scheduling(&mut self) {
        assert!(self.need_scheduling);
        self.need_scheduling = false;
    }

    pub fn emptiness_check(&self) {
        if !self.worker_msgs.is_empty() {
            let ids: Vec<_> = self.worker_msgs.keys().collect();
            panic!("Unexpected worker messages for workers: {ids:?}");
        }
        assert!(self.broadcast_msgs.is_empty());
        self.client.emptiness_check();
        assert!(!self.need_scheduling);
    }
}

impl EventProcessor for TestClientProcessor {
    fn on_task_finished(&mut self, task_id: TaskId) {
        self.task_finished.push(task_id);
    }

    fn on_task_started(
        &mut self,
        task_id: TaskId,
        _instance_id: InstanceId,
        _worker_id: &[WorkerId],
        _rv_id: ResourceVariantId,
        _context: SerializedTaskContext,
    ) {
        self.task_running.push(task_id);
    }

    fn on_task_error(
        &mut self,
        task_id: TaskId,
        consumers: Vec<TaskId>,
        error_info: TaskFailInfo,
    ) -> Vec<TaskId> {
        self.task_errors.push((task_id, consumers, error_info));
        Vec::new()
    }

    fn on_worker_new(&mut self, worker_id: WorkerId, configuration: &WorkerConfiguration) {
        self.new_workers.push((worker_id, configuration.clone()));
    }

    fn on_worker_lost(
        &mut self,
        worker_id: WorkerId,
        running_tasks: &[TaskId],
        _reason: LostWorkerReason,
    ) {
        self.lost_workers.push((worker_id, running_tasks.to_vec()));
    }

    fn on_worker_overview(&mut self, overview: Box<WorkerOverview>) {
        self.worker_overviews.push(overview);
    }

    fn on_task_notify(&mut self, _task_id: TaskId, _worker_id: WorkerId, _message: Box<[u8]>) {
        todo!()
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

    fn client(&mut self) -> &mut dyn EventProcessor {
        &mut self.client
    }
}

impl TestComm {
    pub fn new() -> Self {
        Default::default()
    }
}
