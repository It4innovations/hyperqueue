use std::collections::BTreeMap;

use tako::messages::gateway::{
    CancelTasks, FromGatewayMessage, LostWorkerMessage, NewWorkerMessage, TaskFailedMessage,
    TaskState, TaskUpdate, ToGatewayMessage,
};

use crate::server::autoalloc::AutoAllocService;
use crate::server::event::events::JobInfo;
use crate::server::event::storage::EventStorage;
use crate::server::job::Job;
use crate::server::rpc::Backend;
use crate::server::worker::Worker;
use crate::WrappedRcRefCell;
use crate::{JobId, JobTaskCount, Map, TakoTaskId, WorkerId};
use std::cmp::min;
use tako::common::index::ItemId;
use tako::{define_wrapped_type, TaskId};

pub struct State {
    jobs: crate::Map<JobId, Job>,
    workers: crate::Map<WorkerId, Worker>,

    // Here we store TaskId -> JobId data, but to make it sparse
    // we store ONLY the base_task_id there, i.e. each job has here
    // only one entry.
    // Example:
    // Real mapping: TaskId   JobId
    //                 1   ->    1
    //                 2   ->    1
    //                 3   ->    2
    //                 4   ->    2
    //                 5   ->    2
    // The actual base_task_id_to_job will be 1 -> 1, 3 -> 2
    // Therefore we need to find biggest key that is lower then a given task id
    // To make this query efficient, we use BTreeMap and not Map
    base_task_id_to_job_id: BTreeMap<TakoTaskId, JobId>,
    job_id_counter: <JobId as ItemId>::IdType,
    task_id_counter: <TaskId as ItemId>::IdType,

    pub(crate) autoalloc_service: Option<AutoAllocService>,
    event_storage: EventStorage,
}

define_wrapped_type!(StateRef, State, pub);

fn cancel_tasks_from_callback(
    state_ref: &StateRef,
    tako_ref: &Backend,
    job_id: JobId,
    tasks: Vec<TakoTaskId>,
) {
    if tasks.is_empty() {
        return;
    }
    log::debug!("Canceling {:?} tasks", tasks);
    let tako_ref = tako_ref.clone();
    let state_ref = state_ref.clone();
    tokio::task::spawn_local(async move {
        let message = FromGatewayMessage::CancelTasks(CancelTasks { tasks });
        let response = tako_ref.send_tako_message(message).await.unwrap();

        match response {
            ToGatewayMessage::CancelTasksResponse(msg) => {
                let mut state = state_ref.get_mut();
                let job = state.get_job_mut(job_id).unwrap();
                log::debug!("Tasks {:?} canceled", msg.cancelled_tasks);
                log::debug!("Tasks {:?} already finished", msg.already_finished);
                for tako_id in msg.cancelled_tasks {
                    job.set_cancel_state(tako_id, &tako_ref);
                }
            }
            ToGatewayMessage::Error(msg) => {
                log::debug!("Canceling job {} failed: {}", job_id, msg.message);
            }
            _ => {
                panic!("Invalid message");
            }
        };
    });
}

impl State {
    pub fn get_job(&self, job_id: JobId) -> Option<&Job> {
        self.jobs.get(&job_id)
    }

    pub fn get_job_mut(&mut self, job_id: JobId) -> Option<&mut Job> {
        self.jobs.get_mut(&job_id)
    }

    pub fn jobs(&self) -> impl Iterator<Item = &Job> {
        self.jobs.values()
    }

    pub fn add_worker(&mut self, worker: Worker) {
        let worker_id = worker.worker_id();
        assert!(self.workers.insert(worker_id, worker).is_none())
    }

    pub fn add_job(&mut self, job: Job) {
        let job_id = job.job_id;
        assert!(self
            .base_task_id_to_job_id
            .insert(job.base_task_id, job_id)
            .is_none());
        self.event_storage.on_job_submitted(
            job_id,
            JobInfo {
                name: job.name.clone(),
                job_desc: job.job_desc.clone(),
                task_ids: job.tasks.iter().map(|(id, _)| *id).collect(),
                max_fails: job.max_fails,
                log: job.log.clone(),
                submission_date: job.submission_date,
            },
        );
        assert!(self.jobs.insert(job_id, job).is_none());

        if let Some(autoalloc) = &self.autoalloc_service {
            autoalloc.on_job_created(job_id);
        }
    }

    pub fn get_job_mut_by_tako_task_id(&mut self, task_id: TakoTaskId) -> Option<&mut Job> {
        let job_id: JobId = *self
            .base_task_id_to_job_id
            .range(..=task_id)
            .rev()
            .next()?
            .1;
        let job = self.jobs.get_mut(&job_id)?;
        if task_id
            < TakoTaskId::new(
                job.base_task_id.as_num() + job.n_tasks() as <TaskId as ItemId>::IdType,
            )
        {
            Some(job)
        } else {
            None
        }
    }

    pub fn new_job_id(&mut self) -> JobId {
        let id = self.job_id_counter;
        self.job_id_counter += 1;
        id.into()
    }

    pub fn revert_to_job_id(&mut self, id: JobId) {
        self.job_id_counter = id.as_num();
    }

    pub fn last_n_ids(&self, n: u32) -> impl Iterator<Item = JobId> {
        let n = min(n, self.job_id_counter - 1);
        ((self.job_id_counter - n)..self.job_id_counter).map(|id| id.into())
    }

    pub fn new_task_id(&mut self, task_count: JobTaskCount) -> TakoTaskId {
        let id = self.task_id_counter;
        self.task_id_counter += task_count as u32;
        id.into()
    }

    pub fn get_workers(&self) -> &Map<WorkerId, Worker> {
        &self.workers
    }

    pub fn get_worker(&self, worker_id: WorkerId) -> Option<&Worker> {
        self.workers.get(&worker_id)
    }

    pub fn get_worker_mut(&mut self, worker_id: WorkerId) -> Option<&mut Worker> {
        self.workers.get_mut(&worker_id)
    }

    pub fn process_task_failed(
        &mut self,
        state_ref: &StateRef,
        tako_ref: &Backend,
        msg: TaskFailedMessage,
    ) {
        log::debug!("Task id={} failed: {:?}", msg.id, msg.info);

        let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
        for task_id in msg.cancelled_tasks {
            log::debug!(
                "Task id={} canceled because of task dependency fails",
                task_id
            );
            job.set_cancel_state(task_id, tako_ref);
        }
        job.set_failed_state(msg.id, msg.info.message, tako_ref);

        if let Some(max_fails) = job.max_fails {
            if job.counters.n_failed_tasks > max_fails {
                let task_ids = job.non_finished_task_ids();
                cancel_tasks_from_callback(state_ref, tako_ref, job.job_id, task_ids);
            }
        }
        self.event_storage.on_task_failed(msg.id);
    }

    pub fn process_task_update(&mut self, msg: TaskUpdate, backend: &Backend) {
        log::debug!("Task id={} updated {:?}", msg.id, msg.state);
        let (mut job_id, mut is_job_terminated): (Option<JobId>, bool) = (None, false);
        match msg.state {
            TaskState::Running { worker_id, context } => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_running_state(msg.id, worker_id, context);
                self.event_storage.on_task_started(msg.id, worker_id);
            }
            TaskState::Finished => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_finished_state(msg.id, backend);
                (job_id, is_job_terminated) = (Some(job.job_id), job.is_terminated());
                self.event_storage.on_task_finished(msg.id);
            }
            TaskState::Waiting => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_waiting_state(msg.id);
            }
            TaskState::Invalid => {
                unreachable!()
            }
        };

        if is_job_terminated {
            self.event_storage
                .on_job_completed(job_id.unwrap(), chrono::offset::Utc::now());
        }
    }

    pub fn process_worker_new(&mut self, msg: NewWorkerMessage) {
        log::debug!("New worker id={}", msg.worker_id);
        self.add_worker(Worker::new(msg.worker_id, msg.configuration.clone()));
        // TODO: use observer in event storage instead of sending these messages directly
        if let Some(autoalloc) = &self.autoalloc_service {
            autoalloc.on_worker_connected(msg.worker_id, &msg.configuration);
        }

        self.event_storage
            .on_worker_added(msg.worker_id, msg.configuration);
    }

    pub fn process_worker_lost(&mut self, msg: LostWorkerMessage) {
        log::debug!("Worker lost id={}", msg.worker_id);
        let worker = self.workers.get_mut(&msg.worker_id).unwrap();
        worker.set_offline_state(msg.reason.clone());

        if let Some(autoalloc) = &self.autoalloc_service {
            autoalloc.on_worker_lost(msg.worker_id, &worker.configuration, msg.reason.clone());
        }

        for task_id in msg.running_tasks {
            let job = self.get_job_mut_by_tako_task_id(task_id).unwrap();
            job.set_waiting_state(task_id);
        }

        self.event_storage.on_worker_lost(msg.worker_id, msg.reason);
    }

    pub fn stop_autoalloc(&mut self) {
        // Drop the sender
        self.autoalloc_service = None;
    }

    pub fn event_storage(&self) -> &EventStorage {
        &self.event_storage
    }
    pub fn event_storage_mut(&mut self) -> &mut EventStorage {
        &mut self.event_storage
    }

    pub fn autoalloc(&self) -> &AutoAllocService {
        self.autoalloc_service.as_ref().unwrap()
    }
}

impl StateRef {
    pub fn new(event_storage: EventStorage) -> StateRef {
        Self(WrappedRcRefCell::wrap(State {
            jobs: Default::default(),
            workers: Default::default(),
            base_task_id_to_job_id: Default::default(),
            job_id_counter: 1,
            task_id_counter: 1,
            autoalloc_service: None,
            event_storage,
        }))
    }
}

#[cfg(test)]
mod tests {
    use tako::messages::common::{ProgramDefinition, StdioDef};

    use crate::common::arraydef::IntArray;
    use crate::server::job::Job;
    use crate::server::state::State;
    use crate::tests::utils::create_hq_state;
    use crate::transfer::messages::{JobDescription, TaskDescription};
    use crate::{JobId, TakoTaskId};

    fn dummy_program_definition() -> ProgramDefinition {
        ProgramDefinition {
            args: vec![],
            env: Default::default(),
            stdout: StdioDef::Null,
            stderr: StdioDef::Null,
            stdin: vec![],
            cwd: Default::default(),
        }
    }

    fn test_job<J: Into<JobId>, T: Into<TakoTaskId>>(
        ids: IntArray,
        job_id: J,
        base_task_id: T,
    ) -> Job {
        let job_desc = JobDescription::Array {
            ids,
            entries: None,
            task_desc: TaskDescription {
                program: dummy_program_definition(),
                resources: Default::default(),
                pin: false,
                task_dir: false,
                time_limit: None,
                priority: 0,
            },
        };
        Job::new(
            job_desc,
            job_id.into(),
            base_task_id.into(),
            "".to_string(),
            None,
            None,
            Default::default(),
        )
    }

    fn check_id<T: Into<TakoTaskId>>(state: &mut State, task_id: T, expected: Option<u32>) {
        assert_eq!(
            state
                .get_job_mut_by_tako_task_id(task_id.into())
                .map(|j| j.job_id.as_num()),
            expected
        );
    }

    #[test]
    fn test_find_job_id_by_task_id() {
        let state_ref = create_hq_state();
        let mut state = state_ref.get_mut();
        state.add_job(test_job(IntArray::from_range(0, 10), 223, 100));
        state.add_job(test_job(IntArray::from_range(0, 15), 224, 110));
        state.add_job(test_job(IntArray::from_id(0), 225, 125));
        state.add_job(test_job(IntArray::from_id(0), 226, 126));
        state.add_job(test_job(IntArray::from_id(0), 227, 130));

        let state = &mut state;
        check_id(state, 99, None);

        check_id(state, 100, Some(223));
        check_id(state, 101, Some(223));
        check_id(state, 109, Some(223));

        check_id(state, 110, Some(224));
        check_id(state, 124, Some(224));

        check_id(state, 125, Some(225));

        check_id(state, 126, Some(226));

        check_id(state, 127, None);
        check_id(state, 129, None);

        check_id(state, 130, Some(227));

        check_id(state, 131, None);
    }
}
