use std::cmp::min;
use std::collections::BTreeMap;

use chrono::Utc;

use tako::gateway::{
    CancelTasks, FromGatewayMessage, LostWorkerMessage, NewWorkerMessage, TaskFailedMessage,
    TaskState, TaskUpdate, ToGatewayMessage,
};
use tako::ItemId;
use tako::{define_wrapped_type, TaskId};

use crate::server::autoalloc::LostWorkerDetails;
use crate::server::job::Job;
use crate::server::restore::StateRestorer;
use crate::server::worker::Worker;
use crate::server::Senders;
use crate::transfer::messages::ServerInfo;
use crate::WrappedRcRefCell;
use crate::{JobId, JobTaskCount, Map, TakoTaskId, WorkerId};

pub struct State {
    jobs: Map<JobId, Job>,
    workers: Map<WorkerId, Worker>,

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
    task_id_counter: <TakoTaskId as ItemId>::IdType,

    server_info: ServerInfo,
}

define_wrapped_type!(StateRef, State, pub);

fn cancel_tasks_from_callback(
    state_ref: &StateRef,
    senders: &Senders,
    job_id: JobId,
    tasks: Vec<TakoTaskId>,
) {
    if tasks.is_empty() {
        return;
    }
    log::debug!("Canceling {:?} tasks", tasks);
    let senders2 = senders.clone();
    let state_ref = state_ref.clone();
    tokio::task::spawn_local(async move {
        let message = FromGatewayMessage::CancelTasks(CancelTasks { tasks });
        let response = senders2.backend.send_tako_message(message).await.unwrap();

        match response {
            ToGatewayMessage::CancelTasksResponse(msg) => {
                let mut state = state_ref.get_mut();
                if let Some(job) = state.get_job_mut(job_id) {
                    log::debug!("Tasks {:?} canceled", msg.cancelled_tasks);
                    log::debug!("Tasks {:?} already finished", msg.already_finished);
                    for tako_id in msg.cancelled_tasks {
                        job.set_cancel_state(tako_id, &senders2);
                    }
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

    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    pub fn set_worker_port(&mut self, port: u16) {
        self.server_info.worker_port = port;
    }

    pub fn add_job(&mut self, job: Job) {
        let job_id = job.job_id;
        assert!(self
            .base_task_id_to_job_id
            .insert(job.base_task_id, job_id)
            .is_none());
        assert!(self.jobs.insert(job_id, job).is_none());
    }

    /// Completely forgets this job, in order to reduce memory usage.
    pub(crate) fn forget_job(&mut self, job_id: JobId) -> Option<Job> {
        let job = match self.jobs.remove(&job_id) {
            Some(job) => {
                assert!(job.is_terminated());
                job
            }
            None => {
                log::error!("Trying to forget unknown job {job_id}");
                return None;
            }
        };
        self.base_task_id_to_job_id.remove(&job.base_task_id);
        Some(job)
    }

    pub fn get_job_mut_by_tako_task_id(&mut self, task_id: TakoTaskId) -> Option<&mut Job> {
        let job_id: JobId = *self.base_task_id_to_job_id.range(..=task_id).next_back()?.1;
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
        self.task_id_counter += task_count;
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
        senders: &Senders,
        msg: TaskFailedMessage,
    ) {
        log::debug!("Task id={} failed: {:?}", msg.id, msg.info);

        let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
        for task_id in msg.cancelled_tasks {
            log::debug!(
                "Task id={} canceled because of task dependency fails",
                task_id
            );
            job.set_cancel_state(task_id, senders);
        }
        let task_id = job.set_failed_state(msg.id, msg.info.message.clone(), senders);

        if let Some(max_fails) = &job.job_desc.max_fails {
            if job.counters.n_failed_tasks > *max_fails {
                let task_ids = job.non_finished_task_ids();
                cancel_tasks_from_callback(state_ref, senders, job.job_id, task_ids);
            }
        }
        let job_id = job.job_id;
        senders
            .events
            .on_task_failed(job_id, task_id, msg.info.message);
    }

    pub fn process_task_update(&mut self, msg: TaskUpdate, senders: &Senders) {
        log::debug!("Task id={} updated {:?}", msg.id, msg.state);
        match msg.state {
            TaskState::Running {
                instance_id,
                worker_ids,
                context,
            } => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                let task_id = job.set_running_state(msg.id, worker_ids.clone(), context);

                let job_id = job.job_id;
                senders
                    .events
                    .on_task_started(job_id, task_id, instance_id, worker_ids.clone());
            }
            TaskState::Finished => {
                let now = Utc::now();
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                let task_id = job.set_finished_state(msg.id, now, senders);
                let is_job_terminated = job.is_terminated();
                let job_id = job.job_id;
                senders.events.on_task_finished(job_id, task_id);
                if is_job_terminated {
                    senders.events.on_job_completed(job_id);
                }
            }
            TaskState::Waiting => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_waiting_state(msg.id);
            }
            TaskState::Invalid => {
                unreachable!()
            }
        };
    }

    pub fn process_worker_new(&mut self, msg: NewWorkerMessage, senders: &Senders) {
        log::debug!("New worker id={}", msg.worker_id);
        self.add_worker(Worker::new(msg.worker_id, msg.configuration.clone()));
        // TODO: use observer in event storage instead of sending these messages directly
        senders
            .autoalloc
            .on_worker_connected(msg.worker_id, &msg.configuration);
        senders
            .events
            .on_worker_added(msg.worker_id, msg.configuration);
    }

    pub fn process_worker_lost(
        &mut self,
        _state_ref: &StateRef,
        senders: &Senders,
        msg: LostWorkerMessage,
    ) {
        log::debug!("Worker lost id={}", msg.worker_id);
        for task_id in msg.running_tasks {
            let job = self.get_job_mut_by_tako_task_id(task_id).unwrap();
            job.set_waiting_state(task_id);
        }

        let worker = self.workers.get_mut(&msg.worker_id).unwrap();
        worker.set_offline_state(msg.reason.clone());

        senders.autoalloc.on_worker_lost(
            msg.worker_id,
            &worker.configuration,
            LostWorkerDetails {
                reason: msg.reason.clone(),
                lifetime: (Utc::now() - worker.started_at()).to_std().unwrap(),
            },
        );
        senders.events.on_worker_lost(msg.worker_id, msg.reason);
    }

    pub(crate) fn restore_state(&mut self, restorer: &StateRestorer) {
        self.job_id_counter = restorer.job_id_counter()
    }
}

impl StateRef {
    pub fn new(server_info: ServerInfo) -> StateRef {
        Self(WrappedRcRefCell::wrap(State {
            jobs: Default::default(),
            workers: Default::default(),
            base_task_id_to_job_id: Default::default(),
            job_id_counter: 1,
            task_id_counter: 1,
            server_info,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;
    use tako::program::{ProgramDefinition, StdioDef};

    use crate::common::arraydef::IntArray;
    use crate::server::job::Job;
    use crate::server::state::State;
    use crate::tests::utils::create_hq_state;
    use crate::transfer::messages::{
        JobDescription, JobTaskDescription, PinMode, TaskDescription, TaskKind, TaskKindProgram,
    };
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
        let task_desc = JobTaskDescription::Array {
            ids,
            entries: None,
            task_desc: TaskDescription {
                kind: TaskKind::ExternalProgram(TaskKindProgram {
                    program: dummy_program_definition(),
                    pin_mode: PinMode::None,
                    task_dir: false,
                }),
                resources: Default::default(),
                time_limit: None,
                priority: 0,
                crash_limit: 5,
            },
        };
        Job::new(
            Arc::new(JobDescription {
                task_desc,
                name: "".to_string(),
                max_fails: None,
                submit_dir: Default::default(),
                log: None,
            }),
            job_id.into(),
            base_task_id.into(),
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
