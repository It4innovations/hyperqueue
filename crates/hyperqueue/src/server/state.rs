use std::cmp::min;

use chrono::Utc;
use smallvec::SmallVec;
use tako::{InstanceId, define_wrapped_type};
use tako::{ItemId, TaskId};

use crate::WrappedRcRefCell;
use crate::server::Senders;
use crate::server::autoalloc::LostWorkerDetails;
use crate::server::job::Job;
use crate::server::restore::StateRestorer;
use crate::server::worker::Worker;
use crate::transfer::messages::ServerInfo;
use tako::gateway::LostWorkerReason;
use tako::internal::messages::common::TaskFailInfo;
use tako::task::SerializedTaskContext;
use tako::worker::WorkerConfiguration;
use tako::{JobId, Map, WorkerId};

pub struct State {
    jobs: Map<JobId, Job>,
    workers: Map<WorkerId, Worker>,
    job_id_counter: <JobId as ItemId>::IdType,
    server_info: ServerInfo,
}

define_wrapped_type!(StateRef, State, pub);

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
        Some(job)
    }

    /// Try to release unused memory if possible.
    /// Can be called e.g. after forgetting jobs.
    pub fn try_release_memory(&mut self) {
        self.jobs.shrink_to_fit();
        self.workers.shrink_to_fit();
    }

    pub fn new_job_id(&mut self) -> JobId {
        let id = self.job_id_counter;
        self.job_id_counter += 1;
        id.into()
    }

    pub fn revert_to_job_id(&mut self, id: JobId) {
        self.job_id_counter = id.as_num();
    }

    pub fn last_n_ids(&self, n: u32) -> impl Iterator<Item = JobId> + use<> {
        let n = min(n, self.job_id_counter - 1);
        ((self.job_id_counter - n)..self.job_id_counter).map(|id| id.into())
    }

    pub fn last_job_id(&self) -> JobId {
        JobId::new(self.job_id_counter - 1)
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
        senders: &Senders,
        task_id: TaskId,
        cancelled_tasks: Vec<TaskId>,
        info: TaskFailInfo,
    ) -> Vec<TaskId> {
        log::debug!("Task id={task_id} failed: {info:?}");

        let job_id = task_id.job_id();
        let job = self.get_job_mut(job_id).unwrap();
        if !cancelled_tasks.is_empty() {
            log::debug!(
                "Tasks {:?} canceled because of task dependency fails",
                &cancelled_tasks
            );
        }

        job.set_cancel_state(cancelled_tasks, senders);
        job.set_failed_state(task_id.job_task_id(), info.message, senders);

        if let Some(max_fails) = &job.job_desc.max_fails {
            if job.counters.n_failed_tasks > *max_fails {
                log::debug!("Max task fails reached for job {}", job.job_id);
                let task_ids = job.non_finished_task_ids();
                job.set_cancel_state(task_ids.clone(), senders);
                return task_ids;
            }
        }
        Vec::new()
    }

    pub fn process_task_started(
        &mut self,
        senders: &Senders,
        task_id: TaskId,
        instance_id: InstanceId,
        worker_ids: &[WorkerId],
        context: SerializedTaskContext,
    ) {
        let job = self.get_job_mut(task_id.job_id()).unwrap();
        let now = Utc::now();
        let worker_ids: SmallVec<_> = worker_ids.iter().copied().collect();

        job.set_running_state(task_id.job_task_id(), worker_ids.clone(), context, now);
        for worker_id in &worker_ids {
            if let Some(worker) = self.workers.get_mut(worker_id) {
                worker.update_task_started(task_id, now);
            }
        }
        senders
            .events
            .on_task_started(task_id, instance_id, worker_ids, now);
    }

    pub fn process_task_finished(&mut self, senders: &Senders, id: TaskId) {
        let now = Utc::now();
        let job = self.get_job_mut(id.job_id()).unwrap();
        job.set_finished_state(id.job_task_id(), now, senders);
    }

    /*
    pub fn process_task_update(&mut self, id: TaskId, state: TaskState, senders: &Senders) {
        log::debug!("Task id={} updated {:?}", id, state);
        match state {
            TaskState::Running {
                instance_id,
                worker_ids,
                context,
            } => {
                let job = self.get_job_mut(id.job_id()).unwrap();
                let now = Utc::now();
                job.set_running_state(id.job_task_id(), worker_ids.clone(), context, now);
                for worker_id in &worker_ids {
                    if let Some(worker) = self.workers.get_mut(worker_id) {
                        worker.update_task_started(id, now);
                    }
                }
                senders
                    .events
                    .on_task_started(id, instance_id, worker_ids.clone(), now);
            }
            TaskState::Finished => {
                let now = Utc::now();
                let job = self.get_job_mut(id.job_id()).unwrap();
                job.set_finished_state(id.job_task_id(), now, senders);
            }
            TaskState::Waiting => {
                let job = self.get_job_mut(id.job_id()).unwrap();
                job.set_waiting_state(id.job_task_id());
            }
            TaskState::Invalid => {
                unreachable!()
            }
        };
    }*/

    pub fn process_worker_new(
        &mut self,
        senders: &Senders,
        worker_id: WorkerId,
        configuration: &WorkerConfiguration,
    ) {
        log::debug!("New worker id={worker_id}");
        self.add_worker(Worker::new(worker_id, configuration.clone()));
        // TODO: use observer in event storage instead of sending these messages directly
        senders
            .autoalloc
            .on_worker_connected(worker_id, configuration);
        senders
            .events
            .on_worker_added(worker_id, configuration.clone());
    }

    pub fn process_worker_lost(
        &mut self,
        senders: &Senders,
        worker_id: WorkerId,
        running_tasks: &[TaskId],
        reason: LostWorkerReason,
    ) {
        log::debug!("Worker lost id={worker_id}");
        for tako_id in running_tasks {
            let job = self.get_job_mut(tako_id.job_id()).unwrap();
            job.set_waiting_state(tako_id.job_task_id());
        }

        let worker = self.workers.get_mut(&worker_id).unwrap();
        worker.set_offline_state(reason.clone());

        senders.autoalloc.on_worker_lost(
            worker_id,
            &worker.configuration,
            LostWorkerDetails {
                reason: reason.clone(),
                lifetime: (Utc::now() - worker.started_at()).to_std().unwrap(),
            },
        );
        senders.events.on_worker_lost(worker_id, reason);
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
            job_id_counter: 1,
            server_info,
        }))
    }
}
