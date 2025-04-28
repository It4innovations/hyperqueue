use crate::common::error::HqError;
use crate::server::autoalloc::QueueId;
use crate::server::client::{submit_job_desc, validate_submit};
use crate::server::event::journal::JournalReader;
use crate::server::event::payload::EventPayload;
use crate::server::job::{Job, JobTaskState, StartedTaskData, SubmittedJobDescription};
use crate::server::state::State;
use crate::transfer::messages::{AllocationQueueParams, JobDescription, SubmitRequest};
use crate::worker::start::RunningTaskContext;
use crate::{make_tako_id, unwrap_tako_id};
use std::path::Path;
use tako::gateway::NewTasksMessage;
use tako::{InstanceId, ItemId, JobId, JobTaskId, Map, WorkerId};

struct RestorerTaskInfo {
    state: JobTaskState,
    instance_id: Option<InstanceId>,
    crash_counter: u32,
}

impl RestorerTaskInfo {
    fn is_completed(&self) -> bool {
        match self.state {
            JobTaskState::Waiting | JobTaskState::Running { .. } => false,
            JobTaskState::Finished { .. }
            | JobTaskState::Failed { .. }
            | JobTaskState::Canceled { .. } => true,
        }
    }
}

struct RestorerJob {
    job_desc: JobDescription,
    submit_descs: Vec<SubmittedJobDescription>,
    tasks: Map<JobTaskId, RestorerTaskInfo>,
    is_open: bool,
}

pub struct Queue {
    pub queue_id: QueueId,
    pub params: Box<AllocationQueueParams>,
}

fn is_task_completed(tasks: &Map<JobTaskId, RestorerTaskInfo>, task_id: JobTaskId) -> bool {
    tasks.get(&task_id).is_some_and(|tt| tt.is_completed())
}

impl RestorerJob {
    pub fn restore_job(
        mut self,
        job_id: JobId,
        state: &mut State,
    ) -> crate::Result<Vec<NewTasksMessage>> {
        log::debug!("Restoring job {}", job_id);
        let job = Job::new(job_id, self.job_desc, self.is_open);
        state.add_job(job);
        let mut result: Vec<NewTasksMessage> = Vec::new();
        for submit in self.submit_descs {
            if let Some(e) = validate_submit(state.get_job(job_id), &submit.description().task_desc)
            {
                return Err(HqError::GenericError(format!(
                    "Job validation failed {e:?}"
                )));
            }
            let mut new_tasks = submit_job_desc(
                state,
                job_id,
                submit.description().clone(),
                submit.submitted_at(),
            );
            let job = state.get_job_mut(job_id).unwrap();

            new_tasks.tasks.retain_mut(|t| {
                let (_, task_id) = unwrap_tako_id(t.id);
                t.task_deps
                    .retain(|d| !is_task_completed(&self.tasks, unwrap_tako_id(*d).1));
                !is_task_completed(&self.tasks, task_id)
            });

            for (task_id, job_task) in job.tasks.iter_mut() {
                if let Some(task) = self.tasks.get_mut(task_id) {
                    if task.crash_counter > 0 || task.instance_id.is_some() {
                        new_tasks.adjust_instance_id_and_crash_counters.insert(
                            make_tako_id(job_id, *task_id),
                            (
                                task.instance_id.map(|x| x.as_num() + 1).unwrap_or(0).into(),
                                task.crash_counter,
                            ),
                        );
                    }
                    match &task.state {
                        JobTaskState::Waiting | JobTaskState::Running { .. } => continue,
                        JobTaskState::Finished { .. } => job.counters.n_finished_tasks += 1,
                        JobTaskState::Failed { .. } => job.counters.n_failed_tasks += 1,
                        JobTaskState::Canceled { .. } => job.counters.n_canceled_tasks += 1,
                    }
                    job_task.state = task.state.clone();
                }
            }
            if !new_tasks.tasks.is_empty() {
                result.push(new_tasks);
            }
        }
        Ok(result)
    }

    pub fn new(job_desc: JobDescription, is_open: bool) -> Self {
        RestorerJob {
            job_desc,
            submit_descs: Vec::new(),
            tasks: Map::new(),
            is_open,
        }
    }

    pub fn add_submit(&mut self, submit: SubmittedJobDescription) {
        self.submit_descs.push(submit)
    }

    pub fn increase_crash_counters(&mut self, worker_id: WorkerId) {
        for task in self.tasks.values_mut() {
            match &task.state {
                JobTaskState::Running { started_data }
                    if started_data.worker_ids.contains(&worker_id) =>
                {
                    task.crash_counter += 1;
                }
                _ => {}
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct StateRestorer {
    jobs: Map<JobId, RestorerJob>,
    max_job_id: <JobId as ItemId>::IdType,
    max_worker_id: <WorkerId as ItemId>::IdType,
    truncate_size: Option<u64>,
    queues: Map<QueueId, Box<AllocationQueueParams>>,
    max_queue_id: QueueId,
    server_uid: String,
}

impl StateRestorer {
    pub fn job_id_counter(&self) -> <JobId as ItemId>::IdType {
        self.max_job_id + 1
    }
    pub fn worker_id_counter(&self) -> WorkerId {
        (self.max_worker_id + 1).into()
    }
    pub fn queue_id_counter(&self) -> QueueId {
        self.max_queue_id + 1
    }
    pub fn truncate_size(&self) -> Option<u64> {
        self.truncate_size
    }

    pub fn take_server_uid(&mut self) -> String {
        std::mem::take(&mut self.server_uid)
    }

    pub fn restore_jobs_and_queues(
        self,
        state: &mut State,
    ) -> crate::Result<(Vec<NewTasksMessage>, Vec<Queue>)> {
        let mut jobs = Vec::new();
        for (job_id, job) in self.jobs {
            let mut new_jobs = job.restore_job(job_id, state)?;
            jobs.append(&mut new_jobs);
        }
        let queues = self
            .queues
            .into_iter()
            .map(|(queue_id, params)| Queue { queue_id, params })
            .collect();
        Ok((jobs, queues))
    }

    fn add_job(&mut self, job_id: JobId, job: RestorerJob) {
        self.jobs.insert(job_id, job);
        self.max_job_id = self.max_job_id.max(job_id.as_num());
    }

    fn get_job_mut(&mut self, job_id: JobId) -> Option<&mut RestorerJob> {
        self.jobs.get_mut(&job_id)
    }

    pub fn load_event_file(&mut self, path: &Path) -> crate::Result<()> {
        log::debug!("Loading event file {}", path.display());
        let mut event_reader = JournalReader::open(path)?;
        for event in &mut event_reader {
            let event = event.map_err(|error| {
                crate::Error::DeserializationError(format!(
                    "Journal load error: {error:?}.\nIt appears that the journal file is corrupted."
                ))
            })?;
            match event.payload {
                EventPayload::WorkerConnected(worker_id, _) => {
                    log::debug!("Replaying: WorkerConnected {worker_id}");
                    self.max_worker_id = self.max_worker_id.max(worker_id.as_num());
                }
                EventPayload::WorkerLost(worker_id, reason) => {
                    if reason.is_failure() {
                        for job in self.jobs.values_mut() {
                            job.increase_crash_counters(worker_id);
                        }
                    }
                }
                EventPayload::WorkerOverviewReceived(_) => {}
                EventPayload::Submit {
                    job_id,
                    closed_job,
                    serialized_desc,
                } => {
                    log::debug!("Replaying: JobTasksCreated {job_id}");
                    let submit_request: SubmitRequest = serialized_desc.deserialize()?;
                    if closed_job {
                        let mut job = RestorerJob::new(submit_request.job_desc, false);
                        job.add_submit(SubmittedJobDescription::at(
                            event.time,
                            submit_request.submit_desc,
                        ));
                        self.add_job(job_id, job);
                    } else if let Some(job) = self.get_job_mut(job_id) {
                        job.add_submit(SubmittedJobDescription::at(
                            event.time,
                            submit_request.submit_desc,
                        ));
                    } else {
                        log::warn!("Ignoring submit attachment to an non-existing job")
                    }
                }
                EventPayload::JobCompleted(job_id) => {
                    log::debug!("Replaying: JobCompleted {job_id}");
                    self.jobs.remove(&job_id);
                }
                EventPayload::TaskStarted {
                    job_id,
                    task_id,
                    instance_id,
                    workers,
                } => {
                    log::debug!(
                        "Replaying: TaskStarted {job_id} {task_id} {instance_id} {workers:?}"
                    );
                    if let Some(job) = self.jobs.get_mut(&job_id) {
                        job.tasks.insert(
                            task_id,
                            RestorerTaskInfo {
                                state: JobTaskState::Running {
                                    started_data: StartedTaskData {
                                        start_date: event.time,
                                        context: RunningTaskContext { instance_id },
                                        worker_ids: workers,
                                    },
                                },
                                instance_id: Some(instance_id),
                                crash_counter: 0,
                            },
                        );
                    }
                }
                EventPayload::TaskFinished { job_id, task_id } => {
                    log::debug!("Replaying: TaskFinished {job_id} {task_id}");
                    if let Some(job) = self.jobs.get_mut(&job_id) {
                        let task = job.tasks.get_mut(&task_id).unwrap();
                        task.state = match std::mem::replace(&mut task.state, JobTaskState::Waiting)
                        {
                            JobTaskState::Running { started_data } => JobTaskState::Finished {
                                started_data,
                                end_date: event.time,
                            },
                            _ => panic!("Invalid task state"),
                        }
                    }
                }
                EventPayload::TaskFailed {
                    job_id,
                    task_id,
                    error,
                } => {
                    log::debug!("Replaying: TaskFailed {job_id} {task_id}");
                    if let Some(job) = self.jobs.get_mut(&job_id) {
                        let task = job.tasks.get_mut(&task_id).unwrap();
                        task.state = match std::mem::replace(&mut task.state, JobTaskState::Waiting)
                        {
                            JobTaskState::Waiting => JobTaskState::Failed {
                                started_data: None,
                                end_date: event.time,
                                error,
                            },
                            JobTaskState::Running { started_data } => JobTaskState::Failed {
                                started_data: Some(started_data),
                                end_date: event.time,
                                error,
                            },
                            _ => panic!("Invalid task state"),
                        }
                    }
                }
                EventPayload::TaskCanceled { job_id, task_id } => {
                    log::debug!("Replaying: TaskCanceled {job_id} {task_id}");
                    if let Some(job) = self.jobs.get_mut(&job_id) {
                        let task = job.tasks.get_mut(&task_id);
                        if let Some(task) = task {
                            task.state =
                                match std::mem::replace(&mut task.state, JobTaskState::Waiting) {
                                    JobTaskState::Running { started_data } => {
                                        JobTaskState::Canceled {
                                            started_data: Some(started_data),
                                            cancelled_date: event.time,
                                        }
                                    }
                                    _ => JobTaskState::Canceled {
                                        started_data: None,
                                        cancelled_date: event.time,
                                    },
                                }
                        } else {
                            job.tasks.insert(
                                task_id,
                                RestorerTaskInfo {
                                    state: JobTaskState::Canceled {
                                        started_data: None,
                                        cancelled_date: event.time,
                                    },
                                    instance_id: None,
                                    crash_counter: 0,
                                },
                            );
                        }
                    }
                }
                EventPayload::AllocationQueueCreated(queue_id, params) => {
                    assert!(self.queues.insert(queue_id, params).is_none());
                    self.max_queue_id = self.max_queue_id.max(queue_id);
                }
                EventPayload::AllocationQueueRemoved(queue_id) => {
                    self.queues.remove(&queue_id);
                }
                EventPayload::AllocationQueued { .. } => {}
                EventPayload::AllocationStarted(_, _) => {}
                EventPayload::AllocationFinished(_, _) => {}
                EventPayload::ServerStart { server_uid } => self.server_uid = server_uid,
                EventPayload::ServerStop => { /* Do nothing */ }
                EventPayload::JobOpen(job_id, job_description) => {
                    let job = RestorerJob::new(job_description, true);
                    self.add_job(job_id, job);
                }
                EventPayload::JobClose(job_id) => {
                    self.jobs.get_mut(&job_id).unwrap().is_open = false;
                }
            }
        }
        if event_reader.contains_partial_data() {
            self.truncate_size = Some(event_reader.position())
        }
        Ok(())
    }
}
