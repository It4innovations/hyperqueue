use crate::server::client::submit_job_desc;
use crate::server::event::bincode_config;
use crate::server::event::log::EventLogReader;
use crate::server::event::payload::EventPayload;
use crate::server::job::{Job, JobTaskState, StartedTaskData};
use crate::server::state::{State, StateRef};
use crate::transfer::messages::JobDescription;
use crate::worker::start::RunningTaskContext;
use crate::{JobId, JobTaskId, Map};
use bincode::Options;
use chrono::Utc;
use std::path::Path;
use std::rc::Rc;
use tako::gateway::{FromGatewayMessage, NewTasksMessage, ToGatewayMessage};
use tako::{ItemId, WorkerId};

struct RestorerTaskInfo {
    state: JobTaskState,
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
    tasks: Map<JobTaskId, RestorerTaskInfo>,
}

impl RestorerJob {
    pub fn restore_job(
        mut self,
        job_id: JobId,
        state: &mut State,
    ) -> crate::Result<NewTasksMessage> {
        log::debug!("Restoring job {}", job_id);
        let mut new_tasks = submit_job_desc(state, job_id, self.job_desc)?;
        let job = state.get_job_mut(job_id).unwrap();

        new_tasks.tasks = new_tasks
            .tasks
            .into_iter()
            .filter(|t| {
                self.tasks
                    .get(&job.get_task_state_mut(t.id).0)
                    .map(|tt| !tt.is_completed())
                    .unwrap_or(true)
            })
            .collect();

        for (tako_id, job_task) in job.tasks.iter_mut() {
            if let Some(task) = self.tasks.get_mut(&job_task.task_id) {
                match &task.state {
                    JobTaskState::Waiting => continue,
                    JobTaskState::Running { started_data } => {
                        let instance_id = started_data.context.instance_id.as_num() + 1;
                        new_tasks
                            .adjust_instance_id
                            .insert(*tako_id, instance_id.into());
                        continue;
                    }
                    JobTaskState::Finished { .. } => job.counters.n_finished_tasks += 1,
                    JobTaskState::Failed { .. } => job.counters.n_failed_tasks += 1,
                    JobTaskState::Canceled { .. } => job.counters.n_canceled_tasks += 1,
                }
                job_task.state = task.state.clone();
            }
        }
        Ok(new_tasks)
    }
}

impl RestorerJob {
    pub fn new(job_desc: JobDescription) -> Self {
        RestorerJob {
            job_desc,
            tasks: Map::new(),
        }
    }
}

#[derive(Default)]
pub(crate) struct StateRestorer {
    jobs: Map<JobId, RestorerJob>,
    max_job_id: <JobId as ItemId>::IdType,
    max_worker_id: <WorkerId as ItemId>::IdType,
}

impl StateRestorer {
    pub fn job_id_counter(&self) -> <JobId as ItemId>::IdType {
        self.max_job_id + 1
    }
    pub fn worker_id_counter(&self) -> WorkerId {
        (self.max_worker_id + 1).into()
    }

    pub fn restore_jobs(self, state: &mut State) -> crate::Result<Vec<NewTasksMessage>> {
        self.jobs
            .into_iter()
            .map(|(job_id, job)| job.restore_job(job_id, state))
            .collect::<crate::Result<Vec<NewTasksMessage>>>()
    }

    pub fn load_event_file(&mut self, path: &Path) -> crate::Result<Option<u64>> {
        log::debug!("Loading event file {}", path.display());
        let mut event_reader = EventLogReader::open(path)?;
        for event in &mut event_reader {
            let event = event?;
            match event.payload {
                EventPayload::WorkerConnected(worker_id, _) => {
                    log::debug!("Replaying: WorkerConnected {worker_id}");
                    self.max_worker_id = self.max_worker_id.max(worker_id.as_num());
                }
                EventPayload::WorkerLost(_, _) => {}
                EventPayload::WorkerOverviewReceived(_) => {}
                //EventPayload::JobCreatedShort(job_id, job_info) => {}
                EventPayload::JobCreatedFull(job_id, serialized_job_desc) => {
                    log::debug!("Replaying: JobCreated {job_id}");
                    let job_desc = bincode_config().deserialize(&serialized_job_desc)?;
                    self.jobs.insert(job_id, RestorerJob::new(job_desc));
                    self.max_job_id = self.max_job_id.max(job_id.as_num());
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
                                },
                            );
                        }
                    }
                }
                EventPayload::AllocationQueueCreated(_, _) => {}
                EventPayload::AllocationQueueRemoved(_) => {}
                EventPayload::AllocationQueued { .. } => {}
                EventPayload::AllocationStarted(_, _) => {}
                EventPayload::AllocationFinished(_, _) => {}
                EventPayload::ServerStop => { /* Do nothing */ }
            }
        }
        Ok(if event_reader.contains_partial_data() {
            Some(event_reader.position())
        } else {
            None
        })
    }
}
