use serde::{Deserialize, Serialize};

use crate::client::status::get_task_status;
use crate::server::Senders;
use crate::stream::server::control::StreamServerControlMessage;
use crate::transfer::messages::{
    JobDescription, JobDetail, JobInfo, JobSubmitDescription, TaskIdSelector, TaskSelector,
    TaskStatusSelector,
};
use crate::worker::start::RunningTaskContext;
use crate::{JobId, JobTaskCount, JobTaskId, Map, TakoTaskId, WorkerId};
use chrono::{DateTime, Utc};
use smallvec::SmallVec;
use std::sync::Arc;
use tako::comm::deserialize;
use tako::task::SerializedTaskContext;
use tako::Set;
use tokio::sync::oneshot;

/// State of a task that has been started at least once.
/// It contains the last known state of the task.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StartedTaskData {
    pub start_date: DateTime<Utc>,
    pub context: RunningTaskContext,
    pub worker_ids: SmallVec<[WorkerId; 1]>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobTaskState {
    Waiting,
    Running {
        started_data: StartedTaskData,
    },
    Finished {
        started_data: StartedTaskData,
        end_date: DateTime<Utc>,
    },
    Failed {
        started_data: Option<StartedTaskData>,
        end_date: DateTime<Utc>,
        error: String,
    },
    Canceled {
        started_data: Option<StartedTaskData>,
        cancelled_date: DateTime<Utc>,
    },
}

impl JobTaskState {
    pub fn started_data(&self) -> Option<&StartedTaskData> {
        match self {
            JobTaskState::Running { started_data, .. }
            | JobTaskState::Finished { started_data, .. } => Some(started_data),
            JobTaskState::Failed { started_data, .. }
            | JobTaskState::Canceled { started_data, .. } => started_data.as_ref(),
            _ => None,
        }
    }

    pub fn get_workers(&self) -> Option<&[WorkerId]> {
        self.started_data().map(|data| data.worker_ids.as_slice())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobTaskInfo {
    pub state: JobTaskState,
    pub task_id: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default)]
pub struct JobTaskCounters {
    pub n_running_tasks: JobTaskCount,
    pub n_finished_tasks: JobTaskCount,
    pub n_failed_tasks: JobTaskCount,
    pub n_canceled_tasks: JobTaskCount,
}

impl std::ops::Add<JobTaskCounters> for JobTaskCounters {
    type Output = JobTaskCounters;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            n_running_tasks: self.n_running_tasks + rhs.n_running_tasks,
            n_finished_tasks: self.n_finished_tasks + rhs.n_finished_tasks,
            n_failed_tasks: self.n_failed_tasks + rhs.n_failed_tasks,
            n_canceled_tasks: self.n_canceled_tasks + rhs.n_canceled_tasks,
        }
    }
}

impl JobTaskCounters {
    pub fn n_waiting_tasks(&self, n_tasks: JobTaskCount) -> JobTaskCount {
        n_tasks
            - self.n_running_tasks
            - self.n_finished_tasks
            - self.n_failed_tasks
            - self.n_canceled_tasks
    }

    pub fn has_unsuccessful_tasks(&self) -> bool {
        self.n_failed_tasks > 0 || self.n_canceled_tasks > 0
    }

    pub fn is_terminated(&self, n_tasks: JobTaskCount) -> bool {
        self.n_running_tasks == 0 && self.n_waiting_tasks(n_tasks) == 0
    }
}

pub struct JobCompletionCallback {
    callback: oneshot::Sender<JobId>,
    wait_for_close: bool,
}

pub struct Job {
    pub job_id: JobId,
    pub counters: JobTaskCounters,
    pub tasks: Map<TakoTaskId, JobTaskInfo>,

    pub job_desc: JobDescription,
    pub submit_descs: SmallVec<[(Arc<JobSubmitDescription>, TakoTaskId); 1]>,

    // If true, new tasks may be submitted into this job
    // If true and all tasks in the job are terminated then the job
    // is in state OPEN not FINISHED.
    is_open: bool,

    pub submission_date: DateTime<Utc>,
    pub completion_date: Option<DateTime<Utc>>,

    /// Holds channels that will receive information about the job after the it finishes in any way.
    /// You can subscribe to the completion message with [`Self::subscribe_to_completion`].
    completion_callbacks: Vec<JobCompletionCallback>,
}

impl Job {
    pub fn new(job_id: JobId, job_desc: JobDescription, is_open: bool) -> Self {
        Job {
            job_id,
            counters: Default::default(),
            tasks: Default::default(),
            job_desc,
            submit_descs: Default::default(),
            is_open,
            submission_date: Utc::now(),
            completion_date: None,
            completion_callbacks: Default::default(),
        }
    }

    #[inline]
    pub fn is_open(&self) -> bool {
        self.is_open
    }

    pub fn close(&mut self) {
        self.is_open = false;
    }

    pub fn make_task_id_set(&self) -> Set<JobTaskId> {
        self.tasks.values().map(|t| t.task_id).collect()
    }

    pub fn max_id(&self) -> Option<JobTaskId> {
        self.tasks.values().map(|t| t.task_id).max()
    }

    pub fn make_job_detail(&self, task_selector: Option<&TaskSelector>) -> JobDetail {
        let mut tasks: Vec<JobTaskInfo> = Vec::new();
        let mut tasks_not_found: Vec<JobTaskId> = vec![];

        if let Some(selector) = task_selector {
            filter_tasks(self.tasks.values(), selector, &mut tasks);

            if let TaskIdSelector::Specific(requested_ids) = &selector.id_selector {
                let task_ids: Set<_> = self
                    .tasks
                    .values()
                    .map(|task| task.task_id.as_num())
                    .collect();
                tasks_not_found.extend(
                    requested_ids
                        .iter()
                        .filter(|id| !task_ids.contains(id))
                        .map(JobTaskId::new),
                );
            }
        }

        tasks.sort_unstable_by_key(|task| task.task_id);

        JobDetail {
            info: self.make_job_info(),
            job_desc: self.job_desc.clone(),
            submit_descs: self.submit_descs.iter().map(|x| x.0.clone()).collect(),
            tasks,
            tasks_not_found,
            submission_date: self.submission_date,
            completion_date_or_now: self.completion_date.unwrap_or_else(Utc::now),
        }
    }

    pub fn make_job_info(&self) -> JobInfo {
        JobInfo {
            id: self.job_id,
            name: self.job_desc.name.clone(),
            n_tasks: self.n_tasks(),
            counters: self.counters,
            is_open: self.is_open,
        }
    }

    #[inline]
    pub fn n_tasks(&self) -> JobTaskCount {
        self.tasks.len() as JobTaskCount
    }

    pub fn has_no_active_tasks(&self) -> bool {
        self.counters.is_terminated(self.n_tasks())
    }

    pub fn is_terminated(&self) -> bool {
        !self.is_open && self.counters.is_terminated(self.n_tasks())
    }

    pub fn get_task_state_mut(
        &mut self,
        tako_task_id: TakoTaskId,
    ) -> (JobTaskId, &mut JobTaskState) {
        let state = self.tasks.get_mut(&tako_task_id).unwrap();
        (state.task_id, &mut state.state)
    }

    pub fn iter_task_states(
        &self,
    ) -> impl Iterator<Item = (TakoTaskId, JobTaskId, &JobTaskState)> + '_ {
        self.tasks.iter().map(|(k, v)| (*k, v.task_id, &v.state))
    }

    pub fn non_finished_task_ids(&self) -> Vec<TakoTaskId> {
        let mut result = Vec::new();
        for (tako_id, _task_id, state) in self.iter_task_states() {
            match state {
                JobTaskState::Waiting | JobTaskState::Running { .. } => result.push(tako_id),
                JobTaskState::Finished { .. }
                | JobTaskState::Failed { .. }
                | JobTaskState::Canceled { .. } => { /* Do nothing */ }
            }
        }
        result
    }

    pub fn set_running_state(
        &mut self,
        tako_task_id: TakoTaskId,
        workers: SmallVec<[WorkerId; 1]>,
        context: SerializedTaskContext,
    ) -> JobTaskId {
        let (task_id, state) = self.get_task_state_mut(tako_task_id);

        let context: RunningTaskContext =
            deserialize(&context).expect("Could not deserialize task context");

        if matches!(state, JobTaskState::Waiting) {
            *state = JobTaskState::Running {
                started_data: StartedTaskData {
                    start_date: Utc::now(),
                    context,
                    worker_ids: workers,
                },
            };
            self.counters.n_running_tasks += 1;
        }

        task_id
    }

    pub fn check_termination(&mut self, senders: &Senders, now: DateTime<Utc>) {
        if self.has_no_active_tasks() {
            if self.is_open {
                let callbacks = std::mem::take(&mut self.completion_callbacks);
                self.completion_callbacks = callbacks
                    .into_iter()
                    .filter_map(|c| {
                        if !c.wait_for_close {
                            c.callback.send(self.job_id).ok();
                            None
                        } else {
                            Some(c)
                        }
                    })
                    .collect();
            } else {
                self.completion_date = Some(now);
                if self
                    .submit_descs
                    .first()
                    .map(|x| x.0.log.is_some())
                    .unwrap_or(false)
                {
                    senders.backend.send_stream_control(
                        StreamServerControlMessage::UnregisterStream(self.job_id),
                    );
                }

                for handler in self.completion_callbacks.drain(..) {
                    handler.callback.send(self.job_id).ok();
                }
                senders.events.on_job_completed(self.job_id);
            }
        }
    }

    pub fn set_finished_state(
        &mut self,
        tako_task_id: TakoTaskId,
        now: DateTime<Utc>,
        senders: &Senders,
    ) -> JobTaskId {
        let (task_id, state) = self.get_task_state_mut(tako_task_id);
        match state {
            JobTaskState::Running { started_data } => {
                *state = JobTaskState::Finished {
                    started_data: started_data.clone(),
                    end_date: now,
                };
                self.counters.n_running_tasks -= 1;
                self.counters.n_finished_tasks += 1;
            }
            _ => panic!("Invalid worker state, expected Running, got {state:?}"),
        }
        senders.events.on_task_finished(self.job_id, task_id);
        self.check_termination(senders, now);
        task_id
    }

    pub fn set_waiting_state(&mut self, tako_task_id: TakoTaskId) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        assert!(matches!(state, JobTaskState::Running { .. }));
        *state = JobTaskState::Waiting;
        self.counters.n_running_tasks -= 1;
    }

    pub fn set_failed_state(
        &mut self,
        tako_task_id: TakoTaskId,
        error: String,
        senders: &Senders,
    ) -> JobTaskId {
        let (task_id, state) = self.get_task_state_mut(tako_task_id);
        let now = Utc::now();
        match state {
            JobTaskState::Running { started_data } => {
                *state = JobTaskState::Failed {
                    error: error.clone(),
                    started_data: Some(started_data.clone()),
                    end_date: now,
                };

                self.counters.n_running_tasks -= 1;
            }
            JobTaskState::Waiting => {
                *state = JobTaskState::Failed {
                    error: error.clone(),
                    started_data: None,
                    end_date: now,
                }
            }
            _ => panic!("Invalid task {task_id} state, expected Running or Waiting, got {state:?}"),
        }
        self.counters.n_failed_tasks += 1;

        senders.events.on_task_failed(self.job_id, task_id, error);
        self.check_termination(senders, now);
        task_id
    }

    pub fn set_cancel_state(&mut self, tako_task_id: TakoTaskId, senders: &Senders) -> JobTaskId {
        let now = Utc::now();

        let (task_id, state) = self.get_task_state_mut(tako_task_id);
        match state {
            JobTaskState::Running { started_data, .. } => {
                *state = JobTaskState::Canceled {
                    started_data: Some(started_data.clone()),
                    cancelled_date: now,
                };
                self.counters.n_running_tasks -= 1;
            }
            JobTaskState::Waiting => {
                *state = JobTaskState::Canceled {
                    started_data: None,
                    cancelled_date: now,
                };
            }
            state => panic!("Invalid job state that is being canceled: {task_id:?} {state:?}"),
        }

        senders.events.on_task_canceled(self.job_id, task_id);
        self.counters.n_canceled_tasks += 1;
        self.check_termination(senders, now);
        task_id
    }

    /// Subscribes to the completion event of this job.
    /// When the job finishes in any way (completion, failure, cancellation), the channel will
    /// receive a single message.
    pub fn subscribe_to_completion(&mut self, wait_for_close: bool) -> oneshot::Receiver<JobId> {
        let (tx, rx) = oneshot::channel();
        self.completion_callbacks.push(JobCompletionCallback {
            callback: tx,
            wait_for_close,
        });
        rx
    }
}

/// Applies ID and status filters from `selector` to filter `tasks`.
fn filter_tasks<'a, T: Iterator<Item = &'a JobTaskInfo>>(
    tasks: T,
    selector: &TaskSelector,
    result: &mut Vec<JobTaskInfo>,
) {
    for task in tasks {
        match &selector.id_selector {
            TaskIdSelector::Specific(ids) => {
                if !ids.contains(task.task_id.as_num()) {
                    continue;
                }
            }
            TaskIdSelector::All => {}
        }

        match &selector.status_selector {
            TaskStatusSelector::Specific(states) => {
                if !states.contains(&get_task_status(&task.state)) {
                    continue;
                }
            }
            TaskStatusSelector::All => {}
        }

        result.push(task.clone());
    }
}
