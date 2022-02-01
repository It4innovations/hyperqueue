use serde::{Deserialize, Serialize};

use crate::client::status::get_task_status;
use crate::server::rpc::Backend;
use crate::stream::server::control::StreamServerControlMessage;
use crate::transfer::messages::{
    JobDescription, JobDetail, JobInfo, TaskIdSelector, TaskSelector, TaskStatusSelector,
};
use crate::worker::start::RunningTaskContext;
use crate::{JobId, JobTaskCount, JobTaskId, Map, TakoTaskId, WorkerId};
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use tako::common::index::ItemId;
use tako::common::Set;
use tako::server::task::SerializedTaskContext;
use tako::transfer::auth::deserialize;
use tako::TaskId;
use tokio::sync::oneshot;

/// State of a task that has been started at least once.
/// It contains the last known state of the task.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StartedTaskData {
    pub start_date: DateTime<Utc>,
    pub context: RunningTaskContext,
    pub worker_id: WorkerId,
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
        started_data: StartedTaskData,
        end_date: DateTime<Utc>,
        error: String,
    },
    Canceled {
        started_data: Option<StartedTaskData>,
    },
}

impl JobTaskState {
    pub fn started_data(&self) -> Option<&StartedTaskData> {
        match self {
            JobTaskState::Running { started_data, .. }
            | JobTaskState::Finished { started_data, .. }
            | JobTaskState::Failed { started_data, .. } => Some(started_data),
            JobTaskState::Canceled { started_data } => started_data.as_ref(),
            _ => None,
        }
    }

    pub fn get_worker(&self) -> Option<WorkerId> {
        self.started_data().map(|data| data.worker_id)
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

pub struct Job {
    pub job_id: JobId,
    pub base_task_id: TakoTaskId,
    pub max_fails: Option<JobTaskCount>,
    pub counters: JobTaskCounters,

    pub tasks: Map<TakoTaskId, JobTaskInfo>,

    pub log: Option<PathBuf>,

    pub job_desc: JobDescription,
    pub name: String,

    pub submission_date: DateTime<Utc>,
    pub completion_date: Option<DateTime<Utc>>,

    submit_dir: PathBuf,

    /// Holds channels that will receive information about the job after the it finishes in any way.
    /// You can subscribe to the completion message with [`Self::subscribe_to_completion`].
    completion_callbacks: Vec<oneshot::Sender<JobId>>,
}

impl Job {
    // Probably we need some structure for the future, but as it is called in exactly one place,
    // I am disabling it now
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_desc: JobDescription,
        job_id: JobId,
        base_task_id: TakoTaskId,
        name: String,
        max_fails: Option<JobTaskCount>,
        log: Option<PathBuf>,
        submit_dir: PathBuf,
    ) -> Self {
        let base = base_task_id.as_num();
        let tasks = match &job_desc {
            JobDescription::Array { ids, .. } => ids
                .iter()
                .enumerate()
                .map(|(i, task_id)| {
                    (
                        TakoTaskId::new(base + i as <TaskId as ItemId>::IdType),
                        JobTaskInfo {
                            state: JobTaskState::Waiting,
                            task_id: task_id.into(),
                        },
                    )
                })
                .collect(),
            JobDescription::Graph { tasks } => tasks
                .iter()
                .enumerate()
                .map(|(i, task)| {
                    (
                        TakoTaskId::new(base + i as <TaskId as ItemId>::IdType),
                        JobTaskInfo {
                            state: JobTaskState::Waiting,
                            task_id: task.id,
                        },
                    )
                })
                .collect(),
        };

        Job {
            job_desc,
            job_id,
            counters: Default::default(),
            base_task_id,
            name,
            tasks,
            max_fails,
            log,
            submission_date: Utc::now(),
            completion_date: None,
            submit_dir,
            completion_callbacks: Default::default(),
        }
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
            tasks,
            tasks_not_found,
            max_fails: self.max_fails,
            submission_date: self.submission_date,
            submit_dir: self.submit_dir.clone(),
            completion_date_or_now: self.completion_date.unwrap_or_else(Utc::now),
        }
    }

    pub fn make_job_info(&self) -> JobInfo {
        JobInfo {
            id: self.job_id,
            name: self.name.clone(),
            n_tasks: self.n_tasks(),
            counters: self.counters,
        }
    }

    #[inline]
    pub fn n_tasks(&self) -> JobTaskCount {
        self.tasks.len() as JobTaskCount
    }

    pub fn is_terminated(&self) -> bool {
        self.counters.is_terminated(self.n_tasks())
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
        worker: WorkerId,
        context: SerializedTaskContext,
    ) {
        let (_, state) = self.get_task_state_mut(tako_task_id);

        let context: RunningTaskContext =
            deserialize(&context).expect("Could not deserialize task context");

        if matches!(state, JobTaskState::Waiting) {
            *state = JobTaskState::Running {
                started_data: StartedTaskData {
                    start_date: Utc::now(),
                    context,
                    worker_id: worker,
                },
            };
            self.counters.n_running_tasks += 1;
        }
    }

    pub fn check_termination(&mut self, backend: &Backend, now: DateTime<Utc>) {
        if self.is_terminated() {
            self.completion_date = Some(now);
            if self.log.is_some() {
                backend
                    .send_stream_control(StreamServerControlMessage::UnregisterStream(self.job_id));
            }

            for handler in self.completion_callbacks.drain(..) {
                handler.send(self.job_id).ok();
            }
        }
    }

    pub fn set_finished_state(&mut self, tako_task_id: TakoTaskId, backend: &Backend) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        let now = Utc::now();
        match state {
            JobTaskState::Running { started_data } => {
                *state = JobTaskState::Finished {
                    started_data: started_data.clone(),
                    end_date: now,
                };
                self.counters.n_running_tasks -= 1;
                self.counters.n_finished_tasks += 1;
            }
            _ => panic!("Invalid worker state, expected Running, got {:?}", state),
        }
        self.check_termination(backend, now);
    }

    pub fn set_waiting_state(&mut self, tako_task_id: TakoTaskId) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        assert!(matches!(state, JobTaskState::Running { .. }));
        *state = JobTaskState::Waiting;
        self.counters.n_running_tasks -= 1;
    }

    pub fn set_failed_state(&mut self, tako_task_id: TakoTaskId, error: String, backend: &Backend) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        let now = Utc::now();
        match state {
            JobTaskState::Running { started_data } => {
                *state = JobTaskState::Failed {
                    error,
                    started_data: started_data.clone(),
                    end_date: now,
                };
                self.counters.n_running_tasks -= 1;
                self.counters.n_failed_tasks += 1;
            }
            _ => panic!("Invalid worker state, expected Running, got {:?}", state),
        }
        self.check_termination(backend, now);
    }

    pub fn set_cancel_state(&mut self, tako_task_id: TakoTaskId, backend: &Backend) -> JobTaskId {
        let now = Utc::now();

        let (task_id, state) = self.get_task_state_mut(tako_task_id);
        match state {
            JobTaskState::Running { started_data, .. } => {
                *state = JobTaskState::Canceled {
                    started_data: Some(started_data.clone()),
                };
                self.counters.n_running_tasks -= 1;
            }
            JobTaskState::Waiting => {
                *state = JobTaskState::Canceled { started_data: None };
            }
            state => panic!(
                "Invalid job state that is being canceled: {:?} {:?}",
                task_id, state
            ),
        }

        self.counters.n_canceled_tasks += 1;
        self.check_termination(backend, now);
        task_id
    }

    /// Subscribes to the completion event of this job.
    /// When the job finishes in any way (completion, failure, cancellation), the channel will
    /// receive a single message.
    pub fn subscribe_to_completion(&mut self) -> oneshot::Receiver<JobId> {
        let (tx, rx) = oneshot::channel();
        self.completion_callbacks.push(tx);
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
