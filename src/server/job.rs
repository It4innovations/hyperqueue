use serde::{Deserialize, Serialize};
use tako::messages::common::ProgramDefinition;

use crate::transfer::messages::{JobDetail, JobInfo, JobType};
use crate::{JobId, JobTaskCount, JobTaskId, Map, TakoTaskId};
use tako::common::resources::ResourceRequest;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JobTaskState {
    Waiting,
    Running,
    Finished,
    Failed(String),
    Canceled,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JobTaskInfo {
    pub state: JobTaskState,
    pub task_id: JobTaskId,
}

pub enum JobState {
    SingleTask(JobTaskState),
    ManyTasks(Map<TakoTaskId, JobTaskInfo>),
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default)]
pub struct JobTaskCounters {
    pub n_running_tasks: JobTaskCount,
    pub n_finished_tasks: JobTaskCount,
    pub n_failed_tasks: JobTaskCount,
    pub n_canceled_tasks: JobTaskCount,
}

impl JobTaskCounters {
    pub fn n_waiting_tasks(&self, n_tasks: JobTaskCount) -> JobTaskCount {
        n_tasks
            - self.n_running_tasks
            - self.n_finished_tasks
            - self.n_failed_tasks
            - self.n_canceled_tasks
    }
}

pub struct Job {
    pub job_id: JobId,
    pub base_task_id: TakoTaskId,
    pub max_fails: Option<TakoTaskId>,
    pub counters: JobTaskCounters,

    pub state: JobState,

    pub job_type: JobType,
    pub name: String,

    pub program_def: ProgramDefinition,
    pub resources: ResourceRequest,
    pub pin: bool,
}

impl Job {
    pub fn new(
        job_type: JobType,
        job_id: JobId,
        base_task_id: TakoTaskId,
        name: String,
        program_def: ProgramDefinition,
        resources: ResourceRequest,
        pin: bool,
        max_fails: Option<TakoTaskId>,
    ) -> Self {
        let state = match &job_type {
            JobType::Simple => JobState::SingleTask(JobTaskState::Waiting),
            JobType::Array(m) if m.task_count() == 1 => JobState::SingleTask(JobTaskState::Waiting),
            JobType::Array(m) => JobState::ManyTasks(
                m.iter()
                    .enumerate()
                    .map(|(i, task_id)| {
                        (
                            base_task_id + i as TakoTaskId,
                            JobTaskInfo {
                                state: JobTaskState::Waiting,
                                task_id,
                            },
                        )
                    })
                    .collect(),
            ),
        };

        Job {
            job_type,
            job_id,
            counters: Default::default(),
            base_task_id,
            name,
            state,
            program_def,
            resources,
            pin,
            max_fails,
        }
    }

    pub fn make_job_detail(&self, include_tasks: bool) -> JobDetail {
        JobDetail {
            info: self.make_job_info(),
            job_type: self.job_type.clone(),
            program_def: self.program_def.clone(),
            resources: self.resources.clone(),
            tasks: if include_tasks {
                match &self.state {
                    JobState::SingleTask(s) => {
                        vec![JobTaskInfo {
                            task_id: 0,
                            state: s.clone(),
                        }]
                    }
                    JobState::ManyTasks(m) => m.values().cloned().collect(),
                }
            } else {
                Vec::new()
            },
            pin: self.pin,
        }
    }

    pub fn make_job_info(&self) -> JobInfo {
        /*let error = match &self.state {
            JobState::Waiting => (JobStatus::Waiting, None),
            JobState::Finished => (JobStatus::Finished, None),
            JobState::Failed(e) => (JobStatus::Failed, Some(e.clone())),
            JobState::Running => (JobStatus::Running, None),
            JobState::Canceled => (JobStatus::Canceled, None),
        };*/

        JobInfo {
            id: self.job_id,
            name: self.name.clone(),
            n_tasks: self.n_tasks(),
            counters: self.counters,
            resources: self.resources.clone(),
        }
    }

    #[inline]
    pub fn n_tasks(&self) -> JobTaskCount {
        match &self.state {
            JobState::SingleTask(_) => 1,
            JobState::ManyTasks(s) => s.len() as JobTaskCount,
        }
    }

    pub fn get_task_state_mut(
        &mut self,
        tako_task_id: TakoTaskId,
    ) -> (JobTaskId, &mut JobTaskState) {
        match &mut self.state {
            JobState::SingleTask(ref mut s) => {
                debug_assert_eq!(tako_task_id, self.base_task_id);
                (0, s)
            }
            JobState::ManyTasks(m) => {
                let state = m.get_mut(&tako_task_id).unwrap();
                (state.task_id, &mut state.state)
            }
        }
    }

    pub fn iter_task_states<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = (TakoTaskId, JobTaskId, &'a JobTaskState)> + 'a> {
        match self.state {
            JobState::SingleTask(ref s) => Box::new(Some((self.base_task_id, 0, s)).into_iter()),
            JobState::ManyTasks(ref m) => {
                Box::new(m.iter().map(|(k, v)| (*k, v.task_id, &v.state)))
            }
        }
    }

    /*pub fn iter_task_states_mut(&mut self) -> Box<dyn Iterator<Item=(TakoTaskId, &mut JobTaskState)>>
    {
        match &mut self.state {
            JobState::SingleTask(ref mut s) => {
                Box::new(Some((0, s)))
            }
            JobState::ManyTasks(ref mut m) => {
                Box::new(m.iter_mut().map(|(k, v)| (k, &mut v.state)))
            }
        }
    }*/

    pub fn set_running_state(&mut self, tako_task_id: TakoTaskId) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        if matches!(state, JobTaskState::Waiting) {
            *state = JobTaskState::Running;
            self.counters.n_running_tasks += 1;
        }
    }

    pub fn set_finished_state(&mut self, tako_task_id: TakoTaskId) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        assert!(matches!(state, JobTaskState::Running));
        *state = JobTaskState::Finished;
        self.counters.n_running_tasks -= 1;
        self.counters.n_finished_tasks += 1;
    }

    pub fn set_waiting_state(&mut self, tako_task_id: TakoTaskId) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        assert!(matches!(state, JobTaskState::Running));
        *state = JobTaskState::Waiting;
        self.counters.n_running_tasks -= 1;
    }

    pub fn set_failed_state(&mut self, tako_task_id: TakoTaskId, error: String) {
        let (_, state) = self.get_task_state_mut(tako_task_id);
        assert!(matches!(state, JobTaskState::Running));
        *state = JobTaskState::Failed(error);
        self.counters.n_running_tasks -= 1;
        self.counters.n_failed_tasks += 1;
    }

    pub fn set_cancel_state(&mut self, tako_task_id: TakoTaskId) -> JobTaskId {
        let (task_id, state) = self.get_task_state_mut(tako_task_id);
        let old_state = std::mem::replace(state, JobTaskState::Canceled);
        assert!(matches!(
            old_state,
            JobTaskState::Running | JobTaskState::Waiting
        ));
        if let JobTaskState::Running = old_state {
            self.counters.n_running_tasks -= 1;
        }
        task_id
        //assert!(matches!(
        //    old_state,
        //    JobTaskState::Running | JobTaskState::Waiting
        //));
    }
}
