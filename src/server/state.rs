use std::collections::BTreeMap;

use tako::messages::gateway::{
    CancelTasks, FromGatewayMessage, LostWorkerMessage, LostWorkerReason, NewWorkerMessage,
    TaskFailedMessage, TaskState, TaskUpdate, ToGatewayMessage,
};

use crate::common::WrappedRcRefCell;
use crate::server::job::Job;
use crate::server::rpc::TakoServer;
use crate::server::worker::Worker;
use crate::transfer::messages::LostWorkerReasonInfo;
use crate::{JobId, JobTaskCount, Map, TakoTaskId, WorkerId};
use std::cmp::min;

pub struct State {
    jobs: crate::Map<JobId, Job>,
    workers: crate::Map<WorkerId, Worker>,

    // Here we store TaskId -> JobId data, but to make it sparse
    // we store ONLY the base_task_id there, i.e. each job has here
    // only one entry.
    // Example:
    // Real mapping: TaskId   JobId
    //                 1   ->   1
    //                 2   ->    1
    //                 3   ->    2
    //                 4   ->    2
    //                 5   ->    2
    // The actual base_task_id_to_job will be 1 -> 1, 3 -> 2
    // Therefore we need to find biggest key that is lower then a given task id
    // To make this query efficient, we use BTreeMap and not Map
    base_task_id_to_job_id: BTreeMap<TakoTaskId, WorkerId>,
    job_id_counter: JobId,
    task_id_counter: TakoTaskId,
}

pub type StateRef = WrappedRcRefCell<State>;

fn cancel_tasks_from_callback(
    state_ref: &StateRef,
    tako_ref: &TakoServer,
    job_id: JobId,
    tasks: Vec<TakoTaskId>,
) {
    if tasks.is_empty() {
        return;
    }
    let tako_ref = tako_ref.clone();
    let state_ref = state_ref.clone();
    tokio::task::spawn_local(async move {
        let message = FromGatewayMessage::CancelTasks(CancelTasks { tasks });
        let response = tako_ref.send_message(message).await.unwrap();

        match response {
            ToGatewayMessage::CancelTasksResponse(msg) => {
                let mut state = state_ref.get_mut();
                let job = state.get_job_mut(job_id).unwrap();
                for tako_id in msg.cancelled_tasks {
                    job.set_cancel_state(tako_id);
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
        assert!(self.jobs.insert(job_id, job).is_none());
    }

    pub fn get_job_mut_by_tako_task_id(&mut self, task_id: TakoTaskId) -> Option<&mut Job> {
        let job_id: JobId = *self
            .base_task_id_to_job_id
            .range(..=task_id)
            .rev()
            .next()?
            .1;
        let job = self.jobs.get_mut(&job_id)?;
        if task_id < job.base_task_id + job.n_tasks() as u64 {
            Some(job)
        } else {
            None
        }
    }

    pub fn new_job_id(&mut self) -> JobId {
        let id = self.job_id_counter;
        self.job_id_counter += 1;
        id
    }

    pub fn last_n_ids(&self, n: JobId) -> impl Iterator<Item = JobId> {
        let n = min(n, self.job_id_counter - 1);
        (self.job_id_counter - n)..self.job_id_counter
    }

    pub fn new_task_id(&mut self, task_count: JobTaskCount) -> TakoTaskId {
        let id = self.task_id_counter;
        self.task_id_counter += task_count as u64;
        id
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
        tako_ref: &TakoServer,
        msg: TaskFailedMessage,
    ) {
        log::debug!("Task id={} failed", msg.id);

        let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
        job.set_failed_state(msg.id, msg.info.message);

        if let Some(max_fails) = job.max_fails {
            if job.counters.n_failed_tasks > max_fails {
                let task_ids = job.non_finished_task_ids();
                cancel_tasks_from_callback(state_ref, tako_ref, job.job_id, task_ids);
            }
        }
    }
    pub fn process_task_update(&mut self, msg: TaskUpdate) {
        log::debug!("Task id={} updated {:?}", msg.id, msg.state);
        match msg.state {
            TaskState::Running(worker_id) => {
                let hostname = self
                    .get_worker(worker_id)
                    .unwrap()
                    .configuration
                    .hostname
                    .clone();
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_running_state(msg.id, hostname)
            }
            TaskState::Finished => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_finished_state(msg.id)
            }
            TaskState::Waiting => {
                let job = self.get_job_mut_by_tako_task_id(msg.id).unwrap();
                job.set_waiting_state(msg.id)
            }
            TaskState::Invalid => {
                unreachable!()
            }
        };
    }

    pub fn process_worker_new(&mut self, msg: NewWorkerMessage) {
        log::debug!("New worker id={}", msg.worker_id);
        self.add_worker(Worker::new(msg.worker_id, msg.configuration));
    }

    pub fn process_worker_lost(&mut self, msg: LostWorkerMessage) {
        log::debug!("Worker lost id={}", msg.worker_id);
        let worker = self.workers.get_mut(&msg.worker_id).unwrap();
        worker.set_offline_state(match msg.reason {
            LostWorkerReason::Stopped => LostWorkerReasonInfo::Stopped,
            LostWorkerReason::ConnectionLost => LostWorkerReasonInfo::ConnectionLost,
            LostWorkerReason::HeartbeatLost => LostWorkerReasonInfo::HeartbeatLost,
            LostWorkerReason::IdleTimeout => LostWorkerReasonInfo::IdleTimeout,
        });
        for task_id in msg.running_tasks {
            let job = self.get_job_mut_by_tako_task_id(task_id).unwrap();
            job.set_waiting_state(task_id);
        }
    }
}

impl StateRef {
    pub fn new() -> StateRef {
        WrappedRcRefCell::wrap(State {
            jobs: Default::default(),
            workers: Default::default(),
            base_task_id_to_job_id: Default::default(),
            job_id_counter: 1,
            task_id_counter: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use tako::messages::common::ProgramDefinition;

    use crate::common::arraydef::ArrayDef;
    use crate::server::job::Job;
    use crate::server::state::StateRef;
    use crate::transfer::messages::JobType;
    use tako::common::resources::ResourceRequest;

    fn dummy_program_definition() -> ProgramDefinition {
        ProgramDefinition {
            args: vec![],
            env: Default::default(),
            stdout: None,
            stderr: None,
            cwd: None,
        }
    }

    #[test]
    fn test_find_job_id_by_task_id() {
        let state_ref = StateRef::new();
        let mut state = state_ref.get_mut();
        state.add_job(Job::new(
            JobType::Array(ArrayDef::simple_range(0, 10)),
            223,
            100,
            "".to_string(),
            dummy_program_definition(),
            ResourceRequest::default(),
            false,
            None,
            Some(Vec::new()),
            0,
        ));
        state.add_job(Job::new(
            JobType::Array(ArrayDef::simple_range(0, 15)),
            224,
            110,
            "".to_string(),
            dummy_program_definition(),
            ResourceRequest::default(),
            false,
            None,
            Some(Vec::new()),
            1,
        ));
        state.add_job(Job::new(
            JobType::Simple,
            225,
            125,
            "".to_string(),
            dummy_program_definition(),
            ResourceRequest::default(),
            false,
            None,
            Some(Vec::new()),
            5,
        ));
        state.add_job(Job::new(
            JobType::Simple,
            226,
            126,
            "".to_string(),
            dummy_program_definition(),
            ResourceRequest::default(),
            false,
            None,
            Some(Vec::new()),
            1,
        ));
        state.add_job(Job::new(
            JobType::Simple,
            227,
            130,
            "".to_string(),
            dummy_program_definition(),
            ResourceRequest::default(),
            false,
            None,
            Some(Vec::new()),
            2,
        ));

        assert!(state.get_job_mut_by_tako_task_id(99).is_none());
        assert_eq!(state.get_job_mut_by_tako_task_id(100).unwrap().job_id, 223);
        assert_eq!(state.get_job_mut_by_tako_task_id(101).unwrap().job_id, 223);
        assert_eq!(state.get_job_mut_by_tako_task_id(109).unwrap().job_id, 223);
        assert_eq!(state.get_job_mut_by_tako_task_id(110).unwrap().job_id, 224);
        assert_eq!(state.get_job_mut_by_tako_task_id(124).unwrap().job_id, 224);
        assert_eq!(state.get_job_mut_by_tako_task_id(125).unwrap().job_id, 225);
        assert_eq!(state.get_job_mut_by_tako_task_id(126).unwrap().job_id, 226);
        assert!(state.get_job_mut_by_tako_task_id(127).is_none());
        assert!(state.get_job_mut_by_tako_task_id(129).is_none());
        assert_eq!(state.get_job_mut_by_tako_task_id(130).unwrap().job_id, 227);
        assert!(state.get_job_mut_by_tako_task_id(131).is_none());
    }
}
