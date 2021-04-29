use crate::common::WrappedRcRefCell;
use crate::server::job::{Job, JobId, JobState};
use crate::server::worker::Worker;
use crate::{Map, TaskId, WorkerId};
use tako::messages::gateway::{
    LostWorkerMessage, NewWorkerMessage, TaskFailedMessage, TaskState, TaskUpdate,
};
use tako::messages::worker::NewWorkerMsg;

pub struct State {
    jobs: crate::Map<TaskId, Job>,
    workers: crate::Map<WorkerId, Worker>,
    id_counter: TaskId,
}

pub type StateRef = WrappedRcRefCell<State>;

/*pub fn new_state_ref() -> StateRef {
        WrappedRcRefCell::wrap(State {

        })
}*/

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
        let task_id = job.task_id;
        assert!(self.jobs.insert(task_id, job).is_none())
    }

    pub fn new_job_id(&mut self) -> TaskId {
        let id = self.id_counter;
        self.id_counter += 1;
        id
    }

    pub fn get_workers(&self) -> &Map<WorkerId, Worker> {
        &self.workers
    }

    pub fn get_worker_mut(&mut self, worker_id: WorkerId) -> Option<&mut Worker> {
        self.workers.get_mut(&worker_id)
    }

    pub fn process_task_failed(&mut self, msg: TaskFailedMessage) {
        log::debug!("Task id={} failed", msg.id);
        let job = self.jobs.get_mut(&msg.id).unwrap();
        job.state = JobState::Failed(msg.info.message);
    }

    pub fn process_task_update(&mut self, msg: TaskUpdate) {
        log::debug!("Task id={} updated", msg.id);
        let mut job = self.jobs.get_mut(&msg.id).unwrap();
        job.state = match msg.state {
            TaskState::Running => JobState::Running,
            TaskState::Finished => JobState::Finished,
            TaskState::Waiting | TaskState::Invalid => {
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
        let mut worker = self.workers.get_mut(&msg.worker_id).unwrap();
        worker.set_offline_state();
    }
}

impl StateRef {
    pub fn new() -> StateRef {
        WrappedRcRefCell::wrap(State {
            jobs: Default::default(),
            workers: Default::default(),
            id_counter: 1,
        })
    }
}
