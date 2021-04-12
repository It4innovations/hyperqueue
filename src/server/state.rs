use crate::common::WrappedRcRefCell;
use crate::server::job::{Job, JobStatus};
use crate::TaskId;
use tako::messages::gateway::{TaskFailedMessage, TaskUpdate};

pub struct State {
    jobs: crate::Map<TaskId, Job>,
    id_counter: TaskId,
}

pub type StateRef = WrappedRcRefCell<State>;

/*pub fn new_state_ref() -> StateRef {
        WrappedRcRefCell::wrap(State {

        })
}*/

impl State {
    pub fn jobs(&self) -> impl Iterator<Item=&Job> {
        self.jobs.values()
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

    pub fn process_task_failed(&mut self, msg: TaskFailedMessage) {
        log::debug!("Task id={} failed", msg.id);
        let job = self.jobs.get_mut(&msg.id).unwrap();
        job.status = JobStatus::Failed(msg.info.message);
    }

    pub fn process_task_update(&mut self, msg: TaskUpdate) {
        log::debug!("Task id={} updated", msg.id);
        let job = self.jobs.get_mut(&msg.id).unwrap();
        job.status = JobStatus::Finished;
    }
}

impl StateRef {
    pub fn new() -> StateRef {
        WrappedRcRefCell::wrap(State {
            jobs: Default::default(),
            id_counter: 1,
        })
    }
}