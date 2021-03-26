use crate::common::WrappedRcRefCell;
use crate::server::job::Job;
use crate::TaskId;

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
}

impl StateRef {
    pub fn new() -> StateRef {
        WrappedRcRefCell::wrap(State {
            jobs: Default::default(),
            id_counter: 1,
        })
    }
}