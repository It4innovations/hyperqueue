use tokio::sync::oneshot;

use crate::common::resources::NumOfCpus;
use crate::common::resources::ResourceRequest;
use crate::common::Set;
use crate::messages::common::WorkerConfiguration;
use crate::messages::worker::WorkerOverview;
use crate::server::task::{Task, TaskRef};
use crate::server::worker_load::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};
use std::fmt;

pub type WorkerId = u64;

bitflags::bitflags! {
    pub struct WorkerFlags: u32 {
        // There is no "waiting" task for resources of this worker
        // Therefore this worker should not cause balancing even it is underloaded
        const PARKED = 0b00000001;
        // The worker is in processed of being stopped. In the current implementation
        // it is always the case when the user ask for stop
        const STOPPING = 0b00000010;
    }
}

pub struct Worker {
    pub id: WorkerId,

    // This is list of assigned tasks
    // !! In case of stealinig T from W1 to W2, T is in "tasks" of W2, even T was not yet canceled from W1.
    tasks: Set<TaskRef>,

    pub load: WorkerLoad,
    pub resources: WorkerResources,
    pub flags: WorkerFlags,

    // COLD DATA move it into a box (?)
    pub last_heartbeat: std::time::Instant,
    pub last_occupied: std::time::Instant,
    pub configuration: WorkerConfiguration,
    pub overview_callbacks: Vec<oneshot::Sender<WorkerOverview>>,
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        //let task_ids : Vec<_> = self.tasks.iter().map(|r| r.get().id.to_string()).collect();
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("resources", &self.configuration.resources)
            .field("load", &self.load)
            .field("tasks", &self.tasks.len())
            .finish()
    }
}

impl Worker {
    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn tasks(&self) -> &Set<TaskRef> {
        &self.tasks
    }

    pub fn insert_task(&mut self, task: &Task, task_ref: TaskRef) {
        assert!(self.tasks.insert(task_ref));
        self.load.add_request(&task.resources, &self.resources);
    }

    pub fn remove_task(&mut self, task: &Task, task_ref: &TaskRef) {
        assert!(self.tasks.remove(&task_ref));
        self.load.remove_request(&task.resources, &self.resources);
    }

    pub fn sanity_check(&self) {
        let mut check_load = WorkerLoad::default();
        for task_ref in &self.tasks {
            let task = task_ref.get();
            check_load.add_request(&task.resources, &self.resources);
        }
        assert_eq!(self.load, check_load);
    }

    /*#[inline]
    pub fn address(&self) -> &str {
        &self.listen_address
    }*/

    /*pub fn hostname(&self) -> String {
        let s = self.configuration.listen_address.as_str();
        let s: &str = s.find("://").map(|p| &s[p + 3..]).unwrap_or(s);
        s.chars().take_while(|x| *x != ':').collect()
    }*/

    pub fn load(&self) -> &WorkerLoad {
        &self.load
    }

    pub fn is_underloaded(&self) -> bool {
        self.load.is_underloaded(&self.resources)
    }

    pub fn is_overloaded(&self) -> bool {
        self.load.is_overloaded(&self.resources)
    }

    pub fn have_immediate_resources_for_rq(&self, request: &ResourceRequest) -> bool {
        self.load
            .have_immediate_resources_for_rq(request, &self.resources)
    }

    pub fn have_immediate_resources_for_lb(&self, rrb: &ResourceRequestLowerBound) -> bool {
        self.load
            .have_immediate_resources_for_lb(rrb, &self.resources)
    }

    pub fn is_more_loaded_then(&self, worker: &Worker) -> bool {
        self.load.is_more_loaded_then(&worker.load)
    }

    pub fn get_free_cores(&self) -> NumOfCpus {
        todo!()
    }

    pub fn set_parked_flag(&mut self, value: bool) {
        self.flags.set(WorkerFlags::PARKED, value);
    }

    pub fn is_parked(&self) -> bool {
        self.flags.contains(WorkerFlags::PARKED)
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest) -> bool {
        self.resources.is_capable_to_run(&request)
    }

    pub fn set_stopping_flag(&mut self, value: bool) {
        self.flags.set(WorkerFlags::STOPPING, value);
    }

    pub fn is_stopping(&self) -> bool {
        self.flags.contains(WorkerFlags::STOPPING)
    }
}

//pub type WorkerRef = WrappedRcRefCell<Worker>;

impl Worker {
    pub fn new(id: WorkerId, configuration: WorkerConfiguration) -> Self {
        let resources = WorkerResources::from_description(&configuration.resources);
        let now = std::time::Instant::now();
        Worker {
            id,
            configuration,
            resources,
            tasks: Default::default(),
            flags: WorkerFlags::empty(),
            overview_callbacks: Default::default(),
            last_heartbeat: now,
            last_occupied: now,
            load: Default::default(),
        }
    }
}
