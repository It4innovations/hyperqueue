use std::fmt;

use tokio::sync::oneshot;

use crate::common::resources::map::ResourceMap;
use crate::common::resources::ResourceRequest;
use crate::common::resources::{NumOfCpus, TimeRequest};
use crate::common::Set;
use crate::messages::common::WorkerConfiguration;
use crate::messages::worker::WorkerOverview;
use crate::server::task::Task;
use crate::server::taskmap::TaskMap;
use crate::server::worker_load::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};
use crate::{TaskId, WorkerId};
use std::time::Duration;

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
    // !! In case of stealing T from W1 to W2, T is in "tasks" of W2, even T was not yet canceled from W1.
    tasks: Set<TaskId>,

    pub load: WorkerLoad,
    pub resources: WorkerResources,
    pub flags: WorkerFlags,
    // When the worker will be terminated
    pub termination_time: Option<std::time::Instant>,

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

    pub fn tasks(&self) -> &Set<TaskId> {
        &self.tasks
    }

    pub fn insert_task(&mut self, task: &Task) {
        assert!(self.tasks.insert(task.id));
        self.load
            .add_request(&task.configuration.resources, &self.resources);
    }

    pub fn remove_task(&mut self, task: &Task) {
        assert!(self.tasks.remove(&task.id));
        self.load
            .remove_request(&task.configuration.resources, &self.resources);
    }

    pub fn sanity_check(&self, task_map: &TaskMap) {
        let mut check_load = WorkerLoad::new(&self.resources);
        for &task_id in &self.tasks {
            let task = task_map.get_task(task_id);
            check_load.add_request(&task.configuration.resources, &self.resources);
        }
        assert_eq!(self.load, check_load);
    }

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

    pub fn is_capable_to_run(&self, request: &ResourceRequest, now: std::time::Instant) -> bool {
        self.has_time_to_run(request.min_time(), now) && self.resources.is_capable_to_run(request)
    }

    // Returns None if there is no time limit for a worker or time limit was passed
    pub fn remaining_time(&self, now: std::time::Instant) -> Option<Duration> {
        self.termination_time.map(|time| {
            if time < now {
                Duration::default()
            } else {
                time - now
            }
        })
    }

    pub fn has_time_to_run(&self, time_request: TimeRequest, now: std::time::Instant) -> bool {
        if let Some(time) = self.termination_time {
            now + time_request < time
        } else {
            true
        }
    }

    pub fn set_stopping_flag(&mut self, value: bool) {
        self.flags.set(WorkerFlags::STOPPING, value);
    }

    pub fn is_stopping(&self) -> bool {
        self.flags.contains(WorkerFlags::STOPPING)
    }
}

impl Worker {
    pub fn new(
        id: WorkerId,
        configuration: WorkerConfiguration,
        resource_map: ResourceMap,
    ) -> Self {
        let resources = WorkerResources::from_description(&configuration.resources, resource_map);
        let load = WorkerLoad::new(&resources);
        let now = std::time::Instant::now();
        Self {
            id,
            termination_time: configuration.time_limit.map(|duration| now + duration),
            configuration,
            resources,
            load,
            tasks: Default::default(),
            flags: WorkerFlags::empty(),
            overview_callbacks: Default::default(),
            last_heartbeat: now,
            last_occupied: now,
        }
    }
}
