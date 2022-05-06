use std::fmt;

use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::ResourceRequest;
use crate::internal::common::resources::{NumOfCpus, TimeRequest};
use crate::internal::common::Set;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::server::comm::Comm;
use crate::internal::server::task::Task;
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::workerload::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::{TaskId, WorkerId};
use std::time::Duration;

bitflags::bitflags! {
    pub(crate) struct WorkerFlags: u32 {
        // There is no "waiting" task for resources of this worker
        // Therefore this worker should not cause balancing even it is underloaded
        const PARKED = 0b00000001;
        // The worker is in processed of being stopped. In the current implementation
        // it is always the case when the user ask for stop
        const STOPPING = 0b00000010;
        // The server sent message ReservationOn to worker that causes
        // that the worker will not be turned off because of idle timeout.
        // This state induced by processing multi node tasks.
        // It may occur when we are waiting for remaining workers for multi-node tasks
        // and for non-master nodes of a multi-node tasks (because they will not receive any
        // ComputeTask message).
        const RESERVED = 0b00000100;
    }
}

#[derive(Debug)]
pub struct MultiNodeTaskAssignment {
    pub task_id: TaskId,

    // If true, then task is assigned to this worker on a logical level and no other task
    // is running. However, no work is done by HQ on this node.
    // For example for MPI application, HQ starts "mpirun" on a root node and HQ
    // does no other action other nodes expect ensuring that nothing else is running there.
    pub reservation_only: bool,
}

pub struct Worker {
    pub(crate) id: WorkerId,

    // This is list of single node assigned tasks
    // !! In case of stealing T from W1 to W2, T is in "tasks" of W2, even T was not yet canceled from W1.
    sn_tasks: Set<TaskId>,
    pub(crate) sn_load: WorkerLoad,
    pub(crate) resources: WorkerResources,
    pub(crate) flags: WorkerFlags,
    // When the worker will be terminated
    pub(crate) termination_time: Option<std::time::Instant>,

    pub(crate) mn_task: Option<MultiNodeTaskAssignment>,

    // COLD DATA move it into a box (?)
    pub(crate) last_heartbeat: std::time::Instant,
    pub(crate) configuration: WorkerConfiguration,
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("resources", &self.configuration.resources)
            .field("load", &self.sn_load)
            .field("tasks", &self.sn_tasks.len())
            .finish()
    }
}

impl Worker {
    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn configuration(&self) -> &WorkerConfiguration {
        &self.configuration
    }

    pub fn sn_tasks(&self) -> &Set<TaskId> {
        &self.sn_tasks
    }

    pub fn mn_task(&self) -> Option<&MultiNodeTaskAssignment> {
        self.mn_task.as_ref()
    }

    pub fn set_mn_task(&mut self, task_id: TaskId, reservation_only: bool) {
        assert!(self.mn_task.is_none() && self.sn_tasks.is_empty());
        self.mn_task = Some(MultiNodeTaskAssignment {
            task_id,
            reservation_only,
        });
    }

    pub fn reset_mn_task(&mut self) {
        self.mn_task = None;
    }

    pub fn set_reservation(&mut self, value: bool, comm: &mut impl Comm) {
        if self.is_reserved() != value {
            self.flags.set(WorkerFlags::RESERVED, value);
            comm.send_worker_message(self.id, &ToWorkerMessage::SetReservation(value))
        }
    }

    pub fn is_reserved(&self) -> bool {
        self.flags.contains(WorkerFlags::RESERVED)
    }

    pub fn is_free(&self) -> bool {
        self.sn_tasks.is_empty() && self.mn_task.is_none() && !self.is_stopping()
    }

    pub fn insert_sn_task(&mut self, task: &Task) {
        assert!(self.sn_tasks.insert(task.id));
        self.sn_load
            .add_request(&task.configuration.resources, &self.resources);
    }

    pub fn remove_sn_task(&mut self, task: &Task) {
        assert!(self.sn_tasks.remove(&task.id));
        self.sn_load
            .remove_request(&task.configuration.resources, &self.resources);
    }

    pub fn sanity_check(&self, task_map: &TaskMap) {
        assert!(self.sn_tasks.is_empty() || self.mn_task.is_none());
        let mut check_load = WorkerLoad::new(&self.resources);
        for &task_id in &self.sn_tasks {
            let task = task_map.get_task(task_id);
            check_load.add_request(&task.configuration.resources, &self.resources);
        }
        assert_eq!(self.sn_load, check_load);
    }

    pub fn load(&self) -> &WorkerLoad {
        &self.sn_load
    }

    pub fn is_underloaded(&self) -> bool {
        self.sn_load.is_underloaded(&self.resources) && self.mn_task.is_none()
    }

    pub fn is_overloaded(&self) -> bool {
        self.sn_load.is_overloaded(&self.resources)
    }

    pub fn have_immediate_resources_for_rq(&self, request: &ResourceRequest) -> bool {
        self.sn_load
            .have_immediate_resources_for_rq(request, &self.resources)
    }

    pub fn have_immediate_resources_for_lb(&self, rrb: &ResourceRequestLowerBound) -> bool {
        self.sn_load
            .have_immediate_resources_for_lb(rrb, &self.resources)
    }

    pub fn is_more_loaded_then(&self, worker: &Worker) -> bool {
        self.sn_load.is_more_loaded_then(&worker.sn_load)
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
            sn_load: load,
            sn_tasks: Default::default(),
            flags: WorkerFlags::empty(),
            last_heartbeat: now,
            mn_task: None,
        }
    }
}
