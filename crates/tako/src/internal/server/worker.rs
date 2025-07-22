use std::fmt;

use crate::gateway::{LostWorkerReason, WorkerRuntimeInfo};
use crate::internal::common::Set;
use crate::internal::common::resources::TimeRequest;
use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::{ResourceRequest, ResourceRequestVariants};
use crate::internal::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::internal::server::comm::Comm;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::workerload::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::{TaskId, WorkerId};
use std::time::{Duration, Instant};

bitflags::bitflags! {
    pub(crate) struct WorkerFlags: u32 {
        // There is no "waiting" task for resources of this worker
        // Therefore this worker should not cause balancing even it is underloaded
        const PARKED = 0b00000001;
        // The server sent message ReservationOn to worker that causes
        // that the worker will not be turned off because of idle timeout.
        // This state induced by processing multi node tasks.
        // It may occur when we are waiting for remaining workers for multi-node tasks
        // and for non-master nodes of a multi-node tasks (because they will not receive any
        // ComputeTask message).
        const RESERVED = 0b00000010;
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

pub const DEFAULT_WORKER_OVERVIEW_INTERVAL: Duration = Duration::from_secs(2);

pub struct Worker {
    pub(crate) id: WorkerId,

    // This is list of single node assigned tasks
    // !! In case of stealing T from W1 to W2, T is in "tasks" of W2, even T was not yet canceled from W1.
    sn_tasks: Set<TaskId>,
    pub(crate) sn_load: WorkerLoad,
    pub(crate) resources: WorkerResources,
    pub(crate) flags: WorkerFlags,
    // When the worker will be terminated
    pub(crate) termination_time: Option<Instant>,
    pub(crate) stop_reason: Option<(LostWorkerReason, Instant)>,

    pub(crate) mn_task: Option<MultiNodeTaskAssignment>,

    // Saved timestamp when a worker is put into an idle state
    pub(crate) idle_timestamp: Instant,

    // COLD DATA move it into a box (?)
    pub(crate) last_heartbeat: Instant,
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

    pub fn worker_info(&self, task_map: &TaskMap) -> WorkerRuntimeInfo {
        if let Some(mn_task) = &self.mn_task {
            WorkerRuntimeInfo::MultiNodeTask {
                main_node: !mn_task.reservation_only,
            }
        } else {
            let mut running_tasks = 0;
            self.sn_tasks.iter().for_each(|task_id| {
                if task_map.get_task(*task_id).is_sn_running() {
                    running_tasks += 1;
                }
            });
            let assigned_tasks = self.sn_tasks.len() as u32;
            WorkerRuntimeInfo::SingleNodeTasks {
                assigned_tasks,
                running_tasks,
                is_reserved: self.is_reserved(),
            }
        }
    }

    pub fn reset_mn_task(&mut self) {
        self.mn_task = None;
        self.idle_timestamp = Instant::now();
    }

    pub fn set_reservation(&mut self, value: bool) {
        if self.is_reserved() != value {
            self.flags.set(WorkerFlags::RESERVED, value);
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
            .add_request(task.id, &task.configuration.resources, &self.resources);
    }

    pub fn remove_sn_task(&mut self, task: &Task) {
        assert!(self.sn_tasks.remove(&task.id));
        if self.sn_tasks.is_empty() {
            self.idle_timestamp = Instant::now();
        }
        self.sn_load
            .remove_request(task.id, &task.configuration.resources, &self.resources);
    }

    pub fn sanity_check(&self, task_map: &TaskMap) {
        assert!(self.sn_tasks.is_empty() || self.mn_task.is_none());
        let mut check_load = WorkerLoad::new(&self.resources);
        let mut trivial = true;
        for &task_id in &self.sn_tasks {
            let task = task_map.get_task(task_id);
            trivial &= task.configuration.resources.is_trivial();
            check_load.add_request(task_id, &task.configuration.resources, &self.resources);
        }
        if trivial {
            assert_eq!(self.sn_load, check_load);
        }
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

    pub fn have_immediate_resources_for_rqv(&self, rqv: &ResourceRequestVariants) -> bool {
        self.sn_load
            .have_immediate_resources_for_rqv(rqv, &self.resources)
    }

    pub fn have_immediate_resources_for_lb(&self, rrb: &ResourceRequestLowerBound) -> bool {
        self.sn_load
            .have_immediate_resources_for_lb(rrb, &self.resources)
    }

    pub fn load_wrt_rqv(&self, rqv: &ResourceRequestVariants) -> u32 {
        self.sn_load.load_wrt_rqv(&self.resources, rqv)
    }

    pub fn set_parked_flag(&mut self, value: bool) {
        self.flags.set(WorkerFlags::PARKED, value);
    }

    pub fn is_parked(&self) -> bool {
        self.flags.contains(WorkerFlags::PARKED)
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest, now: Instant) -> bool {
        self.has_time_to_run(request.min_time(), now)
            && self.resources.is_capable_to_run_request(request)
    }

    pub fn is_capable_to_run_rqv(
        &self,
        rqv: &ResourceRequestVariants,
        now: std::time::Instant,
    ) -> bool {
        rqv.requests()
            .iter()
            .any(|r| self.is_capable_to_run(r, now))
    }

    // Returns None if there is no time limit for a worker or time limit was passed
    pub fn remaining_time(&self, now: Instant) -> Option<Duration> {
        self.termination_time.map(|time| {
            if time < now {
                Duration::default()
            } else {
                time - now
            }
        })
    }

    pub fn set_stop(&mut self, reason: LostWorkerReason) {
        self.stop_reason = Some((reason, Instant::now()));
    }

    pub fn is_stopping(&self) -> bool {
        self.stop_reason.is_some()
    }

    pub fn retract_overtime_tasks(
        &mut self,
        comm: &mut impl Comm,
        task_map: &mut TaskMap,
        now: Instant,
    ) {
        if self.termination_time.is_none() || self.mn_task.is_some() {
            return;
        }
        let task_ids: Vec<TaskId> = self
            .sn_tasks
            .iter()
            .copied()
            .filter(|task_id| {
                let task = task_map.get_task_mut(*task_id);
                if task.is_assigned()
                    && !self.is_capable_to_run_rqv(&task.configuration.resources, now)
                {
                    log::debug!(
                        "Retracting task={task_id}, time request cannot be fulfilled anymore"
                    );
                    task.state = TaskRuntimeState::Stealing(self.id, None);
                    true
                } else {
                    false
                }
            })
            .collect();
        if !task_ids.is_empty() {
            for task_id in &task_ids {
                self.sn_tasks.remove(task_id);
            }
            comm.send_worker_message(
                self.id,
                &ToWorkerMessage::StealTasks(TaskIdsMsg { ids: task_ids }),
            );
            comm.ask_for_scheduling();
        }
    }

    pub fn has_time_to_run(&self, time_request: TimeRequest, now: Instant) -> bool {
        if let Some(time) = self.termination_time {
            now + time_request < time
        } else {
            true
        }
    }

    pub fn has_time_to_run_for_rqv(&self, rqv: &ResourceRequestVariants, now: Instant) -> bool {
        self.has_time_to_run(rqv.min_time(), now)
    }
}

impl Worker {
    pub fn new(
        id: WorkerId,
        configuration: WorkerConfiguration,
        resource_map: &ResourceMap,
        now: Instant,
    ) -> Self {
        let resources = WorkerResources::from_description(&configuration.resources, resource_map);
        let load = WorkerLoad::new(&resources);
        Self {
            id,
            termination_time: configuration.time_limit.map(|duration| now + duration),
            configuration,
            resources,
            sn_load: load,
            sn_tasks: Default::default(),
            flags: WorkerFlags::empty(),
            stop_reason: None,
            last_heartbeat: now,
            mn_task: None,
            idle_timestamp: now,
        }
    }
}
