use std::fmt;

use crate::gateway::{LostWorkerReason, WorkerRuntimeInfo};
use crate::internal::common::Set;
use crate::internal::common::resources::TimeRequest;
use crate::internal::common::resources::map::{ResourceIdMap, ResourceRqMap};
use crate::internal::common::resources::{ResourceRequest, ResourceRequestVariants};
use crate::internal::server::comm::Comm;
use crate::internal::server::task::TaskRuntimeState;
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::server::workermap::WorkerMap;
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::resources::ResourceRqId;
use crate::{Map, ResourceVariantId, TaskId, WorkerId};
use bitflags::Flags;
use itertools::Itertools;
use nix::libc::W_OK;
use serde_json::json;
use std::time::{Duration, Instant};

bitflags::bitflags! {
    pub(crate) struct WorkerFlags: u32 {
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

    // If false, then task is assigned to this worker on a logical level and no other task
    // is running. However, no work is done by HQ on this node.
    // For example for MPI application, HQ starts "mpirun" on a root node and HQ
    // does no other action other nodes expect ensuring that nothing else is running there.
    pub is_root: bool,
}

#[derive(Debug)]
pub struct SingleNodeTaskAssignment {
    // This is list of single node assigned tasks
    pub assign_tasks: Set<TaskId>,
    pub free_resources: WorkerResources,
    pub prefilled_tasks: Set<TaskId>,
}

pub enum WorkerAssignment {
    Sn(SingleNodeTaskAssignment),
    Mn(MultiNodeTaskAssignment),
}

impl WorkerAssignment {
    fn empty_sn(wr: &WorkerResources) -> Self {
        Self::Sn(SingleNodeTaskAssignment {
            assign_tasks: Default::default(),
            free_resources: wr.clone(),
            prefilled_tasks: Default::default(),
        })
    }
}

pub const DEFAULT_WORKER_OVERVIEW_INTERVAL: Duration = Duration::from_secs(2);

pub struct Worker {
    pub(crate) id: WorkerId,

    assignment: WorkerAssignment,

    pub(crate) resources: WorkerResources,
    pub(crate) blocked_requests: Set<(ResourceRqId, ResourceVariantId)>,
    // When the worker will be terminated
    pub(crate) termination_time: Option<Instant>,

    pub(crate) flags: WorkerFlags,
    pub(crate) stop_reason: Option<(LostWorkerReason, Instant)>,

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

    pub fn assignment(&self) -> &WorkerAssignment {
        &self.assignment
    }

    #[inline]
    pub fn sn_assignment(&self) -> Option<&SingleNodeTaskAssignment> {
        match &self.assignment {
            WorkerAssignment::Sn(a) => Some(a),
            WorkerAssignment::Mn(_) => None,
        }
    }

    #[inline]
    pub fn mn_assignment(&self) -> Option<&MultiNodeTaskAssignment> {
        match &self.assignment {
            WorkerAssignment::Sn(_) => None,
            WorkerAssignment::Mn(a) => Some(a),
        }
    }

    pub fn set_mn_task(&mut self, task_id: TaskId, is_root: bool) {
        assert!(self.is_free());
        self.assignment = WorkerAssignment::Mn(MultiNodeTaskAssignment { task_id, is_root });
    }

    pub fn has_mn_task(&self) -> bool {
        match &self.assignment {
            WorkerAssignment::Sn(_) => false,
            WorkerAssignment::Mn(_) => true,
        }
    }

    pub fn is_reserved(&self) -> bool {
        self.flags.contains(WorkerFlags::RESERVED)
    }

    pub fn worker_info(&self, task_map: &TaskMap) -> WorkerRuntimeInfo {
        match &self.assignment {
            WorkerAssignment::Sn(a) => {
                let mut running_tasks = 0;
                a.assign_tasks.iter().for_each(|task_id| {
                    if task_map.get_task(*task_id).is_sn_running() {
                        running_tasks += 1;
                    }
                });
                let assigned_tasks = a.assign_tasks.len() as u32;
                WorkerRuntimeInfo::SingleNodeTasks {
                    assigned_tasks,
                    running_tasks,
                    is_reserved: self.is_reserved(),
                }
            }
            WorkerAssignment::Mn(a) => WorkerRuntimeInfo::MultiNodeTask {
                main_node: a.is_root,
            },
        }
    }

    pub fn reset_mn_task(&mut self) {
        self.assignment = WorkerAssignment::empty_sn(&self.resources);
        self.idle_timestamp = Instant::now();
    }

    pub fn set_reservation(&mut self, value: bool) {
        self.flags.set(WorkerFlags::RESERVED, value);
    }

    pub fn is_free(&self) -> bool {
        (match &self.assignment {
            WorkerAssignment::Sn(a) => a.assign_tasks.is_empty(),
            WorkerAssignment::Mn(a) => false,
        }) && !self.is_stopping()
    }

    pub fn insert_sn_task(&mut self, task_id: TaskId, rq: &ResourceRequest) {
        match &mut self.assignment {
            WorkerAssignment::Sn(a) => {
                a.free_resources.remove(rq);
                assert!(a.assign_tasks.insert(task_id));
            }
            WorkerAssignment::Mn(_) => unreachable!(),
        }
    }

    pub fn insert_prefill_task(&mut self, task_id: TaskId) {
        match &mut self.assignment {
            WorkerAssignment::Sn(a) => assert!(a.prefilled_tasks.insert(task_id)),
            WorkerAssignment::Mn(_) => unreachable!(),
        }
    }

    pub fn remove_prefill_task(&mut self, task_id: TaskId) {
        match &mut self.assignment {
            WorkerAssignment::Sn(a) => assert!(a.prefilled_tasks.remove(&task_id)),
            WorkerAssignment::Mn(_) => unreachable!(),
        }
    }

    pub fn task_from_prefilled_to_started(&mut self, task_id: TaskId, rq: &ResourceRequest) {
        match &mut self.assignment {
            WorkerAssignment::Sn(a) => {
                assert!(a.prefilled_tasks.remove(&task_id));
                assert!(a.assign_tasks.insert(task_id));
                a.free_resources.remove(rq);
            }
            WorkerAssignment::Mn(_) => unreachable!(),
        }
    }

    pub fn remove_sn_task(&mut self, task_id: TaskId, rq: &ResourceRequest) {
        match &mut self.assignment {
            WorkerAssignment::Sn(a) => {
                assert!(a.assign_tasks.remove(&task_id));
                if a.assign_tasks.is_empty() {
                    self.idle_timestamp = Instant::now();
                }
                a.free_resources.add(rq, &self.resources);
            }
            WorkerAssignment::Mn(_) => unreachable!(),
        }
    }

    pub fn sanity_check(
        &self,
        task_map: &TaskMap,
        request_map: &ResourceRqMap,
        transfers: &Map<TaskId, (WorkerId, ResourceVariantId)>,
    ) {
        if let Some(a) = self.sn_assignment() {
            let mut resources = self.resources.clone();
            for task_id in a.assign_tasks.iter() {
                let task = task_map.get_task(*task_id);
                let (worker_id, rv_id) = match &task.state {
                    TaskRuntimeState::Assigned { worker_id, rv_id }
                    | TaskRuntimeState::Running { worker_id, rv_id } => (*worker_id, *rv_id),
                    TaskRuntimeState::Retracting { .. } => {
                        let (worker_id, rv_id) = transfers.get(task_id).unwrap();
                        (*worker_id, *rv_id)
                    }
                    s => panic!("Invalid state {s:?}"),
                };
                assert_eq!(self.id, worker_id);
                let rq = request_map.get(task.resource_rq_id).get(rv_id);
                resources.remove(rq);
            }
            assert_eq!(a.free_resources, resources);
            for task_id in a.prefilled_tasks.iter() {
                let task = task_map.get_task(*task_id);
                match &task.state {
                    TaskRuntimeState::Prefilled { worker_id } => {
                        assert_eq!(self.id, *worker_id);
                    }
                    _ => panic!("Invalid state {:?}", task.state),
                }
            }
        }
    }

    pub fn have_immediate_resources_for_rq(&self, request: &ResourceRequest) -> bool {
        let Some(a) = self.sn_assignment() else {
            return false;
        };
        a.free_resources.is_capable_to_run_request(request)
    }

    pub fn is_capable_to_run(&self, request: &ResourceRequest, now: Instant) -> bool {
        if !self.has_time_to_run(request.min_time(), now) {
            return false;
        }
        if request.is_multi_node() {
            true
        } else {
            self.resources.is_capable_to_run_request(request)
        }
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

    pub fn has_time_to_run(&self, time_request: TimeRequest, now: Instant) -> bool {
        if let Some(time) = self.termination_time {
            now + time_request <= time
        } else {
            true
        }
    }

    pub fn has_time_to_run_for_rqv(&self, rqv: &ResourceRequestVariants, now: Instant) -> bool {
        self.has_time_to_run(rqv.min_time(), now)
    }

    pub fn is_request_blocked(
        &self,
        resource_rq_id: ResourceRqId,
        rv_id: ResourceVariantId,
    ) -> bool {
        self.blocked_requests.contains(&(resource_rq_id, rv_id))
    }

    pub fn block_request(&mut self, resource_rq_id: ResourceRqId, rv_id: ResourceVariantId) {
        self.blocked_requests.insert((resource_rq_id, rv_id));
    }

    pub fn unblock_request(&mut self, resource_rq_id: ResourceRqId, rv_id: ResourceVariantId) {
        self.blocked_requests.remove(&(resource_rq_id, rv_id));
    }

    pub fn new(
        id: WorkerId,
        configuration: WorkerConfiguration,
        resource_map: &ResourceIdMap,
        now: Instant,
    ) -> Self {
        let resources = WorkerResources::from_description(&configuration.resources, resource_map);
        Self {
            id,
            termination_time: configuration.time_limit.map(|duration| now + duration),
            configuration,
            assignment: WorkerAssignment::empty_sn(&resources),
            resources,
            flags: WorkerFlags::empty(),
            stop_reason: None,
            last_heartbeat: now,
            idle_timestamp: now,
            blocked_requests: Set::new(),
        }
    }

    pub(crate) fn dump(&self, now: Instant) -> serde_json::Value {
        json! ({
            "id": self.id,
            "assignment": match &self.assignment {
                WorkerAssignment::Sn(a) => json! ({
                    "assigned_tasks": &a.assign_tasks
                }),
                WorkerAssignment::Mn(a) => json! ({
                    "task_id": a.task_id,
                    "is_root": a.is_root,
                })
            },
            "flags": self.flags.bits(),
            "termination_time": self.termination_time.map(|x| x - now),
            "idle_timestamp": now - self.idle_timestamp,
            "last_heartbeat": now - self.last_heartbeat,
            "configuration": self.configuration,
        })
    }
}
