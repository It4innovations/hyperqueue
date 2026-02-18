use std::fmt;

use crate::gateway::WorkerRuntimeInfo::SingleNodeTasks;
use crate::gateway::{LostWorkerReason, WorkerRuntimeInfo};
use crate::internal::common::Set;
use crate::internal::common::resources::map::{ResourceIdMap, ResourceRqMap};
use crate::internal::common::resources::{ResourceRequest, ResourceRequestVariants};
use crate::internal::common::resources::{ResourceRqId, TimeRequest};
use crate::internal::messages::worker::{TaskIdsMsg, ToWorkerMessage};
use crate::internal::server::comm::Comm;
use crate::internal::server::task::{Task, TaskRuntimeState};
use crate::internal::server::taskmap::TaskMap;
use crate::internal::server::workergroup::WorkerGroup;
use crate::internal::server::workerload::{ResourceRequestLowerBound, WorkerLoad, WorkerResources};
use crate::internal::worker::configuration::WorkerConfiguration;
use crate::{Map, TaskId, WorkerId};
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

    // If true, then task is assigned to this worker on a logical level and no other task
    // is running. However, no work is done by HQ on this node.
    // For example for MPI application, HQ starts "mpirun" on a root node and HQ
    // does no other action other nodes expect ensuring that nothing else is running there.
    pub reservation_only: bool,
}

pub struct SingleNodeTaskAssignment {
    // This is list of single node assigned tasks
    pub assign_tasks: Set<TaskId>,
    pub free_resources: WorkerResources,
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
        })
    }
}

pub const DEFAULT_WORKER_OVERVIEW_INTERVAL: Duration = Duration::from_secs(2);

pub struct Worker {
    pub(crate) id: WorkerId,

    assignment: WorkerAssignment,

    pub(crate) resources: WorkerResources,
    pub(crate) flags: WorkerFlags,
    // When the worker will be terminated
    pub(crate) termination_time: Option<Instant>,
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

    pub fn set_mn_task(&mut self, task_id: TaskId, reservation_only: bool) {
        assert!(self.sn_assignment().unwrap().assign_tasks.is_empty());
        self.assignment = WorkerAssignment::Mn(MultiNodeTaskAssignment {
            task_id,
            reservation_only,
        });
        if reservation_only {
            self.set_reservation(true);
        }
    }

    pub fn has_mn_task(&self) -> bool {
        match &self.assignment {
            WorkerAssignment::Sn(_) => false,
            WorkerAssignment::Mn(_) => true,
        }
    }

    pub fn worker_info(&self, task_map: &TaskMap) -> WorkerRuntimeInfo {
        match &self.assignment {
            WorkerAssignment::Sn(a) => {
                todo!()
                /*let mut running_tasks = 0;
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
                }*/
            }
            WorkerAssignment::Mn(a) => WorkerRuntimeInfo::MultiNodeTask {
                main_node: !a.reservation_only,
            },
        }
    }

    pub fn reset_mn_task(&mut self) {
        self.assignment = WorkerAssignment::empty_sn(&self.resources);
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

    /*pub fn start_task(&mut self, task_id: TaskId, rq: &ResourceRequest) {
        match &mut self.assignment {
            WorkerAssignment::Sn(a) => {
                a.free_resources.remove(rq);
            }
            WorkerAssignment::Mn(_) => {
                unreachable!()
            }
        }
    }*/

    /*pub fn collect_assigned_non_running_tasks(&mut self, out: &mut Vec<TaskId>) {
        match &self.assignment {
            WorkerAssignment::Sn(sn) => {
                for (task_id, is_running) in &sn.assign_tasks {
                    if !is_running {
                        out.push(*task_id);
                    }
                }
            }
            WorkerAssignment::Mn(_) => {
                todo!()
            }
        }
    }*/

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

    pub fn sanity_check(&self, task_map: &TaskMap, request_map: &ResourceRqMap) {
        let mut check_load = WorkerLoad::new(&self.resources);
        let mut trivial = true;
        if let Some(a) = self.sn_assignment() {
            for task_id in &a.assign_tasks {
                let task = task_map.get_task(*task_id);
                let rqv = request_map.get(task.resource_rq_id);
                trivial &= rqv.is_trivial();
                check_load.add_request(*task_id, rqv, task.running_variant(), &self.resources);
            }
        }
    }

    pub fn have_immediate_resources_for_rq(&self, request: &ResourceRequest) -> bool {
        let Some(a) = self.sn_assignment() else {
            return false;
        };
        a.free_resources.is_capable_to_run_request(request)
    }

    pub fn have_immediate_resources_for_rqv(&self, rqv: &ResourceRequestVariants) -> bool {
        todo!()
        // self.sn_load
        //     .have_immediate_resources_for_rqv(rqv, &self.resources)
    }

    pub fn have_immediate_resources_for_rqv_now(
        &self,
        rqv: &ResourceRequestVariants,
        now: Instant,
    ) -> bool {
        /*self.has_time_to_run_for_rqv(rqv, now)
        && self
            .sn_load
            .have_immediate_resources_for_rqv(rqv, &self.resources)*/
        todo!()
    }

    pub fn have_immediate_resources_for_lb(&self, rrb: &ResourceRequestLowerBound) -> bool {
        todo!()
        /*self.sn_load
        .have_immediate_resources_for_lb(rrb, &self.resources)*/
    }

    pub fn load_wrt_rqv(&self, rqv: &ResourceRequestVariants) -> u32 {
        todo!()
        //self.sn_load.load_wrt_rqv(&self.resources, rqv)
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

    pub fn retract_overtime_tasks(
        &mut self,
        comm: &mut impl Comm,
        task_map: &mut TaskMap,
        request_map: &ResourceRqMap,
        now: Instant,
    ) {
        todo!()
        /*if self.termination_time.is_none() || self.mn_assignment().is_some() {
            return;
        }
        let task_ids: Vec<TaskId> = self
            .sn_assignment()
            .unwrap()
            .assign_tasks
            .iter()
            .filter_map(|task_id| {
                let task = task_map.get_task_mut(*task_id);
                if task.is_assigned()
                    && !self.is_capable_to_run_rqv(request_map.get(task.resource_rq_id), now)
                {
                    log::debug!(
                        "Retracting task={task_id}, time request cannot be fulfilled anymore"
                    );
                    task.state = TaskRuntimeState::Retracting { source: self.id };
                    Some(*task_id)
                } else {
                    None
                }
            })
            .collect();
        if !task_ids.is_empty() {
            for task_id in &task_ids {
                let task = task_map.get_task(*task_id);
                let (_, rv_id) = task.get_assignments().unwrap();
                let rq = request_map.get(task.resource_rq_id).get(rv_id);
                self.remove_sn_task(*task_id, rq);
            }
            comm.send_worker_message(
                self.id,
                &ToWorkerMessage::StealTasks(TaskIdsMsg { ids: task_ids }),
            );
            comm.ask_for_scheduling();
        }*/
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

    pub fn new(
        id: WorkerId,
        configuration: WorkerConfiguration,
        resource_map: &ResourceIdMap,
        now: Instant,
    ) -> Self {
        let resources = WorkerResources::from_description(&configuration.resources, resource_map);
        let load = WorkerLoad::new(&resources);
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
                    "reservation_only": a.reservation_only,
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
