use crate::internal::common::resources::{Allocation, ResourceRequestVariants};
use crate::internal::common::Map;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::allocator::ResourceAllocator;
use crate::internal::worker::state::TaskMap;
use crate::internal::worker::task::Task;
use crate::{Priority, PriorityTuple, Set, TaskId, WorkerId};
use priority_queue::PriorityQueue;
use std::rc::Rc;
use std::time::Duration;

type QueuePriorityTuple = (Priority, Priority, Priority); // user priority, resource priority, scheduler priority

/// QueueForRequest is priority queue of the tasks that has the same resource request
/// The idea is that if we cannot schedule one task from this queue, we cannot schedule
/// any task in this queue.
/// So when we are finding a candidate to schedule,
/// it allows just to consider only first task in the resource queue.
///
/// We remember only PriorityTuple (i.e. user and scheduler priority) for each task
/// Because all tasks share the same resource request they all have the same resource priority
/// So we remember resource priority only for the whole queue, not for each task individually.
///
/// The queue also remember if it is currently blocked, that means that the
/// resource request cannot be scheduled right now, and we should skip it.
///
/// Note: Allocator also remembers blocked resources, so more "clean" solution would be to
/// do a lookup into allocator. But testing if queue is blocked is relatively often so
/// we directly remember information in the queue. It also allows to make queue more independent
/// on allocator.
#[derive(Debug)]
pub(crate) struct QueueForRequest {
    resource_priority: Priority,
    queue: PriorityQueue<TaskId, PriorityTuple>,
    is_blocked: bool,
}

impl QueueForRequest {
    pub fn reset_temporaries(&mut self) {
        self.is_blocked = false;
    }

    pub fn set_blocked(&mut self) {
        self.is_blocked = true;
    }

    pub fn current_priority(&self) -> Option<QueuePriorityTuple> {
        if self.is_blocked {
            None
        } else {
            self.peek().map(|x| x.1)
        }
    }

    pub fn peek(&self) -> Option<(TaskId, QueuePriorityTuple)> {
        if self.is_blocked {
            return None;
        }
        self.queue
            .peek()
            .map(|(task_id, priority)| (*task_id, (priority.0, self.resource_priority, priority.1)))
    }
}

pub struct ResourceWaitQueue {
    pub(super) queues: Map<ResourceRequestVariants, QueueForRequest>,
    pub(super) requests: Vec<ResourceRequestVariants>,
    pub(super) allocator: ResourceAllocator,
    pub(super) worker_resources: Map<WorkerResources, Set<WorkerId>>,
}

impl ResourceWaitQueue {
    pub fn new(allocator: ResourceAllocator) -> Self {
        ResourceWaitQueue {
            queues: Default::default(),
            requests: Default::default(),
            allocator,
            worker_resources: Default::default(),
        }
    }

    pub fn new_worker(&mut self, worker_id: WorkerId, resources: WorkerResources) {
        assert!(self
            .worker_resources
            .entry(resources)
            .or_default()
            .insert(worker_id));
        self.recompute_resource_priorities();
    }

    pub fn remove_worker(&mut self, worker_id: WorkerId) {
        self.worker_resources.retain(|_, value| {
            let is_empty = value.remove(&worker_id) && value.is_empty();
            !is_empty
        });
        self.recompute_resource_priorities();
    }

    pub fn resource_priority(&self, rqv: &ResourceRequestVariants) -> Priority {
        let mut p = 0;
        for (r, s) in &self.worker_resources {
            if !r.is_capable_to_run(rqv) {
                p += s.len() as Priority;
            }
        }
        p
    }

    pub fn release_allocation(&mut self, allocation: Rc<Allocation>) {
        self.allocator.release_allocation(allocation);
    }

    pub fn add_task(&mut self, task: &Task) {
        let priority = task.priority;
        let (queue, priority, task_id) = {
            (
                if let Some(qfr) = self.queues.get_mut(&task.resources) {
                    &mut qfr.queue
                } else {
                    log::debug!(
                        "Creating new request queue for {:?} (task {})",
                        task.resources,
                        task.id
                    );
                    self.requests.push(task.resources.clone());

                    let mut requests = std::mem::take(&mut self.requests);
                    // Sort bigger values first
                    requests.sort_unstable_by(|x, y| {
                        y.sort_key(&self.allocator)
                            .partial_cmp(&x.sort_key(&self.allocator))
                            .unwrap()
                    });
                    self.requests = requests;
                    let resource_priority = self.resource_priority(&task.resources);
                    &mut self
                        .queues
                        .entry(task.resources.clone())
                        .or_insert(QueueForRequest {
                            resource_priority,
                            queue: PriorityQueue::new(),
                            is_blocked: false,
                        })
                        .queue
                },
                priority,
                task.id,
            )
        };
        queue.push(task_id, priority);
    }

    pub fn remove_task(&mut self, task_id: TaskId) {
        for qfr in self.queues.values_mut() {
            if qfr.queue.remove(&task_id).is_some() {
                return;
            }
        }
        panic!("Removing unknown task");
    }

    pub fn recompute_resource_priorities(&mut self) {
        log::debug!("Recomputing resource priorities");
        let mut queues = std::mem::take(&mut self.queues);
        for (rq, qfr) in queues.iter_mut() {
            qfr.resource_priority = self.resource_priority(rq);
        }
        self.queues = queues;
    }

    pub fn try_start_tasks(
        &mut self,
        task_map: &TaskMap,
        remaining_time: Option<Duration>,
    ) -> Vec<(TaskId, Rc<Allocation>, usize)> {
        for qfr in self.queues.values_mut() {
            qfr.reset_temporaries()
        }
        self.allocator.reset_temporaries(remaining_time);
        let mut out = Vec::new();
        while !self.try_start_tasks_helper(task_map, &mut out) {
            self.allocator.close_priority_level()
        }
        out
    }

    /// This is "main" function of the worker resource allocation process.
    /// It tries to find a candidate task and try to schedule it
    ///
    /// It goes through all resource priority queues and peeks of
    /// the priority of the first in the queue for each non-blocked queue.
    /// We take the maximal priority of all these priorities to get "current_priority"
    ///
    /// Then we go through all tasks with this priority (technically it is a prefix of each
    /// resource queue, because they are sorted by priorities) and we try to find
    /// resources for it in the allocator. If the allocation failed, we mark the
    /// whole queue as blocked and skip rest of the queue because if we cannot schedule one task
    /// we cannot schedule any task in the queue.
    ///
    /// The function always explore only the current highest non-blocked priority.
    /// and it expects that it is called again when it returns `false`.
    fn try_start_tasks_helper(
        &mut self,
        _task_map: &TaskMap,
        out: &mut Vec<(TaskId, Rc<Allocation>, usize)>,
    ) -> bool {
        let current_priority: QueuePriorityTuple = if let Some(Some(priority)) =
            self.queues.values().map(|qfr| qfr.current_priority()).max()
        {
            priority
        } else {
            return true;
        };
        for rqv in &self.requests {
            let qfr = self.queues.get_mut(rqv).unwrap();
            while let Some((_task_id, priority)) = qfr.peek() {
                if current_priority != priority {
                    break;
                }
                let (allocation, resource_index) = {
                    if let Some(x) = self.allocator.try_allocate(rqv) {
                        x
                    } else {
                        qfr.set_blocked();
                        break;
                    }
                };
                let task_id = qfr.queue.pop().unwrap().0;
                out.push((task_id, allocation, resource_index));
            }
        }
        false
    }
}
