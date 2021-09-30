use std::cmp::Reverse;

use crate::common::resources::{ResourceAllocation, ResourceDescriptor, ResourceRequest};
use crate::common::Map;
use crate::worker::pool::ResourcePool;
use crate::worker::task::TaskRef;
use crate::PriorityTuple;
use std::time::Duration;

pub struct ResourceWaitQueue {
    queues: Map<ResourceRequest, priority_queue::PriorityQueue<TaskRef, PriorityTuple>>,
    requests: Vec<ResourceRequest>,
    pool: ResourcePool,
}

impl ResourceWaitQueue {
    pub fn new(desc: &ResourceDescriptor) -> Self {
        ResourceWaitQueue {
            queues: Default::default(),
            requests: Default::default(),
            pool: ResourcePool::new(desc),
        }
    }

    pub fn release_allocation(&mut self, allocation: ResourceAllocation) {
        self.pool.release_allocation(allocation);
    }

    pub fn add_task(&mut self, task_ref: TaskRef) {
        let (queue, priority) = {
            let task = task_ref.get();
            let priority = task.priority;
            (
                if let Some(queue) = self.queues.get_mut(&task.configuration.resources) {
                    queue
                } else {
                    self.requests.push(task.configuration.resources.clone());
                    self.requests
                        .sort_unstable_by_key(|r| Reverse(r.sort_key()));
                    self.queues
                        .insert(task.configuration.resources.clone(), Default::default());
                    self.queues.get_mut(&task.configuration.resources).unwrap()
                },
                priority,
            )
        };
        queue.push(task_ref, priority);
    }

    pub fn remove_task(&mut self, task_ref: &TaskRef) {
        for queue in self.queues.values_mut() {
            if queue.remove(task_ref).is_some() {
                return;
            }
        }
        panic!("Removing unknown task");
    }

    pub fn try_start_tasks(
        &mut self,
        remaining_time: Option<Duration>,
    ) -> Vec<(TaskRef, ResourceAllocation)> {
        let current_priority: PriorityTuple = if let Some(Some(priority)) =
            self.queues.values().map(|q| q.peek().map(|v| *v.1)).max()
        {
            priority
        } else {
            return Vec::new();
        };
        let mut results: Vec<(TaskRef, ResourceAllocation)> = Vec::new();
        for request in &self.requests {
            let queue = self.queues.get_mut(request).unwrap();
            while let Some((task_ref, priority)) = queue.peek() {
                if current_priority != *priority {
                    break;
                }
                let allocation = {
                    if let Some(allocation) = self.pool.try_allocate_resources(
                        &task_ref.get().configuration.resources,
                        remaining_time,
                    ) {
                        allocation
                    } else {
                        break;
                    }
                };
                let task_ref = queue.pop().unwrap().0;
                results.push((task_ref, allocation));
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use crate::common::resources::{
        CpuRequest, ResourceAllocation, ResourceDescriptor, ResourceRequest,
    };
    use crate::common::Map;
    use crate::worker::rqueue::ResourceWaitQueue;
    use crate::worker::test_util::worker_task;
    use crate::TaskId;
    use std::time::Duration;

    fn start_tasks_map(rq: &mut ResourceWaitQueue) -> Map<TaskId, ResourceAllocation> {
        rq.try_start_tasks(None)
            .into_iter()
            .map(|(t, a)| (t.get().id, a))
            .collect()
    }

    fn start_tasks_map_time(
        rq: &mut ResourceWaitQueue,
        remaining_time: Duration,
    ) -> Map<TaskId, ResourceAllocation> {
        rq.try_start_tasks(Some(remaining_time))
            .into_iter()
            .map(|(t, a)| (t.get().id, a))
            .collect()
    }

    #[test]
    fn test_rqueue_resource_priority() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(1, 5));
        let t = worker_task(10, CpuRequest::Scatter(4).into(), 1);
        rq.add_task(t);
        let t = worker_task(11, CpuRequest::Compact(4).into(), 1);
        rq.add_task(t);
        let t = worker_task(12, CpuRequest::ForceCompact(4).into(), 1);
        rq.add_task(t);

        let mut tasks = rq.try_start_tasks(None);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0.get().id, 12);
        assert!(rq.try_start_tasks(None).is_empty());
        rq.release_allocation(std::mem::take(&mut tasks[0].1));

        let mut tasks = rq.try_start_tasks(None);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0.get().id, 11);
        assert!(rq.try_start_tasks(None).is_empty());
        rq.release_allocation(std::mem::take(&mut tasks[0].1));

        let mut tasks = rq.try_start_tasks(None);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0.get().id, 10);
        assert!(rq.try_start_tasks(None).is_empty());
        rq.release_allocation(std::mem::take(&mut tasks[0].1));
    }

    #[test]
    fn test_rqueue1() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(3, 5));
        let t = worker_task(10, CpuRequest::Compact(2).into(), 1);
        rq.add_task(t);
        let t = worker_task(11, CpuRequest::Compact(5).into(), 1);
        rq.add_task(t);
        let t = worker_task(12, CpuRequest::Compact(2).into(), 1);
        rq.add_task(t);

        let a = start_tasks_map(&mut rq);
        assert_eq!(a.get(&10).unwrap().cpus.len(), 2);
        assert_eq!(a.get(&11).unwrap().cpus.len(), 5);
        assert_eq!(a.get(&12).unwrap().cpus.len(), 2);
    }

    #[test]
    fn test_rqueue2() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(1, 4));
        let t = worker_task(10, CpuRequest::Compact(2).into(), 1);
        rq.add_task(t);
        let t = worker_task(11, CpuRequest::Compact(1).into(), 2);
        rq.add_task(t);
        let t = worker_task(12, CpuRequest::Compact(2).into(), 2);
        rq.add_task(t);

        let a = start_tasks_map(&mut rq);
        assert!(!a.contains_key(&10));
        assert!(a.contains_key(&11));
        assert!(a.contains_key(&12));

        let a = start_tasks_map(&mut rq);
        assert!(a.is_empty())
    }

    #[test]
    fn test_rqueue3() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(1, 4));
        let t = worker_task(10, CpuRequest::Compact(2).into(), 1);
        rq.add_task(t);
        let t = worker_task(11, CpuRequest::Compact(1).into(), 1);
        rq.add_task(t);
        let t = worker_task(12, CpuRequest::Compact(2).into(), 2);
        rq.add_task(t);

        let a = start_tasks_map(&mut rq);
        assert!(!a.contains_key(&10));
        assert!(!a.contains_key(&11));
        assert!(a.contains_key(&12));

        let a = start_tasks_map(&mut rq);
        assert!(a.contains_key(&10));
        assert!(!a.contains_key(&11));
        assert!(!a.contains_key(&12));

        let a = start_tasks_map(&mut rq);
        assert!(a.is_empty())
    }

    #[test]
    fn test_rqueue_time_request() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(1, 4));
        let t = worker_task(
            10,
            ResourceRequest::new(CpuRequest::Compact(1), Duration::new(10, 0)),
            1,
        );
        rq.add_task(t);

        let tasks = rq.try_start_tasks(Some(Duration::new(9, 0)));
        assert_eq!(tasks.len(), 0);

        let tasks = rq.try_start_tasks(Some(Duration::new(11, 0)));
        assert_eq!(tasks.len(), 1);
    }

    #[test]
    fn test_rqueue_time_request_priority1() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(1, 4));
        let t = worker_task(
            10,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(10, 0)),
            1,
        );
        rq.add_task(t);
        let t = worker_task(
            11,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(40, 0)),
            1,
        );
        rq.add_task(t);
        let t = worker_task(
            12,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(20, 0)),
            1,
        );
        rq.add_task(t);
        let t = worker_task(
            13,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(30, 0)),
            1,
        );
        rq.add_task(t);

        let a = start_tasks_map_time(&mut rq, Duration::new(40, 0));
        assert_eq!(a.len(), 2);
        assert!(a.contains_key(&11));
        assert!(a.contains_key(&13));
    }

    #[test]
    fn test_rqueue_time_request_priority2() {
        let mut rq = ResourceWaitQueue::new(&ResourceDescriptor::new_with_socket_size(1, 4));
        let t = worker_task(
            10,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(10, 0)),
            1,
        );
        rq.add_task(t);
        let t = worker_task(
            11,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(40, 0)),
            1,
        );
        rq.add_task(t);
        let t = worker_task(
            12,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(20, 0)),
            1,
        );
        rq.add_task(t);
        let t = worker_task(
            13,
            ResourceRequest::new(CpuRequest::Compact(2), Duration::new(30, 0)),
            1,
        );
        rq.add_task(t);

        let a = start_tasks_map_time(&mut rq, Duration::new(30, 0));
        assert_eq!(a.len(), 2);
        assert!(a.contains_key(&12));
        assert!(a.contains_key(&13));
    }
}
