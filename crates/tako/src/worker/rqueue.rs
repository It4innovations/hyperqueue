use crate::common::resources::map::ResourceMap;
use crate::common::resources::{ResourceAllocation, ResourceDescriptor, ResourceRequest};
use crate::common::Map;
use crate::worker::pool::ResourcePool;
use crate::worker::task::Task;
use crate::worker::taskmap::TaskMap;
use crate::{PriorityTuple, TaskId};
use std::time::Duration;

pub struct ResourceWaitQueue {
    queues: Map<ResourceRequest, priority_queue::PriorityQueue<TaskId, PriorityTuple>>,
    requests: Vec<ResourceRequest>,
    pool: ResourcePool,
}

impl ResourceWaitQueue {
    pub fn new(desc: &ResourceDescriptor, resource_map: &ResourceMap) -> Self {
        ResourceWaitQueue {
            queues: Default::default(),
            requests: Default::default(),
            pool: ResourcePool::new(desc, resource_map),
        }
    }

    pub fn release_allocation(&mut self, allocation: ResourceAllocation) {
        self.pool.release_allocation(allocation);
    }

    pub fn add_task(&mut self, task: &Task) {
        let (queue, priority, task_id) = {
            let priority = task.priority;
            (
                if let Some(queue) = self.queues.get_mut(&task.configuration.resources) {
                    queue
                } else {
                    self.requests.push(task.configuration.resources.clone());

                    let mut requests = std::mem::take(&mut self.requests);
                    // Sort bigger values first
                    requests.sort_unstable_by(|x, y| {
                        y.sort_key(&self.pool)
                            .partial_cmp(&x.sort_key(&self.pool))
                            .unwrap()
                    });
                    self.requests = requests;

                    self.queues
                        .entry(task.configuration.resources.clone())
                        .or_default()
                },
                priority,
                task.id,
            )
        };
        queue.push(task_id, priority);
    }

    pub fn remove_task(&mut self, task_id: TaskId) {
        for queue in self.queues.values_mut() {
            if queue.remove(&task_id).is_some() {
                return;
            }
        }
        panic!("Removing unknown task");
    }

    pub fn try_start_tasks(
        &mut self,
        task_map: &TaskMap,
        remaining_time: Option<Duration>,
    ) -> Vec<(TaskId, ResourceAllocation)> {
        let current_priority: PriorityTuple = if let Some(Some(priority)) =
            self.queues.values().map(|q| q.peek().map(|v| *v.1)).max()
        {
            priority
        } else {
            return Vec::new();
        };
        let mut results: Vec<(TaskId, ResourceAllocation)> = Vec::new();
        for request in &self.requests {
            let queue = self.queues.get_mut(request).unwrap();
            while let Some((&task_id, priority)) = queue.peek() {
                if current_priority != *priority {
                    break;
                }
                let allocation = {
                    if let Some(allocation) = self.pool.try_allocate_resources(
                        &task_map.get(task_id).configuration.resources,
                        remaining_time,
                    ) {
                        allocation
                    } else {
                        break;
                    }
                };
                let task_id = queue.pop().unwrap().0;
                results.push((task_id, allocation));
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use crate::common::resources::descriptor::cpu_descriptor_from_socket_size;
    use crate::common::resources::map::ResourceMap;
    use crate::common::resources::{
        CpuRequest, GenericResourceDescriptor, ResourceDescriptor, ResourceRequest,
    };
    use crate::tests::utils::resources::{cpus_compact, cpus_force_compact, cpus_scatter};
    use crate::worker::rqueue::ResourceWaitQueue;
    use crate::worker::test_util::worker_task;
    use std::time::Duration;

    use crate::worker::test_util::ResourceQueueBuilder as RB;

    #[test]
    fn test_rqueue_resource_priority() {
        let cpus = cpu_descriptor_from_socket_size(1, 5);
        let mut rq: RB = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(10, cpus_scatter(4).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(4).finish(), 1));
        rq.add_task(worker_task(12, cpus_force_compact(4).finish(), 1));

        let mut tasks = rq.start_tasks();
        assert_eq!(tasks.len(), 1);
        assert!(tasks.contains_key(&12));
        assert!(rq.start_tasks().is_empty());
        rq.queue.release_allocation(tasks.remove(&12).unwrap());

        let mut tasks = rq.start_tasks();
        assert_eq!(tasks.len(), 1);
        assert!(tasks.contains_key(&11));
        assert!(rq.start_tasks().is_empty());
        rq.queue.release_allocation(tasks.remove(&11).unwrap());

        let mut tasks = rq.start_tasks();
        assert_eq!(tasks.len(), 1);
        assert!(tasks.contains_key(&10));
        assert!(rq.start_tasks().is_empty());
        rq.queue.release_allocation(tasks.remove(&10).unwrap());
    }

    #[test]
    fn test_rqueue1() {
        let cpus = cpu_descriptor_from_socket_size(3, 5);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(10, cpus_compact(2).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(5).finish(), 1));
        rq.add_task(worker_task(12, cpus_compact(2).finish(), 1));

        let a = rq.start_tasks();
        assert_eq!(a.get(&10).unwrap().cpus.len(), 2);
        assert_eq!(a.get(&11).unwrap().cpus.len(), 5);
        assert_eq!(a.get(&12).unwrap().cpus.len(), 2);
    }

    #[test]
    fn test_rqueue2() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(10, cpus_compact(2).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(1).finish(), 2));
        rq.add_task(worker_task(12, cpus_compact(2).finish(), 2));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
        assert!(map.contains_key(&12));

        assert!(rq.start_tasks().is_empty())
    }

    #[test]
    fn test_rqueue3() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(10, cpus_compact(2).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(1).finish(), 1));
        rq.add_task(worker_task(12, cpus_compact(2).finish(), 2));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(!map.contains_key(&11));
        assert!(map.contains_key(&12));

        let map = rq.start_tasks();
        assert!(map.contains_key(&10));
        assert!(!map.contains_key(&11));
        assert!(!map.contains_key(&12));

        assert!(rq.start_tasks().is_empty())
    }

    #[test]
    fn test_rqueue_time_request() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(
            10,
            ResourceRequest::new(
                CpuRequest::Compact(1),
                Duration::new(10, 0),
                Default::default(),
            ),
            1,
        ));

        assert_eq!(rq.start_tasks_duration(Duration::new(9, 0)).len(), 0);
        assert_eq!(rq.start_tasks_duration(Duration::new(11, 0)).len(), 1);
    }

    #[test]
    fn test_rqueue_time_request_priority1() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(
            10,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(10, 0),
                Default::default(),
            ),
            1,
        ));
        rq.add_task(worker_task(
            11,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(40, 0),
                Default::default(),
            ),
            1,
        ));
        rq.add_task(worker_task(
            12,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(20, 0),
                Default::default(),
            ),
            1,
        ));
        rq.add_task(worker_task(
            13,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(30, 0),
                Default::default(),
            ),
            1,
        ));

        let map = rq.start_tasks_duration(Duration::new(40, 0));
        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&11));
        assert!(map.contains_key(&13));
    }

    #[test]
    fn test_rqueue_time_request_priority2() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(cpus, Vec::new()),
            &Default::default(),
        ));
        rq.add_task(worker_task(
            10,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(10, 0),
                Default::default(),
            ),
            1,
        ));
        rq.add_task(worker_task(
            11,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(40, 0),
                Default::default(),
            ),
            1,
        ));
        rq.add_task(worker_task(
            12,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(20, 0),
                Default::default(),
            ),
            1,
        ));
        rq.add_task(worker_task(
            13,
            ResourceRequest::new(
                CpuRequest::Compact(2),
                Duration::new(30, 0),
                Default::default(),
            ),
            1,
        ));

        let map = rq.start_tasks_duration(Duration::new(30, 0));
        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&12));
        assert!(map.contains_key(&13));
    }

    #[test]
    fn test_rqueue_generic_resource1_priorities() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let generic = vec![
            GenericResourceDescriptor::indices("Res0", 1, 20),
            GenericResourceDescriptor::sum("Res1", 100_000_000),
            GenericResourceDescriptor::indices("Res2", 1, 50),
        ];
        let descriptor = ResourceDescriptor::new(cpus, generic);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &descriptor,
            &ResourceMap::from_vec(vec!["Res0".into(), "Res1".into(), "Res2".into()]),
        ));

        let request: ResourceRequest = cpus_compact(2).add_generic(0, 2).finish();

        rq.add_task(worker_task(10, request, 1));
        rq.add_task(worker_task(11, cpus_compact(4).finish(), 1));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
    }

    #[test]
    fn test_rqueue_generic_resource2_priorities() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let generic = vec![
            GenericResourceDescriptor::indices("Res0", 1, 20),
            GenericResourceDescriptor::sum("Res1", 100_000_000),
            GenericResourceDescriptor::indices("Res2", 1, 50),
        ];
        let descriptor = ResourceDescriptor::new(cpus, generic);

        let mut rq = RB::new(ResourceWaitQueue::new(
            &descriptor,
            &ResourceMap::from_vec(vec!["Res0".into(), "Res1".into(), "Res2".into()]),
        ));

        let request: ResourceRequest = cpus_compact(2).add_generic(0, 8).finish();
        rq.add_task(worker_task(10, request, 1));

        let request: ResourceRequest = cpus_compact(2).add_generic(0, 12).finish();
        rq.add_task(worker_task(11, request, 1));

        let request: ResourceRequest = cpus_compact(2).add_generic(1, 50_000_000).finish();
        rq.add_task(worker_task(12, request, 1));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
        assert!(map.contains_key(&12));
    }

    #[test]
    fn test_rqueue_generic_resource3_priorities() {
        let cpus = cpu_descriptor_from_socket_size(1, 4);
        let generic = vec![
            GenericResourceDescriptor::indices("Res0", 1, 20),
            GenericResourceDescriptor::sum("Res1", 100_000_000),
            GenericResourceDescriptor::indices("Res2", 1, 50),
        ];
        let descriptor = ResourceDescriptor::new(cpus, generic);

        let mut rq = RB::new(ResourceWaitQueue::new(
            &descriptor,
            &ResourceMap::from_vec(vec!["Res0".into(), "Res1".into(), "Res2".into()]),
        ));

        let request: ResourceRequest = cpus_compact(2).add_generic(0, 18).finish();
        rq.add_task(worker_task(10, request, 1));

        let request: ResourceRequest = cpus_compact(2)
            .add_generic(0, 10)
            .add_generic(1, 60_000_000)
            .finish();
        rq.add_task(worker_task(11, request, 1));

        let request: ResourceRequest = cpus_compact(2).add_generic(1, 99_000_000).finish();
        rq.add_task(worker_task(12, request, 1));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
        assert!(!map.contains_key(&12));
    }
}
