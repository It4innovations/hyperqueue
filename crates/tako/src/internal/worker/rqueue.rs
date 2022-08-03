use crate::internal::common::resources::map::ResourceMap;
use crate::internal::common::resources::{Allocation, ResourceDescriptor, ResourceRequest};
use crate::internal::common::Map;
use crate::internal::worker::allocator::ResourceAllocator;
use crate::internal::worker::state::TaskMap;
use crate::internal::worker::task::Task;
use crate::{PriorityTuple, TaskId};
use std::time::Duration;

pub struct ResourceWaitQueue {
    queues: Map<ResourceRequest, priority_queue::PriorityQueue<TaskId, PriorityTuple>>,
    requests: Vec<ResourceRequest>,
    allocator: ResourceAllocator,
}

impl ResourceWaitQueue {
    pub fn new(desc: &ResourceDescriptor, resource_map: &ResourceMap) -> Self {
        ResourceWaitQueue {
            queues: Default::default(),
            requests: Default::default(),
            allocator: ResourceAllocator::new(desc, resource_map),
        }
    }

    pub fn release_allocation(&mut self, allocation: Allocation) {
        self.allocator.release_allocation(allocation);
    }

    pub fn add_task(&mut self, task: &Task) {
        let (queue, priority, task_id) = {
            let priority = task.priority;
            (
                if let Some(queue) = self.queues.get_mut(&task.resources) {
                    queue
                } else {
                    self.requests.push(task.resources.clone());

                    let mut requests = std::mem::take(&mut self.requests);
                    // Sort bigger values first
                    requests.sort_unstable_by(|x, y| {
                        y.sort_key(&self.allocator)
                            .partial_cmp(&x.sort_key(&self.allocator))
                            .unwrap()
                    });
                    self.requests = requests;

                    self.queues.entry(task.resources.clone()).or_default()
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
    ) -> Vec<(TaskId, Allocation)> {
        self.allocator.init_allocator(remaining_time);
        let mut out = Vec::new();
        while !self.try_start_tasks_helper(task_map, &mut out) {
            self.allocator.close_priority_level()
        }
        out
    }

    fn try_start_tasks_helper(
        &mut self,
        task_map: &TaskMap,
        out: &mut Vec<(TaskId, Allocation)>,
    ) -> bool {
        let current_priority: PriorityTuple = if let Some(Some(priority)) =
            self.queues.values().map(|q| q.peek().map(|v| *v.1)).max()
        {
            priority
        } else {
            return true;
        };
        let mut is_finished = true;
        for request in &self.requests {
            let queue = self.queues.get_mut(request).unwrap();
            while let Some((&task_id, priority)) = queue.peek() {
                if current_priority != *priority {
                    break;
                }
                let allocation = {
                    if let Some(allocation) = self
                        .allocator
                        .try_allocate(&task_map.get(&task_id).resources)
                    {
                        allocation
                    } else {
                        break;
                    }
                };
                let task_id = queue.pop().unwrap().0;
                out.push((task_id, allocation));
                is_finished = false;
            }
        }
        is_finished
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::map::ResourceMap;
    use crate::internal::common::resources::{ResourceDescriptor, ResourceRequest};
    use crate::internal::tests::utils::resources::cpus_compact;
    use crate::internal::tests::utils::resources::ResBuilder;
    use crate::internal::worker::rqueue::ResourceWaitQueue;
    use crate::internal::worker::test_util::worker_task;
    use std::time::Duration;

    use crate::internal::worker::test_util::ResourceQueueBuilder as RB;
    use crate::resources::{ResourceDescriptorItem, ResourceDescriptorKind};
    use crate::{Map, PriorityTuple, TaskId};

    impl ResourceWaitQueue {
        pub fn queues(
            &self,
        ) -> &Map<ResourceRequest, priority_queue::PriorityQueue<TaskId, PriorityTuple>> {
            &self.queues
        }
        pub fn requests(&self) -> &[ResourceRequest] {
            &self.requests
        }
    }

    #[test]
    fn test_rqueue_resource_priority() {
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::new(vec![ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::groups(vec![
                    vec![0.into(), 1.into(), 2.into(), 3.into()],
                    vec![7.into(), 8.into()],
                ])
                .unwrap(),
            }]),
            &ResourceMap::from_ref(&["cpus"]),
        ));

        rq.add_task(worker_task(
            10,
            ResBuilder::default().add_scatter(0, 3).finish(),
            1,
        ));
        rq.add_task(worker_task(11, cpus_compact(4).finish(), 1));
        rq.add_task(worker_task(
            12,
            ResBuilder::default().add_force_compact(0, 4).finish(),
            1,
        ));

        let mut a = rq.start_tasks();
        assert!(!a.contains_key(&10));
        assert!(!a.contains_key(&11));
        assert!(a.contains_key(&12));

        let tasks = rq.start_tasks();
        assert!(tasks.is_empty());
        rq.queue.release_allocation(a.remove(&12).unwrap());

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
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::sockets(3, 5),
            &ResourceMap::from_ref(&["cpus"]),
        ));
        rq.add_task(worker_task(10, cpus_compact(2).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(5).finish(), 1));
        rq.add_task(worker_task(12, cpus_compact(2).finish(), 1));

        let a = rq.start_tasks();
        assert_eq!(a.get(&10).unwrap().get_indices(0).len(), 2);
        assert_eq!(a.get(&11).unwrap().get_indices(0).len(), 5);
        assert_eq!(a.get(&12).unwrap().get_indices(0).len(), 2);
    }

    #[test]
    fn test_rqueue2() {
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::simple(4),
            &ResourceMap::from_ref(&["cpus"]),
        ));

        rq.add_task(worker_task(10, cpus_compact(2).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(1).finish(), 2));
        rq.add_task(worker_task(12, cpus_compact(2).finish(), 2));

        let a = rq.start_tasks();
        assert!(!a.contains_key(&10));
        assert!(a.contains_key(&11));
        assert!(a.contains_key(&12));
        assert!(rq.start_tasks().is_empty());
    }

    #[test]
    fn test_rqueue3() {
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::simple(4),
            &ResourceMap::from_ref(&["cpus"]),
        ));

        rq.add_task(worker_task(10, cpus_compact(2).finish(), 1));
        rq.add_task(worker_task(11, cpus_compact(1).finish(), 1));
        rq.add_task(worker_task(12, cpus_compact(2).finish(), 2));

        let a = rq.start_tasks();
        assert!(a.contains_key(&10));
        assert!(!a.contains_key(&11));
        assert!(a.contains_key(&12));
    }

    #[test]
    fn test_rqueue_time_request() {
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::simple(4),
            &ResourceMap::from_ref(&["cpus"]),
        ));
        rq.add_task(worker_task(
            10,
            ResBuilder::default().add(0, 1).min_time_secs(10).finish(),
            1,
        ));

        assert_eq!(rq.start_tasks_duration(Duration::new(9, 0)).len(), 0);
        assert_eq!(rq.start_tasks_duration(Duration::new(11, 0)).len(), 1);
    }

    #[test]
    fn test_rqueue_time_request_priority1() {
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::simple(4),
            &ResourceMap::from_ref(&["cpus"]),
        ));
        rq.add_task(worker_task(
            10,
            cpus_compact(2).min_time_secs(10).finish(),
            1,
        ));
        rq.add_task(worker_task(
            11,
            cpus_compact(2).min_time_secs(40).finish(),
            1,
        ));
        rq.add_task(worker_task(
            12,
            cpus_compact(2).min_time_secs(20).finish(),
            1,
        ));
        rq.add_task(worker_task(
            13,
            cpus_compact(2).min_time_secs(30).finish(),
            1,
        ));

        let map = rq.start_tasks_duration(Duration::new(40, 0));
        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&11));
        assert!(map.contains_key(&13));
    }

    #[test]
    fn test_rqueue_time_request_priority2() {
        let mut rq = RB::new(ResourceWaitQueue::new(
            &ResourceDescriptor::simple(4),
            &ResourceMap::from_ref(&["cpus"]),
        ));
        rq.add_task(worker_task(
            10,
            cpus_compact(2).min_time_secs(10).finish(),
            1,
        ));
        rq.add_task(worker_task(
            11,
            cpus_compact(2).min_time_secs(40).finish(),
            1,
        ));
        rq.add_task(worker_task(
            12,
            cpus_compact(2).min_time_secs(20).finish(),
            1,
        ));
        rq.add_task(worker_task(
            13,
            cpus_compact(2).min_time_secs(30).finish(),
            1,
        ));

        let map = rq.start_tasks_duration(Duration::new(30, 0));
        assert_eq!(map.len(), 2);
        assert!(map.contains_key(&12));
        assert!(map.contains_key(&13));
    }

    #[test]
    fn test_rqueue_generic_resource1_priorities() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 3),
            ResourceDescriptorItem::range("Res0", 1, 20),
            ResourceDescriptorItem::sum("Res1", 100_000_000),
            ResourceDescriptorItem::range("Res2", 1, 50),
        ];
        let descriptor = ResourceDescriptor::new(resources);
        let mut rq = RB::new(ResourceWaitQueue::new(
            &descriptor,
            &ResourceMap::from_ref(&["cpus", "Res0", "Res1", "Res2"]),
        ));

        let request: ResourceRequest = cpus_compact(2).add(1, 2).finish();

        rq.add_task(worker_task(10, request, 1));
        rq.add_task(worker_task(11, cpus_compact(4).finish(), 1));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
    }

    #[test]
    fn test_rqueue_generic_resource2_priorities() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 3),
            ResourceDescriptorItem::range("Res0", 1, 20),
            ResourceDescriptorItem::sum("Res1", 100_000_000),
            ResourceDescriptorItem::range("Res2", 1, 50),
        ];
        let descriptor = ResourceDescriptor::new(resources);

        let mut rq = RB::new(ResourceWaitQueue::new(
            &descriptor,
            &ResourceMap::from_ref(&["cpus", "Res0", "Res1", "Res2"]),
        ));

        let request: ResourceRequest = cpus_compact(2).add(1, 8).finish();
        rq.add_task(worker_task(10, request, 1));

        let request: ResourceRequest = cpus_compact(2).add(1, 12).finish();
        rq.add_task(worker_task(11, request, 1));

        let request: ResourceRequest = cpus_compact(2).add(2, 50_000_000).finish();
        rq.add_task(worker_task(12, request, 1));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
        assert!(map.contains_key(&12));
    }

    #[test]
    fn test_rqueue_generic_resource3_priorities() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 3),
            ResourceDescriptorItem::range("Res0", 1, 20),
            ResourceDescriptorItem::sum("Res1", 100_000_000),
            ResourceDescriptorItem::range("Res2", 1, 50),
        ];
        let descriptor = ResourceDescriptor::new(resources);

        let mut rq = RB::new(ResourceWaitQueue::new(
            &descriptor,
            &ResourceMap::from_ref(&["cpus", "Res0", "Res1", "Res2"]),
        ));

        let request: ResourceRequest = cpus_compact(2).add(1, 18).finish();
        rq.add_task(worker_task(10, request, 1));

        let request: ResourceRequest = cpus_compact(2).add(1, 10).add(2, 60_000_000).finish();
        rq.add_task(worker_task(11, request, 1));

        let request: ResourceRequest = cpus_compact(2).add(2, 99_000_000).finish();
        rq.add_task(worker_task(12, request, 1));

        let map = rq.start_tasks();
        assert!(!map.contains_key(&10));
        assert!(map.contains_key(&11));
        assert!(!map.contains_key(&12));
    }
}
