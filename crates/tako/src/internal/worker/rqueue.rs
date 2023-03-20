use crate::internal::common::resources::{Allocation, ResourceRequestVariants};
use crate::internal::common::Map;
use crate::internal::server::workerload::WorkerResources;
use crate::internal::worker::resources::allocator::ResourceAllocator;
use crate::internal::worker::state::TaskMap;
use crate::internal::worker::task::Task;
use crate::{Priority, PriorityTuple, Set, TaskId, WorkerId};
use priority_queue::PriorityQueue;
use std::time::Duration;

type QueuePriorityTuple = (Priority, Priority, Priority); // user priority, resource priority, scheduler priority

#[derive(Debug)]
pub(crate) struct QueueForRequest {
    resource_priority: Priority,
    queue: priority_queue::PriorityQueue<TaskId, PriorityTuple>,
}

impl QueueForRequest {
    pub fn current_priority(&self) -> Option<QueuePriorityTuple> {
        self.peek().map(|x| x.1)
    }

    pub fn peek(&self) -> Option<(TaskId, QueuePriorityTuple)> {
        self.queue
            .peek()
            .map(|(task_id, priority)| (*task_id, (priority.0, self.resource_priority, priority.1)))
    }
}

pub struct ResourceWaitQueue {
    queues: Map<ResourceRequestVariants, QueueForRequest>,
    requests: Vec<ResourceRequestVariants>,
    allocator: ResourceAllocator,
    worker_resources: Map<WorkerResources, Set<WorkerId>>,
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

    pub fn release_allocation(&mut self, allocation: Allocation) {
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
    ) -> Vec<(TaskId, Allocation, usize)> {
        self.allocator.init_allocator(remaining_time);
        let mut out = Vec::new();
        while !self.try_start_tasks_helper(task_map, &mut out) {
            self.allocator.close_priority_level()
        }
        out
    }

    fn try_start_tasks_helper(
        &mut self,
        _task_map: &TaskMap,
        out: &mut Vec<(TaskId, Allocation, usize)>,
    ) -> bool {
        let current_priority: QueuePriorityTuple = if let Some(Some(priority)) =
            self.queues.values().map(|qfr| qfr.current_priority()).max()
        {
            priority
        } else {
            return true;
        };
        let mut is_finished = true;
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
                        break;
                    }
                };
                let task_id = qfr.queue.pop().unwrap().0;
                out.push((task_id, allocation, resource_index));
                is_finished = false;
            }
        }
        is_finished
    }
}

#[cfg(test)]
mod tests {
    use crate::internal::common::resources::{
        ResourceDescriptor, ResourceRequest, ResourceRequestVariants,
    };
    use crate::internal::tests::utils::resources::ResBuilder;
    use crate::internal::tests::utils::resources::{cpus_compact, ResourceRequestBuilder};
    use crate::internal::worker::rqueue::ResourceWaitQueue;
    use crate::internal::worker::test_util::{worker_task, WorkerTaskBuilder};
    use std::time::Duration;

    use crate::internal::messages::worker::WorkerResourceCounts;
    use crate::internal::server::workerload::WorkerResources;
    use crate::internal::tests::utils::shared::{
        res_allocator_from_descriptor, res_item, res_kind_groups, res_kind_list, res_kind_range,
    };
    use crate::internal::worker::test_util::ResourceQueueBuilder as RB;
    use crate::resources::ResourceDescriptorItem;
    use crate::{Map, Set, WorkerId};

    impl ResourceWaitQueue {
        pub fn requests(&self) -> &[ResourceRequestVariants] {
            &self.requests
        }

        pub fn worker_resources(&self) -> &Map<WorkerResources, Set<WorkerId>> {
            &self.worker_resources
        }
    }

    #[test]
    fn test_rqueue_resource_priority() {
        let mut rq = RB::new(wait_queue(vec![res_item(
            "cpus",
            res_kind_groups(&[vec!["0", "1", "2", "3"], vec!["7", "8"]]),
        )]));

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
        let mut rq = RB::new(wait_queue(ResourceDescriptor::sockets(3, 5)));
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
        let mut rq = RB::new(wait_queue(ResourceDescriptor::simple(4)));

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
        let mut rq = RB::new(wait_queue(ResourceDescriptor::simple(4)));

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
        let mut rq = RB::new(wait_queue(ResourceDescriptor::simple(4)));
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
        let mut rq = RB::new(wait_queue(ResourceDescriptor::simple(4)));
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
        let mut rq = RB::new(wait_queue(ResourceDescriptor::simple(4)));
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
        let mut rq = RB::new(wait_queue(resources));

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

        let mut rq = RB::new(wait_queue(resources));

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

        let mut rq = RB::new(wait_queue(resources));

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

    #[test]
    fn test_worker_resource_priorities() {
        let r1 = ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: res_kind_range(0, 4),
        };
        let r2 = ResourceDescriptorItem {
            name: "res1".to_string(),
            kind: res_kind_list(&["2", "3", "4"]),
        };
        let mut rq = wait_queue(vec![r1, r2]);

        let rq1 = ResourceRequestBuilder::default().cpus(1).finish_v();
        let rq2 = ResourceRequestBuilder::default().cpus(3).finish_v();
        let rq3 = ResourceRequestBuilder::default()
            .cpus(1)
            .add(1, 1)
            .finish_v();

        assert_eq!(rq.resource_priority(&rq1), 0);
        assert_eq!(rq.resource_priority(&rq2), 0);
        assert_eq!(rq.resource_priority(&rq3), 0);

        rq.new_worker(
            400.into(),
            WorkerResources::from_transport(WorkerResourceCounts {
                n_resources: vec![2, 0],
            }),
        );

        assert_eq!(rq.resource_priority(&rq1), 0);
        assert_eq!(rq.resource_priority(&rq2), 1);
        assert_eq!(rq.resource_priority(&rq3), 1);

        rq.new_worker(
            401.into(),
            WorkerResources::from_transport(WorkerResourceCounts {
                n_resources: vec![2, 2],
            }),
        );
        assert_eq!(rq.resource_priority(&rq1), 0);
        assert_eq!(rq.resource_priority(&rq2), 2);
        assert_eq!(rq.resource_priority(&rq3), 1);

        for i in 500..540 {
            rq.new_worker(
                WorkerId::new(i),
                WorkerResources::from_transport(WorkerResourceCounts {
                    n_resources: vec![3, 0],
                }),
            );
        }
        assert_eq!(rq.resource_priority(&rq1), 0);
        assert_eq!(rq.resource_priority(&rq2), 2);
        assert_eq!(rq.resource_priority(&rq3), 41);

        rq.remove_worker(504.into());
        assert_eq!(rq.resource_priority(&rq1), 0);
        assert_eq!(rq.resource_priority(&rq2), 2);
        assert_eq!(rq.resource_priority(&rq3), 40);
    }

    #[test]
    fn test_uniq_resource_priorities1() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 16),
            ResourceDescriptorItem::range("res0", 1, 10),
            ResourceDescriptorItem::range("res1", 1, 2),
        ];

        let mut rq = RB::new(wait_queue(resources));

        let request: ResourceRequest = cpus_compact(16).finish();
        rq.add_task(
            WorkerTaskBuilder::new(10)
                .resources(request)
                .server_priority(1)
                .build(),
        );

        let request: ResourceRequest = cpus_compact(16).add(2, 2).finish();
        rq.add_task(WorkerTaskBuilder::new(11).resources(request).build());

        let map = rq.start_tasks();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&10));
    }

    #[test]
    fn test_uniq_resource_priorities2() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 16),
            ResourceDescriptorItem::range("res0", 1, 10),
            ResourceDescriptorItem::range("res1", 1, 2),
        ];

        let mut rq = RB::new(wait_queue(resources));

        rq.new_worker(
            400.into(),
            WorkerResources::from_transport(WorkerResourceCounts {
                n_resources: vec![16, 2, 0, 1],
            }),
        );

        let request: ResourceRequest = cpus_compact(16).finish();
        rq.add_task(
            WorkerTaskBuilder::new(10)
                .resources(request)
                .server_priority(1)
                .build(),
        );

        let request: ResourceRequest = cpus_compact(16).add(2, 2).finish();
        rq.add_task(WorkerTaskBuilder::new(11).resources(request).build());

        let map = rq.start_tasks();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&11));
    }

    #[test]
    fn test_uniq_resource_priorities3() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 16),
            ResourceDescriptorItem::range("res0", 1, 10),
            ResourceDescriptorItem::range("res1", 1, 2),
        ];

        let mut rq = RB::new(wait_queue(resources));

        rq.new_worker(
            400.into(),
            WorkerResources::from_transport(WorkerResourceCounts {
                n_resources: vec![16, 2, 0, 1],
            }),
        );

        let request: ResourceRequest = cpus_compact(16).finish();
        rq.add_task(
            WorkerTaskBuilder::new(10)
                .resources(request)
                .user_priority(1)
                .build(),
        );

        let request: ResourceRequest = cpus_compact(16).add(2, 2).finish();
        rq.add_task(WorkerTaskBuilder::new(11).resources(request).build());

        let map = rq.start_tasks();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&10));
    }

    #[test]
    fn test_uniq_resource_priorities4() {
        let resources = vec![
            ResourceDescriptorItem::range("cpus", 0, 16),
            ResourceDescriptorItem::range("res0", 1, 10),
            ResourceDescriptorItem::range("res1", 1, 2),
        ];

        let mut rq = RB::new(wait_queue(resources));

        rq.new_worker(
            400.into(),
            WorkerResources::from_transport(WorkerResourceCounts {
                n_resources: vec![16, 2, 0, 1],
            }),
        );

        let request: ResourceRequest = cpus_compact(16).finish();
        rq.add_task(
            WorkerTaskBuilder::new(10)
                .resources(request)
                .server_priority(1)
                .build(),
        );

        rq.queue.remove_worker(400.into());

        let request: ResourceRequest = cpus_compact(16).add(2, 2).finish();
        rq.add_task(WorkerTaskBuilder::new(11).resources(request).build());

        let map = rq.start_tasks();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&10));
    }

    fn wait_queue<Resources: Into<ResourceDescriptor>>(descriptor: Resources) -> ResourceWaitQueue {
        let descriptor: ResourceDescriptor = descriptor.into();
        let allocator = res_allocator_from_descriptor(descriptor);
        ResourceWaitQueue::new(allocator)
    }

    impl From<Vec<ResourceDescriptorItem>> for ResourceDescriptor {
        fn from(value: Vec<ResourceDescriptorItem>) -> Self {
            Self { resources: value }
        }
    }
}
