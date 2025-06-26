use crate::internal::common::resources::{
    ResourceDescriptor, ResourceRequest, ResourceRequestVariants,
};
use crate::internal::tests::utils::resources::{ResBuilder, ra_builder};
use crate::internal::tests::utils::resources::{ResourceRequestBuilder, cpus_compact};
use crate::internal::worker::rqueue::ResourceWaitQueue;
use crate::internal::worker::test_util::{WorkerTaskBuilder, worker_task};
use std::ops::Deref;
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
    let mut rq = RB::new(wait_queue(ResourceDescriptor::simple_cpus(4)));

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
    let mut rq = RB::new(wait_queue(ResourceDescriptor::simple_cpus(4)));

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
    let mut rq = RB::new(wait_queue(ResourceDescriptor::simple_cpus(4)));
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
    let mut rq = RB::new(wait_queue(ResourceDescriptor::simple_cpus(4)));
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
    let mut rq = RB::new(wait_queue(ResourceDescriptor::simple_cpus(4)));
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
            n_resources: ra_builder(&[2, 0]).deref().clone(),
        }),
    );

    assert_eq!(rq.resource_priority(&rq1), 0);
    assert_eq!(rq.resource_priority(&rq2), 1);
    assert_eq!(rq.resource_priority(&rq3), 1);

    rq.new_worker(
        401.into(),
        WorkerResources::from_transport(WorkerResourceCounts {
            n_resources: ra_builder(&[2, 2]).deref().clone(),
        }),
    );
    assert_eq!(rq.resource_priority(&rq1), 0);
    assert_eq!(rq.resource_priority(&rq2), 2);
    assert_eq!(rq.resource_priority(&rq3), 1);

    for i in 500..540 {
        rq.new_worker(
            WorkerId::new(i),
            WorkerResources::from_transport(WorkerResourceCounts {
                n_resources: ra_builder(&[3, 0]).deref().clone(),
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
            n_resources: ra_builder(&[16, 2, 0, 1]).deref().clone(),
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
            n_resources: ra_builder(&[16, 2, 0, 1]).deref().clone(),
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
fn test_different_resources_and_priorities() {
    let resources = vec![
        ResourceDescriptorItem::range("cpus", 0, 63),
        ResourceDescriptorItem::range("gpus/nvidia", 0, 3),
    ];
    let mut rq = RB::new(wait_queue(resources));

    for i in 0..20 {
        let request: ResourceRequest = cpus_compact(1).add(1, 1).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i)
                .resources(request)
                .user_priority(if i % 2 == 0 { 0 } else { -1 })
                .build(),
        );
    }
    for i in 0..12 {
        let request: ResourceRequest = cpus_compact(16).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i + 20)
                .resources(request)
                .user_priority(-3)
                .build(),
        );
    }
    let map = rq.start_tasks();
    assert_eq!(map.len(), 7);
    let ids = map.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        ids.into_iter()
            .map(|x| if x >= 20 { 1 } else { 0 })
            .sum::<usize>(),
        3
    );
}

#[test]
fn test_different_resources_and_priorities1() {
    let resources = vec![
        ResourceDescriptorItem::range("cpus", 0, 63),
        ResourceDescriptorItem::range("gpus/nvidia", 0, 3),
    ];
    let mut rq = RB::new(wait_queue(resources));

    for i in 0..20 {
        let request: ResourceRequest = cpus_compact(1).add(1, 1).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i)
                .resources(request)
                .user_priority(if i % 2 == 0 { 0 } else { -1 })
                .build(),
        );
    }
    for i in 0..12 {
        let request: ResourceRequest = cpus_compact(16).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i + 20)
                .resources(request)
                .user_priority(-3)
                .build(),
        );
    }
    let map = rq.start_tasks();
    assert_eq!(map.len(), 7);
    let ids = map.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        ids.into_iter()
            .map(|x| if x >= 20 { 1 } else { 0 })
            .sum::<usize>(),
        3
    );
}

#[test]
fn test_different_resources_and_priorities2() {
    let resources = vec![
        ResourceDescriptorItem::range("cpus", 0, 10),
        ResourceDescriptorItem::range("foo", 1, 3),
    ];
    let mut rq = RB::new(wait_queue(resources));

    for i in 0..6 {
        let request: ResourceRequest = cpus_compact(1).add(1, 1).finish();
        rq.add_task(WorkerTaskBuilder::new(i).resources(request).build());
    }
    let map = rq.start_tasks();
    assert_eq!(map.len(), 3);
    for i in 0..6 {
        let request: ResourceRequest = cpus_compact(1).add(1, 1).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i + 10)
                .resources(request)
                .user_priority(1)
                .build(),
        );
    }
    let map = rq.start_tasks();
    assert!(map.is_empty());
    for i in 0..6 {
        let request: ResourceRequest = cpus_compact(5).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i + 20)
                .resources(request)
                .user_priority(-3)
                .build(),
        );
    }
    let map = rq.start_tasks();
    assert_eq!(map.len(), 1);
    assert!(map.keys().all(|id| *id >= 20));
}

#[test]
fn test_different_resources_and_priorities3() {
    let resources = vec![
        ResourceDescriptorItem::range("cpus", 0, 9),
        ResourceDescriptorItem::range("foo", 1, 3),
    ];
    let mut rq = RB::new(wait_queue(resources));

    for i in 0..6 {
        let request: ResourceRequest = cpus_compact(1).add(1, 2).finish();
        rq.add_task(WorkerTaskBuilder::new(i).resources(request).build());
    }
    let map = rq.start_tasks();
    assert_eq!(map.len(), 1);
    for i in 0..6 {
        let request: ResourceRequest = cpus_compact(1).add(1, 3).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i + 10)
                .resources(request)
                .user_priority(1)
                .build(),
        );
    }
    let map = rq.start_tasks();
    assert!(map.is_empty());
    for i in 0..6 {
        let request: ResourceRequest = cpus_compact(2).finish();
        rq.add_task(
            WorkerTaskBuilder::new(i + 20)
                .resources(request)
                .user_priority(-3)
                .build(),
        );
    }
    let map = rq.start_tasks();
    assert_eq!(map.len(), 4);
    assert!(map.keys().all(|id| *id >= 20));
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
            n_resources: ra_builder(&[16, 2, 0, 1]).deref().clone(),
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
        Self {
            resources: value,
            coupling: None,
        }
    }
}
