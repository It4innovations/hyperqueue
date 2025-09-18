use crate::internal::common::resources::descriptor::{ResourceDescriptor, ResourceDescriptorKind};
use crate::internal::common::resources::{Allocation, ResourceId, ResourceRequestVariants};
use crate::internal::tests::utils::resources::{cpus_compact, ResBuilder};
use crate::internal::tests::utils::shared::res_allocator_from_descriptor;
use crate::internal::tests::utils::sorted_vec;
use crate::internal::worker::resources::allocator::{AllocatorStaticInfo, ResourceAllocator};
use crate::internal::worker::resources::concise::{ConciseFreeResources, ConciseResourceState};
use crate::internal::worker::resources::pool::ResourcePool;
use crate::resources::{
    AllocationRequest, ResourceAmount, ResourceDescriptorCoupling, ResourceDescriptorItem,
    ResourceIndex, ResourceUnits,
};
use std::cell::RefCell;

use std::rc::Rc;
use std::time::Duration;

impl ResourceAllocator {
    pub fn get_sockets(&self, allocation: &Allocation, idx: u32) -> Vec<usize> {
        let idx = ResourceId::new(idx);
        let al = allocation.resource_allocation(idx).unwrap();
        self.pools[idx].get_sockets(al)
    }

    pub fn get_current_free(&self, idx: u32) -> ResourceAmount {
        self.pools[idx.into()].current_free()
    }

    pub fn force_claim_from_groups(
        &mut self,
        resource: ResourceId,
        group_idxs: &[usize],
        count: ResourceAmount,
    ) -> Allocation {
        let al = self.pools[resource].claim_resources_with_group_mask(
            resource,
            &AllocationRequest::Compact(count),
            group_idxs,
        );
        let mut allocation = Allocation::new();
        allocation.add_resource_allocation(al);
        self.free_resources.remove(&allocation);
        allocation
    }
}

fn test_static_info(free: ConciseFreeResources) -> AllocatorStaticInfo {
    AllocatorStaticInfo {
        coupling_weights: Vec::new(),
        optional_objectives: RefCell::new(Default::default()),
        all_resources: free,
    }
}

pub fn simple_descriptor(
    n_sockets: ResourceUnits,
    socket_size: ResourceUnits,
) -> ResourceDescriptor {
    ResourceDescriptor::new(
        vec![ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: ResourceDescriptorKind::regular_sockets(n_sockets, socket_size),
        }],
        Default::default(),
    )
}

pub fn test_allocator(descriptor: &ResourceDescriptor) -> ResourceAllocator {
    res_allocator_from_descriptor(descriptor.clone())
}

fn simple_allocator(
    free: &[ResourceUnits],
    running: &[&[ResourceUnits]],
    remaining_time: Option<Duration>,
) -> ResourceAllocator {
    let mut names = vec!["cpus".to_string()];
    names.extend((1..free.len()).map(|i| format!("res{i}")));
    let ds: Vec<_> = free
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut total = *c;
            for r in running {
                if i < r.len() {
                    total += r[i]
                }
            }
            ResourceDescriptorItem {
                name: names[i].clone(),
                kind: ResourceDescriptorKind::simple_indices(total),
            }
        })
        .collect();
    let descriptor = ResourceDescriptor::new(ds, Default::default());

    let mut ac = res_allocator_from_descriptor(descriptor);
    for r in running {
        assert_eq!(r.len(), 1); // As far I see, only 1 element array is now used in tests
        let units = r[0];
        let rq = ResBuilder::default()
            .add(0, ResourceAmount::new_units(units))
            .finish();
        ac.try_allocate_variant(&rq).unwrap();
    }
    ac.reset_temporaries(remaining_time);
    ac
}

fn simple_alloc(allocator: &mut ResourceAllocator, counts: &[ResourceUnits], expect_pass: bool) {
    let mut builder = ResBuilder::default();
    for (i, count) in counts.iter().enumerate() {
        if *count > 0 {
            builder = builder.add(
                ResourceId::from(i as u32),
                ResourceAmount::new_units(*count),
            );
        }
    }
    let al = allocator.try_allocate_variant(&builder.finish());
    if expect_pass {
        al.unwrap();
    } else {
        assert!(al.is_none());
    }
}

#[test]
fn test_compute_blocking_level() {
    let pools = [
        ResourcePool::new(
            &ResourceDescriptorKind::simple_indices(3),
            0.into(),
            &Default::default(),
        ),
        ResourcePool::new(
            &ResourceDescriptorKind::simple_indices(3),
            0.into(),
            &Default::default(),
        ),
    ];
    let free = ConciseFreeResources::new_simple(&[1, 2]);
    let rq = ResBuilder::default().add(0, 3).add(1, 1).finish();
    let sinfo = test_static_info(free.clone());
    assert!(ResourceAllocator::compute_witness(&pools, &free, &rq, [].iter(), &sinfo).is_none());
    assert!(ResourceAllocator::compute_witness(
        &pools,
        &free,
        &rq,
        [Rc::new(Allocation::new_simple(&[1]))].iter(),
        &sinfo,
    )
    .is_none());
    assert_eq!(
        ResourceAllocator::compute_witness(
            &pools,
            &free,
            &rq,
            [
                Rc::new(Allocation::new_simple(&[1])),
                Rc::new(Allocation::new_simple(&[1])),
                Rc::new(Allocation::new_simple(&[1]))
            ]
            .iter(),
            &sinfo,
        ),
        Some(ConciseFreeResources::new_simple(&[3, 2]))
    );

    let free = ConciseFreeResources::new_simple(&[4, 2]);
    assert_eq!(
        ResourceAllocator::compute_witness(&pools, &free, &rq, [].iter(), &sinfo),
        Some(free.clone())
    );

    let rq = ResBuilder::default().add(0, 4).add(1, 4).finish();
    assert_eq!(
        ResourceAllocator::compute_witness(
            &pools,
            &free,
            &rq,
            [
                Rc::new(Allocation::new_simple(&[2])),
                Rc::new(Allocation::new_simple(&[2])),
                Rc::new(Allocation::new_simple(&[2])),
                Rc::new(Allocation::new_simple(&[0, 3])),
                Rc::new(Allocation::new_simple(&[0, 3]))
            ]
            .iter(),
            &sinfo,
        ),
        Some(ConciseFreeResources::new_simple(&[10, 5]))
    );
}

#[test]
fn test_allocator_single_socket() {
    let mut allocator = simple_allocator(&[4], &[], None);

    simple_alloc(&mut allocator, &[3], true);
    allocator.free_resources.assert_eq(&[1]);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    simple_alloc(&mut allocator, &[1], true);
    allocator.free_resources.assert_eq(&[0]);

    allocator.validate();
}

#[test]
fn test_allocator_priority_check1() {
    // Running: [1,1,1], free: [1], try: [2p1] [1p0]
    let mut allocator = simple_allocator(&[1], &[&[1], &[1], &[1]], None);
    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);
    allocator.validate();

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[1], false);
    allocator.free_resources.assert_eq(&[1]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check2() {
    // Running: [1,1,1], free: [1], try: [2p0] [1p0]
    let mut allocator = simple_allocator(&[1], &[&[1], &[1], &[1]], None);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    simple_alloc(&mut allocator, &[1], true);
    allocator.free_resources.assert_eq(&[0]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check3() {
    // Running: [3], free: [1], try: [2p1] [1p0]
    let mut allocator = simple_allocator(&[1], &[&[3]], None);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[1], true);
    allocator.free_resources.assert_eq(&[0]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check4() {
    // Running: [3], free: [1], try: [2p0] [1p0]
    let mut allocator = simple_allocator(&[1], &[&[3]], None);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    simple_alloc(&mut allocator, &[1], true);
    allocator.free_resources.assert_eq(&[0]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check5() {
    // Running: [], free: [4], try: [3p2] [2p1] [1p0]
    let mut allocator = simple_allocator(&[4], &[], None);

    simple_alloc(&mut allocator, &[3], true);
    allocator.free_resources.assert_eq(&[1]);

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[1], true);
    allocator.free_resources.assert_eq(&[0]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check6() {
    let mut allocator = simple_allocator(&[10], &[], None);

    for _ in 0..5 {
        simple_alloc(&mut allocator, &[2], true);
    }
    allocator.free_resources.assert_eq(&[0]);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[0]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check7() {
    let mut allocator = simple_allocator(&[10], &[], None);

    for _ in 0..5 {
        simple_alloc(&mut allocator, &[2], true);
        allocator.close_priority_level();
    }
    allocator.free_resources.assert_eq(&[0]);
    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[0]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check8() {
    let mut allocator = simple_allocator(&[1], &[&[1], &[1], &[1], &[2]], None);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[1], false);
    allocator.free_resources.assert_eq(&[1]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check9() {
    let mut allocator = simple_allocator(&[1], &[&[2], &[1], &[1], &[1]], None);

    simple_alloc(&mut allocator, &[2], false);
    allocator.free_resources.assert_eq(&[1]);

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[1], false);
    allocator.free_resources.assert_eq(&[1]);
    allocator.validate();
}

#[test]
fn test_allocator_priority_check10() {
    let mut allocator = simple_allocator(&[1], &[&[3], &[1], &[3], &[1]], None);

    simple_alloc(&mut allocator, &[3], false);
    allocator.free_resources.assert_eq(&[1]);

    allocator.close_priority_level();

    simple_alloc(&mut allocator, &[1], false);
    allocator.free_resources.assert_eq(&[1]);
    allocator.validate();
}

#[test]
fn test_pool_single_socket() {
    let descriptor = simple_descriptor(1, 4);
    let mut allocator = test_allocator(&descriptor);

    let rq = cpus_compact(3).finish_v();
    let (al, idx) = allocator.try_allocate(&rq).unwrap();
    assert_eq!(idx.as_num(), 0);
    assert_eq!(al.resources.len(), 1);
    assert_eq!(al.resources[0].resource_id, ResourceId::new(0));
    assert_eq!(al.resources[0].indices.len(), 3);
    assert!(al.resources[0].resource_indices().all(|x| x.as_num() < 4));

    let rq = cpus_compact(2).finish_v();
    assert!(allocator.try_allocate(&rq).is_none());

    assert_eq!(allocator.running_tasks.len(), 1);
    allocator.release_allocation(al);
    assert_eq!(allocator.running_tasks.len(), 0);

    allocator.reset_temporaries(None);

    let rq = cpus_compact(4).finish_v();
    let (al, _idx) = allocator.try_allocate(&rq).unwrap();
    assert_eq!(
        al.resources[0].resource_indices().collect::<Vec<_>>(),
        vec![3.into(), 2.into(), 1.into(), 0.into()]
    );
    assert_eq!(allocator.running_tasks.len(), 1);
    allocator.release_allocation(al);
    assert_eq!(allocator.running_tasks.len(), 0);
    allocator
        .free_resources
        .get(0.into())
        .amount_sum()
        .assert_eq_units(4);
    assert_eq!(allocator.free_resources.get(0.into()).n_groups(), 1);

    allocator.reset_temporaries(None);

    let rq = cpus_compact(1).finish_v();
    let rq2 = cpus_compact(2).finish_v();
    let (al1, _) = allocator.try_allocate(&rq).unwrap();
    let (al2, _) = allocator.try_allocate(&rq).unwrap();
    let (al3, _) = allocator.try_allocate(&rq).unwrap();
    let (al4, _) = allocator.try_allocate(&rq).unwrap();
    assert!(allocator.try_allocate(&rq).is_none());
    assert!(allocator.try_allocate(&rq2).is_none());

    allocator.release_allocation(al2);
    allocator.release_allocation(al4);

    allocator.reset_temporaries(None);

    let (al5, _) = allocator.try_allocate(&rq2).unwrap();

    assert!(allocator.try_allocate(&rq).is_none());
    assert!(allocator.try_allocate(&rq2).is_none());

    let mut v = Vec::new();
    v.extend(al1.resources[0].resource_indices());
    assert_eq!(v.len(), 1);
    v.extend(al5.resources[0].resource_indices());
    assert_eq!(v.len(), 3);
    v.extend(al3.resources[0].resource_indices());
    assert_eq!(sorted_vec(v), vec![0.into(), 1.into(), 2.into(), 3.into()]);
    allocator.validate();
}

#[test]
fn test_pool_compact1() {
    let descriptor = simple_descriptor(4, 6);
    let mut allocator = test_allocator(&descriptor);

    // Allocate 4 cpus
    let rq1 = cpus_compact(4).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    let s1 = allocator.get_sockets(&al1, 0);
    assert_eq!(s1.len(), 1);

    // Allocate 4 cpus
    let (al2, _) = allocator.try_allocate(&rq1).unwrap();
    let s2 = allocator.get_sockets(&al2, 0);
    assert_eq!(s2.len(), 1);
    assert_ne!(s1, s2);

    // Allocate 3 cpus
    let rq2 = cpus_compact(3).finish_v();
    let (al3, _) = allocator.try_allocate(&rq2).unwrap();
    let s3 = allocator.get_sockets(&al3, 0);
    assert_eq!(s3.len(), 1);

    // Allocate 3 cpus
    let (al4, _) = allocator.try_allocate(&rq2).unwrap();
    let s4 = allocator.get_sockets(&al4, 0);
    assert_eq!(s4.len(), 1);
    assert_ne!(s3, s1);
    assert_ne!(s4, s1);
    assert_ne!(s3, s2);
    assert_ne!(s4, s2);
    assert_eq!(s3, s4);

    allocator.close_priority_level();

    let rq3 = cpus_compact(6).finish_v();
    let (al, _) = allocator.try_allocate(&rq3).unwrap();
    assert_eq!(allocator.get_sockets(&al, 0).len(), 1);
    allocator.release_allocation(al);
    allocator.reset_temporaries(None);

    let rq3 = cpus_compact(7).finish_v();
    let (al, _) = allocator.try_allocate(&rq3).unwrap();
    assert_eq!(allocator.get_sockets(&al, 0).len(), 2);

    allocator.release_allocation(al);
    allocator.reset_temporaries(None);

    let rq3 = cpus_compact(8).finish_v();
    let (al, _) = allocator.try_allocate(&rq3).unwrap();
    assert_eq!(allocator.get_sockets(&al, 0).len(), 2);
    allocator.release_allocation(al);
    allocator.reset_temporaries(None);

    let rq3 = cpus_compact(9).finish_v();
    let (al, _) = allocator.try_allocate(&rq3).unwrap();
    assert_eq!(allocator.get_sockets(&al, 0).len(), 3);
    allocator.release_allocation(al);
    allocator.validate();
}

#[test]
fn test_pool_allocate_compact_all() {
    let descriptor = simple_descriptor(4, 6);
    let mut allocator = test_allocator(&descriptor);

    let rq = cpus_compact(24).finish_v();
    let (al, _) = allocator.try_allocate(&rq).unwrap();
    assert_eq!(
        al.resource_allocation(0.into())
            .unwrap()
            .resource_indices()
            .collect::<Vec<_>>(),
        (0..24u32).map(ResourceIndex::new).collect::<Vec<_>>()
    );
    assert!(allocator.get_current_free(0).is_zero());
    allocator.release_allocation(al);
    assert_eq!(allocator.get_current_free(0), ResourceAmount::new_units(24));
    allocator.validate();
}

#[test]
fn test_pool_allocate_all() {
    let descriptor = simple_descriptor(4, 6);
    let mut allocator = test_allocator(&descriptor);

    let rq = ResBuilder::default().add_all(0).finish_v();
    let (al, _) = allocator.try_allocate(&rq).unwrap();
    assert_eq!(
        al.resource_allocation(0.into())
            .unwrap()
            .resource_indices()
            .collect::<Vec<_>>(),
        (0..24u32).map(ResourceIndex::new).collect::<Vec<_>>()
    );
    assert!(allocator.get_current_free(0).is_zero());
    allocator.release_allocation(al);
    assert_eq!(allocator.get_current_free(0), ResourceAmount::new_units(24));

    allocator.reset_temporaries(None);

    let rq2 = cpus_compact(1).finish_v();
    assert!(allocator.try_allocate(&rq2).is_some());
    assert!(allocator.try_allocate(&rq).is_none());
    allocator.validate();
}

#[test]
fn test_pool_force_compact1() {
    let descriptor = simple_descriptor(2, 4);
    let mut allocator = test_allocator(&descriptor);

    let rq1 = ResBuilder::default().add_force_compact(0, 9).finish_v();
    assert!(allocator.try_allocate(&rq1).is_none());

    let rq2 = ResBuilder::default().add_force_compact(0, 2).finish_v();
    for _ in 0..4 {
        let (al1, _) = allocator.try_allocate(&rq2).unwrap();
        assert_eq!(al1.get_indices(0).len(), 2);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 1);
    }

    assert!(allocator.try_allocate(&rq2).is_none());
    allocator.validate();
}

#[test]
fn test_pool_force_compact2() {
    let descriptor = simple_descriptor(2, 4);
    let mut allocator = test_allocator(&descriptor);

    let rq1 = ResBuilder::default().add_force_compact(0, 3).finish_v();
    for _ in 0..2 {
        let (al1, _) = allocator.try_allocate(&rq1).unwrap();
        assert_eq!(al1.get_indices(0).len(), 3);
        assert_eq!(allocator.get_sockets(&al1, 0).len(), 1);
    }

    let rq1 = ResBuilder::default().add_force_compact(0, 2).finish_v();
    let al2 = allocator.try_allocate(&rq1);
    assert!(al2.is_none());

    let rq1 = cpus_compact(2).finish_v();
    assert!(allocator.try_allocate(&rq1).is_some());
    allocator.validate();
}

#[test]
fn test_pool_force_compact3() {
    let descriptor = simple_descriptor(3, 4);
    let mut allocator = test_allocator(&descriptor);

    let rq1 = ResBuilder::default().add_force_compact(0, 8).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    assert_eq!(al1.get_indices(0).len(), 8);
    assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);
    allocator.release_allocation(al1);
    allocator.validate();

    allocator.reset_temporaries(None);

    let rq1 = ResBuilder::default().add_force_compact(0, 5).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    assert_eq!(al1.get_indices(0).len(), 5);
    assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);
    allocator.release_allocation(al1);
    allocator.validate();

    allocator.reset_temporaries(None);

    let rq1 = ResBuilder::default().add_force_compact(0, 10).finish_v();
    let (al1, _idx) = allocator.try_allocate(&rq1).unwrap();
    assert_eq!(al1.get_indices(0).len(), 10);
    assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);
    allocator.release_allocation(al1);
    allocator.validate();
}

#[test]
fn test_pool_force_scatter1() {
    let descriptor = simple_descriptor(3, 4);
    let mut allocator = test_allocator(&descriptor);

    let rq1 = ResBuilder::default().add_scatter(0, 3).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    assert_eq!(al1.get_indices(0).len(), 3);
    assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);

    let rq1 = ResBuilder::default().add_scatter(0, 4).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    assert_eq!(al1.get_indices(0).len(), 4);
    assert_eq!(allocator.get_sockets(&al1, 0).len(), 3);

    let rq1 = ResBuilder::default().add_scatter(0, 2).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    assert_eq!(al1.get_indices(0).len(), 2);
    assert_eq!(allocator.get_sockets(&al1, 0).len(), 2);

    allocator.validate();
}

#[test]
fn test_pool_force_scatter2() {
    let descriptor = simple_descriptor(3, 4);
    let mut allocator = test_allocator(&descriptor);

    let rq1 = ResBuilder::default().add_force_compact(0, 4).finish_v();
    let _al1 = allocator.try_allocate(&rq1).unwrap();

    let rq2 = ResBuilder::default().add_scatter(0, 5).finish_v();
    let (al2, _) = allocator.try_allocate(&rq2).unwrap();
    assert_eq!(al2.get_indices(0).len(), 5);
    assert_eq!(allocator.get_sockets(&al2, 0).len(), 2);

    allocator.validate();
}

#[test]
fn test_pool_generic_resources() {
    let descriptor = ResourceDescriptor::new(
        vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(1, 4),
            },
            ResourceDescriptorItem {
                name: "res0".to_string(),
                kind: ResourceDescriptorKind::Range {
                    start: 5.into(),
                    end: 100.into(),
                },
            },
            ResourceDescriptorItem {
                name: "res1".to_string(),
                kind: ResourceDescriptorKind::Sum {
                    size: ResourceAmount::new_units(100_000_000),
                },
            },
            ResourceDescriptorItem {
                name: "res2".to_string(),
                kind: ResourceDescriptorKind::simple_indices(2),
            },
            ResourceDescriptorItem {
                name: "res3".to_string(),
                kind: ResourceDescriptorKind::simple_indices(2),
            },
        ],
        Default::default(),
    );
    let mut allocator = test_allocator(&descriptor);

    assert_eq!(
        allocator.free_resources,
        ConciseFreeResources::new(
            vec![
                ConciseResourceState::new_simple(&[4]),
                ConciseResourceState::new_simple(&[96]),
                ConciseResourceState::new_simple(&[100_000_000]),
                ConciseResourceState::new_simple(&[2]),
                ConciseResourceState::new_simple(&[2]),
            ]
            .into()
        )
    );

    let rq: ResourceRequestVariants = cpus_compact(1)
        .add(4, 1)
        .add(1, 12)
        .add(2, 1_000_000)
        .finish_v();
    rq.validate().unwrap();
    let (al, _) = allocator.try_allocate(&rq).unwrap();
    assert_eq!(al.resources.len(), 4);
    assert_eq!(al.resources[0].resource_id.as_num(), 0);

    assert_eq!(al.resources[1].resource_id.as_num(), 1);
    assert_eq!(al.resources[1].indices.len(), 12);

    assert_eq!(al.resources[2].resource_id.as_num(), 2);
    assert_eq!(al.resources[2].amount, ResourceAmount::new_units(1_000_000));

    assert_eq!(al.resources[3].resource_id.as_num(), 4);
    assert_eq!(al.resources[3].indices.len(), 1);

    assert_eq!(allocator.get_current_free(1), ResourceAmount::new_units(84));
    assert_eq!(
        allocator.get_current_free(2),
        ResourceAmount::new_units(99_000_000)
    );
    assert_eq!(allocator.get_current_free(3), ResourceAmount::new_units(2));
    assert_eq!(allocator.get_current_free(4), ResourceAmount::new_units(1));

    let rq = cpus_compact(1).add(4, 2).finish_v();
    assert!(allocator.try_allocate(&rq).is_none());

    allocator.release_allocation(al);

    assert_eq!(allocator.get_current_free(1), ResourceAmount::new_units(96));
    assert_eq!(
        allocator.get_current_free(2),
        ResourceAmount::new_units(100_000_000)
    );
    assert_eq!(allocator.get_current_free(3), ResourceAmount::new_units(2));
    assert_eq!(allocator.get_current_free(4), ResourceAmount::new_units(2));

    allocator.reset_temporaries(None);
    assert!(allocator.try_allocate(&rq).is_some());

    allocator.validate();
}

#[test]
fn test_allocator_remaining_time_no_known() {
    let descriptor = simple_descriptor(1, 4);
    let mut allocator = test_allocator(&descriptor);

    allocator.reset_temporaries(None);
    let rq = ResBuilder::default()
        .add(0, 1)
        .min_time_secs(100)
        .finish_v();
    let (al, _) = allocator.try_allocate(&rq).unwrap();
    allocator.release_allocation(al);

    allocator.reset_temporaries(Some(Duration::from_secs(101)));
    let (al, _) = allocator.try_allocate(&rq).unwrap();
    allocator.release_allocation(al);

    allocator.reset_temporaries(Some(Duration::from_secs(99)));
    assert!(allocator.try_allocate(&rq).is_none());

    allocator.validate();
}

#[test]
fn test_allocator_sum_max_fractions() {
    let descriptor = ResourceDescriptor::new(
        vec![ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: ResourceDescriptorKind::Sum {
                size: ResourceAmount::new(0, 300),
            },
        }],
        Default::default(),
    );
    let mut allocator = test_allocator(&descriptor);
    allocator.reset_temporaries(None);
    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(1, 0))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());
    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 301))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());
    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 250))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_some());
}

#[test]
fn test_allocator_indices_and_fractions() {
    let descriptor = simple_descriptor(1, 4);
    let mut allocator = test_allocator(&descriptor);
    allocator.reset_temporaries(None);
    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(4, 1))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(2, 1500))
        .finish_v();
    let al1 = allocator.try_allocate(&rq).unwrap().0;
    assert_eq!(al1.resources[0].indices.len(), 3);
    assert_eq!(al1.resources[0].indices[0].fractions, 0);
    assert_eq!(al1.resources[0].indices[1].fractions, 0);
    assert_eq!(al1.resources[0].indices[2].fractions, 1500);
    assert_eq!(al1.resources[0].amount, ResourceAmount::new(2, 1500));

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 5200))
        .finish_v();
    let al2 = allocator.try_allocate(&rq).unwrap().0;
    assert_eq!(al2.resources[0].indices.len(), 1);
    assert_eq!(al2.resources[0].indices[0].fractions, 5200);
    assert_eq!(
        al2.resources[0].indices[0].index,
        al1.resources[0].indices[2].index
    );
    assert_eq!(al2.resources[0].amount, ResourceAmount::new(0, 5200));

    let al3 = allocator.try_allocate(&rq).unwrap().0;
    assert_eq!(al3.resources[0].indices.len(), 1);
    assert_eq!(al3.resources[0].indices[0].fractions, 5200);
    assert_ne!(
        al3.resources[0].indices[0].index,
        al1.resources[0].indices[2].index
    );
    assert_eq!(al3.resources[0].amount, ResourceAmount::new(0, 5200));

    assert!(allocator.try_allocate(&rq).is_none());

    allocator.release_allocation(al1);

    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new(2, 9600)
    );

    allocator.release_allocation(al3);
    allocator.release_allocation(al2);

    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new(4, 0)
    );
}

#[test]
fn test_allocator_fractions_compactness() {
    // Two 0.75 does not gives 1.5
    let descriptor = simple_descriptor(1, 2);
    let mut allocator = test_allocator(&descriptor);
    allocator.reset_temporaries(None);
    let rq1 = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 7500))
        .finish_v();
    let rq2 = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 2500))
        .finish_v();

    let al1 = allocator.try_allocate(&rq1).unwrap().0;
    let al2 = allocator.try_allocate(&rq1).unwrap().0;
    let al3 = allocator.try_allocate(&rq2).unwrap().0;
    let al4 = allocator.try_allocate(&rq2).unwrap().0;
    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new(0, 0)
    );
    allocator.release_allocation(al1);
    allocator.release_allocation(al2);

    allocator.reset_temporaries(None);

    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new(1, 5000)
    );

    let rq3 = ResBuilder::default()
        .add(0, ResourceAmount::new(1, 5000))
        .finish_v();
    assert!(allocator.try_allocate(&rq3).is_none());
    allocator.release_allocation(al4);
    allocator.reset_temporaries(None);
    let al5 = allocator.try_allocate(&rq3).unwrap().0;
    allocator.release_allocation(al3);
    allocator.release_allocation(al5);

    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new(2, 0)
    );
}

#[test]
fn test_allocator_groups_and_fractions_scatter() {
    let descriptor = simple_descriptor(3, 2);
    let mut allocator = test_allocator(&descriptor);
    allocator.reset_temporaries(None);
    let rq1 = ResBuilder::default()
        .add_scatter(0, ResourceAmount::new(6, 1))
        .finish_v();
    assert!(allocator.try_allocate(&rq1).is_none());

    let rq1 = ResBuilder::default()
        .add_scatter(0, ResourceAmount::new(2, 5000))
        .finish_v();
    let al1 = allocator.try_allocate(&rq1).unwrap().0;
    let al2 = allocator.try_allocate(&rq1).unwrap().0;
    allocator.validate();
    let r1 = &al1.resources[0].indices;
    let r2 = &al2.resources[0].indices;
    assert_eq!(r1[2].fractions, 5000);
    assert_eq!(r2[2].fractions, 5000);
    assert_eq!(r1[2].group_idx, r2[2].group_idx);
    allocator.release_allocation(al1);
    allocator.release_allocation(al2);
    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new_units(6)
    );
}

#[test]
fn test_allocator_groups_and_fractions() {
    let descriptor = simple_descriptor(3, 2);
    let mut allocator = test_allocator(&descriptor);
    allocator.reset_temporaries(None);
    let rq1 = ResBuilder::default()
        .add(0, ResourceAmount::new(6, 1))
        .finish_v();
    assert!(allocator.try_allocate(&rq1).is_none());

    let rq1 = ResBuilder::default()
        .add_compact(0, ResourceAmount::new(3, 5000))
        .finish_v();
    let al1 = allocator.try_allocate(&rq1).unwrap().0;
    // pools [[1 1] [0 0.5] [0 0]
    let r1 = &al1.resources[0].indices;
    assert_eq!(r1.len(), 4);
    assert_eq!(r1[0].group_idx, r1[1].group_idx);
    assert_eq!(r1[2].group_idx, r1[3].group_idx);
    assert_ne!(r1[0].group_idx, r1[2].group_idx);

    let rq2 = ResBuilder::default()
        .add_compact(0, ResourceAmount::new(0, 4000))
        .finish_v();
    let al2 = allocator.try_allocate(&rq2).unwrap().0;
    // pools [[1 1] [0.1 0] [0 0]

    let r2 = &al2.resources[0].indices;
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].group_idx, r1[2].group_idx);

    allocator.release_allocation(al1);
    // pools [[1 1] [0.6 1] [1 1]

    let rq3 = ResBuilder::default()
        .add_compact(0, ResourceAmount::new(2, 8000))
        .finish_v();
    let al3 = allocator.try_allocate(&rq3).unwrap().0;
    // pools [[1 1] [0.6 0] [0.2 0]

    let r3 = &al3.resources[0].indices;
    assert_eq!(r3.len(), 3);
    assert_eq!(r3[0].group_idx, r3[2].group_idx);
    assert_ne!(r3[0].group_idx, r3[1].group_idx);
    assert_ne!(r3[0].group_idx, r2[0].group_idx);
    assert_ne!(r3[2].group_idx, r2[0].group_idx);
    assert_ne!(r3[2].index, r2[0].index);

    let rq4 = ResBuilder::default()
        .add_compact(0, ResourceAmount::new(0, 7000))
        .finish_v();
    let al4 = allocator.try_allocate(&rq4).unwrap().0;
    // pools [[1 0.3] [0.6 0] [0.2 0]
    allocator.validate();

    allocator.release_allocation(al2);
    // pools [[1 0.3] [1 0] [0.2 0]
    let rq6 = ResBuilder::default()
        .add_compact(0, ResourceAmount::new(2, 3000))
        .finish_v();
    let al6 = allocator.try_allocate(&rq6).unwrap().0;
    let r5 = &al6.resources[0].indices;
    assert_eq!(r5.len(), 3);
    assert_eq!(r5[0].fractions, 0);
    assert_eq!(r5[1].fractions, 0);
    assert_eq!(r5[2].fractions, 3000);

    assert!(r5[0].group_idx == r5[2].group_idx || r5[1].group_idx == r5[2].group_idx);
    assert_ne!(r5[1].group_idx, r5[0].group_idx);
    allocator.validate();

    allocator.release_allocation(al3);
    allocator.release_allocation(al4);
    allocator.release_allocation(al6);

    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new_units(6)
    );
}

#[test]
fn test_allocator_sum_fractions() {
    let descriptor = ResourceDescriptor::new(
        vec![ResourceDescriptorItem {
            name: "cpus".to_string(),
            kind: ResourceDescriptorKind::Sum {
                size: ResourceAmount::new_units(2),
            },
        }],
        Default::default(),
    );
    let mut allocator = test_allocator(&descriptor);
    allocator.reset_temporaries(None);
    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(2, 3000))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(1, 3000))
        .finish_v();
    let al1 = allocator.try_allocate(&rq).unwrap().0;
    assert_eq!(al1.resources[0].indices.len(), 0);
    assert_eq!(al1.resources[0].amount, ResourceAmount::new(1, 3000));
    allocator.validate();

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 7001))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 7000))
        .finish_v();
    let al2 = allocator.try_allocate(&rq).unwrap().0;
    assert_eq!(al2.resources[0].indices.len(), 0);
    assert_eq!(al2.resources[0].amount, ResourceAmount::new(0, 7000));

    allocator.release_allocation(al1);

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(2, 0))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());
    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(1, 3001))
        .finish_v();
    assert!(allocator.try_allocate(&rq).is_none());

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(1, 0))
        .finish_v();
    let al3 = allocator.try_allocate(&rq).unwrap().0;

    let rq = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 2000))
        .finish_v();
    let al4 = allocator.try_allocate(&rq).unwrap().0;

    allocator.release_allocation(al4);
    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new(0, 3000)
    );
    allocator.release_allocation(al2);
    allocator.release_allocation(al3);
    assert_eq!(
        allocator.pools[0.into()].concise_state().amount_sum(),
        ResourceAmount::new_units(2)
    )
}

#[test]
fn test_coupling1() {
    for i in 0..=2 {
        let mut coupling = ResourceDescriptorCoupling::default();
        for j in 0..4 {
            coupling.add(0, j, 2, j, 256);
        }
        let descriptor = ResourceDescriptor::new(
            vec![
                ResourceDescriptorItem {
                    name: "cpus".to_string(),
                    kind: ResourceDescriptorKind::regular_sockets(4, 3),
                },
                ResourceDescriptorItem {
                    name: "foo".to_string(),
                    kind: ResourceDescriptorKind::regular_sockets(4, 1),
                },
                ResourceDescriptorItem {
                    name: "gpus".to_string(),
                    kind: ResourceDescriptorKind::regular_sockets(4, 4),
                },
            ],
            coupling,
        );

        let mut allocator = test_allocator(&descriptor);
        let rq1 = cpus_compact(2).finish_v();
        for _ in 0..i {
            allocator.try_allocate(&rq1).unwrap();
        }
        let rq2 = cpus_compact(2).add(2, 2).finish_v();
        let (al3, _) = allocator.try_allocate(&rq2).unwrap();
        let s1 = allocator.get_sockets(&al3, 0);
        let s2 = allocator.get_sockets(&al3, 2);
        assert_eq!(s1.len(), 1);
        assert_eq!(s1, s2);
        assert_eq!(al3.get_indices(0).len(), 2);
        assert_eq!(al3.get_indices(2).len(), 2);
        allocator.validate();
    }
}

fn descriptor_cpus_gpus(
    n_sockets: u32,
    socket_size1: u32,
    socket_size2: u32,
    coupled: bool,
) -> ResourceDescriptor {
    let mut coupling = ResourceDescriptorCoupling::default();
    if coupled {
        for j in 0..n_sockets as u8 {
            coupling.add(0, j, 1, j, 256);
        }
    }
    ResourceDescriptor::new(
        vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(n_sockets, socket_size1),
            },
            ResourceDescriptorItem {
                name: "gpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(n_sockets, socket_size2),
            },
        ],
        coupling,
    )
}

#[test]
fn test_coupling2() {
    let descriptor = descriptor_cpus_gpus(4, 4, 2, true);
    let mut allocator = test_allocator(&descriptor);
    let rq1 = cpus_compact(4).add(1, 3).finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    allocator.validate();
    let s1 = allocator.get_sockets(&al1, 0);
    let s2 = allocator.get_sockets(&al1, 1);
    assert_eq!(s1.len(), 1);
    assert_eq!(s2.len(), 2);
    let mut f = false;
    for s in s2 {
        f |= s == s1[0];
    }
    assert!(f);
    let g0 = al1.get_groups(0);
    assert_eq!(g0.len(), 1);
    assert!(g0.values().all(|x| *x == 4));
    let g1 = al1.get_groups(1);
    let v: Vec<_> = g1.values().copied().collect();
    assert_eq!(sorted_vec(v), vec![1, 2]);
}

#[test]
fn test_coupling3() {
    let descriptor = descriptor_cpus_gpus(4, 4, 2, true);
    let mut allocator = test_allocator(&descriptor);
    let rq1 = ResBuilder::default()
        .add(0, ResourceAmount::new(0, 1000))
        .add(1, ResourceAmount::new(0, 5000))
        .finish_v();
    let (al1, _) = allocator.try_allocate(&rq1).unwrap();
    let s1 = allocator.get_sockets(&al1, 0);
    let s2 = allocator.get_sockets(&al1, 1);
    assert_eq!(s1.len(), 1);
    assert_eq!(s1, s2);
    let g0 = al1.get_groups(0);
    let g1 = al1.get_groups(1);
    assert_eq!(g0, g1);
    let v: Vec<_> = g1.values().copied().collect();
    assert_eq!(v, vec![1]);
}

#[test]
fn test_complex_coupling1() {
    let mut coupling = ResourceDescriptorCoupling::default();

    for i in 0..6 {
        coupling.add(0, i, 1, i / 2, 256);
        coupling.add(1, i / 2, 2, i, 128);
    }

    let descriptor = ResourceDescriptor::new(
        vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(6, 2),
            },
            ResourceDescriptorItem {
                name: "gpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(3, 1),
            },
            ResourceDescriptorItem {
                name: "foo".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(6, 3),
            },
        ],
        coupling,
    );

    let mut allocator = test_allocator(&descriptor);

    allocator.force_claim_from_groups(0.into(), &[0], 1.into());
    allocator.force_claim_from_groups(2.into(), &[5], 2.into());

    let rq = ResBuilder::default()
        .add_force_compact(0, ResourceAmount::new_units(4))
        .add_force_compact(1, ResourceAmount::new_units(1))
        .add_force_compact(2, ResourceAmount::new_units(5))
        .finish_v();
    let (al1, _) = allocator.try_allocate(&rq).unwrap();
    let g = al1.get_groups(0);
    assert_eq!(sorted_vec(g.keys().copied().collect()), vec![2, 3]);
    assert_eq!(sorted_vec(g.values().copied().collect()), vec![2, 2]);

    let g = al1.get_groups(1);
    assert_eq!(sorted_vec(g.keys().copied().collect()), vec![1]);
    assert_eq!(sorted_vec(g.values().copied().collect()), vec![1]);

    let g = al1.get_groups(2);
    assert_eq!(sorted_vec(g.keys().copied().collect()), vec![2, 3]);
    assert_eq!(sorted_vec(g.values().copied().collect()), vec![2, 3]);
}

#[test]
fn test_complex_coupling2() {
    let mut coupling = ResourceDescriptorCoupling::default();

    coupling.add(0, 2, 1, 1, 256);
    coupling.add(0, 0, 1, 1, 128);
    coupling.add(1, 1, 2, 0, 256);

    let descriptor = ResourceDescriptor::new(
        vec![
            ResourceDescriptorItem {
                name: "cpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(3, 1),
            },
            ResourceDescriptorItem {
                name: "gpus".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(3, 1),
            },
            ResourceDescriptorItem {
                name: "foo".to_string(),
                kind: ResourceDescriptorKind::regular_sockets(3, 1),
            },
        ],
        coupling,
    );

    let mut allocator = test_allocator(&descriptor);

    let rq = ResBuilder::default()
        .add_force_compact(0, ResourceAmount::new_units(1))
        .add_force_compact(1, ResourceAmount::new_units(1))
        .add_force_compact(2, ResourceAmount::new_units(1))
        .finish_v();
    let (al1, _) = allocator.try_allocate(&rq).unwrap();
    assert_eq!(al1.get_indices(0), vec![ResourceIndex::new(2)]);
    assert_eq!(al1.get_indices(1), vec![ResourceIndex::new(1)]);
    assert_eq!(al1.get_indices(2), vec![ResourceIndex::new(0)]);
}

#[test]
fn test_coupling_force2() {
    for coupled in [true, false] {
        let descriptor = descriptor_cpus_gpus(3, 2, 2, coupled);
        let mut allocator = test_allocator(&descriptor);

        for g in [0, 1] {
            allocator.force_claim_from_groups(0.into(), &[g], 2.into());
        }
        for g in [1, 2] {
            allocator.force_claim_from_groups(1.into(), &[g], 2.into());
        }

        let rq = ResBuilder::default()
            .add_force_compact(0, ResourceAmount::new_units(1))
            .add_force_compact(1, ResourceAmount::new_units(1))
            .finish_v();
        assert_eq!(allocator.try_allocate(&rq).is_none(), coupled);
    }
}

#[test]
fn test_coupling_force3() {
    let descriptor = descriptor_cpus_gpus(4, 2, 2, true);
    let mut allocator = test_allocator(&descriptor);

    for g in [0, 1] {
        allocator.force_claim_from_groups(0.into(), &[g], 2.into());
    }
    for g in [1, 3] {
        allocator.force_claim_from_groups(1.into(), &[g], 1.into());
    }
    allocator.validate();
    let rq = ResBuilder::default()
        .add_force_compact(0, ResourceAmount::new_units(3))
        .add_force_compact(1, ResourceAmount::new_units(3))
        .finish_v();
    let (al, _) = allocator.try_allocate(&rq).unwrap();
    let g0 = al.get_groups(0);
    assert_eq!(g0.len(), 2);
    assert!(g0.contains_key(&2));
    assert!(g0.contains_key(&3));
    let g1 = al.get_groups(1);
    assert_eq!(g1.len(), 2);
    assert!(g1.contains_key(&2));
    assert!(g1.contains_key(&3));
}

#[test]
fn test_compact_scattering() {
    let descriptor = simple_descriptor(4, 4);
    let mut allocator = test_allocator(&descriptor);
    let rq1 = ResBuilder::default()
        .add_compact(0, ResourceAmount::new_units(6))
        .finish_v();
    let al1 = allocator.try_allocate(&rq1).unwrap().0;
    let r1 = &al1.resources[0].indices;
    assert_eq!(r1.len(), 6);
    assert_eq!(r1[0].group_idx, r1[1].group_idx);
    assert_eq!(r1[0].group_idx, r1[2].group_idx);
    assert_eq!(r1[3].group_idx, r1[4].group_idx);
    assert_eq!(r1[3].group_idx, r1[5].group_idx);
    assert_ne!(r1[0].group_idx, r1[3].group_idx);
}

#[test]
fn test_tight_scattering() {
    let descriptor = simple_descriptor(4, 4);
    let mut allocator = test_allocator(&descriptor);
    let rq1 = ResBuilder::default()
        .add_tight(0, ResourceAmount::new_units(6))
        .finish_v();
    let al1 = allocator.try_allocate(&rq1).unwrap().0;
    let r1 = &al1.resources[0].indices;
    assert_eq!(r1.len(), 6);
    assert_eq!(r1[0].group_idx, r1[1].group_idx);
    assert_eq!(r1[0].group_idx, r1[2].group_idx);
    assert_eq!(r1[0].group_idx, r1[3].group_idx);
    assert_eq!(r1[4].group_idx, r1[5].group_idx);
    assert_ne!(r1[0].group_idx, r1[4].group_idx);
}

// #[test]
// fn test_coupling_tight() {
//     for coupled in [true, false] {
//         let descriptor = descriptor_cpus_gpus(3, 4, 2, coupled);
//         let mut allocator = test_allocator(&descriptor);
//
//         allocator.force_claim_from_groups(0.into(), &[0], 4.into());
//         allocator.force_claim_from_groups(1.into(), &[1, 2], 4.into());
//
//         let rq = ResBuilder::default()
//             .add_force_compact(0, ResourceAmount::new_units(1))
//             .add_force_compact(1, ResourceAmount::new_units(1))
//             .finish_v();
//         let al = allocator.try_allocate(&rq);
//         assert_eq!(al.is_none(), coupled);
//     }
// }
