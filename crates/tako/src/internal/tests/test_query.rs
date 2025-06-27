use crate::control::WorkerTypeQuery;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::server::core::Core;
use crate::internal::tests::utils::env::{TestEnv, create_test_comm};
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_workers, submit_test_tasks,
};
use crate::internal::tests::utils::task::TaskBuilder;
use crate::resources::{ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind};
use std::time::Duration;

#[test]
fn test_query_no_tasks() {
    let mut core = Core::default();
    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            partial: false,
            descriptor: ResourceDescriptor::simple_cpus(4),
            time_limit: None,
            max_sn_workers: 2,
            max_workers_per_allocation: 1,
            min_utilization: 0.0,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_enough_workers() {
    let mut core = Core::default();

    create_test_workers(&mut core, &[2, 3]);

    let t1 = TaskBuilder::new(1).cpus_compact(3).build();
    let t2 = TaskBuilder::new(2).cpus_compact(1).build();
    let t3 = TaskBuilder::new(3).cpus_compact(1).build();
    submit_test_tasks(&mut core, vec![t1, t2, t3]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            partial: false,
            descriptor: ResourceDescriptor::simple_cpus(4),
            time_limit: None,
            max_sn_workers: 2,
            max_workers_per_allocation: 1,
            min_utilization: 0.0,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_no_enough_workers1() {
    let mut core = Core::default();

    create_test_workers(&mut core, &[2, 3]);

    let t1 = TaskBuilder::new(1).cpus_compact(3).build();
    let t2 = TaskBuilder::new(2).cpus_compact(3).build();
    let t3 = TaskBuilder::new(3).cpus_compact(1).build();
    submit_test_tasks(&mut core, vec![t1, t2, t3]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    let r = compute_new_worker_query(
        &mut core,
        &[
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(3),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0, 1]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_enough_workers2() {
    let mut rt = TestEnv::new();

    rt.new_workers(&[2]);

    rt.new_task_running(TaskBuilder::new(10).cpus_compact(1), 100);
    rt.new_task_assigned(TaskBuilder::new(11).cpus_compact(1), 100);
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(3),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0, 0]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_not_enough_workers3() {
    let mut rt = TestEnv::new();

    rt.new_workers(&[2]);

    rt.new_task_running(TaskBuilder::new(10).cpus_compact(1), 100);
    rt.new_task_running(TaskBuilder::new(12).cpus_compact(1), 100);
    rt.new_task_assigned(TaskBuilder::new(11).cpus_compact(1), 100);
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(3),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![1, 0]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_many_workers_needed() {
    let mut rt = TestEnv::new();

    rt.new_workers(&[4, 4, 4]);

    for i in 1..=100 {
        rt.new_task(TaskBuilder::new(i).cpus_compact(1));
    }
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 5,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(1),
                time_limit: None,
                max_sn_workers: 1,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(3),
                time_limit: None,
                max_sn_workers: 200,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![5, 1, 26]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_multi_node_tasks() {
    let mut rt = TestEnv::new();

    rt.new_workers(&[4, 4, 4]);

    for i in 0..5 {
        rt.new_task(TaskBuilder::new(i).n_nodes(3));
    }
    for i in 5..15 {
        rt.new_task(TaskBuilder::new(i).n_nodes(6));
    }
    for i in 15..20 {
        rt.new_task(TaskBuilder::new(i).n_nodes(12));
    }
    for i in 20..40 {
        rt.new_task(TaskBuilder::new(i).n_nodes(3).user_priority(10));
    }
    rt.new_task(TaskBuilder::new(1000).n_nodes(1));

    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(1),
                time_limit: None,
                max_sn_workers: 1,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(1),
                time_limit: None,
                max_sn_workers: 1,
                max_workers_per_allocation: 11,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0, 0]);
    assert_eq!(r.multi_node_allocations.len(), 3);
    assert_eq!(r.multi_node_allocations[0].worker_type, 0);
    assert_eq!(r.multi_node_allocations[0].worker_per_allocation, 1);
    assert_eq!(r.multi_node_allocations[0].max_allocations, 1);

    assert_eq!(r.multi_node_allocations[1].worker_type, 0);
    assert_eq!(r.multi_node_allocations[1].worker_per_allocation, 3);
    assert_eq!(r.multi_node_allocations[1].max_allocations, 24); // <-- Total is 25, but one is running

    assert_eq!(r.multi_node_allocations[2].worker_type, 1);
    assert_eq!(r.multi_node_allocations[2].worker_per_allocation, 6);
    assert_eq!(r.multi_node_allocations[2].max_allocations, 10);
}

#[test]
fn test_query_multi_node_time_limit() {
    let mut rt = TestEnv::new();

    rt.new_task(TaskBuilder::new(1).n_nodes(4).time_request(750));
    rt.schedule();

    for (secs, allocs) in [(740, 0), (760, 1)] {
        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(1),
                time_limit: Some(Duration::from_secs(secs)),
                max_sn_workers: 4,
                max_workers_per_allocation: 4,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(r.multi_node_allocations.len(), allocs);
    }
}

#[test]
fn test_query_min_utilization1() {
    let mut core = Core::default();

    let t1 = TaskBuilder::new(1).cpus_compact(3).build();
    let t2 = TaskBuilder::new(2).cpus_compact(1).build();
    let t3 = TaskBuilder::new(3).cpus_compact(1).build();
    submit_test_tasks(&mut core, vec![t1, t2, t3]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    for (min_utilization, alloc_value, cpus) in &[
        (0.5, 0, 12),
        (0.3, 1, 12),
        (0.8, 0, 12),
        (1.0, 1, 5),
        (0.5, 2, 3),
        (0.7, 1, 3),
    ] {
        let r = compute_new_worker_query(
            &mut core,
            &[WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(*cpus),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: *min_utilization,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![*alloc_value]);
        assert!(r.multi_node_allocations.is_empty());
    }
}

#[test]
fn test_query_min_utilization2() {
    let mut core = Core::default();

    let t1 = TaskBuilder::new(1)
        .cpus_compact(1)
        .add_resource(1, 10)
        .build();
    let t2 = TaskBuilder::new(2)
        .cpus_compact(1)
        .add_resource(1, 10)
        .build();
    submit_test_tasks(&mut core, vec![t1, t2]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    for (min_utilization, alloc_value, cpus, gpus) in &[
        (0.5, 1, 12, 30),
        (0.67, 0, 12, 30),
        (0.55, 0, 4, 200),
        (0.45, 1, 4, 200),
    ] {
        let descriptor = ResourceDescriptor::new(vec![
            ResourceDescriptorItem {
                name: "cpus".into(),
                kind: ResourceDescriptorKind::simple_indices(*cpus),
            },
            ResourceDescriptorItem {
                name: "gpus".into(),
                kind: ResourceDescriptorKind::simple_indices(*gpus),
            },
        ]);
        let r = compute_new_worker_query(
            &mut core,
            &[WorkerTypeQuery {
                partial: false,
                descriptor,
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: *min_utilization,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![*alloc_value]);
        assert!(r.multi_node_allocations.is_empty());
    }
}

#[test]
fn test_query_min_utilization3() {
    let mut core = Core::default();

    let t1 = TaskBuilder::new(1).cpus_compact(2).build();
    let t2 = TaskBuilder::new(2).cpus_compact(2).build();
    submit_test_tasks(&mut core, vec![t1, t2]);

    let descriptor = ResourceDescriptor::new(vec![ResourceDescriptorItem {
        name: "cpus".into(),
        kind: ResourceDescriptorKind::simple_indices(4),
    }]);
    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            partial: false,
            descriptor,
            time_limit: None,
            max_sn_workers: 2,
            max_workers_per_allocation: 1,
            min_utilization: 1.0,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![1]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_min_time2() {
    let mut core = Core::default();

    let t1 = TaskBuilder::new(1)
        .cpus_compact(1)
        .time_request(100)
        .next_resources()
        .cpus_compact(4)
        .time_request(50)
        .build();
    submit_test_tasks(&mut core, vec![t1]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    for (cpus, secs, alloc) in [(2, 75, 0), (1, 100, 1), (4, 50, 1)] {
        let descriptor = ResourceDescriptor::new(vec![ResourceDescriptorItem {
            name: "cpus".into(),
            kind: ResourceDescriptorKind::simple_indices(cpus),
        }]);
        let r = compute_new_worker_query(
            &mut core,
            &[WorkerTypeQuery {
                partial: false,
                descriptor: descriptor.clone(),
                time_limit: Some(Duration::from_secs(secs)),
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0f32,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![alloc]);
        assert!(r.multi_node_allocations.is_empty());
    }
}

#[test]
fn test_query_min_time1() {
    let mut core = Core::default();

    let t1 = TaskBuilder::new(1)
        .cpus_compact(1)
        .time_request(100)
        .build();
    let t2 = TaskBuilder::new(2)
        .cpus_compact(10)
        .time_request(100)
        .build();
    submit_test_tasks(&mut core, vec![t1, t2]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut core, &mut comm);

    let descriptor = ResourceDescriptor::new(vec![ResourceDescriptorItem {
        name: "cpus".into(),
        kind: ResourceDescriptorKind::simple_indices(10),
    }]);
    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            partial: false,
            descriptor: descriptor.clone(),
            time_limit: Some(Duration::from_secs(99)),
            max_sn_workers: 2,
            max_workers_per_allocation: 1,
            min_utilization: 0.0f32,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0]);
    assert!(r.multi_node_allocations.is_empty());

    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            partial: false,
            descriptor: descriptor.clone(),
            time_limit: Some(Duration::from_secs(101)),
            max_sn_workers: 2,
            max_workers_per_allocation: 1,
            min_utilization: 0.0f32,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![2]);
    assert!(r.multi_node_allocations.is_empty());

    let descriptor = ResourceDescriptor::new(vec![ResourceDescriptorItem {
        name: "cpus".into(),
        kind: ResourceDescriptorKind::simple_indices(1),
    }]);
    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            partial: false,
            descriptor,
            time_limit: Some(Duration::from_secs(101)),
            max_sn_workers: 2,
            max_workers_per_allocation: 1,
            min_utilization: 0.0f32,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![1]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_sn_leftovers1() {
    for (n, m) in [(1, 0), (4, 0), (8, 0), (9, 1), (12, 1)] {
        let mut rt = TestEnv::new();

        rt.new_workers(&[4]);
        for i in 1..=n {
            rt.new_task(TaskBuilder::new(i).cpus_compact(1).time_request(5_000));
        }
        rt.schedule();

        let r = compute_new_worker_query(
            rt.core(),
            &[
                WorkerTypeQuery {
                    partial: false,
                    descriptor: ResourceDescriptor::simple_cpus(2),
                    time_limit: None,
                    max_sn_workers: 2,
                    max_workers_per_allocation: 1,
                    min_utilization: 0.0,
                },
                WorkerTypeQuery {
                    partial: true,
                    descriptor: ResourceDescriptor::new(Vec::new()),
                    time_limit: None,
                    max_sn_workers: 2,
                    max_workers_per_allocation: 1,
                    min_utilization: 0.0,
                },
            ],
        );
        assert_eq!(r.single_node_workers_per_query[1], m);
    }
}

#[test]
fn test_query_sn_leftovers2() {
    for (cpus, out) in [(1, 0), (2, 1)] {
        let mut rt = TestEnv::new();
        for i in 1..=100 {
            rt.new_task(
                TaskBuilder::new(i)
                    .cpus_compact(2)
                    .time_request(((i as u64 % 2) + 1) * 10_000),
            );
        }
        rt.schedule();

        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                partial: true,
                descriptor: ResourceDescriptor::simple_cpus(cpus),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![out]);
    }
}

#[test]
fn test_query_sn_leftovers() {
    let mut rt = TestEnv::new();

    rt.new_task(TaskBuilder::new(1).cpus_compact(4).time_request(750));
    rt.new_task(TaskBuilder::new(2).cpus_compact(8).time_request(1750));
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new()),
                time_limit: Some(Duration::from_secs(1000)),
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new()),
                time_limit: Some(Duration::from_secs(50)),
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new()),
                time_limit: None,
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![1, 0, 1]);
}
