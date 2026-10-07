use crate::control::WorkerTypeQuery;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::on_cancel_tasks;
use crate::internal::tests::utils::env::TestEnv;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::resources::{ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind};
use crate::tests::utils::env::TestComm;
use crate::tests::utils::worker::WorkerBuilder;
use std::time::Duration;

#[test]
fn allocation_time_tier_reuses_existing_workers_without_extra_tier() {
    for (task_hours, remaining_seconds) in [(1, 3601), (24, 86401), (48, 172801), (72, 259201)] {
        let mut rt = TestEnv::new();
        rt.new_worker(&WorkerBuilder::new(2).time_limit_s(remaining_seconds));
        let task = rt.new_task(&TaskBuilder::new().cpus(2).time_request(task_hours * 3600));
        // ServerRef::new_worker_query schedules existing workers before planning allocations.
        rt.schedule();
        assert!(rt.core().get_task(task).is_assigned());
        let queries = [0, 3, 12, 24, 72, 168]
            .windows(2)
            .map(|hours| WorkerTypeQuery {
                allocation_min_task_memory: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: Some(Duration::from_secs(hours[1] * 3600)),
                allocation_task_time_range: Some(
                    Duration::from_secs(hours[0] * 3600)..Duration::from_secs(hours[1] * 3600),
                ),
                max_sn_workers: 1,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            })
            .collect::<Vec<_>>();
        let response = compute_new_worker_query(rt.core(), &queries);
        assert_eq!(response.single_node_workers_per_query, vec![0; 5]);
        assert!(rt.core().get_task(task).is_assigned());
    }
}

#[test]
fn allocation_time_tier_uses_next_tier_when_existing_worker_has_insufficient_time() {
    let mut rt = TestEnv::new();
    rt.new_worker(&WorkerBuilder::new(2).time_limit_s(23 * 3600));
    rt.new_task(&TaskBuilder::new().cpus(2).time_request(24 * 3600));
    let queries = [0, 3, 12, 24, 72, 168]
        .windows(2)
        .map(|hours| WorkerTypeQuery {
            allocation_min_task_memory: None,
            partial: false,
            descriptor: ResourceDescriptor::simple_cpus(2),
            time_limit: Some(Duration::from_secs(hours[1] * 3600)),
            allocation_task_time_range: Some(
                Duration::from_secs(hours[0] * 3600)..Duration::from_secs(hours[1] * 3600),
            ),
            max_sn_workers: 1,
            max_workers_per_allocation: 1,
            min_utilization: 0.0,
        })
        .collect::<Vec<_>>();
    let response = compute_new_worker_query(rt.core(), &queries);
    assert_eq!(response.single_node_workers_per_query, vec![0, 0, 0, 1, 0]);
}

#[test]
fn test_query_no_tasks() {
    let mut core = Core::default();
    let r = compute_new_worker_query(
        &mut core,
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
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
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[2, 3]);
    rt.new_tasks_cpus(&[3, 1, 1]);

    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
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
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[2, 3]);
    rt.new_tasks_cpus(&[3, 3, 1]);
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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

    let w1 = rt.new_worker_cpus(2);

    rt.new_task_running(&TaskBuilder::new(), w1);
    rt.new_task_assigned(&TaskBuilder::new(), w1);
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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

    let w1 = rt.new_worker_cpus(2);

    let t = TaskBuilder::new();
    rt.new_task_running(&t, w1);
    rt.new_task_assigned(&t, w1);
    rt.new_task(&t);
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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

    rt.new_workers_cpus(&[4, 4, 4]);
    rt.new_tasks(100, &TaskBuilder::new());

    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(2),
                time_limit: None,
                max_sn_workers: 5,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(1),
                time_limit: None,
                max_sn_workers: 1,
                max_workers_per_allocation: 1,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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

    rt.new_workers_cpus(&[4, 4, 4]);

    rt.new_tasks(5, &TaskBuilder::new().n_nodes(3));
    rt.new_tasks(10, &TaskBuilder::new().n_nodes(6));
    rt.new_tasks(5, &TaskBuilder::new().n_nodes(12));
    rt.new_tasks(20, &TaskBuilder::new().n_nodes(3).user_priority(10));
    rt.new_task(&TaskBuilder::new().n_nodes(1));

    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(1),
                time_limit: None,
                max_sn_workers: 1,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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

    rt.new_task(&TaskBuilder::new().n_nodes(4).time_request(750));
    rt.schedule();

    for (secs, allocs) in [(740, 0), (760, 1)] {
        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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
    let mut rt = TestEnv::new();
    rt.new_tasks_cpus(&[3, 1, 1]);

    rt.schedule();

    for (min_utilization, alloc_value, cpus) in &[
        (0.5, 0, 12),
        (0.3, 1, 12),
        (0.8, 0, 12),
        (1.0, 1, 5),
        (0.5, 2, 3),
        (0.7, 1, 3),
    ] {
        let r = compute_new_worker_query(
            &mut rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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
    let mut rt = TestEnv::new();
    rt.new_named_resource("gpus");
    rt.new_tasks(2, &TaskBuilder::new().cpus(10).add_resource(1, 20));

    rt.schedule();

    for (min_utilization, alloc_value, cpus, gpus) in &[
        (0.49, 1, 29, 40),
        (0.49, 0, 29, 30),
        (0.67, 0, 41, 30),
        (0.50, 0, 41, 200),
        (0.45, 1, 39, 200),
    ] {
        let descriptor = ResourceDescriptor::new(
            vec![
                ResourceDescriptorItem {
                    name: "cpus".into(),
                    kind: ResourceDescriptorKind::simple_indices(*cpus),
                },
                ResourceDescriptorItem {
                    name: "gpus".into(),
                    kind: ResourceDescriptorKind::simple_indices(*gpus),
                },
            ],
            Default::default(),
        );
        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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
    let mut rt = TestEnv::new();
    rt.new_tasks(2, &TaskBuilder::new().cpus(2));

    let descriptor = ResourceDescriptor::new(
        vec![ResourceDescriptorItem {
            name: "cpus".into(),
            kind: ResourceDescriptorKind::simple_indices(4),
        }],
        Default::default(),
    );
    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
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
fn test_query_min_utilization_vs_partial() {
    for (cpu_tasks, gpu_tasks, alloc) in [
        (1, 0, 0),
        (2, 0, 1),
        (3, 0, 1),
        (4, 1, 2),
        (1, 1, 1),
        (2, 1, 1),
        (3, 1, 2),
        (4, 1, 2),
        (0, 1, 0),
        (0, 2, 1),
        (0, 3, 1),
        (0, 4, 2),
        (0, 0, 0),
    ] {
        let mut rt = TestEnv::new();
        rt.new_named_resource("gpus");
        rt.new_tasks(cpu_tasks, &TaskBuilder::new().cpus(2));
        rt.new_tasks(gpu_tasks, &TaskBuilder::new().cpus(2).add_resource(1, 1));

        let descriptor = ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: "cpus".into(),
                kind: ResourceDescriptorKind::simple_indices(4),
            }],
            Default::default(),
        );
        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true, // !!! Worker is partial!
                descriptor,
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 1.0,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![alloc]);
        assert!(r.multi_node_allocations.is_empty());
    }
}

#[test]
fn test_query_min_utilization_vs_partial2() {
    for (cpu_tasks, alloc) in [(1, 1), (2, 1), (3, 1), (4, 1), (0, 0)] {
        let mut rt = TestEnv::new();
        rt.new_tasks(cpu_tasks, &TaskBuilder::new().cpus(2));

        let descriptor = ResourceDescriptor::new(vec![], Default::default());
        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true, // !!! Worker is partial!
                descriptor,
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 1,
                min_utilization: 1.0,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![alloc]);
        assert!(r.multi_node_allocations.is_empty());
    }
}

#[test]
fn test_query_min_time2() {
    let mut rt = TestEnv::new();
    let t1 = TaskBuilder::new()
        .cpus(1)
        .time_request(100)
        .next_variant()
        .cpus(4)
        .time_request(50);
    rt.new_task(&t1);
    rt.schedule();

    for (cpus, secs, alloc) in [(2, 75, 0), (1, 101, 1), (4, 50, 1)] {
        let descriptor = ResourceDescriptor::new(
            vec![ResourceDescriptorItem {
                name: "cpus".into(),
                kind: ResourceDescriptorKind::simple_indices(cpus),
            }],
            Default::default(),
        );
        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
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
    let mut rt = TestEnv::new();
    rt.new_task(&TaskBuilder::new().cpus(1).time_request(100));
    rt.new_task(&TaskBuilder::new().cpus(10).time_request(100));

    rt.schedule();

    let descriptor = ResourceDescriptor::new(
        vec![ResourceDescriptorItem {
            name: "cpus".into(),
            kind: ResourceDescriptorKind::simple_indices(10),
        }],
        Default::default(),
    );
    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
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
        &mut rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
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

    let descriptor = ResourceDescriptor::new(
        vec![ResourceDescriptorItem {
            name: "cpus".into(),
            kind: ResourceDescriptorKind::simple_indices(1),
        }],
        Default::default(),
    );
    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
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

        rt.new_workers_cpus(&[4]);
        rt.new_tasks(n, &TaskBuilder::new().cpus(1).time_request(5_000));

        rt.schedule();

        let r = compute_new_worker_query(
            rt.core(),
            &[
                WorkerTypeQuery {
                    allocation_min_task_memory: None,
                    allocation_task_time_range: None,
                    partial: false,
                    descriptor: ResourceDescriptor::simple_cpus(2),
                    time_limit: None,
                    max_sn_workers: 2,
                    max_workers_per_allocation: 1,
                    min_utilization: 0.0,
                },
                WorkerTypeQuery {
                    allocation_min_task_memory: None,
                    allocation_task_time_range: None,
                    partial: true,
                    descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
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
    for (cpus, out) in [(1, 0), (2, 3)] {
        let mut rt = TestEnv::new();
        rt.new_tasks(100, &TaskBuilder::new().cpus(2));
        rt.schedule();

        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::simple_cpus(cpus),
                time_limit: None,
                max_sn_workers: 3,
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

    rt.new_task(&TaskBuilder::new().cpus(4).time_request(750));
    rt.new_task(&TaskBuilder::new().cpus(8).time_request(1750));
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
                time_limit: Some(Duration::from_secs(1000)),
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
                time_limit: Some(Duration::from_secs(50)),
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
                time_limit: None,
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![1, 0, 1]);
}

#[test]
fn test_query_partial_query_cpus() {
    let mut rt = TestEnv::new();

    rt.new_task_cpus(4);
    rt.new_tasks(4, &TaskBuilder::new().cpus(8));
    rt.schedule();

    let r = compute_new_worker_query(
        rt.core(),
        &[
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::simple_cpus(4),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::simple_cpus(16),
                time_limit: Some(Duration::from_secs(50)),
                max_sn_workers: 5,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
                time_limit: None,
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
        ],
    );
    assert_eq!(r.single_node_workers_per_query, vec![1, 2, 0]);
}

#[test]
fn test_query_partial_query_gpus1() {
    for (gpus, has_extra, out) in [
        (Some(4), false, 3),
        (Some(4), true, 3),
        (None, false, 2),
        (None, true, 2),
        (Some(0), false, 0),
        (Some(0), true, 0),
        (Some(100), false, 2),
        (Some(100), true, 2),
    ] {
        let mut rt = TestEnv::new();
        rt.new_named_resource("gpus");
        rt.new_named_resource("foo");
        let mut builder = TaskBuilder::new().cpus(1).add_resource(1, 2);
        if has_extra {
            builder = builder.add_resource(2, 1);
        }
        rt.new_tasks(10, &builder);
        rt.schedule();

        let mut items = vec![ResourceDescriptorItem {
            name: "cpus".into(),
            kind: ResourceDescriptorKind::simple_indices(8),
        }];
        if let Some(gpus) = gpus {
            items.push(ResourceDescriptorItem {
                name: "gpus".into(),
                kind: ResourceDescriptorKind::simple_indices(gpus),
            });
        }
        let descriptor = ResourceDescriptor::new(items, Default::default());

        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
                allocation_min_task_memory: None,
                allocation_task_time_range: None,
                partial: true,
                descriptor,
                time_limit: None,
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(r.single_node_workers_per_query, vec![out]);
    }
}

#[test]
fn test_query_unknown_do_not_add_extra() {
    let mut rt = TestEnv::new();
    rt.new_task_default();
    rt.new_task(&TaskBuilder::new().cpus(1).add_resource(1, 1));
    rt.new_task_default();
    rt.new_task(&TaskBuilder::new().cpus(1).add_resource(1, 1));

    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
            partial: true,
            descriptor: ResourceDescriptor::simple_cpus(1),
            time_limit: None,
            max_sn_workers: 5,
            max_workers_per_allocation: 3,
            min_utilization: 0.0,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![2]);
}

#[test]
fn test_query_after_task_cancel() {
    let mut rt = TestEnv::new();
    let t1 = rt.new_task_cpus(10);
    rt.new_worker(&WorkerBuilder::new(1));
    rt.schedule();
    let mut comm = TestComm::new();
    on_cancel_tasks(rt.core(), &mut comm, &[t1]);
    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
            allocation_min_task_memory: None,
            allocation_task_time_range: None,
            partial: true,
            descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
            time_limit: None,
            max_sn_workers: 5,
            max_workers_per_allocation: 3,
            min_utilization: 0.0,
        }],
    );
    assert_eq!(r.single_node_workers_per_query, vec![0]);
}

fn large_memory_query(cpus: u32, count: u32) -> WorkerTypeQuery {
    let mut resources = ResourceDescriptor::simple_cpus(cpus).resources;
    resources.push(ResourceDescriptorItem::sum("mem", 1536000));
    resources.push(ResourceDescriptorItem::sum("worker/cpuLarge", cpus));
    WorkerTypeQuery {
        descriptor: ResourceDescriptor::new(resources, Default::default()),
        partial: false,
        time_limit: Some(Duration::from_secs(72 * 3600)),
        allocation_task_time_range: Some(
            Duration::from_secs(24 * 3600)..Duration::from_secs(72 * 3600),
        ),
        allocation_min_task_memory: Some(766000.into()),
        max_sn_workers: count,
        max_workers_per_allocation: 1,
        min_utilization: 0.5,
    }
}

#[test]
fn large_memory_allocation_requires_single_task_strictly_above_threshold() {
    for memory in [765999, 766000, 766001] {
        let mut rt = TestEnv::new();
        let mem = rt.new_named_resource("mem");
        let class = rt.new_named_resource("worker/cpuLarge");
        rt.new_task(
            &TaskBuilder::new()
                .cpus(24)
                .add_resource(mem, memory)
                .add_resource(class, 1)
                .time_request(24 * 3600),
        );
        let response = compute_new_worker_query(rt.core(), &[large_memory_query(48, 1)]);
        assert_eq!(
            response.single_node_workers_per_query,
            vec![u32::from(memory > 766000)]
        );
    }
}

#[test]
fn large_memory_allocation_cannot_be_triggered_by_sum_of_small_tasks() {
    let mut rt = TestEnv::new();
    let mem = rt.new_named_resource("mem");
    let class = rt.new_named_resource("worker/cpuLarge");
    rt.new_tasks(
        48,
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(mem, 20000)
            .add_resource(class, 1)
            .time_request(24 * 3600),
    );
    let response = compute_new_worker_query(rt.core(), &[large_memory_query(48, 3)]);
    assert_eq!(response.single_node_workers_per_query, vec![0]);
}

#[test]
fn large_memory_allocation_small_tasks_fill_but_do_not_trigger_extra_workers() {
    let mut rt = TestEnv::new();
    let mem = rt.new_named_resource("mem");
    let class = rt.new_named_resource("worker/cpuLarge");
    rt.new_task(
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(mem, 800000)
            .add_resource(class, 1)
            .time_request(24 * 3600),
    );
    rt.new_tasks(
        100,
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(mem, 1)
            .add_resource(class, 1)
            .time_request(24 * 3600),
    );
    let response = compute_new_worker_query(rt.core(), &[large_memory_query(48, 3)]);
    assert_eq!(response.single_node_workers_per_query, vec![1]);
}

#[test]
fn large_memory_allocation_still_requires_fifty_percent_cpu_demand() {
    for cpus in [23, 24] {
        let mut rt = TestEnv::new();
        let mem = rt.new_named_resource("mem");
        let class = rt.new_named_resource("worker/cpuLarge");
        rt.new_task(
            &TaskBuilder::new()
                .cpus(cpus)
                .add_resource(mem, 800000)
                .add_resource(class, 1)
                .time_request(24 * 3600),
        );
        let response = compute_new_worker_query(rt.core(), &[large_memory_query(48, 1)]);
        assert_eq!(
            response.single_node_workers_per_query,
            vec![u32::from(cpus >= 24)]
        );
    }
}

#[test]
fn large_memory_allocation_connected_worker_accepts_small_tasks_only_from_same_class() {
    let mut rt = TestEnv::new();
    let mem = rt.new_named_resource("mem");
    let class = rt.new_named_resource("worker/cpuLarge");
    let base = rt.new_named_resource("worker/cpu");
    rt.new_worker(
        &WorkerBuilder::new(48)
            .res_sum("mem", 1536000)
            .res_sum("worker/cpuLarge", 48)
            .time_limit_s(3601),
    );
    let small = rt.new_task(
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(mem, 100)
            .add_resource(class, 1)
            .time_request(3600),
    );
    let regular = rt.new_task(
        &TaskBuilder::new()
            .cpus(1)
            .add_resource(mem, 100)
            .add_resource(base, 1)
            .time_request(3600),
    );
    rt.schedule();
    assert!(rt.core().get_task(small).is_assigned());
    assert!(!rt.core().get_task(regular).is_assigned());
    let response = compute_new_worker_query(rt.core(), &[large_memory_query(48, 1)]);
    assert_eq!(response.single_node_workers_per_query, vec![0]);
}

#[test]
fn large_memory_allocation_missing_memory_pool_is_absent_not_probed() {
    let mut rt = TestEnv::new();
    let class = rt.new_named_resource("worker/cpuLarge");
    rt.new_task(
        &TaskBuilder::new()
            .cpus(24)
            .add_resource(class, 1)
            .time_request(24 * 3600),
    );
    let mut query = large_memory_query(48, 1);
    query
        .descriptor
        .resources
        .retain(|resource| resource.name != "mem");
    let response = compute_new_worker_query(rt.core(), &[query]);
    assert_eq!(response.single_node_workers_per_query, vec![0]);
}
