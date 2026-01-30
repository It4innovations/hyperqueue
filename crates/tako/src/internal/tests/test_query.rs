use crate::TaskId;
use crate::control::WorkerTypeQuery;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::server::core::Core;
use crate::internal::server::reactor::on_cancel_tasks;
use crate::internal::tests::utils::env::{TestEnv, create_test_comm};
use crate::internal::tests::utils::schedule::create_test_scheduler;
use crate::internal::tests::utils::task::TaskBuilder;
use crate::resources::{ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind};
use crate::tests::utils::worker::WorkerBuilder;
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
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[2, 3]);
    rt.new_tasks_cpus(&[3, 1, 1]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(rt.core(), &mut comm);

    let r = compute_new_worker_query(
        rt.core(),
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
    let mut rt = TestEnv::new();
    rt.new_workers_cpus(&[2, 3]);
    rt.new_tasks_cpus(&[3, 3, 1]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(rt.core(), &mut comm);

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
    assert_eq!(r.single_node_workers_per_query, vec![0, 1]);
    assert!(r.multi_node_allocations.is_empty());
}

#[test]
fn test_query_enough_workers2() {
    let mut rt = TestEnv::new();

    rt.new_workers_cpus(&[2]);

    rt.new_task_running(&TaskBuilder::new(), 100);
    rt.new_task_assigned(&TaskBuilder::new(), 100);
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

    let w1 = rt.new_worker_cpus(2);

    let t = TaskBuilder::new().cpus(1);
    rt.new_task_running(&t, w1);
    rt.new_task_running(&t, w1);
    rt.new_task_assigned(&t, w1);
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

    rt.new_workers_cpus(&[4, 4, 4]);
    rt.new_tasks(100, &TaskBuilder::new());

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

    rt.new_task(&TaskBuilder::new().n_nodes(4).time_request(750));
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
    let mut rt = TestEnv::new();
    rt.new_tasks_cpus(&[3, 1, 1]);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(&mut rt.core(), &mut comm);

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
    rt.new_tasks(2, &TaskBuilder::new().cpus(1).add_resource(1, 10));

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(rt.core(), &mut comm);

    for (min_utilization, alloc_value, cpus, gpus) in &[
        (0.5, 1, 12, 30),
        (0.67, 0, 12, 30),
        (0.55, 0, 4, 200),
        (0.45, 1, 4, 200),
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
        (3, 1, 1),
        (4, 1, 2),
        (0, 1, 1),
        (0, 2, 1),
        (0, 3, 1),
        (0, 0, 0),
    ] {
        let mut rt = TestEnv::new();
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
                partial: true,
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
        .next_resources()
        .cpus(4)
        .time_request(50);
    rt.new_task(&t1);

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(rt.core(), &mut comm);

    for (cpus, secs, alloc) in [(2, 75, 0), (1, 100, 1), (4, 50, 1)] {
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

    let mut scheduler = create_test_scheduler();
    let mut comm = create_test_comm();
    scheduler.run_scheduling(rt.core(), &mut comm);

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
                    partial: false,
                    descriptor: ResourceDescriptor::simple_cpus(2),
                    time_limit: None,
                    max_sn_workers: 2,
                    max_workers_per_allocation: 1,
                    min_utilization: 0.0,
                },
                WorkerTypeQuery {
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
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
                time_limit: Some(Duration::from_secs(1000)),
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: true,
                descriptor: ResourceDescriptor::new(Vec::new(), Default::default()),
                time_limit: Some(Duration::from_secs(50)),
                max_sn_workers: 3,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
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
                partial: true,
                descriptor: ResourceDescriptor::simple_cpus(4),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
                partial: true,
                descriptor: ResourceDescriptor::simple_cpus(16),
                time_limit: Some(Duration::from_secs(50)),
                max_sn_workers: 5,
                max_workers_per_allocation: 3,
                min_utilization: 0.0,
            },
            WorkerTypeQuery {
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
        (4, false, 3),
        (4, true, 1),
        (0, false, 1),
        (0, true, 1),
        (100, false, 2),
        (100, true, 1),
    ] {
        let mut rt = TestEnv::new();
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
        if gpus > 0 {
            items.push(ResourceDescriptorItem {
                name: "gpus".into(),
                kind: ResourceDescriptorKind::simple_indices(gpus),
            });
        }
        let descriptor = ResourceDescriptor::new(items, Default::default());

        let r = compute_new_worker_query(
            rt.core(),
            &[WorkerTypeQuery {
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
    rt.new_worker_with_id(102, &WorkerBuilder::new(1));
    rt.schedule();
    let mut comm = create_test_comm();
    on_cancel_tasks(rt.core(), &mut comm, &[t1]);
    let r = compute_new_worker_query(
        rt.core(),
        &[WorkerTypeQuery {
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
