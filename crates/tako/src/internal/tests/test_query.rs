use crate::gateway::WorkerTypeQuery;
use crate::internal::scheduler::query::compute_new_worker_query;
use crate::internal::server::core::Core;
use crate::internal::tests::utils::env::{create_test_comm, TestEnv};
use crate::internal::tests::utils::schedule::{
    create_test_scheduler, create_test_workers, submit_test_tasks,
};
use crate::internal::tests::utils::task::TaskBuilder;
use crate::resources::ResourceDescriptor;

#[test]
fn test_query_no_tasks() {
    let core = Core::default();
    let r = compute_new_worker_query(
        &core,
        &[WorkerTypeQuery {
            descriptor: ResourceDescriptor::simple(4),
            max_sn_workers: 2,
            max_worker_per_allocation: 1,
        }],
    );
    assert_eq!(r.single_node_allocations, vec![0]);
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
        &core,
        &[WorkerTypeQuery {
            descriptor: ResourceDescriptor::simple(4),
            max_sn_workers: 2,
            max_worker_per_allocation: 1,
        }],
    );
    assert_eq!(r.single_node_allocations, vec![0]);
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
        &core,
        &[
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(2),
                max_sn_workers: 2,
                max_worker_per_allocation: 1,
            },
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(3),
                max_sn_workers: 2,
                max_worker_per_allocation: 1,
            },
        ],
    );
    assert_eq!(r.single_node_allocations, vec![0, 1]);
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
        &rt.core(),
        &[
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(2),
                max_sn_workers: 2,
                max_worker_per_allocation: 1,
            },
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(3),
                max_sn_workers: 2,
                max_worker_per_allocation: 1,
            },
        ],
    );
    assert_eq!(r.single_node_allocations, vec![0, 0]);
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
        &rt.core(),
        &[
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(2),
                max_sn_workers: 2,
                max_worker_per_allocation: 1,
            },
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(3),
                max_sn_workers: 2,
                max_worker_per_allocation: 1,
            },
        ],
    );
    assert_eq!(r.single_node_allocations, vec![1, 0]);
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
        &rt.core(),
        &[
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(2),
                max_sn_workers: 5,
                max_worker_per_allocation: 1,
            },
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(1),
                max_sn_workers: 1,
                max_worker_per_allocation: 1,
            },
            WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(3),
                max_sn_workers: 200,
                max_worker_per_allocation: 1,
            },
        ],
    );
    assert_eq!(r.single_node_allocations, vec![5, 1, 26]);
    assert!(r.multi_node_allocations.is_empty());
}
