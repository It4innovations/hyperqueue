use std::rc::Rc;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use smallvec::smallvec;
use tako::TaskId;
use tako::gateway::TaskDataFlags;
use tako::internal::messages::worker::{ComputeTaskSeparateData, ComputeTaskSharedData};
use tako::internal::tests::utils::shared::res_allocator_from_descriptor;
use tako::internal::worker::comm::WorkerComm;
use tako::internal::worker::rqueue::ResourceWaitQueue;
use tako::internal::worker::state::{TaskMap, WorkerStateRef};
use tako::internal::worker::task::{Task, TaskState};
use tako::launcher::{StopReason, TaskBuildContext, TaskLaunchData, TaskLauncher, TaskResult};
use tako::resources::{
    AllocationRequest, CPU_RESOURCE_NAME, NVIDIA_GPU_RESOURCE_NAME, ResourceAllocRequest,
    ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, ResourceRequest,
    ResourceRequestVariants, ResourceRqMap, TimeRequest,
};
use tako::resources::{ResourceAmount, ResourceRqId};
use tokio::sync::Notify;
use tokio::sync::mpsc::unbounded_channel;

use crate::create_worker;

struct BenchmarkTaskLauncher;

impl TaskLauncher for BenchmarkTaskLauncher {
    fn build_task(
        &self,
        _ctx: TaskBuildContext,
        _stop_receiver: tokio::sync::oneshot::Receiver<StopReason>,
    ) -> tako::Result<TaskLaunchData> {
        Ok(TaskLaunchData::from_future(Box::pin(async move {
            Ok(TaskResult::Finished)
        })))
    }
}

fn create_worker_state(resource_rq_map: tako::resources::ResourceRqMap) -> WorkerStateRef {
    let worker = create_worker(1);
    let (tx, _) = unbounded_channel();

    let start_task_notify = Rc::new(Notify::new());
    let comm = WorkerComm::new(tx, start_task_notify);

    WorkerStateRef::new(
        comm,
        worker.id(),
        worker.configuration().clone(),
        None,
        Default::default(),
        resource_rq_map,
        Box::new(BenchmarkTaskLauncher),
        "testuid".to_string(),
    )
}

fn create_worker_task(id: u32, resource_rq_id: ResourceRqId) -> Task {
    Task::new(
        ComputeTaskSeparateData {
            shared_index: 0,
            id: TaskId::new_test(id),
            resource_rq_id,
            instance_id: Default::default(),
            scheduler_priority: 0,
            node_list: vec![],
            data_deps: vec![],
            entry: None,
        },
        ComputeTaskSharedData {
            user_priority: 0,
            time_limit: None,
            data_flags: TaskDataFlags::empty(),
            body: Default::default(),
        },
        TaskState::Waiting(0),
    )
}

macro_rules! measure_time {
    ($body: block) => {{
        let start = ::std::time::Instant::now();
        $body
        start.elapsed()
    }}
}

fn bench_add_task(c: &mut BenchmarkGroup<WallTime>) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("add task", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::new(0, 0);
                    let mut resource_map = ResourceRqMap::default();
                    let rq_id = resource_map.insert(ResourceRequestVariants::new_cpu1());
                    for _ in 0..iters {
                        let state = create_worker_state(resource_map.clone());
                        let mut state = state.get_mut();

                        for id in 0..task_count {
                            state.add_task(create_worker_task(id, rq_id));
                        }
                        let task = create_worker_task(task_count, rq_id);

                        let duration = measure_time!({
                            state.add_task(task);
                        });

                        total += duration;
                    }
                    total
                });
            },
        );
    }
}

fn bench_add_tasks(c: &mut BenchmarkGroup<WallTime>) {
    let mut resource_map = ResourceRqMap::default();
    let rq_id = resource_map.insert(ResourceRequestVariants::new_cpu1());
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("add tasks", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched(
                    || {
                        let state = create_worker_state(resource_map.clone());
                        let tasks: Vec<_> = (0..task_count)
                            .map(|x| create_worker_task(x, rq_id))
                            .collect();
                        (state, tasks)
                    },
                    |(state, tasks)| {
                        let mut state = state.get_mut();
                        for task in tasks {
                            state.add_task(task);
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_cancel_waiting_task(c: &mut BenchmarkGroup<WallTime>) {
    let mut resource_map = ResourceRqMap::default();
    let rq_id = resource_map.insert(ResourceRequestVariants::new_cpu1());
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("cancel waiting task", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let state = create_worker_state(resource_map.clone());

                        {
                            let mut state = state.get_mut();
                            for id in 0..task_count {
                                state.add_task(create_worker_task(id, rq_id));
                            }
                        }
                        (state, TaskId::new_test(0))
                    },
                    |(state, task_id)| {
                        let mut state = state.get_mut();
                        state.cancel_task(*task_id);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn create_resource_queue(num_cpus: u32) -> ResourceWaitQueue {
    let descriptor = ResourceDescriptor::new(
        vec![
            ResourceDescriptorItem {
                name: CPU_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::simple_indices(num_cpus),
            },
            ResourceDescriptorItem {
                name: NVIDIA_GPU_RESOURCE_NAME.to_string(),
                kind: ResourceDescriptorKind::simple_indices(8),
            },
        ],
        Default::default(),
    );
    ResourceWaitQueue::new(res_allocator_from_descriptor(descriptor))
}

fn bench_resource_queue_add_task(c: &mut BenchmarkGroup<WallTime>) {
    let mut resource_map = ResourceRqMap::default();
    let rq_id = resource_map.insert(ResourceRequestVariants::new_cpu1());
    c.bench_function("add task to resource queue", |b| {
        b.iter_batched_ref(
            || (create_resource_queue(64), create_worker_task(0, rq_id)),
            |(queue, task)| queue.add_task(&resource_map, task),
            BatchSize::SmallInput,
        );
    });
}

fn bench_resource_queue_release_allocation(c: &mut BenchmarkGroup<WallTime>) {
    let mut resource_map = ResourceRqMap::default();
    let rq_id = resource_map.insert(ResourceRequestVariants::new(smallvec![
        ResourceRequest::new(
            0,
            TimeRequest::new(0, 0),
            smallvec![
                ResourceAllocRequest {
                    resource_id: 0.into(),
                    request: AllocationRequest::Compact(ResourceAmount::new_units(64)),
                },
                ResourceAllocRequest {
                    resource_id: 1.into(),
                    request: AllocationRequest::Compact(ResourceAmount::new_units(2)),
                },
            ],
        )
    ]));
    c.bench_function("release allocation from resource queue", |b| {
        b.iter_batched_ref(
            || {
                let mut queue = create_resource_queue(64);
                let task = create_worker_task(0, rq_id);
                queue.add_task(&resource_map, &task);

                let mut map = TaskMap::default();
                map.insert(task);

                let mut started = queue.try_start_tasks(&map, &resource_map, None);
                (queue, Some(started.pop().unwrap().1))
            },
            |(queue, allocation)| queue.release_allocation(allocation.take().unwrap()),
            BatchSize::SmallInput,
        );
    });
}

fn bench_resource_queue_start_tasks(c: &mut BenchmarkGroup<WallTime>) {
    let mut resource_map = ResourceRqMap::default();
    let rq_id = resource_map.insert(ResourceRequestVariants::new(smallvec![
        ResourceRequest::new(
            0,
            TimeRequest::new(0, 0),
            smallvec![
                ResourceAllocRequest {
                    resource_id: 0.into(),
                    request: AllocationRequest::Compact(ResourceAmount::new_units(64)),
                },
                ResourceAllocRequest {
                    resource_id: 1.into(),
                    request: AllocationRequest::Compact(ResourceAmount::new_units(2)),
                },
            ],
        )
    ]));
    for task_count in [1, 10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("start tasks in resource queue", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut queue = create_resource_queue(64);
                        let mut map = TaskMap::default();

                        for id in 0..task_count {
                            let task = create_worker_task(id, rq_id);
                            queue.add_task(&resource_map, &task);
                            map.insert(task);
                        }

                        (queue, map)
                    },
                    |(queue, map)| queue.try_start_tasks(map, &resource_map, None),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

pub fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("worker");
    bench_add_task(&mut group);
    bench_add_tasks(&mut group);
    bench_cancel_waiting_task(&mut group);
    bench_resource_queue_add_task(&mut group);
    bench_resource_queue_release_allocation(&mut group);
    bench_resource_queue_start_tasks(&mut group);
}
