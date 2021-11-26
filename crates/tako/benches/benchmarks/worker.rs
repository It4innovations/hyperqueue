use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use tako::common::index::AsIdVec;
use tako::common::resources::descriptor::GenericResourceKindIndices;
use tako::common::resources::map::ResourceMap;
use tako::common::resources::{
    CpuRequest, GenericResourceDescriptor, GenericResourceDescriptorKind, GenericResourceRequest,
    ResourceDescriptor, ResourceRequest, TimeRequest,
};
use tako::messages::common::TaskConfiguration;
use tokio::sync::mpsc::unbounded_channel;

use tako::messages::worker::ComputeTaskMsg;
use tako::worker::rqueue::ResourceWaitQueue;
use tako::worker::state::{TaskMap, WorkerStateRef};
use tako::worker::task::Task;
use tako::worker::taskenv::TaskResult;
use tako::TaskId;

use crate::create_worker;

fn create_worker_state() -> WorkerStateRef {
    let worker = create_worker(1);
    let (tx, _) = unbounded_channel();
    let (tx2, _) = unbounded_channel();
    WorkerStateRef::new(
        worker.id,
        worker.configuration,
        None,
        tx,
        tx2,
        Default::default(),
        Default::default(),
        Box::new(|_, _, _| Box::pin(async move { Ok(TaskResult::Finished) })),
    )
}

fn create_worker_task(id: u64) -> Task {
    Task::new(ComputeTaskMsg {
        id: id.into(),
        instance_id: Default::default(),
        dep_info: vec![],
        configuration: Default::default(),
        user_priority: 0,
        scheduler_priority: 0,
    })
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

                    for _ in 0..iters {
                        let state = create_worker_state();
                        let mut state = state.get_mut();

                        for id in 0..task_count {
                            state.add_task(create_worker_task(id));
                        }
                        let task = create_worker_task(task_count);

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
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("add tasks", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched(
                    || {
                        let state = create_worker_state();
                        let tasks: Vec<_> =
                            (0..task_count).map(|id| create_worker_task(id)).collect();
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
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("cancel waiting task", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let state = create_worker_state();

                        {
                            let mut state = state.get_mut();
                            for id in 0..task_count {
                                state.add_task(create_worker_task(id));
                            }
                        }
                        (state, TaskId::new(0))
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
    ResourceWaitQueue::new(
        &ResourceDescriptor {
            cpus: vec![(0..num_cpus).collect::<Vec<_>>().to_ids()],
            generic: vec![GenericResourceDescriptor {
                name: "GPU".to_string(),
                kind: GenericResourceDescriptorKind::Indices(GenericResourceKindIndices {
                    start: 0.into(),
                    end: 8.into(),
                }),
            }],
        },
        &ResourceMap::from_vec(vec!["GPU".to_string()]),
    )
}

fn bench_resource_queue_add_task(c: &mut BenchmarkGroup<WallTime>) {
    c.bench_function("add task to resource queue", |b| {
        b.iter_batched_ref(
            || (create_resource_queue(64), create_worker_task(0)),
            |(queue, task)| queue.add_task(task),
            BatchSize::SmallInput,
        );
    });
}

fn bench_resource_queue_release_allocation(c: &mut BenchmarkGroup<WallTime>) {
    c.bench_function("release allocation from resource queue", |b| {
        b.iter_batched_ref(
            || {
                let mut queue = create_resource_queue(64);
                let mut task = create_worker_task(0);
                task.configuration = TaskConfiguration {
                    resources: ResourceRequest::new(
                        CpuRequest::Compact(64),
                        TimeRequest::new(0, 0),
                        vec![GenericResourceRequest {
                            resource: 0.into(),
                            amount: 2,
                        }]
                        .into(),
                    ),
                    n_outputs: 0,
                    time_limit: None,
                    body: vec![],
                };

                queue.add_task(&task);

                let mut map = TaskMap::default();
                map.insert(0.into(), task);

                let mut started = queue.try_start_tasks(&map, None);
                (queue, Some(started.pop().unwrap().1))
            },
            |(queue, allocation)| queue.release_allocation(allocation.take().unwrap()),
            BatchSize::SmallInput,
        );
    });
}

fn bench_resource_queue_start_tasks(c: &mut BenchmarkGroup<WallTime>) {
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
                            let mut task = create_worker_task(id);
                            task.configuration = TaskConfiguration {
                                resources: ResourceRequest::new(
                                    CpuRequest::Compact(64),
                                    TimeRequest::new(0, 0),
                                    vec![GenericResourceRequest {
                                        resource: 0.into(),
                                        amount: 2,
                                    }]
                                    .into(),
                                ),
                                n_outputs: 0,
                                time_limit: None,
                                body: vec![],
                            };

                            queue.add_task(&task);
                            map.insert(id.into(), task);
                        }

                        (queue, map)
                    },
                    |(queue, map)| queue.try_start_tasks(&map, None),
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
