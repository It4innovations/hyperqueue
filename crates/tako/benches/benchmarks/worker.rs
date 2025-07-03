use std::rc::Rc;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use smallvec::smallvec;
use tako::TaskId;
use tako::gateway::TaskDataFlags;
use tako::internal::messages::worker::ComputeTaskMsg;
use tako::internal::tests::utils::shared::res_allocator_from_descriptor;
use tako::internal::worker::comm::WorkerComm;
use tako::internal::worker::rqueue::ResourceWaitQueue;
use tako::internal::worker::state::{TaskMap, WorkerStateRef};
use tako::internal::worker::task::{Task, TaskState};
use tako::launcher::{StopReason, TaskBuildContext, TaskLaunchData, TaskLauncher, TaskResult};
use tako::resources::ResourceAmount;
use tako::resources::{
    AllocationRequest, CPU_RESOURCE_NAME, NVIDIA_GPU_RESOURCE_NAME, ResourceAllocRequest,
    ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind, ResourceRequest,
    ResourceRequestVariants, TimeRequest,
};
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

fn create_worker_state() -> WorkerStateRef {
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
        Box::new(BenchmarkTaskLauncher),
        "testuid".to_string(),
    )
}

fn create_worker_task(id: u32) -> Task {
    Task::new(
        ComputeTaskMsg {
            id: TaskId::new_test(id),
            instance_id: Default::default(),
            user_priority: 0,
            scheduler_priority: 0,
            resources: Default::default(),
            time_limit: None,
            node_list: vec![],
            data_deps: vec![],
            data_flags: TaskDataFlags::empty(),
            body: Default::default(),
            entry: None,
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
                        let tasks: Vec<_> = (0..task_count).map(create_worker_task).collect();
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
    let descriptor = ResourceDescriptor::new(vec![
        ResourceDescriptorItem {
            name: CPU_RESOURCE_NAME.to_string(),
            kind: ResourceDescriptorKind::simple_indices(num_cpus),
        },
        ResourceDescriptorItem {
            name: NVIDIA_GPU_RESOURCE_NAME.to_string(),
            kind: ResourceDescriptorKind::simple_indices(8),
        },
    ]);
    ResourceWaitQueue::new(res_allocator_from_descriptor(descriptor))
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
                task.resources = ResourceRequestVariants::new(smallvec![ResourceRequest::new(
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
                )]);
                queue.add_task(&task);

                let mut map = TaskMap::default();
                map.insert(task);

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
                            task.resources =
                                ResourceRequestVariants::new(smallvec![ResourceRequest::new(
                                    0,
                                    TimeRequest::new(0, 0),
                                    smallvec![
                                        ResourceAllocRequest {
                                            resource_id: 0.into(),
                                            request: AllocationRequest::Compact(
                                                ResourceAmount::new_units(64)
                                            ),
                                        },
                                        ResourceAllocRequest {
                                            resource_id: 1.into(),
                                            request: AllocationRequest::Compact(
                                                ResourceAmount::new_units(2)
                                            ),
                                        },
                                    ],
                                )]);
                            queue.add_task(&task);
                            map.insert(task);
                        }

                        (queue, map)
                    },
                    |(queue, map)| queue.try_start_tasks(map, None),
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
