use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use tokio::sync::mpsc::unbounded_channel;

use tako::messages::worker::ComputeTaskMsg;
use tako::worker::state::WorkerStateRef;
use tako::worker::task::TaskRef;
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

fn create_worker_task(id: u64) -> TaskRef {
    TaskRef::new(ComputeTaskMsg {
        id: id.into(),
        instance_id: Default::default(),
        dep_info: vec![],
        configuration: Default::default(),
        user_priority: 0,
        scheduler_priority: 0,
    })
}

fn bench_add_task(c: &mut BenchmarkGroup<WallTime>) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("add task", task_count),
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
                        (state, create_worker_task(task_count))
                    },
                    |(state, task)| {
                        let mut state = state.get_mut();
                        state.add_task(task.clone());
                        state.self_ref = None;
                    },
                    BatchSize::SmallInput,
                );
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
                b.iter_batched_ref(
                    || {
                        let state = create_worker_state();
                        let tasks: Vec<_> =
                            (0..task_count).map(|id| create_worker_task(id)).collect();
                        (state, tasks)
                    },
                    |(state, tasks)| {
                        let mut state = state.get_mut();
                        for task in tasks {
                            state.add_task(task.clone());
                        }
                        state.self_ref = None;
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
                        state.self_ref = None;
                    },
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
}
