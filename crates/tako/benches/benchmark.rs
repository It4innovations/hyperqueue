use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use tako::common::{Map, Set};
use tako::scheduler::metrics::compute_b_level_metric;
use tako::scheduler::state::SchedulerState;
use tako::server::core::Core;
use tako::server::task::TaskRef;
use tako::TaskId;

use crate::utils::{add_tasks, create_task, create_worker, NullComm};

mod utils;

fn bench_remove_single_task(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("remove a single task", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        add_tasks(&mut core, task_count);
                        let task = core.get_task_by_id_or_panic(0.into()).clone();
                        (core, task)
                    },
                    |(ref mut core, task)| {
                        let _ = core.remove_task(&mut task.get_mut());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_remove_all_tasks(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("remove all tasks", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        let tasks: Set<_> = add_tasks(&mut core, task_count).into_iter().collect();
                        (core, tasks)
                    },
                    |(ref mut core, tasks)| {
                        core.remove_tasks_batched(tasks);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_add_task(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("add task", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        add_tasks(&mut core, task_count);

                        let task = create_task((task_count + 1) as u64);
                        (core, task.clone())
                    },
                    |(ref mut core, task)| {
                        core.add_task(task.clone());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_add_tasks(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("add tasks", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let core = Core::default();
                        let tasks: Vec<_> = (0..task_count).map(|id| create_task(id)).collect();
                        (core, tasks)
                    },
                    |(ref mut core, tasks)| {
                        for task in tasks {
                            core.add_task(task.clone());
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn core_benchmark(c: &mut Criterion) {
    bench_remove_single_task(c);
    bench_remove_all_tasks(c);
    bench_add_task(c);
    bench_add_tasks(c);
}

fn bench_b_level(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("compute b-level", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        let tasks: Map<TaskId, TaskRef> = add_tasks(&mut core, task_count)
                            .into_iter()
                            .map(|t| {
                                let id = t.get().id();
                                (id, t)
                            })
                            .collect();
                        (core, tasks)
                    },
                    |(_core, tasks)| {
                        compute_b_level_metric(tasks);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_schedule(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        for worker_count in [1, 8, 16, 32] {
            c.bench_with_input(
                BenchmarkId::new(
                    "schedule",
                    format!("tasks={}, workers={}", task_count, worker_count),
                ),
                &(task_count, worker_count),
                |b, &(task_count, worker_count)| {
                    b.iter_batched_ref(
                        || {
                            let mut core = Core::default();
                            add_tasks(&mut core, task_count);

                            for worker_id in 0..worker_count {
                                core.new_worker(create_worker(worker_id as u64));
                            }

                            let scheduler = SchedulerState::new();
                            (core, scheduler)
                        },
                        |(core, scheduler)| {
                            scheduler.run_scheduling(core, &mut NullComm);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

fn scheduler_benchmark(c: &mut Criterion) {
    bench_b_level(c);
    bench_schedule(c);
}

criterion_group!(benches, core_benchmark, scheduler_benchmark);
criterion_main!(benches);
