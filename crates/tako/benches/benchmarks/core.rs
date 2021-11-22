use criterion::{black_box, BatchSize, BenchmarkId, Criterion};

use tako::common::Set;
use tako::server::core::Core;

use crate::{add_tasks, create_task};

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

fn bench_iterate_tasks(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("iterate tasks", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        add_tasks(&mut core, task_count);
                        core
                    },
                    |ref mut core| {
                        let mut sum = 0;
                        for task in core.get_tasks() {
                            sum += task.get().id().as_num();
                        }
                        black_box(sum);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

pub fn benchmark(c: &mut Criterion) {
    bench_remove_single_task(c);
    bench_remove_all_tasks(c);
    bench_add_task(c);
    bench_add_tasks(c);
    bench_iterate_tasks(c);
}
