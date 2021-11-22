use criterion::{BatchSize, BenchmarkId, Criterion};

use tako::common::Map;
use tako::scheduler::metrics::compute_b_level_metric;
use tako::scheduler::state::SchedulerState;
use tako::server::core::Core;
use tako::server::task::TaskRef;
use tako::TaskId;

use crate::{add_tasks, create_worker, NullComm};

fn bench_b_level(c: &mut Criterion) {
    for task_count in [10, 1_000, 100_000, 100] {
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

pub fn benchmark(c: &mut Criterion) {
    bench_b_level(c);
    bench_schedule(c);
}
