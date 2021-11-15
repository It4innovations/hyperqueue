use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use tako::server::core::Core;
use tako::server::task::TaskRef;

fn remove_single_task(c: &mut Criterion) {
    for task_count in [10, 100, 1_000, 10_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("remove a single task", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        for id in 0..task_count {
                            let task = TaskRef::new(
                                id.into(),
                                vec![],
                                Default::default(),
                                0,
                                false,
                                false,
                            );
                            core.add_task(task.clone());
                        }
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

fn core_benchmark(c: &mut Criterion) {
    remove_single_task(c);
}

criterion_group!(benches, core_benchmark);
criterion_main!(benches);
