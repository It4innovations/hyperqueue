use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use tako::common::Set;
use tako::server::core::Core;
use tako::server::task::TaskRef;

fn create_task(id: u64) -> TaskRef {
    TaskRef::new(id.into(), vec![], Default::default(), 0, false, false)
}

fn add_tasks(core: &mut Core, count: usize) -> Vec<TaskRef> {
    let mut tasks = Vec::with_capacity(count);
    for id in 0..count {
        let task = create_task(id as u64);
        core.add_task(task.clone());
        tasks.push(task);
    }
    tasks
}

fn remove_single_task(c: &mut Criterion) {
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

fn remove_all_tasks(c: &mut Criterion) {
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

fn add_task(c: &mut Criterion) {
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

fn core_benchmark(c: &mut Criterion) {
    remove_single_task(c);
    remove_all_tasks(c);
    add_task(c);
}

criterion_group!(benches, core_benchmark);
criterion_main!(benches);
