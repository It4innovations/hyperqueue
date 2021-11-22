use criterion::{criterion_group, criterion_main};

use crate::utils::{add_tasks, create_task, create_worker, NullComm};

mod benchmarks;
mod utils;

criterion_group!(
    benches,
    benchmarks::core::benchmark,
    benchmarks::scheduler::benchmark
);
criterion_main!(benches);
