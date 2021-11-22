use criterion::{criterion_group, criterion_main};

use crate::utils::{add_tasks, create_task, create_worker};

mod benchmarks;
mod utils;

criterion_group!(core, benchmarks::core::benchmark);
criterion_group!(scheduler, benchmarks::scheduler::benchmark);
criterion_group!(worker, benchmarks::worker::benchmark);

criterion_main!(core, scheduler, worker);
