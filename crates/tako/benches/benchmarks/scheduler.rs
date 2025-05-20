use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use std::time::Instant;

use crate::{add_tasks, create_worker};
use tako::events::EventProcessor;
use tako::gateway::LostWorkerReason;
use tako::internal::messages::common::TaskFailInfo;
use tako::internal::messages::worker::ToWorkerMessage;
use tako::internal::scheduler::state::SchedulerState;
use tako::internal::server::comm::Comm;
use tako::internal::server::core::Core;
use tako::task::SerializedTaskContext;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, TaskId, WorkerId};

fn bench_schedule(c: &mut BenchmarkGroup<WallTime>) {
    for task_count in [10, 1_000, 100_000] {
        for worker_count in [1, 8, 16, 32] {
            c.bench_with_input(
                BenchmarkId::new(
                    "schedule",
                    format!("tasks={task_count}, workers={worker_count}"),
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

                            let scheduler = SchedulerState::new(Instant::now());
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
    let mut group = c.benchmark_group("scheduler");

    bench_schedule(&mut group);
}

// Utils
struct NullComm;

impl Comm for NullComm {
    fn send_worker_message(&mut self, _worker_id: WorkerId, _message: &ToWorkerMessage) {}

    fn broadcast_worker_message(&mut self, _message: &ToWorkerMessage) {}

    fn ask_for_scheduling(&mut self) {}

    fn client(&mut self) -> &mut dyn EventProcessor {
        unreachable!()
    }
}
