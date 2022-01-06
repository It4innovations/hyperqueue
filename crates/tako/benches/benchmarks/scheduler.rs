use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};

use tako::messages::common::{TaskFailInfo, WorkerConfiguration};
use tako::messages::gateway::LostWorkerReason;
use tako::messages::worker::ToWorkerMessage;
use tako::scheduler::metrics::compute_b_level_metric;
use tako::scheduler::state::SchedulerState;
use tako::server::comm::Comm;
use tako::server::core::Core;
use tako::server::task::SerializedTaskContext;
use tako::{TaskId, WorkerId};

use crate::{add_tasks, create_worker};

fn bench_b_level(c: &mut BenchmarkGroup<WallTime>) {
    for task_count in [10, 1_000, 100_000] {
        c.bench_with_input(
            BenchmarkId::new("compute b-level", task_count),
            &task_count,
            |b, &task_count| {
                b.iter_batched_ref(
                    || {
                        let mut core = Core::default();
                        add_tasks(&mut core, task_count);
                        core
                    },
                    |core| {
                        compute_b_level_metric(core.task_map_mut());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_schedule(c: &mut BenchmarkGroup<WallTime>) {
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
    let mut group = c.benchmark_group("scheduler");

    bench_b_level(&mut group);
    bench_schedule(&mut group);
}

// Utils
struct NullComm;

impl Comm for NullComm {
    fn send_worker_message(&mut self, _worker_id: WorkerId, _message: &ToWorkerMessage) {}

    fn broadcast_worker_message(&mut self, _message: &ToWorkerMessage) {}

    fn ask_for_scheduling(&mut self) {}

    fn send_client_task_finished(&mut self, _task_id: TaskId) {}

    fn send_client_task_started(
        &mut self,
        _task_id: TaskId,
        _worker_id: WorkerId,
        _context: SerializedTaskContext,
    ) {
    }

    fn send_client_task_error(
        &mut self,
        _task_id: TaskId,
        _consumers_id: Vec<TaskId>,
        _error_info: TaskFailInfo,
    ) {
    }

    fn send_client_worker_new(
        &mut self,
        _worker_id: WorkerId,
        _configuration: &WorkerConfiguration,
    ) {
    }

    fn send_client_worker_lost(
        &mut self,
        _worker_id: WorkerId,
        _running_tasks: Vec<TaskId>,
        _reason: LostWorkerReason,
    ) {
    }
}
