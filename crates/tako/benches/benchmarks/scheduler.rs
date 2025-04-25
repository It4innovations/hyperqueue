use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion};
use std::time::Instant;

use crate::{add_tasks, create_worker};
use tako::gateway::LostWorkerReason;
use tako::internal::messages::common::TaskFailInfo;
use tako::internal::messages::worker::ToWorkerMessage;
use tako::internal::scheduler::metrics::compute_b_level_metric;
use tako::internal::scheduler::state::SchedulerState;
use tako::internal::server::comm::Comm;
use tako::internal::server::core::Core;
use tako::task::SerializedTaskContext;
use tako::worker::{WorkerConfiguration, WorkerOverview};
use tako::{InstanceId, TaskId, WorkerId};

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
        _instance_id: InstanceId,
        _worker_id: &[WorkerId],
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

    fn send_client_worker_overview(&mut self, _overview: Box<WorkerOverview>) {}
}
