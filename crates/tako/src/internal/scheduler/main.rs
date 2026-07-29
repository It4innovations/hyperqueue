use crate::internal::scheduler::mapping::create_task_mapping;
use crate::internal::scheduler::{create_task_batches, run_scheduling_solver};
use crate::internal::server::comm::{Comm, CommSender, CommSenderRef};
use crate::internal::server::core::{Core, CoreRef};
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::sleep;

pub(crate) async fn scheduler_loop(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    scheduler_wakeup: Rc<Notify>,
    minimum_delay: Duration,
) {
    let mut last_schedule = Instant::now().checked_sub(minimum_delay * 2).unwrap();
    loop {
        scheduler_wakeup.notified().await;
        let mut now = Instant::now();
        if !comm_ref.get().get_scheduling_flag() {
            last_schedule = now;
            continue;
        }
        let since_last_schedule = now - last_schedule;
        if minimum_delay > since_last_schedule {
            sleep(minimum_delay - since_last_schedule).await;
            now = Instant::now();
        }
        if !comm_ref.get_mut().get_scheduling_flag() {
            last_schedule = now;
            continue;
        }
        while matches!(
            run_scheduling(&mut core_ref.get_mut(), &mut comm_ref.get_mut(), now),
            SchedulerResult::NeedMoreCompute
        ) {
            sleep(minimum_delay).await;
        }
        comm_ref.get_mut().reset_scheduling_flag();
        last_schedule = Instant::now();
    }
}

pub(crate) enum SchedulerResult {
    Done,
    NeedMoreCompute,
    NoProgress,
}

pub(crate) fn run_scheduling_inner(
    core: &mut Core,
    comm: &mut impl Comm,
    now: Instant,
) -> SchedulerResult {
    let batches = create_task_batches(core, now, None);
    let solution = run_scheduling_solver(core, now, &batches, None);
    let need_more_compute = if !solution.is_optimal {
        if solution.is_empty() {
            log::error!("Scheduler made no progress within given time limit");
            SchedulerResult::NoProgress
        } else {
            log::debug!("Scheduler dispatched a non-optimal placement this round");
            SchedulerResult::NeedMoreCompute
        }
    } else {
        SchedulerResult::Done
    };
    let mapping = create_task_mapping(core, solution);
    //mapping.dump();
    mapping.send_messages(core, comm);
    need_more_compute
}

pub(crate) fn run_scheduling(
    core: &mut Core,
    comm: &mut CommSender,
    now: Instant,
) -> SchedulerResult {
    trace_time!(
        "scheduler",
        "run_scheduling_inner",
        run_scheduling_inner(core, comm, now)
    )
}
