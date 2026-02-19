use crate::internal::scheduler::mapping::create_task_mapping;
use crate::internal::scheduler::{create_task_batches, run_scheduling_solver};
use crate::internal::server::comm::{Comm, CommSender, CommSenderRef};
use crate::internal::server::core::{Core, CoreRef};
use crate::{Set, TaskId};
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
        if !comm_ref.get().get_scheduling_flag() {
            last_schedule = Instant::now();
            continue;
        }
        let mut now = Instant::now();
        let since_last_schedule = now - last_schedule;
        if minimum_delay > since_last_schedule {
            sleep(minimum_delay - since_last_schedule).await;
            now = Instant::now();
        }
        let mut comm = comm_ref.get_mut();
        if !comm.get_scheduling_flag() {
            last_schedule = now;
            continue;
        }
        let mut core = core_ref.get_mut();
        run_scheduling(&mut core, &mut comm, now);
        last_schedule = Instant::now();
    }
}

pub(crate) fn run_scheduling_inner(core: &mut Core, comm: &mut impl Comm, now: Instant) {
    let batches = create_task_batches(core, now, None);
    let solution = run_scheduling_solver(core, now, &batches, None);
    let mapping = create_task_mapping(core, solution);
    mapping.send_messages(core, comm);
}

pub(crate) fn run_scheduling(core: &mut Core, comm: &mut CommSender, now: Instant) {
    run_scheduling_inner(core, comm, now);
    comm.reset_scheduling_flag();
}

/*pub(crate) fn collect_assigned_not_running_tasks(core: &mut Core) -> Vec<TaskId> {
    let mut result = Vec::new();
    for worker in core.get_workers_mut() {
        worker.collect_assigned_non_running_tasks(&mut result);
    }
    result
}*/
