use crate::scheduler::FromSchedulerMessage;
use crate::server::core::CoreRef;
use crate::server::reactor::on_assignments;
use crate::server::comm::CommSenderRef;
use crate::Error;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn observe_scheduler(
    core_ref: CoreRef,
    comm_ref: CommSenderRef,
    mut receiver: UnboundedReceiver<FromSchedulerMessage>,
) -> crate::Result<()> {
    log::debug!("Starting scheduler");

    match receiver.next().await {
        Some(crate::scheduler::FromSchedulerMessage::Register(r)) => {
            log::debug!("Scheduler registered: {:?}", r)
        }
        None => {
            return Err(Error::SchedulerError(
                "Scheduler closed connection without registration".to_owned(),
            ));
        }
        _ => {
            return Err(Error::SchedulerError(
                "First message of scheduler has to be registration".to_owned(),
            ));
        }
    }

    while let Some(msg) = receiver.next().await {
        match msg {
            FromSchedulerMessage::TaskAssignments(assignments) => {
                let mut core = core_ref.get_mut();
                let mut comm = comm_ref.get_mut();
                trace_time!("core", "process_assignments", {
                    on_assignments(&mut core, &mut *comm, assignments);
                });
            }
            FromSchedulerMessage::Register(_) => {
                return Err(Error::SchedulerError(
                    "Double registration of scheduler".to_owned(),
                ));
            }
        }
    }

    Ok(())
}
