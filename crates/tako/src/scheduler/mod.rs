use tokio::sync::mpsc::UnboundedSender;

pub use comm::{drive_scheduler, prepare_scheduler_comm, SchedulerComm};
pub use level::LevelScheduler;
pub use metrics::{BLevelMetric, TLevelMetric};
pub use protocol::{FromSchedulerMessage, ToSchedulerMessage};
pub use protocol::{TaskAssignment};
pub use random::RandomScheduler;
pub use workstealing::WorkstealingScheduler;

use crate::scheduler::protocol::SchedulerRegistration;

mod comm;
mod graph;
mod metrics;
pub mod protocol;
mod task;
mod utils;
mod worker;

mod level;
mod random;
mod workstealing;

#[cfg(test)]
mod test_util;

pub type SchedulerSender = UnboundedSender<FromSchedulerMessage>;

pub trait Scheduler {
    fn identify(&self) -> SchedulerRegistration;

    /// Returns true if the scheduler requires someone to invoke `schedule` sometime in the future.
    fn handle_messages(&mut self, messages: ToSchedulerMessage) -> bool;
    fn schedule(&mut self) -> Vec<TaskAssignment>;
}
