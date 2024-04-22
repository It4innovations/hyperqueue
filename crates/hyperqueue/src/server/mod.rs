use crate::server::autoalloc::AutoAllocService;
use crate::server::backend::Backend;
use crate::server::event::streamer::EventStreamer;

pub mod autoalloc;
pub mod backend;
pub mod bootstrap;
pub mod client;
pub mod event;
pub mod job;
mod restore;
pub mod state;
pub mod worker;

#[derive(Clone)]
pub struct Senders {
    pub backend: Backend,
    pub events: EventStreamer,
    pub autoalloc: AutoAllocService,
}
