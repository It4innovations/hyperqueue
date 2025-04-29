use crate::server::autoalloc::AutoAllocService;
use crate::server::event::streamer::EventStreamer;
use tako::control::ServerRef;

pub mod autoalloc;
pub mod backend;
pub mod bootstrap;
pub mod client;
pub mod event;
pub mod job;
mod restore;
pub mod state;
mod tako_events;
pub mod worker;

#[derive(Clone)]
pub struct Senders {
    pub server_control: ServerRef,
    pub events: EventStreamer,
    pub autoalloc: AutoAllocService,
}
