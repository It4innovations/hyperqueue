pub use crate::internal::transfer::auth::deserialize;
pub use crate::internal::transfer::auth::serialize;
pub use crate::internal::transfer::auth::{
    do_authentication, forward_queue_to_sealed_sink, open_message, seal_message,
};
pub use crate::internal::worker::rpc::connect_to_server_and_authenticate;
use crate::worker::WorkerConfiguration;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterWorker {
    pub configuration: WorkerConfiguration,
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug)]
pub enum ConnectionRegistration {
    Worker(RegisterWorker),
    Custom,
}
