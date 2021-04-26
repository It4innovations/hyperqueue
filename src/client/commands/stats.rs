use std::path::PathBuf;

use crate::common::error::error;
use crate::server::bootstrap::get_client_connection;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use crate::client::utils::handle_message;
use crate::client::job::print_job_stats;
use crate::transfer::connection::ClientConnection;

pub async fn get_server_stats(connection: &mut ClientConnection) -> crate::Result<()> {
    match handle_message(connection.send_and_receive(FromClientMessage::Stats).await)? {
        ToClientMessage::StatsResponse(stats) => {
            print_job_stats(stats.jobs);
        }
        msg => return error(format!("Received an invalid message {:?}", msg))
    }
    Ok(())
}
