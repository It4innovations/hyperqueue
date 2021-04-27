use std::path::PathBuf;

use crate::common::error::error;
use crate::server::bootstrap::get_client_connection;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};
use crate::client::utils::handle_message;
use crate::client::job::print_job_list;
use crate::transfer::connection::ClientConnection;
use crate::client::globalsettings::GlobalSettings;

pub async fn get_job_list(gsettings: &GlobalSettings, connection: &mut ClientConnection) -> crate::Result<()> {
    match handle_message(connection.send_and_receive(FromClientMessage::JobList).await)? {
        ToClientMessage::JobListResponse(mut stats) => {
            stats.jobs.sort_unstable_by_key(|j| j.id);
            print_job_list(gsettings, stats.jobs);
        }
        msg => return error(format!("Received an invalid message {:?}", msg))
    }
    Ok(())
}
