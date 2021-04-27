use std::path::PathBuf;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::{print_job_detail, print_job_list};
use crate::client::utils::handle_message;
use crate::common::error::error;
use crate::server::bootstrap::get_client_connection;
use crate::server::job::JobId;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, JobInfoRequest, ToClientMessage};

pub async fn get_job_list(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
) -> crate::Result<()> {
    match handle_message(
        connection
            .send_and_receive(FromClientMessage::JobInfo(JobInfoRequest {
                job_ids: None,
                include_program_def: false,
            }))
            .await,
    )? {
        ToClientMessage::JobInfoResponse(mut response) => {
            response.jobs.sort_unstable_by_key(|j| j.id);
            print_job_list(gsettings, response.jobs);
        }
        msg => return error(format!("Received an invalid message {:?}", msg)),
    }
    Ok(())
}

pub async fn get_job_detail(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    job_id: JobId,
) -> crate::Result<()> {
    match handle_message(
        connection
            .send_and_receive(FromClientMessage::JobInfo(JobInfoRequest {
                job_ids: Some(vec![job_id]),
                include_program_def: true,
            }))
            .await,
    )? {
        ToClientMessage::JobInfoResponse(mut response) => {
            assert!(response.jobs.len() <= 1);
            if let Some(job) = response.jobs.pop() {
                print_job_detail(gsettings, job);
            } else {
                log::error!("Job {} not found", job_id);
            }
        }
        msg => return error(format!("Received an invalid message {:?}", msg)),
    }
    Ok(())
}
