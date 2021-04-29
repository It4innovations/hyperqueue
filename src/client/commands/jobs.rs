use crate::client::globalsettings::GlobalSettings;
use crate::client::job::{print_job_detail, print_job_list};
use crate::rpc_call;
use crate::server::job::JobId;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    CancelJobResponse, CancelRequest, FromClientMessage, JobInfoRequest, ToClientMessage,
};

pub async fn get_job_list(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
) -> crate::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        job_ids: None,
        include_program_def: false,
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;
    response.jobs.sort_unstable_by_key(|j| j.id);
    print_job_list(gsettings, response.jobs);
    Ok(())
}

pub async fn get_job_detail(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    job_id: JobId,
) -> crate::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        job_ids: Some(vec![job_id]),
        include_program_def: true,
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;
    assert!(response.jobs.len() <= 1);
    if let Some(job) = response.jobs.pop() {
        print_job_detail(gsettings, job);
    } else {
        log::error!("Job {} not found", job_id);
    }
    Ok(())
}

pub async fn cancel_job(
    _gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    job_id: JobId,
) -> crate::Result<()> {
    let mut response =
        rpc_call!(connection, FromClientMessage::Cancel(CancelRequest { job_id }), ToClientMessage::CancelJobResponse(r) => r).await?;

    match response {
        CancelJobResponse::Canceled => {
            log::info!("Job {} canceled", job_id)
        }
        CancelJobResponse::AlreadyFinished => {
            log::error!("Canceling job {} failed; job is already finished", job_id)
        }
        CancelJobResponse::InvalidJob => {
            log::error!("Canceling job {} failed; job not found", job_id)
        }
    }
    Ok(())
}
