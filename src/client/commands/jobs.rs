use crate::client::globalsettings::GlobalSettings;
use crate::client::job::{print_job_detail, print_job_list};
use crate::rpc_call;
use crate::JobId;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{CancelJobResponse, CancelRequest, FromClientMessage, JobInfoRequest, ToClientMessage, JobDetailRequest};

pub async fn get_job_list(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
) -> crate::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        job_ids: None,
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
    show_tasks: bool,
) -> crate::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id,
        include_tasks: true,
    });
    let response =
        rpc_call!(connection, message, ToClientMessage::JobDetailResponse(r) => r).await?;

    if let Some(job) = response {
        print_job_detail(gsettings, job, false, show_tasks);
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
    let response =
        rpc_call!(connection, FromClientMessage::Cancel(CancelRequest { job_id }), ToClientMessage::CancelJobResponse(r) => r).await?;

    match response {
        CancelJobResponse::Canceled(canceled, already_finished) if !canceled.is_empty() => {
            log::info!("Job {} canceled ({} tasks canceled, {} tasks already finished)", job_id, canceled.len(), already_finished)
        }
        CancelJobResponse::Canceled(_, _) => {
            log::error!("Canceling job {} failed; all tasks are already finished", job_id)
        }
        CancelJobResponse::InvalidJob => {
            log::error!("Canceling job {} failed; job not found", job_id)
        }
    }
    Ok(())
}
