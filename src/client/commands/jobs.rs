use crate::client::globalsettings::GlobalSettings;
use crate::client::job::{job_status, print_job_detail, print_job_list, Status};
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    CancelJobResponse, CancelRequest, FromClientMessage, JobDetailRequest, JobInfoRequest,
    JobSelector, ToClientMessage,
};
use crate::JobId;

pub async fn get_last_job_id(connection: &mut ClientConnection) -> crate::Result<Option<JobId>> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: JobSelector::LastN(1),
    });
    let response = rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;

    Ok(response.jobs.last().map(|job| job.id))
}

pub async fn get_job_ids(connection: &mut ClientConnection) -> crate::Result<Option<Vec<JobId>>> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: JobSelector::All,
    });
    let response = rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;

    let mut ids: Vec<JobId> = Vec::new();
    for job in response.jobs {
        ids.push(job.id);
    }
    Ok(Option::from(ids))
}

pub async fn output_job_list(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    job_filters: Vec<Status>,
) -> crate::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: JobSelector::All,
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;

    if !job_filters.is_empty() {
        response
            .jobs
            .retain(|j| job_filters.contains(&job_status(&j)));
    }
    response.jobs.sort_unstable_by_key(|j| j.id);
    print_job_list(gsettings, response.jobs);
    Ok(())
}

pub async fn output_job_detail(
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
    job_ids: Vec<JobId>,
) -> crate::Result<()> {
    let mut sorted_ids = job_ids.clone();
    sorted_ids.sort_unstable();

    let responses =
        rpc_call!(connection, FromClientMessage::Cancel(CancelRequest { job_ids: sorted_ids }), ToClientMessage::CancelJobResponse(r) => r).await?;

    for response in responses{
        match response {
            CancelJobResponse::Canceled(job_id, canceled, already_finished) if !canceled.is_empty() => {
                log::info!(
                    "Job {} canceled ({} tasks canceled, {} tasks already finished)",
                    job_id,
                    canceled.len(),
                    already_finished
                )
            }
            CancelJobResponse::Canceled(job_id, _, _) => {
                log::error!(
                    "Canceling job {} failed; all tasks are already finished",
                    job_id
                )
            }
            CancelJobResponse::InvalidJob(job_id) => {
                log::error!("Canceling job {} failed; job not found", job_id)
            }
        }
    }
    Ok(())
}
