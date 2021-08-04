use crate::client::globalsettings::GlobalSettings;
use crate::client::job::{get_worker_map, print_job_detail, print_job_list};
use crate::client::status::{job_status, Status};
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
            .retain(|j| job_filters.contains(&job_status(j)));
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
        print_job_detail(
            gsettings,
            job,
            false,
            show_tasks,
            get_worker_map(connection).await?,
        );
    } else {
        log::error!("Job {} not found", job_id);
    }
    Ok(())
}

pub async fn cancel_job(
    _gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    selector: JobSelector,
) -> crate::Result<()> {
    let mut responses = rpc_call!(connection, FromClientMessage::Cancel(CancelRequest {
         selector,
    }), ToClientMessage::CancelJobResponse(r) => r)
    .await?;
    responses.sort_unstable_by_key(|x| x.0);

    if responses.is_empty() {
        log::info!("There is nothing to cancel")
    }

    for (job_id, response) in responses {
        match response {
            CancelJobResponse::Canceled(canceled, already_finished) if !canceled.is_empty() => {
                log::info!(
                    "Job {} canceled ({} tasks canceled, {} tasks already finished)",
                    job_id,
                    canceled.len(),
                    already_finished
                )
            }
            CancelJobResponse::Canceled(_, _) => {
                log::error!(
                    "Canceling job {} failed; all tasks are already finished",
                    job_id
                )
            }
            CancelJobResponse::InvalidJob => {
                log::error!("Canceling job {} failed; job not found", job_id)
            }
            CancelJobResponse::Failed(msg) => {
                log::error!("Canceling job {} failed; {}", job_id, msg)
            }
        }
    }
    Ok(())
}
