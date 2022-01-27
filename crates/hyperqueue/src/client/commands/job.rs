use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::outputs::OutputStream;
use crate::client::status::{job_status, Status};
use crate::common::arraydef::IntArray;
use crate::common::cli::SelectorArg;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    CancelJobResponse, CancelRequest, FromClientMessage, JobDetailRequest, JobInfoRequest,
    Selector, ToClientMessage,
};
use crate::JobId;
use clap::Parser;

#[derive(Parser)]
pub struct JobListOpts {
    /// List only jobs with the given states.
    /// You can use multiple states separated by a comma.
    #[clap(long, multiple_occurrences(false), use_delimiter(true))]
    pub filter: Vec<Status>,
}

#[derive(Parser)]
pub struct JobInfoOpts {
    /// Single ID, ID range or `last` to display the most recently submitted job
    pub selector_arg: SelectorArg,
}

#[derive(Parser)]
pub struct JobTasksOpts {
    /// Job ID
    pub job_id: u32,
}

#[derive(Parser)]
pub struct JobCancelOpts {
    /// Select job(s) to cancel
    pub selector_arg: SelectorArg,
}

#[derive(Parser)]
pub struct JobCatOpts {
    /// Select job
    pub job_id: u32,

    /// Select task(s) outputs to view
    #[clap(long)]
    pub tasks: Option<SelectorArg>,

    /// Type of output stream to display
    #[clap(possible_values = &["stdout", "stderr"])]
    pub stream: OutputStream,
}

pub async fn get_last_job_id(connection: &mut ClientConnection) -> crate::Result<Option<JobId>> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: Selector::LastN(1),
    });
    let response = rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;

    Ok(response.jobs.last().map(|job| job.id))
}

pub async fn get_job_ids(connection: &mut ClientConnection) -> crate::Result<Option<Vec<JobId>>> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: Selector::All,
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
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: Selector::All,
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;

    if !job_filters.is_empty() {
        response
            .jobs
            .retain(|j| job_filters.contains(&job_status(j)));
    }
    response.jobs.sort_unstable_by_key(|j| j.id);
    gsettings.printer().print_job_list(response.jobs);
    Ok(())
}

pub async fn output_job_detail(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    selector: Selector,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        selector,
        include_tasks: true,
    });
    let responses =
        rpc_call!(connection, message, ToClientMessage::JobDetailResponse(r) => r).await?;

    for response in responses {
        if let Some(job) = response.1 {
            gsettings
                .printer()
                .print_job_detail(job, get_worker_map(connection).await?);
        } else {
            log::error!("Job {} not found", response.0);
        }
    }
    Ok(())
}

pub async fn output_job_tasks(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    job_id: JobId,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        selector: Selector::Specific(IntArray::from_id(job_id.into())),
        include_tasks: true,
    });
    let mut response =
        rpc_call!(connection, message, ToClientMessage::JobDetailResponse(r) => r).await?;

    if let Some(job) = response.pop().and_then(|item| item.1) {
        gsettings
            .printer()
            .print_job_tasks(job, get_worker_map(connection).await?);
    } else {
        log::error!("Job {} not found", job_id);
    }
    Ok(())
}

pub async fn output_job_cat(
    gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    job_id: u32,
    task_selector: Option<Selector>,
    output_stream: OutputStream,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        selector: Selector::Specific(IntArray::from_id(job_id)),
        include_tasks: true,
    });
    let mut responses =
        rpc_call!(connection, message, ToClientMessage::JobDetailResponse(r) => r).await?;

    if let Some(job) = responses.pop().and_then(|v| v.1) {
        return gsettings
            .printer()
            .print_job_output(job, task_selector, output_stream);
    } else {
        log::error!("Job {job_id} not found");
    }
    Ok(())
}

pub async fn cancel_job(
    _gsettings: &GlobalSettings,
    connection: &mut ClientConnection,
    selector: Selector,
) -> anyhow::Result<()> {
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
