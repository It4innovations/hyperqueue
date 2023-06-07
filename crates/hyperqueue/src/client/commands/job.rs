use clap::Parser;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::outputs::OutputStream;
use crate::client::output::resolve_task_paths;
use crate::client::status::{job_status, Status};
use crate::common::cli::{parse_last_all_range, parse_last_range, TaskSelectorArg};
use crate::common::utils::str::pluralize;
use crate::rpc_call;
use crate::transfer::connection::{ClientConnection, ClientSession};
use crate::transfer::messages::{
    CancelJobResponse, CancelRequest, ForgetJobRequest, FromClientMessage, IdSelector, JobDetail,
    JobDetailRequest, JobInfoRequest, TaskIdSelector, TaskSelector, TaskStatusSelector,
    ToClientMessage,
};
use crate::JobId;

#[derive(Parser)]
pub struct JobListOpts {
    /// Display all jobs.
    #[arg(long, conflicts_with("filter"))]
    pub all: bool,

    /// Display only jobs with the given states.
    /// You can use multiple states separated by a comma.
    #[arg(long, value_delimiter(','), value_enum)]
    pub filter: Vec<Status>,
}

#[derive(Parser)]
pub struct JobInfoOpts {
    /// Single ID, ID range or `last` to display the most recently submitted job
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,
}

#[derive(Parser)]
pub struct JobCancelOpts {
    /// Select job(s) to cancel
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,
}

#[derive(Parser)]
pub struct JobForgetOpts {
    /// Select job(s) to forget
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,
    /// Forget only jobs with the given states.
    /// You can use multiple states separated by a comma.
    /// You can only filter by states that mark a completed job.
    #[arg(
        long,
        value_delimiter(','),
        value_enum,
        default_value("finished,failed,canceled")
    )]
    pub filter: Vec<CompletedJobStatus>,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum CompletedJobStatus {
    Finished,
    Failed,
    Canceled,
}

impl CompletedJobStatus {
    pub fn into_status(self) -> Status {
        match self {
            CompletedJobStatus::Finished => Status::Finished,
            CompletedJobStatus::Failed => Status::Failed,
            CompletedJobStatus::Canceled => Status::Canceled,
        }
    }
}

#[derive(Parser)]
pub struct JobCatOpts {
    /// Select specific job
    #[arg(value_parser = parse_last_range)]
    pub job_selector: IdSelector,

    #[clap(flatten)]
    pub task_selector: TaskSelectorArg,

    /// Prepend the output of each task with a header line that identifies the task
    /// which produced that output.
    #[arg(long)]
    pub print_task_header: bool,

    /// Type of output stream to display
    #[arg(value_enum)]
    pub stream: OutputStream,
}

pub async fn get_last_job_id(connection: &mut ClientConnection) -> crate::Result<Option<JobId>> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: IdSelector::LastN(1),
    });
    let response = rpc_call!(connection, message, ToClientMessage::JobInfoResponse(r) => r).await?;

    Ok(response.jobs.last().map(|job| job.id))
}

pub async fn get_job_ids(connection: &mut ClientConnection) -> crate::Result<Option<Vec<JobId>>> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: IdSelector::All,
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
    session: &mut ClientSession,
    job_filters: Vec<Status>,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: IdSelector::All,
    });
    let mut response =
        rpc_call!(session.connection(), message, ToClientMessage::JobInfoResponse(r) => r).await?;

    let total_count = response.jobs.len();
    if !job_filters.is_empty() {
        response
            .jobs
            .retain(|j| job_filters.contains(&job_status(j)));
    }
    response.jobs.sort_unstable_by_key(|j| j.id);
    gsettings
        .printer()
        .print_job_list(response.jobs, total_count);
    Ok(())
}

pub async fn output_job_summary(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobInfo(JobInfoRequest {
        selector: IdSelector::All,
    });
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::JobInfoResponse(r) => r).await?;

    gsettings.printer().print_job_summary(response.jobs);
    Ok(())
}

pub async fn output_job_detail(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    selector: IdSelector,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id_selector: selector,
        task_selector: Some(TaskSelector {
            id_selector: TaskIdSelector::All,
            status_selector: TaskStatusSelector::All,
        }),
    });
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    let worker_map = get_worker_map(session).await?;

    let jobs: Vec<JobDetail> = response
        .details
        .into_iter()
        .filter_map(|(id, job)| match job {
            Some(job) => Some(job),
            None => {
                log::error!("Job {id} not found");
                None
            }
        })
        .collect();
    gsettings
        .printer()
        .print_job_detail(jobs, worker_map, &response.server_uid);
    Ok(())
}

pub async fn output_job_cat(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    job_selector: IdSelector,
    task_selector: Option<TaskSelector>,
    output_stream: OutputStream,
    task_header: bool,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id_selector: job_selector,
        task_selector,
    });
    let mut response =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    if let Some((job_id, opt_job)) = response.details.pop() {
        match opt_job {
            None => log::error!("Job {job_id} was not found"),
            Some(job) => {
                let task_paths = resolve_task_paths(&job, &response.server_uid);
                for task_id in job.tasks_not_found {
                    log::warn!("Task {task_id} not found");
                }

                if job.tasks.is_empty() {
                    log::warn!("No tasks were selected, there is nothing to print");
                    return Ok(());
                }

                return gsettings.printer().print_job_output(
                    job.tasks,
                    output_stream,
                    task_header,
                    task_paths,
                );
            }
        }
    } else {
        log::error!("No jobs were found");
    }
    Ok(())
}

pub async fn cancel_job(
    _gsettings: &GlobalSettings,
    session: &mut ClientSession,
    selector: IdSelector,
) -> anyhow::Result<()> {
    let mut responses = rpc_call!(session.connection(), FromClientMessage::Cancel(CancelRequest {
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

pub async fn forget_job(
    _gsettings: &GlobalSettings,
    session: &mut ClientSession,
    opts: JobForgetOpts,
) -> anyhow::Result<()> {
    let JobForgetOpts { selector, filter } = opts;

    let response = rpc_call!(session.connection(), FromClientMessage::ForgetJob(ForgetJobRequest {
            selector,
            filter: filter.into_iter().map(|s| s.into_status()).collect()
    }), ToClientMessage::ForgetJobResponse(r) => r)
    .await?;

    let mut message = format!(
        "{} {} were forgotten",
        response.forgotten,
        pluralize("job", response.forgotten)
    );
    if response.ignored > 0 {
        message.push_str(&format!(
            ", {} were ignored due to wrong state or invalid ID",
            response.ignored
        ));
    }
    log::info!("{message}");

    Ok(())
}
