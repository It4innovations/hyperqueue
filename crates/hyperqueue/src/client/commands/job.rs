use clap::Parser;

use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::outputs::OutputStream;
use crate::client::output::resolve_task_paths;
use crate::client::status::{Status, job_status};
use crate::common::cli::{TaskSelectorArg, parse_last_all_range, parse_last_range};
use crate::common::utils::str::pluralize;
use crate::rpc_call;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    CancelJobResponse, CancelRequest, CloseJobRequest, CloseJobResponse, ForgetJobRequest,
    FromClientMessage, IdSelector, JobDetail, JobDetailRequest, JobInfoRequest, TaskIdSelector,
    TaskSelector, TaskStatusSelector, ToClientMessage,
};

#[derive(Parser)]
pub struct JobListOpts {
    /// Display all jobs
    #[arg(long, conflicts_with("filter"))]
    pub all: bool,

    /// Display only jobs with the given states
    ///
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
    /// Message attached to cancelled task
    #[arg(long)]
    pub message: Option<String>,
}

#[derive(Parser)]
pub struct JobCloseOpts {
    /// Select job(s) to close
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,
}

#[derive(Parser)]
pub struct JobForgetOpts {
    /// Select job(s) to forget
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,
    /// Forget only jobs with the given states
    ///
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

#[derive(Parser)]
pub struct JobTaskIdsOpts {
    /// Selects job(s)
    #[arg(value_parser = parse_last_all_range)]
    pub selector: IdSelector,

    /// Selects only tasks with the given state(s)
    ///
    /// You can use multiple states separated by a comma.
    #[arg(long, value_delimiter(','), value_enum)]
    pub filter: Vec<Status>,
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
    /// Select a job
    #[arg(value_parser = parse_last_range)]
    pub job_selector: IdSelector,

    #[clap(flatten)]
    pub task_selector: TaskSelectorArg,

    /// Add task headers to the output
    ///
    /// Prepends the output of each task with a header line that identifies the task
    /// which produced that output.
    #[arg(long)]
    pub print_task_header: bool,

    /// Type of output stream to display
    #[arg(value_enum)]
    pub stream: OutputStream,
}

pub async fn output_job_list(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    job_filters: Vec<Status>,
    show_open: bool,
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
            .retain(|j| (show_open && j.is_open) || job_filters.contains(&job_status(j)));
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
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    if response.details.is_empty() {
        log::error!("No jobs were found");
        return Ok(());
    }

    for (job_id, opt_job) in response.details {
        match opt_job {
            Some(job) => {
                let task_paths = resolve_task_paths(&job, &response.server_uid);
                for task_id in &job.tasks_not_found {
                    log::warn!("Task {task_id} of job {job_id} not found");
                }

                if job.tasks.is_empty() {
                    log::warn!(
                        "No tasks were selected for job {job_id}, there is nothing to print"
                    );
                    continue;
                }

                gsettings.printer().print_job_output(
                    job,
                    output_stream,
                    task_header,
                    task_paths,
                )?;
            }
            None => log::error!("Job {job_id} was not found"),
        }
    }
    Ok(())
}

pub async fn cancel_job(
    _gsettings: &GlobalSettings,
    session: &mut ClientSession,
    selector: IdSelector,
    message: String,
) -> anyhow::Result<()> {
    let mut responses = rpc_call!(session.connection(), FromClientMessage::Cancel(CancelRequest {
         selector,
    message,}), ToClientMessage::CancelJobResponse(r) => r)
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
                log::error!("Canceling job {job_id} failed; all tasks are already finished")
            }
            CancelJobResponse::InvalidJob => {
                log::error!("Canceling job {job_id} failed; job not found")
            }
            CancelJobResponse::Failed(msg) => {
                log::error!("Canceling job {job_id} failed; {msg}")
            }
        }
    }
    Ok(())
}

pub async fn close_job(
    _gsettings: &GlobalSettings,
    session: &mut ClientSession,
    selector: IdSelector,
) -> anyhow::Result<()> {
    let mut responses =
        rpc_call!(session.connection(), FromClientMessage::CloseJob(CloseJobRequest {
         selector,
    }), ToClientMessage::CloseJobResponse(r) => r)
        .await?;
    responses.sort_unstable_by_key(|x| x.0);

    if responses.is_empty() {
        log::info!("There is nothing to close")
    }

    for (job_id, response) in responses {
        match response {
            CloseJobResponse::Closed => {
                log::info!("Job {job_id} closed")
            }
            CloseJobResponse::InvalidJob => {
                log::error!("Closing job {job_id} failed; job not found")
            }
            CloseJobResponse::AlreadyClosed => {
                log::error!("Closing job {job_id} failed; job is already closed")
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

    let message = FromClientMessage::ForgetJob(ForgetJobRequest {
        selector,
        filter: filter.into_iter().map(|s| s.into_status()).collect(),
    });
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::ForgetJobResponse(r) => r)
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
