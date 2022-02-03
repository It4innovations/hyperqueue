use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::outputs::OutputStream;
use crate::client::output::resolve_task_paths;
use crate::client::status::{get_task_status, job_status, Status};
use crate::common::arraydef::IntArray;
use crate::common::cli::SelectorArg;
use crate::server::job::JobTaskInfo;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{
    CancelJobResponse, CancelRequest, FromClientMessage, JobDetailRequest, JobInfoRequest,
    Selector, ToClientMessage,
};
use crate::{rpc_call, JobTaskId};
use crate::{JobId, Set};
use clap::Parser;
use std::collections::HashMap;

#[derive(Parser)]
pub struct JobListOpts {
    /// Display all jobs.
    #[clap(long, conflicts_with("filter"))]
    pub all: bool,

    /// Display only jobs with the given states.
    /// You can use multiple states separated by a comma.
    #[clap(long, multiple_occurrences(false), use_delimiter(true), arg_enum)]
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

    /// Display only task(s) with the given states.
    /// You can use multiple states separated by a comma.
    #[clap(long, multiple_occurrences(false), use_delimiter(true), arg_enum)]
    pub task_status: Vec<Status>,

    /// Prepend the output of each task with a header line that identifies the task
    /// which produced that output.
    #[clap(long)]
    pub print_task_header: bool,

    /// Type of output stream to display
    #[clap(arg_enum)]
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
    task_header: bool,
    task_status: Vec<Status>,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        selector: Selector::Specific(IntArray::from_id(job_id)),
        include_tasks: true,
    });
    let mut responses =
        rpc_call!(connection, message, ToClientMessage::JobDetailResponse(r) => r).await?;

    if let Some(mut job) = responses.pop().and_then(|v| v.1) {
        let all_ids: Set<JobTaskId> = job.tasks.iter().map(|task| task.task_id).collect();
        if !task_status.is_empty() {
            job.tasks
                .retain(|task_info| task_status.contains(&get_task_status(&task_info.state)));
        }

        let task_paths = resolve_task_paths(&job);

        let tasks: Vec<JobTaskInfo> = match task_selector {
            None | Some(Selector::All) => {
                job.tasks.sort_unstable_by_key(|task| task.task_id);
                job.tasks
            }

            Some(Selector::Specific(arr)) => {
                let tasks_map: HashMap<JobTaskId, JobTaskInfo> = job
                    .tasks
                    .into_iter()
                    .map(|info| (info.task_id, info))
                    .collect();

                arr.iter()
                    .filter_map(|task_id| {
                        let task_id: JobTaskId = task_id.into();
                        let task_opt = tasks_map.get(&task_id);

                        if task_opt.is_none() && !all_ids.contains(&task_id) {
                            log::warn!("Task {task_id} not found");
                        }
                        task_opt.cloned()
                    })
                    .collect()
            }

            Some(Selector::LastN(_)) => {
                let last_task = job.tasks.iter().max_by_key(|&x| x.task_id);
                match last_task {
                    None => vec![],
                    Some(task) => vec![task.clone()],
                }
            }
        };

        if tasks.is_empty() {
            log::warn!("No tasks were selected, there is nothing to print");
            return Ok(());
        }

        return gsettings
            .printer()
            .print_job_output(tasks, output_stream, task_header, task_paths);
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
