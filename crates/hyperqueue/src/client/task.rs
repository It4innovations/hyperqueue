use crate::client::commands::job::JobTaskIdsOpts;
use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::{Verbosity, VerbosityFlag};
use crate::common::arraydef::IntArray;
use crate::common::cli::{parse_last_range, parse_last_single_id, TaskSelectorArg};
use crate::common::error::HqError;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDetailRequest, SingleIdSelector, TaskIdSelector,
    TaskSelector, TaskStatusSelector, ToClientMessage,
};
use crate::{rpc_call, JobId};

#[derive(clap::Parser)]
pub struct TaskOpts {
    #[clap(subcommand)]
    pub subcmd: TaskCommand,
}

#[derive(clap::Parser)]
pub enum TaskCommand {
    /// Displays task(s) associated with selected job(s)
    List(TaskListOpts),
    /// Displays detailed task info
    Info(TaskInfoOpts),
}

#[derive(clap::Parser)]
pub struct TaskListOpts {
    /// Select specific job(s).
    #[arg(value_parser = parse_last_range)]
    pub job_selector: IdSelector,

    #[clap(flatten)]
    pub task_selector: TaskSelectorArg,

    #[clap(flatten)]
    pub verbosity: VerbosityFlag,
}

#[derive(clap::Parser)]
pub struct TaskInfoOpts {
    /// Select specific job
    #[arg(value_parser = parse_last_single_id)]
    pub job_selector: SingleIdSelector,

    /// Select specific task(s)
    pub task_selector: IntArray,

    #[clap(flatten)]
    pub verbosity: VerbosityFlag,
}

pub async fn output_job_task_list(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    job_id_selector: IdSelector,
    task_selector: Option<TaskSelector>,
    verbosity: Verbosity,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id_selector,
        task_selector,
    });
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    let jobs = response
        .details
        .into_iter()
        .filter_map(|(job_id, opt_job)| match opt_job {
            Some(job) => Some((job_id, job)),
            None => {
                log::warn!("Job {job_id} not found");
                None
            }
        })
        .collect();

    gsettings.printer().print_task_list(
        jobs,
        get_worker_map(session).await?,
        &response.server_uid,
        verbosity,
    );
    Ok(())
}

pub async fn output_job_task_info(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    job_id_selector: SingleIdSelector,
    task_id_selector: TaskIdSelector,
    verbosity: Verbosity,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id_selector: match job_id_selector {
            SingleIdSelector::Specific(id) => IdSelector::Specific(IntArray::from_id(id)),
            SingleIdSelector::Last => IdSelector::LastN(1),
        },
        task_selector: Some(TaskSelector {
            id_selector: task_id_selector,
            status_selector: TaskStatusSelector::All,
        }),
    });
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    let (job_id, opt_job) = response.details.first().unwrap();
    match opt_job {
        None => log::error!("Cannot find job {job_id}"),
        Some(job) => {
            gsettings.printer().print_task_info(
                (*job_id, job.clone()),
                &job.tasks,
                get_worker_map(session).await?,
                &response.server_uid,
                verbosity,
            );

            for task_id in &job.tasks_not_found {
                log::warn!("Task {task_id} not found");
            }
        }
    }

    Ok(())
}

pub async fn output_job_task_ids(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    opts: JobTaskIdsOpts,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id_selector: opts.selector,
        task_selector: Some(TaskSelector {
            id_selector: TaskIdSelector::All,
            status_selector: if opts.filter.is_empty() {
                TaskStatusSelector::All
            } else {
                TaskStatusSelector::Specific(opts.filter)
            },
        }),
    });
    let response =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;
    let mut job_task_ids: Vec<_> = response
        .details
        .iter()
        .map(|(job_id, detail)| {
            Ok((
                *job_id,
                IntArray::from_sorted_ids(
                    detail
                        .as_ref()
                        .ok_or_else(|| HqError::GenericError("Job Id not found".to_string()))?
                        .tasks
                        .iter()
                        .map(|(task_id, _)| task_id.as_num()),
                ),
            ))
        })
        .collect::<crate::Result<Vec<(JobId, IntArray)>>>()?;
    job_task_ids.sort_unstable_by_key(|x| x.0);
    gsettings.printer().print_task_ids(job_task_ids);
    Ok(())
}
