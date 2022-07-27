use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::{Verbosity, VerbosityFlag};
use crate::common::arraydef::IntArray;
use crate::common::cli::{JobSelectorArg, TaskSelectorArg};
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDetailRequest, TaskIdSelector, TaskSelector,
    TaskStatusSelector, ToClientMessage,
};
use crate::{rpc_call, JobTaskId};

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
    pub job_selector: JobSelectorArg,

    #[clap(flatten)]
    pub task_selector: TaskSelectorArg,

    #[clap(flatten)]
    pub verbosity: VerbosityFlag,
}

#[derive(clap::Parser)]
pub struct TaskInfoOpts {
    /// Select specific job(s).
    pub job_selector: JobSelectorArg,

    /// Select specific task by id
    pub task_id: JobTaskId,
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
    let responses =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    let jobs = responses
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
        session.server_uid(),
        verbosity,
    );
    Ok(())
}

pub async fn output_job_task_info(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    job_id_selector: IdSelector,
    task_id: JobTaskId,
) -> anyhow::Result<()> {
    let message = FromClientMessage::JobDetail(JobDetailRequest {
        job_id_selector,
        task_selector: Some(TaskSelector {
            id_selector: TaskIdSelector::Specific(IntArray::from_id(task_id.as_num())),
            status_selector: TaskStatusSelector::All,
        }),
    });
    let responses =
        rpc_call!(session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
            .await?;

    let (job_id, opt_job) = responses.get(0).unwrap();
    match opt_job {
        None => log::error!("Cannot find job {job_id}"),
        Some(job) => {
            let opt_task = job.tasks.iter().find(|t| t.task_id == task_id);
            match opt_task {
                None => log::error!("Cannot find task {task_id} in job {job_id}"),
                Some(task) => gsettings.printer().print_task_info(
                    (job_id.clone(), job.clone()),
                    task,
                    get_worker_map(session).await?,
                    session.server_uid(),
                ),
            }
        }
    }

    Ok(())
}
