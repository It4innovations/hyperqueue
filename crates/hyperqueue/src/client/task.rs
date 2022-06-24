use crate::client::globalsettings::GlobalSettings;
use crate::client::job::get_worker_map;
use crate::client::output::{Verbosity, VerbosityFlag};
use crate::common::cli::{JobSelectorArg, TaskSelectorArg};
use crate::rpc_call;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    FromClientMessage, IdSelector, JobDetailRequest, TaskSelector, ToClientMessage,
};

#[derive(clap::Parser)]
pub struct TaskOpts {
    #[clap(subcommand)]
    pub subcmd: TaskCommand,
}

#[derive(clap::Parser)]
pub enum TaskCommand {
    /// Displays task(s) associated with selected job(s)
    List(TaskListOpts),
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

pub async fn output_job_tasks(
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

    gsettings.printer().print_tasks(
        jobs,
        get_worker_map(session).await?,
        session.server_uid(),
        verbosity,
    );
    Ok(())
}
