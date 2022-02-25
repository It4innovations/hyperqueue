use hyperqueue::client::status::Status;
use hyperqueue::common::arraydef::IntArray;
use hyperqueue::common::utils::fs::get_current_dir;
use hyperqueue::server::job::JobTaskState;
use hyperqueue::tako::messages::common::{ProgramDefinition, StdioDef};
use hyperqueue::transfer::messages::{
    FromClientMessage, IdSelector, JobDescription as HqJobDescription, JobDetailRequest,
    SubmitRequest, TaskDescription as HqTaskDescription, TaskIdSelector, TaskSelector,
    TaskStatusSelector, TaskWithDependencies, ToClientMessage, WaitForJobsRequest,
};
use hyperqueue::{rpc_call, JobTaskCount};
use pyo3::{PyResult, Python};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::utils::error::ToPyResult;
use crate::{borrow_mut, run_future, ContextPtr, FromPyObject, PyJobId, PyTaskId};

#[derive(Debug, FromPyObject)]
pub struct TaskDescription {
    id: u32,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    env: HashMap<String, String>,
    stdout: Option<PathBuf>,
    stderr: Option<PathBuf>,
    stdin: Option<Vec<u8>>,
    dependencies: Vec<u32>,
}

#[derive(Debug, FromPyObject)]
pub struct JobDescription {
    tasks: Vec<TaskDescription>,
    max_fails: Option<JobTaskCount>,
}

pub(crate) fn submit_job_impl(py: Python, ctx: ContextPtr, job: JobDescription) -> PyResult<u32> {
    run_future(async move {
        let submit_dir = get_current_dir();
        let tasks = build_tasks(job.tasks, &submit_dir);
        let job_desc = HqJobDescription::Graph { tasks };

        let message = FromClientMessage::Submit(SubmitRequest {
            job_desc,
            name: "".to_string(),
            max_fails: job.max_fails,
            submit_dir,
            log: None,
        });

        let mut ctx = borrow_mut!(py, ctx);
        let response = rpc_call!(ctx.connection, message, ToClientMessage::SubmitResponse(r) => r)
            .await
            .map_py_err()?;
        Ok(response.job.info.id.as_num())
    })
}

fn build_tasks(tasks: Vec<TaskDescription>, submit_dir: &Path) -> Vec<TaskWithDependencies> {
    tasks
        .into_iter()
        .map(|mut task| TaskWithDependencies {
            id: task.id.into(),
            dependencies: std::mem::take(&mut task.dependencies)
                .into_iter()
                .map(|id| id.into())
                .collect(),
            task_desc: build_task_desc(task, submit_dir),
        })
        .collect()
}

fn build_task_desc(desc: TaskDescription, submit_dir: &Path) -> HqTaskDescription {
    let args = desc.args.into_iter().map(|arg| arg.into()).collect();
    let env = desc
        .env
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect();
    let stdout = desc.stdout.map(StdioDef::File).unwrap_or_default();
    let stderr = desc.stderr.map(StdioDef::File).unwrap_or_default();
    let stdin = desc.stdin.unwrap_or_default();
    let cwd = desc.cwd.unwrap_or_else(|| submit_dir.to_path_buf());

    HqTaskDescription {
        program: ProgramDefinition {
            args,
            env,
            stdout,
            stderr,
            stdin,
            cwd,
        },
        resources: Default::default(),
        pin: false,
        task_dir: false,
        time_limit: None,
        priority: 0,
    }
}

pub(crate) fn wait_for_jobs_impl(
    py: Python,
    ctx: ContextPtr,
    job_ids: Vec<PyJobId>,
) -> PyResult<u32> {
    run_future(async move {
        let message = FromClientMessage::WaitForJobs(WaitForJobsRequest {
            selector: IdSelector::Specific(IntArray::from_ids(job_ids)),
        });

        let mut ctx = borrow_mut!(py, ctx);
        let response =
            rpc_call!(ctx.connection, message, ToClientMessage::WaitForJobsResponse(r) => r)
                .await
                .map_py_err()?;
        Ok(response.finished)
    })
}

pub(crate) fn get_error_messages_impl(
    py: Python,
    ctx: ContextPtr,
    job_ids: Vec<PyJobId>,
) -> PyResult<HashMap<PyJobId, HashMap<PyTaskId, String>>> {
    run_future(async move {
        let message = FromClientMessage::JobDetail(JobDetailRequest {
            job_id_selector: IdSelector::Specific(IntArray::from_ids(job_ids)),
            task_selector: Some(TaskSelector {
                id_selector: TaskIdSelector::All,
                status_selector: TaskStatusSelector::Specific(vec![Status::Failed]),
            }),
        });

        let mut ctx = borrow_mut!(py, ctx);
        let response =
            rpc_call!(ctx.connection, message, ToClientMessage::JobDetailResponse(r) => r)
                .await
                .map_py_err()?;

        Ok(response
            .into_iter()
            .filter_map(|(job_id, opt_job)| {
                opt_job.map(|job| {
                    (
                        job_id.as_num(),
                        job.tasks
                            .into_iter()
                            .map(|t| match t.state {
                                JobTaskState::Failed { error, .. } => (t.task_id.as_num(), error),
                                _ => panic!("Invalid state"),
                            })
                            .collect::<HashMap<PyTaskId, String>>(),
                    )
                })
            })
            .collect())
    })
}
