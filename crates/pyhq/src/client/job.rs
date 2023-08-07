use hyperqueue::client::commands::submit::command::{
    DEFAULT_CRASH_LIMIT, DEFAULT_STDERR_PATH, DEFAULT_STDOUT_PATH,
};
use hyperqueue::client::output::resolve_task_paths;
use hyperqueue::client::resources::parse_allocation_request;
use hyperqueue::client::status::{is_terminated, Status};
use hyperqueue::common::arraydef::IntArray;
use hyperqueue::common::utils::fs::get_current_dir;
use hyperqueue::server::job::JobTaskState;
use hyperqueue::transfer::messages::{
    ForgetJobRequest, FromClientMessage, IdSelector, JobDescription as HqJobDescription,
    JobDetailRequest, JobInfoRequest, JobInfoResponse, PinMode, SubmitRequest,
    TaskDescription as HqTaskDescription, TaskIdSelector, TaskSelector, TaskStatusSelector,
    TaskWithDependencies, ToClientMessage,
};
use hyperqueue::{rpc_call, tako, JobTaskCount, JobTaskId, Set};
use pyo3::types::PyTuple;
use pyo3::{IntoPy, PyAny, PyResult, Python};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tako::gateway::{ResourceRequestEntries, ResourceRequestEntry, ResourceRequestVariants};
use tako::program::{FileOnCloseBehavior, ProgramDefinition, StdioDef};
use tako::resources::{AllocationRequest, NumOfNodes, ResourceAmount};

use crate::marshal::FromPy;
use crate::utils::error::ToPyResult;
use crate::{borrow_mut, run_future, ClientContextPtr, FromPyObject, PyJobId, PyTaskId};

#[derive(Debug, FromPyObject)]
enum AllocationValue {
    Int(u64),
    String(String),
}

#[derive(Debug, FromPyObject)]
pub struct ResourceRequestDescription {
    n_nodes: NumOfNodes,
    resources: HashMap<String, AllocationValue>,
}

#[derive(Debug, FromPyObject)]
pub struct StdioDefInput {
    path: Option<PathBuf>,
    on_close: FromPy<FileOnCloseBehavior>,
}

#[derive(Debug, FromPyObject)]
pub struct TaskDescription {
    id: u32,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    env: HashMap<String, String>,
    stdout: Option<StdioDefInput>,
    stderr: Option<StdioDefInput>,
    stdin: Option<Vec<u8>>,
    dependencies: Vec<u32>,
    task_dir: bool,
    priority: tako::Priority,
    resource_request: Vec<ResourceRequestDescription>,
}

#[derive(Debug, FromPyObject)]
pub struct JobDescription {
    tasks: Vec<TaskDescription>,
    max_fails: Option<JobTaskCount>,
}

pub fn submit_job_impl(py: Python, ctx: ClientContextPtr, job: JobDescription) -> PyResult<u32> {
    run_future(async move {
        let submit_dir = get_current_dir();
        let tasks = build_tasks(job.tasks, &submit_dir)?;
        let job_desc = HqJobDescription::Graph { tasks };

        let message = FromClientMessage::Submit(SubmitRequest {
            job_desc,
            name: "".to_string(),
            max_fails: job.max_fails,
            submit_dir,
            log: None,
        });

        let mut ctx = borrow_mut!(py, ctx);
        let response =
            rpc_call!(ctx.session.connection(), message, ToClientMessage::SubmitResponse(r) => r)
                .await
                .map_py_err()?;
        Ok(response.job.info.id.as_num())
    })
}

pub fn forget_job_impl(py: Python, ctx: ClientContextPtr, job_id: PyJobId) -> PyResult<()> {
    run_future(async move {
        let message = FromClientMessage::ForgetJob(ForgetJobRequest {
            selector: IdSelector::Specific(IntArray::from_id(job_id)),
            filter: vec![Status::Canceled, Status::Failed, Status::Finished],
        });

        let mut ctx = borrow_mut!(py, ctx);
        let response =
            rpc_call!(ctx.session.connection(), message, ToClientMessage::ForgetJobResponse(r) => r)
                .await
                .map_py_err()?;
        if response.forgotten == 1 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Cannot forget job {job_id}. It either hasn't been completed yet, or it does not exist").into())
        }
    })
}

fn build_tasks(
    tasks: Vec<TaskDescription>,
    submit_dir: &Path,
) -> anyhow::Result<Vec<TaskWithDependencies>> {
    tasks
        .into_iter()
        .map(|mut task| {
            Ok(TaskWithDependencies {
                id: task.id.into(),
                dependencies: std::mem::take(&mut task.dependencies)
                    .into_iter()
                    .map(|id| id.into())
                    .collect(),
                task_desc: build_task_desc(task, submit_dir)?,
            })
        })
        .collect()
}

fn build_task_desc(desc: TaskDescription, submit_dir: &Path) -> anyhow::Result<HqTaskDescription> {
    let args = desc.args.into_iter().map(|arg| arg.into()).collect();
    let env = desc
        .env
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect();
    let stdout = desc
        .stdout
        .map(|stdio| StdioDef::File {
            path: stdio.path.unwrap_or(PathBuf::from(DEFAULT_STDOUT_PATH)),
            on_close: stdio.on_close.extract(),
        })
        .unwrap_or_default();
    let stderr = desc
        .stderr
        .map(|stdio| StdioDef::File {
            path: stdio.path.unwrap_or(PathBuf::from(DEFAULT_STDERR_PATH)),
            on_close: stdio.on_close.extract(),
        })
        .unwrap_or_default();
    let stdin = desc.stdin.unwrap_or_default();
    let cwd = desc.cwd.unwrap_or_else(|| submit_dir.to_path_buf());

    let resources = if !desc.resource_request.is_empty() {
        ResourceRequestVariants::new(
            desc.resource_request
                .into_iter()
                .map(|rq| {
                    anyhow::Ok(tako::gateway::ResourceRequest {
                        n_nodes: rq.n_nodes,
                        resources: rq
                            .resources
                            .into_iter()
                            .map(|(resource, alloc)| {
                                Ok(ResourceRequestEntry {
                                    resource,
                                    policy: match alloc {
                                        AllocationValue::Int(value) => {
                                            AllocationRequest::Compact(value as ResourceAmount)
                                        }
                                        AllocationValue::String(str) => {
                                            parse_allocation_request(&str)?
                                        }
                                    },
                                })
                            })
                            .collect::<anyhow::Result<ResourceRequestEntries>>()?,
                        min_time: Default::default(),
                    })
                })
                .collect::<anyhow::Result<_>>()?,
        )
    } else {
        Default::default()
    };

    Ok(HqTaskDescription {
        program: ProgramDefinition {
            args,
            env,
            stdout,
            stderr,
            stdin,
            cwd,
        },
        resources,
        pin_mode: PinMode::None,
        task_dir: desc.task_dir,
        priority: desc.priority,
        time_limit: None,
        crash_limit: DEFAULT_CRASH_LIMIT,
    })
}

#[derive(dict_derive::IntoPyObject)]
pub struct JobWaitStatus {
    finished: u64,
    failed: u64,
    total: u64,
    completed: bool,
}

/// Waits until the specified job(s) finish executing.
/// Returns IDs of jobs that had any failures.
pub fn wait_for_jobs_impl(
    py: Python,
    ctx: ClientContextPtr,
    job_ids: Vec<PyJobId>,
    callback: &PyAny,
) -> PyResult<Vec<PyJobId>> {
    run_future(async move {
        let mut remaining_job_ids: Set<PyJobId> = job_ids.iter().copied().collect();

        let mut ctx = borrow_mut!(py, ctx);
        let mut response: JobInfoResponse;

        loop {
            let selector = IdSelector::Specific(IntArray::from_ids(
                remaining_job_ids.iter().copied().collect(),
            ));

            response = hyperqueue::rpc_call!(
                ctx.session.connection(),
                FromClientMessage::JobInfo(JobInfoRequest {
                    selector,
                }),
                ToClientMessage::JobInfoResponse(r) => r
            )
            .await
            .map_py_err()?;

            for job in response.jobs.iter() {
                if is_terminated(job) {
                    remaining_job_ids.remove(&PyJobId::from(job.id));
                }
            }

            let status: HashMap<PyJobId, JobWaitStatus> = response
                .jobs
                .iter()
                .map(|job| {
                    let counters = job.counters;
                    let status = JobWaitStatus {
                        finished: counters.n_finished_tasks.into(),
                        failed: counters.n_failed_tasks.into(),
                        total: job.n_tasks.into(),
                        completed: is_terminated(job),
                    };
                    (job.id.into(), status)
                })
                .collect();
            let args = PyTuple::new(py, &[status.into_py(py)]);
            callback.call1(args)?;

            if remaining_job_ids.is_empty() {
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let failed_jobs = response
            .jobs
            .into_iter()
            .filter(|j| j.counters.has_unsuccessful_tasks())
            .map(|j| j.id.into())
            .collect();
        Ok(failed_jobs)
    })
}

#[derive(dict_derive::IntoPyObject)]
pub struct FailedTaskContext {
    stdout: Option<String>,
    stderr: Option<String>,
    cwd: Option<String>,
    error: String,
}

pub type FailedTaskMap = HashMap<PyJobId, HashMap<PyTaskId, FailedTaskContext>>;

fn stdio_to_string(stdio: StdioDef) -> Option<String> {
    match stdio {
        StdioDef::File { path, .. } => Some(path.to_string_lossy().to_string()),
        _ => None,
    }
}

pub fn get_failed_tasks_impl(
    py: Python,
    ctx: ClientContextPtr,
    job_ids: Vec<PyJobId>,
) -> PyResult<FailedTaskMap> {
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
            rpc_call!(ctx.session.connection(), message, ToClientMessage::JobDetailResponse(r) => r)
                .await
                .map_py_err()?;

        let mut result = HashMap::with_capacity(response.details.len());
        for (job_id, job_detail) in response.details {
            if let Some(job_detail) = job_detail {
                let mut task_path_map = resolve_task_paths(&job_detail, &response.server_uid);
                let mut tasks = HashMap::with_capacity(job_detail.tasks.len());
                for task in job_detail.tasks {
                    match task.state {
                        JobTaskState::Failed { error, .. } => {
                            let id = task.task_id.as_num();
                            let path_ctx = task_path_map.remove(&JobTaskId::from(id)).flatten();
                            let (stdout, stderr, cwd) = match path_ctx {
                                Some(paths) => (
                                    stdio_to_string(paths.stdout),
                                    stdio_to_string(paths.stderr),
                                    Some(paths.cwd.to_string_lossy().to_string()),
                                ),
                                None => (None, None, None),
                            };

                            tasks.insert(
                                id,
                                FailedTaskContext {
                                    stdout,
                                    stderr,
                                    cwd,
                                    error,
                                },
                            );
                        }
                        _ => panic!("Invalid state"),
                    }
                }
                result.insert(job_id.into(), tasks);
            }
        }

        Ok(result)
    })
}
