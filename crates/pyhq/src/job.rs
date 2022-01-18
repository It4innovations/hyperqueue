use hyperqueue::common::fsutils::get_current_dir;
use hyperqueue::rpc_call;
use hyperqueue::tako::messages::common::ProgramDefinition;
use hyperqueue::transfer::messages::{
    FromClientMessage, JobDescription as HqJobDescription, SubmitRequest,
    TaskDescription as HqTaskDescription, TaskWithDependencies, ToClientMessage,
};
use pyo3::{PyResult, Python};
use std::collections::HashMap;

use crate::utils::error::ToPyResult;
use crate::{borrow_mut, run_future, ContextPtr, FromPyObject};

#[derive(Debug, FromPyObject)]
pub struct TaskDescription {
    id: u32,
    args: Vec<String>,
    cwd: Option<String>,
    env: HashMap<String, String>,
    dependencies: Vec<u32>,
}

#[derive(Debug, FromPyObject)]
pub struct JobDescription {
    tasks: Vec<TaskDescription>,
}

pub(crate) fn submit_job_impl(py: Python, ctx: ContextPtr, job: JobDescription) -> PyResult<u32> {
    run_future(async move {
        let tasks = build_tasks(job.tasks);
        let job_desc = HqJobDescription::Graph { tasks };

        let message = FromClientMessage::Submit(SubmitRequest {
            job_desc,
            name: "".to_string(),
            max_fails: None,
            submit_dir: get_current_dir(),
            log: None,
        });

        let mut ctx = borrow_mut!(py, ctx);
        let response = rpc_call!(ctx.connection, message, ToClientMessage::SubmitResponse(r) => r)
            .await
            .map_py_err()?;
        Ok(response.job.info.id.as_num())
    })
}

fn build_tasks(tasks: Vec<TaskDescription>) -> Vec<TaskWithDependencies> {
    tasks
        .into_iter()
        .map(|task| TaskWithDependencies {
            id: task.id.into(),
            task_desc: build_task_desc(task.args, task.env, task.cwd),
            dependencies: task.dependencies.into_iter().map(|id| id.into()).collect(),
        })
        .collect()
}

fn build_task_desc(
    args: Vec<String>,
    env: HashMap<String, String>,
    cwd: Option<String>,
) -> HqTaskDescription {
    HqTaskDescription {
        program: ProgramDefinition {
            args: args.into_iter().map(|arg| arg.into()).collect(),
            env: env.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
            stdout: Default::default(),
            stderr: Default::default(),
            cwd: cwd.map(|p| p.into()),
        },
        resources: Default::default(),
        pin: false,
        time_limit: None,
        priority: 0,
    }
}
