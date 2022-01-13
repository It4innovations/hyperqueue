use hyperqueue::common::arraydef::IntArray;
use hyperqueue::common::fsutils::get_current_dir;
use hyperqueue::rpc_call;
use hyperqueue::tako::messages::common::ProgramDefinition;
use hyperqueue::transfer::messages::{
    FromClientMessage, JobDescription as HqJobDescription, SubmitRequest,
    TaskDescription as HqTaskDescription, ToClientMessage,
};
use pyo3::{PyResult, Python};

use crate::utils::error::ToPyResult;
use crate::{borrow_mut, run_future, ContextPtr, FromPyObject};

#[derive(Debug, FromPyObject)]
pub struct TaskDescription {
    args: Vec<String>,
    cwd: Option<String>,
}

#[derive(Debug, FromPyObject)]
pub struct JobDescription {
    tasks: Vec<TaskDescription>,
}

pub(crate) fn submit_job_impl(
    py: Python,
    ctx: ContextPtr,
    mut job: JobDescription,
) -> PyResult<u32> {
    run_future(async move {
        let task = job.tasks.pop().unwrap();

        let job_desc = HqJobDescription::Array {
            ids: IntArray::from_id(0),
            entries: None,
            task_desc: HqTaskDescription {
                program: ProgramDefinition {
                    args: task.args.into_iter().map(|arg| arg.into()).collect(),
                    env: Default::default(),
                    stdout: Default::default(),
                    stderr: Default::default(),
                    cwd: task.cwd.map(|p| p.into()),
                },
                resources: Default::default(),
                pin: false,
                time_limit: None,
                priority: 0,
            },
        };

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
