use std::path::{Path, PathBuf};

use bstr::BString;
use tokio::sync::oneshot;

use tako::messages::common::ProgramDefinition;
use tako::messages::gateway::{
    FromGatewayMessage, NewTasksMessage, TaskConf, TaskDef, ToGatewayMessage,
};
use tako::TaskId;

use crate::client::status::task_status;
use crate::common::arraydef::IntArray;
use crate::common::env::{HQ_ENTRY, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::common::placeholders::{
    fill_placeholders_after_submit, fill_placeholders_log, normalize_path,
};
use crate::server::job::Job;
use crate::server::rpc::Backend;
use crate::server::state::StateRef;
use crate::stream::server::control::StreamServerControlMessage;
use crate::transfer::messages::{
    JobDescription, ResubmitRequest, SubmitRequest, SubmitResponse, TaskBody, TaskDescription,
    ToClientMessage,
};
use crate::{JobId, JobTaskId};

pub async fn handle_submit(
    state_ref: &StateRef,
    tako_ref: &Backend,
    message: SubmitRequest,
) -> ToClientMessage {
    let SubmitRequest {
        mut job_desc,
        name,
        max_fails,
        submit_dir,
        mut log,
    } = message;

    let (resources, time_limit, priority) = match job_desc {
        JobDescription::Array { ref task_desc, .. } => (
            task_desc.resources.clone(),
            task_desc.time_limit,
            task_desc.priority,
        ),
    };

    let (task_defs, job_detail, job_id) = {
        let mut state = state_ref.get_mut();
        let job_id = state.new_job_id();

        // Prefill currently known placeholders eagerly
        match job_desc {
            JobDescription::Array {
                ref mut task_desc, ..
            } => {
                fill_placeholders_after_submit(&mut task_desc.program, job_id, &submit_dir);
            }
        };

        let task_count = job_desc.task_count();
        let tako_base_id = state.new_task_id(task_count).as_num();
        let task_defs =
            make_task_defs(&job_desc, job_id, JobTaskId::new(tako_base_id), &submit_dir);

        if let Some(ref mut log) = log {
            fill_placeholders_log(log, job_id, &submit_dir);
            *log = normalize_path(log, &submit_dir);
        }

        let job = Job::new(
            job_desc,
            job_id,
            tako_base_id.into(),
            name,
            max_fails,
            log.clone(),
            submit_dir,
        );
        let job_detail = job.make_job_detail(false);
        state.add_job(job);

        (task_defs, job_detail, job_id)
    };

    if let Some(log) = log {
        start_log_streaming(tako_ref, job_id, log).await;
    }

    match tako_ref
        .send_tako_message(FromGatewayMessage::NewTasks(NewTasksMessage {
            tasks: task_defs,
            configurations: vec![TaskConf {
                resources,
                n_outputs: 0,
                time_limit,
                keep: false,
                observe: true,
                priority,
            }],
        }))
        .await
        .unwrap()
    {
        ToGatewayMessage::NewTasksResponse(_) => { /* Ok */ }
        _ => panic!("Invalid response"),
    };

    ToClientMessage::SubmitResponse(SubmitResponse { job: job_detail })
}

pub async fn handle_resubmit(
    state_ref: &StateRef,
    tako_ref: &Backend,
    message: ResubmitRequest,
) -> ToClientMessage {
    let msg_submit: SubmitRequest = {
        let state = state_ref.get_mut();
        let job = state.get_job(message.job_id);
        if let Some(job) = job {
            let job_desc = if let Some(filter) = &message.status {
                match &job.job_desc {
                    JobDescription::Array {
                        task_desc,
                        entries,
                        ids: _,
                    } => {
                        let mut ids: Vec<u32> = job
                            .tasks
                            .values()
                            .filter_map(|v| {
                                if filter.contains(&task_status(&v.state)) {
                                    Some(v.task_id.as_num())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        ids.sort_unstable();
                        JobDescription::Array {
                            ids: IntArray::from_ids(ids),
                            entries: entries.clone(),
                            task_desc: task_desc.clone(),
                        }
                    }
                }
            } else {
                job.job_desc.clone()
            };

            SubmitRequest {
                job_desc,
                name: job.name.clone(),
                max_fails: job.max_fails,
                submit_dir: std::env::current_dir().expect("Cannot get current working directory"),
                log: None, // TODO: Reuse log configuration
            }
        } else {
            return ToClientMessage::Error("Invalid job_id".to_string());
        }
    };
    handle_submit(state_ref, tako_ref, msg_submit).await
}

async fn start_log_streaming(tako_ref: &Backend, job_id: JobId, path: PathBuf) {
    let (sender, receiver) = oneshot::channel();
    tako_ref.send_stream_control(StreamServerControlMessage::RegisterStream {
        job_id,
        path,
        response: sender,
    });
    assert!(receiver.await.is_ok());
}

fn make_task_defs(
    job_desc: &JobDescription,
    job_id: JobId,
    tako_base_id: JobTaskId,
    submit_dir: &Path,
) -> Vec<TaskDef> {
    let tako_base_id = tako_base_id.as_num();

    match &job_desc {
        JobDescription::Array {
            ids,
            entries: None,
            task_desc: task_def,
        } => ids
            .iter()
            .zip(tako_base_id..)
            .map(|(task_id, tako_id)| {
                make_task_def(
                    job_id,
                    task_id.into(),
                    tako_id.into(),
                    None,
                    task_def,
                    submit_dir,
                )
            })
            .collect(),
        JobDescription::Array {
            ids,
            entries: Some(entries),
            task_desc: task_def,
        } => ids
            .iter()
            .zip(tako_base_id..)
            .zip(entries.iter())
            .map(|((task_id, tako_id), entry)| {
                make_task_def(
                    job_id,
                    task_id.into(),
                    tako_id.into(),
                    Some(entry.clone()),
                    task_def,
                    submit_dir,
                )
            })
            .collect(),
    }
}

fn make_program_def_for_task(
    program_def: &ProgramDefinition,
    job_id: JobId,
    task_id: JobTaskId,
    submit_dir: &Path,
) -> ProgramDefinition {
    let mut def = program_def.clone();
    def.env.insert(HQ_JOB_ID.into(), job_id.to_string().into());
    def.env
        .insert(HQ_TASK_ID.into(), task_id.to_string().into());
    def.env.insert(
        HQ_SUBMIT_DIR.into(),
        BString::from(submit_dir.to_string_lossy().as_bytes()),
    );
    def
}

fn make_task_def(
    job_id: JobId,
    task_id: JobTaskId,
    tako_id: TaskId,
    entry: Option<BString>,
    def: &TaskDescription,
    submit_dir: &Path,
) -> TaskDef {
    let mut program = make_program_def_for_task(&def.program, job_id, task_id, submit_dir);
    if let Some(e) = entry {
        program.env.insert(HQ_ENTRY.into(), e);
    }
    let body_msg = TaskBody {
        program,
        pin: def.pin,
        job_id,
        task_id,
    };
    let body = tako::transfer::auth::serialize(&body_msg).unwrap();
    TaskDef {
        id: tako_id,
        conf_idx: 0,
        task_deps: Vec::new(),
        body,
    }
}
