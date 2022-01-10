use crate::common::env::{HQ_ENTRY, HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::common::placeholders::{
    fill_placeholders_after_submit, fill_placeholders_log, normalize_path,
};
use crate::server::job::Job;
use crate::server::rpc::Backend;
use crate::server::state::StateRef;
use crate::stream::server::control::StreamServerControlMessage;
use crate::transfer::messages::{
    JobType, SubmitRequest, SubmitResponse, TaskBody, ToClientMessage,
};
use crate::{JobId, JobTaskId};
use bstr::BString;
use std::path::{Path, PathBuf};
use tako::messages::common::ProgramDefinition;
use tako::messages::gateway::{
    FromGatewayMessage, NewTasksMessage, TaskConf, TaskDef, ToGatewayMessage,
};
use tokio::sync::oneshot;

pub async fn handle_submit(
    state_ref: &StateRef,
    tako_ref: &Backend,
    message: SubmitRequest,
) -> ToClientMessage {
    let SubmitRequest {
        job_type,
        name,
        max_fails,
        spec: mut def,
        resources,
        pin,
        entries,
        submit_dir,
        priority,
        time_limit,
        mut log,
    } = message;

    let make_task = |job_id, task_id, tako_id, entry: Option<BString>, def: &ProgramDefinition| {
        let mut program = make_program_def_for_task(def, job_id, task_id, &submit_dir);
        if let Some(e) = entry {
            program.env.insert(HQ_ENTRY.into(), e);
        }
        let body_msg = TaskBody {
            program,
            pin,
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
    };
    let (task_defs, job_detail, job_id) = {
        let mut state = state_ref.get_mut();
        let job_id = state.new_job_id();

        fill_placeholders_after_submit(&mut def, job_id, &submit_dir);

        let task_count = match &job_type {
            JobType::Simple => 1,
            JobType::Array(a) => a.id_count(),
        };
        let tako_base_id = state.new_task_id(task_count).as_num();
        let task_defs = match (&job_type, entries.clone()) {
            (JobType::Simple, _) => {
                vec![make_task(job_id, 0.into(), tako_base_id.into(), None, &def)]
            }
            (JobType::Array(a), None) => a
                .iter()
                .zip(tako_base_id..)
                .map(|(task_id, tako_id)| {
                    make_task(job_id, task_id.into(), tako_id.into(), None, &def)
                })
                .collect(),
            (JobType::Array(a), Some(entries)) => a
                .iter()
                .zip(tako_base_id..)
                .zip(entries.into_iter())
                .map(|((task_id, tako_id), entry)| {
                    make_task(job_id, task_id.into(), tako_id.into(), Some(entry), &def)
                })
                .collect(),
        };

        if let Some(ref mut log) = log {
            fill_placeholders_log(log, job_id, &submit_dir);
            *log = normalize_path(log, &submit_dir);
        }

        let job = Job::new(
            job_type,
            job_id,
            tako_base_id.into(),
            name,
            def,
            resources.clone(),
            pin,
            max_fails,
            entries,
            priority,
            time_limit,
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

async fn start_log_streaming(tako_ref: &Backend, job_id: JobId, path: PathBuf) {
    let (sender, receiver) = oneshot::channel();
    tako_ref.send_stream_control(StreamServerControlMessage::RegisterStream {
        job_id,
        path,
        response: sender,
    });
    assert!(receiver.await.is_ok());
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
