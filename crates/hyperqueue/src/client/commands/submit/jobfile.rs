use crate::client::commands::submit::command::{
    send_submit_request, DEFAULT_STDERR_PATH, DEFAULT_STDOUT_PATH,
};
use crate::client::commands::submit::defs::PinMode as PinModeDef;
use crate::client::commands::submit::defs::{JobDef, TaskDef};
use crate::client::globalsettings::GlobalSettings;
use crate::common::utils::fs::get_current_dir;
use crate::common::utils::time::parse_human_time;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    JobDescription, PinMode, SubmitRequest, TaskDescription, TaskWithDependencies,
};
use clap::Parser;
use smallvec::smallvec;
use std::path::PathBuf;
use tako::gateway::{ResourceRequest, ResourceRequestVariants};
use tako::program::{ProgramDefinition, StdioDef};

#[derive(Parser)]
pub struct JobSubmitFileOpts {
    /// Path to file with job definition
    path: PathBuf,
}

fn create_stdio(def: &str, default: &str, is_log: bool) -> StdioDef {
    match def {
        "" => {
            if is_log {
                StdioDef::Pipe
            } else {
                StdioDef::File(PathBuf::from(default))
            }
        }
        "none" => StdioDef::Null,
        x => StdioDef::File(PathBuf::from(x)),
    }
}

fn build_task_description(tdef: TaskDef) -> TaskDescription {
    TaskDescription {
        program: ProgramDefinition {
            args: tdef.command.into_iter().map(|x| x.into()).collect(),
            env: tdef.env,
            stdout: create_stdio(&tdef.stdout, DEFAULT_STDOUT_PATH, false),
            stderr: create_stdio(&tdef.stderr, DEFAULT_STDERR_PATH, false),
            stdin: vec![],
            cwd: PathBuf::from(tdef.cwd),
        },
        resources: ResourceRequestVariants {
            variants: if tdef.request.is_empty() {
                smallvec![ResourceRequest::default()]
            } else {
                tdef.request.into_iter().map(|r| r.into_request()).collect()
            },
        },
        pin_mode: match tdef.pin {
            PinModeDef::None => PinMode::None,
            PinModeDef::TaskSet => PinMode::TaskSet,
            PinModeDef::OpenMP => PinMode::OpenMP,
        },
        task_dir: tdef.task_dir,
        time_limit: tdef.time_limit,
        priority: tdef.priority,
        crash_limit: tdef.crash_limit,
    }
}

fn build_task(tdef: TaskDef) -> TaskWithDependencies {
    let id = tdef.id.unwrap();
    let task_desc = build_task_description(tdef);
    TaskWithDependencies {
        id,
        task_desc,
        dependencies: vec![],
    }
}

fn build_job_submit(jdef: JobDef) -> SubmitRequest {
    let job_desc = {
        JobDescription::Graph {
            tasks: jdef.tasks.into_iter().map(build_task).collect(),
        }
    };

    SubmitRequest {
        job_desc,
        name: jdef.name,
        max_fails: jdef.max_fails,
        submit_dir: get_current_dir(),
        log: jdef.stream_log,
    }
}

pub async fn submit_computation_from_job_file(
    gsettings: &GlobalSettings,
    session: &mut ClientSession,
    opts: JobSubmitFileOpts,
) -> anyhow::Result<()> {
    let jdef = {
        JobDef::parse(&std::fs::read_to_string(&opts.path).map_err(|e| {
            anyhow::anyhow!(format!(
                "Cannot read {}: {}",
                opts.path.display(),
                e.to_string()
            ))
        })?)?
    };
    let request = build_job_submit(jdef);
    send_submit_request(gsettings, session, request, false, false).await
}
