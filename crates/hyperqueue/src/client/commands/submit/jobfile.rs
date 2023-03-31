use crate::client::commands::submit::command::{
    send_submit_request, DEFAULT_STDERR_PATH, DEFAULT_STDOUT_PATH,
};
use crate::client::commands::submit::defs::{ArrayDef, JobDef, TaskDef};
use crate::client::commands::submit::defs::{PinMode as PinModeDef, TaskConfigDef};
use crate::client::globalsettings::GlobalSettings;
use crate::common::arraydef::IntArray;
use crate::common::utils::fs::get_current_dir;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    JobDescription, PinMode, SubmitRequest, TaskDescription, TaskWithDependencies,
};
use crate::{JobTaskCount, JobTaskId};
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

fn create_stdio(def: Option<&str>, default: &str, is_log: bool) -> StdioDef {
    match def {
        None => {
            if is_log {
                StdioDef::Pipe
            } else {
                StdioDef::File(PathBuf::from(default))
            }
        }
        Some("none") => StdioDef::Null,
        Some(x) => StdioDef::File(PathBuf::from(x)),
    }
}

fn build_task_description(cfg: TaskConfigDef) -> TaskDescription {
    TaskDescription {
        program: ProgramDefinition {
            args: cfg.command.into_iter().map(|x| x.into()).collect(),
            env: cfg.env,
            stdout: create_stdio(cfg.stdout.as_deref(), DEFAULT_STDOUT_PATH, false),
            stderr: create_stdio(cfg.stderr.as_deref(), DEFAULT_STDERR_PATH, false),
            stdin: cfg.stdin.map(|s| s.as_bytes().into()).unwrap_or_default(),
            cwd: PathBuf::from(cfg.cwd.unwrap_or("".into())),
        },
        resources: ResourceRequestVariants {
            variants: if cfg.request.is_empty() {
                smallvec![ResourceRequest::default()]
            } else {
                cfg.request.into_iter().map(|r| r.into_request()).collect()
            },
        },
        pin_mode: match cfg.pin {
            PinModeDef::None => PinMode::None,
            PinModeDef::TaskSet => PinMode::TaskSet,
            PinModeDef::OpenMP => PinMode::OpenMP,
        },
        task_dir: cfg.task_dir,
        time_limit: cfg.time_limit,
        priority: cfg.priority,
        crash_limit: cfg.crash_limit,
    }
}

fn build_task(tdef: TaskDef, max_id: &mut JobTaskId) -> TaskWithDependencies {
    let id = tdef.id.unwrap_or_else(|| {
        *max_id = JobTaskId::new(max_id.as_num() + 1);
        *max_id
    });
    let task_desc = build_task_description(tdef.config);
    TaskWithDependencies {
        id,
        task_desc,
        dependencies: vec![],
    }
}

fn build_job_desc_array(array: ArrayDef) -> JobDescription {
    let ids = array
        .ids
        .unwrap_or_else(|| IntArray::from_range(0, array.entries.len() as JobTaskCount));
    let entries = if array.entries.is_empty() {
        None
    } else {
        Some(array.entries.into_iter().map(|s| s.into()).collect())
    };
    JobDescription::Array {
        ids,
        entries,
        task_desc: build_task_description(array.config),
    }
}

fn build_job_desc_individual_tasks(tasks: Vec<TaskDef>) -> JobDescription {
    let mut max_id: JobTaskId = tasks
        .iter()
        .map(|t| t.id)
        .max()
        .flatten()
        .unwrap_or(JobTaskId(0));

    JobDescription::Graph {
        tasks: tasks
            .into_iter()
            .map(|t| build_task(t, &mut max_id))
            .collect(),
    }
}

fn build_job_submit(jdef: JobDef) -> SubmitRequest {
    let job_desc = if let Some(array) = jdef.array {
        build_job_desc_array(array)
    } else {
        build_job_desc_individual_tasks(jdef.tasks)
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
    let jdef =
        {
            JobDef::parse(&std::fs::read_to_string(&opts.path).map_err(|e| {
                anyhow::anyhow!(format!("Cannot read {}: {}", opts.path.display(), e))
            })?)?
        };
    let request = build_job_submit(jdef);
    send_submit_request(gsettings, session, request, false, false).await
}
