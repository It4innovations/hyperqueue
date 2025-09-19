use crate::client::commands::submit::command::{
    DEFAULT_STDERR_PATH, DEFAULT_STDOUT_PATH, send_submit_request,
};
use crate::client::commands::submit::defs::{
    ArrayDef, JobDef, StdioDefFull, StdioDefInput, TaskDef,
};
use crate::client::commands::submit::defs::{PinMode as PinModeDef, TaskConfigDef};
use crate::client::globalsettings::GlobalSettings;
use crate::common::arraydef::IntArray;
use crate::common::utils::fs::get_current_dir;
use crate::transfer::connection::ClientSession;
use crate::transfer::messages::{
    JobDescription, JobSubmitDescription, JobTaskDescription, PinMode, SubmitRequest,
    TaskDescription, TaskKind, TaskKindProgram, TaskWithDependencies,
};
use clap::Parser;
use smallvec::smallvec;
use std::path::PathBuf;
use tako::Map;
use tako::gateway::{EntryType, ResourceRequest, ResourceRequestVariants, TaskDataFlags};
use tako::program::{FileOnCloseBehavior, ProgramDefinition, StdioDef};
use tako::{JobId, JobTaskCount, JobTaskId};

#[derive(Parser)]
pub struct JobSubmitFileOpts {
    /// Path to file with job definition
    path: PathBuf,

    /// Attach a submission to an open job
    #[clap(long)]
    job: Option<JobId>,
}

fn create_stdio(def: Option<StdioDefInput>, default: &str, has_streaming: bool) -> StdioDef {
    match def {
        None => {
            if has_streaming {
                StdioDef::Pipe
            } else {
                StdioDef::File {
                    path: PathBuf::from(default),
                    on_close: FileOnCloseBehavior::default(),
                }
            }
        }
        Some(StdioDefInput::None) => StdioDef::Null,
        Some(StdioDefInput::Path(path)) => StdioDef::File {
            path,
            on_close: FileOnCloseBehavior::default(),
        },
        Some(StdioDefInput::Full(StdioDefFull { path, on_close })) => {
            StdioDef::File { path, on_close }
        }
    }
}

fn build_task_description(cfg: TaskConfigDef, has_streaming: bool) -> TaskDescription {
    TaskDescription {
        kind: TaskKind::ExternalProgram(TaskKindProgram {
            program: ProgramDefinition {
                args: cfg.command.into_iter().map(|x| x.into()).collect(),
                env: cfg.env,
                stdout: create_stdio(cfg.stdout, DEFAULT_STDOUT_PATH, has_streaming),
                stderr: create_stdio(cfg.stderr, DEFAULT_STDERR_PATH, has_streaming),
                stdin: cfg.stdin.map(|s| s.as_bytes().into()).unwrap_or_default(),
                cwd: cfg.cwd.map(|x| x.into()).unwrap_or_else(get_current_dir),
            },
            pin_mode: match cfg.pin {
                PinModeDef::None => PinMode::None,
                PinModeDef::TaskSet => PinMode::TaskSet,
                PinModeDef::OpenMP => PinMode::OpenMP,
            },
            task_dir: cfg.task_dir,
        }),
        resources: ResourceRequestVariants {
            variants: if cfg.request.is_empty() {
                smallvec![ResourceRequest::default()]
            } else {
                cfg.request.into_iter().map(|r| r.into_request()).collect()
            },
        },
        time_limit: cfg.time_limit,
        priority: cfg.priority,
        crash_limit: cfg.crash_limit,
    }
}

fn build_task(
    tdef: TaskDef,
    max_id: &mut JobTaskId,
    data_flags: TaskDataFlags,
    has_streaming: bool,
) -> TaskWithDependencies {
    let id = tdef.id.unwrap_or_else(|| {
        *max_id = JobTaskId::new(max_id.as_num() + 1);
        *max_id
    });
    TaskWithDependencies {
        id,
        data_flags,
        task_desc: build_task_description(tdef.config, has_streaming),
        task_deps: tdef.deps,
        data_deps: tdef.data_deps,
    }
}

fn build_job_desc_array(array: ArrayDef, has_streaming: bool) -> JobTaskDescription {
    let ids = array
        .ids
        .unwrap_or_else(|| IntArray::from_range(0, array.entries.len() as JobTaskCount));
    let entries = if array.entries.is_empty() {
        None
    } else {
        Some(
            array
                .entries
                .into_iter()
                .map(|s| EntryType::from(s.as_bytes()))
                .collect(),
        )
    };
    JobTaskDescription::Array {
        ids,
        entries,
        task_desc: build_task_description(array.config, has_streaming),
    }
}

fn build_job_desc_individual_tasks(
    tasks: Vec<TaskDef>,
    data_flags: TaskDataFlags,
    has_streaming: bool,
) -> crate::Result<JobTaskDescription> {
    let mut max_id: JobTaskId = tasks
        .iter()
        .map(|t| t.id)
        .max()
        .flatten()
        .unwrap_or(JobTaskId::new(0));

    /* Topological sort */
    let original_len = tasks.len();
    let mut new_tasks = Vec::with_capacity(original_len);
    let mut unprocessed_tasks = Map::new();
    let mut in_degrees = Map::new();
    let mut consumers: Map<JobTaskId, Vec<_>> = Map::new();
    for task in tasks {
        let t = build_task(task, &mut max_id, data_flags, has_streaming);
        if in_degrees.insert(t.id, t.task_deps.len()).is_some() {
            return Err(crate::Error::GenericError(format!(
                "Task {} is defined multiple times",
                t.id
            )));
        }
        let is_empty = t.task_deps.is_empty();
        if is_empty {
            new_tasks.push(t);
        } else {
            for dep in &t.task_deps {
                consumers.entry(*dep).or_default().push(t.id);
            }
            unprocessed_tasks.insert(t.id, t);
        }
    }
    let mut idx = 0;
    while idx < new_tasks.len() {
        if let Some(consumers) = consumers.get(&new_tasks[idx].id) {
            for c in consumers {
                let d = in_degrees.get_mut(c).unwrap();
                assert!(*d > 0);
                *d -= 1;
                if *d == 0 {
                    new_tasks.push(unprocessed_tasks.remove(c).unwrap())
                }
            }
        }
        idx += 1;
    }

    if unprocessed_tasks.is_empty() {
        assert_eq!(new_tasks.len(), original_len);
    } else {
        let t = unprocessed_tasks.values().next().unwrap();
        return Err(crate::Error::GenericError(format!(
            "Task {} is part of dependency cycle or has an invalid dependencies",
            t.id
        )));
    }

    Ok(JobTaskDescription::Graph { tasks: new_tasks })
}

fn build_job_submit(jdef: JobDef, job_id: Option<JobId>) -> crate::Result<SubmitRequest> {
    let task_desc = if let Some(array) = jdef.array {
        build_job_desc_array(array, jdef.stream.is_some())
    } else {
        let mut data_flags = TaskDataFlags::empty();
        if jdef.data_layer {
            data_flags.insert(TaskDataFlags::ENABLE_DATA_LAYER);
        }
        build_job_desc_individual_tasks(jdef.tasks, data_flags, jdef.stream.is_some())?
    };
    Ok(SubmitRequest {
        job_desc: JobDescription {
            name: jdef.name,
            max_fails: jdef.max_fails,
        },
        submit_desc: JobSubmitDescription {
            task_desc,
            submit_dir: get_current_dir(),
            stream_path: jdef.stream,
        },
        job_id,
    })
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
    let request = build_job_submit(jdef, opts.job)?;
    send_submit_request(gsettings, session, request, false, false, None).await
}
