use std::borrow::Cow;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use tako::gateway::{
    EntryType, SharedTaskConfiguration, TaskConfiguration, TaskDataFlags, TaskSubmit,
};
use tako::{Set, TaskId};
use thin_vec::ThinVec;

use crate::common::arraydef::IntArray;
use crate::common::placeholders::{
    fill_placeholders_after_submit, fill_placeholders_log, normalize_path,
};
use crate::server::Senders;
use crate::server::job::{Job, JobTaskState, SubmittedJobDescription};
use crate::server::state::{State, StateRef};
use crate::transfer::messages::{
    JobDescription, JobSubmitDescription, JobTaskDescription, OpenJobResponse, SingleIdSelector,
    SubmitRequest, SubmitResponse, TaskBuildDescription, TaskDescription, TaskExplainRequest,
    TaskExplainResponse, TaskIdSelector, TaskKind, TaskKindProgram, TaskSelector,
    TaskStatusSelector, TaskWithDependencies, ToClientMessage,
};
use tako::{JobId, JobTaskCount, JobTaskId};

fn create_task_submit(job_id: JobId, submit_desc: &mut JobSubmitDescription) -> TaskSubmit {
    match &mut submit_desc.task_desc {
        JobTaskDescription::Array {
            ids,
            entries,
            task_desc,
        } => build_tasks_array(
            job_id,
            ids,
            std::mem::take(entries),
            task_desc,
            &submit_desc.submit_dir,
            submit_desc.stream_path.as_ref(),
        ),
        JobTaskDescription::Graph { tasks } => build_tasks_graph(
            job_id,
            tasks,
            &submit_desc.submit_dir,
            submit_desc.stream_path.as_ref(),
        ),
    }
}

pub(crate) fn submit_job_desc(
    state: &mut State,
    job_id: JobId,
    mut submit_desc: JobSubmitDescription,
    submitted_at: DateTime<Utc>,
) -> TaskSubmit {
    prepare_job(job_id, &mut submit_desc, state);
    let task_submit = create_task_submit(job_id, &mut submit_desc);
    submit_desc.strip_large_data();
    state
        .get_job_mut(job_id)
        .unwrap()
        .attach_submit(SubmittedJobDescription::at(submitted_at, submit_desc));
    task_submit
}

pub(crate) fn validate_submit(
    job: Option<&Job>,
    task_desc: &JobTaskDescription,
) -> Option<SubmitResponse> {
    match &task_desc {
        JobTaskDescription::Array { ids, .. } => {
            if let Some(job) = job {
                for id in ids.iter() {
                    let id = JobTaskId::new(id);
                    if job.tasks.contains_key(&id) {
                        return Some(SubmitResponse::TaskIdAlreadyExists(id));
                    }
                }
            }
        }
        JobTaskDescription::Graph { tasks } => {
            if let Some(job) = job {
                for task in tasks {
                    if job.tasks.contains_key(&task.id) {
                        let id = task.id;
                        return Some(SubmitResponse::TaskIdAlreadyExists(id));
                    }
                }
            }
            let mut task_ids = Set::new();
            for task in tasks {
                if !task_ids.insert(task.id) {
                    return Some(SubmitResponse::NonUniqueTaskId(task.id));
                }
                for dep_id in &task.task_deps {
                    if *dep_id == task.id
                        || (!task_ids.contains(dep_id)
                            && !job
                                .map(|job| job.tasks.contains_key(dep_id))
                                .unwrap_or(false))
                    {
                        return Some(SubmitResponse::InvalidDependencies(*dep_id));
                    }
                }
            }
        }
    }
    None
}

#[allow(clippy::await_holding_refcell_ref)] // Disable lint as it does not work well with drop
pub(crate) fn handle_submit(
    state_ref: &StateRef,
    senders: &Senders,
    mut message: SubmitRequest,
) -> ToClientMessage {
    let mut state = state_ref.get_mut();
    if let Some(err) = validate_submit(
        message.job_id.and_then(|job_id| state.get_job(job_id)),
        &message.submit_desc.task_desc,
    ) {
        return ToClientMessage::SubmitResponse(err);
    }

    let (job_id, new_job) = if let Some(job_id) = message.job_id {
        if let Some(job) = state.get_job(job_id) {
            if !job.is_open() {
                return ToClientMessage::SubmitResponse(SubmitResponse::JobNotOpened);
            }
            match &mut message.submit_desc.task_desc {
                JobTaskDescription::Array { ids, entries, .. } => {
                    if ids.is_empty() {
                        let new_id = job.max_id().map(|x| x.as_num() + 1).unwrap_or(0);
                        if let Some(entries) = entries {
                            *ids =
                                IntArray::from_range(new_id, new_id + entries.len() as JobTaskCount)
                        } else {
                            *ids = IntArray::from_id(new_id)
                        }
                    }
                }
                JobTaskDescription::Graph { .. } => {}
            }
        } else {
            return ToClientMessage::SubmitResponse(SubmitResponse::JobNotFound);
        }
        (job_id, false)
    } else {
        match &mut message.submit_desc.task_desc {
            JobTaskDescription::Array { ids, entries, .. } => {
                /* Try fillin task ids */
                if ids.is_empty() {
                    if let Some(entries) = entries {
                        *ids = IntArray::from_range(0, entries.len() as JobTaskCount)
                    } else {
                        *ids = IntArray::from_id(0)
                    }
                }
            }
            JobTaskDescription::Graph { .. } => { /* Do nothing */ }
        }
        (state.new_job_id(), true)
    };

    senders.events.on_job_submitted(job_id, &message).unwrap();

    let SubmitRequest {
        job_desc,
        submit_desc,
        job_id: _,
    } = message;

    if new_job {
        let job = Job::new(job_id, job_desc, false);
        state.add_job(job);
    }

    let new_tasks = submit_job_desc(&mut state, job_id, submit_desc, Utc::now());
    senders.autoalloc.on_job_submit(job_id);

    let job_detail = state
        .get_job(job_id)
        .unwrap()
        .make_job_detail(Some(&TaskSelector {
            id_selector: TaskIdSelector::All,
            status_selector: TaskStatusSelector::All,
        }));
    drop(state);

    senders.server_control.add_new_tasks(new_tasks).unwrap();
    ToClientMessage::SubmitResponse(SubmitResponse::Ok {
        job: job_detail,
        server_uid: state_ref.get().server_info().server_uid.clone(),
    })
}

/// Prefills placeholders in the submit request and creates job ID
fn prepare_job(job_id: JobId, submit_desc: &mut JobSubmitDescription, state: &mut State) {
    // Prefill currently known placeholders eagerly
    if let JobTaskDescription::Array {
        ref mut task_desc, ..
    } = submit_desc.task_desc
    {
        match &mut task_desc.kind {
            TaskKind::ExternalProgram(TaskKindProgram { program, .. }) => {
                fill_placeholders_after_submit(
                    program,
                    job_id,
                    &submit_desc.submit_dir,
                    &state.server_info().server_uid,
                );
            }
        }
    };

    if let Some(path) = &mut submit_desc.stream_path {
        fill_placeholders_log(
            path,
            job_id,
            &submit_desc.submit_dir,
            &state.server_info().server_uid,
        );
        *path = normalize_path(path.as_path(), &submit_desc.submit_dir);
    }
}

fn serialize_task_body(
    task_desc: &TaskDescription,
    submit_dir: &PathBuf,
    stream_path: Option<&PathBuf>,
) -> Box<[u8]> {
    let body_msg = TaskBuildDescription {
        task_kind: Cow::Borrowed(&task_desc.kind),
        submit_dir: Cow::Borrowed(submit_dir),
        stream_path: stream_path.map(Cow::Borrowed),
    };
    let body = tako::comm::serialize(&body_msg).expect("Could not serialize task body");
    // Make sure that `into_boxed_slice` is a no-op.
    debug_assert_eq!(body.capacity(), body.len());
    body.into_boxed_slice()
}

fn build_tasks_array(
    job_id: JobId,
    ids: &IntArray,
    entries: Option<Vec<EntryType>>,
    task_desc: &TaskDescription,
    submit_dir: &PathBuf,
    stream_path: Option<&PathBuf>,
) -> TaskSubmit {
    let build_task_conf = |tako_id: TaskId, entry: Option<EntryType>| TaskConfiguration {
        id: tako_id,
        shared_data_index: 0,
        task_deps: ThinVec::new(),
        dataobj_deps: ThinVec::new(),
        entry,
    };

    let tasks = match entries {
        None => ids
            .iter()
            .map(|job_task_id| {
                let task_id = TaskId::new(job_id, job_task_id.into());
                build_task_conf(task_id, None)
            })
            .collect(),
        Some(entries) => ids
            .iter()
            .zip(entries)
            .map(|(job_task_id, entry)| {
                let task_id = TaskId::new(job_id, job_task_id.into());
                build_task_conf(task_id, Some(entry))
            })
            .collect(),
    };

    TaskSubmit {
        tasks,
        shared_data: vec![SharedTaskConfiguration {
            resources: task_desc.resources.clone(),
            time_limit: task_desc.time_limit,
            priority: task_desc.priority,
            crash_limit: task_desc.crash_limit,
            data_flags: TaskDataFlags::empty(),
            body: serialize_task_body(task_desc, submit_dir, stream_path),
        }],
        adjust_instance_id_and_crash_counters: Default::default(),
    }
}

fn build_tasks_graph(
    job_id: JobId,
    tasks: &[TaskWithDependencies],
    submit_dir: &PathBuf,
    stream_path: Option<&PathBuf>,
) -> TaskSubmit {
    let mut shared_data = vec![];
    let mut allocate_shared_data = |task: &TaskDescription, data_flags: TaskDataFlags| -> u32 {
        let index = shared_data.len();
        shared_data.push(SharedTaskConfiguration {
            resources: task.resources.clone(),
            time_limit: task.time_limit,
            priority: task.priority,
            crash_limit: task.crash_limit,
            data_flags,
            body: serialize_task_body(task, submit_dir, stream_path),
        });
        index as u32
    };

    let mut task_configs = Vec::with_capacity(tasks.len());
    for task in tasks {
        let shared_data_index = allocate_shared_data(&task.task_desc, task.data_flags);

        let mut task_dep_ids: Set<JobTaskId> = task.task_deps.iter().copied().collect();

        let dataobj_deps = task
            .data_deps
            .iter()
            .map(|job_do_id| {
                // If we depend on data from a task, we need to depends on the whole task
                task_dep_ids.insert(job_do_id.task_id);
                job_do_id.to_dataobj_id(job_id)
            })
            .collect();

        let task_deps = task_dep_ids
            .into_iter()
            .map(|task_id| TaskId::new(job_id, task_id))
            .collect();

        task_configs.push(TaskConfiguration {
            id: TaskId::new(job_id, task.id),
            shared_data_index,
            task_deps,
            dataobj_deps,
            entry: None,
        });
    }

    TaskSubmit {
        tasks: task_configs,
        shared_data,
        adjust_instance_id_and_crash_counters: Default::default(),
    }
}

pub(crate) fn handle_task_explain(
    state_ref: &StateRef,
    senders: &Senders,
    request: TaskExplainRequest,
) -> ToClientMessage {
    let state = state_ref.get();
    let job_id = match request.job_selector {
        SingleIdSelector::Specific(job_id) => JobId::new(job_id),
        SingleIdSelector::Last => state.last_job_id(),
    };
    let Some(job) = state.get_job(job_id) else {
        return ToClientMessage::Error("Job not found".to_string());
    };
    let Some(task) = job.tasks.get(&request.task_id) else {
        return ToClientMessage::Error("Task not found".to_string());
    };
    match task.state {
        JobTaskState::Waiting | JobTaskState::Running { .. } => {
            let task_id = TaskId::new(job_id, request.task_id);
            match senders.server_control.task_explain(task_id) {
                Ok(explanation) => ToClientMessage::TaskExplain(TaskExplainResponse {
                    task_id,
                    explanation,
                }),
                Err(e) => ToClientMessage::Error(e.to_string()),
            }
        }
        _ => ToClientMessage::Error(
            "Explain command works only for tasks in WAITING or RUNNING state".to_string(),
        ),
    }
}

pub(crate) fn handle_open_job(
    state_ref: &StateRef,
    senders: &Senders,
    job_description: JobDescription,
) -> ToClientMessage {
    let job_id = state_ref.get_mut().new_job_id();
    senders
        .events
        .on_job_opened(job_id, job_description.clone());
    let job = Job::new(job_id, job_description, true);
    state_ref.get_mut().add_job(job);
    ToClientMessage::OpenJobResponse(OpenJobResponse { job_id })
}

#[cfg(test)]
mod tests {
    use crate::common::arraydef::IntArray;
    use crate::server::client::submit::build_tasks_graph;
    use crate::server::client::validate_submit;
    use crate::server::job::{Job, SubmittedJobDescription};
    use crate::transfer::messages::{
        JobDescription, JobSubmitDescription, JobTaskDescription, PinMode, SubmitResponse,
        TaskDescription, TaskKind, TaskKindProgram, TaskWithDependencies,
    };
    use chrono::Utc;
    use smallvec::smallvec;
    use std::path::PathBuf;
    use std::time::Duration;
    use tako::gateway::{
        CrashLimit, ResourceRequest, ResourceRequestEntry, ResourceRequestVariants,
        SharedTaskConfiguration, TaskDataFlags,
    };
    use tako::internal::tests::utils::sorted_vec;
    use tako::program::ProgramDefinition;
    use tako::resources::{AllocationRequest, CPU_RESOURCE_NAME, ResourceAmount};
    use tako::{Priority, TaskId};

    #[test]
    fn test_validate_submit() {
        let mut job = Job::new(
            10.into(),
            JobDescription {
                name: "".to_string(),
                max_fails: None,
            },
            true,
        );
        job.attach_submit(SubmittedJobDescription::at(
            Utc::now(),
            JobSubmitDescription {
                task_desc: JobTaskDescription::Array {
                    ids: IntArray::from_range(100, 10),
                    entries: None,
                    task_desc: task_desc(None, 0, 1),
                },
                submit_dir: Default::default(),
                stream_path: None,
            },
        ));

        let job_task_desc = JobTaskDescription::Array {
            ids: IntArray::from_range(109, 2),
            entries: None,
            task_desc: task_desc(None, 0, 1),
        };
        assert!(validate_submit(None, &job_task_desc).is_none());
        assert!(matches!(
            validate_submit(Some(&job), &job_task_desc),
            Some(SubmitResponse::TaskIdAlreadyExists(x)) if x.as_num() == 109
        ));
        let job_task_desc = JobTaskDescription::Graph {
            tasks: vec![TaskWithDependencies {
                id: 102.into(),
                task_desc: task_desc(None, 0, 1),
                task_deps: vec![],
                data_deps: vec![],
                data_flags: TaskDataFlags::empty(),
            }],
        };
        assert!(validate_submit(None, &job_task_desc).is_none());
        assert!(matches!(
            validate_submit(Some(&job), &job_task_desc),
            Some(SubmitResponse::TaskIdAlreadyExists(x)) if x.as_num() == 102
        ));
        let job_task_desc = JobTaskDescription::Graph {
            tasks: vec![
                TaskWithDependencies {
                    id: 2.into(),
                    task_desc: task_desc(None, 0, 1),
                    task_deps: vec![],
                    data_deps: vec![],
                    data_flags: TaskDataFlags::empty(),
                },
                TaskWithDependencies {
                    id: 2.into(),
                    task_desc: task_desc(None, 0, 1),
                    task_deps: vec![],
                    data_deps: vec![],
                    data_flags: TaskDataFlags::empty(),
                },
            ],
        };
        assert!(matches!(
            validate_submit(None, &job_task_desc),
            Some(SubmitResponse::NonUniqueTaskId(x)) if x.as_num() == 2
        ));
        let job_task_desc = JobTaskDescription::Graph {
            tasks: vec![TaskWithDependencies {
                id: 2.into(),
                task_desc: task_desc(None, 0, 1),
                task_deps: vec![3.into()],
                data_deps: vec![],
                data_flags: TaskDataFlags::empty(),
            }],
        };
        assert!(matches!(
            validate_submit(None, &job_task_desc),
            Some(SubmitResponse::InvalidDependencies(x)) if x.as_num() == 3
        ));
        let job_task_desc = JobTaskDescription::Graph {
            tasks: vec![TaskWithDependencies {
                id: 2.into(),
                task_desc: task_desc(None, 0, 1),
                task_deps: vec![2.into()],
                data_deps: vec![],
                data_flags: TaskDataFlags::empty(),
            }],
        };
        assert!(matches!(
            validate_submit(None, &job_task_desc),
            Some(SubmitResponse::InvalidDependencies(x)) if x.as_num() == 2
        ));
    }

    #[test]
    fn test_build_graph_with_dependencies() {
        let desc = || task_desc(None, 0, 1);
        let tasks = vec![
            task(0, desc(), vec![2, 1]),
            task(1, desc(), vec![0]),
            task(2, desc(), vec![3, 4]),
            task(3, desc(), vec![]),
            task(4, desc(), vec![0]),
        ];

        let msg = build_tasks_graph(1.into(), &tasks, &PathBuf::from("foo"), None);
        assert_eq!(
            sorted_vec(msg.tasks[0].task_deps.to_vec()),
            vec![
                TaskId::new(1.into(), 1.into()),
                TaskId::new(1.into(), 2.into())
            ]
        );
        assert_eq!(
            msg.tasks[1].task_deps,
            vec![TaskId::new(1.into(), 0.into())]
        );
        assert_eq!(
            sorted_vec(msg.tasks[2].task_deps.to_vec()),
            vec![
                TaskId::new(1.into(), 3.into()),
                TaskId::new(1.into(), 4.into())
            ]
        );
        assert_eq!(msg.tasks[3].task_deps, vec![]);
        assert_eq!(
            msg.tasks[4].task_deps,
            vec![TaskId::new(1.into(), 0.into()),]
        );
    }

    fn task_desc(
        time_limit: Option<Duration>,
        priority: Priority,
        cpu_count: u32,
    ) -> TaskDescription {
        TaskDescription {
            kind: TaskKind::ExternalProgram(TaskKindProgram {
                program: ProgramDefinition {
                    args: vec![],
                    env: Default::default(),
                    stdout: Default::default(),
                    stderr: Default::default(),
                    stdin: vec![],
                    cwd: Default::default(),
                },
                pin_mode: PinMode::None,
                task_dir: false,
            }),
            resources: ResourceRequestVariants::new_simple(ResourceRequest {
                n_nodes: 0,
                min_time: Duration::from_secs(2),
                resources: smallvec![ResourceRequestEntry {
                    resource: CPU_RESOURCE_NAME.to_string(),
                    policy: AllocationRequest::Compact(ResourceAmount::new_units(cpu_count)),
                }],
            }),
            time_limit,
            priority,
            crash_limit: CrashLimit::default(),
        }
    }

    fn task(id: u32, task_desc: TaskDescription, dependencies: Vec<u32>) -> TaskWithDependencies {
        TaskWithDependencies {
            id: id.into(),
            task_desc,
            task_deps: dependencies.into_iter().map(|id| id.into()).collect(),
            data_deps: vec![],
            data_flags: TaskDataFlags::empty(),
        }
    }
}
