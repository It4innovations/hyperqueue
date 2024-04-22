use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use bstr::BString;
use tako::ItemId;
use tako::Map;
use tokio::sync::oneshot;

use tako::gateway::{
    FromGatewayMessage, NewTasksMessage, ResourceRequestVariants, SharedTaskConfiguration,
    TaskConfiguration, ToGatewayMessage,
};
use tako::launcher::TaskResult;
use tako::TaskId;

use crate::common::arraydef::IntArray;
use crate::common::placeholders::{
    fill_placeholders_after_submit, fill_placeholders_log, normalize_path,
};
use crate::server::job::Job;
use crate::server::state::{State, StateRef};
use crate::server::Senders;
use crate::stream::server::control::StreamServerControlMessage;
use crate::transfer::messages::{
    JobDescription, JobTaskDescription, SubmitRequest, SubmitResponse, TaskBuildDescription,
    TaskDescription, TaskIdSelector, TaskKind, TaskKindProgram, TaskSelector, TaskStatusSelector,
    TaskWithDependencies, ToClientMessage,
};
use crate::{JobId, JobTaskId, Priority, TakoTaskId};

fn create_new_task_message(
    job_id: JobId,
    tako_base_id: TakoTaskId,
    job_desc: &mut JobDescription,
) -> anyhow::Result<NewTasksMessage> {
    match &mut job_desc.task_desc {
        JobTaskDescription::Array {
            ids,
            entries,
            task_desc,
        } => Ok(build_tasks_array(
            job_id,
            tako_base_id,
            &ids,
            std::mem::take(entries),
            &task_desc,
            &job_desc.submit_dir,
        )),
        JobTaskDescription::Graph { tasks } => {
            build_tasks_graph(job_id, tako_base_id, tasks, &job_desc.submit_dir)
        }
    }
}

pub(crate) fn submit_job_desc(
    state: &mut State,
    job_id: JobId,
    mut job_desc: JobDescription,
) -> crate::Result<NewTasksMessage> {
    let (job_id, tako_base_id) = prepare_job(job_id, &mut job_desc, state);
    let new_tasks = create_new_task_message(job_id, tako_base_id, &mut job_desc)?;
    job_desc.strip_large_data();
    let job = Job::new(Rc::new(job_desc), job_id, tako_base_id);
    state.add_job(job);
    Ok(new_tasks)
}

pub(crate) async fn handle_submit(
    state_ref: &StateRef,
    senders: &Senders,
    mut message: SubmitRequest,
) -> ToClientMessage {
    let job_id = state_ref.get_mut().new_job_id();
    let SubmitRequest { mut job_desc } = message;

    senders.events.on_job_submitted(job_id, &job_desc).unwrap();

    let new_tasks = match submit_job_desc(&mut state_ref.get_mut(), job_id, job_desc) {
        Err(error) => {
            state_ref.get_mut().revert_to_job_id(job_id);
            return ToClientMessage::Error(error.to_string());
        }
        Ok(new_tasks) => new_tasks,
    };
    senders.autoalloc.on_job_created(job_id);

    let (job_detail, log) = {
        let state = state_ref.get();
        let job = state.get_job(job_id).unwrap();
        let job_detail = job.make_job_detail(Some(&TaskSelector {
            id_selector: TaskIdSelector::All,
            status_selector: TaskStatusSelector::All,
        }));
        (job_detail, job.job_desc.log.clone())
    };

    if let Some(log) = log {
        start_log_streaming(senders, job_id, log).await;
    }

    match senders
        .backend
        .send_tako_message(FromGatewayMessage::NewTasks(new_tasks))
        .await
        .unwrap()
    {
        ToGatewayMessage::NewTasksResponse(_) => { /* Ok */ }
        r => panic!("Invalid response: {r:?}"),
    };

    ToClientMessage::SubmitResponse(SubmitResponse {
        job: job_detail,
        server_uid: state_ref.get().server_info().server_uid.clone(),
    })
}

/// Prefills placeholders in the submit request and creates job ID
fn prepare_job(
    job_id: JobId,
    job_desc: &mut JobDescription,
    state: &mut State,
) -> (JobId, TakoTaskId) {
    // Prefill currently known placeholders eagerly
    if let JobTaskDescription::Array {
        ref mut task_desc, ..
    } = job_desc.task_desc
    {
        match &mut task_desc.kind {
            TaskKind::ExternalProgram(TaskKindProgram { program, .. }) => {
                fill_placeholders_after_submit(
                    program,
                    job_id,
                    &job_desc.submit_dir,
                    &state.server_info().server_uid,
                );
            }
        }
    };

    if let Some(ref mut log) = job_desc.log {
        fill_placeholders_log(
            log,
            job_id,
            &job_desc.submit_dir,
            &state.server_info().server_uid,
        );
        *log = normalize_path(log, &job_desc.submit_dir);
    }

    let task_count = job_desc.task_desc.task_count();
    (job_id, state.new_task_id(task_count))
}

pub(crate) async fn start_log_streaming(senders: &Senders, job_id: JobId, path: PathBuf) {
    let (sender, receiver) = oneshot::channel();
    senders
        .backend
        .send_stream_control(StreamServerControlMessage::RegisterStream {
            job_id,
            path,
            response: sender,
        });
    assert!(receiver.await.is_ok());
}

fn serialize_task_body(
    job_id: JobId,
    task_id: JobTaskId,
    entry: Option<BString>,
    task_desc: &TaskDescription,
    submit_dir: &PathBuf,
) -> Box<[u8]> {
    let body_msg = TaskBuildDescription {
        task_kind: Cow::Borrowed(&task_desc.kind),
        job_id: job_id,
        task_id,
        submit_dir: Cow::Borrowed(submit_dir),
        entry,
    };
    let body = tako::comm::serialize(&body_msg).expect("Could not serialize task body");
    // Make sure that `into_boxed_slice` is a no-op.
    debug_assert_eq!(body.capacity(), body.len());
    body.into_boxed_slice()
}

fn build_tasks_array(
    job_id: JobId,
    tako_base_id: TakoTaskId,
    ids: &IntArray,
    entries: Option<Vec<BString>>,
    task_desc: &TaskDescription,
    submit_dir: &PathBuf,
) -> NewTasksMessage {
    let tako_base_id = tako_base_id.as_num();

    let build_task_conf =
        |body: Box<[u8]>, tako_id: <TakoTaskId as ItemId>::IdType| TaskConfiguration {
            id: tako_id.into(),
            shared_data_index: 0,
            task_deps: Vec::new(),
            body,
        };

    let tasks = match entries {
        None => ids
            .iter()
            .zip(tako_base_id..)
            .map(|(task_id, tako_id)| {
                build_task_conf(
                    serialize_task_body(job_id, task_id.into(), None, task_desc, submit_dir),
                    tako_id,
                )
            })
            .collect(),
        Some(entries) => ids
            .iter()
            .zip(tako_base_id..)
            .zip(entries)
            .map(|((task_id, tako_id), entry)| {
                build_task_conf(
                    serialize_task_body(job_id, task_id.into(), Some(entry), task_desc, submit_dir),
                    tako_id,
                )
            })
            .collect(),
    };

    NewTasksMessage {
        tasks,
        shared_data: vec![SharedTaskConfiguration {
            resources: task_desc.resources.clone(),
            n_outputs: 0,
            time_limit: task_desc.time_limit,
            keep: false,
            observe: true,
            priority: task_desc.priority,
            crash_limit: task_desc.crash_limit,
        }],
        adjust_instance_id: Default::default(),
    }
}

fn build_tasks_graph(
    job_id: JobId,
    tako_base_id: TakoTaskId,
    tasks: &[TaskWithDependencies],
    submit_dir: &PathBuf,
) -> anyhow::Result<NewTasksMessage> {
    let mut job_task_id_to_tako_id: Map<JobTaskId, TaskId> = Map::with_capacity(tasks.len());

    let mut tako_id = tako_base_id.as_num();
    for task in tasks {
        if job_task_id_to_tako_id
            .insert(task.id, tako_id.into())
            .is_some()
        {
            return Err(anyhow::anyhow!("Duplicate task ID {}", task.id));
        }
        tako_id += 1;
    }

    let mut shared_data = vec![];
    let mut shared_data_map =
        Map::<(Cow<ResourceRequestVariants>, Option<Duration>, Priority), usize>::new();
    let mut allocate_shared_data = |task: &TaskDescription| -> u32 {
        shared_data_map
            .get(&(
                Cow::Borrowed(&task.resources),
                task.time_limit,
                task.priority,
            ))
            .copied()
            .unwrap_or_else(|| {
                let index = shared_data.len();
                shared_data_map.insert(
                    (
                        Cow::Owned(task.resources.clone()),
                        task.time_limit,
                        task.priority,
                    ),
                    index,
                );
                shared_data.push(SharedTaskConfiguration {
                    resources: task.resources.clone(),
                    n_outputs: 0,
                    time_limit: task.time_limit,
                    priority: task.priority,
                    keep: false,
                    observe: true,
                    crash_limit: task.crash_limit,
                });
                index
            }) as u32
    };

    let mut task_configs = Vec::with_capacity(tasks.len());
    for task in tasks {
        let body = serialize_task_body(job_id, task.id, None, &task.task_desc, submit_dir);
        let shared_data_index = allocate_shared_data(&task.task_desc);

        let mut task_deps = Vec::with_capacity(task.dependencies.len());
        for dependency in &task.dependencies {
            if *dependency == task.id {
                return Err(anyhow::anyhow!("Task {} depends on itself", task.id));
            }
            match job_task_id_to_tako_id.get(dependency) {
                Some(id) => task_deps.push(*id),
                None => {
                    return Err(anyhow::anyhow!(
                        "Task {} depends on an unknown task with ID {}",
                        task.id,
                        dependency
                    ));
                }
            }
        }

        task_configs.push(TaskConfiguration {
            id: job_task_id_to_tako_id[&task.id],
            shared_data_index,
            task_deps,
            body,
        });
    }

    Ok(NewTasksMessage {
        tasks: task_configs,
        shared_data,
        adjust_instance_id: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use crate::server::client::submit::build_tasks_graph;
    use crate::transfer::messages::{
        PinMode, TaskDescription, TaskKind, TaskKindProgram, TaskWithDependencies,
    };
    use smallvec::smallvec;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use tako::gateway::{
        NewTasksMessage, ResourceRequest, ResourceRequestEntry, ResourceRequestVariants,
    };
    use tako::program::ProgramDefinition;
    use tako::resources::{AllocationRequest, ResourceAmount, CPU_RESOURCE_NAME};
    use tako::Priority;

    #[test]
    fn test_build_graph_deduplicate_shared_confs() {
        let desc_a = || task_desc(None, 0, 1);
        let desc_b = || task_desc(Some(Duration::default()), 0, 1);
        let desc_c = || task_desc(None, 0, 3);

        let tasks = vec![
            task(0, desc_a(), vec![]),
            task(1, desc_c(), vec![]),
            task(2, desc_a(), vec![]),
            task(3, desc_b(), vec![]),
            task(4, desc_a(), vec![]),
        ];

        let msg = build_tasks_graph(1.into(), 2.into(), &tasks, &PathBuf::from("foo")).unwrap();

        check_shared_data(&msg, vec![desc_a(), desc_c(), desc_b()]);
        assert_eq!(
            msg.tasks
                .into_iter()
                .map(|t| t.shared_data_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 0, 2, 0]
        );
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

        let msg = build_tasks_graph(1.into(), 2.into(), &tasks, &PathBuf::from("foo")).unwrap();
        assert_eq!(msg.tasks[0].task_deps, vec![4.into(), 3.into()]);
        assert_eq!(msg.tasks[1].task_deps, vec![2.into()]);
        assert_eq!(msg.tasks[2].task_deps, vec![5.into(), 6.into()]);
        assert_eq!(msg.tasks[3].task_deps, vec![]);
        assert_eq!(msg.tasks[4].task_deps, vec![2.into()]);
    }

    #[test]
    fn test_build_graph_duplicate_id() {
        let desc = || task_desc(None, 0, 1);
        let tasks = vec![
            task(0, desc(), vec![]),
            task(1, desc(), vec![]),
            task(0, desc(), vec![]),
        ];

        assert!(build_tasks_graph(1.into(), 2.into(), &tasks, &PathBuf::from("foo")).is_err());
    }

    #[test]
    fn test_build_graph_task_depends_on_itself() {
        let desc = || task_desc(None, 0, 1);
        let tasks = vec![task(0, desc(), vec![]), task(1, desc(), vec![1])];

        assert!(build_tasks_graph(1.into(), 2.into(), &tasks, &PathBuf::from("foo")).is_err());
    }

    #[test]
    fn test_build_graph_task_missing_dependency() {
        let desc = || task_desc(None, 0, 1);
        let tasks = vec![task(0, desc(), vec![3]), task(1, desc(), vec![])];

        assert!(build_tasks_graph(1.into(), 2.into(), &tasks, &PathBuf::from("foo")).is_err());
    }

    fn check_shared_data(msg: &NewTasksMessage, expected: Vec<TaskDescription>) {
        assert_eq!(msg.shared_data.len(), expected.len());
        for (shared, expected) in msg.shared_data.iter().zip(expected) {
            assert_eq!(shared.resources, expected.resources);
            assert_eq!(shared.time_limit, expected.time_limit);
            assert_eq!(shared.priority, expected.priority);
        }
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
            crash_limit: 5,
        }
    }

    fn task(id: u32, task_desc: TaskDescription, dependencies: Vec<u32>) -> TaskWithDependencies {
        TaskWithDependencies {
            id: id.into(),
            task_desc,
            dependencies: dependencies.into_iter().map(|id| id.into()).collect(),
        }
    }
}
