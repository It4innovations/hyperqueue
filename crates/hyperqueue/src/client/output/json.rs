use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use anyhow::Error;
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use serde_json;
use serde_json::{Value, json};

use tako::Map;
use tako::gateway::ResourceRequest;
use tako::program::{ProgramDefinition, StdioDef};
use tako::resources::{ResourceDescriptor, ResourceDescriptorItem, ResourceDescriptorKind};
use tako::worker::WorkerConfiguration;

use crate::client::job::WorkerMap;
use crate::client::output::Verbosity;
use crate::client::output::common::{TaskToPathsMap, group_jobs_by_status, resolve_task_paths};
use crate::client::output::outputs::{Output, OutputStream};
use crate::common::arraydef::IntArray;
use crate::common::manager::info::{GetManagerInfo, ManagerType};
use crate::server::autoalloc::{Allocation, AllocationState, QueueId};
use crate::server::job::{JobTaskInfo, JobTaskState, StartedTaskData};
use crate::stream::reader::outputlog::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, JobTaskDescription, PinMode, QueueData, ServerInfo,
    TaskDescription, TaskKind, TaskKindProgram, WaitForJobsResponse, WorkerInfo,
};
use crate::{JobId, JobTaskId};

#[derive(Default)]
pub struct JsonOutput;

impl JsonOutput {
    fn print(&self, data: serde_json::Value) {
        println!(
            "{}",
            serde_json::to_string_pretty(&data).expect("Could not format JSON")
        );
    }
}

// Remember to modify JSON documentation when the JSON output is changed.
impl Output for JsonOutput {
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        self.print(workers.into_iter().map(format_worker_info).collect());
    }
    fn print_worker_info(&self, worker_info: WorkerInfo) {
        self.print(format_worker_info(worker_info));
    }

    fn print_server_description(&self, server_dir: Option<&Path>, record: &ServerInfo) {
        let json = json!({
            "server_dir": server_dir,
            "server_uid": record.server_uid,
            "worker_host": record.worker_host,
            "client_host": record.client_host,
            "client_port": record.client_port,
            "worker_port": record.worker_port,
            "pid": record.pid,
            "start_date": record.start_date,
            "version": record.version,
        });
        self.print(json);
    }

    fn print_job_submitted(&self, job: JobDetail) {
        self.print(json!({
            "id": job.info.id
        }))
    }

    fn print_job_open(&self, job_id: JobId) {
        self.print(json!({
            "id": job_id
        }))
    }

    fn print_job_list(&self, jobs: Vec<JobInfo>, _total_jobs: usize) {
        self.print(
            jobs.into_iter()
                .map(|info| format_job_info(&info))
                .collect(),
        );
    }
    fn print_job_summary(&self, jobs: Vec<JobInfo>) {
        let statuses = group_jobs_by_status(&jobs);
        self.print(json!(statuses))
    }
    fn print_job_detail(&self, jobs: Vec<JobDetail>, _worker_map: WorkerMap, server_uid: &str) {
        let job_details: Vec<_> = jobs
            .into_iter()
            .map(|job| {
                let task_paths = resolve_task_paths(&job, server_uid);

                let JobDetail {
                    info,
                    job_desc,
                    submit_descs,
                    tasks,
                    tasks_not_found: _,
                    submission_date,
                    completion_date_or_now,
                } = job;

                let finished_at = if info.counters.is_terminated(info.n_tasks) {
                    Some(completion_date_or_now)
                } else {
                    None
                };

                json!({
                                    "info": format_job_info(&info),
                                    "max_fails": job_desc.max_fails,
                                    "started_at": format_datetime(submission_date),
                                    "finished_at": finished_at.map(format_datetime),
                                    "submits": submit_descs.iter().map(|submit_desc|
                match &submit_desc.task_desc {
                                    JobTaskDescription::Array { task_desc, .. } => {
                                        json!({
                                            "array": format_task_description(task_desc)
                                        })
                                    }
                                    JobTaskDescription::Graph { tasks } => {
                                        let tasks: Vec<Value> = tasks
                                            .iter()
                                            .map(|task| format_task_description(&task.task_desc))
                                            .collect();
                                        json!({
                                            "graph": tasks
                                        })
                                    }
                                }
                                    ).collect::<Vec<_>>(),
                                    "tasks": format_tasks(&tasks, task_paths)
                                })
            })
            .collect();
        self.print(Value::Array(job_details));
    }

    fn print_job_wait(
        &self,
        duration: Duration,
        response: &WaitForJobsResponse,
        _details: &[(JobId, Option<JobDetail>)],
        _worker_map: WorkerMap,
    ) {
        let WaitForJobsResponse {
            finished,
            failed,
            canceled,
            invalid,
        } = response;
        self.print(json!({
            "duration": format_duration(duration),
            "finished": finished,
            "failed": failed,
            "canceled": canceled,
            "invalid": invalid,
        }))
    }

    fn print_job_output(
        &self,
        _job_detail: JobDetail,
        _output_stream: OutputStream,
        _task_header: bool,
        _task_paths: TaskToPathsMap,
    ) -> anyhow::Result<()> {
        anyhow::bail!("JSON output mode doesn't support job output");
    }

    fn print_task_list(
        &self,
        jobs: Vec<(JobId, JobDetail)>,
        _worker_map: WorkerMap,
        server_uid: &str,
        _verbosity: Verbosity,
    ) {
        let mut json_obj = json!({});
        for (id, job) in jobs {
            let map = resolve_task_paths(&job, server_uid);
            json_obj[id.to_string()] = format_tasks(&job.tasks, map);
        }
        self.print(json_obj);
    }

    fn print_task_info(
        &self,
        job: (JobId, JobDetail),
        tasks: &[(JobTaskId, JobTaskInfo)],
        _worker_map: WorkerMap,
        server_uid: &str,
        _verbosity: Verbosity,
    ) {
        let map = resolve_task_paths(&job.1, server_uid);
        self.print(format_tasks(tasks, map));
    }

    fn print_task_ids(&self, job_task_ids: Vec<(JobId, IntArray)>) {
        let map: HashMap<JobId, Vec<u32>> = job_task_ids
            .into_iter()
            .map(|(key, value)| (key, value.iter().collect()))
            .collect();
        self.print(json!(map));
    }

    fn print_summary(&self, filename: &Path, summary: Summary) {
        let json = json!({
            "filename": filename,
            "summary": summary,
        });
        self.print(json);
    }

    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        let mut queues: Vec<_> = info.queues.into_iter().collect();
        queues.sort_by_key(|descriptor| descriptor.0);

        self.print(
            queues
                .into_iter()
                .map(|(key, queue)| format_autoalloc_queue(key, queue))
                .collect(),
        );
    }

    fn print_allocations(&self, allocations: Vec<Allocation>) {
        self.print(allocations.into_iter().map(format_allocation).collect());
    }

    fn print_hw(&self, descriptor: &ResourceDescriptor) {
        self.print(format_resource_descriptor(descriptor));
    }

    fn print_error(&self, error: Error) {
        self.print(json!({ "error": format!("{error:?}") }))
    }
}

fn format_task_description(task_desc: &TaskDescription) -> Value {
    let TaskDescription {
        kind,
        resources,
        time_limit,
        priority,
        crash_limit,
    } = task_desc;

    match kind {
        TaskKind::ExternalProgram(TaskKindProgram {
            program,
            pin_mode,
            task_dir,
        }) => {
            let ProgramDefinition {
                args,
                env,
                stdout,
                stderr,
                cwd,
                stdin: _,
            } = program;
            json!({
                "program": {
                    "args": args.iter().map(|args| args.to_string()).collect::<Vec<_>>(),
                    "env": env.into_iter().map(|(key, value)| (key.to_string(), value.to_string())).collect::<Map<String, String>>(),
                    "cwd": cwd,
                    "stderr": format_stdio_def(stderr),
                    "stdout": format_stdio_def(stdout),
                },
                "resources": resources
                    .variants
                    .iter()
                    .map(|v| {
                        let ResourceRequest {
                            n_nodes,
                            resources,
                            min_time,
                        } = v;
                        json!({
                            "n_nodes": n_nodes,
                            "resources": resources.into_iter().map(|res| {
                                json!({
                                    "resource": res.resource,
                                    "request": res.policy
                                })
                            }).collect::<Vec<_>>(),
                            "min_time": format_duration(*min_time)
                        })
                    })
                    .collect::<Vec<_>>(),
                "pin_mode": match pin_mode {
                    PinMode::None => None,
                    PinMode::OpenMP => Some("openmp"),
                    PinMode::TaskSet => Some("taskset"),
                },
                "priority": priority,
                "time_limit": time_limit.map(format_duration),
                "task_dir": task_dir,
                "crash_limit": crash_limit,
            })
        }
    }
}

fn fill_task_started_data(dict: &mut Value, data: &StartedTaskData) {
    dict["started_at"] = format_datetime(data.start_date);
    if data.worker_ids.len() == 1 {
        dict["worker"] = data.worker_ids[0].as_num().into();
    } else {
        dict["workers"] = data
            .worker_ids
            .iter()
            .map(|worker_id| worker_id.as_num().into())
            .collect::<Vec<Value>>()
            .into();
    }
}

fn fill_task_paths(dict: &mut Value, map: &TaskToPathsMap, task_id: JobTaskId) {
    if let Some(ref paths) = map[&task_id] {
        dict["cwd"] = paths.cwd.to_str().unwrap().into();
        dict["stdout"] = format_stdio_def(&paths.stdout);
        dict["stderr"] = format_stdio_def(&paths.stderr);
    }
}

fn format_stdio_def(stdio: &StdioDef) -> Value {
    match stdio {
        StdioDef::Null => Value::Null,
        StdioDef::File { path, .. } => json!(Some(path)),
        StdioDef::Pipe => json!("<pipe>"),
    }
}

fn format_job_info(info: &JobInfo) -> Value {
    let JobInfo {
        id,
        name,
        n_tasks,
        counters,
        is_open,
    } = info;

    json!({
        "id": id,
        "name": name,
        "task_count": n_tasks,
        "is_open": is_open,
        "task_stats": json!({
            "running": counters.n_running_tasks,
            "finished": counters.n_finished_tasks,
            "failed": counters.n_failed_tasks,
            "canceled": counters.n_canceled_tasks,
            "waiting": counters.n_waiting_tasks(*n_tasks)
        })
    })
}

fn format_tasks(tasks: &[(JobTaskId, JobTaskInfo)], map: TaskToPathsMap) -> Value {
    tasks
        .iter()
        .map(|(task_id, task)| {
            let state = &match task.state {
                JobTaskState::Waiting => "waiting",
                JobTaskState::Running { .. } => "running",
                JobTaskState::Finished { .. } => "finished",
                JobTaskState::Failed { .. } => "failed",
                JobTaskState::Canceled { .. } => "canceled",
            };
            let mut data = json!({
                "id": *task_id,
                "state": state,
            });
            fill_task_paths(&mut data, &map, *task_id);

            match &task.state {
                JobTaskState::Running { started_data } => {
                    fill_task_started_data(&mut data, started_data);
                }
                JobTaskState::Finished {
                    started_data,
                    end_date,
                } => {
                    fill_task_started_data(&mut data, started_data);
                    data["finished_at"] = format_datetime(*end_date);
                }
                JobTaskState::Failed {
                    started_data,
                    end_date,
                    error,
                } => {
                    if let Some(started_data) = started_data {
                        fill_task_started_data(&mut data, started_data);
                    }
                    data["finished_at"] = format_datetime(*end_date);
                    data["error"] = error.clone().into();
                }
                _ => {}
            };
            data
        })
        .collect()
}

struct FormattedManagerType(ManagerType);

impl Serialize for FormattedManagerType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            ManagerType::Pbs => serializer.serialize_str("PBS"),
            ManagerType::Slurm => serializer.serialize_str("Slurm"),
        }
    }
}

fn format_autoalloc_queue(id: QueueId, descriptor: QueueData) -> serde_json::Value {
    let QueueData {
        info,
        name,
        manager_type,
        state,
    } = descriptor;

    let manager = FormattedManagerType(manager_type);
    json!({
        "id": id,
        "name": name,
        "state": state,
        "manager": manager,
        "additional_args": info.additional_args(),
        "backlog": info.backlog(),
        "workers_per_alloc": info.workers_per_alloc(),
        "timelimit": format_duration(info.timelimit()),
        "max_worker_count": info.max_worker_count(),
        "worker_args": info.worker_args(),
    })
}
fn format_allocation(allocation: Allocation) -> serde_json::Value {
    let Allocation {
        id,
        target_worker_count,
        queued_at,
        status,
        working_dir,
    } = allocation;

    let status_name = match &status {
        AllocationState::Queued { .. } => "queued",
        AllocationState::Running { .. } => "running",
        AllocationState::Finished { .. } | AllocationState::FinishedUnexpectedly { .. } => {
            match status.is_failed() {
                true => "failed",
                false => "finished",
            }
        }
    };
    let started_at = match status {
        AllocationState::Running { started_at, .. }
        | AllocationState::Finished { started_at, .. } => Some(started_at),
        _ => None,
    };
    let ended_at = match status {
        AllocationState::Finished { finished_at, .. } => Some(finished_at),
        _ => None,
    };

    json!({
        "id": id,
        "target_worker_count": target_worker_count,
        "queued_at": format_datetime(queued_at),
        "started_at": started_at.map(format_datetime),
        "ended_at": ended_at.map(format_datetime),
        "status": status_name,
        "workdir": working_dir
    })
}

fn format_worker_info(worker_info: WorkerInfo) -> serde_json::Value {
    let manager_info = worker_info.configuration.get_manager_info();

    let WorkerInfo {
        id,
        configuration:
            WorkerConfiguration {
                resources,
                listen_address,
                hostname,
                work_dir,
                heartbeat_interval,
                overview_configuration: _,
                idle_timeout,
                time_limit,
                on_server_lost,
                group,
                extra: _,
            },
        started,
        ended,
        runtime_info,
        last_task_started,
    } = worker_info;

    json!({
        "id": id,
        "configuration": json!({
            "heartbeat_interval": format_duration(heartbeat_interval),
            "idle_timeout": idle_timeout.map(format_duration),
            "time_limit": time_limit.map(format_duration),
            "work_dir": work_dir,
            "hostname": hostname,
            "group": group,
            "listen_address": listen_address,
            "resources": format_resource_descriptor(&resources),
            "on_server_lost": crate::common::format::server_lost_policy_to_str(&on_server_lost),
        }),
        "allocation": manager_info.map(|info| json!({
            "manager": FormattedManagerType(info.manager),
            "id": info.allocation_id
        })),
        "runtime_info": runtime_info,
        "last_task_started": last_task_started,
        "started": format_datetime(started),
        "ended": ended.map(|info| json!({
            "at": format_datetime(info.ended_at)
        }))
    })
}
fn format_resource_descriptor(descriptor: &ResourceDescriptor) -> Value {
    let ResourceDescriptor { resources } = descriptor;
    json!({
        "resources": resources.iter().map(format_resource).collect::<Vec<_>>()
    })
}

fn format_resource(resource: &ResourceDescriptorItem) -> Value {
    match &resource.kind {
        ResourceDescriptorKind::List { values } => json!({
            "name": resource.name,
            "kind": "list",
            "values": values,
        }),
        ResourceDescriptorKind::Range { start, end } => json!({
            "name": resource.name,
            "kind": "range",
            "start": start,
            "end": end
        }),
        ResourceDescriptorKind::Sum { size } => json!({
            "name": resource.name,
            "kind": "sum",
            "size": size
        }),
        ResourceDescriptorKind::Groups { groups } => json!({
            "name": resource.name,
            "kind": "groups",
            "groups": groups,
        }),
    }
}

fn format_duration(duration: Duration) -> serde_json::Value {
    let value = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;
    json!(value)
}
pub fn format_datetime<T: Into<DateTime<Utc>>>(time: T) -> serde_json::Value {
    json!(time.into())
}
