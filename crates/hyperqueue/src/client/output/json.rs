use std::path::Path;
use std::time::Duration;

use anyhow::Error;
use chrono::{DateTime, Utc};
use serde_json;
use serde_json::{json, Value};
use tako::Map;

use tako::gateway::ResourceRequest;
use tako::program::{ProgramDefinition, StdioDef};
use tako::resources::{
    CpuRequest, GenericResourceDescriptor, GenericResourceDescriptorKind, ResourceDescriptor,
};
use tako::worker::WorkerConfiguration;

use crate::client::job::WorkerMap;
use crate::client::output::common::{resolve_task_paths, TaskToPathsMap};
use crate::client::output::outputs::{Output, OutputStream};
use crate::common::manager::info::ManagerType;
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{Allocation, AllocationState, QueueId};
use crate::server::job::{JobTaskInfo, JobTaskState, StartedTaskData};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDescription, JobDetail, JobInfo, QueueData, StatsResponse,
    TaskDescription, WaitForJobsResponse, WorkerInfo,
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

    fn print_server_record(&self, server_dir: &Path, record: &AccessRecord) {
        let json = json!({
            "server_dir": server_dir,
            "server_uid": record.server_uid(),
            "host": record.host(),
            "pid": record.pid(),
            "hq_port": record.server_port(),
            "worker_port": record.worker_port(),
            "start_date": record.start_date(),
            "version": record.version(),
        });
        self.print(json);
    }
    fn print_server_stats(&self, stats: StatsResponse) {
        self.print(json!(stats));
    }

    fn print_job_submitted(&self, job: JobDetail) {
        self.print(json!({
            "id": job.info.id
        }))
    }

    fn print_job_list(&self, jobs: Vec<JobInfo>, _total_jobs: usize) {
        self.print(jobs.into_iter().map(format_job_info).collect());
    }
    fn print_job_detail(&self, job: JobDetail, _worker_map: WorkerMap) {
        let task_paths = resolve_task_paths(&job);

        let JobDetail {
            info,
            job_desc,
            tasks,
            tasks_not_found: _,
            max_fails,
            submission_date,
            completion_date_or_now,
            submit_dir,
        } = job;

        let finished_at = if info.counters.is_terminated(info.n_tasks) {
            Some(completion_date_or_now)
        } else {
            None
        };

        let mut json = json!({
            "info": format_job_info(info),
            "max_fails": max_fails,
            "started_at": format_datetime(submission_date),
            "finished_at": finished_at.map(format_datetime),
            "submit_dir": submit_dir
        });

        if let JobDescription::Array {
            task_desc:
                TaskDescription {
                    program:
                        ProgramDefinition {
                            args,
                            env,
                            stdout,
                            stderr,
                            cwd,
                            stdin: _,
                        },
                    resources:
                        ResourceRequest {
                            n_nodes,
                            cpus,
                            generic,
                            min_time,
                        },
                    pin_mode,
                    time_limit,
                    priority,
                    task_dir,
                },
            ..
        } = job_desc
        {
            json["program"] = json!({
                "args": args.into_iter().map(|args| args.to_string()).collect::<Vec<_>>(),
                "env": env.into_iter().map(|(key, value)| (key.to_string(), value.to_string())).collect::<Map<String, String>>(),
                "cwd": cwd,
                "stderr": format_stdio_def(&stderr),
                "stdout": format_stdio_def(&stdout),
            });
            json["resources"] = json!({
                "n_nodes": n_nodes,
                "cpus": format_cpu_request(cpus),
                "generic": generic,
                "min_time": format_duration(min_time)
            });
            json["pin_mode"] = json!(pin_mode);
            json["priority"] = json!(priority);
            json["time_limit"] = json!(time_limit.map(format_duration));
            json["task_dir"] = json!(task_dir);
        }

        json["tasks"] = format_tasks(tasks, task_paths);
        self.print(json);
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
        _tasks: Vec<JobTaskInfo>,
        _output_stream: OutputStream,
        _task_header: bool,
        _task_paths: TaskToPathsMap,
    ) -> anyhow::Result<()> {
        anyhow::bail!("JSON output mode doesn't support job output");
    }

    fn print_tasks(&self, jobs: Vec<(JobId, JobDetail)>, _worker_map: WorkerMap) {
        let mut json_obj = json!({});
        for (id, job) in jobs {
            let map = resolve_task_paths(&job);
            json_obj[id.to_string()] = format_tasks(job.tasks, map);
        }
        self.print(json_obj);
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
                .iter()
                .map(|(key, queue)| format_autoalloc_queue(*key, queue))
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
        self.print(json!({ "error": format!("{:?}", error) }))
    }
}

fn fill_task_started_data(dict: &mut Value, data: StartedTaskData) {
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
    json!(stdio)
}

fn format_job_info(info: JobInfo) -> serde_json::Value {
    let JobInfo {
        id,
        name,
        n_tasks,
        counters,
    } = info;

    json!({
        "id": id,
        "name": name,
        "task_count": n_tasks,
        "task_stats": json!({
            "running": counters.n_running_tasks,
            "finished": counters.n_finished_tasks,
            "failed": counters.n_failed_tasks,
            "canceled": counters.n_canceled_tasks,
            "waiting": counters.n_waiting_tasks(n_tasks)
        })
    })
}
fn format_cpu_request(request: CpuRequest) -> serde_json::Value {
    let cpus = &match request {
        CpuRequest::Compact(count)
        | CpuRequest::ForceCompact(count)
        | CpuRequest::Scatter(count) => Some(count),
        CpuRequest::All => None,
    };
    let name = match request {
        CpuRequest::Compact(_) => "compact",
        CpuRequest::ForceCompact(_) => "force-compact",
        CpuRequest::Scatter(_) => "scatter",
        CpuRequest::All => "all",
    };
    json!({
        "type": name,
        "cpus": cpus
    })
}

fn format_tasks(tasks: Vec<JobTaskInfo>, map: TaskToPathsMap) -> serde_json::Value {
    tasks
        .into_iter()
        .map(|task| {
            let state = &match task.state {
                JobTaskState::Waiting => "waiting",
                JobTaskState::Running { .. } => "running",
                JobTaskState::Finished { .. } => "finished",
                JobTaskState::Failed { .. } => "failed",
                JobTaskState::Canceled { .. } => "canceled",
            };
            let mut data = json!({
                "id": task.task_id,
                "state": state,
            });
            fill_task_paths(&mut data, &map, task.task_id);

            match task.state {
                JobTaskState::Running { started_data } => {
                    fill_task_started_data(&mut data, started_data);
                }
                JobTaskState::Finished {
                    started_data,
                    end_date,
                } => {
                    fill_task_started_data(&mut data, started_data);
                    data["finished_at"] = format_datetime(end_date);
                }
                JobTaskState::Failed {
                    started_data,
                    end_date,
                    error,
                } => {
                    fill_task_started_data(&mut data, started_data);
                    data["finished_at"] = format_datetime(end_date);
                    data["error"] = error.into();
                }
                _ => {}
            };
            data
        })
        .collect()
}

fn format_autoalloc_queue(id: QueueId, descriptor: &QueueData) -> serde_json::Value {
    let manager = match descriptor.manager_type {
        ManagerType::Pbs => "PBS",
        ManagerType::Slurm => "Slurm",
    };
    let info = &descriptor.info;

    json!({
        "id": id,
        "name": descriptor.name,
        "manager": manager,
        "additional_args": info.additional_args(),
        "backlog": info.backlog(),
        "workers_per_alloc": info.workers_per_alloc(),
        "timelimit": format_duration(info.timelimit()),
        "max_worker_count": info.max_worker_count(),
        "worker_cpu_args": info.worker_cpu_args(),
        "worker_resource_args": info.worker_resource_args(),
        "on_server_lost": crate::common::format::server_lost_policy_to_str(info.on_server_lost()),
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
        AllocationState::Queued => "queued",
        AllocationState::Running { .. } => "running",
        AllocationState::Finished { .. } | AllocationState::Invalid { .. } => {
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
    let WorkerInfo {
        id,
        configuration:
            WorkerConfiguration {
                resources,
                listen_address,
                hostname,
                work_dir,
                log_dir,
                heartbeat_interval,
                send_overview_interval: _,
                idle_timeout,
                time_limit,
                on_server_lost,
                extra: _,
            },
        ended,
    } = worker_info;

    json!({
        "id": id,
        "configuration": json!({
            "heartbeat_interval": format_duration(heartbeat_interval),
            "idle_timeout": idle_timeout.map(format_duration),
            "time_limit": time_limit.map(format_duration),
            "log_dir": log_dir,
            "work_dir": work_dir,
            "hostname": hostname,
            "listen_address": listen_address,
            "resources": format_resource_descriptor(&resources),
            "on_server_lost": crate::common::format::server_lost_policy_to_str(&on_server_lost),
        }),
        "ended": ended.map(|info| json!({
            "at": format_datetime(info.ended_at)
        }))
    })
}
fn format_resource_descriptor(descriptor: &ResourceDescriptor) -> serde_json::Value {
    let ResourceDescriptor { cpus, generic } = descriptor;
    json!({
        "cpus": cpus,
        "generic": generic.iter().map(format_generic_resource).collect::<Vec<_>>()
    })
}
fn format_generic_resource(resource: &GenericResourceDescriptor) -> serde_json::Value {
    json!({
        "name": resource.name,
        "kind": match &resource.kind {
            GenericResourceDescriptorKind::List { .. } => "list",
            GenericResourceDescriptorKind::Range { .. } => "range",
            GenericResourceDescriptorKind::Sum { .. } => "sum",
        },
        "params": match &resource.kind {
            GenericResourceDescriptorKind::List { values } => json!({
                "values": values,
            }),
            GenericResourceDescriptorKind::Range { start, end } => json!({
                "start": start,
                "end": end
            }),
            GenericResourceDescriptorKind::Sum { size } => json!({
                "size": size
            }),
        }
    })
}

fn format_duration(duration: Duration) -> serde_json::Value {
    let value = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;
    json!(value)
}
pub fn format_datetime<T: Into<DateTime<Utc>>>(time: T) -> serde_json::Value {
    json!(time.into())
}
