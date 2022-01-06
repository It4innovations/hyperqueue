use std::path::Path;
use std::time::Duration;

use anyhow::Error;
use chrono::{DateTime, Utc};
use serde_json;
use serde_json::json;

use tako::common::resources::{
    CpuRequest, GenericResourceDescriptor, GenericResourceDescriptorKind, ResourceDescriptor,
};
use tako::messages::common::{ProgramDefinition, WorkerConfiguration};
use tako::messages::gateway::ResourceRequest;

use crate::client::job::WorkerMap;
use crate::client::output::outputs::Output;
use crate::common::manager::info::ManagerType;
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{
    Allocation, AllocationEvent, AllocationEventHolder, AllocationStatus, DescriptorId,
};
use crate::server::job::JobTaskState;
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, QueueDescriptorData, StatsResponse,
    WaitForJobsResponse, WorkerInfo,
};
use crate::Map;

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

    fn print_job_list(&self, tasks: Vec<JobInfo>) {
        self.print(tasks.into_iter().map(format_job_info).collect());
    }
    fn print_job_detail(&self, job: JobDetail, show_tasks: bool, _worker_map: WorkerMap) {
        let JobDetail {
            info,
            job_type: _,
            program_def:
                ProgramDefinition {
                    args,
                    env,
                    stdout,
                    stderr,
                    cwd,
                },
            tasks,
            pin,
            max_fails,
            priority,
            time_limit,
            submission_date,
            completion_date_or_now,
        } = job;

        let finished_at = if info.counters.is_terminated(info.n_tasks) {
            Some(completion_date_or_now)
        } else {
            None
        };

        let mut json = json!({
            "info": format_job_info(info),
            "program": json!({
                "args": args.into_iter().map(|args| args.to_string()).collect::<Vec<_>>(),
                "env": env.into_iter().map(|(key, value)| (key.to_string(), value.to_string())).collect::<Map<String, String>>(),
                "cwd": cwd,
                "stderr": stderr,
                "stdout": stdout,
            }),
            "pin": pin,
            "max_fails": max_fails,
            "priority": priority,
            "time_limit": time_limit.map(format_duration),
            "started_at": format_datetime(submission_date),
            "finished_at": finished_at.map(format_datetime)
        });
        if show_tasks {
            json["tasks"] = tasks
                .into_iter()
                .map(|task| {
                    let state = &match task.state {
                        JobTaskState::Waiting => "waiting",
                        JobTaskState::Running { .. } => "running",
                        JobTaskState::Finished { .. } => "finished",
                        JobTaskState::Failed { .. } => "failed",
                        JobTaskState::Canceled => "canceled",
                    };
                    let mut data = json!({
                        "id": task.task_id,
                        "state": state,
                    });
                    match task.state {
                        JobTaskState::Running { start_date, worker } => {
                            data["worker"] = worker.as_num().into();
                            data["started_at"] = format_datetime(start_date)
                        }
                        JobTaskState::Finished {
                            start_date,
                            worker,
                            end_date,
                        } => {
                            data["worker"] = worker.as_num().into();
                            data["started_at"] = format_datetime(start_date);
                            data["finished_at"] = format_datetime(end_date);
                        }
                        JobTaskState::Failed {
                            start_date,
                            end_date,
                            worker,
                            error,
                        } => {
                            data["worker"] = worker.as_num().into();
                            data["started_at"] = format_datetime(start_date);
                            data["finished_at"] = format_datetime(end_date);
                            data["error"] = error.into();
                        }
                        _ => {}
                    };
                    data
                })
                .collect();
        }
        self.print(json);
    }

    fn print_job_wait(&self, duration: Duration, response: &WaitForJobsResponse) {
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

    fn print_summary(&self, filename: &Path, summary: Summary) {
        let json = json!({
            "filename": filename,
            "summary": summary,
        });
        self.print(json);
    }

    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        let mut descriptors: Vec<_> = info.descriptors.into_iter().collect();
        descriptors.sort_by_key(|descriptor| descriptor.0);

        self.print(
            descriptors
                .iter()
                .map(|(key, descriptor)| format_queue_descriptor(*key, descriptor))
                .collect(),
        );
    }

    fn print_event_log(&self, events: Vec<AllocationEventHolder>) {
        self.print(events.into_iter().map(format_allocation_event).collect());
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

fn format_job_info(info: JobInfo) -> serde_json::Value {
    let JobInfo {
        id,
        name,
        n_tasks,
        counters,
        resources:
            ResourceRequest {
                cpus,
                generic,
                min_time,
            },
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
        }),
        "resources": json!({
            "cpus": format_cpu_request(cpus),
            "generic": generic,
            "min_time": format_duration(min_time)
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

fn format_queue_descriptor(
    id: DescriptorId,
    descriptor: &QueueDescriptorData,
) -> serde_json::Value {
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
    })
}
fn format_allocation(allocation: Allocation) -> serde_json::Value {
    let Allocation {
        id,
        worker_count,
        queued_at,
        status,
        working_dir,
    } = allocation;

    let status_name = match &status {
        AllocationStatus::Queued => "queued",
        AllocationStatus::Running { .. } => "running",
        AllocationStatus::Finished { .. } => "finished",
        AllocationStatus::Failed { .. } => "failed",
    };
    let started_at = match status {
        AllocationStatus::Running { started_at }
        | AllocationStatus::Finished { started_at, .. }
        | AllocationStatus::Failed { started_at, .. } => Some(started_at),
        _ => None,
    };
    let ended_at = match status {
        AllocationStatus::Finished { finished_at, .. }
        | AllocationStatus::Failed { finished_at, .. } => Some(finished_at),
        _ => None,
    };

    json!({
        "id": id,
        "worker_count": worker_count,
        "queued_at": format_datetime(queued_at),
        "started_at": started_at.map(format_datetime),
        "ended_at": ended_at.map(format_datetime),
        "status": status_name,
        "workdir": working_dir
    })
}
fn format_allocation_event(event: AllocationEventHolder) -> serde_json::Value {
    let name = match &event.event {
        AllocationEvent::AllocationQueued(_) => "allocation-queued",
        AllocationEvent::AllocationStarted(_) => "allocation-started",
        AllocationEvent::AllocationFinished(_) => "allocation-finished",
        AllocationEvent::AllocationFailed(_) => "allocation-failed",
        AllocationEvent::AllocationDisappeared(_) => "allocation-disappeared",
        AllocationEvent::QueueFail { .. } => "queue-fail",
        AllocationEvent::StatusFail { .. } => "status-fail",
    };
    let params = match event.event {
        AllocationEvent::AllocationQueued(id)
        | AllocationEvent::AllocationStarted(id)
        | AllocationEvent::AllocationFinished(id)
        | AllocationEvent::AllocationFailed(id)
        | AllocationEvent::AllocationDisappeared(id) => {
            json!({ "id": id })
        }
        AllocationEvent::QueueFail { error } | AllocationEvent::StatusFail { error } => {
            json!({ "error": error })
        }
    };

    json!({
        "date": format_datetime(event.date),
        "event": name,
        "params": params
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
            "resources": format_resource_descriptor(&resources)
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
            GenericResourceDescriptorKind::Indices(_) => "indices",
            GenericResourceDescriptorKind::Sum(_) => "sum",
        },
        "params": match &resource.kind {
            GenericResourceDescriptorKind::Indices(params) => json!({
                "start": params.start,
                "end": params.end
            }),
            GenericResourceDescriptorKind::Sum(params) => json!({
                "size": params.size
            }),
        }
    })
}

fn format_duration(duration: Duration) -> serde_json::Value {
    let value = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;
    json!(value)
}
fn format_datetime<T: Into<DateTime<Utc>>>(time: T) -> serde_json::Value {
    json!(time.into())
}
