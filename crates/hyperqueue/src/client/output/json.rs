use crate::client::job::WorkerMap;
use crate::client::output::cli::{format_job_workers, format_task_duration};
use crate::client::output::outputs::Output;
use crate::client::status::{task_status, Status};
use crate::common::manager::info::ManagerType;
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{
    Allocation, AllocationEvent, AllocationEventHolder, AllocationStatus,
};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, QueueDescriptorData, StatsResponse,
    WaitForJobsResponse, WorkerInfo,
};
use crate::JobTaskId;
use anyhow::Error;
use chrono::{DateTime, Utc};
use serde_json;
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use tako::common::resources::{
    CpuRequest, GenericResourceDescriptor, GenericResourceDescriptorKind, ResourceDescriptor,
};
use tako::messages::common::WorkerConfiguration;
use tako::messages::gateway::ResourceRequest;

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
    fn print_job_detail(&self, job: JobDetail, show_tasks: bool, worker_map: WorkerMap) {
        let worker = format_job_workers(&job, &worker_map);
        let json = json!({
            "job_detail": job,
            "worker": worker,
        });
        if !job.tasks.is_empty() {
            self.print_job_tasks(
                job.completion_date_or_now,
                job.tasks,
                show_tasks,
                &job.info.counters,
                &worker_map,
            );
        }
        self.print(json);
    }

    fn print_job_tasks(
        &self,
        completion_date_or_now: chrono::DateTime<chrono::Utc>,
        mut tasks: Vec<JobTaskInfo>,
        _show_tasks: bool,
        counters: &JobTaskCounters,
        worker_map: &WorkerMap,
    ) {
        tasks.sort_unstable_by_key(|t| t.task_id);

        let output_tasks_duration: Vec<String> = tasks
            .iter()
            .map(|t| format_task_duration(&completion_date_or_now, &t.state))
            .collect();
        let tasks_id: Vec<JobTaskId> = tasks.iter().map(|t| t.task_id).collect();
        let tasks_state: Vec<Status> = tasks.iter().map(|t| task_status(&t.state)).collect();
        let json = json!({
            "tasks_state": tasks_state,
            "tasks_duration": output_tasks_duration,
            "tasks_id": tasks_id,
            "counters": counters,
            "worker": worker_map,
        });
        self.print(json);
    }
    fn print_job_wait(&self, _duration: Duration, _response: &WaitForJobsResponse) {}

    fn print_summary(&self, filename: &Path, summary: Summary) {
        let json = json!({
            "filename": filename,
            "summary": summary,
        });
        self.print(json);
    }

    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        self.print(
            info.descriptors
                .iter()
                .map(|(key, descriptor)| (key.to_string(), format_queue_descriptor(descriptor)))
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

fn format_queue_descriptor(descriptor: &QueueDescriptorData) -> serde_json::Value {
    let manager = match descriptor.manager_type {
        ManagerType::Pbs => "PBS",
        ManagerType::Slurm => "Slurm",
    };
    let info = &descriptor.info;

    json!({
        "manager": manager,
        "additional_args": info.additional_args(),
        "backlog": info.backlog(),
        "workers_per_alloc": info.workers_per_alloc(),
        "timelimit": format_duration(info.timelimit()),
        "max_worker_count": info.max_worker_count(),
        "worker_cpu_args": info.worker_cpu_args(),
        "worker_resource_args": info.worker_resource_args(),
        "name": descriptor.name
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
        AllocationStatus::Queued => "queue",
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
                hw_state_poll_interval: _,
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
