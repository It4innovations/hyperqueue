use crate::client::job::WorkerMap;
use crate::client::output::cli::{format_job_workers, format_task_duration};
use crate::client::output::outputs::Output;
use crate::client::status::{task_status, Status};
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{Allocation, AllocationEventHolder};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, StatsResponse, WaitForJobsResponse, WorkerInfo,
};
use crate::{JobTaskId, WorkerId};
use serde_json;
use std::path::Path;
use std::time::Duration;
use tako::common::resources::ResourceDescriptor;
use tako::messages::common::WorkerConfiguration;

#[derive(Default)]
pub struct JsonOutput;

impl JsonOutput {
    fn print(&self, data: serde_json::Value) {
        println!("{}", data.to_string());
    }
}

// TODO: output machine-readable data
impl Output for JsonOutput {
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        self.print(serde_json::json!(workers));
    }
    fn print_worker_info(&self, worker_id: WorkerId, configuration: WorkerConfiguration) {
        let json = serde_json::json!({
            "id": worker_id,
            "worker_configuration": configuration
        });
        self.print(json);
    }

    // Server
    fn print_server_record(&self, server_dir: &Path, record: &AccessRecord) {
        let json = serde_json::json!({
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
        self.print(serde_json::json!(stats));
    }

    fn print_job_submitted(&self, job: JobDetail) {
        self.print(serde_json::json!({
            "id": job.info.id
        }))
    }

    // Jobs
    fn print_job_list(&self, tasks: Vec<JobInfo>) {
        self.print(serde_json::json!(tasks));
    }
    fn print_job_detail(&self, job: JobDetail, show_tasks: bool, worker_map: WorkerMap) {
        let worker = format_job_workers(&job, &worker_map);
        let json = serde_json::json!({
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
        let json = serde_json::json!({
            "tasks_state": tasks_state,
            "tasks_duration": output_tasks_duration,
            "tasks_id": tasks_id,
            "counters": counters,
            "worker": worker_map,
        });
        self.print(json);
    }
    fn print_job_wait(&self, _duration: Duration, _response: &WaitForJobsResponse) {}

    // Log
    fn print_summary(&self, filename: &Path, summary: Summary) {
        let json = serde_json::json!({
            "filename":filename,
            "summary":summary,
        });
        self.print(json);
    }

    // Autoalloc
    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        self.print(serde_json::json!(info));
    }

    fn print_event_log(&self, events: Vec<AllocationEventHolder>) {
        self.print(serde_json::json!(events));
    }
    fn print_allocations(&self, allocations: Vec<Allocation>) {
        self.print(serde_json::json!(allocations));
    }

    // Hw
    fn print_hw(&self, descriptor: &ResourceDescriptor) {
        self.print(serde_json::json!(descriptor));
    }
}
