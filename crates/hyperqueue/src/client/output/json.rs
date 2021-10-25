use crate::client::job::WorkerMap;
use crate::client::output::outputs::Output;
use crate::common::serverdir::AccessRecord;
use crate::server::autoalloc::{Allocation, AllocationEventHolder};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::stream::reader::logfile::Summary;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, StatsResponse, WaitForJobsResponse, WorkerInfo,
};
use crate::WorkerId;
use serde_json;
use std::path::Path;
use std::time::Duration;
use tako::common::resources::ResourceDescriptor;
use tako::messages::common::WorkerConfiguration;

pub const DEFAULT_JSON_FILE: &str = "output.json";

pub struct JSONOutput {}

impl JSONOutput {
    pub fn new() -> JSONOutput {
        JSONOutput {}
    }
    pub fn save(&self, data: String) {
        println!("{}", data);
    }
}

impl Output for JSONOutput {
    fn print_worker_list(&self, workers: Vec<WorkerInfo>) {
        let data: Vec<_> = workers.into_iter().collect();

        let json = serde_json::json!(data).to_string();
        self.save(json);
    }
    fn print_worker_info(&self, worker_id: WorkerId, configuration: WorkerConfiguration) {
        let json = serde_json::json!({
            "id":worker_id,
            "worker_configuration":configuration});
        self.save(json.to_string());
    }

    // Server
    fn print_server_record(&self, server_dir: &Path, record: &AccessRecord) {
        let json = serde_json::json!({
            "server_dir":server_dir,
            "record":record,

        });
        self.save(json.to_string());
    }
    fn print_server_stats(&self, stats: StatsResponse) {
        let json = serde_json::json!(stats);
        self.save(json.to_string());
    }

    // Jobs
    fn print_job_list(&self, tasks: Vec<JobInfo>) {
        let json = serde_json::json!(tasks);
        self.save(json.to_string());
    }
    fn print_job_detail(
        &self,
        job: JobDetail,
        _just_submitted: bool,
        _show_tasks: bool,
        worker_map: WorkerMap,
    ) {
        let worker = if worker_map.len() > 0 {
            worker_map[&job.info.id].clone()
        } else {
            String::new()
        };
        let json = serde_json::json!({
            "job_detail":job,
            "worker":worker,

        });
        self.save(json.to_string());
    }

    fn print_job_tasks(
        &self,
        completion_date_or_now: chrono::DateTime<chrono::Utc>,
        tasks: Vec<JobTaskInfo>,
        _show_tasks: bool,
        counters: &JobTaskCounters,
        worker_map: &WorkerMap,
    ) {
        let json = serde_json::json!({
         "completion_date_or_now":completion_date_or_now,
            "tasks":tasks,
            "counters":counters,
            "worker_map":worker_map,

        });
        self.save(json.to_string());
    }
    fn print_job_wait(&self, _duration: Duration, _response: &WaitForJobsResponse) {}

    // Log
    fn print_summary(&self, filename: &Path, summary: Summary) {
        let json = serde_json::json!({
            "filename":filename,
            "summary":summary,
        });
        self.save(json.to_string());
    }

    // Autoalloc
    fn print_autoalloc_queues(&self, info: AutoAllocListResponse) {
        let json = serde_json::json!(info);
        self.save(json.to_string());
    }

    fn print_event_log(&self, _events: Vec<AllocationEventHolder>) {}
    fn print_allocations(&self, allocations: Vec<Allocation>) {
        let json = serde_json::json!(allocations);
        self.save(json.to_string());
    }

    // Hw
    fn print_hw(&self, descriptor: &ResourceDescriptor) {
        let json = serde_json::json!(descriptor);
        self.save(json.to_string());
    }
}
