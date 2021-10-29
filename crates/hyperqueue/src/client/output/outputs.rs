use crate::common::serverdir::AccessRecord;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, StatsResponse, WaitForJobsResponse, WorkerInfo,
};

use crate::client::job::WorkerMap;
use crate::server::autoalloc::{Allocation, AllocationEventHolder};
use crate::server::job::{JobTaskCounters, JobTaskInfo};
use crate::stream::reader::logfile::Summary;
use crate::WorkerId;

use std::path::Path;
use std::str::FromStr;

use core::time::Duration;
use tako::common::resources::ResourceDescriptor;
use tako::messages::common::WorkerConfiguration;

pub const MAX_DISPLAYED_WORKERS: usize = 2;

pub enum Outputs {
    CLI,
}

impl FromStr for Outputs {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cli" => Ok(Outputs::CLI),
            _ => anyhow::bail!("Invalid output"),
        }
    }
}

pub trait Output {
    // Workers
    fn print_worker_list(&self, workers: Vec<WorkerInfo>);
    fn print_worker_info(&self, worker_id: WorkerId, configuration: WorkerConfiguration);

    // Server
    fn print_server_record(&self, server_dir: &Path, record: &AccessRecord);
    fn print_server_stats(&self, stats: StatsResponse);

    // Jobs
    fn print_job_list(&self, tasks: Vec<JobInfo>);
    fn print_job_detail(
        &self,
        job: JobDetail,
        just_submitted: bool,
        show_tasks: bool,
        worker_map: WorkerMap,
        resource_names: &[String],
    );
    fn print_job_tasks(
        &self,
        completion_date_or_now: chrono::DateTime<chrono::Utc>,
        tasks: Vec<JobTaskInfo>,
        show_tasks: bool,
        counters: &JobTaskCounters,
        worker_map: &WorkerMap,
    );
    fn print_job_wait(&self, duration: Duration, response: &WaitForJobsResponse);

    // Log
    fn print_summary(&self, filename: &Path, summary: Summary);

    // Autoalloc
    fn print_autoalloc_queues(&self, info: AutoAllocListResponse);
    fn print_event_log(&self, events: Vec<AllocationEventHolder>);
    fn print_allocations(&self, allocations: Vec<Allocation>);

    // Hw
    fn print_hw(&self, descriptor: &ResourceDescriptor);
}
