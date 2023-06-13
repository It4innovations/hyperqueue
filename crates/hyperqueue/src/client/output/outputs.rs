use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, ServerInfo, StatsResponse, WaitForJobsResponse,
    WorkerInfo,
};

use crate::client::job::WorkerMap;
use crate::server::autoalloc::Allocation;
use crate::stream::reader::logfile::Summary;
use std::path::Path;

use crate::client::output::common::TaskToPathsMap;
use crate::client::output::Verbosity;
use crate::server::job::JobTaskInfo;
use crate::JobId;
use core::time::Duration;
use tako::resources::ResourceDescriptor;

pub const MAX_DISPLAYED_WORKERS: usize = 2;

#[derive(clap::ValueEnum, Clone)]
pub enum Outputs {
    CLI,
    JSON,
    Quiet,
}

#[derive(clap::ValueEnum, Clone)]
pub enum OutputStream {
    /// Displays stdout output stream for given job and task(s)
    Stdout,
    /// Displays stderr output stream for given job and task(s)
    Stderr,
}

pub trait Output {
    // Workers
    fn print_worker_list(&self, workers: Vec<WorkerInfo>);
    fn print_worker_info(&self, worker_info: WorkerInfo);

    // Server
    fn print_server_description(&self, server_dir: Option<&Path>, record: &ServerInfo);
    fn print_server_stats(&self, stats: StatsResponse);

    // Jobs
    fn print_job_submitted(&self, job: JobDetail);
    fn print_job_list(&self, jobs: Vec<JobInfo>, total_jobs: usize);
    fn print_job_summary(&self, jobs: Vec<JobInfo>);
    fn print_job_detail(&self, jobs: Vec<JobDetail>, worker_map: WorkerMap, server_uid: &str);
    fn print_job_wait(
        &self,
        duration: Duration,
        response: &WaitForJobsResponse,
        details: &[(JobId, Option<JobDetail>)],
        worker_map: WorkerMap,
    );
    fn print_job_output(
        &self,
        tasks: Vec<JobTaskInfo>,
        output_stream: OutputStream,
        task_header: bool,
        task_paths: TaskToPathsMap,
    ) -> anyhow::Result<()>;

    // Tasks
    fn print_task_list(
        &self,
        jobs: Vec<(JobId, JobDetail)>,
        worker_map: WorkerMap,
        server_uid: &str,
        verbosity: Verbosity,
    );
    fn print_task_info(
        &self,
        job: (JobId, JobDetail),
        tasks: Vec<JobTaskInfo>,
        worker_map: WorkerMap,
        server_uid: &str,
        verbosity: Verbosity,
    );

    // Log
    fn print_summary(&self, filename: &Path, summary: Summary);

    // Autoalloc
    fn print_autoalloc_queues(&self, info: AutoAllocListResponse);
    fn print_allocations(&self, allocations: Vec<Allocation>);

    // Hw
    fn print_hw(&self, descriptor: &ResourceDescriptor);

    fn print_error(&self, error: anyhow::Error);
}
