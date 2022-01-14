use crate::common::serverdir::AccessRecord;
use crate::transfer::messages::{
    AutoAllocListResponse, JobDetail, JobInfo, Selector, StatsResponse, WaitForJobsResponse,
    WorkerInfo,
};

use crate::client::job::WorkerMap;
use crate::server::autoalloc::{Allocation, AllocationEventHolder};
use crate::stream::reader::logfile::Summary;
use std::path::Path;
use std::str::FromStr;

use clap::Parser;
use core::time::Duration;
use tako::common::resources::ResourceDescriptor;

pub const MAX_DISPLAYED_WORKERS: usize = 2;

#[derive(Parser)]
pub enum Outputs {
    CLI,
    JSON,
    Quiet,
}

impl FromStr for Outputs {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "cli" => Ok(Outputs::CLI),
            "json" => Ok(Outputs::JSON),
            "quiet" => Ok(Outputs::Quiet),
            _ => {
                anyhow::bail!("Invalid output mode. Possible values are `cli`, `json` or `quiet`.")
            }
        }
    }
}

pub enum OutputStream {
    /// Displays stdout output stream for given job and task(s)
    Stdout,
    /// Displays stderr output stream for given job and task(s)
    Stderr,
}

impl FromStr for OutputStream {
    type Err = anyhow::Error;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "stdout" => Ok(OutputStream::Stdout),
            "stderr" => Ok(OutputStream::Stderr),
            _ => anyhow::bail!("Invalid output stream. Possible values are `stdout` or `stderr`."),
        }
    }
}

pub trait Output {
    // Workers
    fn print_worker_list(&self, workers: Vec<WorkerInfo>);
    fn print_worker_info(&self, worker_info: WorkerInfo);

    // Server
    fn print_server_record(&self, server_dir: &Path, record: &AccessRecord);
    fn print_server_stats(&self, stats: StatsResponse);

    // Jobs
    fn print_job_submitted(&self, job: JobDetail);
    fn print_job_list(&self, tasks: Vec<JobInfo>);
    fn print_job_detail(&self, job: JobDetail, worker_map: WorkerMap);
    fn print_job_tasks(&self, job: JobDetail, worker_map: WorkerMap);
    fn print_job_wait(&self, duration: Duration, response: &WaitForJobsResponse);
    fn print_job_output(
        &self,
        job: JobDetail,
        task_selector: Option<Selector>,
        output_stream: OutputStream,
    ) -> anyhow::Result<()>;

    // Log
    fn print_summary(&self, filename: &Path, summary: Summary);

    // Autoalloc
    fn print_autoalloc_queues(&self, info: AutoAllocListResponse);
    fn print_event_log(&self, events: Vec<AllocationEventHolder>);
    fn print_allocations(&self, allocations: Vec<Allocation>);

    // Hw
    fn print_hw(&self, descriptor: &ResourceDescriptor);

    fn print_error(&self, error: anyhow::Error);
}
