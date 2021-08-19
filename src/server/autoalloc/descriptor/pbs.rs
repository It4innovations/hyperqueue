use std::time::Duration;

use async_trait::async_trait;

use crate::common::manager::pbs::format_pbs_duration;
use crate::server::autoalloc::descriptor::QueueDescriptor;
use crate::server::autoalloc::state::{AllocationId, AllocationStatus};
use crate::server::autoalloc::{AutoAllocError, AutoAllocResult};
use bstr::ByteSlice;
use std::path::PathBuf;
use tokio::process::Command;

pub struct PbsDescriptor {
    max_workers_per_alloc: u32,
    target_worker_count: u32,
    queue: String,
    walltime: Option<Duration>,
    server_directory: PathBuf,
    name: String,
    hq_path: PathBuf,
}

impl PbsDescriptor {
    pub fn new(
        max_workers_per_alloc: u32,
        target_worker_count: u32,
        queue: String,
        walltime: Option<Duration>,
        name: String,
        server_directory: PathBuf,
    ) -> Self {
        let hq_path = std::env::current_exe().unwrap();
        Self {
            max_workers_per_alloc,
            target_worker_count,
            queue,
            walltime,
            server_directory,
            name,
            hq_path,
        }
    }

    fn create_allocation_dir(&self) -> Result<PathBuf, std::io::Error> {
        let mut dir = self.server_directory.clone();
        dir.push("autoalloc");
        dir.push(&self.name);

        std::fs::create_dir_all(&dir)?;

        Ok(tempdir::TempDir::new_in(dir, "allocation")?.into_path())
    }
}

#[async_trait(?Send)]
impl QueueDescriptor for PbsDescriptor {
    fn target_scale(&self) -> u32 {
        self.target_worker_count
    }

    fn max_workers_per_alloc(&self) -> u32 {
        self.max_workers_per_alloc
    }

    async fn schedule_allocation(&self, worker_count: u64) -> AutoAllocResult<AllocationId> {
        let directory = self.create_allocation_dir()?;

        let mut command = Command::new("qsub");
        command
            .arg("-q")
            .arg(self.queue.as_str())
            .arg("-wd")
            .arg(directory.display().to_string())
            .arg("-o")
            .arg(directory.join("stdout").display().to_string())
            .arg("-e")
            .arg(directory.join("stderr").display().to_string())
            .arg(format!("-lselect={}", worker_count));

        if let Some(ref walltime) = self.walltime {
            command.arg(format!("-lwalltime={}", format_pbs_duration(walltime)));
        }

        // `hq worker` arguments
        command.arg(self.hq_path.display().to_string());
        command.args([
            "worker",
            "start",
            "--idle-timeout",
            "10m",
            "--manager",
            "pbs",
            "--server-dir",
            &self.server_directory.display().to_string(),
        ]);

        log::debug!("Running PBS command {:?}", command);

        let output = command.output().await?;
        let status = output.status;
        if !status.success() {
            return AutoAllocResult::Err(AutoAllocError::SubmitFailed(format!(
                "Exit code {}, stderr: {}, stdout: {}",
                status.code().unwrap(),
                output.stderr.to_str().unwrap(),
                output.stdout.to_str().unwrap()
            )));
        }

        let job_id = output
            .stdout
            .to_str()
            .map_err(|e| AutoAllocError::Custom(format!("Invalid UTF-8 qsub output: {:?}", e)))?
            .trim();

        AutoAllocResult::Ok(job_id.to_string())
    }

    async fn get_allocation_status(
        &self,
        _allocation_id: &str,
    ) -> AutoAllocResult<Option<AllocationStatus>> {
        todo!()
    }
}
