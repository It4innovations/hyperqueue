use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{anyhow, Context};
use bstr::{BString, ByteSlice};
use clap::Clap;
use humantime::format_rfc3339;
use tako::messages::common::{LauncherDefinition, ProgramDefinition, WorkerConfiguration};
use tako::worker::launcher::pin_program;
use tako::worker::rpc::run_worker;
use tako::worker::task::Task;
use tempdir::TempDir;
use tokio::task::LocalSet;

use crate::client::globalsettings::GlobalSettings;
use crate::common::env::{HQ_CPUS, HQ_JOB_ID, HQ_PIN, HQ_SUBMIT_DIR, HQ_TASK_ID};
use crate::common::error::error;
use crate::common::serverdir::ServerDir;
use crate::common::timeutils::ArgDuration;
use crate::worker::hwdetect::detect_resource;
use crate::worker::output::print_worker_configuration;
use crate::worker::parser::parse_cpu_definition;
use crate::Map;
use hashbrown::HashMap;

#[derive(Clap)]
pub enum ManagerOpts {
    Detect,
    None,
    Pbs,
    Slurm,
}

impl FromStr for ManagerOpts {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "detect" => Self::Detect,
            "none" => Self::None,
            "pbs" => Self::Pbs,
            "slurm" => Self::Slurm,
            _ => {
                return error(
                    "Invalid manager value. Allowed values are 'detect', 'none', 'pbs', 'slurm'"
                        .to_string(),
                )
            }
        })
    }
}

#[derive(Clap)]
#[clap(setting = clap::AppSettings::ColoredHelp)]
pub struct WorkerStartOpts {
    /// How many cores should be allocated for the worker
    #[clap(long)]
    cpus: Option<String>,

    /// How often should the worker announce its existence to the server. (default: "8s")
    #[clap(long, default_value = "8s")]
    heartbeat: ArgDuration,

    #[clap(long)]
    idle_timeout: Option<ArgDuration>,

    /// What HPC job manager should be used by the worker.
    #[clap(long, default_value = "detect", possible_values = &["detect", "slurm", "pbs", "none"])]
    manager: ManagerOpts,
}

/// Replace placeholders in user-defined program attributes
fn replace_placeholders(program: &mut ProgramDefinition) {
    let date = format_rfc3339(std::time::SystemTime::now()).to_string();
    let submit_dir = PathBuf::from(
        program.env[&BString::from(HQ_SUBMIT_DIR)]
            .to_os_str()
            .unwrap_or_default(),
    );

    let mut placeholder_map = HashMap::new();
    placeholder_map.insert(
        "%{JOB_ID}",
        program.env[&BString::from(HQ_JOB_ID)].to_string(),
    );
    placeholder_map.insert(
        "%{TASK_ID}",
        program.env[&BString::from(HQ_TASK_ID)].to_string(),
    );
    placeholder_map.insert(
        "%{SUBMIT_DIR}",
        program.env[&BString::from(HQ_SUBMIT_DIR)].to_string(),
    );
    placeholder_map.insert("%{DATE}", date);

    let replace = |replacement_map: &HashMap<&str, String>, path: &PathBuf| -> PathBuf {
        let mut result: String = path.to_str().unwrap().into();
        for (placeholder, replacement) in replacement_map.iter() {
            result = result.replace(placeholder, replacement);
        }
        result.into()
    };

    // Replace CWD
    program.cwd = program
        .cwd
        .as_ref()
        .map(|cwd| submit_dir.join(replace(&placeholder_map, cwd)))
        .or_else(|| Some(std::env::current_dir().unwrap()));

    // Replace STDOUT and STDERR
    placeholder_map.insert(
        "%{CWD}",
        program.cwd.as_ref().unwrap().to_str().unwrap().to_string(),
    );

    program.stdout = program
        .stdout
        .as_ref()
        .map(|path| submit_dir.join(replace(&placeholder_map, path)));
    program.stderr = program
        .stderr
        .as_ref()
        .map(|path| submit_dir.join(replace(&placeholder_map, path)));
}

#[allow(clippy::unnecessary_wraps)]
fn launcher_setup(task: &Task, def: LauncherDefinition) -> tako::Result<ProgramDefinition> {
    let allocation = task
        .resource_allocation()
        .expect("Missing resource allocation for running task");
    let mut program = def.program;

    if def.pin {
        pin_program(&mut program, &allocation);
        program.env.insert(HQ_PIN.into(), "1".into());
    }

    program
        .env
        .insert(HQ_CPUS.into(), allocation.comma_delimited_cpu_ids().into());

    replace_placeholders(&mut program);

    Ok(program)
}

pub async fn start_hq_worker(
    gsettings: &GlobalSettings,
    opts: WorkerStartOpts,
) -> anyhow::Result<()> {
    log::info!("Starting hyperqueue worker {}", env!("CARGO_PKG_VERSION"));
    let server_dir =
        ServerDir::open(gsettings.server_directory()).context("Cannot load server directory")?;
    let record = server_dir.read_access_record().with_context(|| {
        format!(
            "Cannot load access record from {:?}",
            server_dir.access_filename()
        )
    })?;
    let server_address = format!("{}:{}", record.host(), record.worker_port());
    log::info!("Connecting to: {}", server_address);

    let configuration = gather_configuration(opts)?;
    let ((worker_id, configuration), worker_future) = run_worker(
        &server_address,
        configuration,
        Some(record.tako_secret_key().clone()),
        Box::new(launcher_setup),
    )
    .await?;
    print_worker_configuration(gsettings, worker_id, configuration);
    let local_set = LocalSet::new();
    local_set.run_until(worker_future).await;
    Ok(())
}

fn try_get_pbs_info() -> anyhow::Result<Map<String, String>> {
    log::debug!("Detecting PBS environment");

    std::env::var("PBS_ENVIRONMENT")
        .map_err(|_| anyhow!("PBS_ENVIRONMENT not found. The process is not running under PBS"))?;

    let manager_job_id = std::env::var("PBS_JOBID").unwrap_or_else(|_| "unknown".to_string());

    let mut result = Map::with_capacity(2);
    result.insert("MANAGER".to_string(), "PBS".to_string());
    result.insert("MANAGER_JOB_ID".to_string(), manager_job_id);

    // TODO: Run "qstat -f -F json $PBS_JOBID" to get walltime

    log::info!("PBS environment detected");
    Ok(result)
}

fn try_get_slurm_info() -> anyhow::Result<Map<String, String>> {
    log::debug!("Detecting SLURM environment");

    let manager_job_id = std::env::var("SLURM_JOB_ID")
        .or_else(|_| std::env::var("SLURM_JOBID"))
        .map_err(|_| {
            anyhow!("SLURM_JOB_ID/SLURM_JOBID not found. The process is not running under SLURM")
        })?;

    let mut result = Map::with_capacity(2);
    result.insert("MANAGER".to_string(), "SLURM".to_string());
    result.insert("MANAGER_JOB_ID".to_string(), manager_job_id);

    // TODO: Get walltime info

    log::info!("SLURM environment detected");
    Ok(result)
}

fn gather_manager_info(opts: ManagerOpts) -> anyhow::Result<Map<String, String>> {
    match opts {
        ManagerOpts::Detect => {
            log::debug!("Trying to detect manager");
            try_get_pbs_info()
                .or_else(|_| try_get_slurm_info())
                .or_else(|_| Ok(Map::new()))
        }
        ManagerOpts::None => {
            log::debug!("Manager detection disabled");
            Ok(Map::new())
        }
        ManagerOpts::Pbs => try_get_pbs_info(),
        ManagerOpts::Slurm => try_get_slurm_info(),
    }
}

fn gather_configuration(opts: WorkerStartOpts) -> anyhow::Result<WorkerConfiguration> {
    let hostname = gethostname::gethostname()
        .into_string()
        .expect("Invalid hostname");

    let resources = opts
        .cpus
        .map(|cpus| parse_cpu_definition(&cpus))
        .unwrap_or_else(detect_resource)?;

    let (work_dir, log_dir) = {
        let tmpdir = TempDir::new("hq-worker").unwrap().into_path();
        (tmpdir.join("work"), tmpdir.join("logs"))
    };

    let extra = gather_manager_info(opts.manager)?;

    Ok(WorkerConfiguration {
        resources,
        listen_address: Default::default(), // Will be filled during init
        hostname,
        work_dir,
        log_dir,
        heartbeat_interval: opts.heartbeat.into_duration(),
        idle_timeout: opts.idle_timeout.map(|x| x.into_duration()),
        extra,
    })
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;
    use tako::messages::common::ProgramDefinition;

    use crate::common::env::{HQ_JOB_ID, HQ_SUBMIT_DIR, HQ_TASK_ID};
    use crate::{JobId, JobTaskId};

    use super::replace_placeholders;

    #[test]
    fn test_replace_task_id() {
        let mut program = program_def(
            "dir-%{TASK_ID}",
            Some("%{TASK_ID}.out"),
            Some("%{TASK_ID}.err"),
            "",
            0,
            1,
        );
        replace_placeholders(&mut program);
        assert_eq!(program.cwd, Some("dir-1".into()));
        assert_eq!(program.stdout, Some("1.out".into()));
        assert_eq!(program.stderr, Some("1.err".into()));
    }

    #[test]
    fn test_replace_job_id() {
        let mut program = program_def(
            "dir-%{JOB_ID}-%{TASK_ID}",
            Some("%{JOB_ID}-%{TASK_ID}.out"),
            Some("%{JOB_ID}-%{TASK_ID}.err"),
            "",
            5,
            1,
        );
        replace_placeholders(&mut program);
        assert_eq!(program.cwd, Some("dir-5-1".into()));
        assert_eq!(program.stdout, Some("5-1.out".into()));
        assert_eq!(program.stderr, Some("5-1.err".into()));
    }

    #[test]
    fn test_replace_submit_dir() {
        let mut program = program_def(
            "%{SUBMIT_DIR}",
            Some("%{SUBMIT_DIR}/out"),
            Some("%{SUBMIT_DIR}/err"),
            "/submit-dir",
            5,
            1,
        );
        replace_placeholders(&mut program);
        assert_eq!(program.cwd, Some("/submit-dir".into()));
        assert_eq!(program.stdout, Some("/submit-dir/out".into()));
        assert_eq!(program.stderr, Some("/submit-dir/err".into()));
    }

    #[test]
    fn test_replace_cwd() {
        let mut program = program_def(
            "dir-%{JOB_ID}-%{TASK_ID}",
            Some("%{CWD}.out"),
            Some("%{CWD}.err"),
            "",
            5,
            1,
        );
        replace_placeholders(&mut program);
        assert_eq!(program.cwd, Some("dir-5-1".into()));
        assert_eq!(program.stdout, Some("dir-5-1.out".into()));
        assert_eq!(program.stderr, Some("dir-5-1.err".into()));
    }

    fn program_def(
        cwd: &str,
        stdout: Option<&str>,
        stderr: Option<&str>,
        submit_dir: &str,
        job_id: JobId,
        task_id: JobTaskId,
    ) -> ProgramDefinition {
        let mut env = HashMap::new();
        env.insert(HQ_SUBMIT_DIR.into(), submit_dir.into());
        env.insert(HQ_JOB_ID.into(), job_id.to_string().into());
        env.insert(HQ_TASK_ID.into(), task_id.to_string().into());

        ProgramDefinition {
            args: vec![],
            env,
            stdout: stdout.map(|v| v.into()),
            stderr: stderr.map(|v| v.into()),
            cwd: Some(cwd.into()),
        }
    }
}
