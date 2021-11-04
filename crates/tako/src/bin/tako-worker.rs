use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::task::LocalSet;

use orion::kdf::SecretKey;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tako::common::error::DsError;
use tako::common::resources::descriptor::{GenericResourceKindIndices, GenericResourceKindSum};
use tako::common::resources::{
    GenericResourceAmount, GenericResourceDescriptor, GenericResourceDescriptorKind,
    ResourceDescriptor,
};
use tako::common::secret::read_secret_file;
use tako::common::setup::setup_logging;
use tako::messages::common::{ProgramDefinition, WorkerConfiguration};
use tako::worker::launcher::command_from_definitions;
use tako::worker::rpc::run_worker;
use tako::worker::state::WorkerState;
use tako::worker::task::TaskRef;
use tako::worker::taskenv::{StopReason, TaskResult};
use tokio::net::lookup_host;

#[derive(Parser)]
#[clap(version = "1.0")]
struct Opts {
    server_address: String,

    #[clap(long)]
    work_dir: Option<PathBuf>,

    #[clap(long)]
    local_directory: Option<PathBuf>,

    #[clap(long, default_value = "4000")]
    heartbeat: u32,

    #[clap(long)]
    hw_state_poll_interval: Option<u32>, // Miliseconds

    #[clap(long)]
    time_limit: Option<u32>, // Seconds

    #[clap(long)]
    cpus: Option<u32>,

    #[clap(long)]
    sockets: Option<u32>,

    #[clap(long)]
    resources_i: Option<String>,

    #[clap(long)]
    resources_s: Option<String>,

    #[clap(long)]
    secret_file: Option<PathBuf>,
}

fn parse_resource_def(input: &str) -> tako::Result<Vec<(String, GenericResourceAmount)>> {
    let mut result = Vec::new();
    for s in input.split(':') {
        let (key, value) = s
            .split_once('=')
            .ok_or_else(|| DsError::from("Invalid resource definition ('=' not found)"))?;
        let value: GenericResourceAmount = value
            .parse()
            .map_err(|e| DsError::from(format!("Invalid resource definition: {}", e)))?;
        result.push((key.to_string(), value))
    }
    Ok(result)
}

fn create_local_directory(prefix: PathBuf) -> Result<PathBuf, std::io::Error> {
    let mut work_dir = prefix;
    let rnd_string: String = String::from_utf8(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(5)
            .collect::<Vec<u8>>(),
    )
    .unwrap();
    work_dir.push(format!("rsds-{}", rnd_string));
    fs::create_dir_all(&work_dir)?;
    Ok(work_dir)
}

fn create_paths(
    workdir: PathBuf,
    local_directory: PathBuf,
) -> Result<(PathBuf, PathBuf), std::io::Error> {
    fs::create_dir_all(&workdir)?;
    let work_dir = fs::canonicalize(workdir)?;
    let local_dir = create_local_directory(local_directory)?;
    Ok((work_dir, local_dir))
}

async fn launcher_main(task_ref: TaskRef) -> tako::Result<()> {
    log::debug!(
        "Starting program launcher {} {:?} {:?}",
        task_ref.get().id,
        &task_ref.get().configuration.resources,
        task_ref.get().resource_allocation()
    );

    let program: ProgramDefinition = {
        let task = task_ref.get();
        rmp_serde::from_slice(&task.configuration.body)?
    };

    let mut command = command_from_definitions(&program)?;
    let status = command.status().await?;
    if !status.success() {
        let code = status.code().unwrap_or(-1);
        return tako::Result::Err(DsError::GenericError(format!(
            "Program terminated with exit code {}",
            code
        )));
    }
    Ok(())
}

fn launcher(
    _state: &WorkerState,
    task_ref: &TaskRef,
    end_receiver: tokio::sync::oneshot::Receiver<StopReason>,
) -> Pin<Box<dyn Future<Output = tako::Result<TaskResult>> + 'static>> {
    /*let mut program = def.program;
    if def.pin {
        pin_program(&mut program, task.resource_allocation().unwrap());
    }
    Ok(program)*/
    let task_ref = task_ref.clone();

    Box::pin(async move {
        tokio::select! {
            biased;
                r = end_receiver => {
                    Ok(r.unwrap().into())
                }
                r = launcher_main(task_ref) => {
                    r?;
                    Ok(TaskResult::Finished)
                }
        }
    })
}

async fn worker_main(
    server_address: SocketAddr,
    configuration: WorkerConfiguration,
    secret_key: Option<Arc<SecretKey>>,
) -> tako::Result<()> {
    let (_, worker_future) = run_worker(
        server_address,
        configuration,
        secret_key,
        Box::new(launcher),
    )
    .await?;
    worker_future.await;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> tako::Result<()> {
    let opts: Opts = Opts::parse();
    setup_logging();
    log::info!("tako worker v0.1 started");

    let (work_dir, log_dir) = create_paths(
        opts.work_dir
            .unwrap_or_else(|| PathBuf::from("rsds-worker-space")),
        opts.local_directory.unwrap_or_else(std::env::temp_dir),
    )?;

    let heartbeat_interval = Duration::from_millis(opts.heartbeat as u64);
    let hw_state_poll_interval = opts
        .hw_state_poll_interval
        .map(|interval| Duration::from_millis(interval as u64));
    let time_limit = opts
        .time_limit
        .map(|interval| Duration::from_secs(interval as u64));

    let mut resources = match (opts.cpus, opts.sockets) {
        (Some(c), s) => ResourceDescriptor::new_with_socket_size(s.unwrap_or(1), c),
        (None, Some(_)) => todo!(),
        (None, None) => todo!(),
    };

    if let Some(def) = &opts.resources_i {
        for (name, value) in parse_resource_def(def)? {
            resources.add_generic_resource(GenericResourceDescriptor {
                name,
                kind: GenericResourceDescriptorKind::Indices(GenericResourceKindIndices {
                    start: 0,
                    end: value as u32 - 1,
                }),
            })
        }
    }

    if let Some(def) = &opts.resources_s {
        for (name, value) in parse_resource_def(def)? {
            resources.add_generic_resource(GenericResourceDescriptor {
                name,
                kind: GenericResourceDescriptorKind::Sum(GenericResourceKindSum { size: value }),
            })
        }
    }

    let configuration = WorkerConfiguration {
        resources,
        listen_address: Default::default(), // Will be set later
        hostname: gethostname::gethostname()
            .into_string()
            .expect("Invalid hostname"),
        work_dir,
        log_dir,
        heartbeat_interval,
        hw_state_poll_interval,
        time_limit,
        idle_timeout: None,
        extra: Default::default(),
    };

    log::debug!("Worker configuration: {:?}", &configuration);

    let secret_key = opts.secret_file.map(|key_file| {
        Arc::new(read_secret_file(&key_file).unwrap_or_else(|e| {
            log::error!("Reading secret file {}: {:?}", key_file.display(), e);
            std::process::exit(1);
        }))
    });

    if secret_key.is_none() {
        log::info!("Authentication is switched off");
    };

    let local_set = LocalSet::new();
    let server_address = opts.server_address;
    local_set
        .run_until(async move {
            match lookup_host(&server_address).await {
                Ok(mut addrs) => {
                    let address = addrs.next().expect("Invalid server address");
                    worker_main(address, configuration, secret_key).await
                }
                Err(e) => Result::Err(e.into()),
            }
        })
        .await?;
    log::info!("tako worker ends");
    Ok(())
}
