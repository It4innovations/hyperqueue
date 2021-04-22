use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use clap::Clap;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::task::LocalSet;

use tako::common::secret::read_secret_file;
use tako::common::setup::setup_logging;
use tako::messages::common::WorkerConfiguration;
use tako::worker::rpc::run_worker;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clap)]
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
    ncpus: Option<u32>,

    #[clap(long)]
    secret_file: Option<PathBuf>,
}

fn create_local_directory(prefix: PathBuf) -> Result<PathBuf, std::io::Error> {
    let mut work_dir = prefix;
    let rnd_string: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(5)
        .collect();
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> tako::Result<()> {
    let opts: Opts = Opts::parse();
    setup_logging();
    log::info!("tako worker v0.1 started");

    let (work_dir, log_dir) = create_paths(
        opts.work_dir.unwrap_or(PathBuf::from("rsds-worker-space")),
        opts.local_directory.unwrap_or(std::env::temp_dir()),
    )?;

    let n_cpus = opts.ncpus.unwrap_or(1);
    if n_cpus < 1 {
        panic!("Invalid number of cpus");
    }

    let heartbeat_interval = Duration::from_millis(opts.heartbeat as u64);

    let configuration = WorkerConfiguration {
        n_cpus,
        listen_address: Default::default(), // Will be set later
        hostname: hostname::get()
            .expect("Cannot get hostname")
            .into_string()
            .expect("Invalid hostname"),
        work_dir,
        log_dir,
        heartbeat_interval,
        extra: vec![],
    };

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
    local_set
        .run_until(run_worker(&opts.server_address, configuration, secret_key))
        .await?;
    log::info!("tako worker ends");
    Ok(())
}
