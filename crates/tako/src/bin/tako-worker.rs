use std::path::PathBuf;
use clap::Clap;
use tako::common::setup::setup_logging;
use std::fs;
use tako::worker::paths::WorkerPaths;
use rand::Rng;
use rand::distributions::Alphanumeric;
use tako::worker::rpc::run_worker;
use tokio::task::LocalSet;


#[derive(Clap)]
#[clap(version = "1.0")]
struct Opts {

    server_address: String,

    #[clap(long)]
    work_dir: Option<PathBuf>,

    #[clap(long)]
    local_directory: Option<PathBuf>,

    #[clap(long)]
    ncpus: Option<u32>,

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


#[tokio::main(basic_scheduler)]
async fn main() -> tako::Result<()> {
    let opts: Opts = Opts::parse();
    setup_logging();
    log::info!("tako worker v0.1 started");

    let (work_dir, log_dir) = create_paths(
        opts.work_dir.unwrap_or(PathBuf::from("rsds-worker-space")),
        opts.local_directory.unwrap_or(std::env::temp_dir()),
    )?;

    let ncpus = opts.ncpus.unwrap_or(1);
    if ncpus < 1 {
        panic!("Invalid number of cpus");
    }

    let local_set = LocalSet::new();
    local_set.run_until(run_worker(&opts.server_address, ncpus, work_dir, log_dir)).await?;
    log::info!("tako worker ends");
    Ok(())
}