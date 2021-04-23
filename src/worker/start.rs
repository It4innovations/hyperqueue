use std::path::PathBuf;
use crate::server::bootstrap::read_access_record_from_rundir;

pub async fn start_hq_worker(rundir_path: PathBuf) -> crate::Result<()> {
    let record = read_access_record_from_rundir(rundir_path.clone()).map_err(|e| format!("No running instance of HQ found: {}", e))?;
    Ok(())
}
