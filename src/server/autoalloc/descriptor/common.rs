use bstr::ByteSlice;
use std::path::PathBuf;
use std::process::Output;

use crate::server::autoalloc::AutoAllocResult;

pub fn create_allocation_dir(
    server_directory: PathBuf,
    name: &str,
) -> Result<PathBuf, std::io::Error> {
    let mut dir = server_directory;
    dir.push("autoalloc");
    dir.push(name);

    std::fs::create_dir_all(&dir)?;

    Ok(tempdir::TempDir::new_in(dir, "allocation")?.into_path())
}

pub fn check_command_output(output: Output) -> AutoAllocResult<Output> {
    let status = output.status;
    if !status.success() {
        return Err(anyhow::anyhow!(
            "Exit code {}\nstderr: {}\nstdout: {}",
            status.code().unwrap(),
            output.stderr.to_str().unwrap(),
            output.stdout.to_str().unwrap()
        ));
    }
    Ok(output)
}
