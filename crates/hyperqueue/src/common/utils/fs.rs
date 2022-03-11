use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};

pub fn absolute_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        let env = std::env::current_dir().unwrap();
        env.join(path)
    }
}

pub fn create_symlink(symlink_path: &Path, target: &Path) -> crate::Result<()> {
    if symlink_path.exists() {
        std::fs::remove_file(symlink_path)?;
    }
    std::os::unix::fs::symlink(target, symlink_path)?;
    Ok(())
}

pub fn get_current_dir() -> PathBuf {
    std::env::current_dir().expect("Cannot get current working directory")
}

/// Returns true if the path is relative and doesn't start with `.`.
pub fn is_implicit_path(path: &Path) -> bool {
    !path.is_absolute() && !path.starts_with(".")
}

pub fn bytes_to_path(path: &[u8]) -> &Path {
    Path::new(OsStr::from_bytes(path))
}

pub fn path_has_extension(path: &Path, extension: &str) -> bool {
    path.extension() == Some(OsStr::from_bytes(extension.as_bytes()))
}
