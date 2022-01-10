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
