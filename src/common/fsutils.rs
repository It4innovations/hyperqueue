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

#[cfg(test)]
pub(crate) mod test_utils {
    use std::future::Future;
    use tokio::task::{JoinHandle, LocalSet};

    pub(crate) async fn run_concurrent<
        R: 'static,
        Fut1: 'static + Future<Output = R>,
        Fut2: Future<Output = ()>,
    >(
        background_fut: Fut1,
        fut: Fut2,
    ) -> (LocalSet, JoinHandle<R>) {
        let set = tokio::task::LocalSet::new();
        let handle = set.spawn_local(background_fut);
        set.run_until(fut).await;
        (set, handle)
    }
}
