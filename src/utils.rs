use std::path::PathBuf;

pub fn absolute_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        let env = std::env::current_dir().unwrap();
        env.join(path)
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::future::Future;
    use tokio::task::{LocalSet, JoinHandle};

    pub(crate) async fn run_concurrent<
        R: 'static,
        Fut1: 'static + Future<Output=R>,
        Fut2: Future<Output=()>
    >(background_fut: Fut1, fut: Fut2) -> (LocalSet, JoinHandle<R>) {
        let set = tokio::task::LocalSet::new();
        let handle = set.spawn_local(background_fut);
        set.run_until(fut).await;
        (set, handle)
    }
}
