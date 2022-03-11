use std::future::Future;
use tokio::task::LocalSet;

/// Enables spawning a group of tasks that will no longer be polled after the group is dropped.
/// Does not allow getting the results of the individual tasks, they thus have to be
/// "fire-and-forget" tasks.
pub struct TaskGroup {
    set: LocalSet,
}

impl Default for TaskGroup {
    fn default() -> Self {
        Self {
            set: Default::default(),
        }
    }
}

impl TaskGroup {
    /// Runs the passed future, along with all tasks within the group, until the future finishes.
    pub async fn run_until<F: Future<Output = R>, R>(&self, future: F) -> R {
        self.set.run_until(future).await
    }

    /// Add a new task to the group.
    pub fn add_task<F: Future + 'static>(&self, future: F) {
        self.set.spawn_local(future);
    }
}
