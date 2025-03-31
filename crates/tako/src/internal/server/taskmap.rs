use crate::TaskId;
use crate::internal::common::stablemap::StableMap;
use crate::internal::server::task::Task;

#[derive(Default, Debug)]
pub struct TaskMap {
    tasks: StableMap<TaskId, Task>,
}

impl TaskMap {
    // Insertion
    #[inline(always)]
    pub fn insert(&mut self, task: Task) -> Option<Task> {
        self.tasks.insert(task);
        // StableMap panics on duplicate insertion
        None
    }

    // Removal
    #[inline(always)]
    pub fn remove(&mut self, task_id: TaskId) -> Option<Task> {
        self.tasks.remove(&task_id)
    }

    // Accessors
    #[inline(always)]
    pub fn get_task(&self, task_id: TaskId) -> &Task {
        self.tasks.find(&task_id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={task_id}");
        })
    }

    #[inline(always)]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.tasks.find_mut(&task_id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={task_id}");
        })
    }

    #[inline(always)]
    pub fn find_task(&self, task_id: TaskId) -> Option<&Task> {
        self.tasks.find(&task_id)
    }

    #[inline(always)]
    pub fn find_task_mut(&mut self, task_id: TaskId) -> Option<&mut Task> {
        self.tasks.find_mut(&task_id)
    }

    // Iteration
    #[inline(always)]
    pub fn task_ids(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.tasks.keys().copied()
    }

    #[inline(always)]
    pub fn tasks(&self) -> impl Iterator<Item = &Task> {
        self.tasks.values()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn shrink_to_fit(&mut self) {
        self.tasks.shrink_to_fit();
    }
}
