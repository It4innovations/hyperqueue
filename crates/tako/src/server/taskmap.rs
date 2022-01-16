use crate::common::stablemap::StableMap;
use crate::server::system::TaskSystem;
use crate::server::task::Task;
use crate::TaskId;

#[derive(Default, Debug)]
pub struct TaskMap<System: TaskSystem> {
    tasks: StableMap<TaskId, Task<System>>,
}

impl<System: TaskSystem> TaskMap<System> {
    // Insertion
    #[inline(always)]
    pub fn insert(&mut self, task: Task<System>) -> Option<Task<System>> {
        self.tasks.insert(task);
        // StableMap panics on duplicate insertion
        None
    }

    // Removal
    #[inline(always)]
    pub fn remove(&mut self, task_id: TaskId) -> Option<Task<System>> {
        self.tasks.remove(&task_id)
    }

    // Accessors
    #[inline(always)]
    pub fn get_task(&self, task_id: TaskId) -> &Task<System> {
        self.tasks.find(&task_id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", task_id);
        })
    }

    #[inline(always)]
    pub fn get_task_mut(&mut self, task_id: TaskId) -> &mut Task<System> {
        self.tasks.find_mut(&task_id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={}", task_id);
        })
    }

    #[inline(always)]
    pub fn find_task(&self, task_id: TaskId) -> Option<&Task<System>> {
        self.tasks.find(&task_id)
    }

    #[inline(always)]
    pub fn find_task_mut(&mut self, task_id: TaskId) -> Option<&mut Task<System>> {
        self.tasks.find_mut(&task_id)
    }

    // Iteration
    #[inline(always)]
    pub fn task_ids(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.tasks.keys().copied()
    }

    #[inline(always)]
    pub fn tasks(&self) -> impl Iterator<Item = &Task<System>> {
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
}
