use crate::worker::task::Task;
use crate::TaskId;
use hashbrown::HashMap;

#[derive(Default)]
#[repr(transparent)]
pub struct TaskMap {
    map: HashMap<TaskId, Task>,
}

impl TaskMap {
    #[inline]
    pub fn get(&self, task_id: TaskId) -> &Task {
        &self.map[&task_id]
    }

    #[inline]
    pub fn get_mut(&mut self, task_id: TaskId) -> &mut Task {
        self.map.get_mut(&task_id).expect("Task not found")
    }

    #[inline]
    pub fn find(&self, task_id: TaskId) -> Option<&Task> {
        self.map.get(&task_id)
    }

    #[inline]
    pub fn find_mut(&mut self, task_id: TaskId) -> Option<&mut Task> {
        self.map.get_mut(&task_id)
    }

    #[inline]
    pub fn insert(&mut self, task_id: TaskId, task: Task) {
        self.map.insert(task_id, task);
    }

    #[inline]
    pub fn remove(&mut self, task_id: TaskId) -> Option<Task> {
        self.map.remove(&task_id)
    }
}
