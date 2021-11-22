use crate::worker::task::TaskRef;
use crate::TaskId;
use hashbrown::HashMap;

#[derive(Default)]
#[repr(transparent)]
pub struct TaskMap {
    map: HashMap<TaskId, TaskRef>,
}

impl TaskMap {
    #[inline]
    pub fn get(&self, task_id: TaskId) -> &TaskRef {
        &self.map[&task_id]
    }

    #[inline]
    pub fn find(&self, task_id: TaskId) -> Option<&TaskRef> {
        self.map.get(&task_id)
    }

    #[inline]
    pub fn insert(&mut self, task_id: TaskId, task: TaskRef) {
        self.map.insert(task_id, task);
    }

    #[inline]
    pub fn remove(&mut self, task_id: TaskId) -> Option<TaskRef> {
        self.map.remove(&task_id)
    }
}
