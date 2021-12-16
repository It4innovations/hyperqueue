use crate::common::Map;
use crate::server::task::{Task, TaskRef};
use crate::TaskId;
use std::cell::{Ref, RefMut};
use std::ops::{Deref, DerefMut};

#[derive(Default, Debug)]
pub struct TaskMap {
    tasks: Map<TaskId, TaskRef>,
}

impl TaskMap {
    #[inline]
    pub fn get_task_ref(&self, task_id: TaskId) -> Ref<Task> {
        self.tasks.get(&task_id).unwrap().get()
    }

    #[inline]
    pub fn get_task_ref_mut(&mut self, task_id: TaskId) -> RefMut<Task> {
        self.tasks.get(&task_id).unwrap().get_mut()
    }
}

impl Deref for TaskMap {
    type Target = Map<TaskId, TaskRef>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tasks
    }
}
impl DerefMut for TaskMap {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tasks
    }
}
