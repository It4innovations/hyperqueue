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
    pub fn new(tasks: Map<TaskId, TaskRef>) -> Self {
        Self { tasks }
    }

    #[inline]
    pub fn get_task_ref(&self, task_id: TaskId) -> Ref<Task> {
        self.tasks.get(&task_id).unwrap().get()
    }

    #[inline]
    pub fn get_task_ref_mut(&mut self, task_id: TaskId) -> RefMut<Task> {
        self.tasks.get(&task_id).unwrap().get_mut()
    }

    #[inline]
    pub fn get_task_opt(&self, task_id: TaskId) -> Option<Ref<Task>> {
        self.tasks.get(&task_id).map(|t| t.get())
    }

    #[inline]
    pub fn get_task_opt_mut(&mut self, task_id: TaskId) -> Option<RefMut<Task>> {
        self.tasks.get(&task_id).map(|t| t.get_mut())
    }

    #[inline]
    pub fn iter_tasks(&self) -> impl Iterator<Item = Ref<Task>> {
        self.tasks.values().map(|t| t.get())
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
