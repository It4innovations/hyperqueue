use crate::common::Map;
use crate::server::task::TaskRef;
use crate::TaskId;
use std::ops::{Deref, DerefMut};

#[derive(Default, Debug)]
pub struct TaskMap {
    tasks: Map<TaskId, TaskRef>,
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
