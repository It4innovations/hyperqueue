use crate::{TaskId, define_id_type};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

define_id_type!(DataId, u32);
define_id_type!(DataInputId, u32);

pub(crate) type InputMap = crate::Map<DataInputId, DataObjectId>;

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DataObjectId {
    pub task_id: TaskId,
    pub data_id: DataId,
}

impl DataObjectId {
    pub fn new(task_id: TaskId, data_id: DataId) -> Self {
        Self { task_id, data_id }
    }
}

impl Display for DataObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.task_id, self.data_id)
    }
}

impl Debug for DataObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.task_id, self.data_id)
    }
}
