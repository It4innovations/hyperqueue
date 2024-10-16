use crate::{define_id_type, TaskId};
use std::fmt::{Display, Formatter};

define_id_type!(DataId, u32);

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) struct DataObjectId {
    pub task_id: TaskId,
    pub data_id: DataId,
}

impl Display for DataObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.task_id, self.data_id)
    }
}

pub(crate) struct DataObject {
    pub data_object_id: DataObjectId,
    pub mime_type: String,
    pub data: Vec<u8>,
}

impl Display for DataObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<DataObj {} len={}>",
            self.data_object_id,
            self.data.len()
        )
    }
}
