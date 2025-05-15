use crate::{TaskId, define_id_type};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::rc::Rc;

define_id_type!(OutputId, u32);
define_id_type!(DataInputId, u32);

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DataObjectId {
    pub task_id: TaskId,
    pub data_id: OutputId,
}

impl DataObjectId {
    pub fn new(task_id: TaskId, data_id: OutputId) -> Self {
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

#[derive(Serialize, Deserialize)]
pub struct DataObject {
    mime_type: Option<String>,

    #[serde(with = "serde_bytes")]
    data: bytes::Bytes,
}

impl Debug for DataObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<MemDataObj mimetype={:?} size={}>",
            self.mime_type,
            self.size(),
        )
    }
}

impl DataObject {
    pub fn new(mime_type: Option<String>, data: Vec<u8>) -> Self {
        DataObject { mime_type, data }
    }

    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn mime_type(&self) -> Option<&String> {
        self.mime_type.as_ref()
    }
}

pub type DataObjectRef = Rc<DataObject>;
