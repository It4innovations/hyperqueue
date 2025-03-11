use crate::datasrv::DataObjectId;
use crate::internal::datasrv::dataobj::{DataInputId, OutputId};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

#[derive(Serialize, Deserialize)]
pub struct DataObject {
    pub mime_type: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

impl Debug for DataObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<DataObj mimetype='{}' len={}>",
            self.mime_type,
            self.data.len()
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FromLocalDataClientMessage {
    PutDataObject {
        data_id: OutputId,
        data_object: DataObject,
    },
    GetInput {
        input_id: DataInputId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ToLocalDataClientMessage {
    Uploaded(OutputId),
    DataObject(Rc<DataObject>),
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FromDataClientMessage {
    GetObject { data_id: DataObjectId },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ToDataClientMessage {
    DataObject(Rc<DataObject>),
}
