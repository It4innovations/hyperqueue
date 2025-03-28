use crate::datasrv::DataObjectId;
use crate::internal::datasrv::dataobj::{DataInputId, OutputId};
use crate::internal::datasrv::{DataObject, DataObjectRef};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FromLocalDataClientMessage {
    PutDataObject {
        data_id: OutputId,
        data_object: DataObjectRef,
    },
    GetInput {
        input_id: DataInputId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ToLocalDataClientMessage {
    Uploaded(OutputId),
    DataObject(DataObjectRef),
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FromDataClientMessage {
    GetObject { data_id: DataObjectId },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ToDataClientMessage {
    DataObject(Rc<DataObject>),
    DataObjectNotFound,
}
