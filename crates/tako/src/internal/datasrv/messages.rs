use crate::internal::datasrv::dataobj::DataId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DataObject {
    pub mime_type: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ToDataNodeLocalMessage {
    PutDataObject {
        data_id: DataId,
        data_object: DataObject,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FromDataNodeLocalMessage {
    Uploaded(DataId),
    Error(String),
}
