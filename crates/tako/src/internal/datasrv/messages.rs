use crate::internal::datasrv::dataobj::DataId;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct DataObject {
    mime_type: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
enum ToDataNodeLocalMessage {
    PutDataObject {
        data_id: DataId,
        data_object: DataObject,
    },
}

#[derive(Debug, Deserialize)]
enum FromDataNodeLocalMessage {
    Uploaded(DataId),
    Error(String),
}
