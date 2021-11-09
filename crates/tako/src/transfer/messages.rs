use serde::{Deserialize, Serialize};

use crate::common::data::SerializationType;
use crate::TaskId;

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchRequestMsg {
    pub task_id: TaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadDataMsg {
    pub task_id: TaskId,
    pub serializer: SerializationType,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum DataRequest {
    FetchRequest(FetchRequestMsg),
    UploadData(UploadDataMsg),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FetchResponseData {
    pub serializer: SerializationType,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadResponseMsg {
    pub task_id: TaskId,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum DataResponse {
    Data(FetchResponseData),
    NotAvailable,
    DataUploaded(UploadResponseMsg),
}
