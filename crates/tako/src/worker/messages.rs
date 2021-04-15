use serde::{Deserialize, Serialize};

use crate::common::data::SerializationType;
use crate::messages::common::TaskFailInfo;
use crate::TaskId;

use super::subworker::SubworkerId;

#[derive(Deserialize, Debug)]
pub(crate) struct RegisterSubworkerMessage {
    pub(crate) subworker_id: SubworkerId,
}

#[derive(Serialize, Debug)]
pub(crate) struct RegisterSubworkerResponse {
    pub(crate) worker: String,
}

#[derive(Serialize, Debug)]
pub struct UploadMsg {
    pub id: TaskId,
    pub serializer: SerializationType,
} // The following message contains data

#[derive(Serialize, Debug)]
pub struct DownloadRequestMsg {
    pub id: TaskId,
}

#[derive(Serialize, Debug)]
pub struct ComputeTaskMsg<'a> {
    pub id: TaskId,

    #[serde(with = "serde_bytes")]
    pub spec: &'a Vec<u8>,
}

#[derive(Serialize, Debug)]
pub struct RemoveDataMsg {
    pub id: TaskId,
}

#[derive(Serialize, Debug)]
#[serde(tag = "op")]
pub enum ToSubworkerMessage<'a> {
    ComputeTask(ComputeTaskMsg<'a>),
    Upload(UploadMsg),
    DownloadRequest(DownloadRequestMsg),
    RemoveData(RemoveDataMsg),
}

#[derive(Deserialize, Debug)]
pub struct TaskFinishedMsg {
    pub id: TaskId,
    pub size: u64,
}

#[derive(Deserialize, Debug)]
pub struct TaskFailedMsg {
    pub id: TaskId,
    pub info: TaskFailInfo,
}

#[derive(Deserialize, Debug)]
pub struct DownloadResponseMsg {
    pub id: TaskId,
    pub serializer: SerializationType,
} // The following message contains data

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
pub enum FromSubworkerMessage {
    TaskFinished(TaskFinishedMsg),
    TaskFailed(TaskFailedMsg),
    DownloadResponse(DownloadResponseMsg),
}
