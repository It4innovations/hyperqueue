/*
   This file contains message for uploading/download data objects

   To avoid unnecessary data copying, some messages has variants Up and Down,
   Up is used for uploading side (serialization end)
   Down is used by downloading side (deserialization end)
*/

use crate::datasrv::DataObjectId;
use crate::internal::datasrv::dataobj::{DataInputId, OutputId};
use crate::internal::datasrv::{DataObject, DataObjectRef};
use serde::{Deserialize, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

#[derive(Debug)]
pub struct DataObjectSlice {
    pub data_object: Rc<DataObject>,
    pub start: usize,
    pub end: usize,
}

impl Serialize for DataObjectSlice {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.data_object.data()[self.start..self.end])
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct PutDataUp<'a> {
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

#[derive(Debug, Serialize)]
pub(crate) enum FromLocalDataClientMessageUp<'a> {
    PutDataObject {
        data_id: OutputId,
        mime_type: &'a str,
        size: u64,
        data: PutDataUp<'a>,
    },
    PutDataObjectPart(PutDataUp<'a>),
    GetInput {
        input_id: DataInputId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DataDown {
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) enum FromLocalDataClientMessageDown {
    PutDataObject {
        data_id: OutputId,
        mime_type: String,
        size: u64,
        data: DataDown,
    },
    PutDataObjectPart(DataDown),
    GetInput {
        input_id: DataInputId,
    },
}

#[derive(Debug, Serialize)]
pub(crate) enum ToLocalDataClientMessageUp {
    Uploaded(OutputId),
    DataObject {
        mime_type: String,
        size: u64,
        data: DataObjectSlice,
    },
    DataObjectPart(DataObjectSlice),
    Error(String),
}

#[derive(Debug, Deserialize)]
pub(crate) enum ToLocalDataClientMessageDown {
    Uploaded(OutputId),
    DataObject {
        mime_type: String,
        size: u64,
        data: DataDown,
    },
    DataObjectPart(DataDown),
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FromDataClientMessage {
    GetObject { data_id: DataObjectId },
}

#[derive(Debug, Serialize)]
pub(crate) enum ToDataClientMessageUp {
    DataObject {
        mime_type: String,
        size: u64,
        data: DataObjectSlice,
    },
    DataObjectPart(DataObjectSlice),
    DataObjectNotFound,
}

#[derive(Debug, Deserialize)]
pub(crate) enum ToDataClientMessageDown {
    DataObject {
        mime_type: String,
        size: u64,
        data: DataDown,
    },
    DataObjectPart(DataDown),
    DataObjectNotFound,
}
