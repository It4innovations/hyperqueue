use std::fmt::Debug;
use std::fmt::Formatter;

use bytes::Bytes;
use hashbrown::HashSet;

use crate::common::data::SerializationType;
use crate::common::WrappedRcRefCell;
use crate::worker::task::TaskRef;
use crate::{TaskId, WorkerId};

pub struct LocalData {
    pub serializer: SerializationType,
    pub bytes: Bytes,
}

impl Debug for LocalData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalData")
    }
}

#[derive(Debug)]
pub struct RemoteData {
    pub workers: Vec<WorkerId>,
}

pub enum Subscriber {
    Task(TaskRef),
    OneShot(tokio::sync::oneshot::Sender<(SerializationType, Bytes)>),
}

#[derive(Debug)]
pub enum DataObjectState {
    Remote(RemoteData),
    Local(LocalData),
    Removed,
}

pub struct DataObject {
    pub id: TaskId,
    pub state: DataObjectState,
    pub consumers: HashSet<TaskRef>,
    pub size: u64,
}

impl DataObject {
    #[inline]
    pub fn local_data(&self) -> Option<&LocalData> {
        match &self.state {
            DataObjectState::Local(x) => Some(x),
            _ => None,
        }
    }
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(id: TaskId, size: u64, state: DataObjectState) -> Self {
        WrappedRcRefCell::wrap(DataObject {
            id,
            size,
            state,
            consumers: Default::default(),
        })
    }
}
