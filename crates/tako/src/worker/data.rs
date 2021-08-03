use std::fmt::Debug;
use std::fmt::Formatter;

use bytes::Bytes;
use hashbrown::HashSet;
use smallvec::SmallVec;

use crate::common::data::SerializationType;
use crate::common::WrappedRcRefCell;
use crate::worker::subworker::SubworkerRef;
use crate::worker::task::TaskRef;
use crate::{TaskId, WorkerId};

pub struct LocalData {
    pub serializer: SerializationType,
    pub bytes: Bytes,
    pub subworkers: SmallVec<[SubworkerRef; 1]>,
}

impl Debug for LocalData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "subworkers: {}", self.subworkers.len())
    }
}

#[derive(Debug)]
pub struct RemoteData {
    pub workers: Vec<WorkerId>,
}

impl Debug for InSubworkersData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "subworkers: {}", self.subworkers.len())
    }
}

pub enum Subscriber {
    Task(TaskRef),
    OneShot(tokio::sync::oneshot::Sender<(SerializationType, Bytes)>),
}

pub struct InSubworkersData {
    pub subworkers: SmallVec<[SubworkerRef; 1]>,
}

pub struct LocalDownloadingData {
    pub subworkers: SmallVec<[SubworkerRef; 1]>,
    pub source: SubworkerRef,
    pub subscribers: SmallVec<[Subscriber; 1]>,
}

impl Debug for LocalDownloadingData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "subworkers: {}", self.subworkers.len())
    }
}

#[derive(Debug)]
pub enum DataObjectState {
    Remote(RemoteData),
    InSubworkers(InSubworkersData),
    LocalDownloading(LocalDownloadingData),
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

    #[inline]
    pub fn get_placement(&self) -> Option<&[SubworkerRef]> {
        match &self.state {
            DataObjectState::InSubworkers(d) => Some(&d.subworkers),
            DataObjectState::LocalDownloading(d) => Some(&d.subworkers),
            DataObjectState::Local(d) => Some(&d.subworkers),
            DataObjectState::Remote(_) | DataObjectState::Removed => None,
        }
    }

    pub fn is_in_subworker(&self, subworker_ref: &SubworkerRef) -> bool {
        self.get_placement()
            .map(|refs| refs.contains(subworker_ref))
            .unwrap_or(false)
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
