use super::dataobj::{DataObject, DataObjectId};
use crate::internal::common::error::DsError;
use crate::internal::worker::state::WorkerStateRef;
use crate::{Map, TaskId, WrappedRcRefCell};
use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream, StreamExt};
use hashbrown::hash_map::Entry;
use std::path::Path;
use std::rc::Rc;
use tokio::net::UnixListener;
use tokio_util::codec::length_delimited::Builder;
use tokio_util::codec::LengthDelimitedCodec;

pub(crate) struct DataNode {
    store: Map<DataObjectId, Rc<DataObject>>,
}

pub(crate) type DataNodeRef = WrappedRcRefCell<DataNode>;

impl DataNode {
    pub fn new() -> Self {
        DataNode { store: Map::new() }
    }

    pub fn get_object(&self, data_object_id: DataObjectId) -> Option<&Rc<DataObject>> {
        self.store.get(&data_object_id)
    }

    pub fn put_object(
        &mut self,
        data_object_id: DataObjectId,
        data_object: Rc<DataObject>,
    ) -> crate::Result<()> {
        match self.store.entry(data_object_id) {
            Entry::Occupied(_) => Err(DsError::GenericError(format!(
                "DataObject {data_object_id} already exists"
            ))),
            Entry::Vacant(entry) => {
                entry.insert(data_object);
                Ok(())
            }
        }
    }
}

impl DataNodeRef {
    pub fn new() -> Self {
        DataNodeRef::wrap(DataNode::new())
    }
}

pub(crate) async fn datanode_connection_handler(
    data_node_ref: DataNodeRef,
    mut rx: impl Stream<Item = Result<BytesMut, std::io::Error>> + std::marker::Unpin,
    tx: impl Sink<Bytes>,
    task_id: TaskId,
) -> crate::Result<()> {
    while let Some(data) = rx.next().await {
        let data = data?;
    }
    Ok(())
}
