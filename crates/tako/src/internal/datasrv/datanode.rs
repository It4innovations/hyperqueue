use super::dataobj::{DataObject, DataObjectId};
use crate::internal::common::error::DsError;
use crate::{Map, WrappedRcRefCell};
use hashbrown::hash_map::Entry;
use std::path::Path;
use std::rc::Rc;
use tokio::net::UnixListener;

pub(crate) struct DataNode {
    store: Map<DataObjectId, Rc<DataObject>>,
}

pub type DataNodeRef = WrappedRcRefCell<DataNode>;

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

async fn run_datanode_over_unix_socket(
    listener: UnixListener,
    data_node_ref: DataNodeRef,
) -> crate::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                log::debug!("New data client connected via unix socket: {addr:?}")
            }
            Err(e) => {
                log::debug!("Accepting a new data client via unix socket failed: {e}")
            }
        }
    }
}
