use crate::internal::common::error::DsError;
use crate::internal::datasrv::dataobj::DataObjectId;
use crate::internal::datasrv::messages::{
    DataObject, FromLocalDataClientMessage, ToLocalDataClientMessage,
};
use crate::internal::messages::worker::{DataNodeOverview, DataObjectOverview, TaskOutput};
use crate::internal::worker::data::download::Downloads;
use crate::internal::worker::state::WorkerStateRef;
use crate::{Map, TaskId};
use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use hashbrown::hash_map::Entry;
use std::rc::Rc;

pub(crate) struct DataConnectionSession {
    task_id: TaskId,
    inputs: Vec<DataObjectId>,
}

pub(crate) struct DataNode {
    store: Map<DataObjectId, Rc<DataObject>>,
    downloads: Downloads,
}

impl DataNode {
    pub fn new() -> Self {
        DataNode {
            store: Map::new(),
            downloads: Downloads::new(),
        }
    }

    pub fn downloads(&mut self) -> &mut Downloads {
        &mut self.downloads
    }

    pub fn get_object(&self, data_object_id: DataObjectId) -> Option<&Rc<DataObject>> {
        self.store.get(&data_object_id)
    }

    pub fn has_object(&self, data_object_id: DataObjectId) -> bool {
        self.store.contains_key(&data_object_id)
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

    pub fn remove_object(&mut self, data_object_id: DataObjectId) {
        log::debug!("Removing data object {:?}", data_object_id);
        if self.store.remove(&data_object_id).is_none() {
            log::debug!("Data object {} not found", data_object_id);
        }
    }

    pub fn get_overview(&self) -> DataNodeOverview {
        let objects = self
            .store
            .iter()
            .map(|(id, obj)| DataObjectOverview {
                id: *id,
                size: obj.data.len(),
            })
            .collect();
        DataNodeOverview {
            objects,
            total_downloaded_count: 0,
            total_uploaded_count: 0,
            total_downloaded_bytes: 0,
            total_uploaded_bytes: 0,
        }
    }
}
