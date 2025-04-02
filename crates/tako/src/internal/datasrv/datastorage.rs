use crate::datasrv::DataObjectId;
use crate::internal::common::error::DsError;
use crate::internal::datasrv::download::DownloadManager;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::messages::worker::{DataNodeOverview, DataObjectOverview};
use crate::{Map, WrappedRcRefCell};
use hashbrown::hash_map::Entry;
use std::rc::Rc;

pub(crate) struct DataStorage {
    store: Map<DataObjectId, DataObjectRef>,
}

impl DataStorage {
    pub fn new() -> Self {
        DataStorage { store: Map::new() }
    }

    pub fn get_object(&self, data_object_id: DataObjectId) -> Option<&DataObjectRef> {
        self.store.get(&data_object_id)
    }

    pub fn has_object(&self, data_object_id: DataObjectId) -> bool {
        self.store.contains_key(&data_object_id)
    }

    pub fn put_object(
        &mut self,
        data_object_id: DataObjectId,
        data_object: DataObjectRef,
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
                size: obj.size(),
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
