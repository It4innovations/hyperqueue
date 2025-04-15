use crate::datasrv::DataObjectId;
use crate::internal::common::resources::allocation::AllocationIndex;
use crate::internal::common::stablemap::StableMap;
use crate::internal::server::dataobj::{DataObjectHandle, ObjsToRemoveFromWorkers};
use crate::{Map, WorkerId};
use smallvec::SmallVec;

#[derive(Default, Debug)]
pub(crate) struct DataObjectMap {
    data_objs: StableMap<DataObjectId, DataObjectHandle>,
}

impl DataObjectMap {
    #[inline(always)]
    pub fn insert(&mut self, data_obj: DataObjectHandle) {
        self.data_objs.insert(data_obj);
    }

    pub fn iter(&self) -> impl Iterator<Item = &DataObjectHandle> {
        self.data_objs.values()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut DataObjectHandle> {
        self.data_objs.values_mut()
    }

    #[inline(always)]
    pub fn get_data_object_mut(&mut self, data_object_id: DataObjectId) -> &mut DataObjectHandle {
        self.data_objs.find_mut(&data_object_id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={data_object_id}");
        })
    }

    #[inline(always)]
    pub fn get_data_object(&self, data_object_id: DataObjectId) -> &DataObjectHandle {
        self.data_objs.find(&data_object_id).unwrap_or_else(|| {
            panic!("Asking for invalid task id={data_object_id}");
        })
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.data_objs.len()
    }

    #[inline(always)]
    pub fn find_data_object_mut(
        &mut self,
        data_object_id: DataObjectId,
    ) -> Option<&mut DataObjectHandle> {
        self.data_objs.find_mut(&data_object_id)
    }

    #[inline(always)]
    pub fn find_data_object(&self, data_object_id: DataObjectId) -> Option<&DataObjectHandle> {
        self.data_objs.find(&data_object_id)
    }

    pub fn remove_object(&mut self, data_object_id: DataObjectId) {
        self.data_objs.remove(&data_object_id);
    }

    pub fn try_decrease_ref_count(
        &mut self,
        data_object_id: DataObjectId,
        objs_to_delete: &mut ObjsToRemoveFromWorkers,
    ) {
        if let Some(obj) = self.find_data_object_mut(data_object_id) {
            if obj.decrease_ref_count() {
                for worker_id in obj.placement() {
                    objs_to_delete.add(*worker_id, data_object_id);
                }
                self.data_objs.remove(&data_object_id);
            }
        }
    }
}
