use crate::datasrv::DataObjectId;
use crate::internal::common::stablemap::StableMap;
use crate::internal::server::dataobj::DataObjectHandle;

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

    pub fn remove_object(&mut self, data_object_id: DataObjectId) {
        self.data_objs.remove(&data_object_id);
    }
}
