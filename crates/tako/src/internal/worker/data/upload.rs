use crate::datasrv::DataObjectId;
use crate::internal::datasrv::{DataObjectRef, UploadInterface};
use crate::internal::worker::state::WorkerStateRef;

impl UploadInterface for WorkerStateRef {
    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef> {
        self.get().data_storage.get_object(data_id).cloned()
    }
}
