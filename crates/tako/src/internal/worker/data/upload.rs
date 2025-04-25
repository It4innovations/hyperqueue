use crate::datasrv::DataObjectId;
use crate::internal::datasrv::{DataObjectRef, UploadInterface};
use crate::internal::worker::state::WorkerStateRef;

impl UploadInterface for WorkerStateRef {
    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef> {
        self.get().data_storage.get_object(data_id).cloned()
    }

    fn upload_finished(&self, size: u64) {
        self.get_mut().data_storage.add_stats_remote_upload(size);
    }
}
