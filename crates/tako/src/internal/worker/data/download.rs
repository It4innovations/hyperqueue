use crate::datasrv::DataObjectId;
use crate::internal::datasrv::{DataObjectRef, DownloadInterface, DownloadManagerRef};
use crate::internal::messages::worker::FromWorkerMessage;
use crate::internal::worker::state::WorkerStateRef;
use crate::PriorityTuple;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

impl DownloadInterface for WorkerStateRef {
    fn find_placement(&self, data_id: DataObjectId) -> Receiver<Option<String>> {
        let (sender, receiver) = oneshot::channel();
        self.get_mut().ask_for_data_placement(data_id, sender);
        receiver
    }

    fn on_download_finished(&self, data_id: DataObjectId, data_ref: DataObjectRef) {
        let mut state = self.get_mut();
        state.on_download_finished(data_id, data_ref);
    }

    fn on_download_failed(&self, data_id: DataObjectId) {
        let mut state = self.get_mut();
        state.on_download_failed(data_id);
    }
}

pub(crate) type WorkerDownloadManagerRef = DownloadManagerRef<WorkerStateRef, PriorityTuple>;
