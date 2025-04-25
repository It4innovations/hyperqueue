use crate::datasrv::DataObjectId;
use crate::internal::datasrv::DataObjectRef;
use crate::internal::datasrv::download::{
    DownloadInterface, DownloadManagerRef, download_manager_process,
};
use crate::internal::datasrv::upload::{UploadInterface, data_upload_service};
use crate::{Map, WrappedRcRefCell};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tokio::task::spawn_local;

#[derive(Clone)]
pub(crate) enum PlacementConfig {
    Valid(String),
    Unresolvable,
    Ignore,
}

#[derive(Default)]
pub(crate) struct TestDmInterfaceInner {
    pub hosts: Map<DataObjectId, PlacementConfig>,
    pub failed_downloads: Vec<DataObjectId>,
    pub finished_downloads: Map<DataObjectId, DataObjectRef>,
    pub resolved_ids: Vec<DataObjectId>,
}

pub(crate) type TestDmInterface = WrappedRcRefCell<TestDmInterfaceInner>;

impl DownloadInterface for TestDmInterface {
    fn find_placement(&self, data_id: DataObjectId) -> Receiver<Option<String>> {
        self.get_mut().resolved_ids.push(data_id);
        let response = self.get().hosts.get(&data_id).cloned().unwrap_or_else(|| {
            panic!("Unexpected lookup of placement for object {data_id}");
        });
        let (sender, receiver) = oneshot::channel();
        match response {
            PlacementConfig::Valid(host) => {
                sender.send(Some(host)).unwrap();
            }
            PlacementConfig::Unresolvable => {
                sender.send(None).unwrap();
            }
            PlacementConfig::Ignore => { /* Do nothing */ }
        }
        receiver
    }

    fn on_download_finished(&self, data_id: DataObjectId, data_ref: DataObjectRef) {
        assert!(
            self.get_mut()
                .finished_downloads
                .insert(data_id, data_ref)
                .is_none()
        );
    }

    fn on_download_failed(&self, data_id: DataObjectId) {
        self.get_mut().failed_downloads.push(data_id);
    }
}

impl TestDmInterface {
    pub fn register_hosts(&self, data_ids: &[DataObjectId], host: PlacementConfig) {
        let mut dm = self.get_mut();
        for data_id in data_ids {
            dm.hosts.insert(*data_id, host.clone());
        }
    }

    pub fn check_empty(&self) {
        assert!(self.get().failed_downloads.is_empty());
        assert!(self.get().finished_downloads.is_empty());
        assert!(self.get().resolved_ids.is_empty())
    }
    pub fn take_failed_downloads(&self, length: usize) -> Vec<DataObjectId> {
        let f = std::mem::take(&mut self.get_mut().failed_downloads);
        assert_eq!(f.len(), length);
        f
    }
    pub fn take_finished_downloads(&self, length: usize) -> Map<DataObjectId, DataObjectRef> {
        let f = std::mem::take(&mut self.get_mut().finished_downloads);
        assert_eq!(f.len(), length);
        f
    }
    pub fn take_resolved_ids(&self, length: usize) -> Vec<DataObjectId> {
        let f = std::mem::take(&mut self.get_mut().resolved_ids);
        assert_eq!(f.len(), length);
        f
    }
}

pub(crate) fn test_download_manager() -> (DownloadManagerRef<TestDmInterface, u32>, TestDmInterface)
{
    let interface = TestDmInterface::default();
    let dm_ref = DownloadManagerRef::new(interface.clone(), None);
    (dm_ref, interface)
}

#[derive(Default)]
pub(crate) struct TestUploadInterfaceInner {
    pub objects: Map<DataObjectId, DataObjectRef>,
}

pub(crate) type TestUploadInterface = WrappedRcRefCell<TestUploadInterfaceInner>;

impl UploadInterface for TestUploadInterface {
    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef> {
        self.get().objects.get(&data_id).cloned()
    }

    fn upload_finished(&self, _size: u64) {
        /* Do nothing */
    }
}

impl TestUploadInterface {
    pub fn new() -> TestUploadInterface {
        TestUploadInterface::wrap(TestUploadInterfaceInner::default())
    }

    pub fn insert_object(&self, data_id: DataObjectId, data_obj: DataObjectRef) {
        self.get_mut().objects.insert(data_id, data_obj);
    }
}

pub(crate) async fn start_test_upload_service(interface: TestUploadInterface) -> String {
    let listener = TcpListener::bind(SocketAddr::from_str("127.0.0.1:0").unwrap())
        .await
        .unwrap();
    let listener_port = listener.local_addr().unwrap().port();
    spawn_local(data_upload_service(listener, None, interface));
    format!("127.0.0.1:{listener_port}")
}

pub(crate) fn start_download_manager(
    dm_ref: &DownloadManagerRef<TestDmInterface, u32>,
    repeat_timout: u64,
    timeout: u64,
) {
    let dm_ref = dm_ref.clone();
    spawn_local(async move {
        download_manager_process(
            dm_ref,
            2,
            3,
            Duration::from_secs(repeat_timout),
            Duration::from_secs(timeout),
        )
        .await
    });
}
