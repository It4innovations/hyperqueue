use crate::datasrv::DataObjectId;
use crate::internal::datasrv::datastorage::DataStorage;
use crate::internal::datasrv::download::{
    start_download_manager_process, DownloadInterface, DownloadManager, DownloadManagerRef,
};
use crate::internal::datasrv::{DataObject, DataObjectRef};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

#[test]
fn storage_put_get() {
    let mut storage = DataStorage::new();
    let id = DataObjectId::new(123.into(), 456.into());
    assert!(storage.get_object(id).is_none());
    assert!(!storage.has_object(id));

    let data_obj = DataObjectRef::new(DataObject::new("".to_string(), vec![1, 2, 3]));
    storage.put_object(id, data_obj.clone()).unwrap();
    let obj = storage.get_object(id).unwrap();
    assert_eq!(obj.data(), &[1, 2, 3]);
    assert!(storage.has_object(id));
    assert!(storage.put_object(id, data_obj).is_err());

    storage.remove_object(id);
    assert!(storage.get_object(id).is_none());
    assert!(!storage.has_object(id));
}

#[derive(Default)]
struct TestDmInterface {
    hosts: crate::Map<DataObjectId, String>,
}

impl DownloadInterface for TestDmInterface {
    fn find_placement(&self, data_id: DataObjectId) -> Receiver<String> {
        let host = self.hosts.get(&data_id).unwrap().clone();
        let (sender, receiver) = oneshot::channel();
        sender.send(host).unwrap();
        receiver
    }

    fn get_object(&self, data_id: DataObjectId) -> Option<DataObjectRef> {
        todo!()
    }

    fn on_download_finished(&self, data_id: DataObjectId, data_ref: DataObjectRef) {
        todo!()
    }

    fn on_download_failed(&self, data_id: DataObjectId) {
        todo!()
    }
}

fn test_download_manager() -> DownloadManagerRef<TestDmInterface, u32> {
    let mut dm_ref = DownloadManagerRef::new(None);
    let interface = TestDmInterface::default();
    dm_ref.get_mut().set_interface(interface);
    dm_ref
}

#[test]
fn download_update_priorities() {
    let dm_ref = test_download_manager();
    let data_id1 = DataObjectId::new(1.into(), 5.into());
    let data_id2 = DataObjectId::new(2.into(), 1.into());
    let data_id3 = DataObjectId::new(2.into(), 0.into());

    let mut dm = dm_ref.get_mut();
    dm.download_object(data_id1, 0);
    dm.download_object(data_id2, 2);
    dm.download_object(data_id3, 1);
    let v: Vec<_> = dm.queue().clone().into_sorted_iter().map(|x| x.0).collect();
    assert_eq!(v, vec![data_id2, data_id3, data_id1]);
    dm.download_object(data_id1, 3);
    let v: Vec<_> = dm.queue().clone().into_sorted_iter().map(|x| x.0).collect();
    assert_eq!(v, vec![data_id1, data_id2, data_id3]);
}

#[tokio::test]
async fn download_invalid_hostname() {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    let dm_ref = test_download_manager();
    let dm_ref2 = dm_ref.clone();
    set.spawn_local(async move {
        start_download_manager_process(dm_ref2, 4, 2);
    });

    let data_id1 = DataObjectId::new(1.into(), 5.into());
    dm_ref.get_mut().download_object(data_id1, 0);

    set.run_until(tokio::time::sleep(Duration::new(1, 0))).await;
}
