use crate::datasrv::DataObjectId;
use crate::internal::datasrv::datastorage::DataStorage;
use crate::internal::datasrv::download::start_download_manager_process;
use crate::internal::datasrv::test_utils::{
    start_test_upload_service, test_download_manager, TestUploadInterface,
};
use crate::internal::datasrv::upload::UploadInterface;
use crate::internal::datasrv::{DataObject, DataObjectRef};
use std::time::Duration;

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

#[test]
fn download_update_priorities() {
    let (dm_ref, interface) = test_download_manager();
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
    interface.check_empty();
}

#[tokio::test]
async fn download_invalid_hostname() {
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    let (dm_ref, interface) = test_download_manager();

    let data_id1 = DataObjectId::new(1.into(), 5.into());
    interface
        .get_mut()
        .hosts
        .insert(data_id1, Some("nonexistingx:1".to_string()));

    let dm_ref2 = dm_ref.clone();
    set.spawn_local(async move {
        start_download_manager_process(dm_ref2, 4, 2);
    });

    dm_ref.get_mut().download_object(data_id1, 0);
    set.run_until(tokio::time::sleep(Duration::new(3, 0))).await;

    assert_eq!(interface.take_failed_downloads(1)[0], data_id1);

    interface.check_empty();
}

#[tokio::test]
async fn download_object_not_found() {
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    set.run_until(async move {
        let upload = TestUploadInterface::new();

        let addr = start_test_upload_service(upload).await;

        let (dm_ref, interface) = test_download_manager();

        let data_id1 = DataObjectId::new(1.into(), 2.into());
        interface.get_mut().hosts.insert(data_id1, Some(addr));

        let dm_ref2 = dm_ref.clone();
        start_download_manager_process(dm_ref2, 4, 2);

        dm_ref.get_mut().download_object(data_id1, 0);
        tokio::time::sleep(Duration::new(3, 0)).await;

        assert_eq!(interface.take_failed_downloads(1)[0], data_id1);

        interface.check_empty();
    })
    .await;
}

// TODO: DataNodeInfo
// TODO: Forgetting connections
// TODO: Test maximal parallel downloads
