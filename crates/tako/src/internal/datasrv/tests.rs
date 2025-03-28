use crate::datasrv::DataObjectId;
use crate::internal::datasrv::datastorage::DataStorage;
use crate::internal::datasrv::download::start_download_manager_process;
use crate::internal::datasrv::test_utils::{
    TestUploadInterface, start_test_upload_service, test_download_manager,
};
use crate::internal::datasrv::{DataObject, DataObjectRef};
use crate::internal::tests::utils::sorted_vec;
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
        start_download_manager_process(dm_ref2, 4, 2, Duration::from_secs(1));
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

        start_download_manager_process(dm_ref.clone(), 4, 2, Duration::from_secs(0));

        dm_ref.get_mut().download_object(data_id1, 0);
        tokio::time::sleep(Duration::new(3, 0)).await;

        assert_eq!(interface.take_failed_downloads(1)[0], data_id1);
        interface.check_empty();
    })
    .await;
}

#[tokio::test]
async fn download_parallel_limit() {
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    set.run_until(async move {
        let (dm_ref, interface) = test_download_manager();

        let data_id1 = DataObjectId::new(1.into(), 0.into());
        let data_id2 = DataObjectId::new(1.into(), 1.into());
        let data_id3 = DataObjectId::new(2.into(), 0.into());
        let data_id4 = DataObjectId::new(2.into(), 1.into());

        interface.register_hosts(&[data_id2, data_id3], None);

        start_download_manager_process(dm_ref.clone(), 2, 3, Duration::from_secs(0));

        dm_ref.get_mut().download_object(data_id1, 0);
        dm_ref.get_mut().download_object(data_id2, 1);
        dm_ref.get_mut().download_object(data_id3, 1);
        dm_ref.get_mut().download_object(data_id4, 0);
        tokio::time::sleep(Duration::new(1, 0)).await;

        let ids = sorted_vec(interface.take_resolved_ids(4));
        assert_eq!(ids, vec![data_id2, data_id2, data_id3, data_id3]);
        interface.check_empty();
    })
    .await;
}

#[tokio::test]
async fn download_object_ok() {
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    set.run_until(async move {
        let upload = TestUploadInterface::new();
        let addr = start_test_upload_service(upload.clone()).await;
        let (dm_ref, interface) = test_download_manager();

        let data_id1 = DataObjectId::new(1.into(), 0.into());
        let data_id2 = DataObjectId::new(1.into(), 1.into());
        let data_id3 = DataObjectId::new(2.into(), 0.into());
        let data_id4 = DataObjectId::new(2.into(), 1.into());

        let obj1 = DataObjectRef::new(DataObject::new("abc".to_string(), vec![0]));
        let obj2 = DataObjectRef::new(DataObject::new("xyz".to_string(), vec![1]));
        let obj3 = DataObjectRef::new(DataObject::new("efg".to_string(), vec![2]));
        let obj4 = DataObjectRef::new(DataObject::new("klm".to_string(), vec![3, 4, 5, 6, 7]));

        upload.insert_object(data_id1, obj1.clone());
        upload.insert_object(data_id2, obj2.clone());
        upload.insert_object(data_id3, obj3.clone());
        upload.insert_object(data_id4, obj4.clone());

        interface.register_hosts(&[data_id1, data_id2, data_id3, data_id4], Some(addr));

        start_download_manager_process(dm_ref.clone(), 2, 3, Duration::from_secs(0));

        dm_ref.get_mut().download_object(data_id1, 0);
        dm_ref.get_mut().download_object(data_id2, 1);
        dm_ref.get_mut().download_object(data_id3, 1);
        dm_ref.get_mut().download_object(data_id4, 0);

        dm_ref.get_mut().download_object(data_id1, 0);
        tokio::time::sleep(Duration::new(3, 0)).await;

        let f = interface.take_finished_downloads(4);
        assert_eq!(f.get(&data_id1).unwrap().data(), obj1.data());
        assert_eq!(f.get(&data_id2).unwrap().data(), obj2.data());
        assert_eq!(f.get(&data_id3).unwrap().data(), obj3.data());
        assert_eq!(f.get(&data_id4).unwrap().data(), obj4.data());
        let r = interface.take_resolved_ids(4);
        assert_eq!(sorted_vec(r), vec![data_id1, data_id2, data_id3, data_id4]);
        interface.check_empty();
        let dm = dm_ref.get_mut();
        assert_eq!(dm.idle_connections().len(), 1);
        assert_eq!(dm.idle_connections().iter().next().unwrap().1.len(), 2);
    })
    .await;
}

#[tokio::test]
async fn download_close_idle_connection() {
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    set.run_until(async move {
        let upload = TestUploadInterface::new();
        let addr = start_test_upload_service(upload.clone()).await;
        let (dm_ref, interface) = test_download_manager();

        let data_id1 = DataObjectId::new(1.into(), 0.into());
        let obj1 = DataObjectRef::new(DataObject::new("abc".to_string(), vec![0]));
        upload.insert_object(data_id1, obj1);
        interface.register_hosts(&[data_id1], Some(addr));

        start_download_manager_process(dm_ref.clone(), 2, 3, Duration::from_secs(3));
        dm_ref.get_mut().download_object(data_id1, 0);
        tokio::time::sleep(Duration::new(2, 0)).await;
        interface.take_finished_downloads(1);
        interface.take_resolved_ids(1);
        interface.check_empty();
        {
            let dm = dm_ref.get_mut();
            let v: Vec<_> = dm.idle_connections().values().collect();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0].len(), 1);
        }
        tokio::time::sleep(Duration::new(5, 0)).await;
        {
            let dm = dm_ref.get_mut();
            assert!(dm.idle_connections().is_empty());
        }
    })
    .await;
}

#[test]
fn download_cancel() {
    env_logger::init();
    let set = tokio::task::LocalSet::new();
    set.run_until(async move {
        let data_id1 = DataObjectId::new(1.into(), 5.into());
        let data_id2 = DataObjectId::new(2.into(), 1.into());
        let data_id3 = DataObjectId::new(2.into(), 0.into());

        let (dm_ref, interface) = test_download_manager();
        interface.register_hosts(&[data_id1], None);

        let mut dm = dm_ref.get_mut();
        dm.download_object(data_id1, 2);
        dm.download_object(data_id2, 1);
        dm.download_object(data_id3, 0);
        tokio::time::sleep(Duration::new(1, 0)).await;
        interface.take_resolved_ids(2);
        interface.check_empty();
        let mut dm = dm_ref.get_mut();
        dm.cancel_download(data_id3);
        dm.cancel_download(data_id1);
        dm.cancel_download(data_id2);
        tokio::time::sleep(Duration::new(1, 0)).await;
        assert!(dm.queue().is_empty());
        assert!(dm.running_downloads().is_empty());
        assert!(dm.idle_connections().is_empty());
    });
}

// TODO: Aborting downloads
