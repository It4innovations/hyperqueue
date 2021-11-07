use super::utils::server::ServerConfigBuilder;
use super::utils::worker::WorkerConfigBuilder;
use crate::tests::integration::utils::api::get_overview;
use crate::tests::integration::utils::server::run_test;
use std::time::Duration;

#[tokio::test]
async fn test_overview_server_without_workers() {
    run_test(Default::default(), |mut handler| async move {
        let overview = get_overview(&mut handler).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_overview_single_worker() {
    run_test(Default::default(), |mut handler| async move {
        let worker = handler.start_worker(Default::default()).await.unwrap();
        let overview = get_overview(&mut handler).await;
        assert_eq!(overview.worker_overviews.len(), 1);
        assert_eq!(overview.worker_overviews[0].id, worker.id);
    })
    .await;
}

#[tokio::test]
async fn test_worker_idle_timeout_no_tasks() {
    run_test(Default::default(), |mut handler| async move {
        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(10)));
        handler.start_worker(builder).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        let handler = get_overview(&mut handler).await;
        assert!(handler.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_kill_worker() {
    run_test(Default::default(), |mut handler| async move {
        let worker = handler.start_worker(Default::default()).await.unwrap();
        handler.kill_worker(worker.id).await;

        let handler = get_overview(&mut handler).await;
        assert!(handler.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
#[should_panic]
async fn test_panic_on_worker_lost() {
    let config = ServerConfigBuilder::default().panic_on_worker_lost(true);
    let mut completion = run_test(config, |mut handler| async move {
        let worker = handler.start_worker(Default::default()).await.unwrap();
        handler.kill_worker(worker.id).await;
    })
    .await;
    completion.finish_rpc().await;
    completion.finish().await;
}

#[tokio::test]
async fn test_worker_heartbeat_expired() {
    run_test(Default::default(), |mut handler| async move {
        let config = WorkerConfigBuilder::default().heartbeat_interval(Duration::from_millis(200));
        let worker = handler.start_worker(config).await.unwrap();
        worker.pause().await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        let overview = get_overview(&mut handler).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}
