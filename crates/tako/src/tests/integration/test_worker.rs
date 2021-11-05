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
        let worker = handler.start_worker(Default::default()).await;
        let overview = get_overview(&mut handler).await;
        assert_eq!(overview.worker_overviews.len(), 1);
        assert_eq!(overview.worker_overviews[0].id, worker.id);
    })
    .await;
}

#[tokio::test]
async fn test_worker_timeout() {
    run_test(Default::default(), |mut handler| async move {
        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(10)));
        handler.start_worker(builder).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let handler = get_overview(&mut handler).await;
        assert!(handler.worker_overviews.is_empty());
    })
    .await;
}
