use std::time::Duration;

use tokio::time::sleep;

use crate::tests::integration::utils::api::get_overview;
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::task::{GraphBuilder, simple_task};
use crate::WorkerId;

use super::utils::server::ServerConfigBuilder;
use super::utils::worker::WorkerConfigBuilder;

#[tokio::test]
async fn test_overview_server_without_workers() {
    run_test(Default::default(), |mut handle| async move {
        let overview = get_overview(&mut handle).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_overview_single_worker() {
    run_test(Default::default(), |mut handle| async move {
        let worker = handle.start_worker(Default::default()).await.unwrap();
        let overview = get_overview(&mut handle).await;
        assert_eq!(overview.worker_overviews.len(), 1);
        assert_eq!(overview.worker_overviews[0].id, worker.id);
    })
    .await;
}

#[tokio::test]
async fn test_worker_idle_timeout_no_tasks() {
    run_test(Default::default(), |mut handle| async move {
        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(10)));
        handle.start_worker(builder).await.unwrap();
        sleep(Duration::from_millis(500)).await;

        let overview = get_overview(&mut handle).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_worker_idle_timeout_tasks() {
    run_test(Default::default(), |mut handle| async move {
        handle.submit(GraphBuilder::default().task(simple_task(&["sleep", "1"], 1)).build()).await;

        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(250)));
        handle.start_worker(builder).await.unwrap();

        sleep(Duration::from_millis(500)).await;

        let overview = get_overview(&mut handle).await;
        assert_eq!(overview.worker_overviews.len(), 1);

        sleep(Duration::from_secs(2)).await;

        let overview = get_overview(&mut handle).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_kill_worker() {
    run_test(Default::default(), |mut handle| async move {
        let worker = handle.start_worker(Default::default()).await.unwrap();
        handle.kill_worker(worker.id).await;

        let overview = get_overview(&mut handle).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
#[should_panic]
async fn test_panic_on_worker_lost() {
    let config = ServerConfigBuilder::default().panic_on_worker_lost(true);
    let mut completion = run_test(config, |mut handle| async move {
        let worker = handle.start_worker(Default::default()).await.unwrap();
        handle.kill_worker(worker.id).await;
    })
    .await;
    completion.finish_rpc().await;
    completion.finish().await;
}

#[tokio::test]
async fn test_worker_heartbeat_expired() {
    run_test(Default::default(), |mut handle| async move {
        let config = WorkerConfigBuilder::default().heartbeat_interval(Duration::from_millis(200));
        let worker = handle.start_worker(config).await.unwrap();
        worker.pause().await;

        sleep(Duration::from_millis(500)).await;
        let overview = get_overview(&mut handle).await;
        assert!(overview.worker_overviews.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_lost_worker_with_tasks_continue() {
    run_test(Default::default(), |mut handle| async move {
        handle
            .start_workers(|| Default::default(), 2)
            .await
            .unwrap();

        handle.submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1))).await;

        sleep(Duration::from_millis(400)).await;

        let overview = get_overview(&mut handle).await;
        let mut killed_worker = false;
        for worker in overview.worker_overviews {
            if !worker.running_tasks.is_empty() {
                handle.kill_worker(worker.id).await;
                killed_worker = true;
                break;
            }
        }
        assert!(killed_worker);

        handle.wait(&[1]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_lost_worker_with_tasks_restarts() {
    run_test(Default::default(), |mut handle| async move {
        handle.submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1))).await;

        for _ in 0..5 {
            let worker = handle.start_worker(Default::default()).await.unwrap();
            sleep(Duration::from_millis(300)).await;
            handle.kill_worker(worker.id).await;
        }

        handle.start_worker(Default::default()).await.unwrap();
        handle.wait(&[1]).await.assert_all_finished();
        let overview = get_overview(&mut handle).await;
        assert_eq!(overview.worker_overviews.len(), 1);
        assert_eq!(overview.worker_overviews[0].id, WorkerId(6));
    })
    .await;
}
