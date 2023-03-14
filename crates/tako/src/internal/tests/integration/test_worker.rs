use std::time::Duration;

use tokio::time::sleep;

use crate::gateway::{LostWorkerReason, ToGatewayMessage};
use crate::internal::tests::integration::utils::api::{
    wait_for_task_start, wait_for_worker_connected, wait_for_worker_lost, wait_for_worker_overview,
};
use crate::internal::tests::integration::utils::server::run_test;
use crate::internal::tests::integration::utils::task::{simple_task, GraphBuilder};
use crate::try_wait_for_msg;

use super::utils::server::ServerConfigBuilder;
use super::utils::worker::WorkerConfigBuilder;

#[tokio::test]
async fn test_hw_monitoring() {
    run_test(Default::default(), |mut handler| async move {
        let config =
            WorkerConfigBuilder::default().send_overview_interval(Some(Duration::from_millis(10)));
        let worker = handler.start_worker(config).await.unwrap();

        let overview = wait_for_worker_overview(&mut handler, worker.id).await;
        let usage = overview
            .hw_state
            .as_ref()
            .unwrap()
            .state
            .cpu_usage
            .cpu_per_core_percent_usage
            .clone();

        assert_eq!(psutil::cpu::cpu_count(), usage.len() as u64);

        for pct in usage {
            assert!(pct >= 0.0);
            assert!(pct <= 100.0);
        }
    })
    .await;
}

#[tokio::test]
async fn test_worker_connected_event() {
    run_test(Default::default(), |mut handler| async move {
        for _ in 0..3 {
            let worker = handler
                .start_worker(WorkerConfigBuilder::default())
                .await
                .unwrap();

            wait_for_worker_connected(&mut handler, worker.id).await;
        }
    })
    .await;
}

#[tokio::test]
async fn test_worker_lost_killed() {
    run_test(Default::default(), |mut handler| async move {
        let worker = handler
            .start_worker(WorkerConfigBuilder::default())
            .await
            .unwrap();
        handler.kill_worker(worker.id).await;
        let reason = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(reason, LostWorkerReason::ConnectionLost));
    })
    .await;
}

#[tokio::test]
async fn test_worker_lost_stopped() {
    run_test(Default::default(), |mut handler| async move {
        let worker = handler
            .start_worker(WorkerConfigBuilder::default())
            .await
            .unwrap();
        handler.stop_worker(worker.id).await;
        let reason = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(reason, LostWorkerReason::Stopped));
    })
    .await;
}

#[tokio::test]
async fn test_worker_lost_heartbeat_expired() {
    run_test(Default::default(), |mut handler| async move {
        let config = WorkerConfigBuilder::default().heartbeat_interval(Duration::from_millis(200));
        let worker = handler.start_worker(config).await.unwrap();
        let _token = worker.pause().await;

        let reason = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(reason, LostWorkerReason::HeartbeatLost));
    })
    .await;
}

#[tokio::test]
async fn test_worker_lost_idle_timeout() {
    run_test(Default::default(), |mut handler| async move {
        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(200)));
        let worker = handler.start_worker(builder).await.unwrap();

        let reason = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(reason, LostWorkerReason::IdleTimeout));
    })
    .await;
}

#[tokio::test]
async fn test_worker_idle_timeout_stays_alive_with_tasks() {
    run_test(Default::default(), |mut handler| async move {
        handler
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .build(),
            )
            .await;

        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(250)));
        let worker = handler.start_worker(builder).await.unwrap();

        let msg = try_wait_for_msg!(handler, Duration::from_millis(900), ToGatewayMessage::LostWorker(_) => true);
        assert!(msg.is_none());
        wait_for_worker_lost(&mut handler, worker.id).await;
    })
    .await;
}

#[tokio::test]
#[should_panic]
async fn test_panic_on_worker_lost() {
    let config = ServerConfigBuilder::default().panic_on_worker_lost(true);
    let completion = run_test(config, |mut handle| async move {
        let worker = handle.start_worker(Default::default()).await.unwrap();
        handle.kill_worker(worker.id).await;
    })
    .await;
    completion.finish().await;
}

#[tokio::test]
async fn test_lost_worker_with_tasks_continue() {
    run_test(Default::default(), |mut handler| async move {
        let _workers = handler
            .start_workers(|| Default::default(), 2)
            .await
            .unwrap();

        let task_ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;
        let running_on = wait_for_task_start(&mut handler, task_ids[0]).await;

        handler.kill_worker(running_on).await;
        handler.wait(&[1]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_lost_worker_with_tasks_restarts() {
    run_test(Default::default(), |mut handle| async move {
        handle
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;

        for _ in 0..5 {
            let worker = handle.start_worker(Default::default()).await.unwrap();
            sleep(Duration::from_millis(300)).await;
            handle.kill_worker(worker.id).await;
        }

        let _worker_handle = handle.start_worker(Default::default()).await.unwrap();
        let r = handle.wait(&[1]).await;
        assert!(r.is_failed(1));
    })
    .await;
}
