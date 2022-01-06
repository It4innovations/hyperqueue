use std::time::Duration;

use tokio::time::sleep;

use crate::messages::gateway::LostWorkerReason;
use crate::server::monitoring::{MonitoringEvent, MonitoringEventPayload};
use crate::tests::integration::utils::api::{
    get_current_event_id, get_latest_overview, wait_for_event, wait_for_task_start,
    wait_for_worker_lost,
};
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::task::{simple_task, GraphBuilder};

use super::utils::server::ServerConfigBuilder;
use super::utils::worker::WorkerConfigBuilder;

#[tokio::test]
async fn test_hw_monitoring() {
    run_test(Default::default(), |mut handler| async move {
        let config =
            WorkerConfigBuilder::default().send_overview_interval(Some(Duration::from_millis(10)));
        let worker = handler.start_worker(config).await.unwrap();

        let overview = get_latest_overview(&mut handler, vec![worker.id]).await;
        let usage = overview[&worker.id]
            .hw_state
            .as_ref()
            .unwrap()
            .state
            .worker_cpu_usage
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

            wait_for_event(
                &mut handler,
                |event| {
                    matches!(
                        event.payload,
                        MonitoringEventPayload::WorkerConnected(id, _) if id == worker.id
                    )
                },
                Duration::from_secs(3),
                None,
            )
            .await
            .unwrap();
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
        let event = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(event.payload, MonitoringEventPayload::WorkerLost(id, LostWorkerReason::ConnectionLost) if id == worker.id));
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
        let event = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(event.payload, MonitoringEventPayload::WorkerLost(id, LostWorkerReason::Stopped) if id == worker.id));
    })
        .await;
}

#[tokio::test]
async fn test_worker_lost_heartbeat_expired() {
    run_test(Default::default(), |mut handler| async move {
        let config = WorkerConfigBuilder::default().heartbeat_interval(Duration::from_millis(200));
        let worker = handler.start_worker(config).await.unwrap();
        let _token = worker.pause().await;

        let event = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(event.payload, MonitoringEventPayload::WorkerLost(id, LostWorkerReason::HeartbeatLost) if id == worker.id));
    })
    .await;
}

#[tokio::test]
async fn test_worker_lost_idle_timeout() {
    run_test(Default::default(), |mut handler| async move {
        let builder = WorkerConfigBuilder::default().idle_timeout(Some(Duration::from_millis(200)));
        let worker = handler.start_worker(builder).await.unwrap();

        let event = wait_for_worker_lost(&mut handler, worker.id).await;
        assert!(matches!(event.payload, MonitoringEventPayload::WorkerLost(id, LostWorkerReason::IdleTimeout) if id == worker.id));
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

        let worker_lost_matcher = |event: &MonitoringEvent| match &event.payload {
            MonitoringEventPayload::WorkerLost(id, _) if *id == worker.id => true,
            _ => false,
        };

        let wait_duration = Duration::from_millis(900);
        let event_id = get_current_event_id(&mut handler).await;
        let event =
            wait_for_event(&mut handler, worker_lost_matcher, wait_duration, event_id).await;
        assert!(event.is_none());
        wait_for_worker_lost(&mut handler, worker.id).await;
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
        handle.wait(&[1]).await.assert_all_finished();
    })
    .await;
}
