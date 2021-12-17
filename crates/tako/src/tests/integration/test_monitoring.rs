use super::utils::worker::WorkerConfigBuilder;
use crate::server::monitoring::{MonitoringEvent, MonitoringEventPayload};
use crate::tests::integration::utils::api::{get_events_after, get_overview};
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::worker::WorkerHandle;
use std::time::Duration;

#[tokio::test]
async fn test_hw_monitoring() {
    run_test(Default::default(), |mut handler| async move {
        let config =
            WorkerConfigBuilder::default().hw_state_poll_interval(Some(Duration::from_millis(10)));
        handler.start_worker(config).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let overview = get_overview(&mut handler).await;
        let usage = overview.worker_overviews[0]
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
        let wkr_handle = handler
            .start_worker(WorkerConfigBuilder::default())
            .await
            .unwrap();
        let wkr_handle_2 = handler
            .start_worker(WorkerConfigBuilder::default())
            .await
            .unwrap();

        let events = get_events_after(None, &mut handler).await;
        assert!(matches!(
            events[1].payload,
            MonitoringEventPayload::WorkerConnected(id, _) if id == wkr_handle.id
        ));

        assert!(matches!(
            events[0].payload,
            MonitoringEventPayload::WorkerConnected(id, _) if id == wkr_handle_2.id
        ));
    })
    .await;
}

#[tokio::test]
async fn test_worker_lost_event() {
    run_test(Default::default(), |mut handler| async move {
        let wkr_handle = handler
            .start_worker(WorkerConfigBuilder::default())
            .await
            .unwrap();
        handler.kill_worker(wkr_handle.id).await;

        let events = get_events_after(None, &mut handler).await;
        assert!(matches!(
            events[1].payload,
            MonitoringEventPayload::WorkerConnected(id, _) if id == wkr_handle.id
        ));

        assert!(matches!(
            events[0].payload,
            MonitoringEventPayload::WorkerLost(id, _) if id == wkr_handle.id
        ));
    })
    .await;
}

#[tokio::test]
async fn test_event_sourcing_id_filtering() {
    run_test(Default::default(), |mut handler| async move {
        let mut handles: Vec<WorkerHandle> = vec![];
        let worker_count: u32 = 5;
        for _ in 0..worker_count {
            handles.push(
                handler
                    .start_worker(WorkerConfigBuilder::default())
                    .await
                    .unwrap(),
            );
        }
        for handle in handles {
            handler.kill_worker(handle.id).await;
        }

        let after_id = 6;
        let events_before_after_id = get_events_after(Some(after_id), &mut handler)
            .await
            .into_iter()
            .filter(|evt| evt.id <= after_id)
            .collect::<Vec<MonitoringEvent>>();

        assert!(events_before_after_id.is_empty());
    })
    .await;
}
