use super::utils::worker::WorkerConfigBuilder;
use crate::tests::integration::utils::api::get_overview;
use crate::tests::integration::utils::server::run_test;
use std::time::Duration;

#[tokio::test]
async fn test_hw_monitoring() {
    run_test(Default::default(), |mut handler| async move {
        let config =
            WorkerConfigBuilder::default().hw_state_poll_interval(Some(Duration::from_millis(10)));
        handler.start_worker(config).await;

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
