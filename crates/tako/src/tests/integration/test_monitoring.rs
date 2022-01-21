use crate::server::events::MonitoringEvent;
use crate::tests::integration::utils::api::get_events_after;
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::worker::WorkerHandle;

use super::utils::worker::WorkerConfigBuilder;

#[tokio::test]
async fn test_filter_events_after_id() {
    run_test(Default::default(), |mut handler| async move {
        let mut handles: Vec<WorkerHandle> = vec![];
        for _ in 0..5 {
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
