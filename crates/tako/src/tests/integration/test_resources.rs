use std::time::{Duration, Instant};

use crate::common::resources::CpuRequest;
use tokio::time::sleep;

use crate::tests::integration::utils::api::get_overview;
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::task::{
    simple_args, simple_task, GraphBuilder as GB, GraphBuilder, ResourceRequestConfigBuilder as RR,
    TaskConfigBuilder as TC,
};
use crate::tests::integration::utils::worker::{cpus, numa_cpus, WorkerConfigBuilder as WC};

#[tokio::test]
async fn test_submit_2_sleeps_on_1() {
    run_test(Default::default(), |mut handle| async move {
        handle
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .task(simple_task(&["sleep", "1"], 2))
                    .build(),
            )
            .await;

        handle.start_worker(Default::default()).await.unwrap();

        sleep(Duration::from_millis(200)).await;
        let overview1 = get_overview(&mut handle).await;
        assert_eq!(overview1.worker_overviews[0].running_tasks.len(), 1);

        sleep(Duration::from_millis(500)).await;
        let overview2 = get_overview(&mut handle).await;
        assert_eq!(overview2.worker_overviews[0].running_tasks.len(), 1);
        assert_eq!(
            overview1.worker_overviews[0].running_tasks,
            overview2.worker_overviews[0].running_tasks
        );

        sleep(Duration::from_millis(500)).await;
        let overview3 = get_overview(&mut handle).await;
        assert_eq!(overview3.worker_overviews[0].running_tasks.len(), 1);
        assert_ne!(
            overview2.worker_overviews[0].running_tasks,
            overview3.worker_overviews[0].running_tasks
        );
    })
    .await;
}

#[tokio::test]
async fn test_submit_2_sleeps_on_2() {
    run_test(Default::default(), |mut handle| async move {
        handle
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .task(simple_task(&["sleep", "1"], 2))
                    .build(),
            )
            .await;

        handle
            .start_worker(WC::default().resources(cpus(2)))
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        let overview1 = get_overview(&mut handle).await;
        assert_eq!(overview1.worker_overviews[0].running_tasks.len(), 2);

        handle.wait(&[1, 2]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_submit_2_sleeps_on_separated_2() {
    run_test(Default::default(), |mut handle| async move {
        handle
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .task(simple_task(&["sleep", "1"], 2))
                    .build(),
            )
            .await;

        handle
            .start_workers(|| Default::default(), 3)
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;

        let overview = get_overview(&mut handle).await;
        let empty_workers: Vec<_> = overview
            .worker_overviews
            .iter()
            .filter(|w| w.running_tasks.is_empty())
            .collect();
        assert_eq!(empty_workers.len(), 1);

        for worker in &overview.worker_overviews {
            if worker.id != empty_workers[0].id {
                assert_eq!(worker.running_tasks.len(), 1);
            }
        }

        handle.wait(&[1, 2]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_submit_sleeps_more_cpus1() {
    run_test(Default::default(), |mut handle| async move {
        let rq1 = RR::default().cpus(CpuRequest::Compact(3));
        let rq2 = RR::default().cpus(CpuRequest::Compact(2));
        handle
            .submit(
                GB::default()
                    .task(
                        TC::default()
                            .args(simple_args(&["sleep", "1"]))
                            .resources(rq1),
                    )
                    .task(
                        TC::default()
                            .args(simple_args(&["sleep", "1"]))
                            .resources(rq2.clone()),
                    )
                    .task(
                        TC::default()
                            .args(simple_args(&["sleep", "1"]))
                            .resources(rq2),
                    )
                    .build(),
            )
            .await;

        handle
            .start_workers(|| WC::default().resources(cpus(4)), 2)
            .await
            .unwrap();

        sleep(Duration::from_millis(200)).await;
        let overview = get_overview(&mut handle).await;
        let mut rts: Vec<_> = overview
            .worker_overviews
            .iter()
            .map(|o| o.running_tasks.len())
            .collect();
        rts.sort_unstable();
        assert_eq!(rts, vec![1, 2]);
        handle.wait(&[1, 2]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_submit_sleeps_more_cpus2() {
    run_test(Default::default(), |mut handle| async move {
        let rq1 = RR::default().cpus(CpuRequest::Compact(3));
        let rq2 = RR::default().cpus(CpuRequest::Compact(2));
        let t = |rq: &RR| {
            TC::default()
                .args(simple_args(&["sleep", "1"]))
                .resources(rq.clone())
        };

        handle
            .start_workers(|| WC::default().resources(cpus(4)), 2)
            .await
            .unwrap();

        let start = Instant::now();
        let ids = handle
            .submit(
                GB::default()
                    .task(t(&rq1))
                    .task(t(&rq2))
                    .task(t(&rq2))
                    .task(t(&rq1))
                    .build(),
            )
            .await;
        handle.wait(&ids).await.assert_all_finished();

        let duration = start.elapsed().as_millis();
        assert!(duration >= 2000);
        assert!(duration <= 2300);
    })
    .await;
}

#[tokio::test]
async fn test_submit_sleeps_more_cpus3() {
    run_test(Default::default(), |mut handle| async move {
        let rq1 = RR::default().cpus(CpuRequest::Compact(3));
        let rq2 = RR::default().cpus(CpuRequest::Compact(2));
        let t = |rq: &RR| {
            TC::default()
                .args(simple_args(&["sleep", "1"]))
                .resources(rq.clone())
        };

        handle
            .start_workers(|| WC::default().resources(cpus(5)), 2)
            .await
            .unwrap();

        let start = Instant::now();
        let ids = handle
            .submit(
                GB::default()
                    .task(t(&rq1))
                    .task(t(&rq2))
                    .task(t(&rq2))
                    .task(t(&rq1))
                    .build(),
            )
            .await;
        handle.wait(&ids).await.assert_all_finished();

        let duration = start.elapsed().as_millis();
        assert!(duration >= 1000);
        assert!(duration <= 2300);
    })
    .await;
}

#[tokio::test]
async fn test_force_compact() {
    run_test(Default::default(), |mut handle| async move {
        let rq = RR::default().cpus(CpuRequest::ForceCompact(4));

        handle
            .start_workers(|| WC::default().resources(numa_cpus(2, 2)), 2)
            .await
            .unwrap();

        handle
            .submit(
                GB::default()
                    .task(TC::default().args(simple_args(&["hostname"])).resources(rq))
                    .build(),
            )
            .await;
        handle.wait(&[1]).await.assert_all_finished();
    })
    .await;
}
