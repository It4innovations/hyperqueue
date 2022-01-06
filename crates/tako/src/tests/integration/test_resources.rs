use std::time::{Duration, Instant};

use crate::common::resources::CpuRequest;
use tokio::time::sleep;

use crate::tests::integration::utils::api::{get_latest_overview, wait_for_task_start};
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::task::{
    simple_args, simple_task, GraphBuilder as GB, GraphBuilder, ResourceRequestConfigBuilder as RR,
    TaskConfigBuilder as TC,
};
use crate::tests::integration::utils::worker::{cpus, numa_cpus, WorkerConfigBuilder as WC};
use crate::WorkerId;

#[tokio::test]
async fn test_submit_2_sleeps_on_1() {
    run_test(Default::default(), |mut handler| async move {
        handler
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .task(simple_task(&["sleep", "1"], 2))
                    .build(),
            )
            .await;
        let config = WC::default().send_overview_interval(Some(Duration::from_millis(10)));

        let worker = handler.start_worker(config).await.unwrap();

        wait_for_task_start(&mut handler, 1).await;
        let overview1 = get_latest_overview(&mut handler, vec![worker.id]).await;
        assert_eq!(overview1[&worker.id].running_tasks.len(), 1);

        sleep(Duration::from_millis(100)).await;
        let overview2 = get_latest_overview(&mut handler, vec![worker.id]).await;
        assert_eq!(overview2[&worker.id].running_tasks.len(), 1);
        assert_eq!(
            overview1[&worker.id].running_tasks,
            overview2[&worker.id].running_tasks
        );

        wait_for_task_start(&mut handler, 2).await;
        let overview3 = get_latest_overview(&mut handler, vec![worker.id]).await;
        assert_eq!(overview3[&worker.id].running_tasks.len(), 1);
        assert_ne!(
            overview2[&worker.id].running_tasks,
            overview3[&worker.id].running_tasks
        );
    })
    .await;
}

#[tokio::test]
async fn test_submit_2_sleeps_on_2() {
    run_test(Default::default(), |mut handler| async move {
        handler
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .task(simple_task(&["sleep", "1"], 2))
                    .build(),
            )
            .await;

        let worker = handler
            .start_worker(
                WC::default()
                    .send_overview_interval(Some(Duration::from_millis(100)))
                    .resources(cpus(2)),
            )
            .await
            .unwrap();

        wait_for_task_start(&mut handler, 1).await;
        wait_for_task_start(&mut handler, 2).await;
        let overview1 = get_latest_overview(&mut handler, vec![worker.id]).await;
        assert_eq!(overview1[&worker.id].running_tasks.len(), 2);

        handler.wait(&[1, 2]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_submit_2_sleeps_on_separated_2() {
    run_test(Default::default(), |mut handler| async move {
        handler
            .submit(
                GraphBuilder::default()
                    .task(simple_task(&["sleep", "1"], 1))
                    .task(simple_task(&["sleep", "1"], 2))
                    .build(),
            )
            .await;

        let workers = handler
            .start_workers(|| Default::default(), 3)
            .await
            .unwrap();
        let worker_ids: Vec<WorkerId> = workers.iter().map(|x| x.id).collect();

        wait_for_task_start(&mut handler, 1).await;
        wait_for_task_start(&mut handler, 2).await;

        let overview = get_latest_overview(&mut handler, worker_ids).await;
        let empty_workers: Vec<_> = overview
            .iter()
            .map(|(_, overview)| overview)
            .filter(|overview| overview.running_tasks.is_empty())
            .collect();
        assert_eq!(empty_workers.len(), 1);

        for (worker_id, overview) in &overview {
            if *worker_id != empty_workers[0].id {
                assert_eq!(overview.running_tasks.len(), 1);
            }
        }

        handler.wait(&[1, 2]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_submit_sleeps_more_cpus1() {
    run_test(Default::default(), |mut handler| async move {
        let rq1 = RR::default().cpus(CpuRequest::Compact(3));
        let rq2 = RR::default().cpus(CpuRequest::Compact(2));
        handler
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

        let wkr_handles = handler
            .start_workers(|| WC::default().resources(cpus(4)), 2)
            .await
            .unwrap();

        let worker_ids: Vec<WorkerId> = wkr_handles.iter().map(|x| x.id).collect();

        wait_for_task_start(&mut handler, 1).await;
        wait_for_task_start(&mut handler, 2).await;
        let overview = get_latest_overview(&mut handler, worker_ids).await;
        let mut rts: Vec<_> = overview
            .iter()
            .map(|(_, overview)| overview.running_tasks.len())
            .collect();
        rts.sort_unstable();
        assert_eq!(rts, vec![1, 2]);
        handler.wait(&[1, 2]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_submit_sleeps_more_cpus2() {
    run_test(Default::default(), |mut handler| async move {
        let rq1 = RR::default().cpus(CpuRequest::Compact(3));
        let rq2 = RR::default().cpus(CpuRequest::Compact(2));
        let t = |rq: &RR| {
            TC::default()
                .args(simple_args(&["sleep", "1"]))
                .resources(rq.clone())
        };

        handler
            .start_workers(|| WC::default().resources(cpus(4)), 2)
            .await
            .unwrap();

        let start = Instant::now();
        let ids = handler
            .submit(
                GB::default()
                    .task(t(&rq1))
                    .task(t(&rq2))
                    .task(t(&rq2))
                    .task(t(&rq1))
                    .build(),
            )
            .await;
        handler.wait(&ids).await.assert_all_finished();

        let duration = start.elapsed().as_millis();
        assert!(duration >= 2000);
        assert!(duration <= 2300);
    })
    .await;
}

#[tokio::test]
async fn test_submit_sleeps_more_cpus3() {
    run_test(Default::default(), |mut handler| async move {
        let rq1 = RR::default().cpus(CpuRequest::Compact(3));
        let rq2 = RR::default().cpus(CpuRequest::Compact(2));
        let t = |rq: &RR| {
            TC::default()
                .args(simple_args(&["sleep", "1"]))
                .resources(rq.clone())
        };

        handler
            .start_workers(|| WC::default().resources(cpus(5)), 2)
            .await
            .unwrap();

        let start = Instant::now();
        let ids = handler
            .submit(
                GB::default()
                    .task(t(&rq1))
                    .task(t(&rq2))
                    .task(t(&rq2))
                    .task(t(&rq1))
                    .build(),
            )
            .await;
        handler.wait(&ids).await.assert_all_finished();

        let duration = start.elapsed().as_millis();
        assert!(duration >= 1000);
        assert!(duration <= 2300);
    })
    .await;
}

#[tokio::test]
async fn test_force_compact() {
    run_test(Default::default(), |mut handler| async move {
        let rq = RR::default().cpus(CpuRequest::ForceCompact(4));

        handler
            .start_workers(|| WC::default().resources(numa_cpus(2, 2)), 2)
            .await
            .unwrap();

        handler
            .submit(
                GB::default()
                    .task(TC::default().args(simple_args(&["hostname"])).resources(rq))
                    .build(),
            )
            .await;
        handler.wait(&[1]).await.assert_all_finished();
    })
    .await;
}
