use crate::control::{NewWorkerAllocationResponse, WorkerTypeQuery};
use crate::internal::tests::integration::utils::check_file_contents;
use crate::internal::tests::integration::utils::server::{ServerHandle, run_server_test};
use crate::internal::tests::integration::utils::task::ResourceRequestConfigBuilder;
use crate::internal::tests::integration::utils::task::{
    GraphBuilder, TaskConfigBuilder, simple_args, simple_task,
};
use crate::program::StdioDef;
use crate::resources::ResourceDescriptor;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_submit_simple_task_ok() {
    run_server_test(Default::default(), |mut handler| async move {
        let worker = handler.start_worker(Default::default()).await.unwrap();

        let stdout = worker.workdir.join("test.out");
        let stderr = worker.workdir.join("test.err");

        let ids = handler
            .submit(
                GraphBuilder::default()
                    .simple_task(&["uname"])
                    .simple_task(&["uname"])
                    .task(
                        TaskConfigBuilder::default()
                            .args(simple_args(&["bash", "-c", "echo 'hello'"]))
                            .stdout(StdioDef::File {
                                path: stdout.clone(),
                                on_close: Default::default(),
                            })
                            .stderr(StdioDef::File {
                                path: stderr.clone(),
                                on_close: Default::default(),
                            }),
                    )
                    .build(),
            )
            .await;
        handler.wait(&ids).await.assert_all_finished();
        assert!(stdout.exists());
        assert!(stderr.exists());
        check_file_contents(&stdout, b"hello\n");
    })
    .await;
}

#[tokio::test]
async fn test_submit_simple_task_fail() {
    run_server_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();

        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(
                &["/usr/bin/nonsense"],
                1,
            )))
            .await;
        handler.wait(&ids).await.assert_all_failed();

        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(
                &["bash", "c", "'exit 3'"],
                2,
            )))
            .await;
        handler.wait(&ids).await.assert_all_failed();

        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["uname"], 3)))
            .await;
        handler.wait(&ids).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_task_time_limit_fail() {
    run_server_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        handle
            .submit(GraphBuilder::singleton(
                TaskConfigBuilder::default()
                    .args(simple_args(&["sleep", "2"]))
                    .time_limit(Some(Duration::from_millis(600))),
            ))
            .await;
        handle
            .wait(&[1])
            .await
            .get_state(1)
            .assert_error_message("Time limit reached");
    })
    .await;
}

#[tokio::test]
async fn test_task_time_limit_pass() {
    run_server_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        handle
            .submit(GraphBuilder::singleton(
                TaskConfigBuilder::default()
                    .args(simple_args(&["sleep", "1"]))
                    .time_limit(Some(Duration::from_millis(1600))),
            ))
            .await;
        handle.wait(&[1]).await.assert_all_finished();
    })
    .await;
}

fn query_helper(
    handler: &mut ServerHandle,
    worker_queries: &[WorkerTypeQuery],
) -> NewWorkerAllocationResponse {
    handler.server_ref.new_worker_query(worker_queries).unwrap()
}

#[tokio::test]
async fn test_query_no_output_immediate_call() {
    run_server_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;
        let msg = query_helper(
            &mut handler,
            &[WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(12),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 2,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(msg.single_node_workers_per_query, vec![0]);
        assert!(msg.multi_node_allocations.is_empty());
        handler.wait(&ids).await;
    })
    .await;
}

#[tokio::test]
async fn test_query_no_output_delayed_call() {
    run_server_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;
        sleep(Duration::from_secs(1)).await;
        let msg = query_helper(
            &mut handler,
            &[WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(12),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 2,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(msg.single_node_workers_per_query, vec![0]);
        assert!(msg.multi_node_allocations.is_empty());
        handler.wait(&ids).await;
    })
    .await;
}

#[tokio::test]
async fn test_query_new_workers_delayed_call() {
    run_server_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let _ = handler
            .submit(GraphBuilder::singleton(
                simple_task(&["sleep", "1"], 1)
                    .resources(ResourceRequestConfigBuilder::default().cpus(5)),
            ))
            .await;
        sleep(Duration::from_secs(1)).await;
        let msg = query_helper(
            &mut handler,
            &[WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(12),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 2,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(msg.single_node_workers_per_query, vec![1]);
        assert!(msg.multi_node_allocations.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_query_new_workers_immediate() {
    run_server_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let _ = handler
            .submit(GraphBuilder::singleton(
                simple_task(&["sleep", "1"], 1)
                    .resources(ResourceRequestConfigBuilder::default().cpus(5)),
            ))
            .await;
        let msg = query_helper(
            &mut handler,
            &[WorkerTypeQuery {
                partial: false,
                descriptor: ResourceDescriptor::simple_cpus(12),
                time_limit: None,
                max_sn_workers: 2,
                max_workers_per_allocation: 2,
                min_utilization: 0.0,
            }],
        );
        assert_eq!(msg.single_node_workers_per_query, vec![1]);
        assert!(msg.multi_node_allocations.is_empty());
    })
    .await;
}
