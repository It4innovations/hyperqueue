use crate::gateway::{
    FromGatewayMessage, NewWorkerAllocationResponse, NewWorkerQuery, ToGatewayMessage,
    WorkerTypeQuery,
};
use crate::internal::tests::integration::utils::api::cancel;
use crate::internal::tests::integration::utils::check_file_contents;
use crate::internal::tests::integration::utils::server::{run_test, ServerHandle};
use crate::internal::tests::integration::utils::task::ResourceRequestConfigBuilder;
use crate::internal::tests::integration::utils::task::{
    simple_args, simple_task, GraphBuilder, TaskConfigBuilder,
};
use crate::program::StdioDef;
use crate::resources::ResourceDescriptor;
use crate::{wait_for_msg, TaskId};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_submit_simple_task_ok() {
    run_test(Default::default(), |mut handler| async move {
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
    run_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();

        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(
                &["/usr/bin/nonsense"],
                1,
            )))
            .await;
        let result = handler.wait(&ids).await;
        assert!(result.is_failed(ids[0]));

        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(
                &["bash", "c", "'exit 3'"],
                2,
            )))
            .await;
        let result = handler.wait(&ids).await;
        assert!(result.is_failed(ids[0]));

        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["uname"], 3)))
            .await;
        handler.wait(&ids).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_cancel_immediately() {
    run_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        let ids = handle
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;
        let response = cancel(&mut handle, &ids).await;
        assert_eq!(response.cancelled_tasks, vec![TaskId::new_test(1)]);
    })
    .await;
}

#[tokio::test]
async fn test_cancel_prev() {
    run_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        let ids = handle
            .submit(
                GraphBuilder::default()
                    .tasks((1..100).map(|id| simple_task(&["sleep", "1"], id)))
                    .build(),
            )
            .await;
        let mut to_cancel = ids[..72].to_vec();
        to_cancel.extend(&ids[73..]);
        cancel(&mut handle, &to_cancel).await;

        handle.wait(&[ids[72]]).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_cancel_error_task() {
    run_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        handle
            .submit(GraphBuilder::singleton(simple_task(&["/nonsense"], 1)))
            .await;
        sleep(Duration::from_millis(300)).await;

        let response = cancel(&mut handle, &[1]).await;
        assert_eq!(response.already_finished, vec![TaskId::new_test(1)]);
    })
    .await;
}

#[tokio::test]
async fn test_task_time_limit_fail() {
    run_test(Default::default(), |mut handle| async move {
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
            .get(1)
            .assert_error_message("Time limit reached");
    })
    .await;
}

#[tokio::test]
async fn test_task_time_limit_pass() {
    run_test(Default::default(), |mut handle| async move {
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

async fn query_helper(
    handler: &mut ServerHandle,
    worker_queries: Vec<WorkerTypeQuery>,
) -> NewWorkerAllocationResponse {
    handler
        .send(FromGatewayMessage::NewWorkerQuery(NewWorkerQuery {
            worker_queries,
        }))
        .await;
    wait_for_msg!(handler, ToGatewayMessage::NewWorkerAllocationQueryResponse(msg) => msg)
}

#[tokio::test]
async fn test_query_no_output_immediate_call() {
    run_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;
        let msg = query_helper(
            &mut handler,
            vec![WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(12),
                max_sn_workers: 2,
                max_worker_per_allocation: 2,
            }],
        )
        .await;
        assert_eq!(msg.single_node_allocations, vec![0]);
        assert!(msg.multi_node_allocations.is_empty());
        handler.wait(&ids).await;
    })
    .await;
}

#[tokio::test]
async fn test_query_no_output_delayed_call() {
    run_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let ids = handler
            .submit(GraphBuilder::singleton(simple_task(&["sleep", "1"], 1)))
            .await;
        sleep(Duration::from_secs(1)).await;
        let msg = query_helper(
            &mut handler,
            vec![WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(12),
                max_sn_workers: 2,
                max_worker_per_allocation: 2,
            }],
        )
        .await;
        assert_eq!(msg.single_node_allocations, vec![0]);
        assert!(msg.multi_node_allocations.is_empty());
        handler.wait(&ids).await;
    })
    .await;
}

#[tokio::test]
async fn test_query_new_workers_delayed_call() {
    run_test(Default::default(), |mut handler| async move {
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
            vec![WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(12),
                max_sn_workers: 2,
                max_worker_per_allocation: 2,
            }],
        )
        .await;
        assert_eq!(msg.single_node_allocations, vec![1]);
        assert!(msg.multi_node_allocations.is_empty());
    })
    .await;
}

#[tokio::test]
async fn test_query_new_workers_immediate() {
    run_test(Default::default(), |mut handler| async move {
        handler.start_worker(Default::default()).await.unwrap();
        let _ = handler
            .submit(GraphBuilder::singleton(
                simple_task(&["sleep", "1"], 1)
                    .resources(ResourceRequestConfigBuilder::default().cpus(5)),
            ))
            .await;
        let msg = query_helper(
            &mut handler,
            vec![WorkerTypeQuery {
                descriptor: ResourceDescriptor::simple(12),
                max_sn_workers: 2,
                max_worker_per_allocation: 2,
            }],
        )
        .await;
        assert_eq!(msg.single_node_allocations, vec![1]);
        assert!(msg.multi_node_allocations.is_empty());
    })
    .await;
}
