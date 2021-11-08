use crate::messages::common::StdioDef;
use crate::tests::integration::utils::api::cancel;
use crate::tests::integration::utils::check_file_contents;
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::task::{
    simple_args, simple_task, GraphBuilder, TaskConfigBuilder,
};
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
                    .task(
                        TaskConfigBuilder::default()
                            .args(simple_args(&["/bin/hostname"]))
                            .keep(true),
                    )
                    .simple_task(&["/bin/hostname"])
                    .task(
                        TaskConfigBuilder::default()
                            .args(simple_args(&["bash", "-c", "echo 'hello'"]))
                            .stdout(StdioDef::File(stdout.clone()))
                            .stderr(StdioDef::File(stderr.clone())),
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
            .submit(vec![simple_task(&["/usr/bin/nonsense"], 1)])
            .await;
        let result = handler.wait(&ids).await;
        assert!(result.is_failed(ids[0]));

        let ids = handler
            .submit(vec![simple_task(&["bash", "c", "'exit 3'"], 2)])
            .await;
        let result = handler.wait(&ids).await;
        assert!(result.is_failed(ids[0]));

        let ids = handler
            .submit(vec![simple_task(&["/bin/hostname"], 3)])
            .await;
        handler.wait(&ids).await.assert_all_finished();
    })
    .await;
}

#[tokio::test]
async fn test_cancel_immediately() {
    run_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        let ids = handle.submit(vec![simple_task(&["sleep", "1"], 1)]).await;
        let response = cancel(&mut handle, &ids).await;
        assert_eq!(response.cancelled_tasks, vec![1]);
    })
    .await;
}

#[tokio::test]
async fn test_cancel_prev() {
    run_test(Default::default(), |mut handle| async move {
        handle.start_worker(Default::default()).await.unwrap();

        let ids = handle
            .submit(
                (1..100)
                    .map(|id| simple_task(&["sleep", "1"], id))
                    .collect(),
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

        handle.submit(vec![simple_task(&["/nonsense"], 1)]).await;
        sleep(Duration::from_millis(300)).await;

        let response = cancel(&mut handle, &[1]).await;
        assert_eq!(response.already_finished, vec![1]);

        assert!(handle.wait(&[1]).await.get(1).is_invalid());
    })
    .await;
}
