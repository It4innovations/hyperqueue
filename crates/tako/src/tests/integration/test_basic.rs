use crate::messages::common::StdioDef;
use crate::tests::integration::utils::check_file_contents;
use crate::tests::integration::utils::server::run_test;
use crate::tests::integration::utils::task::{
    simple_args, simple_task, GraphBuilder, TaskConfigBuilder,
};

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
