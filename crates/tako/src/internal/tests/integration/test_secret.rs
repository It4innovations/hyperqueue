use crate::internal::tests::integration::utils::server::ServerConfigBuilder;
use crate::internal::tests::integration::utils::server::{ServerSecretKey, run_server_test};
use crate::internal::tests::integration::utils::worker::{WorkerConfigBuilder, WorkerSecretKey};
use crate::internal::tests::utils::expect_error_message;
use orion::auth::SecretKey;

#[tokio::test]
async fn test_no_auth() {
    let config = ServerConfigBuilder::default().secret_key(ServerSecretKey::Custom(None));
    run_server_test(config, |mut handler| async move {
        let config = WorkerConfigBuilder::default().secret_key(WorkerSecretKey::Custom(None));
        handler.start_worker(config).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_server_auth_worker_no_auth() {
    let config = ServerConfigBuilder::default().secret_key(ServerSecretKey::AutoGenerate);
    run_server_test(config, |mut handler| async move {
        let config = WorkerConfigBuilder::default().secret_key(WorkerSecretKey::Custom(None));
        expect_error_message(
            handler.start_worker(config).await,
            "Authentication failed: Peer requests authentication",
        );
    })
    .await;
}

#[tokio::test]
async fn test_server_no_auth_worker_auth() {
    let config = ServerConfigBuilder::default().secret_key(ServerSecretKey::Custom(None));
    run_server_test(config, |mut handler| async move {
        let config = WorkerConfigBuilder::default()
            .secret_key(WorkerSecretKey::Custom(Some(Default::default())));
        expect_error_message(
            handler.start_worker(config).await,
            "Authentication failed: Peer does not support authentication",
        );
    })
    .await;
}

#[tokio::test]
async fn test_auth_same_key() {
    let key_bytes: Vec<_> = (0u8..32u8).collect();
    let config = ServerConfigBuilder::default().secret_key(ServerSecretKey::Custom(Some(
        SecretKey::from_slice(&key_bytes).unwrap(),
    )));
    run_server_test(config, |mut handler| async move {
        let config = WorkerConfigBuilder::default().secret_key(WorkerSecretKey::Custom(Some(
            SecretKey::from_slice(&key_bytes).unwrap(),
        )));
        handler.start_worker(config).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_auth_different_key() {
    let mut key_bytes: Vec<_> = (0u8..32u8).collect();
    let config = ServerConfigBuilder::default().secret_key(ServerSecretKey::Custom(Some(
        SecretKey::from_slice(&key_bytes).unwrap(),
    )));

    key_bytes[0] = 100;

    run_server_test(config, |mut handler| async move {
        let config = WorkerConfigBuilder::default().secret_key(WorkerSecretKey::Custom(Some(
            SecretKey::from_slice(&key_bytes).unwrap(),
        )));
        expect_error_message(
            handler.start_worker(config).await,
            "Cannot verify challenge",
        );
    })
    .await;
}
