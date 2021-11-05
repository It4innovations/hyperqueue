use crate::messages::gateway::ToGatewayMessage;
use crate::tests::integration::env::{request_get_overview, run_test};

use super::env::wait_for_msg;
use super::env::ServerConfigBuilder;

#[tokio::test]
async fn test_server_without_workers() {
    let builder = ServerConfigBuilder::default();
    run_test(builder, |mut handler| async move {
        request_get_overview(&handler).await;
        let response = wait_for_msg!(handler, ToGatewayMessage::Overview(overview) => overview);
        assert!(response.worker_overviews.is_empty());
    })
    .await;
}
