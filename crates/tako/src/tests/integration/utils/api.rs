use super::macros::wait_for_msg;
use crate::messages::gateway::{
    CollectedOverview, FromGatewayMessage, OverviewRequest, ToGatewayMessage,
};
use crate::tests::integration::utils::server::ServerHandle;

pub async fn get_overview(handler: &mut ServerHandle) -> CollectedOverview {
    handler
        .send(FromGatewayMessage::GetOverview(OverviewRequest {
            enable_hw_overview: true,
        }))
        .await;
    wait_for_msg!(handler, ToGatewayMessage::Overview(overview) => overview)
}
