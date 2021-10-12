use tako::messages::common::MemoryStats;
use tako::messages::gateway::CollectedOverview;
use tako::messages::gateway::OverviewRequest;

use crate::common::error::HqError;
use crate::rpc_call;
use crate::transfer::connection::ClientConnection;
use crate::transfer::messages::{FromClientMessage, ToClientMessage};

pub async fn get_hw_overview(
    connection: &mut ClientConnection,
) -> Result<CollectedOverview, HqError> {
    let response = rpc_call!(
        connection,
        FromClientMessage::Overview(OverviewRequest {
            enable_hw_overview: true
        }),
        ToClientMessage::OverviewResponse(response) => response
    )
    .await;
    response
}

pub fn calculate_memory_usage_percent(memory_stats: MemoryStats) -> u64 {
    if memory_stats.total == 0 {
        return 0;
    }
    (((memory_stats.free as f64) / (memory_stats.total as f64)) * 100.00) as u64
}
