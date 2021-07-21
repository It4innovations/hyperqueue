use tokio::time::sleep;

pub use state::AutoAllocState;

use crate::server::state::StateRef;

mod state;

pub async fn autoalloc_check(state_ref: &StateRef) {
    log::debug!("Running autoalloc");
}

pub async fn autoalloc_process(state_ref: StateRef) {
    loop {
        autoalloc_check(&state_ref).await;
        let duration = state_ref.get().get_autoalloc_state().refresh_interval();
        sleep(duration).await;
    }
}
