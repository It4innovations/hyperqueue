use crate::server::Senders;
use crate::server::client::forget_jobs;
use crate::server::state::StateRef;
use chrono::{TimeDelta, Utc};
use std::time::Duration;
use tako::JobId;

pub async fn auto_forget_process(state_ref: StateRef, senders: Senders, max_age: Duration) {
    let max_age_delta = TimeDelta::from_std(max_age).unwrap_or(TimeDelta::MAX);
    let check_period = (max_age / 10).clamp(Duration::from_secs(1), Duration::from_secs(3600));
    let mut interval = tokio::time::interval(check_period);
    loop {
        interval.tick().await;
        let forgotten_jobs = {
            let mut state = state_ref.get_mut();
            let now = Utc::now();
            let job_ids: Vec<JobId> = state
                .jobs()
                .filter(|job| {
                    job.is_terminated()
                        && job
                            .completion_date
                            .is_some_and(|date| now - date >= max_age_delta)
                })
                .map(|job| job.job_id)
                .collect();
            if job_ids.is_empty() {
                continue;
            }
            log::info!("Auto-forgetting {} job(s)", job_ids.len());
            forget_jobs(&mut state, &senders, &job_ids)
        };
        // Dropping jobs can take a long time, so we do it outside the event loop.
        let _ = tokio::task::spawn_blocking(move || {
            drop(forgotten_jobs);
        })
        .await;
    }
}
