//! Allocation queue backend that submits Slurm allocations through a (remote) FirecREST API
//! (<https://eth-cscs.github.io/firecrest-v2/>).
//!
//! Unlike the PBS/Slurm backends, this backend does not assume that the server runs on the
//! target cluster: submission, status queries and cancellation are performed via HTTPS
//! requests, authenticated with an OAuth2 client-credentials token. The spawned workers then
//! connect back to the server over TCP, exactly like manually started workers.
//!
//! Because the server is not on the cluster, the queue carries cluster-side paths
//! (`hq` binary, worker access file directory, working directory) in
//! [`FirecrestQueueParams`]; see its documentation for details.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

use anyhow::Context;
use serde::Deserialize;

use crate::common::manager::info::ManagerType;
use crate::common::utils::time::AbsoluteTime;
use crate::server::autoalloc::queue::common::{
    add_start_stop_worker_commands, build_worker_args, create_allocation_dir,
    format_allocation_name,
};
use crate::server::autoalloc::queue::slurm::build_slurm_submit_script;
use crate::server::autoalloc::queue::{
    AllocationExternalStatus, AllocationStatusMap, AllocationSubmissionResult, QueueHandler,
    SubmitMode,
};
use crate::server::autoalloc::{
    Allocation, AutoAllocResult, FirecrestQueueParams, QueueId, QueueInfo,
};
use tako::Map;

/// How long before its expiration do we consider a cached token unusable.
/// CSCS tokens are valid for ~5 minutes; a request started just before the expiration
/// could otherwise be rejected mid-flight.
const TOKEN_EXPIRATION_SLACK: Duration = Duration::from_secs(30);

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

/// Shared context for performing authenticated FirecREST requests.
/// It is cheaply cloneable so that it can be moved into the `'static` futures returned
/// by the [`QueueHandler`] methods.
///
/// The token cache is guarded by an async mutex that is held across the token fetch,
/// so that concurrent requests do not each fetch their own token; they wait for the
/// first fetch and then reuse the cached result.
#[derive(Clone)]
struct FirecrestContext {
    client: reqwest::Client,
    config: Rc<FirecrestQueueParams>,
    client_secret: Rc<String>,
    token: Rc<Mutex<Option<CachedToken>>>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Deserialize)]
struct SubmitResponse {
    #[serde(rename = "jobId")]
    job_id: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct JobStatus {
    state: String,
}

#[derive(Deserialize)]
struct JobTime {
    start: Option<i64>,
    end: Option<i64>,
}

#[derive(Deserialize)]
struct JobModel {
    #[serde(rename = "jobId", default)]
    job_id: Option<serde_json::Value>,
    status: JobStatus,
    time: Option<JobTime>,
}

impl JobModel {
    fn job_id_string(&self) -> Option<String> {
        self.job_id.as_ref().map(json_value_to_string)
    }
}

/// The job id is documented as a string, but be lenient if it arrives as a number.
fn json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(id) => id.clone(),
        other => other.to_string(),
    }
}

#[derive(Deserialize)]
struct GetJobResponse {
    jobs: Option<Vec<JobModel>>,
}

impl FirecrestContext {
    /// Returns a valid bearer token, fetching a fresh one through the OAuth2
    /// client-credentials grant if the cached one is missing or about to expire.
    async fn bearer_token(&self) -> AutoAllocResult<String> {
        let mut cached = self.token.lock().await;
        if let Some(token) = cached.as_ref()
            && token.expires_at > Instant::now() + TOKEN_EXPIRATION_SLACK
        {
            return Ok(token.token.clone());
        }

        log::debug!(
            "Fetching FirecREST auth token from {}",
            self.config.token_url
        );
        let response = self
            .client
            .post(&self.config.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.config.client_id),
                ("client_secret", &self.client_secret),
            ])
            .send()
            .await
            .context("Cannot reach the OAuth2 token endpoint")?;
        let response = check_response(response)
            .await
            .context("OAuth2 token request failed")?;
        let token: TokenResponse = response
            .json()
            .await
            .context("Cannot parse OAuth2 token response")?;

        let fresh = CachedToken {
            token: token.access_token,
            expires_at: Instant::now() + Duration::from_secs(token.expires_in),
        };
        *cached = Some(fresh.clone());
        Ok(fresh.token)
    }

    /// Drops the cached token, but only if it is still the one that was just rejected;
    /// a concurrent request may have already fetched a fresh one.
    async fn invalidate_token(&self, rejected: &str) {
        let mut cached = self.token.lock().await;
        if cached.as_ref().is_some_and(|token| token.token == rejected) {
            *cached = None;
        }
    }

    /// Sends an authenticated request. If the API rejects the token (HTTP 401) even
    /// though it has not expired yet (e.g. it was revoked, or the API and the token
    /// endpoint disagree about clocks), the request is retried once with a fresh token.
    async fn request_with_auth_retry(
        &self,
        build: impl Fn(String) -> reqwest::RequestBuilder,
    ) -> AutoAllocResult<reqwest::Response> {
        let token = self.bearer_token().await?;
        let response = build(token.clone())
            .send()
            .await
            .context("Cannot reach the FirecREST API")?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        log::debug!("FirecREST rejected the auth token, retrying with a fresh one");
        self.invalidate_token(&token).await;
        let token = self.bearer_token().await?;
        build(token)
            .send()
            .await
            .context("Cannot reach the FirecREST API")
    }

    fn jobs_url(&self) -> String {
        format!(
            "{}/compute/{}/jobs",
            self.config.api_url.trim_end_matches('/'),
            self.config.system
        )
    }

    /// Submits the given submit script and returns the created Slurm job id.
    async fn submit_job(&self, name: &str, script: &str) -> AutoAllocResult<String> {
        let url = self.jobs_url();
        let body = serde_json::json!({
            "job": {
                "name": name,
                "workingDirectory": self.config.remote_workdir,
                "script": script,
            }
        });
        let response = self
            .request_with_auth_retry(|token| self.client.post(&url).bearer_auth(token).json(&body))
            .await?;
        let response = check_response(response)
            .await
            .context("FirecREST job submission failed")?;
        let response: SubmitResponse = response
            .json()
            .await
            .context("Cannot parse FirecREST submit response")?;

        let job_id = response
            .job_id
            .ok_or_else(|| anyhow::anyhow!("FirecREST submit response is missing the job id"))?;
        Ok(json_value_to_string(&job_id))
    }

    /// Fetches all jobs currently visible to the user in one request and returns them
    /// keyed by job id. Used to refresh the status of all allocations at once, which
    /// keeps the number of API requests (and thus rate-limit pressure) independent of
    /// the number of active allocations.
    async fn list_jobs(&self) -> AutoAllocResult<Map<String, JobModel>> {
        let url = self.jobs_url();
        let response = self
            .request_with_auth_retry(|token| self.client.get(&url).bearer_auth(token))
            .await?;
        let response = check_response(response)
            .await
            .context("FirecREST job list query failed")?;
        let response: GetJobResponse = response
            .json()
            .await
            .context("Cannot parse FirecREST job list response")?;

        Ok(response
            .jobs
            .unwrap_or_default()
            .into_iter()
            .filter_map(|job| job.job_id_string().map(|id| (id, job)))
            .collect())
    }

    async fn job_status(&self, allocation_id: &str) -> AutoAllocResult<AllocationExternalStatus> {
        let url = format!("{}/{allocation_id}", self.jobs_url());
        let response = self
            .request_with_auth_retry(|token| self.client.get(&url).bearer_auth(token))
            .await?;
        let response = check_response(response)
            .await
            .with_context(|| format!("FirecREST status query for job {allocation_id} failed"))?;
        let response: GetJobResponse = response
            .json()
            .await
            .context("Cannot parse FirecREST job status response")?;

        let job = response
            .jobs
            .unwrap_or_default()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("FirecREST returned no record for the job"))?;
        parse_job_status(&job)
    }

    async fn cancel_job(&self, allocation_id: &str) -> AutoAllocResult<()> {
        let url = format!("{}/{allocation_id}", self.jobs_url());
        let response = self
            .request_with_auth_retry(|token| self.client.delete(&url).bearer_auth(token))
            .await?;
        // A job that has already finished or was removed from the scheduler's memory
        // cannot be canceled, which is fine for our purposes.
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        check_response(response)
            .await
            .with_context(|| format!("FirecREST cancellation of job {allocation_id} failed"))?;
        Ok(())
    }
}

async fn check_response(response: reqwest::Response) -> AutoAllocResult<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(anyhow::anyhow!("HTTP status {status}: {}", body.trim()))
    }
}

fn parse_job_status(job: &JobModel) -> AutoAllocResult<AllocationExternalStatus> {
    let epoch_to_time = |secs: i64| -> AbsoluteTime {
        AbsoluteTime::from(UNIX_EPOCH + Duration::from_secs(secs.max(0) as u64))
    };
    let started_at = job
        .time
        .as_ref()
        .and_then(|time| time.start)
        .map(epoch_to_time);
    let finished_at = job
        .time
        .as_ref()
        .and_then(|time| time.end)
        .map(epoch_to_time)
        .unwrap_or_else(|| AbsoluteTime::from(SystemTime::now()));

    // FirecREST reports Slurm job states; the mapping mirrors `slurm.rs::parse_slurm_status`.
    // Cancelled jobs can be reported e.g. as `CANCELLED by <uid>`.
    let state = job.status.state.as_str();
    let status = match state {
        "PENDING" | "CONFIGURING" | "REQUEUED" | "SUSPENDED" => AllocationExternalStatus::Queued,
        "RUNNING" | "COMPLETING" => AllocationExternalStatus::Running,
        "COMPLETED" | "TIMEOUT" => AllocationExternalStatus::Finished {
            started_at,
            finished_at,
        },
        _ if state == "FAILED"
            || state.starts_with("CANCELLED")
            || state == "NODE_FAIL"
            || state == "OUT_OF_MEMORY"
            || state == "PREEMPTED"
            || state == "BOOT_FAIL"
            || state == "DEADLINE" =>
        {
            AllocationExternalStatus::Failed {
                started_at,
                finished_at,
            }
        }
        other => anyhow::bail!("Unknown Slurm job status {other}"),
    };
    Ok(status)
}

pub struct FirecrestHandler {
    ctx: FirecrestContext,
    server_directory: PathBuf,
    name: Option<String>,
    allocation_counter: u64,
}

impl FirecrestHandler {
    pub fn new(
        server_directory: PathBuf,
        name: Option<String>,
        config: FirecrestQueueParams,
    ) -> anyhow::Result<Self> {
        let client_secret = std::env::var(&config.client_secret_env).map_err(|_| {
            anyhow::anyhow!(
                "The environment variable `{}` with the FirecREST client secret is not set \
in the environment of the server",
                config.client_secret_env
            )
        })?;
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("Cannot create HTTP client")?;

        Ok(Self {
            ctx: FirecrestContext {
                client,
                config: Rc::new(config),
                client_secret: Rc::new(client_secret),
                token: Rc::new(Mutex::new(None)),
            },
            server_directory,
            name,
            allocation_counter: 0,
        })
    }

    fn create_allocation_id(&mut self) -> u64 {
        self.allocation_counter += 1;
        self.allocation_counter
    }
}

impl QueueHandler for FirecrestHandler {
    fn submit_allocation(
        &mut self,
        queue_id: QueueId,
        queue_info: &QueueInfo,
        worker_count: u64,
        _mode: SubmitMode,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
        let params = queue_info.params().clone();
        let ctx = self.ctx.clone();
        let name = self.name.clone();
        let server_directory = self.server_directory.clone();
        let allocation_num = self.create_allocation_id();

        Box::pin(async move {
            // Local directory holding a debug copy of the submitted script
            let working_dir = create_allocation_dir(
                server_directory.clone(),
                queue_id,
                name.as_ref(),
                allocation_num,
            )?;

            let allocation_name = format_allocation_name(name, queue_id, allocation_num);

            let worker_args = build_worker_args(
                Path::new(&ctx.config.remote_hq_path),
                // The allocation is an ordinary Slurm job on the target cluster
                ManagerType::Slurm,
                Path::new(&ctx.config.remote_server_dir),
                &params,
            );
            let worker_args = add_start_stop_worker_commands(
                worker_args,
                params.worker_start_cmd.as_deref(),
                params.worker_stop_cmd.as_deref(),
            );

            // stdout/stderr live directly in the remote working directory, which is
            // required to exist; Slurm does not create missing directories for them.
            // These are paths on the target cluster: compose them as strings.
            let remote_workdir = ctx.config.remote_workdir.trim_end_matches('/');
            let stdout = format!("{remote_workdir}/{allocation_name}.stdout");
            let stderr = format!("{remote_workdir}/{allocation_name}.stderr");

            let script = build_slurm_submit_script(
                worker_count,
                params.timelimit,
                &allocation_name,
                &stdout,
                &stderr,
                &params.additional_args.join(" "),
                &worker_args,
            );

            // Keep a debug copy of the script, consistent with the PBS/Slurm backends
            std::fs::write(working_dir.submit_script(), &script)
                .context("Cannot write a debug copy of the submit script")?;

            let id = ctx.submit_job(&allocation_name, &script).await;
            if let Ok(id) = &id {
                std::fs::write(working_dir.jobid_file(), id)?;
            }

            Ok(AllocationSubmissionResult::new(id, working_dir))
        })
    }

    fn get_status_of_allocations(
        &self,
        allocations: &[&Allocation],
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>> {
        let allocation_ids: Vec<String> =
            allocations.iter().map(|alloc| alloc.id.clone()).collect();
        let ctx = self.ctx.clone();

        Box::pin(async move {
            let mut result = Map::with_capacity(allocation_ids.len());
            if allocation_ids.is_empty() {
                return Ok(result);
            }

            // A single list request covers all allocations; only allocations that have
            // already dropped out of the list (the API merges squeue and recent sacct
            // history) are queried individually.
            let jobs = ctx.list_jobs().await?;
            for allocation_id in allocation_ids {
                let status = match jobs.get(&allocation_id) {
                    Some(job) => parse_job_status(job),
                    None => ctx.job_status(&allocation_id).await,
                };
                result.insert(allocation_id, status);
            }
            Ok(result)
        })
    }

    fn remove_allocation(
        &self,
        allocation: &Allocation,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
        let allocation_id = allocation.id.clone();
        let ctx = self.ctx.clone();

        Box::pin(async move { ctx.cancel_job(&allocation_id).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(state: &str, start: Option<i64>, end: Option<i64>) -> JobModel {
        JobModel {
            job_id: None,
            status: JobStatus {
                state: state.to_string(),
            },
            time: Some(JobTime { start, end }),
        }
    }

    #[test]
    fn job_id_accepts_string_and_number() {
        let parse = |json: &str| -> Option<String> {
            serde_json::from_str::<JobModel>(json)
                .unwrap()
                .job_id_string()
        };
        assert_eq!(
            parse(r#"{"jobId": "123", "status": {"state": "PENDING"}}"#),
            Some("123".to_string())
        );
        assert_eq!(
            parse(r#"{"jobId": 123, "status": {"state": "PENDING"}}"#),
            Some("123".to_string())
        );
        assert_eq!(parse(r#"{"status": {"state": "PENDING"}}"#), None);
    }

    #[test]
    fn map_queued_and_running_states() {
        for state in ["PENDING", "CONFIGURING", "REQUEUED", "SUSPENDED"] {
            assert!(matches!(
                parse_job_status(&job(state, None, None)).unwrap(),
                AllocationExternalStatus::Queued
            ));
        }
        for state in ["RUNNING", "COMPLETING"] {
            assert!(matches!(
                parse_job_status(&job(state, Some(1), None)).unwrap(),
                AllocationExternalStatus::Running
            ));
        }
    }

    #[test]
    fn map_finished_states() {
        assert!(matches!(
            parse_job_status(&job("COMPLETED", Some(1), Some(2))).unwrap(),
            AllocationExternalStatus::Finished { .. }
        ));
        assert!(matches!(
            parse_job_status(&job("TIMEOUT", Some(1), Some(2))).unwrap(),
            AllocationExternalStatus::Finished { .. }
        ));
    }

    #[test]
    fn map_failed_states() {
        for state in [
            "FAILED",
            "CANCELLED",
            "CANCELLED by 12345",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
            "BOOT_FAIL",
            "DEADLINE",
        ] {
            assert!(matches!(
                parse_job_status(&job(state, Some(1), Some(2))).unwrap(),
                AllocationExternalStatus::Failed { .. }
            ));
        }
    }

    #[test]
    fn unknown_state_is_an_error() {
        assert!(parse_job_status(&job("FROBNICATING", None, None)).is_err());
    }

    #[test]
    fn finished_without_end_time_falls_back_to_now() {
        assert!(matches!(
            parse_job_status(&job("COMPLETED", None, None)).unwrap(),
            AllocationExternalStatus::Finished { .. }
        ));
    }
}
