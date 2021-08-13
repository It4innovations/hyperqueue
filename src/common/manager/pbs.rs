use chrono::Duration as ChronoDuration;
use serde_json::Value;
use std::process::Command;
use std::str;
use std::time::Duration;

fn parse_duration(raw_time: &str) -> anyhow::Result<Duration> {
    let numbers: Vec<&str> = raw_time.split(':').collect();
    let duration = ChronoDuration::hours(numbers[0].parse()?)
        + ChronoDuration::minutes(numbers[1].parse()?)
        + ChronoDuration::seconds(numbers[2].parse()?);
    Ok(duration.to_std()?)
}

fn get_time(job_id: &str, data: &str) -> anyhow::Result<Duration> {
    let data_json: Value = serde_json::from_str(data)?;

    let walltime = parse_duration(
        data_json["Jobs"][job_id]["Resource_List"]["walltime"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Could not find walltime key for job {}", job_id))?,
    )?;
    let used = parse_duration(
        data_json["Jobs"][job_id]["resources_used"]["walltime"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Could not find used time key for job {}", job_id))?,
    )?;

    Ok(walltime - used)
}

/// Calculates how much time is left for the given job using `qstat`.
pub fn get_remaining_timelimit(job_id: &str) -> anyhow::Result<Duration> {
    let result = Command::new("qstat")
        .args(&["-f", "-F", "json", job_id])
        .output()?;
    if !result.status.success() {
        anyhow::bail!(
            "qstat command exited with {}: {}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        );
    }

    let output = String::from_utf8_lossy(&result.stdout).into_owned();
    log::debug!("qstat output: {}", output.trim());

    get_time(job_id, output.as_str())
}
