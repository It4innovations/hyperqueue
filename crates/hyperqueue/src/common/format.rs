use tako::resources::ResourceAmount;
use tako::worker::ServerLostPolicy;

pub fn human_duration(duration: chrono::Duration) -> String {
    // Truncate to reasonable precision
    if duration.num_hours() > 0 {
        chrono::Duration::minutes(duration.num_minutes())
    } else if duration.num_minutes() > 0 {
        chrono::Duration::seconds(duration.num_seconds())
    } else {
        chrono::Duration::milliseconds(duration.num_milliseconds())
    }
    .to_std()
    .map(|d| humantime::format_duration(d).to_string())
    .unwrap_or_else(|_| "Invalid duration".to_string())
}

pub fn human_size(size: u64) -> String {
    if size < 2048 {
        format!("{size} B")
    } else if size < 2 * 1024 * 1024 {
        format!("{:.2} KiB", size as f64 / 1024.0)
    } else if size < 2 * 1024 * 1024 * 1024 {
        format!("{:.2} MiB", size as f64 / (1024 * 1024) as f64)
    } else {
        format!("{:.2} GiB", size as f64 / (1024 * 1024 * 1024) as f64)
    }
}

pub fn human_mem_amount(amount: ResourceAmount) -> String {
    let f = amount.as_f32();
    if f < 512f32 {
        format!("{f:.2} MiB")
    } else {
        format!("{:.2} GiB", f / 1024.0)
    }
}

pub fn server_lost_policy_to_str(policy: &ServerLostPolicy) -> &str {
    match policy {
        ServerLostPolicy::Stop => "stop",
        ServerLostPolicy::FinishRunning => "finish-running",
    }
}

#[cfg(test)]
mod tests {
    use crate::common::format::{human_duration, human_size};
    use chrono::Duration;

    #[test]
    fn test_sizes() {
        assert_eq!(human_size(0).as_str(), "0 B");
        assert_eq!(human_size(1).as_str(), "1 B");
        assert_eq!(human_size(1230).as_str(), "1230 B");
        assert_eq!(human_size(300_000).as_str(), "292.97 KiB");
        assert_eq!(human_size(50_000_000).as_str(), "47.68 MiB");
        assert_eq!(human_size(500_250_000_000).as_str(), "465.89 GiB");
    }

    #[test]
    fn test_durations() {
        assert_eq!(human_duration(Duration::nanoseconds(123456)).as_str(), "0s");
        assert_eq!(
            human_duration(Duration::nanoseconds(123456890123)).as_str(),
            "2m 3s"
        );
        assert_eq!(
            human_duration(Duration::milliseconds(1500)).as_str(),
            "1s 500ms"
        );
        assert_eq!(human_duration(Duration::milliseconds(1000)).as_str(), "1s");
        assert_eq!(
            human_duration(Duration::milliseconds(62111)).as_str(),
            "1m 2s"
        );
        assert_eq!(
            human_duration(Duration::seconds(100) + Duration::nanoseconds(100)).as_str(),
            "1m 40s"
        );
        assert_eq!(human_duration(Duration::seconds(11111)).as_str(), "3h 5m");
        assert_eq!(
            human_duration(
                Duration::days(7)
                    + Duration::hours(8)
                    + Duration::minutes(9)
                    + Duration::seconds(11)
            )
            .as_str(),
            "7days 8h 9m"
        );
    }
}
