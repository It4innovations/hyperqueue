use std::time::Duration;

/// Format a duration as a PBS/Slurm time string, e.g. 01:05:02
pub(super) fn format_duration(duration: &Duration) -> String {
    let mut seconds = duration.as_secs();
    let hours = seconds / 3600;
    seconds %= 3600;
    let minutes = seconds / 60;
    seconds %= 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

/// Parse duration from string
pub fn parse_hms_duration(raw_time: &str) -> anyhow::Result<Duration> {
    use chrono::Duration as ChronoDuration;
    let numbers: Vec<&str> = raw_time.split(':').collect();
    let duration = ChronoDuration::hours(numbers[0].parse()?)
        + ChronoDuration::minutes(numbers[1].parse()?)
        + ChronoDuration::seconds(numbers[2].parse()?);
    Ok(duration.to_std()?)
}

#[cfg(test)]
mod test {
    use super::{format_duration, parse_hms_duration};
    use std::time::Duration;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(&Duration::from_secs(0)), "00:00:00");
        assert_eq!(format_duration(&Duration::from_secs(1)), "00:00:01");
        assert_eq!(format_duration(&Duration::from_secs(61)), "00:01:01");
        assert_eq!(format_duration(&Duration::from_secs(3661)), "01:01:01");
    }

    #[test]
    fn test_parse_hms_duration() {
        let date = parse_hms_duration("03:01:34").unwrap();
        assert_eq!(date, Duration::from_secs(3 * 3600 + 60 + 34));
    }
}
