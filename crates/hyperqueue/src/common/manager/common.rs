use std::time::Duration;

/// Format a duration as a PBS/Slurm time string, e.g. 01:05:02
pub(super) fn format_duration(duration: &Duration) -> String {
    let mut seconds = duration.as_secs();
    let hours = seconds / 3600;
    seconds %= 3600;
    let minutes = seconds / 60;
    seconds %= 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

#[cfg(test)]
mod test {
    use super::format_duration;
    use std::time::Duration;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(&Duration::from_secs(0)), "00:00:00");
        assert_eq!(format_duration(&Duration::from_secs(1)), "00:00:01");
        assert_eq!(format_duration(&Duration::from_secs(61)), "00:01:01");
        assert_eq!(format_duration(&Duration::from_secs(3661)), "01:01:01");
    }
}
