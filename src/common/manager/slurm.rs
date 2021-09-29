use crate::common::manager::common::format_duration;
use std::time::Duration;

/// Format a duration as a SLURM time string, e.g. 01:05:02
pub fn format_slurm_duration(duration: &Duration) -> String {
    format_duration(duration)
}

pub fn parse_slurm_datetime(datetime: &str) -> anyhow::Result<chrono::NaiveDateTime> {
    Ok(chrono::NaiveDateTime::parse_from_str(
        datetime,
        "%Y-%m-%dT%H:%M:%S",
    )?)
}

#[cfg(test)]
mod test {
    use crate::common::manager::slurm::parse_slurm_datetime;

    #[test]
    fn test_parse_slurm_datetime() {
        let date = parse_slurm_datetime("2021-09-29T09:36:56").unwrap();
        assert_eq!(
            date.format("%d.%m.%Y %H:%M:%S").to_string(),
            "29.09.2021 09:36:56"
        );
    }
}
