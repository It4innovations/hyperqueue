use std::time::Duration;
use std::time::SystemTime;

use anyhow::anyhow;
use chrono::TimeZone;
use nom::character::complete::char;
use nom::combinator::{map_res, opt};
use nom::sequence::{preceded, tuple};

use crate::common::parser::{consume_all, p_u32};

// Allows specifying humantime format (2h, 3m, etc.)
crate::arg_wrapper!(ArgDuration, Duration, humantime::parse_duration);

// Allows specifying humantime format or HH:MM:SS
crate::arg_wrapper!(ExtendedArgDuration, Duration, parse_hms_or_human_time);

fn parse_hms_or_human_time(text: &str) -> anyhow::Result<Duration> {
    parse_hms_time(text)
        .or_else(|_| humantime::parse_duration(text))
        .map_err(|e| {
            anyhow!(
                "Could not parse PBS walltime. Use either `HH:MM:SS` or humantime format (2hours): {:?}", e
            )
        })
}

pub fn local_to_system_time(datetime: chrono::NaiveDateTime) -> SystemTime {
    chrono::Local.from_local_datetime(&datetime).unwrap().into()
}

/// Parses time strings in the format [[hh:]mm:]ss.
/// Individual time values may be zero padded.
pub fn parse_hms_time(input: &str) -> anyhow::Result<Duration> {
    let parser = map_res(
        tuple((
            p_u32,
            opt(preceded(char(':'), p_u32)),
            opt(preceded(char(':'), p_u32)),
        )),
        |parsed| match parsed {
            (seconds, None, None) => Ok(Duration::from_secs(seconds as u64)),
            (minutes, Some(seconds), None) => {
                Ok(Duration::from_secs(minutes as u64 * 60 + seconds as u64))
            }
            (hours, Some(minutes), Some(seconds)) => Ok(Duration::from_secs(
                hours as u64 * 3600 + minutes as u64 * 60 + seconds as u64,
            )),
            _ => Err(anyhow!("Invalid time specification")),
        },
    );
    consume_all(parser, input)
}

#[cfg(test)]
mod tests {
    use crate::common::timeutils::parse_hms_time;

    #[test]
    fn parse_hms_seconds() {
        let duration = parse_hms_time("01").unwrap();
        assert_eq!(duration.as_secs(), 1);

        let duration = parse_hms_time("1").unwrap();
        assert_eq!(duration.as_secs(), 1);
    }

    #[test]
    fn parse_hms_minutes() {
        let duration = parse_hms_time("1:1").unwrap();
        assert_eq!(duration.as_secs(), 1 * 60 + 1);

        let duration = parse_hms_time("80:02").unwrap();
        assert_eq!(duration.as_secs(), 80 * 60 + 2);
    }

    #[test]
    fn parse_hms_hours() {
        let duration = parse_hms_time("1:1:1").unwrap();
        assert_eq!(duration.as_secs(), 1 * 3600 + 1 * 60 + 1);

        let duration = parse_hms_time("02:03:04").unwrap();
        assert_eq!(duration.as_secs(), 2 * 3600 + 3 * 60 + 4);
    }
}
