use std::time::{Duration, SystemTime};

use anyhow::anyhow;
use chrono::TimeZone;
use chumsky::error::Simple;
use chumsky::prelude::just;
use chumsky::Parser;

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

fn parse_hms_time_inner() -> impl CharParser<Duration> {
    let digits = parse_u32();
    let time2 = just(':').ignore_then(parse_u32()).or_not();
    let time3 = just(':').ignore_then(parse_u32()).or_not();

    let time = digits.then(time2).then(time3);
    time.try_map(|parsed, span| match parsed {
        ((seconds, None), None) => Ok(Duration::from_secs(seconds as u64)),
        ((minutes, Some(seconds)), None) => {
            Ok(Duration::from_secs(minutes as u64 * 60 + seconds as u64))
        }
        ((hours, Some(minutes)), Some(seconds)) => Ok(Duration::from_secs(
            hours as u64 * 3600 + minutes as u64 * 60 + seconds as u64,
        )),
        _ => Err(Simple::custom(span, "Invalid time specification")),
    })
    .labelled("time in [[HH:]MM:]SS format")
}

/// Parses time strings in the format [[hh:]mm:]ss.
/// Individual time values may be zero padded.
pub fn parse_hms_time(input: &str) -> anyhow::Result<Duration> {
    all_consuming(parse_hms_time_inner()).parse_text(input)
}

#[cfg(not(test))]
pub fn now_monotonic() -> std::time::Instant {
    std::time::Instant::now()
}

use crate::common::parser2::{all_consuming, parse_u32, CharParser};
#[cfg(test)]
pub use mock_time::now_monotonic;

/// Testing utilities for mocking (monotonic) timestamps.
/// Use the `now_monotonic` function if you want to be able to mock the time in tests.
#[cfg(test)]
pub mod mock_time {
    use std::cell::RefCell;
    use std::time::Instant;

    thread_local! {
        static MOCK_TIME: RefCell<Option<Instant>> = RefCell::new(None);
    }

    pub struct MockTime;

    impl MockTime {
        pub fn mock(time: Instant) -> Self {
            MOCK_TIME.with(|cell| {
                assert!(cell.borrow().is_none());
                *cell.borrow_mut() = Some(time);
            });
            MockTime
        }
    }

    impl Drop for MockTime {
        fn drop(&mut self) {
            MOCK_TIME.with(|cell| *cell.borrow_mut() = None);
        }
    }

    pub fn now_monotonic() -> Instant {
        MOCK_TIME.with(|cell| cell.borrow().as_ref().cloned().unwrap_or_else(Instant::now))
    }
}

#[cfg(test)]
mod tests {
    use crate::common::parser2::{all_consuming, CharParser};
    use crate::common::utils::time::parse_hms_time_inner;
    use crate::tests::utils::expect_parser_error;

    #[test]
    fn parse_hms_seconds() {
        let duration = parse_hms_time_inner().parse_text("01").unwrap();
        assert_eq!(duration.as_secs(), 1);

        let duration = parse_hms_time_inner().parse_text("1").unwrap();
        assert_eq!(duration.as_secs(), 1);
    }

    #[test]
    fn parse_hms_minutes() {
        let duration = parse_hms_time_inner().parse_text("1:1").unwrap();
        assert_eq!(duration.as_secs(), 1 * 60 + 1);

        let duration = parse_hms_time_inner().parse_text("80:02").unwrap();
        assert_eq!(duration.as_secs(), 80 * 60 + 2);
    }

    #[test]
    fn parse_hms_hours() {
        let duration = parse_hms_time_inner().parse_text("1:1:1").unwrap();
        assert_eq!(duration.as_secs(), 1 * 3600 + 1 * 60 + 1);

        let duration = parse_hms_time_inner().parse_text("02:03:04").unwrap();
        assert_eq!(duration.as_secs(), 2 * 3600 + 3 * 60 + 4);
    }

    #[test]
    fn parse_hms_no_number() {
        insta::assert_snapshot!(expect_parser_error(parse_hms_time_inner(), "x"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          x
          |
          --- Unexpected token `x`
        "###);
    }

    #[test]
    fn parse_hms_trailing_colon() {
        insta::assert_snapshot!(expect_parser_error(all_consuming(parse_hms_time_inner()), "12:"), @r###"
        Unexpected end of input found while attempting to parse number, expected something else:
          12:
             |
             --- Unexpected end of input
        "###);
    }

    #[test]
    fn parse_hms_minutes_no_number() {
        insta::assert_snapshot!(expect_parser_error(all_consuming(parse_hms_time_inner()), "12:x"), @r###"
        Unexpected token found while attempting to parse number, expected something else:
          12:x
             |
             --- Unexpected token `x`
        "###);
    }
}
