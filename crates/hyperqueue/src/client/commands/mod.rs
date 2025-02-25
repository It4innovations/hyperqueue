pub mod autoalloc;
pub mod job;
pub mod journal;
pub mod outputlog;
pub mod server;
pub mod submit;
pub mod wait;
pub mod worker;

/// Helper macro for generating CLI help for a `Duration` (or `Option<Duration>`) value
/// that can be specified either using the HMS or humantime formats.
macro_rules! duration_doc {
    ($text:expr) => {
        concat!(
            $text,
            "\n\n",
            r#"You can use either the `HH:MM:SS` format or a "humantime" format.
For example:
- 01:00:00 => 1 hour
- 02:05:10 => 2 hours, 5 minutes, 10 seconds
- 1h => 1 hour
- 2h5m10s => 2 hours, 5 minutes, 10 seconds
- 3h 10m 5s => 3 hours, 10 minutes, 5 seconds
- 2 hours 5 minutes => 2 hours, 5 minutes"#
        )
    };
}

pub(crate) use duration_doc;
