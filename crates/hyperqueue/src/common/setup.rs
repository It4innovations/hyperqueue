use env_logger::DEFAULT_FILTER_ENV;
use env_logger::fmt::style::{AnsiColor, Color, Style};
use log::LevelFilter;
use std::io::Write;

/// Sets the behavior of the logger, based on passed environment variables
/// such as `RUST_LOG`.
pub fn setup_logging(verbose: bool) {
    let mut builder = env_logger::Builder::default();
    builder.filter_level(if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    });

    let has_debug = std::env::var(DEFAULT_FILTER_ENV)
        .map(|v| v.contains("debug"))
        .unwrap_or(false);

    if verbose || has_debug {
        builder.format_timestamp_millis();
    } else {
        // Shortened format
        // <time> <level> <message>
        builder.format(|buf, record| {
            let level_style = buf.default_level_style(record.level()).bold();
            let style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Black)));
            let timestamp = buf.timestamp_seconds();
            let level = record.level();
            let args = record.args();
            writeln!(
                buf,
                "{style}{timestamp}{style:#} {level_style}{level}{level_style:#} {args}",
            )
        });
    }

    // Overwrite the defaults from env
    builder.parse_default_env();
    builder.init();
}
