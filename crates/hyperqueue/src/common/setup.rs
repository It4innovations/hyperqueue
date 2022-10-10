use env_logger::fmt::Color;
use env_logger::DEFAULT_FILTER_ENV;
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
            let mut level_style = buf.default_level_style(record.level());
            level_style.set_bold(true);
            let mut style = buf.style();
            style.set_color(Color::Black).set_intense(true);
            writeln!(
                buf,
                "{} {} {}",
                style.value(buf.timestamp_seconds()),
                level_style.value(record.level()),
                record.args()
            )
        });
    }

    // Overwrite the defaults from env
    builder.parse_default_env();
    builder.init();
}
