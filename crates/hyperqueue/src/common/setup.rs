use env_logger::fmt::Color;
use env_logger::{DEFAULT_FILTER_ENV, DEFAULT_WRITE_STYLE_ENV};
use std::io::Write;

pub fn setup_logging(debug: bool) {
    if std::env::var(DEFAULT_FILTER_ENV).is_err() {
        std::env::set_var(DEFAULT_FILTER_ENV, if debug { "debug" } else { "info" });
    }

    let mut builder = env_logger::builder();
    if std::env::var(DEFAULT_WRITE_STYLE_ENV).is_err() {
        if debug {
            builder.format_timestamp_millis();
        } else {
            // Normal run
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
    };
    builder.init();
}
