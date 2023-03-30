use tui::style::{Color, Modifier, Style};

const GREEN_THRESHOLD: f64 = 0.5;
const YELLOW_THRESHOLD: f64 = 0.7;

/**
 *   Progress bar's structure: [StartBlock]<>[indicator][][]<>[][][][unused_area]<>[end_block]
 **/
pub struct ProgressPrintStyle {
    indicator: char,
    /// It fills the empty space between current progress and max progress
    unused_area: char,
}

pub fn get_progress_bar_color(progress: f64) -> Style {
    let color = if progress <= GREEN_THRESHOLD {
        Color::Green
    } else if progress <= YELLOW_THRESHOLD {
        Color::Yellow
    } else {
        Color::Red
    };

    Style {
        fg: Some(color),
        bg: None,
        add_modifier: Modifier::empty(),
        sub_modifier: Modifier::empty(),
    }
}

/**
 * Creates a string progress bar for 0 < progress < 1
 */
pub fn render_progress_bar_at(
    label: Option<String>,
    progress: f64,
    width: u8,
    style: ProgressPrintStyle,
) -> String {
    assert!((0.00..=1.00).contains(&progress));
    let label = label.unwrap_or_default();

    // Keep the length of the progressbar correct after the padding and %usage label
    let num_characters = width - 6 - label.len() as u8;
    let indicator_count = (progress * (num_characters as f64)).ceil();
    let indicator = std::iter::repeat(style.indicator)
        .take(indicator_count as usize)
        .collect::<String>();
    let filler = std::iter::repeat(style.unused_area)
        .take(num_characters as usize - indicator_count as usize)
        .collect::<String>();

    let percent = format!("({}%)", (progress * 100.00) as u32);
    format!("{label}{indicator}{filler} {percent:>5}")
}

impl Default for ProgressPrintStyle {
    fn default() -> Self {
        Self {
            indicator: '▌',
            unused_area: '░',
        }
    }
}
