use tui::style::{Color, Modifier, Style};

const GREEN_THRESHOLD: f32 = 0.5;
const YELLOW_THRESHOLD: f32 = 0.7;

/**
 *   Progress bar's structure: [StartBlock]<>[indicator][][]<>[][][][unused_area]<>[end_block]
 **/
pub struct ProgressPrintStyle {
    /// The first character of the progress bar
    start_block: char,
    /// The progress indicator
    indicator: char,
    /// It fills the empty space between current progress and max progress
    unused_area: char,
    /// The ending character of the progressbar
    end_block: char,
}

pub fn get_progress_bar_color(progress: f32) -> Style {
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
    progress: f32,
    num_characters: u8,
    style: ProgressPrintStyle,
) -> String {
    assert!((0.00..=1.00).contains(&progress));
    //to keep the length of the progressbar correct after the padding
    let num_characters = num_characters - 2;
    let indicator_count = (progress * (num_characters as f32)).ceil();
    let start_block = style.start_block;
    let indicator = std::iter::repeat(style.indicator)
        .take(indicator_count as usize)
        .collect::<String>();
    let filler = std::iter::repeat(style.unused_area)
        .take(num_characters as usize - indicator_count as usize)
        .collect::<String>();

    format!(
        "{}{}{}{}: {}%",
        start_block,
        indicator,
        filler,
        style.end_block,
        (progress * 100.00) as u32
    )
}

impl Default for ProgressPrintStyle {
    fn default() -> Self {
        Self {
            start_block: '╢',
            end_block: '╟',
            indicator: '▌',
            unused_area: '░',
        }
    }
}
