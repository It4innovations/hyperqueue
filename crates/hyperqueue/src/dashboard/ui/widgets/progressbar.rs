use crate::server::job::JobTaskCounters;

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

pub fn render_job_task_progress_bar(counters: &JobTaskCounters, style: ProgressPrintStyle) -> String {
    let num_tasks = (counters.n_canceled_tasks
        + counters.n_failed_tasks
        + counters.n_finished_tasks
        + counters.n_running_tasks) as f32;
    let num_characters = 20;

    let p_running_tasks = counters.n_running_tasks as f32 / num_tasks ;
    let p_finished_tasks = counters.n_finished_tasks as f32/ num_tasks;
    let p_failed_tasks = counters.n_failed_tasks as f32/ num_tasks;
    let p_cancelled_tasks = counters.n_canceled_tasks as f32/ num_tasks;

    let running_tasks_indicator_count = (p_running_tasks * (num_characters as f32)).ceil();
    let running_tasks_indicator = std::iter::repeat(">")
        .take(running_tasks_indicator_count as usize)
        .collect::<String>();

    let finished_tasks_indicator_count = (p_finished_tasks * (num_characters as f32)).ceil();
    let finished_tasks_indicator = std::iter::repeat(style.indicator)
        .take(finished_tasks_indicator_count as usize)
        .collect::<String>();

    let cancelled_task_indicator_count = (p_cancelled_tasks * (num_characters as f32)).ceil();
    let cancelled_tasks_indicator = std::iter::repeat('c')
        .take(cancelled_task_indicator_count as usize)
        .collect::<String>();

    let failed_tasks_indicator_count = (p_failed_tasks * (num_characters as f32)).ceil();
    let failed_tasks_indicator = std::iter::repeat('F')
        .take(failed_tasks_indicator_count as usize)
        .collect::<String>();

    format!(
        "{}{}{}{}{}{}: {}%",
        style.start_block,
        running_tasks_indicator,
        finished_tasks_indicator,
        cancelled_tasks_indicator,
        failed_tasks_indicator,
        style.end_block,
        (running_tasks_indicator_count * 100.00) as u32
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
