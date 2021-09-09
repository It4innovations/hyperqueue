use crate::server::job::JobTaskCounters;
use crate::JobTaskCount;
use colored::{Color, Colorize};
use std::fmt::Write;

#[macro_export]
macro_rules! rpc_call {
    ($conn:expr, $message:expr, $matcher:pat $(=> $result:expr)?) => {
        async {
            match $conn.send_and_receive($message).await? {
                $matcher => $crate::Result::Ok(($($result),*)),
                $crate::transfer::messages::ToClientMessage::Error(e) => {
                    $crate::common::error::error(format!("Received error: {:?}", e))
                }
                msg => {
                    $crate::common::error::error(format!("Received an invalid message {:?}", msg))
                }
            }
        }
    };
}

pub const TASK_COLOR_CANCELED: Color = Color::Magenta;
pub const TASK_COLOR_FAILED: Color = Color::Red;
pub const TASK_COLOR_FINISHED: Color = Color::Green;
pub const TASK_COLOR_RUNNING: Color = Color::Yellow;

/// Draws a colored progress bar that depicts counts of tasks with individual states
pub fn job_progress_bar(counters: JobTaskCounters, n_tasks: JobTaskCount, width: usize) -> String {
    let mut buffer = String::from("[");

    let parts = vec![
        (counters.n_canceled_tasks, TASK_COLOR_CANCELED),
        (counters.n_failed_tasks, TASK_COLOR_FAILED),
        (counters.n_finished_tasks, TASK_COLOR_FINISHED),
        (counters.n_running_tasks, TASK_COLOR_RUNNING),
    ];

    let chars = |count: JobTaskCount| {
        let ratio = count as f64 / n_tasks as f64;
        (ratio * width as f64).ceil() as usize
    };

    let mut total_char_count: usize = 0;
    for (count, color) in parts {
        let char_count = std::cmp::min(width - total_char_count, chars(count));
        write!(buffer, "{}", "#".repeat(char_count).color(color)).unwrap();
        total_char_count += char_count;
    }
    write!(
        buffer,
        "{}",
        ".".repeat(width.saturating_sub(total_char_count))
    )
    .unwrap();

    buffer.push(']');
    buffer
}
