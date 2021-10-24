use crate::dashboard::ui::terminal::DashboardFrame;
use tui::layout::{Alignment, Rect};
use tui::text::Spans;
use tui::widgets::{Block, Paragraph, Wrap};

pub fn draw_text(rect: Rect, frame: &mut DashboardFrame, text: &str) {
    let header_text = vec![Spans::from(text)];
    let paragraph = Paragraph::new(header_text)
        .block(Block::default())
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, rect);
}
