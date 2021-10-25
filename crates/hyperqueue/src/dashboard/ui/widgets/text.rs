use crate::dashboard::ui::terminal::DashboardFrame;
use tui::layout::{Alignment, Rect};
use tui::style::Style;
use tui::text::Spans;
use tui::widgets::{Block, Paragraph, Wrap};

pub fn draw_text(text: &str, rect: Rect, frame: &mut DashboardFrame, text_style: Style) {
    let header_text = vec![Spans::from(text)];
    let paragraph = Paragraph::new(header_text)
        .style(text_style)
        .block(Block::default())
        .alignment(Alignment::Left)
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, rect);
}
